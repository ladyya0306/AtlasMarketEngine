import argparse
import json
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.config_loader import SimulationConfig
from services.agent_service import AgentService
from simulation_runner import SimulationRunner

DEFAULT_SOURCE_PLAN = ROOT / "results" / "line_a_natural_activation_bridge" / "fixed_supply_spindle_medium_seller_demandheavy1p5x_m3_20260416" / "forced_role_batch_20260416_004439" / "V2_s606_m3_a149" / "plan.yaml"
BASELINE_CONFIG = ROOT / "config" / "baseline.yaml"


def _snapshot_healthy(db_path: Path, expected_agents: int, expected_properties: int = 1) -> bool:
    if not db_path.exists():
        return False
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        agents = int(cur.execute("SELECT COUNT(*) FROM agents_static").fetchone()[0] or 0)
        props = int(cur.execute("SELECT COUNT(*) FROM properties_static").fetchone()[0] or 0)
        conn.close()
    except Exception:
        return False
    return agents >= max(1, int(expected_agents * 0.9)) and props >= int(expected_properties)


def _load_plan(plan_path: Path) -> tuple[SimulationConfig, dict]:
    cfg = SimulationConfig(str(plan_path))
    meta = dict(cfg.get("line_b_metadata", {}) or {})
    return cfg, meta


def _deep_merge_dict(base: dict, override: dict) -> dict:
    merged = dict(base or {})
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_dict(dict(merged.get(key) or {}), value)
        else:
            merged[key] = value
    return merged


def _write_full_config_from_plan(plan_path: Path, target_path: Path, min_hold_months: int) -> SimulationConfig:
    baseline_cfg = SimulationConfig(str(BASELINE_CONFIG))
    try:
        source_payload = yaml.safe_load(plan_path.read_text(encoding="utf-8")) or {}
    except Exception:
        source_payload = {}
    if not isinstance(source_payload, dict):
        source_payload = {}
    baseline_cfg._config = _deep_merge_dict(dict(baseline_cfg._config or {}), source_payload)
    _ensure_min_hold_rule(baseline_cfg, min_hold_months=min_hold_months)
    baseline_cfg.save(str(target_path))
    return SimulationConfig(str(target_path))


def _ensure_min_hold_rule(config: SimulationConfig, min_hold_months: int) -> None:
    config.update("smart_agent.min_holding_months_before_resale", int(min_hold_months))
    config.update("simulation.enable_intervention_panel", False)


def _refresh_profiled_property_buckets(runtime_db: Path, config: SimulationConfig) -> None:
    conn = sqlite3.connect(str(runtime_db))
    conn.row_factory = sqlite3.Row
    try:
        svc = AgentService(config, conn)
        mode_cfg = svc._profiled_market_mode_cfg()
        if not bool(mode_cfg.get("enabled", False)):
            return
        profile_pack = svc._resolve_profiled_market_pack(mode_cfg)
        if not isinstance(profile_pack, dict) or not profile_pack:
            return
        cur = conn.cursor()
        cur.execute(
            """
            SELECT pm.property_id,
                   pm.owner_id,
                   ps.zone,
                   ps.is_school_district,
                   ps.property_type,
                   COALESCE(pm.current_valuation, ps.initial_value, 0) AS base_value,
                   ps.building_area,
                   ps.quality,
                   0 AS bedrooms,
                   pm.status
            FROM properties_market pm
            JOIN properties_static ps
              ON ps.property_id = pm.property_id
            """
        )
        market_properties = [dict(row) for row in (cur.fetchall() or [])]
        if not market_properties:
            return
        experiment_mode = str(
            mode_cfg.get("experiment_mode", config.get("smart_agent.profiled_market_mode.experiment_mode", "abundant"))
            or "abundant"
        ).strip() or "abundant"
        rows = svc._build_runtime_profiled_property_assignments(
            market_properties=market_properties,
            profile_pack=profile_pack,
            experiment_mode=experiment_mode,
        )
        svc._replace_profiled_property_assignments(cur, rows)
    finally:
        conn.close()


def _resolve_resume_checkpoint_db(resume_run_dir: Path, resume_from_month: int | None) -> tuple[Path, int]:
    run_dir = Path(resume_run_dir).resolve()
    candidate_roots = [
        run_dir / "monthly_checkpoints",
        run_dir / "runtime_run" / "monthly_checkpoints",
    ]
    checkpoint_root = next((p for p in candidate_roots if p.exists()), None)
    if checkpoint_root is None:
        raise FileNotFoundError(f"未找到 monthly_checkpoints 目录: {run_dir}")

    if resume_from_month is None:
        month_dirs = sorted(
            [
                p for p in checkpoint_root.iterdir()
                if p.is_dir() and p.name.startswith("month_")
            ],
            key=lambda p: p.name,
        )
        if not month_dirs:
            raise FileNotFoundError(f"未找到任何月度 checkpoint: {checkpoint_root}")
        selected_dir = month_dirs[-1]
        try:
            completed_month = int(selected_dir.name.split("_")[-1])
        except Exception as exc:
            raise ValueError(f"无法识别 checkpoint 月份: {selected_dir.name}") from exc
    else:
        completed_month = int(resume_from_month)
        selected_dir = checkpoint_root / f"month_{completed_month:02d}"
        if not selected_dir.exists():
            raise FileNotFoundError(f"未找到指定月份 checkpoint: {selected_dir}")

    checkpoint_db = selected_dir / "simulation.db"
    if not checkpoint_db.exists():
        raise FileNotFoundError(f"checkpoint 缺少 simulation.db: {checkpoint_db}")
    return checkpoint_db, completed_month


def ensure_init_snapshot_from_plan(
    plan_path: Path,
    snapshot_root: Path,
    min_hold_months: int,
) -> dict:
    source_cfg, meta = _load_plan(plan_path)
    seed = int(meta.get("seed", source_cfg.get("simulation.random_seed", 606) or 606))
    agent_count = int(meta.get("agent_count", 149) or 149)
    months = int(meta.get("months", source_cfg.get("simulation.months", 3) or 3))
    group = str(meta.get("group", "V2") or "V2")

    snapshot_dir = snapshot_root / f"fixed_snapshot_{group}_s{seed}_a{agent_count}"
    snapshot_db = snapshot_dir / "simulation.db"
    snapshot_cfg = snapshot_dir / "config.yaml"
    snapshot_meta = snapshot_dir / "snapshot_meta.json"

    if snapshot_db.exists() and snapshot_cfg.exists() and _snapshot_healthy(snapshot_db, expected_agents=agent_count, expected_properties=50):
        return {
            "snapshot_dir": str(snapshot_dir.resolve()),
            "db_path": str(snapshot_db.resolve()),
            "config_path": str(snapshot_cfg.resolve()),
            "group": group,
            "seed": seed,
            "agent_count": agent_count,
            "months": months,
        }

    shutil.rmtree(snapshot_dir, ignore_errors=True)
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_config = _write_full_config_from_plan(
        plan_path=plan_path,
        target_path=snapshot_cfg,
        min_hold_months=min_hold_months,
    )

    runner = SimulationRunner(
        agent_count=agent_count,
        months=months,
        seed=seed,
        resume=False,
        config=snapshot_config,
        db_path=str(snapshot_db),
    )
    try:
        runner.initialize()
    finally:
        runner.close()

    snapshot_meta.write_text(
        json.dumps(
            {
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "source_plan": str(plan_path.resolve()),
                "group": group,
                "seed": seed,
                "agent_count": agent_count,
                "months": months,
                "min_holding_months_before_resale": int(min_hold_months),
                "snapshot_db": str(snapshot_db.resolve()),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    return {
        "snapshot_dir": str(snapshot_dir.resolve()),
        "db_path": str(snapshot_db.resolve()),
        "config_path": str(snapshot_cfg.resolve()),
        "group": group,
        "seed": seed,
        "agent_count": agent_count,
        "months": months,
    }


def run_replay(
    plan_path: Path,
    output_root: Path,
    snapshot_root: Path,
    min_hold_months: int,
    resume_run_dir: Path | None = None,
    resume_from_month: int | None = None,
) -> Path:
    if resume_run_dir is None:
        snapshot_info = ensure_init_snapshot_from_plan(
            plan_path=plan_path,
            snapshot_root=snapshot_root,
            min_hold_months=min_hold_months,
        )
        group = str(snapshot_info["group"])
        seed = int(snapshot_info["seed"])
        agent_count = int(snapshot_info["agent_count"])
        months = int(snapshot_info["months"])
        source_runtime_db = Path(snapshot_info["db_path"])
        resume_meta: dict[str, object] = {}
    else:
        source_cfg, meta = _load_plan(plan_path)
        group = str(meta.get("group", "V2") or "V2")
        seed = int(meta.get("seed", source_cfg.get("simulation.random_seed", 606) or 606))
        agent_count = int(meta.get("agent_count", 149) or 149)
        months = int(meta.get("months", source_cfg.get("simulation.months", 3) or 3))
        source_runtime_db, completed_month = _resolve_resume_checkpoint_db(
            Path(resume_run_dir),
            resume_from_month,
        )
        resume_meta = {
            "resume_run_dir": str(Path(resume_run_dir).resolve()),
            "resume_checkpoint_db": str(source_runtime_db.resolve()),
            "resume_completed_month": int(completed_month),
        }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_root = output_root / f"fixed_supply_spindle_medium_seller_demandheavy1p5x_m3_replay_fixholdctx_{datetime.now().strftime('%Y%m%d')}"
    run_dir = batch_root / f"forced_role_batch_{timestamp}" / f"{group}_s{seed}_m{months}_a{agent_count}"
    runtime_dir = run_dir / "runtime_run"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    runtime_db = runtime_dir / "simulation.db"
    shutil.copy2(str(source_runtime_db), str(runtime_db))

    run_plan = run_dir / "plan.yaml"
    config = _write_full_config_from_plan(
        plan_path=plan_path,
        target_path=run_plan,
        min_hold_months=min_hold_months,
    )
    _refresh_profiled_property_buckets(runtime_db, config)

    replay_meta = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source_plan": str(plan_path.resolve()),
        "snapshot_db": str(source_runtime_db.resolve()),
        "run_db": str(runtime_db.resolve()),
        "group": group,
        "seed": seed,
        "agent_count": agent_count,
        "months": months,
        "min_holding_months_before_resale": int(min_hold_months),
        "mechanism_fixes": [
            "negotiation_macro_context_uses_current_month",
            "post_purchase_min_hold_before_resale",
            "live_listing_hygiene_same_month_and_pending_filters",
            "runtime_fallback_bucket_split_and_canonical_alias",
        ],
    }
    replay_meta.update(resume_meta)
    (run_dir / "replay_meta.json").write_text(
        json.dumps(replay_meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    status = "fail"
    try:
        runner = SimulationRunner(
            agent_count=agent_count,
            months=months,
            seed=seed,
            resume=True,
            config=config,
            db_path=str(runtime_db),
        )
        runner.run(allow_intervention_panel=False)
        status = "pass" if runner.status == "completed" else "fail"
    except Exception:
        status = "fail"

    final_root = batch_root.parent / f"{batch_root.name}_{status}"
    if final_root.exists():
        shutil.rmtree(final_root, ignore_errors=True)
    shutil.move(str(batch_root), str(final_root))
    final_run_dir = final_root / f"forced_role_batch_{timestamp}" / f"{group}_s{seed}_m{months}_a{agent_count}"
    print(str(final_run_dir.resolve()))
    return final_run_dir


def main():
    parser = argparse.ArgumentParser(description="Replay a fixed-snapshot simulation from an existing plan.")
    parser.add_argument("--source-plan", default=str(DEFAULT_SOURCE_PLAN), help="Path to the source plan.yaml")
    parser.add_argument(
        "--output-root",
        default=str(ROOT / "results" / "line_a_natural_activation_bridge"),
        help="Parent directory for replay results",
    )
    parser.add_argument(
        "--snapshot-root",
        default=str(ROOT / "results" / "init_snapshots"),
        help="Directory used to store reusable month-0 snapshots",
    )
    parser.add_argument("--min-hold-months", type=int, default=12, help="Minimum holding months before resale")
    parser.add_argument(
        "--resume-run-dir",
        default="",
        help="已有 run 目录；若提供，则从该 run 的 monthly_checkpoints 续跑",
    )
    parser.add_argument(
        "--resume-from-month",
        type=int,
        default=None,
        help="从指定已完成月份的 checkpoint 续跑；不填则自动取最新月",
    )
    args = parser.parse_args()

    run_replay(
        plan_path=Path(args.source_plan).resolve(),
        output_root=Path(args.output_root).resolve(),
        snapshot_root=Path(args.snapshot_root).resolve(),
        min_hold_months=int(args.min_hold_months),
        resume_run_dir=Path(args.resume_run_dir).resolve() if str(args.resume_run_dir).strip() else None,
        resume_from_month=int(args.resume_from_month) if args.resume_from_month is not None else None,
    )


if __name__ == "__main__":
    main()
