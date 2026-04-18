from __future__ import annotations

import argparse
import datetime as dt
import importlib
import json
import os
import shutil
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _setup_utf8_stdio() -> None:
    for stream_name in ("stdin", "stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream and hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8")
            except Exception:
                pass


def _parse_income_shock(spec: str) -> Dict[str, Any]:
    parts = [part.strip() for part in str(spec or "").split(":")]
    if len(parts) < 2:
        raise ValueError(f"Invalid --income-shock spec: {spec}")
    month = int(parts[0])
    pct_change = float(parts[1])
    target_tier = parts[2] if len(parts) >= 3 and parts[2] else "all"
    return {
        "action_type": "income_shock",
        "month": month,
        "pct_change": pct_change,
        "target_tier": target_tier,
    }


def _parse_developer_supply(spec: str) -> Dict[str, Any]:
    parts = [part.strip() for part in str(spec or "").split(":")]
    if len(parts) < 3:
        raise ValueError(f"Invalid --developer-supply spec: {spec}")
    month = int(parts[0])
    zone = parts[1].upper()
    count = int(parts[2])
    template = parts[3] if len(parts) >= 4 and parts[3] else "mixed_balanced"
    return {
        "action_type": "developer_supply",
        "month": month,
        "zone": zone,
        "count": count,
        "template": template,
    }


def _parse_supply_cut(spec: str) -> Dict[str, Any]:
    parts = [part.strip() for part in str(spec or "").split(":")]
    if len(parts) < 3:
        raise ValueError(f"Invalid --supply-cut spec: {spec}")
    month = int(parts[0])
    zone = parts[1].upper()
    count = int(parts[2])
    return {
        "action_type": "supply_cut",
        "month": month,
        "zone": zone,
        "count": count,
    }


def _default_preplanned_interventions() -> List[Dict[str, Any]]:
    return [
        {"action_type": "income_shock", "month": 2, "pct_change": -0.10, "target_tier": "all"},
        {"action_type": "developer_supply", "month": 2, "zone": "A", "count": 3, "template": "mixed_balanced"},
        {"action_type": "supply_cut", "month": 3, "zone": "A", "count": 2},
    ]


def _coerce_project_dir(path_str: str | None, *, snapshot_id: str, months: int, demand_multiplier: float) -> Path:
    if path_str:
        path = Path(path_str)
        return path if path.is_absolute() else (PROJECT_ROOT / path)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_multiplier = str(demand_multiplier).replace(".", "p")
    return PROJECT_ROOT / "results" / "release_startup_smoke" / (
        f"smoke_{stamp}_{snapshot_id}_m{months}_d{safe_multiplier}"
    )


def _append_log(log_path: Path, message: str) -> None:
    print(message, flush=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(message + "\n")


def _load_runtime_modules() -> Dict[str, Any]:
    config_loader = importlib.import_module("config.config_loader")
    demo = importlib.import_module("real_estate_demo_v2_1")
    runner_mod = importlib.import_module("simulation_runner")
    return {
        "SimulationConfig": config_loader.SimulationConfig,
        "SimulationRunner": runner_mod.SimulationRunner,
        "apply_scholar_release_config": demo.apply_scholar_release_config,
        "build_scaled_profile_pack_from_snapshot": demo.build_scaled_profile_pack_from_snapshot,
        "load_release_supply_snapshot_options": demo.load_release_supply_snapshot_options,
        "_choose_experiment_mode": demo._choose_experiment_mode,
        "_choose_profile_pack": demo._choose_profile_pack,
        "_derive_agent_count_from_supply": demo._derive_agent_count_from_supply,
        "_estimate_listing_rate": demo._estimate_listing_rate,
        "_scale_role_defaults": demo._scale_role_defaults,
    }


def _build_preplanned_interventions(args: argparse.Namespace) -> List[Dict[str, Any]]:
    plans: List[Dict[str, Any]] = []
    for spec in args.income_shock or []:
        plans.append(_parse_income_shock(spec))
    for spec in args.developer_supply or []:
        plans.append(_parse_developer_supply(spec))
    for spec in args.supply_cut or []:
        plans.append(_parse_supply_cut(spec))
    if plans:
        return plans
    if not bool(getattr(args, "default_interventions", True)):
        return []
    return _default_preplanned_interventions()


def _build_scholar_inputs(args: argparse.Namespace, runtime_modules: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    options = runtime_modules["load_release_supply_snapshot_options"]()
    by_id = {item["snapshot_id"]: item for item in options}
    if args.snapshot_id not in by_id:
        raise ValueError(
            f"Unknown snapshot_id={args.snapshot_id}. Available: {', '.join(sorted(by_id.keys()))}"
        )
    supply_snapshot = dict(by_id[args.snapshot_id])
    property_count = int(supply_snapshot.get("total_selected_supply", 0) or 0)
    requested_agent_count = runtime_modules["_derive_agent_count_from_supply"](
        property_count, args.demand_multiplier
    )
    scaled_profile_pack, demand_bucket_plan = runtime_modules["build_scaled_profile_pack_from_snapshot"](
        base_profile_pack_path=str(
            supply_snapshot.get("profile_pack_path")
            or runtime_modules["_choose_profile_pack"](args.market_goal)
        ),
        snapshot_payload=dict(supply_snapshot.get("snapshot_payload") or {}),
        target_agent_total=requested_agent_count,
    )
    effective_agent_count = int(
        demand_bucket_plan.get("effective_agent_count", requested_agent_count) or requested_agent_count
    )
    effective_demand_multiplier = float(effective_agent_count) / float(max(1, property_count))

    role_defaults = runtime_modules["_scale_role_defaults"](effective_agent_count, args.market_goal)
    buyer_quota = max(0, int(role_defaults.get("BUYER", 0) or 0))
    seller_quota = min(
        max(0, int(role_defaults.get("SELLER", 0) or 0)),
        max(0, effective_agent_count - buyer_quota),
    )
    buyer_seller_quota = min(
        max(0, int(role_defaults.get("BUYER_SELLER", 0) or 0)),
        max(0, effective_agent_count - buyer_quota - seller_quota),
    )
    listing_plan = runtime_modules["_estimate_listing_rate"](
        property_count=property_count,
        buyer_quota=buyer_quota,
        buyer_seller_quota=buyer_seller_quota,
        target_r_order_hint=float(args.target_r_order_hint),
    )
    preplanned_interventions = _build_preplanned_interventions(args)

    scholar_inputs = {
        "market_goal": args.market_goal,
        "months": int(args.months),
        "agent_count": int(effective_agent_count),
        "property_count": int(property_count),
        "demand_multiplier": float(args.demand_multiplier),
        "effective_demand_multiplier": float(effective_demand_multiplier),
        "supply_snapshot": supply_snapshot,
        "profile_pack_inline": scaled_profile_pack,
        "demand_bucket_plan": demand_bucket_plan,
        "buyer_quota": int(buyer_quota),
        "seller_quota": int(seller_quota),
        "buyer_seller_quota": int(buyer_seller_quota),
        "target_r_order_hint": float(args.target_r_order_hint),
        "income_multiplier": float(args.income_multiplier),
        "force_role_months": min(int(args.months), int(args.force_role_months)),
        "profiled_market_mode": True,
        "hard_bucket_matcher": True,
        "enable_intervention_panel": False,
        "open_startup_intervention_menu": False,
        "profile_pack_path": str(
            supply_snapshot.get("profile_pack_path")
            or runtime_modules["_choose_profile_pack"](args.market_goal)
        ),
        "experiment_mode": str(
            supply_snapshot.get("experiment_mode")
            or runtime_modules["_choose_experiment_mode"](args.market_goal)
        ),
        "listing_plan": listing_plan,
        "preplanned_interventions": preplanned_interventions,
        "seed": int(args.seed),
    }
    meta = {
        "requested_agent_count": int(requested_agent_count),
        "effective_agent_count": int(effective_agent_count),
        "effective_demand_multiplier": float(effective_demand_multiplier),
    }
    return scholar_inputs, meta


def _scan_runtime_log(log_path: Path) -> Dict[str, Any]:
    if not log_path.exists():
        return {"preplanned_lines": [], "checkpoint_lines": []}
    preplanned_lines: List[str] = []
    checkpoint_lines: List[str] = []
    with log_path.open("r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            if "Preplanned intervention executed:" in line:
                preplanned_lines.append(line)
            if "checkpoint saved:" in line:
                checkpoint_lines.append(line)
    return {
        "preplanned_lines": preplanned_lines,
        "checkpoint_lines": checkpoint_lines,
    }


def _collect_checkpoint_evidence(runtime_dir: Path, months: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for month in range(1, int(months) + 1):
        checkpoint_dir = runtime_dir / "monthly_checkpoints" / f"month_{month:02d}"
        rows.append(
            {
                "month": month,
                "checkpoint_dir": str(checkpoint_dir),
                "exists": checkpoint_dir.exists(),
                "db_exists": (checkpoint_dir / "simulation.db").exists(),
                "meta_exists": (checkpoint_dir / "checkpoint_meta.json").exists(),
                "status_exists": (checkpoint_dir / "status_snapshot.json").exists(),
            }
        )
    return rows


def _query_db_summary(db_path: Path) -> Dict[str, Any]:
    if not db_path.exists():
        return {"exists": False}
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*), COUNT(DISTINCT month), MIN(month), MAX(month) FROM decision_logs")
        decision_logs = cursor.fetchone() or (0, 0, None, None)
        cursor.execute(
            "SELECT month, COUNT(*) FROM decision_logs GROUP BY month ORDER BY month"
        )
        monthly_decision_counts = [
            {"month": int(month), "count": int(count)}
            for month, count in cursor.fetchall()
        ]
        cursor.execute(
            "SELECT month, COUNT(*) FROM active_participants GROUP BY month ORDER BY month"
        )
        monthly_active_rows = [
            {"month": int(month), "count": int(count)}
            for month, count in cursor.fetchall()
        ]
        return {
            "exists": True,
            "decision_log_count": int(decision_logs[0] or 0),
            "decision_log_month_count": int(decision_logs[1] or 0),
            "decision_log_min_month": decision_logs[2],
            "decision_log_max_month": decision_logs[3],
            "monthly_decision_counts": monthly_decision_counts,
            "monthly_active_rows": monthly_active_rows,
        }
    finally:
        conn.close()


def _write_smoke_report(project_dir: Path, payload: Dict[str, Any]) -> None:
    json_path = project_dir / "smoke_report.json"
    md_path = project_dir / "smoke_report.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    requested = payload.get("requested", {}) or {}
    runtime = payload.get("runtime", {}) or {}
    coverage = ((payload.get("scholar_inputs", {}) or {}).get("demand_bucket_plan", {}) or {}).get(
        "coverage_summary", {}
    ) or {}
    checkpoints = payload.get("checkpoints", []) or []
    preplanned_lines = ((payload.get("runtime_log_scan", {}) or {}).get("preplanned_lines", [])) or []
    lines = [
        "# Release Startup Smoke Report",
        "",
        "## Scenario",
        f"- Snapshot: `{requested.get('snapshot_id', '')}`",
        f"- Rounds: `{requested.get('months', '')}`",
        f"- Market goal: `{requested.get('market_goal', '')}`",
        f"- Requested demand multiplier: `{requested.get('demand_multiplier', '')}`",
        f"- Effective demand multiplier: `{runtime.get('effective_demand_multiplier', '')}`",
        f"- Requested agent count: `{runtime.get('requested_agent_count', '')}`",
        f"- Effective agent count: `{runtime.get('effective_agent_count', '')}`",
        "",
        "## Coverage",
        f"- Buyer buckets preserved: `{coverage.get('buyer_bucket_count_preserved', 0)}/{coverage.get('buyer_bucket_count', 0)}`",
        f"- Supply buckets covered: `{coverage.get('supply_bucket_count_covered', 0)}/{coverage.get('supply_bucket_count', 0)}`",
        f"- All supply buckets covered: `{coverage.get('all_supply_buckets_covered', False)}`",
        f"- Auto-clamped: `{((coverage.get('scale_meta', {}) or {}).get('was_clamped', False))}`",
        "",
        "## Run Outcome",
        f"- Status: `{runtime.get('status', '')}`",
        f"- Completed months: `{runtime.get('completed_months', 0)}`",
        f"- Duration seconds: `{runtime.get('duration_seconds', 0)}`",
        f"- Runtime DB: `{runtime.get('db_path', '')}`",
        f"- Runtime log: `{runtime.get('runtime_log_path', '')}`",
        "",
        "## Checkpoints",
    ]
    for item in checkpoints:
        lines.append(
            f"- Round {item['month']}: dir={item['exists']} db={item['db_exists']} meta={item['meta_exists']} status={item['status_exists']}"
        )
    lines.extend(
        [
            "",
            "## Preplanned Intervention Log Lines",
        ]
    )
    if preplanned_lines:
        for line in preplanned_lines:
            lines.append(f"- {line}")
    else:
        lines.append("- (none)")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a reproducible release-startup smoke scenario using the Scholar fixed-supply entry chain."
    )
    parser.add_argument("--snapshot-id", default="spindle_minimum")
    parser.add_argument("--market-goal", default="balanced", choices=["balanced", "buyer_market", "seller_market"])
    parser.add_argument("--months", type=int, default=3)
    parser.add_argument("--demand-multiplier", type=float, default=0.10)
    parser.add_argument("--target-r-order-hint", type=float, default=1.0)
    parser.add_argument("--income-multiplier", type=float, default=1.0)
    parser.add_argument("--force-role-months", type=int, default=3)
    parser.add_argument("--seed", type=int, default=606)
    parser.add_argument("--project-dir")
    parser.add_argument("--income-shock", action="append")
    parser.add_argument("--developer-supply", action="append")
    parser.add_argument("--supply-cut", action="append")
    parser.add_argument("--default-interventions", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--llm-mock", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--disable-end-reports", action=argparse.BooleanOptionalAction, default=True)
    return parser


def main() -> int:
    _setup_utf8_stdio()
    parser = _build_arg_parser()
    args = parser.parse_args()

    os.environ["PYTHONUTF8"] = "1"
    os.environ["PYTHONIOENCODING"] = "utf-8"
    if args.llm_mock:
        os.environ["LLM_MOCK_MODE"] = "true"
    else:
        os.environ["LLM_MOCK_MODE"] = "false"

    project_dir = _coerce_project_dir(
        args.project_dir,
        snapshot_id=args.snapshot_id,
        months=args.months,
        demand_multiplier=args.demand_multiplier,
    )
    runtime_dir = project_dir / "runtime_run"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    smoke_log_path = project_dir / "smoke_launcher.log"
    if smoke_log_path.exists():
        smoke_log_path.unlink()

    _append_log(smoke_log_path, "==== Release Startup Smoke ====")
    _append_log(smoke_log_path, f"时间: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    _append_log(smoke_log_path, f"ProjectDir: {project_dir}")
    _append_log(smoke_log_path, f"LLM_MOCK_MODE: {os.environ.get('LLM_MOCK_MODE', '')}")

    runtime_modules = _load_runtime_modules()
    scholar_inputs, demand_meta = _build_scholar_inputs(args, runtime_modules)

    config_path = project_dir / "config.yaml"
    shutil.copy2(PROJECT_ROOT / "config" / "baseline.yaml", config_path)
    config = runtime_modules["SimulationConfig"](str(config_path))
    if args.disable_end_reports:
        config.update("reporting.enable_end_reports", False)
    runtime_modules["apply_scholar_release_config"](config, scholar_inputs, start_month=1)
    config.save(str(config_path))

    request_payload = {
        "snapshot_id": args.snapshot_id,
        "market_goal": args.market_goal,
        "months": int(args.months),
        "seed": int(args.seed),
        "demand_multiplier": float(args.demand_multiplier),
        "target_r_order_hint": float(args.target_r_order_hint),
        "income_multiplier": float(args.income_multiplier),
        "preplanned_interventions": scholar_inputs.get("preplanned_interventions", []),
    }
    (project_dir / "smoke_request.json").write_text(
        json.dumps(request_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    _append_log(
        smoke_log_path,
        (
            f"场景: {args.snapshot_id} / {args.market_goal} / {args.months}回合 / "
            f"请求倍率 {args.demand_multiplier:.2f}x / 有效倍率 "
            f"{float(scholar_inputs['effective_demand_multiplier']):.2f}x"
        ),
    )
    coverage_summary = (scholar_inputs.get("demand_bucket_plan", {}) or {}).get("coverage_summary", {}) or {}
    _append_log(
        smoke_log_path,
        (
            "覆盖保障: "
            f"买家桶 {int(coverage_summary.get('buyer_bucket_count_preserved', 0))}/"
            f"{int(coverage_summary.get('buyer_bucket_count', 0))} 保留, "
            f"供应桶 {int(coverage_summary.get('supply_bucket_count_covered', 0))}/"
            f"{int(coverage_summary.get('supply_bucket_count', 0))} 覆盖"
        ),
    )

    runner = runtime_modules["SimulationRunner"](
        agent_count=int(scholar_inputs["agent_count"]),
        months=int(args.months),
        seed=int(args.seed),
        resume=False,
        config=config,
        db_path=str(runtime_dir / "simulation.db"),
    )

    def _progress(payload: Dict[str, Any]) -> None:
        stage = str(payload.get("stage", "") or "")
        month = int(payload.get("month", 0) or 0)
        message = str(payload.get("message", "") or "")
        _append_log(smoke_log_path, f"[progress][m{month:02d}][{stage}] {message}")

    runner.set_progress_callback(_progress)

    started_at = time.monotonic()
    runner.run(allow_intervention_panel=False)
    duration_seconds = round(time.monotonic() - started_at, 2)

    runtime_log_path = runtime_dir / "simulation_run.log"
    report_payload = {
        "requested": request_payload,
        "scholar_inputs": scholar_inputs,
        "runtime": {
            "status": runner.status,
            "completed_months": int(runner.current_month),
            "duration_seconds": duration_seconds,
            "requested_agent_count": demand_meta["requested_agent_count"],
            "effective_agent_count": demand_meta["effective_agent_count"],
            "effective_demand_multiplier": demand_meta["effective_demand_multiplier"],
            "db_path": str(runtime_dir / "simulation.db"),
            "runtime_log_path": str(runtime_log_path),
            "intervention_history": list(runner.intervention_history),
            "final_summary": runner.final_summary,
            "last_error": runner.last_error,
        },
        "checkpoints": _collect_checkpoint_evidence(runtime_dir, args.months),
        "runtime_log_scan": _scan_runtime_log(runtime_log_path),
        "db_summary": _query_db_summary(runtime_dir / "simulation.db"),
        "artifacts": {
            "project_dir": str(project_dir),
            "config_path": str(config_path),
            "request_path": str(project_dir / "smoke_request.json"),
            "smoke_log_path": str(smoke_log_path),
        },
    }
    report_payload["verdict"] = {
        "run_completed": bool(runner.status == "completed" and int(runner.current_month) == int(args.months)),
        "all_checkpoints_present": all(item["db_exists"] and item["meta_exists"] and item["status_exists"] for item in report_payload["checkpoints"]),
        "all_supply_buckets_covered": bool(
            (((scholar_inputs.get("demand_bucket_plan", {}) or {}).get("coverage_summary", {}) or {}).get(
                "all_supply_buckets_covered", False
            ))
        ),
        "preplanned_interventions_logged": len((report_payload.get("runtime_log_scan", {}) or {}).get("preplanned_lines", []))
        >= len(scholar_inputs.get("preplanned_interventions", []) or []),
    }
    _write_smoke_report(project_dir, report_payload)

    verdict = report_payload["verdict"]
    _append_log(smoke_log_path, f"运行状态: {runner.status} / 已完成回合 {runner.current_month}/{args.months}")
    _append_log(
        smoke_log_path,
        (
            f"Verdict: completed={verdict['run_completed']} "
            f"checkpoints={verdict['all_checkpoints_present']} "
            f"supply_coverage={verdict['all_supply_buckets_covered']} "
            f"intervention_logs={verdict['preplanned_interventions_logged']}"
        ),
    )
    _append_log(smoke_log_path, f"Smoke report: {project_dir / 'smoke_report.md'}")
    return 0 if all(verdict.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
