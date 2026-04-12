#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
批次 run 归档与清理工具

规则：
1) 通过门槛 -> 复制到 results/pass_runs/... 并在目录名附加 _pass
2) 不通过门槛 -> 删除 results/run_* 原始目录

说明：
- 本脚本只处理 batch_summary.json 里能定位到的 run。
- 默认不删除 case 目录（market_state_matrix 下的 V*_s*_m*），只处理 run_*。
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
PASS_ROOT = RESULTS / "pass_runs"


@dataclass
class EvalResult:
    group: str
    seed: int
    run_dir: Path
    case_dir: Path
    db_path: Path
    status: str
    reason: str
    metrics: Dict[str, Any]


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve(path_like: str) -> Path:
    p = Path(path_like)
    if p.is_absolute():
        return p
    return (ROOT / p).resolve()


def _safe_count_err(stderr_path: Path, token: str) -> int:
    if not stderr_path.exists():
        return 0
    txt = stderr_path.read_text(encoding="utf-8", errors="ignore")
    return txt.count(token)


def _metrics(db_path: Path) -> Dict[str, Any]:
    if not db_path.exists():
        return {
            "l0": 0,
            "b0_order": 0,
            "r_order": 0.0,
            "matches_m1": 0,
            "orders_m1": 0,
            "tx_m1": 0,
        }
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        l0 = int(cur.execute("SELECT COUNT(*) FROM properties_market WHERE listing_month=0 AND status='for_sale'").fetchone()[0] or 0)
        b0_order = int(cur.execute("SELECT COUNT(DISTINCT buyer_id) FROM transaction_orders WHERE created_month=1").fetchone()[0] or 0)
        matches_m1 = int(cur.execute("SELECT COUNT(*) FROM property_buyer_matches WHERE month=1").fetchone()[0] or 0)
        orders_m1 = int(cur.execute("SELECT COUNT(*) FROM transaction_orders WHERE created_month=1").fetchone()[0] or 0)
        tx_m1 = int(cur.execute("SELECT COUNT(*) FROM transactions WHERE month=1").fetchone()[0] or 0)
    finally:
        conn.close()
    r_order = (float(b0_order) / float(l0)) if l0 > 0 else 0.0
    return {
        "l0": l0,
        "b0_order": b0_order,
        "r_order": round(r_order, 4),
        "matches_m1": matches_m1,
        "orders_m1": orders_m1,
        "tx_m1": tx_m1,
    }


def _is_group_pass(group: str, m: Dict[str, Any]) -> tuple[bool, str]:
    g = group.upper()
    # 有效样本基本门槛
    if m["matches_m1"] <= 0 or m["orders_m1"] <= 0:
        return False, "chain_not_effective_matches_or_orders_zero"
    if g.startswith("V2"):
        if m["r_order"] >= 1.0:
            return False, "v2_boundary_not_less_than_1"
        return True, "v2_pass"
    if g.startswith("V3"):
        if m["r_order"] <= 1.0:
            return False, "v3_boundary_not_greater_than_1"
        return True, "v3_pass"
    if g.startswith("V1"):
        # V1 仅作为对照锚点，要求链路有效即可
        return True, "v1_anchor_pass"
    # 其他组别默认按有效样本保留
    return True, "generic_pass"


def _evaluate_run(run: Dict[str, Any]) -> EvalResult:
    group = str(run.get("group_key") or run.get("group") or "")
    seed = int(run.get("seed") or 0)
    case_dir = _resolve(str(run.get("run_dir") or ""))
    db_path = _resolve(str(run.get("db_path") or ""))
    stderr = _resolve(str(run.get("stderr") or ""))
    run_dir = db_path.parent

    m = _metrics(db_path)
    conn_err = _safe_count_err(stderr, "Connection error")
    circuit_err = _safe_count_err(stderr, "circuit_open")
    breaker_err = _safe_count_err(stderr, "Async LLM breaker open")
    m["stderr_connection_error"] = conn_err
    m["stderr_circuit_open"] = circuit_err
    m["stderr_breaker_open"] = breaker_err
    m["stderr_error_total"] = conn_err + circuit_err + breaker_err

    if m["stderr_error_total"] > 50:
        return EvalResult(
            group=group,
            seed=seed,
            run_dir=run_dir,
            case_dir=case_dir,
            db_path=db_path,
            status="fail",
            reason="stderr_error_total_over_50",
            metrics=m,
        )

    ok, reason = _is_group_pass(group, m)
    return EvalResult(
        group=group,
        seed=seed,
        run_dir=run_dir,
        case_dir=case_dir,
        db_path=db_path,
        status="pass" if ok else "fail",
        reason=reason,
        metrics=m,
    )


def _copy_pass_bundle(batch_dir: Path, ev: EvalResult) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_name = batch_dir.name
    target = PASS_ROOT / batch_name / f"{ev.group}_s{ev.seed}_pass_{stamp}"
    target.mkdir(parents=True, exist_ok=True)

    if ev.case_dir.exists():
        case_target = target / "case_artifacts"
        shutil.copytree(ev.case_dir, case_target, dirs_exist_ok=True)
    if ev.run_dir.exists():
        run_target = target / "run_artifacts"
        shutil.copytree(ev.run_dir, run_target, dirs_exist_ok=True)

    report = {
        "group": ev.group,
        "seed": ev.seed,
        "status": ev.status,
        "reason": ev.reason,
        "metrics": ev.metrics,
        "source_case_dir": str(ev.case_dir),
        "source_run_dir": str(ev.run_dir),
    }
    (target / "pass_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def _delete_run_dir(run_dir: Path) -> bool:
    if not run_dir.exists():
        return False
    resolved = run_dir.resolve()
    legacy_prefix = str((RESULTS / "run_").resolve())
    new_prefix = str((RESULTS / "runs" / "run_").resolve())
    resolved_str = str(resolved)
    if not (resolved_str.startswith(legacy_prefix) or resolved_str.startswith(new_prefix)):
        return False
    shutil.rmtree(resolved, ignore_errors=True)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Promote pass runs and delete failed run directories.")
    parser.add_argument("batch_dir", help="Directory containing batch_summary.json")
    parser.add_argument("--delete-fail", action="store_true", help="Delete failed results run_* directories.")
    parser.add_argument("--promote-pass", action="store_true", help="Copy pass bundles into results/pass_runs.")
    args = parser.parse_args()

    batch_dir = _resolve(args.batch_dir)
    summary_path = batch_dir / "batch_summary.json"
    if not summary_path.exists():
        alt = batch_dir / "calibration_summary.json"
        alt2 = batch_dir / "tuning_summary.json"
        if alt.exists():
            summary_path = alt
        elif alt2.exists():
            summary_path = alt2
        else:
            raise FileNotFoundError(f"batch_summary.json not found: {summary_path}")

    summary = _load_json(summary_path)
    runs: List[Dict[str, Any]]
    if "runs" in summary:
        runs = summary.get("runs", [])
        evals = [_evaluate_run(r) for r in runs]
    elif "rows" in summary:
        # tuning_summary.json compatible shape
        evals = []
        for row in summary.get("rows", []):
            db_path = _resolve(str(row.get("db_path") or ""))
            run_dir = db_path.parent
            case_dir = _resolve(str(row.get("run_dir") or ""))
            group = str(row.get("group") or "")
            seed = int(row.get("seed") or 0)
            m = {
                "l0": int(row.get("l0") or 0),
                "b0_order": int(row.get("b0_order") or 0),
                "r_order": float(row.get("r_order") or 0.0),
                "matches_m1": int(row.get("matches_m1") or 0),
                "orders_m1": int(row.get("orders_m1") or 0),
                "tx_m1": int(row.get("tx_m1") or 0),
                "stderr_connection_error": 0,
                "stderr_circuit_open": 0,
                "stderr_breaker_open": 0,
                "stderr_error_total": 0,
            }
            ok, reason = _is_group_pass(group, m)
            evals.append(
                EvalResult(
                    group=group,
                    seed=seed,
                    run_dir=run_dir,
                    case_dir=case_dir,
                    db_path=db_path,
                    status="pass" if ok else "fail",
                    reason=reason,
                    metrics=m,
                )
            )
    else:
        raise ValueError(f"Unsupported summary shape: {summary_path}")

    actions = []
    for ev in evals:
        row = {
            "group": ev.group,
            "seed": ev.seed,
            "status": ev.status,
            "reason": ev.reason,
            "run_dir": str(ev.run_dir),
            "case_dir": str(ev.case_dir),
            "metrics": ev.metrics,
        }
        if ev.status == "pass" and args.promote_pass:
            target = _copy_pass_bundle(batch_dir, ev)
            row["promoted_to"] = str(target)
        if ev.status == "fail" and args.delete_fail:
            row["deleted"] = bool(_delete_run_dir(ev.run_dir))
        actions.append(row)

    out = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "batch_dir": str(batch_dir),
        "promote_pass": bool(args.promote_pass),
        "delete_fail": bool(args.delete_fail),
        "actions": actions,
    }
    out_path = batch_dir / "pass_promotion_report.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"report={out_path}")
    pass_count = sum(1 for a in actions if a["status"] == "pass")
    fail_count = sum(1 for a in actions if a["status"] == "fail")
    print(f"pass={pass_count} fail={fail_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
