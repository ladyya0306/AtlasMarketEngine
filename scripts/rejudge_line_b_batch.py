#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
课题线B批次重判脚本（不重跑仿真）

用途：
1. 读取已有 batch_summary.json。
2. 按 shock_type / gate_profile 进行新闸门重判。
3. 输出 batch_summary_v2.json / batch_summary_v2.md。
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Tuple


def _competition_hot(metrics: Dict[str, Any]) -> bool:
    avg_buyers = float(metrics.get("avg_buyers_per_ordered_property_m1", 0.0) or 0.0)
    crowded_ratio = float(metrics.get("crowded_property_ratio_m1", 0.0) or 0.0)
    return bool(avg_buyers >= 2.0 or crowded_ratio >= 0.5)


def _seller_score(metrics: Dict[str, Any]) -> int:
    checks = (
        float(metrics.get("r_order", 0.0) or 0.0) >= 0.5,
        float(metrics.get("order_density_m1", 0.0) or 0.0) >= 0.8,
        _competition_hot(metrics),
    )
    return int(sum(1 for x in checks if bool(x)))


def _buyer_score(metrics: Dict[str, Any]) -> int:
    avg_buyers = float(metrics.get("avg_buyers_per_ordered_property_m1", 0.0) or 0.0)
    crowded_ratio = float(metrics.get("crowded_property_ratio_m1", 0.0) or 0.0)
    checks = (
        float(metrics.get("r_order", 0.0) or 0.0) < 1.0,
        float(metrics.get("order_density_m1", 0.0) or 0.0) < 0.8,
        (avg_buyers < 3.5 and crowded_ratio < 0.75),
    )
    return int(sum(1 for x in checks if bool(x)))


def _base_boundary_gate(group_id: str, metrics: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], str]:
    r_order = float(metrics.get("r_order", 0.0) or 0.0)
    order_density = float(metrics.get("order_density_m1", 0.0) or 0.0)
    avg_buyers = float(metrics.get("avg_buyers_per_ordered_property_m1", 0.0) or 0.0)
    crowded_ratio = float(metrics.get("crowded_property_ratio_m1", 0.0) or 0.0)

    if group_id == "V2":
        checks = {
            "r_order_lt_1": bool(r_order < 1.0),
            "order_density_lt_0_8": bool(order_density < 0.8),
            "competition_not_overheated": bool(avg_buyers < 3.5 and crowded_ratio < 0.75),
        }
        score = int(sum(1 for v in checks.values() if bool(v)))
        return bool(score >= 2), {"method": "multi_signal_buyer_soft", "score": score, "checks": checks}, "multi_signal_buyer_soft"

    if group_id == "V3":
        checks = {
            "r_order_ge_0_5": bool(r_order >= 0.5),
            "order_density_ge_0_8": bool(order_density >= 0.8),
            "competition_hot": bool(avg_buyers >= 2.0 or crowded_ratio >= 0.5),
        }
        score = int(sum(1 for v in checks.values() if bool(v)))
        return bool(score >= 2), {"method": "multi_signal_seller_soft", "score": score, "checks": checks}, "multi_signal_seller_soft"

    return True, {"method": "anchor_only"}, "anchor_only"


def _effective_stability_forced(gate: Dict[str, Any]) -> Tuple[bool, bool, bool, bool]:
    return (
        bool(gate.get("effective_chain_pass", False)),
        bool(gate.get("stability_pass", False)),
        bool(gate.get("forced_mode_connected", False)),
        bool(gate.get("forced_mode_pure", False)),
    )


def _infer_shock_type(batch_dir: Path, explicit: str) -> str:
    if explicit:
        return explicit
    name = batch_dir.name.lower()
    if "inject" in name and "supply" in name:
        return "expand_supply"
    if "recover" in name and "supply" in name:
        return "contract_supply"
    if "income" in name:
        return "income"
    return "none"


def _directional_gate_expand_supply(
    *,
    group_id: str,
    metrics: Dict[str, Any],
    baseline_metrics: Dict[str, Any] | None,
) -> Tuple[bool, Dict[str, Any]]:
    if baseline_metrics is None:
        return False, {"method": "directional_expand_supply", "error": "missing_baseline"}

    r_order = float(metrics.get("r_order", 0.0) or 0.0)
    r_order_base = float(baseline_metrics.get("r_order", 0.0) or 0.0)
    order_density = float(metrics.get("order_density_m1", 0.0) or 0.0)
    order_density_base = float(baseline_metrics.get("order_density_m1", 0.0) or 0.0)
    seller_score = _seller_score(metrics)
    seller_score_base = _seller_score(baseline_metrics)
    buyer_score = _buyer_score(metrics)
    buyer_score_base = _buyer_score(baseline_metrics)

    if group_id == "V3":
        checks = {
            "r_order_weakened_vs_baseline": bool(r_order <= r_order_base),
            "order_density_weakened_vs_baseline": bool(order_density <= order_density_base),
            "seller_signal_not_stronger_vs_baseline": bool(seller_score <= seller_score_base),
        }
    elif group_id == "V2":
        # 对 V2 不再强依赖 baseline 相对值（避免 L0 分母变化造成误伤），
        # 而是检查“当前是否保持买方状态”。
        seller_score_now = _seller_score(metrics)
        checks = {
            "buyer_signal_present_now": bool(buyer_score >= 2),
            "seller_signal_not_formed_now": bool(seller_score_now < 2),
            "r_order_lt_0_8_now": bool(r_order < 0.8),
        }
    else:
        return True, {"method": "directional_expand_supply_anchor", "score": 1, "checks": {"anchor": True}}

    score = int(sum(1 for v in checks.values() if bool(v)))
    return bool(score >= 2), {
        "method": "directional_expand_supply",
        "score": score,
        "checks": checks,
        "baseline_ref": {
            "r_order": round(r_order_base, 4),
            "order_density_m1": round(order_density_base, 4),
            "seller_score": int(seller_score_base),
            "buyer_score": int(buyer_score_base),
        },
    }


def _directional_gate_income(
    *,
    group_id: str,
    metrics: Dict[str, Any],
    baseline_metrics: Dict[str, Any] | None,
) -> Tuple[bool, Dict[str, Any]]:
    if baseline_metrics is None:
        return False, {"method": "directional_income", "error": "missing_baseline"}

    order_density = float(metrics.get("order_density_m1", 0.0) or 0.0)
    order_density_base = float(baseline_metrics.get("order_density_m1", 0.0) or 0.0)
    seller_score = _seller_score(metrics)
    seller_score_base = _seller_score(baseline_metrics)
    buyer_score = _buyer_score(metrics)
    buyer_score_base = _buyer_score(baseline_metrics)

    if group_id == "V2":
        checks = {
            "buyer_signal_not_stronger_vs_baseline": bool(buyer_score <= buyer_score_base),
            "seller_signal_not_weaker_vs_baseline": bool(seller_score >= seller_score_base),
            "order_density_not_lower_vs_baseline": bool(order_density >= order_density_base),
        }
    elif group_id == "V3":
        checks = {
            "seller_signal_not_weaker_vs_baseline": bool(seller_score >= seller_score_base),
            "buyer_signal_not_stronger_vs_baseline": bool(buyer_score <= buyer_score_base),
            "order_density_not_lower_vs_baseline": bool(order_density >= order_density_base),
        }
    else:
        return True, {"method": "directional_income_anchor", "score": 1, "checks": {"anchor": True}}

    score = int(sum(1 for v in checks.values() if bool(v)))
    return bool(score >= 2), {
        "method": "directional_income",
        "score": score,
        "checks": checks,
        "baseline_ref": {
            "order_density_m1": round(order_density_base, 4),
            "seller_score": int(seller_score_base),
            "buyer_score": int(buyer_score_base),
        },
    }


def _build_group_summary_v2(runs: list[Dict[str, Any]]) -> Dict[str, Any]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for run in runs:
        group_id = str(run["group"])
        info = grouped.setdefault(
            group_id,
            {
                "group": group_id,
                "total_runs": 0,
                "pass_runs": 0,
                "failed_runs": 0,
                "pass_seeds": [],
                "fail_seeds": [],
                "long_test_ready": True,
            },
        )
        info["total_runs"] += 1
        if bool(run["gate_v2"]["overall_pass"]):
            info["pass_runs"] += 1
            info["pass_seeds"].append(int(run["seed"]))
        else:
            info["failed_runs"] += 1
            info["fail_seeds"].append(int(run["seed"]))
            info["long_test_ready"] = False
    return grouped


def main() -> int:
    parser = argparse.ArgumentParser(description="Re-judge existing line-B batch without rerun.")
    parser.add_argument("batch_dir", help="Directory containing batch_summary.json")
    parser.add_argument("--baseline-batch-dir", default="", help="Baseline batch for directional comparison")
    parser.add_argument(
        "--shock-type",
        default="",
        choices=["", "none", "expand_supply", "contract_supply", "income"],
        help="Shock type; empty means infer from batch dir name",
    )
    parser.add_argument(
        "--gate-profile",
        default="auto",
        choices=["auto", "boundary", "directional_expand_supply", "directional_income"],
        help="Gate profile selector",
    )
    args = parser.parse_args()

    batch_dir = Path(args.batch_dir).resolve()
    summary_path = batch_dir / "batch_summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"batch_summary.json not found: {summary_path}")
    payload = json.loads(summary_path.read_text(encoding="utf-8"))

    baseline_map: Dict[Tuple[str, int], Dict[str, Any]] = {}
    if args.baseline_batch_dir:
        base_summary = Path(args.baseline_batch_dir).resolve() / "batch_summary.json"
        if not base_summary.exists():
            raise FileNotFoundError(f"baseline batch_summary.json not found: {base_summary}")
        base_payload = json.loads(base_summary.read_text(encoding="utf-8"))
        for run in base_payload.get("runs", []):
            baseline_map[(str(run.get("group", "")), int(run.get("seed", 0) or 0))] = dict(
                run.get("metrics", {}) or {}
            )

    shock_type = _infer_shock_type(batch_dir=batch_dir, explicit=str(args.shock_type or ""))
    if args.gate_profile == "auto":
        if shock_type == "expand_supply":
            gate_profile = "directional_expand_supply"
        elif shock_type == "income":
            gate_profile = "directional_income"
        else:
            gate_profile = "boundary"
    else:
        gate_profile = str(args.gate_profile)

    runs_v2 = []
    for run in payload.get("runs", []):
        group_id = str(run.get("group", "") or "")
        seed = int(run.get("seed", 0) or 0)
        metrics = dict(run.get("metrics", {}) or {})
        gate = dict(run.get("gate", {}) or {})
        effective_chain_pass, stability_pass, forced_mode_connected, forced_mode_pure = _effective_stability_forced(gate)

        boundary_pass, boundary_details, boundary_rule = _base_boundary_gate(group_id=group_id, metrics=metrics)

        directional_pass = True
        directional_details: Dict[str, Any] = {"method": "not_applicable"}
        if gate_profile == "directional_expand_supply":
            baseline_metrics = baseline_map.get((group_id, seed))
            directional_pass, directional_details = _directional_gate_expand_supply(
                group_id=group_id,
                metrics=metrics,
                baseline_metrics=baseline_metrics,
            )
        elif gate_profile == "directional_income":
            baseline_metrics = baseline_map.get((group_id, seed))
            directional_pass, directional_details = _directional_gate_income(
                group_id=group_id,
                metrics=metrics,
                baseline_metrics=baseline_metrics,
            )

        if gate_profile in {"directional_expand_supply", "directional_income"}:
            profile_pass = bool(directional_pass)
        else:
            profile_pass = bool(boundary_pass)

        overall_pass = bool(
            effective_chain_pass and stability_pass and forced_mode_connected and forced_mode_pure and profile_pass
        )
        gate_v2 = {
            "effective_chain_pass": effective_chain_pass,
            "stability_pass": stability_pass,
            "forced_mode_connected": forced_mode_connected,
            "forced_mode_pure": forced_mode_pure,
            "gate_profile": gate_profile,
            "shock_type": shock_type,
            "boundary_rule": boundary_rule,
            "boundary_details": boundary_details,
            "boundary_pass": bool(boundary_pass),
            "directional_details": directional_details,
            "directional_pass": bool(directional_pass),
            "profile_pass": bool(profile_pass),
            "overall_pass": bool(overall_pass),
        }
        run_copy = dict(run)
        run_copy["gate_v2"] = gate_v2
        runs_v2.append(run_copy)

    payload_v2 = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_batch_summary": str(summary_path),
        "batch_type": str(payload.get("batch_type", "line_b_forced_role")),
        "months": int(payload.get("months", 0) or 0),
        "seeds": payload.get("seeds", []),
        "shock_type": shock_type,
        "gate_profile": gate_profile,
        "baseline_batch_dir": str(Path(args.baseline_batch_dir).resolve()) if args.baseline_batch_dir else "",
        "run_count": len(runs_v2),
        "pass_gate_count_v2": sum(1 for r in runs_v2 if bool(r.get("gate_v2", {}).get("overall_pass", False))),
        "fail_gate_count_v2": sum(1 for r in runs_v2 if not bool(r.get("gate_v2", {}).get("overall_pass", False))),
        "group_summary_v2": _build_group_summary_v2(runs_v2),
        "runs": runs_v2,
    }

    out_json = batch_dir / "batch_summary_v2.json"
    out_md = batch_dir / "batch_summary_v2.md"
    out_json.write_text(json.dumps(payload_v2, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# 课题线B 批次重判摘要（v2）",
        "",
        f"- 生成时间: {payload_v2['generated_at']}",
        f"- shock_type: {shock_type}",
        f"- gate_profile: {gate_profile}",
        f"- baseline_batch_dir: {payload_v2['baseline_batch_dir'] or '-'}",
        "",
        "| 组别 | seed | Gate_v1 | Gate_v2 | boundary_pass | directional_pass | profile_pass |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for run in runs_v2:
        gate_old = dict(run.get("gate", {}) or {})
        gate_new = dict(run.get("gate_v2", {}) or {})
        lines.append(
            f"| {run.get('group')} | {run.get('seed')} | "
            f"{'PASS' if bool(gate_old.get('overall_pass', False)) else 'FAIL'} | "
            f"{'PASS' if bool(gate_new.get('overall_pass', False)) else 'FAIL'} | "
            f"{bool(gate_new.get('boundary_pass', False))} | "
            f"{bool(gate_new.get('directional_pass', False))} | "
            f"{bool(gate_new.get('profile_pass', False))} |"
        )
    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"batch_summary_v2={out_json}")
    print(f"batch_summary_v2_md={out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
