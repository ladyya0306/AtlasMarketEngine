#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
夜跑门禁编排器

功能：
1. 按计划顺序连续运行多批测试；
2. 每批结束后自动读取 zone_chain_summary.json 做门禁判定；
3. 通过则继续下一批；失败则写明原因并暂停后续批次。
"""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts" / "run_research_experiments.py"


@dataclass
class Stage:
    key: str
    title: str
    groups: List[str]
    seeds: List[int]
    months: int
    use_snapshot: bool
    require_lag_check: bool = False
    require_raise_signal: bool = True
    min_raise_count: int = 1


def _mean(values: List[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values)) / float(len(values))


def _stage_run_dir(base_dir: Path, idx: int, stage_key: str) -> Path:
    return base_dir / f"{idx:02d}_{stage_key}"


def _load_zone_json(batch_dir: Path) -> Dict[str, Any]:
    p = batch_dir / "zone_chain_summary.json"
    if not p.exists():
        raise FileNotFoundError(f"缺少分析结果: {p}")
    return json.loads(p.read_text(encoding="utf-8"))


def _parse_raise_count(run_item: Dict[str, Any]) -> int:
    actions = run_item.get("style_price_adjust_actions", {}) or {}
    total = 0
    for _, amap in actions.items():
        if not isinstance(amap, dict):
            continue
        total += int(amap.get("E", 0) or 0)
        total += int(amap.get("F", 0) or 0)
    return total


def evaluate_stage(stage: Stage, zone_payload: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    runs = zone_payload.get("runs", []) or []
    failures: List[str] = []
    warnings: List[str] = []

    if not runs:
        failures.append("批次无 run 结果。")
        return False, {"failures": failures, "warnings": warnings}

    tx_values: List[int] = []
    pending_values: List[int] = []
    b_tx_values: List[int] = []
    b_order_to_tx_values: List[float] = []
    b_pending_values: List[int] = []
    b_order_to_effective_close_values: List[float] = []
    raise_count_total = 0

    lag_a_rows: List[int] = []
    lag_b_rows: List[int] = []
    lag_a_avg: List[float] = []
    lag_b_avg: List[float] = []

    for r in runs:
        status = str(r.get("status", "")).lower()
        if status != "success":
            failures.append(f"{r.get('group_name')} / seed {r.get('seed')} 状态异常: {status}")

        funnel = r.get("funnel", {}) or {}
        tx = int(funnel.get("transactions", 0) or 0)
        pending = int(funnel.get("pending_settlement_orders", 0) or 0)
        tx_values.append(tx)
        pending_values.append(pending)

        zloss = r.get("zone_loss_metrics", {}) or {}
        b = zloss.get("B", {}) or {}
        b_tx = int(b.get("transactions", 0) or 0)
        b_pending = int(b.get("pending_settlement_orders", 0) or 0)
        b_rate = float(b.get("order_to_tx_rate", 0.0) or 0.0)
        b_effective_rate = float(b.get("order_to_effective_close_rate", b_rate) or b_rate)
        b_tx_values.append(b_tx)
        b_pending_values.append(b_pending)
        b_order_to_tx_values.append(b_rate)
        b_order_to_effective_close_values.append(b_effective_rate)

        raise_count_total += _parse_raise_count(r)

        gkey = str(r.get("group_key", "")).upper()
        if gkey == "R2A":
            lag_a_rows.append(int(r.get("lagged_exposure_rows", 0) or 0))
            lag_a_avg.append(float(r.get("avg_applied_lag_months", 0.0) or 0.0))
        elif gkey == "R2B":
            lag_b_rows.append(int(r.get("lagged_exposure_rows", 0) or 0))
            lag_b_avg.append(float(r.get("avg_applied_lag_months", 0.0) or 0.0))

    is_short_window = int(stage.months) <= 2
    total_tx = int(sum(tx_values))
    total_pending = int(sum(pending_values))
    b_total_tx = int(sum(b_tx_values))
    b_total_pending = int(sum(b_pending_values))
    b_tx_rate_avg = _mean(b_order_to_tx_values)
    b_effective_rate_avg = _mean(b_order_to_effective_close_values)
    primary_close_metric = "order_to_effective_close_rate" if is_short_window else "order_to_tx_rate"
    primary_close_rate_avg = b_effective_rate_avg if is_short_window else b_tx_rate_avg

    if total_tx <= 0:
        if is_short_window and total_pending > 0:
            warnings.append(
                f"本批总成交=0，但存在待结算订单 {total_pending}（短窗统计口径提示，不拦截）。"
            )
        else:
            failures.append("本批总成交=0，未通过。")
    if b_total_tx <= 0:
        if is_short_window and b_total_pending > 0:
            warnings.append(
                f"本批 B 区成交=0，但存在 B区待结算订单 {b_total_pending}（短窗统计口径提示，不拦截）。"
            )
        else:
            failures.append("本批 B 区成交=0，未通过。")
    if primary_close_rate_avg < 0.03:
        failures.append(
            f"本批 B区 {primary_close_metric} 平均值 < 0.03，后段转化过弱。"
        )

    if stage.require_lag_check:
        if sum(lag_b_rows) <= 0:
            failures.append("R2B 滞后暴露记录=0，时滞机制未生效。")
        if _mean(lag_b_avg) <= _mean(lag_a_avg):
            failures.append(
                "R2B 平均滞后月数未高于 R2A，时滞强度未拉开。"
            )

    if stage.require_raise_signal and int(raise_count_total) < int(stage.min_raise_count):
        failures.append(
            f"本批提价动作(E+F)={int(raise_count_total)}，低于门槛 {int(stage.min_raise_count)}。"
        )
    elif raise_count_total <= 0:
        warnings.append("本批未观察到提价动作(E+F=0)。")

    metrics = {
        "is_short_window": bool(is_short_window),
        "total_transactions": int(total_tx),
        "total_pending_settlement_orders": int(total_pending),
        "b_zone_transactions": int(b_total_tx),
        "b_zone_pending_settlement_orders": int(b_total_pending),
        "b_zone_order_to_tx_rate_avg": round(b_tx_rate_avg, 4),
        "b_zone_order_to_effective_close_rate_avg": round(b_effective_rate_avg, 4),
        "b_zone_primary_close_metric": primary_close_metric,
        "b_zone_primary_close_rate_avg": round(primary_close_rate_avg, 4),
        "raise_count_total_e_plus_f": int(raise_count_total),
        "r2a_lag_rows_total": int(sum(lag_a_rows)),
        "r2b_lag_rows_total": int(sum(lag_b_rows)),
        "r2a_avg_lag_months": round(_mean(lag_a_avg), 4),
        "r2b_avg_lag_months": round(_mean(lag_b_avg), 4),
    }
    passed = len(failures) == 0
    return passed, {"failures": failures, "warnings": warnings, "metrics": metrics}


def run_stage(stage: Stage, out_dir: Path) -> int:
    cmd = [
        sys.executable,
        str(RUNNER),
        "--groups",
        *stage.groups,
        "--seeds",
        *[str(s) for s in stage.seeds],
        "--months-override",
        str(stage.months),
        "--out-dir",
        str(out_dir),
    ]
    if stage.use_snapshot:
        cmd.append("--use-init-snapshot")
    cmd.append("--fail-on-quality-gate")
    proc = subprocess.run(cmd, cwd=str(ROOT))
    return int(proc.returncode)


def main() -> int:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_dir = ROOT / "results" / "night_plan" / f"night_gate_{stamp}"
    base_dir.mkdir(parents=True, exist_ok=True)

    stages: List[Stage] = [
        Stage(
            key="gate_snap_r2ab_s101202",
            title="门禁批-快照（R2A/R2B, seeds 101/202, 2个月）",
            groups=["R2A", "R2B"],
            seeds=[101, 202],
            months=2,
            use_snapshot=True,
            require_lag_check=True,
            require_raise_signal=True,
            min_raise_count=1,
        ),
        Stage(
            key="verify_fresh_r2ab_s303",
            title="复核批-Fresh（R2A/R2B, seed 303, 2个月）",
            groups=["R2A", "R2B"],
            seeds=[303],
            months=2,
            use_snapshot=False,
            require_lag_check=True,
            require_raise_signal=True,
            min_raise_count=1,
        ),
        Stage(
            key="stability1_snap_r2ab_all",
            title="稳定批1-快照（R2A/R2B, 3 seeds, 2个月）",
            groups=["R2A", "R2B"],
            seeds=[101, 202, 303],
            months=2,
            use_snapshot=True,
            require_lag_check=True,
            require_raise_signal=True,
            min_raise_count=1,
        ),
        Stage(
            key="stability2_snap_r2ab_all",
            title="稳定批2-快照（R2A/R2B, 3 seeds, 2个月）",
            groups=["R2A", "R2B"],
            seeds=[101, 202, 303],
            months=2,
            use_snapshot=True,
            require_lag_check=True,
            require_raise_signal=True,
            min_raise_count=1,
        ),
        Stage(
            key="stability3_fresh_r2ab_all",
            title="稳定批3-Fresh（R2A/R2B, 3 seeds, 2个月）",
            groups=["R2A", "R2B"],
            seeds=[101, 202, 303],
            months=2,
            use_snapshot=False,
            require_lag_check=True,
            require_raise_signal=True,
            min_raise_count=1,
        ),
    ]

    log_items: List[Dict[str, Any]] = []

    for i, stage in enumerate(stages, start=1):
        stage_out = _stage_run_dir(base_dir, i, stage.key)
        stage_out.mkdir(parents=True, exist_ok=True)
        print(f"\n=== Stage {i}/{len(stages)}: {stage.title} ===", flush=True)
        rc = run_stage(stage, stage_out)
        if rc != 0:
            result = {
                "stage": stage.key,
                "title": stage.title,
                "status": "stopped",
                "reason": f"运行脚本返回码={rc}",
                "batch_dir": str(stage_out),
            }
            log_items.append(result)
            break

        try:
            zone_payload = _load_zone_json(stage_out)
            passed, eval_result = evaluate_stage(stage, zone_payload)
        except Exception as exc:
            result = {
                "stage": stage.key,
                "title": stage.title,
                "status": "stopped",
                "reason": f"门禁评估失败: {exc}",
                "batch_dir": str(stage_out),
            }
            log_items.append(result)
            break

        result = {
            "stage": stage.key,
            "title": stage.title,
            "status": "passed" if passed else "stopped",
            "batch_dir": str(stage_out),
            **eval_result,
        }
        log_items.append(result)

        if not passed:
            break

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "base_dir": str(base_dir),
        "stages_total": len(stages),
        "stages_finished": len(log_items),
        "stages": log_items,
    }

    (base_dir / "night_plan_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    md_lines = [
        "# 夜跑门禁总结",
        "",
        f"- 输出目录: {base_dir}",
        f"- 完成阶段数: {len(log_items)} / {len(stages)}",
        "",
    ]
    for idx, item in enumerate(log_items, start=1):
        md_lines.extend(
            [
                f"## {idx}. {item.get('title', item.get('stage'))}",
                f"- 状态: {item.get('status')}",
                f"- 批次目录: {item.get('batch_dir')}",
            ]
        )
        metrics = item.get("metrics", {})
        if metrics:
            md_lines.append(f"- 指标: `{json.dumps(metrics, ensure_ascii=False)}`")
        failures = item.get("failures", []) or []
        warnings = item.get("warnings", []) or []
        if failures:
            md_lines.append("- 未通过原因:")
            for f in failures:
                md_lines.append(f"  - {f}")
        if warnings:
            md_lines.append("- 预警:")
            for w in warnings:
                md_lines.append(f"  - {w}")
        if item.get("reason"):
            md_lines.append(f"- 原因: {item['reason']}")
        md_lines.append("")

    (base_dir / "night_plan_summary.md").write_text("\n".join(md_lines), encoding="utf-8")
    print(base_dir / "night_plan_summary.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
