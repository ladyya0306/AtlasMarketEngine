#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
角色激活可解释性评估脚本

输入: 矩阵批次目录（包含 batch_summary.json）
输出:
1) role_activation_explainability.json
2) role_activation_explainability.md

说明:
- 该脚本不改动模拟结果，只做证据提取与方向性检查。
- 如果某项检查样本不足，会明确标记为 "insufficient_data"。
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

ROOT = Path(__file__).resolve().parents[1]


BUY_ROLES = {"BUYER", "BUYER_SELLER"}
SELL_ROLES = {"SELLER", "BUYER_SELLER"}
STRESS_KEYWORDS = (
    "失业",
    "裁员",
    "重病",
    "医疗",
    "离婚",
    "债务",
    "现金流",
    "收入下降",
    "经济压力",
    "突发支出",
)


@dataclass
class RunEval:
    group: str
    seed: int
    run_dir: str
    db_path: str
    plan_path: str
    month_count: int
    role_decision_count: int
    buyer_activation_rate: float
    seller_activation_rate: float
    monotonicity: Dict[str, Any]
    event_direction: Dict[str, Any]
    explainable_log: Dict[str, Any]
    counterfactual: Dict[str, Any]


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_json_loads(text: Optional[str]) -> Dict[str, Any]:
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        return {}


def _resolve_db_path(run_item: Dict[str, Any], batch_dir: Path) -> Path:
    raw = run_item.get("db_path") or ""
    p = Path(raw)
    if p.is_absolute():
        return p
    return (ROOT / raw).resolve() if raw else (batch_dir / "missing.db")


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _bucketize_income(items: List[Tuple[int, float]], bins: int = 5) -> List[List[int]]:
    if not items:
        return []
    ordered = sorted(items, key=lambda x: x[1])
    n = len(ordered)
    buckets: List[List[int]] = [[] for _ in range(min(bins, n))]
    for idx, (agent_id, _) in enumerate(ordered):
        b = min(len(buckets) - 1, int(idx * len(buckets) / max(n, 1)))
        buckets[b].append(agent_id)
    return buckets


def _rate(num: float, den: float) -> float:
    return float(num) / float(den) if den else 0.0


def _evaluate_monotonicity(conn: sqlite3.Connection) -> Dict[str, Any]:
    role_rows = conn.execute(
        """
        SELECT agent_id, decision
        FROM decision_logs
        WHERE event_type='ROLE_DECISION'
        """
    ).fetchall()
    if not role_rows:
        return {"status": "insufficient_data", "reason": "no_role_decisions"}
    decision_by_agent: Dict[int, List[str]] = defaultdict(list)
    for r in role_rows:
        decision_by_agent[int(r["agent_id"])].append(str(r["decision"]).upper())

    income_rows = conn.execute(
        "SELECT agent_id, monthly_income FROM agents_finance WHERE monthly_income IS NOT NULL"
    ).fetchall()
    income_items = [(int(r["agent_id"]), float(r["monthly_income"])) for r in income_rows]
    if len(income_items) < 10:
        return {"status": "insufficient_data", "reason": "too_few_income_rows"}

    buckets = _bucketize_income(income_items, bins=5)
    bucket_rates = []
    for i, bucket in enumerate(buckets):
        if not bucket:
            continue
        buyer_hits = 0
        total = 0
        for aid in bucket:
            decs = decision_by_agent.get(aid, [])
            if not decs:
                continue
            total += 1
            if any(d in BUY_ROLES for d in decs):
                buyer_hits += 1
        bucket_rates.append(
            {
                "bucket_index": i + 1,
                "agent_count": total,
                "buyer_activation_rate": round(_rate(buyer_hits, total), 4),
            }
        )

    if len(bucket_rates) < 3:
        return {"status": "insufficient_data", "reason": "too_few_valid_buckets", "buckets": bucket_rates}

    tolerance = 0.03
    non_decreasing = True
    dips = []
    prev = bucket_rates[0]["buyer_activation_rate"]
    for b in bucket_rates[1:]:
        cur = b["buyer_activation_rate"]
        if cur + tolerance < prev:
            non_decreasing = False
            dips.append(
                {
                    "from_bucket": b["bucket_index"] - 1,
                    "to_bucket": b["bucket_index"],
                    "from_rate": prev,
                    "to_rate": cur,
                }
            )
        prev = cur

    return {
        "status": "pass" if non_decreasing else "warn",
        "rule": "higher_income_bucket_should_not_have_lower_buyer_activation_rate",
        "buckets": bucket_rates,
        "dips": dips,
        "tolerance": tolerance,
    }


def _has_stress_keyword(text: str) -> bool:
    return any(k in text for k in STRESS_KEYWORDS)


def _evaluate_event_direction(conn: sqlite3.Connection) -> Dict[str, Any]:
    life_rows = conn.execute(
        """
        SELECT month, agent_id, COALESCE(decision, '') AS event_name, COALESCE(reason, '') AS reason_text
        FROM decision_logs
        WHERE event_type='LIFE_EVENT'
        """
    ).fetchall()
    role_rows = conn.execute(
        """
        SELECT month, agent_id, decision
        FROM decision_logs
        WHERE event_type='ROLE_DECISION'
        """
    ).fetchall()
    if not role_rows:
        return {"status": "insufficient_data", "reason": "no_role_decisions"}

    stress_agents_by_month: Dict[int, set[int]] = defaultdict(set)
    for r in life_rows:
        m = int(r["month"] or 0)
        if m <= 0:
            continue
        txt = f"{r['event_name']} {r['reason_text']}"
        if _has_stress_keyword(txt):
            stress_agents_by_month[m].add(int(r["agent_id"]))

    seller_by_month_agent: Dict[Tuple[int, int], int] = {}
    seen_by_month_agent: Dict[Tuple[int, int], int] = {}
    for r in role_rows:
        m = int(r["month"] or 0)
        aid = int(r["agent_id"] or 0)
        if m <= 0 or aid <= 0:
            continue
        seen_by_month_agent[(m, aid)] = 1
        if str(r["decision"]).upper() in SELL_ROLES:
            seller_by_month_agent[(m, aid)] = 1

    month_stats = []
    for m in sorted({k[0] for k in seen_by_month_agent.keys()}):
        stress_agents = stress_agents_by_month.get(m, set())
        if not stress_agents:
            continue
        all_agents = {aid for mm, aid in seen_by_month_agent.keys() if mm == m}
        non_stress_agents = all_agents - stress_agents
        if not non_stress_agents:
            continue
        stress_seller = sum(1 for aid in stress_agents if seller_by_month_agent.get((m, aid), 0) == 1)
        non_stress_seller = sum(1 for aid in non_stress_agents if seller_by_month_agent.get((m, aid), 0) == 1)
        stress_rate = _rate(stress_seller, len(stress_agents))
        non_stress_rate = _rate(non_stress_seller, len(non_stress_agents))
        month_stats.append(
            {
                "month": m,
                "stress_agent_count": len(stress_agents),
                "non_stress_agent_count": len(non_stress_agents),
                "stress_seller_rate": round(stress_rate, 4),
                "non_stress_seller_rate": round(non_stress_rate, 4),
                "direction_ok": bool(stress_rate >= non_stress_rate),
            }
        )

    if not month_stats:
        return {
            "status": "insufficient_data",
            "reason": "no_month_with_detected_stress_life_events",
        }

    pass_cnt = sum(1 for x in month_stats if x["direction_ok"])
    return {
        "status": "pass" if pass_cnt == len(month_stats) else "warn",
        "rule": "seller_activation_should_increase_after_stress_life_events",
        "pass_months": pass_cnt,
        "total_months": len(month_stats),
        "months": month_stats,
    }


def _flatten_dict(d: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in d.items():
        key = f"{prefix}.{k}" if prefix else str(k)
        if isinstance(v, dict):
            out.update(_flatten_dict(v, key))
        else:
            out[key] = v
    return out


def _load_plan(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _known_counterfactual_direction(key: str, old: Any, new: Any) -> Optional[str]:
    try:
        old_f = float(old)
        new_f = float(new)
    except Exception:
        return None
    if key == "simulation.agent.income_adjustment_rate":
        return "buyer_rate_up_or_equal" if new_f > old_f else "buyer_rate_down_or_equal"
    if key == "mortgage.down_payment_ratio":
        return "buyer_rate_down_or_equal" if new_f > old_f else "buyer_rate_up_or_equal"
    if key == "mortgage.max_dti_ratio":
        return "buyer_rate_up_or_equal" if new_f > old_f else "buyer_rate_down_or_equal"
    return None


def _evaluate_counterfactual(
    seed_runs: List[RunEval],
    plan_by_run: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    checks = []
    for i in range(len(seed_runs)):
        for j in range(i + 1, len(seed_runs)):
            a = seed_runs[i]
            b = seed_runs[j]
            pa = _flatten_dict(plan_by_run.get(a.run_dir, {}))
            pb = _flatten_dict(plan_by_run.get(b.run_dir, {}))
            keys = sorted(set(pa.keys()) | set(pb.keys()))
            diffs = [k for k in keys if pa.get(k) != pb.get(k)]
            if len(diffs) != 1:
                continue
            key = diffs[0]
            expectation = _known_counterfactual_direction(key, pa.get(key), pb.get(key))
            if not expectation:
                continue
            old_rate = a.buyer_activation_rate
            new_rate = b.buyer_activation_rate
            if expectation == "buyer_rate_up_or_equal":
                ok = new_rate + 1e-8 >= old_rate
            else:
                ok = new_rate <= old_rate + 1e-8
            checks.append(
                {
                    "from_group": a.group,
                    "to_group": b.group,
                    "changed_key": key,
                    "old_value": pa.get(key),
                    "new_value": pb.get(key),
                    "old_buyer_rate": round(old_rate, 4),
                    "new_buyer_rate": round(new_rate, 4),
                    "expected_direction": expectation,
                    "direction_ok": ok,
                }
            )
    if not checks:
        return {
            "status": "insufficient_data",
            "reason": "no_single_variable_pair_found_in_batch",
            "detail": "建议增加只改一个关键变量的对照组，用于反事实一致性检查。",
        }
    pass_cnt = sum(1 for c in checks if c["direction_ok"])
    return {
        "status": "pass" if pass_cnt == len(checks) else "warn",
        "pass_count": pass_cnt,
        "total_count": len(checks),
        "checks": checks,
    }


def _extract_explainable_log(conn: sqlite3.Connection) -> Dict[str, Any]:
    rows = conn.execute(
        """
        SELECT agent_id, month, decision, reason, thought_process, context_metrics
        FROM decision_logs
        WHERE event_type='ROLE_DECISION'
        ORDER BY log_id DESC
        LIMIT 200
        """
    ).fetchall()
    if not rows:
        return {"status": "insufficient_data", "reason": "no_role_decisions"}

    trigger_counter = Counter()
    life_pressure_counter = Counter()
    risk_mode_counter = Counter()
    delayed_counter = Counter()
    samples = []
    for r in rows:
        thought = _safe_json_loads(r["thought_process"])
        ctx = _safe_json_loads(r["context_metrics"])
        trigger = str(thought.get("trigger", "unknown"))
        life_pressure = str(thought.get("life_pressure", "unknown"))
        risk_mode = str(thought.get("risk_mode", "unknown"))
        delay = str(ctx.get("m14_info_delay_months", "unknown"))
        trigger_counter[trigger] += 1
        life_pressure_counter[life_pressure] += 1
        risk_mode_counter[risk_mode] += 1
        delayed_counter[delay] += 1
        if len(samples) < 8:
            samples.append(
                {
                    "agent_id": int(r["agent_id"]),
                    "month": int(r["month"] or 0),
                    "role": str(r["decision"]),
                    "trigger": trigger,
                    "life_pressure": life_pressure,
                    "risk_mode": risk_mode,
                    "delayed_months": ctx.get("m14_info_delay_months"),
                    "dti_ratio": ctx.get("dti_ratio"),
                    "real_max_price": ctx.get("real_max_price"),
                    "reason_excerpt": str(r["reason"] or "")[:140],
                }
            )

    return {
        "status": "ok",
        "top_trigger": trigger_counter.most_common(8),
        "top_life_pressure": life_pressure_counter.most_common(8),
        "top_risk_mode": risk_mode_counter.most_common(8),
        "top_delay_months": delayed_counter.most_common(8),
        "samples": samples,
    }


def _eval_one_run(run_item: Dict[str, Any], batch_dir: Path) -> Optional[RunEval]:
    if run_item.get("status") != "success":
        return None
    db_path = _resolve_db_path(run_item, batch_dir)
    if not db_path.exists():
        return None
    conn = _connect(db_path)
    try:
        month_count = int(
            conn.execute("SELECT COALESCE(MAX(month), 0) FROM decision_logs").fetchone()[0] or 0
        )
        role_total = int(
            conn.execute(
                "SELECT COUNT(*) FROM decision_logs WHERE event_type='ROLE_DECISION'"
            ).fetchone()[0]
            or 0
        )
        buyer_total = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM decision_logs
                WHERE event_type='ROLE_DECISION' AND decision IN ('BUYER','BUYER_SELLER')
                """
            ).fetchone()[0]
            or 0
        )
        seller_total = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM decision_logs
                WHERE event_type='ROLE_DECISION' AND decision IN ('SELLER','BUYER_SELLER')
                """
            ).fetchone()[0]
            or 0
        )
        monotonicity = _evaluate_monotonicity(conn)
        event_direction = _evaluate_event_direction(conn)
        explainable_log = _extract_explainable_log(conn)
        return RunEval(
            group=str(run_item.get("group_key", "")),
            seed=int(run_item.get("seed", 0)),
            run_dir=str(run_item.get("run_dir", "")),
            db_path=str(db_path),
            plan_path=str(run_item.get("plan_path", "")),
            month_count=month_count,
            role_decision_count=role_total,
            buyer_activation_rate=_rate(buyer_total, role_total),
            seller_activation_rate=_rate(seller_total, role_total),
            monotonicity=monotonicity,
            event_direction=event_direction,
            explainable_log=explainable_log,
            counterfactual={},
        )
    finally:
        conn.close()


def _summarize_stability(runs: List[RunEval]) -> Dict[str, Any]:
    by_group: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
    for r in runs:
        by_group[r.group]["monotonicity"].append(r.monotonicity.get("status", "unknown"))
        by_group[r.group]["event_direction"].append(r.event_direction.get("status", "unknown"))
    output = {}
    for group, checks in by_group.items():
        output[group] = {}
        for check_name, statuses in checks.items():
            pass_cnt = sum(1 for s in statuses if s == "pass")
            warn_cnt = sum(1 for s in statuses if s == "warn")
            insufficient_cnt = sum(1 for s in statuses if s == "insufficient_data")
            output[group][check_name] = {
                "pass_count": pass_cnt,
                "warn_count": warn_cnt,
                "insufficient_data_count": insufficient_cnt,
                "total": len(statuses),
            }
    return output


def _to_dict(run: RunEval) -> Dict[str, Any]:
    return {
        "group": run.group,
        "seed": run.seed,
        "run_dir": run.run_dir,
        "db_path": run.db_path,
        "plan_path": run.plan_path,
        "month_count": run.month_count,
        "role_decision_count": run.role_decision_count,
        "buyer_activation_rate": round(run.buyer_activation_rate, 4),
        "seller_activation_rate": round(run.seller_activation_rate, 4),
        "monotonicity": run.monotonicity,
        "event_direction": run.event_direction,
        "counterfactual": run.counterfactual,
        "explainable_log": run.explainable_log,
    }


def _render_md(payload: Dict[str, Any]) -> str:
    lines = [
        "# 角色激活可解释性评估",
        "",
        f"- 生成时间: {payload['generated_at']}",
        f"- 批次目录: {payload['batch_dir']}",
        f"- 运行样本数: {payload['run_count']}",
        "",
        "## 一、检查项说明",
        "",
        "1. 收入单调性检查：收入分位越高，买方激活率不应系统性下降。",
        "2. 事件方向检查：资金压力类生活事件出现后，卖方激活率应上升或至少不下降。",
        "3. 反事实一致性检查：若批次内存在只改一个关键变量的对照组，检查方向是否符合预期。",
        "4. 稳定性检查：同一组别在多个随机种子下，上述方向是否一致。",
        "5. 可解释日志：输出触发因子、关键状态和最终角色样例。",
        "",
        "## 二、逐运行结果",
        "",
        "| 组别 | seed | 买方激活率 | 卖方激活率 | 收入单调性 | 事件方向 | 反事实一致性 |",
        "| --- | --- | ---: | ---: | --- | --- | --- |",
    ]
    for run in payload["runs"]:
        lines.append(
            f"| {run['group']} | {run['seed']} | {run['buyer_activation_rate']:.4f} | "
            f"{run['seller_activation_rate']:.4f} | {run['monotonicity'].get('status','unknown')} | "
            f"{run['event_direction'].get('status','unknown')} | {run['counterfactual'].get('status','unknown')} |"
        )

    lines.extend(
        [
            "",
            "## 三、多 seed 稳定性汇总",
            "",
            "```json",
            json.dumps(payload["stability_summary"], ensure_ascii=False, indent=2),
            "```",
            "",
            "## 四、注意事项",
            "",
            "1. 如果反事实一致性显示 insufficient_data，通常表示当前批次没有“只改一个变量”的对照组。",
            "2. 这份报告用于解释机制方向，不替代最终市场量价结论。",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate role activation explainability for a batch.")
    parser.add_argument("batch_dir", help="Batch directory that contains batch_summary.json")
    args = parser.parse_args()

    batch_dir = Path(args.batch_dir).resolve()
    summary_path = batch_dir / "batch_summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"batch_summary.json not found: {summary_path}")

    summary = _load_json(summary_path)
    raw_runs = summary.get("runs", [])
    eval_runs: List[RunEval] = []
    plan_by_run: Dict[str, Dict[str, Any]] = {}
    for item in raw_runs:
        run_eval = _eval_one_run(item, batch_dir=batch_dir)
        if not run_eval:
            continue
        eval_runs.append(run_eval)
        plan_by_run[run_eval.run_dir] = _load_plan(Path(run_eval.plan_path))

    by_seed: Dict[int, List[RunEval]] = defaultdict(list)
    for r in eval_runs:
        by_seed[r.seed].append(r)
    for seed, seed_runs in by_seed.items():
        cf = _evaluate_counterfactual(seed_runs=seed_runs, plan_by_run=plan_by_run)
        for r in seed_runs:
            r.counterfactual = cf

    payload = {
        "generated_at": summary.get("generated_at"),
        "batch_dir": str(batch_dir),
        "run_count": len(eval_runs),
        "runs": [_to_dict(x) for x in eval_runs],
        "stability_summary": _summarize_stability(eval_runs),
    }

    json_path = batch_dir / "role_activation_explainability.json"
    md_path = batch_dir / "role_activation_explainability.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_render_md(payload), encoding="utf-8")
    print(f"role explainability written: {json_path}")
    print(f"role explainability written: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
