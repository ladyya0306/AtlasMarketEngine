#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
课题线 A 低成本快测实验室

目标：
1. 不调用真实 LLM，先离线拆解 buyer 在 shortlist 前流失的主因。
2. 用固定小样本做快速对照，验证：
   - 收入单调性方向
   - 资金压力事件方向
   - 普通买家是否会被市场热度/传闻放大激活
3. 用定制 mock 复现 shortlist/crowd gate 问题，不依赖真实 LLM。

注意：
这不是发布口径结果，也不替代正式批次；它是 A 线的“便宜筛查台”。
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]

import sys

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agent_behavior import calculate_activation_probability
from models import Agent, AgentPreference, AgentStory
import transaction_engine
from transaction_engine import match_properties_for_buyer


def _safe_json_load(raw: Any) -> Dict[str, Any]:
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(str(raw))
    except Exception:
        return {}


def _extract_run_db(run_dir: Path) -> Optional[Path]:
    stdout_path = run_dir / "stdout.log"
    if not stdout_path.exists():
        return None
    text = stdout_path.read_text(encoding="utf-8", errors="ignore")
    matches = re.findall(r"DB path:\s*(.+)", text)
    if not matches:
        return None
    db_path = Path(matches[-1].strip())
    if not db_path.is_absolute():
        db_path = ROOT / db_path
    return db_path if db_path.exists() else None


def _scalar(cur: sqlite3.Cursor, query: str, params: Iterable[Any] = ()) -> int:
    row = cur.execute(query, tuple(params)).fetchone()
    return int(row[0] or 0) if row else 0


def _batch_dropoff_diagnosis(batch_dir: Path) -> Dict[str, Any]:
    runs: List[Dict[str, Any]] = []
    reason_by_group: Dict[str, Counter] = defaultdict(Counter)
    for run_dir in sorted([p for p in batch_dir.iterdir() if p.is_dir()]):
        db_path = _extract_run_db(run_dir)
        if not db_path:
            continue
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        buyer_roles = _scalar(
            cur,
            "SELECT COUNT(*) FROM decision_logs WHERE month=1 AND event_type='ROLE_DECISION' AND decision IN ('BUYER','BUYER_SELLER')",
        )
        shortlist_buyers = _scalar(
            cur,
            "SELECT COUNT(DISTINCT buyer_id) FROM property_buyer_matches WHERE month=1",
        )
        selected_buyers = _scalar(
            cur,
            "SELECT COUNT(DISTINCT buyer_id) FROM property_buyer_matches WHERE month=1 AND order_id IS NOT NULL",
        )
        order_buyers = _scalar(
            cur,
            "SELECT COUNT(DISTINCT buyer_id) FROM transaction_orders WHERE created_month=1",
        )
        reason_counter = Counter()
        for reason_raw, thought_raw, metrics_raw in cur.execute(
            "SELECT reason, thought_process, context_metrics FROM decision_logs WHERE month=1 AND event_type='BUYER_MATCH_SUMMARY'"
        ).fetchall():
            metrics = _safe_json_load(metrics_raw)
            thought = _safe_json_load(thought_raw)
            code = str(
                metrics.get("no_selection_reason_code")
                or thought.get("no_selection_reason_code")
                or ""
            ).strip()
            if code and code != "HAS_SELECTION":
                reason_counter[code] += 1
        conn.close()

        group = run_dir.name.split("_", 1)[0]
        role_to_shortlist = round((shortlist_buyers / buyer_roles), 4) if buyer_roles > 0 else 0.0
        runs.append(
            {
                "run": run_dir.name,
                "group": group,
                "buyer_roles": buyer_roles,
                "shortlist_buyers": shortlist_buyers,
                "selected_buyers": selected_buyers,
                "order_buyers": order_buyers,
                "role_to_shortlist": role_to_shortlist,
                "top_no_selection_codes": dict(reason_counter.most_common(5)),
            }
        )
        reason_by_group[group].update(reason_counter)

    group_summary: Dict[str, Any] = {}
    for group in sorted({item["group"] for item in runs}):
        group_runs = [item for item in runs if item["group"] == group]
        group_summary[group] = {
            "run_count": len(group_runs),
            "avg_role_to_shortlist": round(mean(item["role_to_shortlist"] for item in group_runs), 4) if group_runs else 0.0,
            "top_no_selection_codes": dict(reason_by_group[group].most_common(5)),
        }

    overall_reasons = Counter()
    for item in runs:
        overall_reasons.update(item.get("top_no_selection_codes", {}))

    return {
        "batch_dir": str(batch_dir.resolve()),
        "run_count": len(runs),
        "runs": runs,
        "group_summary": group_summary,
        "overall_top_no_selection_codes": dict(overall_reasons.most_common(8)),
    }


def _same_month_loop_chain(batch_dir: Path) -> Dict[str, Any]:
    selection_reasons = Counter()
    strategy_profiles = Counter()
    crowd_modes = Counter()
    upstream_combos = Counter()
    blocker_counts = Counter()
    sample_rows: List[Dict[str, Any]] = []

    for run_dir in sorted([p for p in batch_dir.iterdir() if p.is_dir()]):
        db_path = _extract_run_db(run_dir)
        if not db_path:
            continue
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        for (thought_raw,) in cur.execute(
            "SELECT thought_process FROM decision_logs WHERE month=1 AND event_type='BUYER_MATCH_SUMMARY'"
        ).fetchall():
            thought = _safe_json_load(thought_raw)
            if str(thought.get("no_selection_reason_code", "") or "") != "SAME_MONTH_LOOP_STOP":
                continue
            selection_reason = str(thought.get("selection_reason", "") or "")
            strategy_profile = str(thought.get("strategy_profile", "") or "")
            crowd_mode = str(thought.get("crowd_mode", "") or "")
            retry_budget = thought.get("retry_budget", {}) or {}
            factor_contract = thought.get("factor_contract", {}) or {}
            finance = factor_contract.get("finance_guard_snapshot", {}) or {}
            deadline = factor_contract.get("deadline_pressure_snapshot", {}) or {}
            no_candidate_filter_counts = thought.get("no_candidate_filter_counts", {}) or {}
            for key, value in (no_candidate_filter_counts or {}).items():
                if str(key).startswith("blocked_") and int(value or 0) > 0:
                    blocker_counts[str(key)] += int(value or 0)

            selection_reasons[selection_reason] += 1
            strategy_profiles[strategy_profile] += 1
            crowd_modes[crowd_mode] += 1
            upstream_combos[
                (
                    selection_reason,
                    crowd_mode,
                    str(deadline.get("stage", "")),
                    str(factor_contract.get("type", {}).get("target", "")),
                )
            ] += 1

            if len(sample_rows) < 10:
                sample_rows.append(
                    {
                        "run": run_dir.name,
                        "strategy_profile": strategy_profile,
                        "selection_reason": selection_reason,
                        "crowd_mode": crowd_mode,
                        "deadline_stage": str(deadline.get("stage", "") or ""),
                        "type_target": str(factor_contract.get("type", {}).get("target", "") or ""),
                        "max_price": round(float(finance.get("max_price", 0.0) or 0.0), 2),
                        "cash": round(float(finance.get("cash", 0.0) or 0.0), 2),
                        "monthly_income": round(float(finance.get("monthly_income", 0.0) or 0.0), 2),
                        "listings_for_buyer_count": int(thought.get("listings_for_buyer_count", 0) or 0),
                        "shortlist_count": int(thought.get("shortlist_count", 0) or 0),
                        "no_candidate_primary_blockers": list(thought.get("no_candidate_primary_blockers", []) or []),
                        "attempt_budget": int(retry_budget.get("attempt_budget", 0) or 0),
                        "attempts_spent": int(retry_budget.get("attempts_spent", 0) or 0),
                        "same_month_max_abandon_cycles": int(retry_budget.get("same_month_max_abandon_cycles", 0) or 0),
                    }
                )
        conn.close()

    return {
        "selection_reasons": dict(selection_reasons.most_common(8)),
        "strategy_profiles": dict(strategy_profiles.most_common(8)),
        "crowd_modes": dict(crowd_modes.most_common(8)),
        "blocker_counts": dict(blocker_counts.most_common(8)),
        "upstream_combos": [
            {
                "selection_reason": combo[0],
                "crowd_mode": combo[1],
                "deadline_stage": combo[2],
                "type_target": combo[3],
                "count": count,
            }
            for combo, count in upstream_combos.most_common(10)
        ],
        "sample_rows": sample_rows,
        "plain_explanation": [
            "SAME_MONTH_LOOP_STOP 不是随机停机，而是“本月反复选不到房 + 系统允许继续搜 + 同月放弃次数达到上限”后触发的节流停机。",
            "它最常见的上一层原因不是谈判失败，而是 no_candidates_after_soft_relax / no_candidates_after_hard_filters，也就是候选房源在 shortlist 前就空了。",
            "当前样本里它主要发生在 early 阶段，说明很多买家不是因为拖太久才停，而是刚进场就找不到能推进的候选。",
        ],
    }


def _make_agent(
    agent_id: int,
    *,
    agent_type: str,
    monthly_income: float,
    cash: float,
    props: int,
    age: int = 31,
    marital_status: str = "single",
    motive: str = "",
    housing_stage: str = "",
    family_stage: str = "",
    education_path: str = "",
    financial_profile: str = "",
    need_school: bool = False,
    target_zone: str = "B",
    max_price: float = 3_000_000,
) -> Agent:
    agent = Agent(
        id=agent_id,
        name=f"Probe-{agent_id}",
        age=age,
        marital_status=marital_status,
        cash=float(cash),
        monthly_income=float(monthly_income),
    )
    agent.agent_type = str(agent_type)
    agent.story = AgentStory(
        background_story="固定快测样本",
        housing_need="购房/换房",
        investment_style="balanced",
        purchase_motive_primary=motive,
        housing_stage=housing_stage,
        family_stage=family_stage,
        education_path=education_path,
        financial_profile=financial_profile,
    )
    agent.preference = AgentPreference(
        target_zone=target_zone,
        max_price=float(max_price),
        need_school_district=bool(need_school),
        max_affordable_price=float(max_price),
        psychological_price=float(max_price * 0.95),
    )
    agent.owned_properties = []
    for idx in range(int(props)):
        agent.owned_properties.append(
            {
                "property_id": int(agent_id * 100 + idx + 1),
                "zone": "B",
                "base_value": 2_600_000,
            }
        )
    return agent


def surrogate_role_decision(
    agent: Agent,
    *,
    market_trend: str = "STABLE",
    rumor_heat: float = 0.0,
    cash_stress_event: bool = False,
) -> Dict[str, Any]:
    """
    便宜版 surrogate：不替代正式 LLM，只用于快测方向。
    """
    base_score = float(calculate_activation_probability(agent))
    score = float(base_score)
    reason_bits: List[str] = []
    herd_activation = False
    role = "OBSERVER"

    owns_property = bool(agent.owned_properties)
    motive = str(getattr(agent.story, "purchase_motive_primary", "") or "").lower()
    need_school = bool(getattr(agent.preference, "need_school_district", False))

    if cash_stress_event and owns_property:
        score += 0.10
        reason_bits.append("cash_stress_owner")

    if (
        agent.agent_type == "normal"
        and not owns_property
        and str(market_trend).upper() in {"UP", "HOT", "PANIC_UP"}
    ):
        herd_bonus = 0.0
        if rumor_heat >= 0.8:
            herd_bonus += 0.12
        elif rumor_heat >= 0.5:
            herd_bonus += 0.07
        if motive in {"starter_entry", "starter_home", "marriage_home", "education_driven"}:
            herd_bonus += 0.03
        if need_school:
            herd_bonus += 0.02
        if herd_bonus > 0:
            score += herd_bonus
            herd_activation = True
            reason_bits.append(f"rumor_herd_bonus={herd_bonus:.2f}")

    if owns_property and (cash_stress_event or agent.cash < 50_000):
        role = "SELLER"
    elif (not owns_property) and score >= 0.15:
        role = "BUYER"
    elif owns_property and score >= 0.17 and motive in {"chain_replacement", "upgrade_living"}:
        role = "BUYER_SELLER"

    return {
        "agent_id": int(agent.id),
        "agent_type": str(agent.agent_type),
        "role": role,
        "base_score": round(base_score, 4),
        "final_score": round(score, 4),
        "herd_activation": bool(herd_activation and role in {"BUYER", "BUYER_SELLER"}),
        "reason_bits": reason_bits,
    }


def run_activation_probes() -> Dict[str, Any]:
    agents = [
        _make_agent(
            1,
            agent_type="normal",
            monthly_income=20_000,
            cash=600_000,
            props=0,
            motive="starter_entry",
            need_school=False,
            max_price=2_000_000,
        ),
        _make_agent(
            2,
            agent_type="normal",
            monthly_income=23_000,
            cash=900_000,
            props=0,
            motive="education_driven",
            need_school=True,
            family_stage="school_age_child",
            education_path="public_school",
            max_price=2_800_000,
        ),
        _make_agent(
            3,
            agent_type="normal",
            monthly_income=26_000,
            cash=40_000,
            props=1,
            motive="asset_allocation",
            financial_profile="income_stressed",
            max_price=2_600_000,
        ),
        _make_agent(
            4,
            agent_type="smart",
            monthly_income=26_000,
            cash=900_000,
            props=0,
            motive="starter_entry",
            max_price=2_500_000,
        ),
    ]

    scenarios = [
        {"name": "stable_base", "market_trend": "STABLE", "rumor_heat": 0.0, "cash_stress_event": False},
        {"name": "hot_rumor", "market_trend": "UP", "rumor_heat": 0.9, "cash_stress_event": False},
        {"name": "owner_cash_stress", "market_trend": "STABLE", "rumor_heat": 0.0, "cash_stress_event": True},
    ]

    scenario_rows = []
    for scenario in scenarios:
        rows = [
            surrogate_role_decision(
                agent,
                market_trend=str(scenario["market_trend"]),
                rumor_heat=float(scenario["rumor_heat"]),
                cash_stress_event=bool(scenario["cash_stress_event"]),
            )
            for agent in agents
        ]
        normal_buyers = sum(1 for row in rows if row["agent_type"] == "normal" and row["role"] in {"BUYER", "BUYER_SELLER"})
        normal_herd = sum(1 for row in rows if row["agent_type"] == "normal" and row["herd_activation"])
        seller_count = sum(1 for row in rows if row["role"] in {"SELLER", "BUYER_SELLER"})
        scenario_rows.append(
            {
                **scenario,
                "normal_buyer_count": normal_buyers,
                "normal_herd_activation_count": normal_herd,
                "seller_count": seller_count,
                "details": rows,
            }
        )

    incomes = [0.8, 1.0, 1.2, 1.4]
    monotonic_rows = []
    for mult in incomes:
        probe = _make_agent(
            11,
            agent_type="normal",
            monthly_income=20_000 * mult,
            cash=750_000 * mult,
            props=0,
            motive="starter_entry",
            max_price=2_200_000 * mult,
        )
        result = surrogate_role_decision(probe, market_trend="STABLE", rumor_heat=0.0, cash_stress_event=False)
        monotonic_rows.append({"income_multiplier": mult, **result})

    monotonic_direction_ok = all(
        monotonic_rows[idx]["final_score"] <= monotonic_rows[idx + 1]["final_score"]
        for idx in range(len(monotonic_rows) - 1)
    )

    return {
        "scenarios": scenario_rows,
        "monotonicity_probe": {
            "rows": monotonic_rows,
            "direction_ok": bool(monotonic_direction_ok),
        },
    }


def _extract_json_array_after_marker(prompt: str, marker: str) -> List[Dict[str, Any]]:
    start_marker = prompt.find(marker)
    if start_marker < 0:
        return []
    bracket_start = prompt.find("[", start_marker)
    if bracket_start < 0:
        return []
    depth = 0
    for idx in range(bracket_start, len(prompt)):
        ch = prompt[idx]
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(prompt[bracket_start : idx + 1])
                except Exception:
                    return []
    return []


def fake_buyer_llm(prompt: str, default_return: dict, system_prompt: str = "", model_type: str = "fast") -> dict:
    if "当前低拥挤备选" in prompt:
        alternatives = _extract_json_array_after_marker(prompt, "当前低拥挤备选")
        if alternatives:
            chosen = sorted(
                alternatives,
                key=lambda item: (
                    0 if bool(item.get("school", False) or str(item.get("school", "")).lower() in {"yes", "true"}) else 1,
                    float(item.get("crowd_pressure_units", 0.0) or 0.0),
                    float(item.get("price", 0.0) or 0.0),
                ),
            )[0]
            return {
                "action": "SWITCH",
                "selected_property_id": int(chosen.get("id")),
                "reason": "切换到更低拥挤的候选房源。",
            }
        return {"action": "WITHDRAW", "selected_property_id": None, "reason": "没有更合适的低拥挤候选。"}

    shortlist = _extract_json_array_after_marker(prompt, "【拥挤修正后Top候选】")
    if not shortlist:
        return default_return

    school_candidates = [
        item for item in shortlist
        if str(item.get("school", "")).strip().lower() in {"yes", "true"}
    ]
    pool = school_candidates or shortlist
    chosen = sorted(
        pool,
        key=lambda item: (
            float(item.get("crowd_pressure_units", 0.0) or 0.0),
            float(item.get("price", 0.0) or 0.0),
        ),
    )[0]
    return {
        "selected_property_id": int(chosen.get("id")),
        "thought_bubble": "先选能进场的房子，不在高拥挤里空转。",
        "reason": "优先选择拥挤度更低、且仍满足核心偏好的候选。",
        "monthly_intent": "CONTINUE",
    }


@contextmanager
def patched_buyer_llm():
    original = transaction_engine.safe_call_llm
    transaction_engine.safe_call_llm = fake_buyer_llm
    try:
        yield
    finally:
        transaction_engine.safe_call_llm = original


def _build_shortlist_probe_inputs() -> Tuple[List[Dict[str, Any]], Dict[int, Dict[str, Any]], List[Agent]]:
    listings = [
        {"property_id": 101, "listed_price": 2_050_000, "owner_id": 1, "listing_age_months": 0},
        {"property_id": 102, "listed_price": 2_180_000, "owner_id": 2, "listing_age_months": 0},
        {"property_id": 103, "listed_price": 1_980_000, "owner_id": 3, "listing_age_months": 0},
        {"property_id": 104, "listed_price": 2_350_000, "owner_id": 4, "listing_age_months": 0},
    ]
    props_map = {
        101: {"property_id": 101, "zone": "B", "building_area": 89, "build_year": 2018, "is_school_district": True, "property_type": "small"},
        102: {"property_id": 102, "zone": "B", "building_area": 96, "build_year": 2020, "is_school_district": False, "property_type": "small"},
        103: {"property_id": 103, "zone": "B", "building_area": 88, "build_year": 2016, "is_school_district": False, "property_type": "small"},
        104: {"property_id": 104, "zone": "A", "building_area": 105, "build_year": 2021, "is_school_district": True, "property_type": "improve"},
    }
    buyers = [
        _make_agent(
            21,
            agent_type="normal",
            monthly_income=24_000,
            cash=850_000,
            props=0,
            motive="education_driven",
            need_school=True,
            target_zone="B",
            max_price=2_300_000,
        ),
        _make_agent(
            22,
            agent_type="normal",
            monthly_income=22_000,
            cash=780_000,
            props=0,
            motive="starter_entry",
            need_school=False,
            target_zone="B",
            max_price=2_150_000,
        ),
    ]
    return listings, props_map, buyers


def run_shortlist_probe() -> Dict[str, Any]:
    listings, props_map, buyers = _build_shortlist_probe_inputs()
    scenarios = [
        {"name": "balanced_pressure", "pressure_map": {101: 1.0, 102: 0.5, 103: 0.4, 104: 1.4}},
        {"name": "crowded_gate", "pressure_map": {101: 5.2, 102: 4.7, 103: 4.3, 104: 5.8}},
    ]
    results = []
    with patched_buyer_llm():
        for scenario in scenarios:
            scenario_rows = []
            for buyer in buyers:
                buyer._candidate_pressure_map = dict(scenario["pressure_map"])
                buyer._current_matching_month = 1
                buyer._attempted_property_ids_by_month = {}
                buyer._repriced_reentry_property_ids = []
                buyer.waited_months = 0
                buyer.max_wait_months = 6
                match = match_properties_for_buyer(
                    buyer,
                    list(listings),
                    props_map,
                    config=None,
                    market_trend="STABLE",
                )
                match_item = None
                if isinstance(match, list):
                    match_item = match[0] if match else None
                else:
                    match_item = match
                ctx = getattr(buyer, "_last_buyer_match_context", {}) or {}
                scenario_rows.append(
                    {
                        "buyer_id": int(buyer.id),
                        "selected_property_id": int(match_item.get("property_id")) if match_item else None,
                        "selection_reason": str(ctx.get("selection_reason", "") or ""),
                        "shortlist_property_ids": list(ctx.get("shortlist_property_ids", []) or []),
                        "crowd_filtered_out": int(ctx.get("crowd_hard_filtered_out_count", 0) or 0),
                        "llm_called": bool(ctx.get("llm_called", False)),
                    }
                )
            results.append({"name": scenario["name"], "rows": scenario_rows})
    return {"scenarios": results}


def run_no_candidate_probe() -> Dict[str, Any]:
    buyer = _make_agent(
        31,
        agent_type="normal",
        monthly_income=12_000,
        cash=120_000,
        props=0,
        motive="education_driven",
        need_school=True,
        target_zone="B",
        max_price=350_000,
    )
    buyer.school_urgency = 2
    buyer.waited_months = 0
    buyer.max_wait_months = 6

    listings = [
        {"property_id": 201, "listed_price": 2_200_000.0, "owner_id": 1, "listing_age_months": 0},
        {"property_id": 202, "listed_price": 2_350_000.0, "owner_id": 2, "listing_age_months": 0},
        {"property_id": 203, "listed_price": 320_000.0, "owner_id": 3, "listing_age_months": 0},
    ]
    props_map = {
        201: {"property_id": 201, "zone": "B", "building_area": 88.0, "bedrooms": 2, "is_school_district": True, "property_type": "small"},
        202: {"property_id": 202, "zone": "B", "building_area": 92.0, "bedrooms": 2, "is_school_district": False, "property_type": "small"},
        203: {"property_id": 203, "zone": "A", "building_area": 80.0, "bedrooms": 1, "is_school_district": False, "property_type": "small"},
    }

    match = match_properties_for_buyer(
        buyer,
        listings,
        props_map,
        config=None,
        market_trend="STABLE",
    )
    ctx = dict(getattr(buyer, "_last_buyer_match_context", {}) or {})
    return {
        "selected": bool(match),
        "selection_reason": str(ctx.get("selection_reason", "") or ""),
        "primary_blockers": list(ctx.get("no_candidate_primary_blockers", []) or []),
        "filter_counts": dict(ctx.get("no_candidate_filter_counts", {}) or {}),
    }


def render_markdown(report: Dict[str, Any]) -> str:
    lines = [
        "# 课题线A低成本快测实验室",
        "",
        f"- 生成时间: {report['generated_at']}",
        f"- 批次诊断目录: {report.get('batch_dir') or '(未提供)'}",
        "",
        "## 一、离线 buyer 流失诊断",
        "",
    ]
    batch = report.get("batch_diagnosis")
    if not batch:
        lines.append("- 未提供正式批次目录，跳过离线诊断。")
    else:
        lines.append(f"- run 数: {batch['run_count']}")
        lines.append(f"- 全批次 top no-selection 码: `{json.dumps(batch['overall_top_no_selection_codes'], ensure_ascii=False)}`")
        lines.append("")
        lines.append("| 组别 | avg_role_to_shortlist | top_no_selection_codes |")
        lines.append("| --- | ---: | --- |")
        for group, item in sorted(batch["group_summary"].items()):
            lines.append(
                f"| {group} | {item['avg_role_to_shortlist']:.4f} | "
                f"{json.dumps(item['top_no_selection_codes'], ensure_ascii=False)} |"
            )
    same_month = report.get("same_month_loop_chain")
    if same_month:
        lines.extend(
            [
                "",
                "## 二、SAME_MONTH_LOOP_STOP 原因链",
                "",
                f"- 上一层直接原因: `{json.dumps(same_month['selection_reasons'], ensure_ascii=False)}`",
                f"- 常见画像/路径: `{json.dumps(same_month['strategy_profiles'], ensure_ascii=False)}`",
                f"- 常见拥挤模式: `{json.dumps(same_month['crowd_modes'], ensure_ascii=False)}`",
                f"- 常见硬过滤拦截: `{json.dumps(same_month.get('blocker_counts', {}), ensure_ascii=False)}`",
                "",
                "| selection_reason | crowd_mode | deadline_stage | type_target | count |",
                "| --- | --- | --- | --- | ---: |",
            ]
        )
        for item in same_month["upstream_combos"]:
            lines.append(
                f"| {item['selection_reason']} | {item['crowd_mode']} | {item['deadline_stage']} | "
                f"{item['type_target']} | {item['count']} |"
            )
        lines.append("")
        for idx, text in enumerate(same_month.get("plain_explanation", []), start=1):
            lines.append(f"{idx}. {text}")

    activation = report["activation_probe"]
    lines.extend(
        [
            "",
            "## 三、激活快测",
            "",
            "| 场景 | normal_buyers | normal_herd | sellers |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for item in activation["scenarios"]:
        lines.append(
            f"| {item['name']} | {item['normal_buyer_count']} | {item['normal_herd_activation_count']} | {item['seller_count']} |"
        )
    lines.append("")
    lines.append(f"- 收入单调性快测方向是否同向: `{activation['monotonicity_probe']['direction_ok']}`")

    shortlist = report["shortlist_probe"]
    no_candidate = report["no_candidate_probe"]
    lines.extend(
        [
            "",
            "## 四、Shortlist 快测",
            "",
            "| 场景 | buyer_id | selected_property_id | selection_reason | shortlist_ids |",
            "| --- | ---: | ---: | --- | --- |",
        ]
    )
    for scenario in shortlist["scenarios"]:
        for row in scenario["rows"]:
            lines.append(
                f"| {scenario['name']} | {row['buyer_id']} | "
                f"{row['selected_property_id'] if row['selected_property_id'] is not None else 'null'} | "
                f"{row['selection_reason'] or '(empty)'} | {row['shortlist_property_ids']} |"
            )
    lines.extend(
        [
            "",
            "## 五、结构性无候选快测",
            "",
            f"- selection_reason: `{no_candidate.get('selection_reason', '')}`",
            f"- primary_blockers: `{json.dumps(no_candidate.get('primary_blockers', []), ensure_ascii=False)}`",
            f"- filter_counts: `{json.dumps(no_candidate.get('filter_counts', {}), ensure_ascii=False)}`",
        ]
    )
    lines.extend(
        [
            "",
            "## 六、说明",
            "",
            "1. 这份报告不调用真实 LLM，适合先做便宜排查与方向筛查。",
            "2. `激活快测` 只用于验证方向，不替代正式批次结论。",
            "3. `Shortlist 快测` 用定制 mock 复现拥挤度/候选集对选房入口的影响。",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run low-cost Line A fast lab.")
    parser.add_argument(
        "--batch-dir",
        type=str,
        default="",
        help="可选：已有正式批次目录，用于离线 buyer 流失诊断。",
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        default=str(ROOT / "results" / "line_a_fast_lab"),
        help="输出目录。",
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    batch_report = None
    if args.batch_dir:
        batch_dir = Path(args.batch_dir)
        if not batch_dir.is_absolute():
            batch_dir = (ROOT / batch_dir).resolve()
        if batch_dir.exists():
            batch_report = _batch_dropoff_diagnosis(batch_dir)

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "batch_dir": batch_report.get("batch_dir") if batch_report else "",
        "batch_diagnosis": batch_report,
        "same_month_loop_chain": _same_month_loop_chain(Path(batch_report["batch_dir"])) if batch_report else None,
        "activation_probe": run_activation_probes(),
        "shortlist_probe": run_shortlist_probe(),
        "no_candidate_probe": run_no_candidate_probe(),
    }

    json_path = out_dir / "line_a_fast_lab.json"
    md_path = out_dir / "line_a_fast_lab.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    print(f"fast-lab json written: {json_path}")
    print(f"fast-lab md written: {md_path}")


if __name__ == "__main__":
    main()
