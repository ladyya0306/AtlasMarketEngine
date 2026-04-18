#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unified activation governance helpers.

This module keeps the first implementation focused on:
1. activation_governance config contract
2. pre-run mismatch gate evaluation
3. monthly activation / bucket funnel exports
"""

from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml


def _safe_json_loads(raw_value: Any, default: Any) -> Any:
    if raw_value in (None, "", b""):
        return default
    if isinstance(raw_value, (dict, list)):
        return raw_value
    try:
        return json.loads(raw_value)
    except Exception:
        return default


def build_activation_governance_config(
    *,
    activation_mode: str = "forced",
    gate_mode: str = "warn",
    profiled_market_required: bool = False,
    hard_bucket_matcher_required: bool = False,
    hybrid_floor_enabled: bool = False,
    hybrid_floor_strategy: str = "bucket_targeted_llm_first",
    autofill_supply_floor: int = 0,
    autofill_demand_floor: int = 0,
    severe_bucket_deficit_ratio: float = 5.0,
    pause_on_severe_mismatch: bool = False,
    emit_bucket_funnel: bool = True,
) -> Dict[str, Any]:
    mode = str(activation_mode or "forced").strip().lower() or "forced"
    if mode not in {"forced", "hybrid", "natural"}:
        mode = "forced"
    resolved_gate = str(gate_mode or "warn").strip().lower() or "warn"
    if resolved_gate not in {"warn", "pause", "autofill"}:
        resolved_gate = "warn"
    return {
        "enabled": True,
        "activation_mode": mode,
        "gate_mode": resolved_gate,
        "profiled_market_required": bool(profiled_market_required),
        "hard_bucket_matcher_required": bool(hard_bucket_matcher_required),
        "hybrid_floor_enabled": bool(hybrid_floor_enabled and mode == "hybrid"),
        "hybrid_floor_strategy": str(hybrid_floor_strategy or "bucket_targeted_llm_first"),
        "autofill_supply_floor": max(0, int(autofill_supply_floor or 0)),
        "autofill_demand_floor": max(0, int(autofill_demand_floor or 0)),
        "severe_bucket_deficit_ratio": float(severe_bucket_deficit_ratio or 0.0),
        "pause_on_severe_mismatch": bool(pause_on_severe_mismatch),
        "emit_bucket_funnel": bool(emit_bucket_funnel),
    }


def evaluate_mismatch_gate(
    *,
    governance_snapshot: Optional[Dict[str, Any]],
    activation_governance: Dict[str, Any],
    profiled_market_enabled: bool,
    hard_bucket_matcher_enabled: bool,
) -> Dict[str, Any]:
    cfg = dict(activation_governance or {})
    mode = str(cfg.get("activation_mode", "forced") or "forced")
    gate_mode = str(cfg.get("gate_mode", "warn") or "warn")
    severe_ratio = float(cfg.get("severe_bucket_deficit_ratio", 5.0) or 5.0)
    if severe_ratio <= 0:
        severe_ratio = 5.0

    reasons: List[str] = []
    bucket_summary: List[Dict[str, Any]] = []
    severe_bucket_count = 0
    warned_bucket_count = 0
    recommended_action = "continue"
    mismatch_grade = "pass"

    if bool(cfg.get("profiled_market_required", False)) and not bool(profiled_market_enabled):
        reasons.append("profiled_market_mode_required")
        mismatch_grade = "block"
        recommended_action = "inspect_profile_pack"
    if bool(cfg.get("hard_bucket_matcher_required", False)) and not bool(hard_bucket_matcher_enabled):
        reasons.append("hard_bucket_matcher_required")
        mismatch_grade = "block"
        recommended_action = "inspect_profile_pack"

    snapshot = governance_snapshot if isinstance(governance_snapshot, dict) else {}
    if snapshot:
        identity = snapshot.get("identity", {}) or {}
        profile_pack_path = str(identity.get("profile_pack_path", "") or "").strip()
        profile_pack = _try_load_profile_pack(profile_pack_path)
        alignment_map = _build_alignment_map(profile_pack) if profile_pack else {}

        supply_mode = str((snapshot.get("supply_library", {}) or {}).get("experiment_mode", "abundant") or "abundant")
        supply_rows = (snapshot.get("supply_library", {}) or {}).get("buckets", []) or []
        property_bucket_price_min: Dict[str, float] = {}
        property_bucket_count_selected: Dict[str, int] = {}
        for item in supply_rows:
            if not isinstance(item, dict):
                continue
            bucket_id = str(item.get("bucket_id", "") or "").strip()
            if not bucket_id:
                continue
            price_range = item.get("price_range", []) or []
            try:
                min_price = float(price_range[0]) if len(price_range) >= 1 else 0.0
            except Exception:
                min_price = 0.0
            property_bucket_price_min[bucket_id] = float(min_price)
            try:
                count_selected = int(item.get("count_selected_mode", 0) or 0)
            except Exception:
                count_selected = 0
            if count_selected <= 0:
                try:
                    count_selected = int((item.get("count_by_supply_mode", {}) or {}).get(supply_mode, 0) or 0)
                except Exception:
                    count_selected = 0
            property_bucket_count_selected[bucket_id] = int(max(0, count_selected))

        budget_report = snapshot.get("budget_consistency_report", {}) or {}
        for failure in budget_report.get("failures", []) or []:
            token = str(failure or "").strip()
            if not token:
                continue
            reasons.append(token)
            mismatch_grade = "block"
            recommended_action = "inspect_profile_pack"

        demand_rows = {
            str(item.get("bucket_id", "")).strip(): item
            for item in ((snapshot.get("demand_library", {}) or {}).get("buckets", []) or [])
            if isinstance(item, dict) and str(item.get("bucket_id", "")).strip()
        }
        competition_rows = ((snapshot.get("competition_control_report", {}) or {}).get(supply_mode, []) or [])
        for row in competition_rows:
            if not isinstance(row, dict):
                continue
            bucket_id = str(row.get("agent_bucket_id", "") or "").strip()
            if not bucket_id:
                continue
            eligible_supply_count = int(row.get("eligible_supply_count", 0) or 0)
            buyer_supply_ratio = float(row.get("buyer_to_supply_ratio", 0.0) or 0.0)
            demand_row = demand_rows.get(bucket_id, {})
            need_school = bool(demand_row.get("need_school_district", False))
            buyer_count = int(row.get("buyer_count", 0) or demand_row.get("count", 0) or 0)
            max_price_range = demand_row.get("max_price_range", []) or []
            try:
                buyer_max_price = float(max_price_range[1]) if len(max_price_range) >= 2 else float(max_price_range[0]) if len(max_price_range) == 1 else 0.0
            except Exception:
                buyer_max_price = 0.0
            eligible_property_buckets = alignment_map.get(bucket_id, [])
            estimated_affordable_supply = 0
            if buyer_max_price > 0 and eligible_property_buckets:
                for pb in eligible_property_buckets:
                    min_price = float(property_bucket_price_min.get(str(pb), 0.0) or 0.0)
                    if min_price <= buyer_max_price:
                        estimated_affordable_supply += int(property_bucket_count_selected.get(str(pb), 0) or 0)

            severity = "pass"
            row_reasons: List[str] = []
            if eligible_supply_count <= 0:
                severity = "block"
                row_reasons.append("eligible_supply_zero")
                if need_school:
                    row_reasons.append("school_supply_gap")
            elif buyer_count > 0 and eligible_property_buckets and estimated_affordable_supply <= 0:
                # Eligible bucket exists, but none is even price-overlapping with this buyer bucket.
                severity = "block"
                row_reasons.append("affordable_supply_zero")
            elif buyer_supply_ratio > severe_ratio:
                severity = "block"
                row_reasons.append("buyer_supply_ratio_high")
            elif buyer_supply_ratio > 3.0 or buyer_supply_ratio < 0.2:
                severity = "warn"
                row_reasons.append(
                    "buyer_supply_ratio_high" if buyer_supply_ratio > 3.0 else "buyer_supply_ratio_low"
                )
            elif need_school and buyer_supply_ratio > 4.0:
                severity = "warn"
                row_reasons.append("school_supply_gap")

            if severity == "block":
                severe_bucket_count += 1
                mismatch_grade = "block"
                recommended_action = "autofill_supply"
            elif severity == "warn":
                warned_bucket_count += 1
                if mismatch_grade != "block":
                    mismatch_grade = "warn"
                if recommended_action == "continue":
                    recommended_action = "autofill_supply"
            reasons.extend(row_reasons)
            bucket_summary.append(
                {
                    "profile_bucket_id": bucket_id,
                    "eligible_supply_count": eligible_supply_count,
                    "estimated_affordable_supply_count": int(estimated_affordable_supply),
                    "buyer_max_price_upper": float(buyer_max_price),
                    "buyer_to_supply_ratio": round(buyer_supply_ratio, 4),
                    "severity": severity,
                    "reasons": row_reasons,
                }
            )

    if not reasons and mismatch_grade == "pass":
        recommended_action = "continue"

    deduped_reasons = []
    seen_reasons = set()
    for reason in reasons:
        key = str(reason or "").strip()
        if not key or key in seen_reasons:
            continue
        seen_reasons.add(key)
        deduped_reasons.append(key)

    governance_status = mismatch_grade
    return {
        "activation_mode": mode,
        "gate_mode": gate_mode,
        "governance_status": governance_status,
        "mismatch_grade": mismatch_grade,
        "mismatch_reasons": deduped_reasons,
        "recommended_action": recommended_action,
        "severe_bucket_count": int(severe_bucket_count),
        "warned_bucket_count": int(warned_bucket_count),
        "autofill_applied": False,
        "bucket_summary": bucket_summary,
    }


def render_governance_summary(mismatch_gate: Dict[str, Any]) -> str:
    top_reasons = ", ".join((mismatch_gate.get("mismatch_reasons", []) or [])[:3]) or "-"
    lines = [
        "ACTIVATION_GOVERNANCE_SUMMARY",
        f"- activation_mode: {mismatch_gate.get('activation_mode', 'unknown')}",
        f"- gate_mode: {mismatch_gate.get('gate_mode', 'warn')}",
        f"- governance_status: {mismatch_gate.get('governance_status', 'pass')}",
        f"- severe_bucket_count: {int(mismatch_gate.get('severe_bucket_count', 0) or 0)}",
        f"- warned_bucket_count: {int(mismatch_gate.get('warned_bucket_count', 0) or 0)}",
        f"- top_reasons: {top_reasons}",
        f"- recommended_action: {mismatch_gate.get('recommended_action', 'continue')}",
    ]
    return "\n".join(lines)


def write_mismatch_gate(path: Path, mismatch_gate: Dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(mismatch_gate, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path.resolve())


def _dict_writer(path: Path, fieldnames: List[str], rows: Iterable[Dict[str, Any]]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})
    return str(path.resolve())


def _fetchall(cur: sqlite3.Cursor, query: str, params: Tuple[Any, ...] = ()) -> List[sqlite3.Row]:
    try:
        cur.execute(query, params)
        return list(cur.fetchall() or [])
    except Exception:
        return []


def _column_exists(cur: sqlite3.Cursor, table_name: str, column_name: str) -> bool:
    try:
        cur.execute(f"PRAGMA table_info({table_name})")
        rows = cur.fetchall() or []
    except Exception:
        return False
    target = str(column_name or "").strip().lower()
    return any(str((row[1] if len(row) > 1 else "") or "").strip().lower() == target for row in rows)


def _try_load_profile_pack(profile_pack_path: str) -> Dict[str, Any]:
    raw = str(profile_pack_path or "").strip()
    if not raw:
        return {}
    path = Path(raw)
    if not path.exists():
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    pack = payload.get("profiled_market_mode", payload)
    return dict(pack) if isinstance(pack, dict) else {}


def _build_alignment_map(profile_pack: Dict[str, Any]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    rules = profile_pack.get("bucket_alignment_rules", [])
    if not isinstance(rules, list):
        return out
    for item in rules:
        if not isinstance(item, dict):
            continue
        agent_bucket = str(item.get("agent_bucket_id", "") or "").strip()
        if not agent_bucket:
            continue
        eligible = [
            str(x).strip()
            for x in (item.get("eligible_property_buckets", []) or [])
            if str(x).strip()
        ]
        if eligible:
            out[agent_bucket] = eligible
    return out


def _load_bucket_metadata(governance_snapshot: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    snapshot = governance_snapshot if isinstance(governance_snapshot, dict) else {}
    demand_rows = {
        str(item.get("bucket_id", "")).strip(): item
        for item in ((snapshot.get("demand_library", {}) or {}).get("buckets", []) or [])
        if isinstance(item, dict) and str(item.get("bucket_id", "")).strip()
    }
    supply_mode = str((snapshot.get("supply_library", {}) or {}).get("experiment_mode", "abundant") or "abundant")
    competition_rows = ((snapshot.get("competition_control_report", {}) or {}).get(supply_mode, []) or [])
    competition_map = {
        str(item.get("agent_bucket_id", "")).strip(): item
        for item in competition_rows
        if isinstance(item, dict) and str(item.get("agent_bucket_id", "")).strip()
    }
    result: Dict[str, Dict[str, Any]] = {}
    for bucket_id in sorted(set(demand_rows.keys()) | set(competition_map.keys())):
        demand = demand_rows.get(bucket_id, {})
        comp = competition_map.get(bucket_id, {})
        result[bucket_id] = {
            "role_side_hint": str(demand.get("role_side", "") or ""),
            "eligible_supply_count": int(comp.get("eligible_supply_count", 0) or 0),
            "buyer_to_supply_ratio": round(float(comp.get("buyer_to_supply_ratio", 0.0) or 0.0), 4),
        }
    return result


def _dropoff_stage_from_code(code: str) -> str:
    token = str(code or "").strip().upper()
    if not token:
        return ""
    if token.startswith("NO_CANDIDATES_") or token in {
        "SAME_MONTH_LOOP_STOP",
        "CROWD_HARD_GATE_ALL_BLOCKED",
        "LLM_STOP_THIS_MONTH",
        "SHORTLIST_EMPTY",
    }:
        return "shortlist"
    if token.startswith("NEGOTIATION_"):
        return "negotiation"
    if token.startswith("ORDER_"):
        return "order"
    return "shortlist"


def export_monthly_activation_funnel(
    *,
    db_path: Path,
    months: int,
    output_path: Path,
    run_id: str,
    group_id: str,
    shock_id: str,
    activation_mode: str,
    governance_status: str,
) -> str:
    fieldnames = [
        "run_id",
        "group_id",
        "shock_id",
        "month_index",
        "activation_mode",
        "governance_status",
        "candidate_count",
        "llm_candidate_count",
        "targeted_llm_candidate_count",
        "targeted_llm_activated_count",
        "targeted_llm_observer_count",
        "activated_count",
        "activated_buyer_count",
        "activated_seller_count",
        "activated_buyer_seller_count",
        "activated_buyer_seller_buy_first_count",
        "activated_buyer_seller_sell_first_count",
        "buyer_seller_wait_count",
        "activated_observer_count",
        "activation_llm_called_share",
        "info_delay_hit_count",
        "normal_activated_count",
        "smart_activated_count",
        "shortlist_entered_count",
        "negotiation_entered_count",
        "order_placed_count",
        "transaction_completed_count",
        "direct_transaction_without_order_count",
        "synthetic_floor_count",
        "has_selection_count",
        "no_buy_choose_to_wait_count",
        "no_buy_mechanism_blocked_count",
        "no_buy_no_suitable_listed_count",
        "no_buy_no_suitable_listed_has_unlisted_suitable_count",
        "no_buy_no_suitable_listed_no_suitable_exists_count",
        "no_buy_no_suitable_listed_unknown_count",
        "top_dropoff_reason",
    ]
    if not db_path.exists():
        rows = [
            {
                "run_id": run_id,
                "group_id": group_id,
                "shock_id": shock_id,
                "month_index": month,
                "activation_mode": activation_mode,
                "governance_status": governance_status,
            }
            for month in range(1, int(max(1, months)) + 1)
        ]
        return _dict_writer(output_path, fieldnames, rows)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        routing_map: Dict[int, Dict[str, Any]] = {}
        for row in _fetchall(
            cur,
            """
            SELECT month, context_metrics
            FROM decision_logs
            WHERE event_type='ROLE_ACTIVATION_ROUTING_SUMMARY'
            """,
        ):
            routing_map[int(row["month"] or 0)] = _safe_json_loads(row["context_metrics"], {})

        role_rows = _fetchall(
            cur,
            """
            SELECT dl.month, dl.agent_id, dl.decision, dl.llm_called, dl.context_metrics,
                   COALESCE(ast.agent_type, 'normal') AS agent_type, dl.thought_process
            FROM decision_logs dl
            LEFT JOIN agents_static ast ON ast.agent_id = dl.agent_id
            WHERE dl.event_type='ROLE_DECISION'
              AND dl.agent_id > 0
            """,
        )
        role_by_month: Dict[int, List[sqlite3.Row]] = {}
        for row in role_rows:
            role_by_month.setdefault(int(row["month"] or 0), []).append(row)

        match_rows = _fetchall(
            cur,
            """
            SELECT month, agent_id, context_metrics
            FROM decision_logs
            WHERE event_type='BUYER_MATCH_SUMMARY'
              AND agent_id > 0
            """,
        )
        match_by_month: Dict[int, List[sqlite3.Row]] = {}
        for row in match_rows:
            match_by_month.setdefault(int(row["month"] or 0), []).append(row)

        order_rows = _fetchall(
            cur,
            """
            SELECT created_month, buyer_id
            FROM transaction_orders
            """,
        )
        orders_by_month: Dict[int, set] = {}
        for row in order_rows:
            orders_by_month.setdefault(int(row["created_month"] or 0), set()).add(int(row["buyer_id"] or 0))

        tx_query = """
            SELECT month, buyer_id, order_id
            FROM transactions
            """
        if not _column_exists(cur, "transactions", "order_id"):
            tx_query = """
                SELECT month, buyer_id, NULL AS order_id
                FROM transactions
                """
        tx_rows = _fetchall(cur, tx_query)
        tx_count_map: Dict[int, int] = {}
        direct_tx_without_order_by_month: Dict[int, int] = {}
        for row in tx_rows:
            month = int(row["month"] or 0)
            tx_count_map[month] = int(tx_count_map.get(month, 0) or 0) + 1
            if row["order_id"] is None:
                direct_tx_without_order_by_month[month] = int(direct_tx_without_order_by_month.get(month, 0) or 0) + 1

        negotiation_rows = _fetchall(
            cur,
            """
            SELECT month, buyer_id
            FROM property_buyer_matches
            WHERE proceeded_to_negotiation=1
            """,
        )
        negotiation_by_month: Dict[int, set] = {}
        for row in negotiation_rows:
            negotiation_by_month.setdefault(int(row["month"] or 0), set()).add(int(row["buyer_id"] or 0))

        rows_out: List[Dict[str, Any]] = []
        for month in range(1, int(max(1, months)) + 1):
            routing = routing_map.get(month, {})
            role_list = role_by_month.get(month, [])
            match_list = match_by_month.get(month, [])
            buyer_shortlist = set()
            dropoff_counts: Dict[str, int] = {}
            has_selection_count = 0
            no_buy_choose_to_wait_count = 0
            no_buy_mechanism_blocked_count = 0
            no_buy_no_suitable_listed_count = 0
            no_buy_no_suitable_listed_has_unlisted_suitable = 0
            no_buy_no_suitable_listed_no_exists = 0
            no_buy_no_suitable_listed_unknown = 0
            for row in match_list:
                payload = _safe_json_loads(row["context_metrics"], {})
                shortlist_ids = payload.get("shortlist_property_ids", []) or []
                if shortlist_ids:
                    buyer_shortlist.add(int(row["agent_id"] or 0))
                code = str(payload.get("no_selection_reason_code", "") or "").strip()
                if code and code != "HAS_SELECTION":
                    dropoff_counts[code] = int(dropoff_counts.get(code, 0) or 0) + 1

                # "Why didn't the buyer buy" classification. Prefer explicit runtime fields; fall back to legacy codes.
                no_buy_class = str(payload.get("no_buy_class", "") or "").strip().upper()
                if not no_buy_class:
                    if code.strip().upper() == "HAS_SELECTION":
                        no_buy_class = "HAS_SELECTION"
                    elif code.strip().upper() in {"LLM_STOP_THIS_MONTH", "LLM_STOP_SIGNALLED"}:
                        no_buy_class = "CHOOSE_TO_WAIT"
                    elif code.strip().upper() in {"SAME_MONTH_LOOP_STOP", "CROWD_HARD_GATE_ALL_BLOCKED"}:
                        no_buy_class = "MECHANISM_BLOCKED"
                if no_buy_class == "HAS_SELECTION":
                    has_selection_count += 1
                elif no_buy_class == "CHOOSE_TO_WAIT":
                    no_buy_choose_to_wait_count += 1
                elif no_buy_class == "MECHANISM_BLOCKED":
                    no_buy_mechanism_blocked_count += 1
                elif no_buy_class == "NO_SUITABLE_LISTED":
                    no_buy_no_suitable_listed_count += 1
                    branch = str(payload.get("no_buy_branch", "") or "").strip().upper()
                    if branch == "HAS_UNLISTED_SUITABLE":
                        no_buy_no_suitable_listed_has_unlisted_suitable += 1
                    elif branch == "NO_SUITABLE_EXISTS":
                        no_buy_no_suitable_listed_no_exists += 1
                    elif branch:
                        no_buy_no_suitable_listed_unknown += 1

            active_rows = [
                row for row in role_list if str(row["decision"] or "").upper() in {"BUYER", "SELLER", "BUYER_SELLER"}
            ]
            observer_rows = [row for row in role_list if str(row["decision"] or "").upper() == "OBSERVER"]
            targeted_llm_activated_count = 0
            targeted_llm_observer_count = 0
            targeted_origin = "hybrid_bucket_targeted_llm"
            for row in active_rows:
                thought = _safe_json_loads(row["thought_process"], {})
                if str(thought.get("_decision_origin", "") or "").strip() == targeted_origin:
                    targeted_llm_activated_count += 1
            for row in observer_rows:
                thought = _safe_json_loads(row["thought_process"], {})
                if str(thought.get("_decision_origin", "") or "").strip() == targeted_origin:
                    targeted_llm_observer_count += 1
            llm_true = sum(1 for row in role_list if bool(row["llm_called"]))
            info_delay_hit = 0
            synthetic_floor_count = 0
            normal_activated = 0
            smart_activated = 0
            for row in active_rows:
                metrics = _safe_json_loads(row["context_metrics"], {})
                if int(metrics.get("m14_info_delay_months", 0) or 0) > 0:
                    info_delay_hit += 1
                if str(row["agent_type"] or "normal").lower() == "smart":
                    smart_activated += 1
                else:
                    normal_activated += 1
                thought = _safe_json_loads(row["thought_process"], {})
                trigger = str(thought.get("trigger", "") or "").strip()
                origin = str(thought.get("_decision_origin", "") or "").strip()
                if trigger == "forced_role_mode" or origin == "forced_role_mode":
                    synthetic_floor_count += 1

            activated_buyer_count = sum(1 for row in active_rows if str(row["decision"]).upper() == "BUYER")
            activated_seller_count = sum(1 for row in active_rows if str(row["decision"]).upper() == "SELLER")
            activated_buyer_seller_count = sum(
                1 for row in active_rows if str(row["decision"]).upper() == "BUYER_SELLER"
            )
            activated_buyer_seller_buy_first_count = 0
            activated_buyer_seller_sell_first_count = 0
            buyer_seller_wait_count = 0
            for row in active_rows:
                if str(row["decision"]).upper() != "BUYER_SELLER":
                    continue
                metrics = _safe_json_loads(row["context_metrics"], {})
                split_choice = str(metrics.get("buyer_seller_split_choice", "") or "").strip().lower()
                if split_choice == "buy_first":
                    activated_buyer_seller_buy_first_count += 1
                elif split_choice == "sell_first":
                    activated_buyer_seller_sell_first_count += 1
            for row in observer_rows:
                metrics = _safe_json_loads(row["context_metrics"], {})
                split_choice = str(metrics.get("buyer_seller_split_choice", "") or "").strip().lower()
                if split_choice == "wait":
                    buyer_seller_wait_count += 1
            top_dropoff_reason = ""
            if dropoff_counts:
                top_dropoff_reason = sorted(
                    dropoff_counts.items(),
                    key=lambda item: (-int(item[1]), str(item[0])),
                )[0][0]

            rows_out.append(
                {
                    "run_id": run_id,
                    "group_id": group_id,
                    "shock_id": shock_id,
                    "month_index": month,
                    "activation_mode": activation_mode,
                    "governance_status": governance_status,
                    "candidate_count": int(routing.get("stage1_prefilter_candidates", 0) or 0),
                    "llm_candidate_count": int(routing.get("llm_candidate_count", 0) or 0),
                    "targeted_llm_candidate_count": int(
                        routing.get("targeted_llm_candidate_count", 0)
                        or 0
                    ),
                    "targeted_llm_activated_count": int(targeted_llm_activated_count),
                    "targeted_llm_observer_count": int(targeted_llm_observer_count),
                    "activated_count": len(active_rows),
                    "activated_buyer_count": activated_buyer_count,
                    "activated_seller_count": activated_seller_count,
                    "activated_buyer_seller_count": activated_buyer_seller_count,
                    "activated_buyer_seller_buy_first_count": int(activated_buyer_seller_buy_first_count),
                    "activated_buyer_seller_sell_first_count": int(activated_buyer_seller_sell_first_count),
                    "buyer_seller_wait_count": int(buyer_seller_wait_count),
                    "activated_observer_count": len(observer_rows),
                    "activation_llm_called_share": round(float(llm_true) / float(max(1, len(role_list))), 4),
                    "info_delay_hit_count": int(info_delay_hit),
                    "normal_activated_count": int(normal_activated),
                    "smart_activated_count": int(smart_activated),
                    "shortlist_entered_count": len(buyer_shortlist),
                    "negotiation_entered_count": len(negotiation_by_month.get(month, set())),
                    "order_placed_count": len(orders_by_month.get(month, set())),
                    "transaction_completed_count": int(tx_count_map.get(month, 0) or 0),
                    "direct_transaction_without_order_count": int(
                        direct_tx_without_order_by_month.get(month, 0) or 0
                    ),
                    "synthetic_floor_count": int(
                        routing.get("synthetic_decision_count", synthetic_floor_count) or synthetic_floor_count
                    ),
                    "has_selection_count": int(has_selection_count),
                    "no_buy_choose_to_wait_count": int(no_buy_choose_to_wait_count),
                    "no_buy_mechanism_blocked_count": int(no_buy_mechanism_blocked_count),
                    "no_buy_no_suitable_listed_count": int(no_buy_no_suitable_listed_count),
                    "no_buy_no_suitable_listed_has_unlisted_suitable_count": int(
                        no_buy_no_suitable_listed_has_unlisted_suitable
                    ),
                    "no_buy_no_suitable_listed_no_suitable_exists_count": int(no_buy_no_suitable_listed_no_exists),
                    "no_buy_no_suitable_listed_unknown_count": int(no_buy_no_suitable_listed_unknown),
                    "top_dropoff_reason": top_dropoff_reason,
                }
            )
        return _dict_writer(output_path, fieldnames, rows_out)
    finally:
        conn.close()


def export_monthly_bucket_funnel(
    *,
    db_path: Path,
    months: int,
    output_path: Path,
    run_id: str,
    group_id: str,
    shock_id: str,
    activation_mode: str,
    governance_status: str,
    governance_snapshot: Optional[Dict[str, Any]],
) -> str:
    fieldnames = [
        "run_id",
        "group_id",
        "shock_id",
        "month_index",
        "profile_bucket_id",
        "role_side_hint",
        "governance_status",
        "eligible_supply_count",
        "buyer_to_supply_ratio",
        "candidate_count",
        "targeted_llm_candidate_count",
        "targeted_llm_activated_count",
        "synthetic_floor_count",
        "activated_count",
        "shortlist_entered_count",
        "negotiation_entered_count",
        "order_placed_count",
        "transaction_completed_count",
        "dominant_dropoff_reason",
        "dominant_dropoff_stage",
    ]
    bucket_meta = _load_bucket_metadata(governance_snapshot)
    if not db_path.exists():
        rows = []
        for month in range(1, int(max(1, months)) + 1):
            for bucket_id, meta in bucket_meta.items():
                rows.append(
                    {
                        "run_id": run_id,
                        "group_id": group_id,
                        "shock_id": shock_id,
                        "month_index": month,
                        "profile_bucket_id": bucket_id,
                        "role_side_hint": meta.get("role_side_hint", ""),
                        "governance_status": governance_status,
                        "eligible_supply_count": meta.get("eligible_supply_count", 0),
                        "buyer_to_supply_ratio": meta.get("buyer_to_supply_ratio", 0.0),
                    }
                )
        return _dict_writer(output_path, fieldnames, rows)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        role_rows = _fetchall(
            cur,
            """
            SELECT dl.month, dl.agent_id, dl.decision, dl.llm_called, dl.thought_process, ab.bucket_id
            FROM decision_logs dl
            JOIN profiled_market_agent_buckets ab ON ab.agent_id = dl.agent_id
            WHERE dl.event_type='ROLE_DECISION'
              AND dl.agent_id > 0
            """,
        )
        match_rows = _fetchall(
            cur,
            """
            SELECT dl.month, dl.agent_id, dl.context_metrics, ab.bucket_id
            FROM decision_logs dl
            JOIN profiled_market_agent_buckets ab ON ab.agent_id = dl.agent_id
            WHERE dl.event_type='BUYER_MATCH_SUMMARY'
              AND dl.agent_id > 0
            """,
        )
        negotiation_rows = _fetchall(
            cur,
            """
            SELECT pbm.month, pbm.buyer_id, ab.bucket_id
            FROM property_buyer_matches pbm
            JOIN profiled_market_agent_buckets ab ON ab.agent_id = pbm.buyer_id
            WHERE pbm.proceeded_to_negotiation=1
            """,
        )
        order_rows = _fetchall(
            cur,
            """
            SELECT o.created_month AS month, o.buyer_id, ab.bucket_id
            FROM transaction_orders o
            JOIN profiled_market_agent_buckets ab ON ab.agent_id = o.buyer_id
            """,
        )
        tx_rows = _fetchall(
            cur,
            """
            SELECT t.month, ab.bucket_id, COUNT(*) AS c
            FROM transactions t
            JOIN profiled_market_agent_buckets ab ON ab.agent_id = t.buyer_id
            GROUP BY t.month, ab.bucket_id
            """,
        )

        role_month_bucket: Dict[Tuple[int, str], List[sqlite3.Row]] = {}
        all_bucket_ids = set(bucket_meta.keys())
        for row in role_rows:
            key = (int(row["month"] or 0), str(row["bucket_id"] or ""))
            role_month_bucket.setdefault(key, []).append(row)
            all_bucket_ids.add(str(row["bucket_id"] or ""))

        shortlist_sets: Dict[Tuple[int, str], set] = {}
        dropoff_counts: Dict[Tuple[int, str], Dict[str, int]] = {}
        for row in match_rows:
            key = (int(row["month"] or 0), str(row["bucket_id"] or ""))
            payload = _safe_json_loads(row["context_metrics"], {})
            shortlist_ids = payload.get("shortlist_property_ids", []) or []
            if shortlist_ids:
                shortlist_sets.setdefault(key, set()).add(int(row["agent_id"] or 0))
            code = str(payload.get("no_selection_reason_code", "") or "").strip()
            if code and code != "HAS_SELECTION":
                bucket_counts = dropoff_counts.setdefault(key, {})
                bucket_counts[code] = int(bucket_counts.get(code, 0) or 0) + 1

        negotiation_sets: Dict[Tuple[int, str], set] = {}
        for row in negotiation_rows:
            key = (int(row["month"] or 0), str(row["bucket_id"] or ""))
            negotiation_sets.setdefault(key, set()).add(int(row["buyer_id"] or 0))

        order_sets: Dict[Tuple[int, str], set] = {}
        for row in order_rows:
            key = (int(row["month"] or 0), str(row["bucket_id"] or ""))
            order_sets.setdefault(key, set()).add(int(row["buyer_id"] or 0))

        tx_count_map = {
            (int(row["month"] or 0), str(row["bucket_id"] or "")): int(row["c"] or 0)
            for row in tx_rows
        }

        rows_out: List[Dict[str, Any]] = []
        for month in range(1, int(max(1, months)) + 1):
            for bucket_id in sorted(all_bucket_ids):
                key = (month, bucket_id)
                role_list = role_month_bucket.get(key, [])
                active_rows = [
                    row
                    for row in role_list
                    if str(row["decision"] or "").upper() in {"BUYER", "SELLER", "BUYER_SELLER"}
                ]
                targeted_candidate_rows = []
                for row in role_list:
                    thought = _safe_json_loads(row["thought_process"], {})
                    if str(thought.get("_decision_origin", "") or "").strip() == "hybrid_bucket_targeted_llm":
                        targeted_candidate_rows.append(row)
                targeted_llm_candidate_count = len(targeted_candidate_rows)
                targeted_llm_activated_count = sum(
                    1
                    for row in active_rows
                    if str(_safe_json_loads(row["thought_process"], {}).get("_decision_origin", "") or "").strip()
                    == "hybrid_bucket_targeted_llm"
                )
                synthetic_floor_count = 0
                for row in active_rows:
                    thought = _safe_json_loads(row["thought_process"], {})
                    trigger = str(thought.get("trigger", "") or "").strip()
                    origin = str(thought.get("_decision_origin", "") or "").strip()
                    if trigger == "forced_role_mode" or origin == "forced_role_mode":
                        synthetic_floor_count += 1

                dominant_dropoff_reason = ""
                dominant_dropoff_stage = ""
                code_counts = dropoff_counts.get(key, {})
                if code_counts:
                    dominant_dropoff_reason = sorted(
                        code_counts.items(),
                        key=lambda item: (-int(item[1]), str(item[0])),
                    )[0][0]
                    dominant_dropoff_stage = _dropoff_stage_from_code(dominant_dropoff_reason)

                meta = bucket_meta.get(bucket_id, {})
                rows_out.append(
                    {
                        "run_id": run_id,
                        "group_id": group_id,
                        "shock_id": shock_id,
                        "month_index": month,
                        "profile_bucket_id": bucket_id,
                        "role_side_hint": meta.get("role_side_hint", ""),
                        "governance_status": governance_status,
                        "eligible_supply_count": int(meta.get("eligible_supply_count", 0) or 0),
                        "buyer_to_supply_ratio": float(meta.get("buyer_to_supply_ratio", 0.0) or 0.0),
                        "candidate_count": len(role_list),
                        "targeted_llm_candidate_count": int(targeted_llm_candidate_count),
                        "targeted_llm_activated_count": int(targeted_llm_activated_count),
                        "synthetic_floor_count": int(synthetic_floor_count),
                        "activated_count": len(active_rows),
                        "shortlist_entered_count": len(shortlist_sets.get(key, set())),
                        "negotiation_entered_count": len(negotiation_sets.get(key, set())),
                        "order_placed_count": len(order_sets.get(key, set())),
                        "transaction_completed_count": int(tx_count_map.get(key, 0) or 0),
                        "dominant_dropoff_reason": dominant_dropoff_reason,
                        "dominant_dropoff_stage": dominant_dropoff_stage,
                    }
                )
        return _dict_writer(output_path, fieldnames, rows_out)
    finally:
        conn.close()
