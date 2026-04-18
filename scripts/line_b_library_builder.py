#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Line-B deterministic governance snapshot builder.
Produces demand/supply libraries and pre-run consistency reports from profile pack.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml


def _safe_range(raw: Any) -> Tuple[float, float]:
    if isinstance(raw, (list, tuple)) and len(raw) >= 2:
        low = float(raw[0] or 0.0)
        high = float(raw[1] or 0.0)
        if low > high:
            low, high = high, low
        return low, high
    return 0.0, 0.0


def _supply_bucket_price_range(bucket: Dict[str, Any]) -> Tuple[float, float]:
    if not isinstance(bucket, dict):
        return 0.0, 0.0
    direct = _safe_range(bucket.get("price_range"))
    if direct != (0.0, 0.0):
        return direct
    market_profile = bucket.get("market_profile", {}) or {}
    if isinstance(market_profile, dict):
        nested = _safe_range(market_profile.get("price_range"))
        if nested != (0.0, 0.0):
            return nested
    return 0.0, 0.0


def _supply_bucket_scalar(bucket: Dict[str, Any], field: str, default: Any = "") -> Any:
    if not isinstance(bucket, dict):
        return default
    if bucket.get(field) not in (None, ""):
        return bucket.get(field)
    market_profile = bucket.get("market_profile", {}) or {}
    if isinstance(market_profile, dict) and market_profile.get(field) not in (None, ""):
        return market_profile.get(field)
    return default


def _range_overlap(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    left = max(a[0], b[0])
    right = min(a[1], b[1])
    return max(0.0, right - left)


def _buyer_budget_ceiling(buyer_max_price_range: Tuple[float, float]) -> float:
    return float(buyer_max_price_range[1] or 0.0)


def _affordable_any_supply(buyer_max_price_range: Tuple[float, float], supply_price_range: Tuple[float, float]) -> bool:
    supply_low = float(supply_price_range[0] or 0.0)
    buyer_ceiling = _buyer_budget_ceiling(buyer_max_price_range)
    return buyer_ceiling > 0.0 and supply_low <= buyer_ceiling


def _normalize_graph_edge(raw_edge: Dict[str, Any], *, direction: str) -> Dict[str, Any]:
    if not isinstance(raw_edge, dict):
        return {}
    if direction == "demand_to_supply":
        source_key = "demand_bucket_id"
        target_key = "supply_bucket_id"
    else:
        source_key = "supply_bucket_id"
        target_key = "demand_bucket_id"
    source = str(raw_edge.get(source_key, "")).strip()
    target = str(raw_edge.get(target_key, "")).strip()
    if not source or not target:
        return {}
    return {
        source_key: source,
        target_key: target,
        "relation_type": str(raw_edge.get("relation_type", "primary") or "primary").strip().lower() or "primary",
        "budget_overlap_min": float(raw_edge.get("budget_overlap_min", 0.0) or 0.0),
        "confidence_weight": float(
            raw_edge.get("confidence_weight", raw_edge.get("reverse_confidence_weight", 1.0)) or 1.0
        ),
        "required_constraints": dict(raw_edge.get("required_constraints", {}) or {}),
        "soft_constraints": dict(raw_edge.get("soft_constraints", {}) or {}),
        "derived_from_alignment": bool(raw_edge.get("derived_from_alignment", False)),
    }


def _build_compatibility_graph(profile_pack: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    graph = profile_pack.get("compatibility_graph_v1", {}) or {}
    demand_edges = [
        _normalize_graph_edge(item, direction="demand_to_supply")
        for item in (graph.get("demand_to_supply", []) or [])
    ]
    demand_edges = [edge for edge in demand_edges if edge]
    supply_edges = [
        _normalize_graph_edge(item, direction="supply_to_demand")
        for item in (graph.get("supply_to_demand", []) or [])
    ]
    supply_edges = [edge for edge in supply_edges if edge]

    if not demand_edges:
        for rule in (profile_pack.get("bucket_alignment_rules", []) or []):
            if not isinstance(rule, dict):
                continue
            demand_bucket_id = str(rule.get("agent_bucket_id", "")).strip()
            if not demand_bucket_id:
                continue
            for supply_bucket_id in (rule.get("eligible_property_buckets", []) or []):
                sid = str(supply_bucket_id).strip()
                if not sid:
                    continue
                demand_edges.append(
                    {
                        "demand_bucket_id": demand_bucket_id,
                        "supply_bucket_id": sid,
                        "relation_type": "primary",
                        "budget_overlap_min": 0.0,
                        "confidence_weight": 1.0,
                        "required_constraints": {},
                        "soft_constraints": {},
                        "derived_from_alignment": True,
                    }
                )
            for supply_bucket_id in (rule.get("soft_property_buckets", []) or []):
                sid = str(supply_bucket_id).strip()
                if not sid:
                    continue
                demand_edges.append(
                    {
                        "demand_bucket_id": demand_bucket_id,
                        "supply_bucket_id": sid,
                        "relation_type": "secondary",
                        "budget_overlap_min": 0.0,
                        "confidence_weight": 0.5,
                        "required_constraints": {},
                        "soft_constraints": {},
                        "derived_from_alignment": True,
                    }
                )

    if not supply_edges:
        reverse_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for edge in demand_edges:
            key = (str(edge.get("supply_bucket_id", "")), str(edge.get("demand_bucket_id", "")))
            if not all(key):
                continue
            current = reverse_map.get(key)
            relation_type = str(edge.get("relation_type", "primary") or "primary")
            confidence = float(edge.get("confidence_weight", 1.0) or 1.0)
            if current is None or (
                current["relation_type"] != "primary" and relation_type == "primary"
            ):
                reverse_map[key] = {
                    "supply_bucket_id": key[0],
                    "demand_bucket_id": key[1],
                    "relation_type": relation_type,
                    "budget_overlap_min": float(edge.get("budget_overlap_min", 0.0) or 0.0),
                    "confidence_weight": confidence,
                    "required_constraints": dict(edge.get("required_constraints", {}) or {}),
                    "soft_constraints": dict(edge.get("soft_constraints", {}) or {}),
                    "derived_from_alignment": bool(edge.get("derived_from_alignment", False)),
                }
        supply_edges = list(reverse_map.values())

    demand_edges.sort(
        key=lambda item: (
            str(item.get("demand_bucket_id", "")),
            str(item.get("supply_bucket_id", "")),
            str(item.get("relation_type", "")),
        )
    )
    supply_edges.sort(
        key=lambda item: (
            str(item.get("supply_bucket_id", "")),
            str(item.get("demand_bucket_id", "")),
            str(item.get("relation_type", "")),
        )
    )
    return {
        "demand_to_supply": demand_edges,
        "supply_to_demand": supply_edges,
    }


def build_runtime_parent_bucket_report(profile_pack: Dict[str, Any]) -> Dict[str, Any]:
    parent_buckets = profile_pack.get("runtime_parent_buckets", {}) or {}
    demand = profile_pack.get("agent_profile_buckets", {}) or {}
    supply = profile_pack.get("property_profile_buckets", {}) or {}
    rows: List[Dict[str, Any]] = []
    missing_refs: List[str] = []
    for parent_id, parent in parent_buckets.items():
        if not isinstance(parent, dict):
            continue
        demand_children = [str(x).strip() for x in (parent.get("child_demand_buckets", []) or []) if str(x).strip()]
        supply_children = [str(x).strip() for x in (parent.get("child_supply_buckets", []) or []) if str(x).strip()]
        missing_demand = [bucket_id for bucket_id in demand_children if bucket_id not in demand]
        missing_supply = [bucket_id for bucket_id in supply_children if bucket_id not in supply]
        missing_refs.extend(f"missing_child_demand:{parent_id}:{bucket_id}" for bucket_id in missing_demand)
        missing_refs.extend(f"missing_child_supply:{parent_id}:{bucket_id}" for bucket_id in missing_supply)
        rows.append(
            {
                "parent_bucket_id": str(parent_id),
                "child_demand_bucket_count": len(demand_children),
                "child_supply_bucket_count": len(supply_children),
                "child_demand_buckets": demand_children,
                "child_supply_buckets": supply_children,
                "missing_child_demand_buckets": missing_demand,
                "missing_child_supply_buckets": missing_supply,
            }
        )
    rows.sort(key=lambda item: item["parent_bucket_id"])
    return {
        "ok": len(missing_refs) == 0,
        "missing_refs": missing_refs,
        "parent_bucket_count": len(rows),
        "rows": rows,
    }


def load_profile_pack_from_path(path: Path) -> Dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        return {}
    pack = payload.get("profiled_market_mode", payload)
    return pack if isinstance(pack, dict) else {}


def build_demand_library(profile_pack: Dict[str, Any]) -> Dict[str, Any]:
    buckets = profile_pack.get("agent_profile_buckets", {}) or {}
    output: List[Dict[str, Any]] = []
    for bucket_id, bucket in buckets.items():
        if not isinstance(bucket, dict):
            continue
        count = int(bucket.get("count", 0) or 0)
        if count <= 0:
            continue
        pref = bucket.get("preference_profile", {}) or {}
        budget = bucket.get("budget_profile", {}) or {}
        runtime = bucket.get("runtime_profile", {}) or {}
        output.append(
            {
                "bucket_id": str(bucket_id),
                "count": count,
                "role_side": str(bucket.get("role_side", "mixed") or "mixed"),
                "target_zone": str(pref.get("target_zone", "") or ""),
                "need_school_district": bool(pref.get("need_school_district", False)),
                "property_type_target": str(pref.get("property_type_target", "") or ""),
                "max_price_range": list(_safe_range(budget.get("max_price_range"))),
                "target_buy_price_range": list(_safe_range(budget.get("target_buy_price_range"))),
                "cash_range": list(_safe_range(budget.get("cash_range"))),
                "income_range": list(_safe_range(budget.get("income_range"))),
                "agent_type": str(runtime.get("agent_type", "normal") or "normal"),
                "info_delay_months": int(runtime.get("info_delay_months", 0) or 0),
            }
        )
    output.sort(key=lambda x: x["bucket_id"])
    return {
        "total_agents_profiled": int(sum(item["count"] for item in output)),
        "bucket_count": len(output),
        "buckets": output,
    }


def build_supply_library(profile_pack: Dict[str, Any], experiment_mode: str) -> Dict[str, Any]:
    buckets = profile_pack.get("property_profile_buckets", {}) or {}
    mode = str(experiment_mode or "abundant")
    output: List[Dict[str, Any]] = []
    for bucket_id, bucket in buckets.items():
        if not isinstance(bucket, dict):
            continue
        by_mode = bucket.get("count_by_supply_mode", {}) or {}
        output.append(
            {
                "bucket_id": str(bucket_id),
                "zone": str(_supply_bucket_scalar(bucket, "zone", "") or ""),
                "is_school_district": bool(_supply_bucket_scalar(bucket, "is_school_district", False)),
                "property_type_bucket": str(_supply_bucket_scalar(bucket, "property_type_bucket", "") or ""),
                "price_range": list(_supply_bucket_price_range(bucket)),
                "count_by_supply_mode": {
                    "abundant": int(by_mode.get("abundant", 0) or 0),
                    "scarce": int(by_mode.get("scarce", 0) or 0),
                },
                "count_selected_mode": int(by_mode.get(mode, 0) or 0),
            }
        )
    output.sort(key=lambda x: x["bucket_id"])
    return {
        "experiment_mode": mode,
        "total_selected_supply": int(sum(item["count_selected_mode"] for item in output)),
        "bucket_count": len(output),
        "buckets": output,
    }


def build_budget_consistency_report(profile_pack: Dict[str, Any]) -> Dict[str, Any]:
    demand = profile_pack.get("agent_profile_buckets", {}) or {}
    supply = profile_pack.get("property_profile_buckets", {}) or {}
    rules = profile_pack.get("bucket_alignment_rules", []) or []
    rule_map = {
        str(item.get("agent_bucket_id", "")).strip(): item
        for item in rules
        if isinstance(item, dict) and str(item.get("agent_bucket_id", "")).strip()
    }
    rows: List[Dict[str, Any]] = []
    failures: List[str] = []
    for bucket_id, bucket in demand.items():
        if not isinstance(bucket, dict):
            continue
        count = int(bucket.get("count", 0) or 0)
        if count <= 0:
            continue
        rule = rule_map.get(str(bucket_id))
        if not isinstance(rule, dict):
            failures.append(f"missing_alignment_rule:{bucket_id}")
            continue
        eligible = [str(x) for x in (rule.get("eligible_property_buckets", []) or []) if str(x).strip()]
        buyer_max_price = _safe_range((bucket.get("budget_profile", {}) or {}).get("max_price_range"))
        overlap_positive = False
        affordable_any_positive = False
        eligible_rows: List[Dict[str, Any]] = []
        for property_bucket_id in eligible:
            p_bucket = supply.get(property_bucket_id, {}) or {}
            p_price = _supply_bucket_price_range(p_bucket)
            overlap = _range_overlap(buyer_max_price, p_price)
            affordable_any = _affordable_any_supply(buyer_max_price, p_price)
            overlap_positive = overlap_positive or (overlap > 0.0)
            affordable_any_positive = affordable_any_positive or affordable_any
            eligible_rows.append(
                {
                    "property_bucket_id": property_bucket_id,
                    "price_range": [p_price[0], p_price[1]],
                    "overlap_width": round(float(overlap), 2),
                    "affordable_any_supply": bool(affordable_any),
                }
            )
        if not affordable_any_positive:
            failures.append(f"budget_mismatch:{bucket_id}")
        rows.append(
            {
                "agent_bucket_id": str(bucket_id),
                "buyer_count": count,
                "buyer_max_price_range": [buyer_max_price[0], buyer_max_price[1]],
                "eligible_supply": eligible_rows,
                "overlap_positive": bool(overlap_positive),
                "affordable_any_positive": bool(affordable_any_positive),
            }
        )
    return {
        "ok": len(failures) == 0,
        "failures": failures,
        "rows": rows,
    }


def build_competition_control_report(profile_pack: Dict[str, Any]) -> Dict[str, Any]:
    demand = profile_pack.get("agent_profile_buckets", {}) or {}
    supply = profile_pack.get("property_profile_buckets", {}) or {}
    rules = profile_pack.get("bucket_alignment_rules", []) or []
    rule_map = {
        str(item.get("agent_bucket_id", "")).strip(): item
        for item in rules
        if isinstance(item, dict) and str(item.get("agent_bucket_id", "")).strip()
    }
    abundant_rows: List[Dict[str, Any]] = []
    scarce_rows: List[Dict[str, Any]] = []
    for bucket_id, bucket in demand.items():
        if not isinstance(bucket, dict):
            continue
        buyer_count = int(bucket.get("count", 0) or 0)
        if buyer_count <= 0:
            continue
        rule = rule_map.get(str(bucket_id), {}) or {}
        eligible = [str(x) for x in (rule.get("eligible_property_buckets", []) or []) if str(x).strip()]
        abundant_supply = 0
        scarce_supply = 0
        for property_bucket_id in eligible:
            p_bucket = supply.get(property_bucket_id, {}) or {}
            by_mode = p_bucket.get("count_by_supply_mode", {}) or {}
            abundant_supply += int(by_mode.get("abundant", 0) or 0)
            scarce_supply += int(by_mode.get("scarce", 0) or 0)
        abundant_ratio = float(buyer_count) / float(max(1, abundant_supply))
        scarce_ratio = float(buyer_count) / float(max(1, scarce_supply))
        abundant_rows.append(
            {
                "agent_bucket_id": str(bucket_id),
                "buyer_count": buyer_count,
                "eligible_supply_count": abundant_supply,
                "buyer_to_supply_ratio": round(abundant_ratio, 4),
            }
        )
        scarce_rows.append(
            {
                "agent_bucket_id": str(bucket_id),
                "buyer_count": buyer_count,
                "eligible_supply_count": scarce_supply,
                "buyer_to_supply_ratio": round(scarce_ratio, 4),
            }
        )
    return {
        "abundant": abundant_rows,
        "scarce": scarce_rows,
    }


def build_reverse_coverage_report(profile_pack: Dict[str, Any], experiment_mode: str) -> Dict[str, Any]:
    supply = profile_pack.get("property_profile_buckets", {}) or {}
    demand = profile_pack.get("agent_profile_buckets", {}) or {}
    graph = _build_compatibility_graph(profile_pack)
    edge_map: Dict[str, List[Dict[str, Any]]] = {}
    for edge in graph.get("supply_to_demand", []):
        supply_bucket_id = str(edge.get("supply_bucket_id", "")).strip()
        if not supply_bucket_id:
            continue
        edge_map.setdefault(supply_bucket_id, []).append(edge)

    mode = str(experiment_mode or "abundant")
    rows: List[Dict[str, Any]] = []
    uncovered_primary_supply_buckets: List[str] = []
    for supply_bucket_id, bucket in supply.items():
        if not isinstance(bucket, dict):
            continue
        by_mode = bucket.get("count_by_supply_mode", {}) or {}
        selected_count = int(by_mode.get(mode, by_mode.get("abundant", 0)) or 0)
        reverse_primary_required = bool(bucket.get("reverse_primary_required", True))
        edges = edge_map.get(str(supply_bucket_id), [])
        primary = [edge for edge in edges if str(edge.get("relation_type", "primary")) == "primary"]
        secondary = [edge for edge in edges if str(edge.get("relation_type", "primary")) != "primary"]
        primary_demand_buckets = [str(edge.get("demand_bucket_id", "")) for edge in primary]
        secondary_demand_buckets = [str(edge.get("demand_bucket_id", "")) for edge in secondary]
        primary_demand_agent_count = int(
            sum(int((demand.get(bucket_id, {}) or {}).get("count", 0) or 0) for bucket_id in primary_demand_buckets)
        )
        reverse_covered = bool(primary_demand_buckets or secondary_demand_buckets)
        if selected_count > 0 and reverse_primary_required and not primary_demand_buckets:
            uncovered_primary_supply_buckets.append(str(supply_bucket_id))
        rows.append(
            {
                "supply_bucket_id": str(supply_bucket_id),
                "count_selected_mode": selected_count,
                "reverse_primary_required": reverse_primary_required,
                "primary_demand_bucket_count": len(primary_demand_buckets),
                "secondary_demand_bucket_count": len(secondary_demand_buckets),
                "primary_demand_agent_count": primary_demand_agent_count,
                "primary_demand_buckets": primary_demand_buckets,
                "secondary_demand_buckets": secondary_demand_buckets,
                "reverse_covered": reverse_covered,
            }
        )
    rows.sort(key=lambda item: item["supply_bucket_id"])
    return {
        "ok": len(uncovered_primary_supply_buckets) == 0,
        "experiment_mode": mode,
        "uncovered_primary_supply_buckets": uncovered_primary_supply_buckets,
        "rows": rows,
    }


def build_graph_consistency_report(profile_pack: Dict[str, Any]) -> Dict[str, Any]:
    demand = profile_pack.get("agent_profile_buckets", {}) or {}
    supply = profile_pack.get("property_profile_buckets", {}) or {}
    graph = _build_compatibility_graph(profile_pack)
    demand_edges = graph.get("demand_to_supply", [])
    supply_edges = graph.get("supply_to_demand", [])

    missing_primary_supply_buckets: List[str] = []
    missing_primary_demand_buckets: List[str] = []
    invalid_edges: List[str] = []
    missing_reverse_edges: List[str] = []
    missing_forward_edges: List[str] = []
    unaffordable_primary_edges: List[Dict[str, Any]] = []

    reverse_lookup = {
        (str(edge.get("supply_bucket_id", "")), str(edge.get("demand_bucket_id", "")), str(edge.get("relation_type", "")))
        for edge in supply_edges
    }
    forward_lookup = {
        (str(edge.get("demand_bucket_id", "")), str(edge.get("supply_bucket_id", "")), str(edge.get("relation_type", "")))
        for edge in demand_edges
    }

    for demand_bucket_id, bucket in demand.items():
        if not isinstance(bucket, dict):
            continue
        if int(bucket.get("count", 0) or 0) <= 0:
            continue
        primary_edges = [
            edge
            for edge in demand_edges
            if str(edge.get("demand_bucket_id", "")) == str(demand_bucket_id)
            and str(edge.get("relation_type", "primary")) == "primary"
        ]
        if not primary_edges:
            missing_primary_supply_buckets.append(str(demand_bucket_id))
        buyer_max_price = _safe_range((bucket.get("budget_profile", {}) or {}).get("max_price_range"))
        for edge in primary_edges:
            supply_bucket_id = str(edge.get("supply_bucket_id", ""))
            p_bucket = supply.get(supply_bucket_id)
            if not isinstance(p_bucket, dict):
                invalid_edges.append(f"missing_supply_bucket:{demand_bucket_id}:{supply_bucket_id}")
                continue
            p_price = _supply_bucket_price_range(p_bucket)
            overlap = _range_overlap(buyer_max_price, p_price)
            min_overlap = float(edge.get("budget_overlap_min", 0.0) or 0.0)
            affordable_any = _affordable_any_supply(buyer_max_price, p_price)
            if (not affordable_any) or (min_overlap > 0.0 and overlap < min_overlap):
                unaffordable_primary_edges.append(
                    {
                        "demand_bucket_id": str(demand_bucket_id),
                        "supply_bucket_id": supply_bucket_id,
                        "overlap_width": round(float(overlap), 2),
                        "budget_overlap_min": round(float(min_overlap), 2),
                        "buyer_budget_ceiling": round(float(_buyer_budget_ceiling(buyer_max_price)), 2),
                        "supply_price_low": round(float(p_price[0]), 2),
                    }
                )
            reverse_key = (supply_bucket_id, str(demand_bucket_id), str(edge.get("relation_type", "primary")))
            if reverse_key not in reverse_lookup:
                missing_reverse_edges.append(
                    f"missing_reverse_edge:{demand_bucket_id}:{supply_bucket_id}:{edge.get('relation_type', 'primary')}"
                )

    for supply_bucket_id, bucket in supply.items():
        if not isinstance(bucket, dict):
            continue
        selected_count = int((bucket.get("count_by_supply_mode", {}) or {}).get("abundant", 0) or 0)
        reverse_primary_required = bool(bucket.get("reverse_primary_required", True))
        if selected_count <= 0:
            continue
        primary_edges = [
            edge
            for edge in supply_edges
            if str(edge.get("supply_bucket_id", "")) == str(supply_bucket_id)
            and str(edge.get("relation_type", "primary")) == "primary"
        ]
        if reverse_primary_required and not primary_edges:
            missing_primary_demand_buckets.append(str(supply_bucket_id))
        for edge in primary_edges:
            demand_bucket_id = str(edge.get("demand_bucket_id", ""))
            if demand_bucket_id not in demand:
                invalid_edges.append(f"missing_demand_bucket:{supply_bucket_id}:{demand_bucket_id}")
                continue
            forward_key = (demand_bucket_id, str(supply_bucket_id), str(edge.get("relation_type", "primary")))
            if forward_key not in forward_lookup:
                missing_forward_edges.append(
                    f"missing_forward_edge:{supply_bucket_id}:{demand_bucket_id}:{edge.get('relation_type', 'primary')}"
                )

    return {
        "ok": not (
            missing_primary_supply_buckets
            or missing_primary_demand_buckets
            or invalid_edges
            or unaffordable_primary_edges
            or missing_reverse_edges
            or missing_forward_edges
        ),
        "demand_edge_count": len(demand_edges),
        "supply_edge_count": len(supply_edges),
        "missing_primary_supply_buckets": missing_primary_supply_buckets,
        "missing_primary_demand_buckets": missing_primary_demand_buckets,
        "invalid_edges": invalid_edges,
        "missing_reverse_edges": missing_reverse_edges,
        "missing_forward_edges": missing_forward_edges,
        "unaffordable_primary_edges": unaffordable_primary_edges,
    }


def build_governance_snapshot(
    *,
    profile_pack: Dict[str, Any],
    profile_pack_path: str,
    experiment_mode: str,
    seed: int,
    group_id: str,
    months: int,
    agent_count: int,
) -> Dict[str, Any]:
    demand_library = build_demand_library(profile_pack)
    supply_library = build_supply_library(profile_pack, experiment_mode=experiment_mode)
    budget_report = build_budget_consistency_report(profile_pack)
    competition_report = build_competition_control_report(profile_pack)
    runtime_parent_report = build_runtime_parent_bucket_report(profile_pack)
    reverse_coverage_report = build_reverse_coverage_report(profile_pack, experiment_mode=experiment_mode)
    graph_consistency_report = build_graph_consistency_report(profile_pack)
    compatibility_graph = _build_compatibility_graph(profile_pack)
    identity = {
        "profile_pack_path": str(profile_pack_path),
        "experiment_mode": str(experiment_mode),
        "seed": int(seed),
        "group": str(group_id),
        "months": int(months),
        "agent_count": int(agent_count),
    }
    identity_hash = hashlib.sha256(
        json.dumps(identity, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]
    return {
        "identity": identity,
        "identity_hash": identity_hash,
        "demand_library": demand_library,
        "supply_library": supply_library,
        "budget_consistency_report": budget_report,
        "competition_control_report": competition_report,
        "runtime_parent_bucket_report": runtime_parent_report,
        "compatibility_graph_summary": {
            "demand_to_supply_edge_count": len(compatibility_graph.get("demand_to_supply", [])),
            "supply_to_demand_edge_count": len(compatibility_graph.get("supply_to_demand", [])),
        },
        "reverse_coverage_report": reverse_coverage_report,
        "graph_consistency_report": graph_consistency_report,
    }
