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


def _range_overlap(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    left = max(a[0], b[0])
    right = min(a[1], b[1])
    return max(0.0, right - left)


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
                "zone": str(bucket.get("zone", "") or ""),
                "is_school_district": bool(bucket.get("is_school_district", False)),
                "property_type_bucket": str(bucket.get("property_type_bucket", "") or ""),
                "price_range": list(_safe_range(bucket.get("price_range"))),
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
        eligible_rows: List[Dict[str, Any]] = []
        for property_bucket_id in eligible:
            p_bucket = supply.get(property_bucket_id, {}) or {}
            p_price = _safe_range(p_bucket.get("price_range"))
            overlap = _range_overlap(buyer_max_price, p_price)
            overlap_positive = overlap_positive or (overlap > 0.0)
            eligible_rows.append(
                {
                    "property_bucket_id": property_bucket_id,
                    "price_range": [p_price[0], p_price[1]],
                    "overlap_width": round(float(overlap), 2),
                }
            )
        if not overlap_positive:
            failures.append(f"budget_mismatch:{bucket_id}")
        rows.append(
            {
                "agent_bucket_id": str(bucket_id),
                "buyer_count": count,
                "buyer_max_price_range": [buyer_max_price[0], buyer_max_price[1]],
                "eligible_supply": eligible_rows,
                "overlap_positive": bool(overlap_positive),
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
    }
