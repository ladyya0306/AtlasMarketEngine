#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Static validator for line-B profiled market pack.
Checks demand/supply mapping, budget overlap, and competition pressure before costly runs.
"""

from __future__ import annotations

import argparse
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


def _supply_bucket_price_range(s_bucket: Dict[str, Any]) -> Tuple[float, float]:
    if not isinstance(s_bucket, dict):
        return 0.0, 0.0
    direct = _safe_range(s_bucket.get("price_range"))
    if direct != (0.0, 0.0):
        return direct
    market_profile = s_bucket.get("market_profile", {}) or {}
    if isinstance(market_profile, dict):
        nested = _safe_range(market_profile.get("price_range"))
        if nested != (0.0, 0.0):
            return nested
    return 0.0, 0.0


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


def load_profile_pack(path: Path) -> Dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        return {}
    pack = payload.get("profiled_market_mode", payload)
    return pack if isinstance(pack, dict) else {}


def validate_profile_pack(pack: Dict[str, Any]) -> Dict[str, Any]:
    demand = pack.get("agent_profile_buckets", {}) or {}
    supply = pack.get("property_profile_buckets", {}) or {}
    rules = pack.get("bucket_alignment_rules", []) or []
    rule_map = {
        str(r.get("agent_bucket_id", "")).strip(): r
        for r in rules
        if isinstance(r, dict) and str(r.get("agent_bucket_id", "")).strip()
    }
    errors: List[str] = []
    warnings: List[str] = []
    budget_coverage: Dict[str, Any] = {}
    competition_pressure: Dict[str, Dict[str, float]] = {"abundant": {}, "scarce": {}}

    for demand_bucket_id, demand_bucket in demand.items():
        if not isinstance(demand_bucket, dict):
            continue
        count = int(demand_bucket.get("count", 0) or 0)
        if count <= 0:
            continue
        rule = rule_map.get(str(demand_bucket_id))
        if not rule:
            errors.append(f"missing_alignment_rule:{demand_bucket_id}")
            continue
        eligible = [str(x) for x in (rule.get("eligible_property_buckets", []) or []) if str(x).strip()]
        soft = [str(x) for x in (rule.get("soft_property_buckets", []) or []) if str(x).strip()]
        all_refs = eligible + soft
        missing_supply = [bid for bid in all_refs if bid not in supply]
        if missing_supply:
            errors.append(f"missing_supply_bucket:{demand_bucket_id}:{','.join(missing_supply)}")

        max_price_range = _safe_range((demand_bucket.get("budget_profile", {}) or {}).get("max_price_range"))
        overlap_total = 0.0
        affordable_any_positive = False
        supply_ranges = []
        abundant_supply = 0
        scarce_supply = 0
        for sbid in eligible:
            s_bucket = supply.get(sbid, {}) or {}
            s_range = _supply_bucket_price_range(s_bucket)
            supply_ranges.append({"bucket_id": sbid, "price_range": [s_range[0], s_range[1]]})
            overlap_total += _range_overlap(max_price_range, s_range)
            affordable_any_positive = affordable_any_positive or _affordable_any_supply(max_price_range, s_range)
            by_mode = s_bucket.get("count_by_supply_mode", {}) or {}
            abundant_supply += int(by_mode.get("abundant", 0) or 0)
            scarce_supply += int(by_mode.get("scarce", 0) or 0)
        budget_ok = affordable_any_positive
        if not budget_ok:
            errors.append(f"budget_mismatch:{demand_bucket_id}")
        budget_coverage[demand_bucket_id] = {
            "buyer_count": count,
            "buyer_max_price_range": [max_price_range[0], max_price_range[1]],
            "eligible_supply_ranges": supply_ranges,
            "budget_overlap_positive": budget_ok,
            "target_overlap_width": round(float(overlap_total), 2),
        }

        if abundant_supply <= 0:
            errors.append(f"no_abundant_supply:{demand_bucket_id}")
        if scarce_supply <= 0:
            warnings.append(f"no_scarce_supply:{demand_bucket_id}")
        competition_pressure["abundant"][str(demand_bucket_id)] = float(count) / float(max(1, abundant_supply))
        competition_pressure["scarce"][str(demand_bucket_id)] = float(count) / float(max(1, scarce_supply))

    return {
        "ok": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "budget_coverage": budget_coverage,
        "competition_pressure": competition_pressure,
        "demand_bucket_count": len([k for k, v in demand.items() if isinstance(v, dict) and int(v.get("count", 0) or 0) > 0]),
        "supply_bucket_count": len([k for k, v in supply.items() if isinstance(v, dict)]),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate line-B profiled market pack.")
    parser.add_argument("--profile-pack-path", required=True)
    parser.add_argument("--out-path", default="")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    pack_path = Path(args.profile_pack_path).resolve()
    pack = load_profile_pack(pack_path)
    report = validate_profile_pack(pack)
    report["profile_pack_path"] = str(pack_path)

    if args.out_path:
        out_path = Path(args.out_path)
        if not out_path.is_absolute():
            out_path = (Path(__file__).resolve().parents[1] / out_path).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"validation_report={out_path}")
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2))

    if args.strict and not bool(report.get("ok", False)):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
