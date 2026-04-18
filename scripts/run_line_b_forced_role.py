#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
课题线 B：强制角色模式批跑 / 判闸 / 归档脚本

目标：
1. 固定走共享交易主链，只在角色注入层施加强制配额。
2. 对 V1 / V2 / V3 进行 1个月门槛验证，并输出 PASS / FAIL。
3. 将结果复制归档到 results/line_b_forced_role/，保留原始 run_*。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.validate_line_b_profile_pack import load_profile_pack, validate_profile_pack
from scripts.activation_governance import (
    build_activation_governance_config,
    evaluate_mismatch_gate,
    export_monthly_activation_funnel,
    export_monthly_bucket_funnel,
    render_governance_summary,
    write_mismatch_gate,
)
from scripts.line_b_library_builder import (
    build_governance_snapshot,
    load_profile_pack_from_path,
)

BASELINE = ROOT / "config" / "baseline.yaml"
NIGHT_RUN = ROOT / "scripts" / "night_run.py"
RESULTS_ROOT = ROOT / "results"
LINE_B_ROOT = RESULTS_ROOT / "line_b_forced_role"

ROLE_NAMES = ("BUYER", "SELLER", "BUYER_SELLER")
ERROR_PATTERNS = (
    "Connection error",
    "circuit_open",
    "Async LLM breaker open",
)
FINANCE_FAILURE_TOKENS = (
    "AFFORD",
    "CASH",
    "MORTGAGE",
    "DTI",
    "PREQUAL",
    "LIQUIDITY",
    "DOWN_PAYMENT",
    "LEVERAGE",
    "FEE",
)
INIT_SUPPLY_SNAPSHOT_RE = re.compile(
    r"Init supply coverage snapshot:\s+"
    r"A_owned=(?P<a_owned>\d+)\s+"
    r"B_owned=(?P<b_owned>\d+)\s+"
    r"A_for_sale=(?P<a_for_sale>\d+)\s+"
    r"B_for_sale=(?P<b_for_sale>\d+)\s+"
    r"tradable=(?P<tradable>\d+)\s+\|\s+"
    r"targets\("
    r"A_owner=(?P<a_owner_target>\d+),\s*"
    r"B_owner=(?P<b_owner_target>\d+),\s*"
    r"A_for_sale=(?P<a_for_sale_target>\d+),\s*"
    r"B_for_sale=(?P<b_for_sale_target>\d+),\s*"
    r"tradable=(?P<tradable_target>\d+)"
    r"\)"
)

GROUP_SPECS: Dict[str, Dict[str, Any]] = {
    "V1": {
        "income_adjustment_rate": 1.00,
        "macro_mode": "stable",
        "initial_listing_rate": 0.05,
        "market_pulse_enabled": False,
        "seed_existing_mortgage_ratio": 0.55,
        "mortgage": {
            "down_payment_ratio": 0.30,
            "max_dti_ratio": 0.50,
            "annual_interest_rate": 0.035,
        },
        "forced_role_quota_base": {
            "buyer": 8,
            "seller": 8,
            "buyer_seller": 4,
        },
    },
    "V2": {
        "income_adjustment_rate": 0.98,
        "macro_mode": "stable",
        "initial_listing_rate": 0.18,
        "market_pulse_enabled": False,
        "seed_existing_mortgage_ratio": 0.72,
        "mortgage": {
            "down_payment_ratio": 0.30,
            "max_dti_ratio": 0.50,
            "annual_interest_rate": 0.036,
        },
        "forced_role_quota_base": {
            "buyer": 4,
            "seller": 12,
            "buyer_seller": 2,
        },
    },
    "V3": {
        "income_adjustment_rate": 1.18,
        "macro_mode": "optimistic",
        "initial_listing_rate": 0.02,
        "market_pulse_enabled": False,
        "seed_existing_mortgage_ratio": 0.46,
        "mortgage": {
            "down_payment_ratio": 0.18,
            "max_dti_ratio": 0.68,
            "annual_interest_rate": 0.0315,
        },
        "forced_role_quota_base": {
            "buyer": 12,
            "seller": 4,
            "buyer_seller": 6,
        },
    },
}


def _safe_json_loads(raw_value: Any, default: Any) -> Any:
    if raw_value in (None, "", b""):
        return default
    if isinstance(raw_value, (dict, list)):
        return raw_value
    try:
        return json.loads(raw_value)
    except Exception:
        return default


def _extract_db_from_stdout(stdout_path: Path) -> Optional[str]:
    if not stdout_path.exists():
        return None
    for line in reversed(stdout_path.read_text(encoding="utf-8", errors="ignore").splitlines()):
        if "DB path:" in line:
            return line.split("DB path:", 1)[1].strip()
    return None


def _write_yaml(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(data, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return data if isinstance(data, dict) else {}


def _safe_div(numerator: float, denominator: float) -> float:
    if float(denominator or 0.0) <= 0.0:
        return 0.0
    return float(numerator or 0.0) / float(denominator)


def _infer_shock_type(batch_dir: Path, explicit: str = "") -> str:
    if explicit:
        return str(explicit)
    name = batch_dir.name.lower()
    if "inject" in name and "supply" in name:
        return "expand_supply"
    if "recover" in name and "supply" in name:
        return "contract_supply"
    if "income" in name:
        return "income"
    return "none"


def _resolve_gate_profile(shock_type: str, requested: str) -> str:
    if requested and requested != "auto":
        return str(requested)
    if str(shock_type) == "expand_supply":
        return "directional_expand_supply"
    if str(shock_type) == "income":
        return "directional_income"
    return "boundary"


def _seller_score(metrics: Dict[str, Any]) -> int:
    r_order = float(metrics.get("r_order", 0.0) or 0.0)
    order_density = float(metrics.get("order_density_m1", 0.0) or 0.0)
    avg_buyers = float(metrics.get("avg_buyers_per_ordered_property_m1", 0.0) or 0.0)
    crowded_ratio = float(metrics.get("crowded_property_ratio_m1", 0.0) or 0.0)
    checks = (
        bool(r_order >= 0.5),
        bool(order_density >= 0.8),
        bool(avg_buyers >= 2.0 or crowded_ratio >= 0.5),
    )
    return int(sum(1 for x in checks if bool(x)))


def _buyer_score(metrics: Dict[str, Any]) -> int:
    r_order = float(metrics.get("r_order", 0.0) or 0.0)
    order_density = float(metrics.get("order_density_m1", 0.0) or 0.0)
    avg_buyers = float(metrics.get("avg_buyers_per_ordered_property_m1", 0.0) or 0.0)
    crowded_ratio = float(metrics.get("crowded_property_ratio_m1", 0.0) or 0.0)
    checks = (
        bool(r_order < 1.0),
        bool(order_density < 0.8),
        bool(avg_buyers < 3.5 and crowded_ratio < 0.75),
    )
    return int(sum(1 for x in checks if bool(x)))


def _load_baseline_metrics_map(baseline_batch_dir: Optional[Path]) -> Dict[Tuple[str, int], Dict[str, Any]]:
    if baseline_batch_dir is None:
        return {}
    summary_path = baseline_batch_dir / "batch_summary.json"
    if not summary_path.exists():
        return {}
    payload = _safe_json_loads(summary_path.read_text(encoding="utf-8"), {})
    runs = payload.get("runs", []) if isinstance(payload, dict) else []
    result: Dict[Tuple[str, int], Dict[str, Any]] = {}
    for run in runs:
        if not isinstance(run, dict):
            continue
        group = str(run.get("group", "") or "")
        seed = int(run.get("seed", 0) or 0)
        metrics = run.get("metrics", {}) or {}
        if group:
            result[(group, seed)] = dict(metrics)
    return result


def _parse_init_supply_snapshot(run_dir: Optional[Path]) -> Dict[str, Any]:
    if run_dir is None:
        return {}
    log_path = run_dir / "simulation_run.log"
    if not log_path.exists():
        return {}

    text = log_path.read_text(encoding="utf-8", errors="ignore")
    match = INIT_SUPPLY_SNAPSHOT_RE.search(text)
    if not match:
        return {}

    values = {key: int(value) for key, value in match.groupdict().items()}
    return {
        "source": "init_supply_coverage_snapshot",
        "log_path": str(log_path.resolve()),
        "l0": int(values["tradable"]),
        "actual": {
            "a_owned": int(values["a_owned"]),
            "b_owned": int(values["b_owned"]),
            "a_for_sale": int(values["a_for_sale"]),
            "b_for_sale": int(values["b_for_sale"]),
            "tradable": int(values["tradable"]),
        },
        "target": {
            "a_owner": int(values["a_owner_target"]),
            "b_owner": int(values["b_owner_target"]),
            "a_for_sale": int(values["a_for_sale_target"]),
            "b_for_sale": int(values["b_for_sale_target"]),
            "tradable": int(values["tradable_target"]),
        },
    }


def _count_errors(stderr_path: Path) -> Dict[str, int]:
    text = ""
    if stderr_path.exists():
        text = stderr_path.read_text(encoding="utf-8", errors="ignore")
    counts = {
        "connection_error": int(text.count(ERROR_PATTERNS[0])),
        "circuit_open": int(text.count(ERROR_PATTERNS[1])),
        "async_breaker_open": int(text.count(ERROR_PATTERNS[2])),
    }
    counts["total"] = int(sum(counts.values()))
    return counts


def _fetch_scalar(cur: sqlite3.Cursor, sql: str, params: Iterable[Any] = ()) -> Any:
    cur.execute(sql, tuple(params))
    row = cur.fetchone()
    return row[0] if row else None


def _fetch_top_counts(
    cur: sqlite3.Cursor,
    sql: str,
    params: Iterable[Any] = (),
) -> Dict[str, int]:
    cur.execute(sql, tuple(params))
    rows = cur.fetchall() or []
    result: Dict[str, int] = {}
    for key, count in rows:
        label = str(key) if key not in (None, "") else "UNKNOWN"
        result[label] = int(count or 0)
    return result


def _scale_quota(raw_quota: Dict[str, Any], agent_count: int) -> Dict[str, int]:
    base_agents = 50.0
    ratio = max(0.0, float(agent_count) / base_agents)
    scaled: Dict[str, int] = {}
    for key in ("buyer", "seller", "buyer_seller"):
        value = int(raw_quota.get(key, 0) or 0)
        scaled[key] = max(0, int(round(value * ratio)))
    return scaled


def build_plan(group_id: str, seed: int, months: int, agent_count: int) -> Dict[str, Any]:
    if group_id not in GROUP_SPECS:
        raise ValueError(f"Unsupported group: {group_id}")
    spec = GROUP_SPECS[group_id]
    quota = _scale_quota(spec["forced_role_quota_base"], agent_count=agent_count)
    return {
        "simulation": {
            "random_seed": int(seed),
            "months": int(months),
            "enable_intervention_panel": False,
            "agent": {
                "income_adjustment_rate": float(spec["income_adjustment_rate"]),
            },
        },
        "macro_environment": {
            "override_mode": str(spec["macro_mode"]),
        },
        "mortgage": dict(spec["mortgage"]),
        "market": {
            "initial_listing_rate": float(spec["initial_listing_rate"]),
        },
        "market_pulse": {
            "enabled": bool(spec["market_pulse_enabled"]),
            "seed_existing_mortgage_ratio": float(spec["seed_existing_mortgage_ratio"]),
        },
        "smart_agent": {
            "activation_batch_size": 10,
            "role_decision_optimization": {
                "adaptive_batch_size_enabled": False,
                "enable_model_dual_routing": False,
                "default_model_type": "smart",
                "gray_model_type": "smart",
            },
            "info_delay_enabled": False,
            "info_delay_ratio": 0.00,
            "info_delay_max_months": 0,
            "activation_governance": build_activation_governance_config(
                activation_mode="forced",
                gate_mode="warn",
            ),
            "forced_role_mode": {
                "enabled": True,
                "apply_months": list(range(1, int(months) + 1)),
                "selection_policy": "affordability_inventory_balanced",
                "allow_force_locked_buyers": True,
                "quota": dict(quota),
            },
        },
        "line_b_metadata": {
            "track": "line_b_forced_role",
            "group": str(group_id),
            "seed": int(seed),
            "months": int(months),
            "agent_count": int(agent_count),
            "activation_governance": {
                "activation_mode": "forced",
                "gate_mode": "warn",
            },
            "forced_role_mode": {
                "quota": dict(quota),
            },
        },
    }


def apply_overrides(plan: Dict[str, Any], args) -> Dict[str, Any]:
    simulation_cfg = plan.setdefault("simulation", {})
    agent_cfg = simulation_cfg.setdefault("agent", {})
    if getattr(args, "income_adjustment_rate", None) is not None:
        agent_cfg["income_adjustment_rate"] = float(args.income_adjustment_rate)
        income_scale = float(args.income_adjustment_rate)
        agent_cfg["external_shock_operator"] = {
            "enabled": True,
            "shock_type": "income",
            "income_scale": float(income_scale),
            "cash_scale": float(income_scale),
            "max_price_scale": float(income_scale),
            "psychological_price_scale": float(income_scale),
            "payment_tolerance_scale": float(max(0.5, min(1.8, income_scale))),
            "down_payment_tolerance_scale": float(max(0.5, min(1.8, income_scale))),
        }

    market_cfg = plan.setdefault("market", {})
    if args.initial_listing_rate is not None:
        market_cfg["initial_listing_rate"] = float(args.initial_listing_rate)

    smart_agent_cfg = plan.setdefault("smart_agent", {})
    activation_governance_cfg = smart_agent_cfg.setdefault(
        "activation_governance",
        build_activation_governance_config(activation_mode="forced", gate_mode="warn"),
    )
    activation_governance_cfg.update(
        build_activation_governance_config(
            activation_mode=str(getattr(args, "activation_mode", "forced") or "forced"),
            gate_mode=str(getattr(args, "governance_gate_mode", "warn") or "warn"),
            profiled_market_required=bool(getattr(args, "governance_profiled_required", False)),
            hard_bucket_matcher_required=bool(getattr(args, "governance_hard_bucket_required", False)),
            hybrid_floor_enabled=bool(getattr(args, "governance_hybrid_floor_enabled", False)),
            hybrid_floor_strategy="bucket_targeted_llm_first",
            autofill_supply_floor=int(getattr(args, "governance_autofill_supply_floor", 0) or 0),
            autofill_demand_floor=int(getattr(args, "governance_autofill_demand_floor", 0) or 0),
            severe_bucket_deficit_ratio=float(
                getattr(args, "governance_severe_bucket_deficit_ratio", 5.0) or 5.0
            ),
            pause_on_severe_mismatch=bool(
                str(getattr(args, "governance_gate_mode", "warn") or "warn").strip().lower() == "pause"
            ),
            emit_bucket_funnel=True,
        )
    )
    buyer_seller_split_cfg = smart_agent_cfg.setdefault("buyer_seller_intent_split", {})
    if bool(getattr(args, "enable_buyer_seller_intent_split", False)):
        buyer_seller_split_cfg["enabled"] = True
    if bool(getattr(args, "buyer_seller_intent_split_apply_to_forced", False)):
        buyer_seller_split_cfg["apply_to_forced"] = True
    if str(getattr(args, "buyer_seller_intent_split_model_type", "") or "").strip():
        buyer_seller_split_cfg["model_type"] = str(
            getattr(args, "buyer_seller_intent_split_model_type", "fast") or "fast"
        ).strip().lower()
    forced_cfg = smart_agent_cfg.setdefault("forced_role_mode", {})
    resolved_activation_mode = str(activation_governance_cfg.get("activation_mode", "forced") or "forced")
    forced_cfg["enabled"] = bool(resolved_activation_mode == "forced")
    quota_cfg = forced_cfg.setdefault("quota", {})
    if args.quota_buyer is not None:
        quota_cfg["buyer"] = max(0, int(args.quota_buyer))
    if args.quota_seller is not None:
        quota_cfg["seller"] = max(0, int(args.quota_seller))
    if args.quota_buyer_seller is not None:
        quota_cfg["buyer_seller"] = max(0, int(args.quota_buyer_seller))
    if args.init_min_for_sale_floor_zone_a is not None:
        smart_agent_cfg["init_min_for_sale_floor_zone_a"] = max(
            0, int(args.init_min_for_sale_floor_zone_a)
        )
    if args.init_min_for_sale_floor_zone_b is not None:
        smart_agent_cfg["init_min_for_sale_floor_zone_b"] = max(
            0, int(args.init_min_for_sale_floor_zone_b)
        )
    if args.init_min_tradable_floor_total is not None:
        smart_agent_cfg["init_min_tradable_floor_total"] = max(
            0, int(args.init_min_tradable_floor_total)
        )
    if args.init_min_for_sale_ratio_zone_a is not None:
        smart_agent_cfg["init_min_for_sale_ratio_zone_a"] = max(
            0.0, float(args.init_min_for_sale_ratio_zone_a)
        )
    if args.init_min_for_sale_ratio_zone_b is not None:
        smart_agent_cfg["init_min_for_sale_ratio_zone_b"] = max(
            0.0, float(args.init_min_for_sale_ratio_zone_b)
        )
    if args.disable_init_multi_owner_listings:
        smart_agent_cfg["init_multi_owner_listings_enabled"] = False
    if bool(getattr(args, "disable_activation_hard_only_prefilter", False)):
        smart_agent_cfg["activation_hard_only_prefilter"] = False
    if getattr(args, "activation_prefilter_normal_min_cash", None) is not None:
        plan.setdefault("decision_factors", {}).setdefault("activation", {}).setdefault("pre_filter", {}).setdefault("normal", {})
        plan["decision_factors"]["activation"]["pre_filter"]["normal"]["min_cash"] = float(
            args.activation_prefilter_normal_min_cash
        )
    if getattr(args, "activation_prefilter_normal_min_income", None) is not None:
        plan.setdefault("decision_factors", {}).setdefault("activation", {}).setdefault("pre_filter", {}).setdefault("normal", {})
        plan["decision_factors"]["activation"]["pre_filter"]["normal"]["min_income"] = float(
            args.activation_prefilter_normal_min_income
        )
    if bool(getattr(args, "enable_mock_stub_select", False)):
        smart_agent_cfg["mock_stub_select_enabled"] = True
    if bool(getattr(args, "enable_profiled_market_mode", False)):
        profiled_cfg = smart_agent_cfg.setdefault("profiled_market_mode", {})
        profiled_cfg["enabled"] = True
        profile_pack_path = getattr(args, "profile_pack_path", "")
        profile_experiment_mode = getattr(args, "profile_experiment_mode", "")
        profile_background_library_path = getattr(args, "profile_background_library_path", "")
        if profile_pack_path:
            profiled_cfg["profile_pack_path"] = str(profile_pack_path)
        # Governance lock: personas must be generated by deterministic code path in this phase.
        profiled_cfg["persona_generation_mode"] = "code_only"
        if profile_background_library_path:
            profiled_cfg["background_library_path"] = str(profile_background_library_path)
        if profile_experiment_mode:
            profiled_cfg["experiment_mode"] = str(profile_experiment_mode)
        if bool(getattr(args, "enable_hard_bucket_matcher", False)):
            profiled_cfg["hard_bucket_matcher_enabled"] = True
        if bool(getattr(args, "hard_bucket_include_soft_buckets", False)):
            profiled_cfg["hard_bucket_include_soft_buckets"] = True
        if bool(getattr(args, "hard_bucket_require_profiled_buyer", False)):
            profiled_cfg["hard_bucket_require_profiled_buyer"] = True
        if bool(getattr(args, "disable_hard_bucket_strict_unmapped", False)):
            profiled_cfg["hard_bucket_strict_unmapped_property"] = False

    plan.setdefault("line_b_metadata", {}).setdefault("activation_governance", {})
    plan["line_b_metadata"]["activation_governance"].update(
        {
            "activation_mode": resolved_activation_mode,
            "gate_mode": str(activation_governance_cfg.get("gate_mode", "warn") or "warn"),
        }
    )
    plan.setdefault("line_b_metadata", {}).setdefault("override_summary", {})
    override_summary = plan["line_b_metadata"]["override_summary"]
    override_summary["income_adjustment_rate"] = agent_cfg.get("income_adjustment_rate")
    override_summary["external_shock_operator"] = dict(
        agent_cfg.get("external_shock_operator", {}) or {}
    )
    override_summary["initial_listing_rate"] = market_cfg.get("initial_listing_rate")
    override_summary["quota"] = dict(quota_cfg)
    override_summary["init_supply_guard"] = {
        "init_min_for_sale_floor_zone_a": smart_agent_cfg.get("init_min_for_sale_floor_zone_a"),
        "init_min_for_sale_floor_zone_b": smart_agent_cfg.get("init_min_for_sale_floor_zone_b"),
        "init_min_tradable_floor_total": smart_agent_cfg.get("init_min_tradable_floor_total"),
        "init_min_for_sale_ratio_zone_a": smart_agent_cfg.get("init_min_for_sale_ratio_zone_a"),
        "init_min_for_sale_ratio_zone_b": smart_agent_cfg.get("init_min_for_sale_ratio_zone_b"),
        "init_multi_owner_listings_enabled": smart_agent_cfg.get("init_multi_owner_listings_enabled"),
    }
    override_summary["activation_prefilter"] = {
        "hard_only": smart_agent_cfg.get("activation_hard_only_prefilter", None),
        "normal_min_cash": (((plan.get("decision_factors", {}) or {}).get("activation", {}) or {}).get("pre_filter", {}) or {}).get("normal", {}).get("min_cash"),
        "normal_min_income": (((plan.get("decision_factors", {}) or {}).get("activation", {}) or {}).get("pre_filter", {}) or {}).get("normal", {}).get("min_income"),
    }
    override_summary["mock_stub_select_enabled"] = bool(
        smart_agent_cfg.get("mock_stub_select_enabled", False)
    )
    profiled_cfg = smart_agent_cfg.get("profiled_market_mode", {}) or {}
    override_summary["profiled_market_mode"] = {
        "enabled": bool(profiled_cfg.get("enabled", False)),
        "profile_pack_path": profiled_cfg.get("profile_pack_path"),
        "persona_generation_mode": profiled_cfg.get("persona_generation_mode"),
        "background_library_path": profiled_cfg.get("background_library_path"),
        "experiment_mode": profiled_cfg.get("experiment_mode"),
        "hard_bucket_matcher_enabled": bool(profiled_cfg.get("hard_bucket_matcher_enabled", False)),
        "hard_bucket_include_soft_buckets": bool(profiled_cfg.get("hard_bucket_include_soft_buckets", False)),
        "hard_bucket_require_profiled_buyer": bool(profiled_cfg.get("hard_bucket_require_profiled_buyer", False)),
        "hard_bucket_strict_unmapped_property": bool(profiled_cfg.get("hard_bucket_strict_unmapped_property", True)),
    }
    return plan


def _analyze_price_adjustment_rows(rows: List[sqlite3.Row]) -> Dict[str, Any]:
    action_counts = {key: 0 for key in ("A", "B", "C", "D", "E", "F")}
    llm_called = 0
    ef_natural = 0
    ef_rule_fallback = 0
    total = 0
    for row in rows:
        total += 1
        action = str(row["decision"] or "UNKNOWN").upper()
        if action in action_counts:
            action_counts[action] += 1
        llm_flag = bool(row["llm_called"])
        if llm_flag:
            llm_called += 1
        if action not in {"E", "F"}:
            continue
        metrics = _safe_json_loads(row["context_metrics"], {})
        pricing_mode = str((metrics or {}).get("pricing_mode", "") or "").strip().lower()
        if llm_flag and not pricing_mode.startswith("rule"):
            ef_natural += 1
        else:
            ef_rule_fallback += 1
    return {
        "total": int(total),
        "action_counts": action_counts,
        "llm_called_count": int(llm_called),
        "llm_called_ratio": round(_safe_div(llm_called, total), 4),
        "ef_source": {
            "natural_decision": int(ef_natural),
            "rule_fallback": int(ef_rule_fallback),
        },
        "validation_status": "ok" if total > 0 else "no_rows_in_sample",
    }


def analyze_db(db_path: Path, months: int = 1, run_dir: Optional[Path] = None) -> Dict[str, Any]:
    l0_snapshot = _parse_init_supply_snapshot(run_dir)
    if not db_path.exists():
        return {
            "db_exists": False,
            "l0": 0,
            "l0_source": "missing_db",
            "l0_snapshot": l0_snapshot,
            "b0_role": 0,
            "b0_order": 0,
            "r_role": 0.0,
            "r_order": 0.0,
            "matches_m1": 0,
            "orders_m1": 0,
            "transactions_m1": 0,
            "order_density_m1": 0.0,
            "avg_buyers_per_ordered_property_m1": 0.0,
            "crowded_property_ratio_m1": 0.0,
            "role_counts_m1": {},
            "forced_role_counts_m1": {},
            "forced_role_mode_connected": False,
            "forced_role_mode_pure": False,
            "failure_reasons_m1": {},
            "buyer_seller_chain_modes_m1": {},
            "buyer_seller_split_choices_m1": {},
            "normal_seller_price_adjustment_m1": _analyze_price_adjustment_rows([]),
            "normal_seller_price_adjustment_window": _analyze_price_adjustment_rows([]),
        }

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        fallback_l0 = int(
            _fetch_scalar(
                cur,
                "SELECT COUNT(*) FROM properties_market WHERE listing_month=0 AND status='for_sale'",
            )
            or 0
        )
        l0 = int(l0_snapshot.get("l0", fallback_l0) or 0)
        b0_role = int(
            _fetch_scalar(
                cur,
                """
                SELECT COUNT(DISTINCT agent_id)
                FROM decision_logs
                WHERE month=1
                  AND event_type='ROLE_DECISION'
                  AND decision IN ('BUYER','BUYER_SELLER')
                """,
            )
            or 0
        )
        b0_order = int(
            _fetch_scalar(
                cur,
                "SELECT COUNT(DISTINCT buyer_id) FROM transaction_orders WHERE created_month=1",
            )
            or 0
        )
        matches_m1 = int(
            _fetch_scalar(cur, "SELECT COUNT(*) FROM property_buyer_matches WHERE month=1") or 0
        )
        orders_m1 = int(
            _fetch_scalar(cur, "SELECT COUNT(*) FROM transaction_orders WHERE created_month=1") or 0
        )
        ordered_properties_m1 = int(
            _fetch_scalar(
                cur,
                "SELECT COUNT(DISTINCT property_id) FROM transaction_orders WHERE created_month=1",
            )
            or 0
        )
        avg_buyers_per_ordered_property_m1 = float(
            _fetch_scalar(
                cur,
                """
                SELECT AVG(c) FROM (
                    SELECT COUNT(DISTINCT buyer_id) AS c
                    FROM transaction_orders
                    WHERE created_month=1
                    GROUP BY property_id
                )
                """,
            )
            or 0.0
        )
        crowded_property_ratio_m1 = float(
            _fetch_scalar(
                cur,
                """
                SELECT AVG(CASE WHEN c>=2 THEN 1.0 ELSE 0.0 END) FROM (
                    SELECT COUNT(DISTINCT buyer_id) AS c
                    FROM transaction_orders
                    WHERE created_month=1
                    GROUP BY property_id
                )
                """,
            )
            or 0.0
        )
        transactions_m1 = int(
            _fetch_scalar(cur, "SELECT COUNT(*) FROM transactions WHERE month=1") or 0
        )
        role_counts_m1 = _fetch_top_counts(
            cur,
            """
            SELECT decision, COUNT(DISTINCT agent_id)
            FROM decision_logs
            WHERE month=1
              AND event_type='ROLE_DECISION'
              AND decision IN ('BUYER','SELLER','BUYER_SELLER')
            GROUP BY decision
            """,
        )

        cur.execute(
            """
            SELECT agent_id, decision, thought_process, context_metrics
            FROM decision_logs
            WHERE month=1
              AND event_type='ROLE_DECISION'
              AND decision IN ('BUYER','SELLER','BUYER_SELLER')
            """
        )
        forced_role_counts_m1 = {role: 0 for role in ROLE_NAMES}
        forced_role_agent_ids = set()
        active_role_agent_ids = set()
        buyer_seller_split_choices_m1: Dict[str, int] = {}
        for row in cur.fetchall() or []:
            agent_id = int(row["agent_id"] or 0)
            decision = str(row["decision"] or "").upper()
            payload = _safe_json_loads(row["thought_process"], {})
            cur_metrics = _safe_json_loads(row["context_metrics"], {}) if "context_metrics" in row.keys() else {}
            active_role_agent_ids.add((decision, agent_id))
            if str(payload.get("trigger", "") or "").strip() == "forced_role_mode" or str(
                payload.get("_decision_origin", "") or ""
            ).strip() == "forced_role_mode":
                if decision in forced_role_counts_m1 and (decision, agent_id) not in forced_role_agent_ids:
                    forced_role_counts_m1[decision] += 1
                forced_role_agent_ids.add((decision, agent_id))
            split_choice = str(cur_metrics.get("buyer_seller_split_choice", "") or "").strip().lower()
            if split_choice:
                buyer_seller_split_choices_m1[split_choice] = int(
                    buyer_seller_split_choices_m1.get(split_choice, 0) or 0
                ) + 1

        failure_reasons_m1 = _fetch_top_counts(
            cur,
            """
            SELECT COALESCE(NULLIF(TRIM(failure_reason), ''), 'UNKNOWN') AS reason_key, COUNT(*)
            FROM property_buyer_matches
            WHERE month=1
              AND COALESCE(NULLIF(TRIM(failure_reason), ''), '') <> ''
            GROUP BY reason_key
            ORDER BY COUNT(*) DESC, reason_key ASC
            LIMIT 10
            """,
        )
        buyer_seller_chain_modes_m1 = _fetch_top_counts(
            cur,
            """
            SELECT COALESCE(NULLIF(TRIM(chain_mode), ''), 'UNKNOWN') AS chain_mode_key, COUNT(*)
            FROM active_participants
            WHERE month=1 AND role='BUYER_SELLER'
            GROUP BY chain_mode_key
            ORDER BY COUNT(*) DESC, chain_mode_key ASC
            """,
        )

        cur.execute(
            """
            SELECT dl.decision, dl.context_metrics, dl.llm_called
            FROM decision_logs dl
            JOIN agents_static ast ON ast.agent_id = dl.agent_id
            WHERE dl.month=1
              AND dl.event_type='PRICE_ADJUSTMENT'
              AND LOWER(COALESCE(ast.agent_type, 'normal'))='normal'
            """
        )
        price_adjustment_rows = list(cur.fetchall() or [])
        cur.execute(
            """
            SELECT dl.decision, dl.context_metrics, dl.llm_called
            FROM decision_logs dl
            JOIN agents_static ast ON ast.agent_id = dl.agent_id
            WHERE dl.month BETWEEN 1 AND ?
              AND dl.event_type='PRICE_ADJUSTMENT'
              AND LOWER(COALESCE(ast.agent_type, 'normal'))='normal'
            """,
            (int(max(1, months)),),
        )
        price_adjustment_window_rows = list(cur.fetchall() or [])
    finally:
        conn.close()

    forced_total = int(sum(forced_role_counts_m1.values()))
    role_total = int(sum(int(role_counts_m1.get(role, 0) or 0) for role in ROLE_NAMES))
    return {
        "db_exists": True,
        "l0": int(l0),
        "l0_source": str(l0_snapshot.get("source", "db_listing_month0_for_sale_fallback")),
        "l0_snapshot": l0_snapshot,
        "l0_db_fallback": int(fallback_l0),
        "b0_role": int(b0_role),
        "b0_order": int(b0_order),
        "r_role": round(_safe_div(b0_role, l0), 4),
        "r_order": round(_safe_div(b0_order, l0), 4),
        "matches_m1": int(matches_m1),
        "orders_m1": int(orders_m1),
        "transactions_m1": int(transactions_m1),
        "order_density_m1": round(_safe_div(orders_m1, l0), 4),
        "ordered_properties_m1": int(ordered_properties_m1),
        "avg_buyers_per_ordered_property_m1": round(float(avg_buyers_per_ordered_property_m1), 4),
        "crowded_property_ratio_m1": round(float(crowded_property_ratio_m1), 4),
        "role_counts_m1": {k: int(v) for k, v in role_counts_m1.items()},
        "forced_role_counts_m1": {k: int(v) for k, v in forced_role_counts_m1.items()},
        "forced_role_mode_connected": bool(forced_total > 0),
        "forced_role_mode_pure": bool(forced_total == role_total and role_total > 0),
        "failure_reasons_m1": failure_reasons_m1,
        "buyer_seller_chain_modes_m1": buyer_seller_chain_modes_m1,
        "buyer_seller_split_choices_m1": {k: int(v) for k, v in buyer_seller_split_choices_m1.items()},
        "normal_seller_price_adjustment_m1": _analyze_price_adjustment_rows(price_adjustment_rows),
        "normal_seller_price_adjustment_window": _analyze_price_adjustment_rows(price_adjustment_window_rows),
    }


def evaluate_gate(
    group_id: str,
    metrics: Dict[str, Any],
    error_counts: Dict[str, int],
    *,
    shock_type: str = "none",
    gate_profile: str = "boundary",
    baseline_metrics: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    effective_chain_pass = bool(
        int(metrics.get("matches_m1", 0) or 0) > 0 and int(metrics.get("orders_m1", 0) or 0) > 0
    )
    stability_pass = bool(int(error_counts.get("total", 0) or 0) <= 50)
    forced_mode_connected = bool(metrics.get("forced_role_mode_connected", False))
    forced_mode_pure = bool(metrics.get("forced_role_mode_pure", False))

    boundary_pass = True
    boundary_rule = "anchor_only"
    boundary_details: Dict[str, Any] = {"method": "anchor_only"}
    r_order = float(metrics.get("r_order", 0.0) or 0.0)
    order_density = float(metrics.get("order_density_m1", 0.0) or 0.0)
    avg_buyers = float(metrics.get("avg_buyers_per_ordered_property_m1", 0.0) or 0.0)
    crowded_ratio = float(metrics.get("crowded_property_ratio_m1", 0.0) or 0.0)
    if group_id == "V2":
        boundary_rule = "multi_signal_buyer_soft"
        checks = {
            "r_order_lt_1": bool(r_order < 1.0),
            "order_density_lt_0_8": bool(order_density < 0.8),
            "competition_not_overheated": bool(avg_buyers < 3.5 and crowded_ratio < 0.75),
        }
        score = int(sum(1 for v in checks.values() if bool(v)))
        boundary_pass = bool(score >= 2)
        boundary_details = {"method": boundary_rule, "score": score, "checks": checks}
    elif group_id == "V3":
        boundary_rule = "multi_signal_seller_soft"
        checks = {
            "r_order_ge_0_5": bool(r_order >= 0.5),
            "order_density_ge_0_8": bool(order_density >= 0.8),
            "competition_hot": bool(avg_buyers >= 2.0 or crowded_ratio >= 0.5),
        }
        score = int(sum(1 for v in checks.values() if bool(v)))
        boundary_pass = bool(score >= 2)
        boundary_details = {"method": boundary_rule, "score": score, "checks": checks}

    directional_pass = True
    directional_details: Dict[str, Any] = {"method": "not_applicable"}
    if str(gate_profile) in {"directional_expand_supply", "directional_income"}:
        if baseline_metrics is None:
            directional_pass = False
            directional_details = {"method": str(gate_profile), "error": "missing_baseline"}
        else:
            r_order = float(metrics.get("r_order", 0.0) or 0.0)
            r_order_base = float(baseline_metrics.get("r_order", 0.0) or 0.0)
            order_density = float(metrics.get("order_density_m1", 0.0) or 0.0)
            order_density_base = float(baseline_metrics.get("order_density_m1", 0.0) or 0.0)
            seller_score = _seller_score(metrics)
            seller_score_base = _seller_score(baseline_metrics)
            buyer_score = _buyer_score(metrics)
            buyer_score_base = _buyer_score(baseline_metrics)

            checks: Dict[str, Any]
            if str(gate_profile) == "directional_expand_supply":
                if group_id == "V3":
                    checks = {
                        "r_order_weakened_vs_baseline": bool(r_order <= r_order_base),
                        "order_density_weakened_vs_baseline": bool(order_density <= order_density_base),
                        "seller_signal_not_stronger_vs_baseline": bool(seller_score <= seller_score_base),
                    }
                elif group_id == "V2":
                    checks = {
                        "buyer_signal_present_now": bool(_buyer_score(metrics) >= 2),
                        "seller_signal_not_formed_now": bool(_seller_score(metrics) < 2),
                        "r_order_lt_0_8_now": bool(r_order < 0.8),
                    }
                else:
                    checks = {"anchor": True}
                score = int(sum(1 for v in checks.values() if bool(v)))
                directional_pass = bool(score >= 2) if group_id in {"V2", "V3"} else True
            else:
                # Income shock directional gate:
                # V2 should weaken buyer-side signals (toward balance/seller),
                # V3 should strengthen or keep seller-side signals.
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
                    checks = {"anchor": True}
                score = int(sum(1 for v in checks.values() if bool(v)))
                directional_pass = bool(score >= 2) if group_id in {"V2", "V3"} else True
            directional_details = {
                "method": str(gate_profile),
                "score": score,
                "checks": checks,
                "baseline_ref": {
                    "r_order": round(r_order_base, 4),
                    "order_density_m1": round(order_density_base, 4),
                    "seller_score": int(seller_score_base),
                    "buyer_score": int(buyer_score_base),
                },
            }

    profile_pass = bool(boundary_pass if str(gate_profile) == "boundary" else directional_pass)
    overall_pass = bool(
        effective_chain_pass
        and stability_pass
        and forced_mode_connected
        and forced_mode_pure
        and profile_pass
    )
    return {
        "effective_chain_pass": effective_chain_pass,
        "stability_pass": stability_pass,
        "forced_mode_connected": forced_mode_connected,
        "forced_mode_pure": forced_mode_pure,
        "shock_type": str(shock_type),
        "gate_profile": str(gate_profile),
        "boundary_rule": boundary_rule,
        "boundary_details": boundary_details,
        "boundary_pass": boundary_pass,
        "directional_details": directional_details,
        "directional_pass": bool(directional_pass),
        "profile_pass": bool(profile_pass),
        "overall_pass": overall_pass,
    }


def classify_root_causes(group_id: str, metrics: Dict[str, Any], gate: Dict[str, Any]) -> List[str]:
    reasons: List[str] = []
    l0 = int(metrics.get("l0", 0) or 0)
    b0_role = int(metrics.get("b0_role", 0) or 0)
    b0_order = int(metrics.get("b0_order", 0) or 0)
    orders_m1 = int(metrics.get("orders_m1", 0) or 0)
    transactions_m1 = int(metrics.get("transactions_m1", 0) or 0)
    failure_top = {
        str(k).upper(): int(v or 0)
        for k, v in (metrics.get("failure_reasons_m1", {}) or {}).items()
    }
    chain_modes = {
        str(k).lower(): int(v or 0)
        for k, v in (metrics.get("buyer_seller_chain_modes_m1", {}) or {}).items()
    }
    price_adjustment = metrics.get("normal_seller_price_adjustment_m1", {}) or {}

    if not bool(metrics.get("forced_role_mode_connected", False)):
        reasons.append("forced_role_mode_not_connected")
    if not bool(metrics.get("forced_role_mode_pure", False)):
        reasons.append("forced_role_mode_mixed_with_natural_activation")
    if l0 <= 0:
        reasons.append("l0_out_of_window_or_zero")
    if not bool(gate.get("effective_chain_pass", False)):
        reasons.append("chain_invalid_matches_or_orders_zero")
    if b0_role > 0 and b0_order < max(1, round(b0_role * 0.35)):
        reasons.append("role_labels_not_converted_to_effective_orders")
    if any(any(token in failure_key for token in FINANCE_FAILURE_TOKENS) for failure_key in failure_top):
        reasons.append("funding_constraints_suppressed_orders")
    if int(chain_modes.get("sell_first", 0) or 0) > 0 and b0_order < b0_role:
        reasons.append("buyer_seller_sell_first_delayed_b0_order")
    if orders_m1 > 0 and transactions_m1 <= 0:
        reasons.append("chain_effective_but_settlement_lag_not_expanded")
    if str(price_adjustment.get("validation_status", "")) == "no_rows_in_sample":
        reasons.append("normal_price_adjustment_not_observable_in_m1_sample")

    gate_profile = str(gate.get("gate_profile", "boundary") or "boundary")
    if gate_profile == "boundary" and group_id == "V2" and not bool(gate.get("boundary_pass", False)):
        reasons.append("buyer_market_boundary_not_formed")
    if gate_profile == "boundary" and group_id == "V3" and not bool(gate.get("boundary_pass", False)):
        reasons.append("seller_market_boundary_not_formed")

    deduped: List[str] = []
    for item in reasons:
        if item not in deduped:
            deduped.append(item)
    return deduped


def build_single_variable_tuning(group_id: str, metrics: Dict[str, Any], root_causes: List[str]) -> List[str]:
    suggestions: List[str] = []
    if "buyer_market_boundary_not_formed" in root_causes:
        suggestions.append("下调 `smart_agent.forced_role_mode.quota.buyer` 2 个名额，其他旋钮不动。")
    if "seller_market_boundary_not_formed" in root_causes:
        suggestions.append("上调 `smart_agent.forced_role_mode.quota.buyer` 2 个名额，其他旋钮不动。")
    if "l0_out_of_window_or_zero" in root_causes:
        suggestions.append("只调整 `market.initial_listing_rate`，先把 `L0` 拉回可交易窗口。")
    if "funding_constraints_suppressed_orders" in root_causes:
        suggestions.append("只调整 `mortgage.max_dti_ratio` 或首付比例一项，验证资金约束是否过紧。")
    if "buyer_seller_sell_first_delayed_b0_order" in root_causes:
        suggestions.append("只下调 `smart_agent.forced_role_mode.quota.buyer_seller`，减少 sell-first 时滞。")
    if "role_labels_not_converted_to_effective_orders" in root_causes and group_id == "V3":
        suggestions.append("只检查并微调买方配额或 `market.initial_listing_rate`，不要同时改按揭和供给。")
    if "normal_price_adjustment_not_observable_in_m1_sample" in root_causes:
        suggestions.append("保持首批门槛口径不变，但在 3个月确认批次里继续观察普通卖家调价行为。")
    return suggestions


def archive_run_artifacts(
    *,
    batch_dir: Path,
    case_dir: Path,
    db_path: Optional[Path],
    case_name: str,
    passed: bool,
) -> Path:
    archive_root = batch_dir / "archived_runs"
    archive_root.mkdir(parents=True, exist_ok=True)
    run_dir = db_path.parent if db_path and db_path.exists() else None
    suffix = "_pass" if passed else "_fail"
    target_dir = archive_root / f"{case_name}{suffix}"
    if target_dir.exists():
        shutil.rmtree(target_dir, ignore_errors=True)
    target_dir.mkdir(parents=True, exist_ok=True)

    if case_dir.exists():
        shutil.copytree(case_dir, target_dir / "case_artifacts", dirs_exist_ok=True)
    # If run_dir is already inside case_dir (runtime_run mode), avoid duplicate copy.
    run_dir_inside_case = False
    if run_dir and run_dir.exists():
        try:
            run_dir.resolve().relative_to(case_dir.resolve())
            run_dir_inside_case = True
        except Exception:
            run_dir_inside_case = False
    if run_dir and run_dir.exists() and not run_dir_inside_case:
        shutil.copytree(run_dir, target_dir / "run_artifacts", dirs_exist_ok=True)
    return target_dir


def _sync_archive_dir(batch_dir: Path, case_name: str, passed: bool) -> str:
    archive_root = batch_dir / "archived_runs"
    archive_root.mkdir(parents=True, exist_ok=True)
    desired = archive_root / f"{case_name}{'_pass' if passed else '_fail'}"
    alternate = archive_root / f"{case_name}{'_fail' if passed else '_pass'}"

    if desired.exists():
        return str(desired.resolve())
    if alternate.exists():
        if desired.exists():
            shutil.rmtree(desired, ignore_errors=True)
        alternate.rename(desired)
        return str(desired.resolve())
    return ""


def run_case(
    *,
    batch_dir: Path,
    group_id: str,
    seed: int,
    months: int,
    agent_count: int,
    args,
    shock_type: str,
    gate_profile: str,
    baseline_metrics_by_case: Optional[Dict[Tuple[str, int], Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    case_name = f"{group_id}_s{int(seed)}_m{int(months)}_a{int(agent_count)}"
    case_dir = batch_dir / case_name
    case_dir.mkdir(parents=True, exist_ok=True)

    plan = build_plan(group_id=group_id, seed=seed, months=months, agent_count=agent_count)
    plan = apply_overrides(plan, args)
    governance_snapshot_path = ""
    governance_identity_hash = ""
    governance_snapshot: Dict[str, Any] = {}
    mismatch_gate_path = ""
    mismatch_gate: Dict[str, Any] = {}
    profiled_cfg = (
        (plan.get("smart_agent", {}) or {}).get("profiled_market_mode", {}) or {}
    )
    if bool(profiled_cfg.get("enabled", False)):
        profile_pack_path = str(profiled_cfg.get("profile_pack_path", "") or "").strip()
        if profile_pack_path:
            resolved_pack = Path(profile_pack_path)
            if not resolved_pack.is_absolute():
                resolved_pack = (ROOT / resolved_pack).resolve()
            pack = load_profile_pack_from_path(resolved_pack)
            governance_snapshot = build_governance_snapshot(
                profile_pack=pack,
                profile_pack_path=str(resolved_pack),
                experiment_mode=str(profiled_cfg.get("experiment_mode", "abundant") or "abundant"),
                seed=int(seed),
                group_id=str(group_id),
                months=int(months),
                agent_count=int(agent_count),
            )
            governance_snapshot_path = str((case_dir / "governance_snapshot.json").resolve())
            (case_dir / "governance_snapshot.json").write_text(
                json.dumps(governance_snapshot, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            governance_identity_hash = str(governance_snapshot.get("identity_hash", "") or "")
            plan.setdefault("line_b_metadata", {}).setdefault("governance", {})
            plan["line_b_metadata"]["governance"].update(
                {
                    "snapshot_path": governance_snapshot_path,
                    "identity_hash": governance_identity_hash,
                }
            )
    activation_governance_cfg = (
        (plan.get("smart_agent", {}) or {}).get("activation_governance", {}) or {}
    )
    mismatch_gate = evaluate_mismatch_gate(
        governance_snapshot=governance_snapshot,
        activation_governance=activation_governance_cfg,
        profiled_market_enabled=bool(profiled_cfg.get("enabled", False)),
        hard_bucket_matcher_enabled=bool(profiled_cfg.get("hard_bucket_matcher_enabled", False)),
    )
    mismatch_gate_path = write_mismatch_gate(case_dir / "mismatch_gate.json", mismatch_gate)
    print(render_governance_summary(mismatch_gate))
    plan.setdefault("line_b_metadata", {}).setdefault("governance", {})
    plan["line_b_metadata"]["governance"]["mismatch_gate_path"] = mismatch_gate_path
    plan_path = case_dir / "plan.yaml"
    _write_yaml(plan_path, plan)

    stdout_path = case_dir / "stdout.log"
    stderr_path = case_dir / "stderr.log"
    # Keep per-case runtime artifacts (DB/log/report) inside the batch directory,
    # so one batch folder is a self-contained evidence package for external review.
    runtime_dir = case_dir / "runtime_run"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    runtime_db_path = runtime_dir / "simulation.db"
    cmd = [
        sys.executable,
        str(NIGHT_RUN),
        "--base-config",
        str(BASELINE),
        "--plan",
        str(plan_path),
        "--seed",
        str(seed),
        "--months",
        str(months),
        "--agent-count",
        str(agent_count),
        "--db-path",
        str(runtime_db_path),
    ]
    env = dict(os.environ)
    prev_py_path = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(ROOT) if not prev_py_path else f"{ROOT};{prev_py_path}"
    blocked_by_governance = bool(
        str(mismatch_gate.get("gate_mode", "warn") or "warn") == "pause"
        and str(mismatch_gate.get("governance_status", "pass") or "pass") == "block"
    )
    if blocked_by_governance:
        stdout_path.write_text(
            render_governance_summary(mismatch_gate) + "\n[governance] case blocked before simulation.\n",
            encoding="utf-8",
        )
        stderr_path.write_text("", encoding="utf-8")
        proc = subprocess.CompletedProcess(cmd, returncode=2)
    else:
        with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open(
            "w", encoding="utf-8"
        ) as stderr_handle:
            proc = subprocess.run(
                cmd,
                cwd=str(ROOT),
                stdout=stdout_handle,
                stderr=stderr_handle,
                env=env,
            )

    db_raw = _extract_db_from_stdout(stdout_path) or ""
    db_path: Optional[Path] = None
    if db_raw:
        db_path = Path(db_raw)
        if not db_path.is_absolute():
            db_path = (ROOT / db_path).resolve()

    run_dir = db_path.parent if db_path and db_path.exists() else None
    metrics = analyze_db(db_path or Path("__missing__.db"), months=int(months), run_dir=run_dir)
    error_counts = _count_errors(stderr_path)
    baseline_metrics = None
    if baseline_metrics_by_case:
        baseline_metrics = baseline_metrics_by_case.get((str(group_id), int(seed)))
    gate = evaluate_gate(
        group_id=group_id,
        metrics=metrics,
        error_counts=error_counts,
        shock_type=shock_type,
        gate_profile=gate_profile,
        baseline_metrics=baseline_metrics,
    )
    root_causes = classify_root_causes(group_id=group_id, metrics=metrics, gate=gate)
    tuning = build_single_variable_tuning(group_id=group_id, metrics=metrics, root_causes=root_causes)
    archive_dir = archive_run_artifacts(
        batch_dir=batch_dir,
        case_dir=case_dir,
        db_path=db_path,
        case_name=case_name,
        passed=bool(gate["overall_pass"]),
    )
    monthly_activation_funnel_path = export_monthly_activation_funnel(
        db_path=db_path or Path("__missing__.db"),
        months=int(months),
        output_path=case_dir / "monthly_activation_funnel.csv",
        run_id=case_name,
        group_id=str(group_id),
        shock_id=str(shock_type),
        activation_mode=str(activation_governance_cfg.get("activation_mode", "forced") or "forced"),
        governance_status=str(mismatch_gate.get("governance_status", "pass") or "pass"),
    )
    monthly_bucket_funnel_path = export_monthly_bucket_funnel(
        db_path=db_path or Path("__missing__.db"),
        months=int(months),
        output_path=case_dir / "monthly_bucket_funnel.csv",
        run_id=case_name,
        group_id=str(group_id),
        shock_id=str(shock_type),
        activation_mode=str(activation_governance_cfg.get("activation_mode", "forced") or "forced"),
        governance_status=str(mismatch_gate.get("governance_status", "pass") or "pass"),
        governance_snapshot=governance_snapshot,
    )

    metadata_path = run_dir / "metadata.json" if run_dir else None
    return {
        "group": group_id,
        "seed": int(seed),
        "months": int(months),
        "agent_count": int(agent_count),
        "status": "blocked" if blocked_by_governance else ("success" if proc.returncode == 0 else "failed"),
        "exit_code": int(proc.returncode),
        "case_name": case_name,
        "case_dir": str(case_dir.resolve()),
        "plan_path": str(plan_path.resolve()),
        "stdout": str(stdout_path.resolve()),
        "stderr": str(stderr_path.resolve()),
        "db_path": str(db_path.resolve()) if db_path else "",
        "run_dir": str(run_dir.resolve()) if run_dir else "",
        "metadata_path": str(metadata_path.resolve()) if metadata_path and metadata_path.exists() else "",
        "forced_role_mode": dict(plan["smart_agent"]["forced_role_mode"]),
        "activation_governance": dict(activation_governance_cfg),
        "metrics": metrics,
        "error_counts": error_counts,
        "gate": gate,
        "root_causes": root_causes,
        "single_variable_tuning": tuning,
        "archive_dir": str(archive_dir.resolve()),
        "governance_snapshot_path": governance_snapshot_path,
        "governance_identity_hash": governance_identity_hash,
        "mismatch_gate": mismatch_gate,
        "mismatch_gate_path": mismatch_gate_path,
        "monthly_activation_funnel_path": monthly_activation_funnel_path,
        "monthly_bucket_funnel_path": monthly_bucket_funnel_path,
    }


def _collect_case_run(
    batch_dir: Path,
    case_dir: Path,
    *,
    shock_type: str,
    gate_profile: str,
    baseline_metrics_by_case: Optional[Dict[Tuple[str, int], Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    if not case_dir.is_dir():
        return None
    plan_path = case_dir / "plan.yaml"
    stdout_path = case_dir / "stdout.log"
    stderr_path = case_dir / "stderr.log"
    if not plan_path.exists():
        return None

    plan = _load_yaml(plan_path)
    meta = plan.get("line_b_metadata", {}) or {}
    group_id = str(meta.get("group", "") or "")
    if not group_id:
        return None
    seed = int(meta.get("seed", 0) or 0)
    months = int(meta.get("months", 0) or 0)
    agent_count = int(meta.get("agent_count", 0) or 0)
    db_raw = _extract_db_from_stdout(stdout_path) or ""
    db_path: Optional[Path] = None
    if db_raw:
        db_path = Path(db_raw)
        if not db_path.is_absolute():
            db_path = (ROOT / db_path).resolve()

    run_dir = db_path.parent if db_path and db_path.exists() else None
    metrics = analyze_db(db_path or Path("__missing__.db"), months=int(months), run_dir=run_dir)
    error_counts = _count_errors(stderr_path)
    baseline_metrics = None
    if baseline_metrics_by_case:
        baseline_metrics = baseline_metrics_by_case.get((str(group_id), int(seed)))
    gate = evaluate_gate(
        group_id=group_id,
        metrics=metrics,
        error_counts=error_counts,
        shock_type=shock_type,
        gate_profile=gate_profile,
        baseline_metrics=baseline_metrics,
    )
    root_causes = classify_root_causes(group_id=group_id, metrics=metrics, gate=gate)
    tuning = build_single_variable_tuning(group_id=group_id, metrics=metrics, root_causes=root_causes)
    metadata_path = run_dir / "metadata.json" if run_dir else None
    status = "success" if metrics.get("db_exists", False) else "failed"
    archive_dir = _sync_archive_dir(
        batch_dir=batch_dir,
        case_name=case_dir.name,
        passed=bool(gate.get("overall_pass", False)),
    )
    governance_meta = meta.get("governance", {}) if isinstance(meta.get("governance", {}), dict) else {}
    governance_snapshot_path = str(governance_meta.get("snapshot_path", "") or "")
    governance_snapshot = {}
    if governance_snapshot_path:
        snapshot_file = Path(governance_snapshot_path)
        if snapshot_file.exists():
            governance_snapshot = _safe_json_loads(snapshot_file.read_text(encoding="utf-8"), {})
    activation_governance = (
        meta.get("activation_governance", {}) if isinstance(meta.get("activation_governance", {}), dict) else {}
    )
    mismatch_gate_path = str(governance_meta.get("mismatch_gate_path", "") or "")
    mismatch_gate = {}
    if mismatch_gate_path and Path(mismatch_gate_path).exists():
        mismatch_gate = _safe_json_loads(Path(mismatch_gate_path).read_text(encoding="utf-8"), {})
    elif governance_snapshot:
        profiled_cfg = (plan.get("smart_agent", {}) or {}).get("profiled_market_mode", {}) or {}
        mismatch_gate = evaluate_mismatch_gate(
            governance_snapshot=governance_snapshot,
            activation_governance=activation_governance,
            profiled_market_enabled=bool(profiled_cfg.get("enabled", False)),
            hard_bucket_matcher_enabled=bool(profiled_cfg.get("hard_bucket_matcher_enabled", False)),
        )
        mismatch_gate_path = write_mismatch_gate(case_dir / "mismatch_gate.json", mismatch_gate)
    monthly_activation_funnel_path = export_monthly_activation_funnel(
        db_path=db_path or Path("__missing__.db"),
        months=int(months),
        output_path=case_dir / "monthly_activation_funnel.csv",
        run_id=case_dir.name,
        group_id=str(group_id),
        shock_id=str(shock_type),
        activation_mode=str(activation_governance.get("activation_mode", "forced") or "forced"),
        governance_status=str(mismatch_gate.get("governance_status", "pass") or "pass"),
    )
    monthly_bucket_funnel_path = export_monthly_bucket_funnel(
        db_path=db_path or Path("__missing__.db"),
        months=int(months),
        output_path=case_dir / "monthly_bucket_funnel.csv",
        run_id=case_dir.name,
        group_id=str(group_id),
        shock_id=str(shock_type),
        activation_mode=str(activation_governance.get("activation_mode", "forced") or "forced"),
        governance_status=str(mismatch_gate.get("governance_status", "pass") or "pass"),
        governance_snapshot=governance_snapshot,
    )
    if (
        status == "failed"
        and str(mismatch_gate.get("gate_mode", "warn") or "warn") == "pause"
        and str(mismatch_gate.get("governance_status", "pass") or "pass") == "block"
    ):
        status = "blocked"
    return {
        "group": group_id,
        "seed": int(seed),
        "months": int(months),
        "agent_count": int(agent_count),
        "status": status,
        "exit_code": 0 if status == "success" else 1,
        "case_name": case_dir.name,
        "case_dir": str(case_dir.resolve()),
        "plan_path": str(plan_path.resolve()),
        "stdout": str(stdout_path.resolve()),
        "stderr": str(stderr_path.resolve()),
        "db_path": str(db_path.resolve()) if db_path else "",
        "run_dir": str(run_dir.resolve()) if run_dir else "",
        "metadata_path": str(metadata_path.resolve()) if metadata_path and metadata_path.exists() else "",
        "forced_role_mode": dict(plan.get("smart_agent", {}).get("forced_role_mode", {}) or {}),
        "activation_governance": dict(activation_governance),
        "metrics": metrics,
        "error_counts": error_counts,
        "gate": gate,
        "root_causes": root_causes,
        "single_variable_tuning": tuning,
        "archive_dir": archive_dir,
        "governance_snapshot_path": governance_snapshot_path,
        "governance_identity_hash": str(governance_meta.get("identity_hash", "") or ""),
        "mismatch_gate": mismatch_gate,
        "mismatch_gate_path": mismatch_gate_path,
        "monthly_activation_funnel_path": monthly_activation_funnel_path,
        "monthly_bucket_funnel_path": monthly_bucket_funnel_path,
    }


def collect_batch_runs(
    batch_dir: Path,
    *,
    shock_type: str,
    gate_profile: str,
    baseline_metrics_by_case: Optional[Dict[Tuple[str, int], Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    runs: List[Dict[str, Any]] = []
    for child in sorted(batch_dir.iterdir()):
        if not child.is_dir() or child.name == "archived_runs":
            continue
        item = _collect_case_run(
            batch_dir=batch_dir,
            case_dir=child,
            shock_type=shock_type,
            gate_profile=gate_profile,
            baseline_metrics_by_case=baseline_metrics_by_case,
        )
        if item is not None:
            runs.append(item)
    return runs


def build_group_summary(runs: List[Dict[str, Any]]) -> Dict[str, Any]:
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
                "root_causes": {},
                "long_test_ready": True,
            },
        )
        info["total_runs"] += 1
        if bool(run["gate"]["overall_pass"]):
            info["pass_runs"] += 1
            info["pass_seeds"].append(int(run["seed"]))
        else:
            info["failed_runs"] += 1
            info["fail_seeds"].append(int(run["seed"]))
            info["long_test_ready"] = False
        for reason in run.get("root_causes", []) or []:
            info["root_causes"][reason] = int(info["root_causes"].get(reason, 0) or 0) + 1
    return grouped


def write_batch_outputs(
    batch_dir: Path,
    runs: List[Dict[str, Any]],
    months: int,
    seeds: List[int],
    *,
    shock_type: str,
    gate_profile: str,
    baseline_batch_dir: str = "",
) -> None:
    group_summary = build_group_summary(runs)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "batch_type": "line_b_forced_role",
        "months": int(months),
        "seeds": [int(x) for x in seeds],
        "shock_type": str(shock_type),
        "gate_profile": str(gate_profile),
        "baseline_batch_dir": str(baseline_batch_dir or ""),
        "run_count": len(runs),
        "success_count": sum(1 for item in runs if item["status"] == "success"),
        "failed_count": sum(1 for item in runs if item["status"] not in {"success", "blocked"}),
        "blocked_count": sum(1 for item in runs if item["status"] == "blocked"),
        "pass_gate_count": sum(1 for item in runs if bool(item["gate"]["overall_pass"])),
        "fail_gate_count": sum(1 for item in runs if not bool(item["gate"]["overall_pass"])),
        "group_summary": group_summary,
        "runs": runs,
    }
    (batch_dir / "batch_summary.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    lines = [
        "# 课题线B 强制角色批次摘要",
        "",
        f"- 生成时间: {payload['generated_at']}",
        f"- 月数: {int(months)}",
        f"- seeds: {', '.join(str(x) for x in seeds)}",
        f"- shock_type: `{shock_type}`",
        f"- gate_profile: `{gate_profile}`",
        f"- baseline_batch_dir: `{baseline_batch_dir or '-'}`",
        "- L0口径: `simulation_run.log` 的 `Init supply coverage snapshot`；缺失时回退到 `properties_market(listing_month=0,status='for_sale')`",
        "",
        "| 组别 | seed | 状态 | activation_mode | governance | Gate | profile_pass | boundary_pass | directional_pass | L0 | B0_role | B0_order | R_role | R_order | matches | orders | tx | err_total | 根因 |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for run in runs:
        metrics = run["metrics"]
        gate = run["gate"]
        roots = ", ".join(run.get("root_causes", []) or []) or "-"
        activation_governance = run.get("activation_governance", {}) or {}
        mismatch_gate = run.get("mismatch_gate", {}) or {}
        lines.append(
            f"| {run['group']} | {run['seed']} | {run['status']} | "
            f"{activation_governance.get('activation_mode', '-') or '-'} | "
            f"{mismatch_gate.get('governance_status', '-') or '-'} | "
            f"{'PASS' if gate['overall_pass'] else 'FAIL'} | "
            f"{bool(gate.get('profile_pass', False))} | "
            f"{bool(gate.get('boundary_pass', False))} | "
            f"{bool(gate.get('directional_pass', False))} | "
            f"{metrics['l0']} | {metrics['b0_role']} | {metrics['b0_order']} | "
            f"{metrics['r_role']:.4f} | {metrics['r_order']:.4f} | "
            f"{metrics['matches_m1']} | {metrics['orders_m1']} | {metrics['transactions_m1']} | "
            f"{run['error_counts']['total']} | {roots} |"
        )
    (batch_dir / "batch_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run line B forced-role market-state verification.")
    parser.add_argument("--months", type=int, default=1)
    parser.add_argument("--agent-count", type=int, default=50)
    parser.add_argument("--groups", nargs="+", default=["V1", "V2", "V3"])
    parser.add_argument("--seeds", nargs="+", type=int, default=[606, 607, 608])
    parser.add_argument("--out-dir", default="")
    parser.add_argument("--income-adjustment-rate", type=float, default=None)
    parser.add_argument("--initial-listing-rate", type=float, default=None)
    parser.add_argument("--quota-buyer", type=int, default=None)
    parser.add_argument("--quota-seller", type=int, default=None)
    parser.add_argument("--quota-buyer-seller", type=int, default=None)
    parser.add_argument("--init-min-for-sale-floor-zone-a", type=int, default=None)
    parser.add_argument("--init-min-for-sale-floor-zone-b", type=int, default=None)
    parser.add_argument("--init-min-tradable-floor-total", type=int, default=None)
    parser.add_argument("--init-min-for-sale-ratio-zone-a", type=float, default=None)
    parser.add_argument("--init-min-for-sale-ratio-zone-b", type=float, default=None)
    parser.add_argument("--disable-init-multi-owner-listings", action="store_true")
    parser.add_argument("--disable-activation-hard-only-prefilter", action="store_true")
    parser.add_argument("--activation-prefilter-normal-min-cash", type=float, default=None)
    parser.add_argument("--activation-prefilter-normal-min-income", type=float, default=None)
    parser.add_argument("--enable-profiled-market-mode", action="store_true")
    parser.add_argument("--profile-pack-path", default="")
    parser.add_argument("--profile-background-library-path", default="")
    parser.add_argument("--profile-experiment-mode", default="abundant")
    parser.add_argument("--enable-hard-bucket-matcher", action="store_true")
    parser.add_argument("--hard-bucket-include-soft-buckets", action="store_true")
    parser.add_argument("--hard-bucket-require-profiled-buyer", action="store_true")
    parser.add_argument("--disable-hard-bucket-strict-unmapped", action="store_true")
    parser.add_argument("--activation-mode", choices=["forced", "hybrid", "natural"], default="forced")
    parser.add_argument("--governance-gate-mode", choices=["warn", "pause", "autofill"], default="warn")
    parser.add_argument("--governance-profiled-required", action="store_true")
    parser.add_argument("--governance-hard-bucket-required", action="store_true")
    parser.add_argument("--governance-hybrid-floor-enabled", action="store_true")
    parser.add_argument("--governance-autofill-supply-floor", type=int, default=0)
    parser.add_argument("--governance-autofill-demand-floor", type=int, default=0)
    parser.add_argument("--governance-severe-bucket-deficit-ratio", type=float, default=5.0)
    parser.add_argument("--enable-buyer-seller-intent-split", action="store_true")
    parser.add_argument("--buyer-seller-intent-split-apply-to-forced", action="store_true")
    parser.add_argument("--buyer-seller-intent-split-model-type", default="")
    parser.add_argument("--enable-mock-stub-select", action="store_true")
    parser.add_argument("--skip-profile-pack-validation", action="store_true")
    parser.add_argument("--no-stop-on-v3-fail", action="store_true")
    parser.add_argument(
        "--shock-type",
        default="",
        choices=["", "none", "expand_supply", "contract_supply", "income"],
        help="外生冲击类型；为空时按 out-dir 名称自动推断。",
    )
    parser.add_argument(
        "--gate-profile",
        default="auto",
        choices=["auto", "boundary", "directional_expand_supply", "directional_income"],
        help="判闸口径：auto 将按 shock_type 自动映射。",
    )
    parser.add_argument(
        "--baseline-batch-dir",
        default="",
        help="方向性闸门可选 baseline 批次目录（含 batch_summary.json）。",
    )
    args = parser.parse_args()

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if args.out_dir:
        provided_out = Path(args.out_dir).resolve()
        # 固定归档规则：若传入的是“课题根目录/普通目录”，自动创建独立批次子目录；
        # 仅当显式传入 forced_role_batch_* 目录时才直接使用。
        if str(provided_out.name).startswith("forced_role_batch_"):
            batch_dir = provided_out
        else:
            batch_dir = provided_out / f"forced_role_batch_{stamp}"
    else:
        batch_dir = LINE_B_ROOT / f"forced_role_batch_{stamp}"
    batch_dir.mkdir(parents=True, exist_ok=True)

    shock_type = _infer_shock_type(batch_dir=batch_dir, explicit=str(args.shock_type or ""))
    gate_profile = _resolve_gate_profile(shock_type=shock_type, requested=str(args.gate_profile or "auto"))
    baseline_batch_dir = Path(args.baseline_batch_dir).resolve() if str(args.baseline_batch_dir or "").strip() else None
    baseline_metrics_by_case = _load_baseline_metrics_map(baseline_batch_dir)

    if bool(getattr(args, "enable_profiled_market_mode", False)):
        profile_pack_path = str(getattr(args, "profile_pack_path", "") or "").strip()
        if not profile_pack_path:
            raise ValueError("enable_profiled_market_mode=true requires --profile-pack-path")
        pack_path = Path(profile_pack_path)
        if not pack_path.is_absolute():
            pack_path = (ROOT / pack_path).resolve()
        report = validate_profile_pack(load_profile_pack(pack_path))
        report["profile_pack_path"] = str(pack_path)
        validation_path = batch_dir / "profile_pack_validation.json"
        validation_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"[profile-pack] validation_report={validation_path}")
        if not bool(report.get("ok", False)) and not bool(
            getattr(args, "skip_profile_pack_validation", False)
        ):
            print(
                "[profile-pack] validation failed under strict gate. "
                "Fix pack or pass --skip-profile-pack-validation."
            )
            return 2

    runs: List[Dict[str, Any]] = []
    stop_on_v3_fail = not bool(getattr(args, "no_stop_on_v3_fail", False))
    should_stop = False
    for group_id in args.groups:
        if group_id not in GROUP_SPECS:
            raise ValueError(f"Unsupported group: {group_id}")
        for seed in args.seeds:
            run = run_case(
                batch_dir=batch_dir,
                group_id=str(group_id),
                seed=int(seed),
                months=int(args.months),
                agent_count=int(args.agent_count),
                args=args,
                shock_type=shock_type,
                gate_profile=gate_profile,
                baseline_metrics_by_case=baseline_metrics_by_case,
            )
            runs.append(run)
            metrics = run["metrics"]
            gate = run["gate"]
            print(
                f"[{group_id}] seed={seed} status={run['status']} "
                f"gate={'PASS' if gate['overall_pass'] else 'FAIL'} "
                f"L0={metrics['l0']} B0_role={metrics['b0_role']} "
                f"B0_order={metrics['b0_order']} R_order={metrics['r_order']:.4f}"
            )
            if stop_on_v3_fail and str(group_id) == "V3" and not bool(gate.get("overall_pass", False)):
                print("[stop] V3 run failed gate; terminating remaining runs.")
                should_stop = True
                break
        if should_stop:
            break

    all_runs = collect_batch_runs(
        batch_dir,
        shock_type=shock_type,
        gate_profile=gate_profile,
        baseline_metrics_by_case=baseline_metrics_by_case,
    )
    write_batch_outputs(
        batch_dir=batch_dir,
        runs=all_runs,
        months=int(args.months),
        seeds=[int(x) for x in args.seeds],
        shock_type=shock_type,
        gate_profile=gate_profile,
        baseline_batch_dir=str(baseline_batch_dir) if baseline_batch_dir else "",
    )
    print(f"batch={batch_dir}")
    return 0 if all(item["status"] == "success" for item in all_runs) else 1


if __name__ == "__main__":
    raise SystemExit(main())
