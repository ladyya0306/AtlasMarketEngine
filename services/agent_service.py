# Moved to local import to avoid circular dependency
# from transaction_engine import generate_seller_listing
import asyncio
import datetime
import hashlib
import json
import logging
import os
import random
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from agent_behavior import (
    apply_event_effects,
    batched_determine_role_async,
    build_activation_lifecycle_packet,
    build_behavior_modifier,
    calculate_activation_probability,
    derive_decision_urgency,
    determine_buyer_seller_chain_mode_async,
    determine_listing_strategy,
    generate_buyer_preference,
    select_monthly_event,
    should_agent_exit_market,
    TIMING_ROLE_BUY_NOW,
    TIMING_ROLE_NEED_WAIT,
    TIMING_ROLE_OBSERVE_WAIT,
    TIMING_ROLE_SELL_THEN_BUY,
    TIMING_ROLE_SELL_NOW,
    VALID_TIMING_ROLES,
    VALID_URGENCY_LEVELS,
)
from config.agent_templates import get_template_for_tier
from config.agent_tiers import AGENT_TIER_CONFIG
from models import Agent
from utils.name_generator import ChineseNameGenerator

logger = logging.getLogger(__name__)


class AgentService:
    def __init__(self, config, db_conn: sqlite3.Connection):
        self.config = config
        self.conn = db_conn
        self.agents: List[Agent] = []
        self.agent_map: Dict[int, Agent] = {}
        self.smart_agent_ids = set()
        self.is_v2 = True  # Default for new runs
        # ROLE_DECISION optimization runtime cache (month-TTL, non-persistent)
        self._role_signature_cache: Dict[str, Dict] = {}
        self._adaptive_activation_batch_size = None
        self._role_budget_month = None
        self._role_budget_used_calls = 0
        self._forced_role_history: List[Dict] = []
        self._last_forced_role_summary: Dict | None = None
        self._profiled_market_pack_cache: Optional[Dict[str, Any]] = None
        self._profiled_market_pack_cache_key: Optional[str] = None
        self._profiled_background_library_cache: Optional[Dict[str, Any]] = None
        self._profiled_background_library_cache_key: Optional[str] = None
        self._activation_serial_mode = self._as_bool(
            os.getenv("ROLE_ACTIVATION_SERIAL_MODE", "false"),
            False,
        )

    @staticmethod
    def _as_bool(value, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    def _role_opt_cfg(self) -> Dict[str, float]:
        """
        Resolve ROLE_DECISION optimization controls.
        These are pre-routing knobs only; they do not replace transaction hard constraints.
        """
        get = self.config.get
        return {
            "enable_uncertainty_router": self._as_bool(
                get("smart_agent.role_decision_optimization.enable_uncertainty_router", True), True
            ),
            "low_score_observer_threshold": float(
                get("smart_agent.role_decision_optimization.low_score_observer_threshold", 0.01)
            ),
            "high_score_candidate_threshold": float(
                get("smart_agent.role_decision_optimization.high_score_candidate_threshold", 0.08)
            ),
            "enable_observer_freeze": self._as_bool(
                get("smart_agent.role_decision_optimization.enable_observer_freeze", True), True
            ),
            "observer_freeze_trigger_streak": int(
                get("smart_agent.role_decision_optimization.observer_freeze_trigger_streak", 3)
            ),
            "observer_freeze_window_months": int(
                get("smart_agent.role_decision_optimization.observer_freeze_window_months", 2)
            ),
            "enable_signature_cache": self._as_bool(
                get("smart_agent.role_decision_optimization.enable_signature_cache", True), True
            ),
            "signature_cache_ttl_months": int(
                get("smart_agent.role_decision_optimization.signature_cache_ttl_months", 1)
            ),
            "signature_cache_random_recheck_ratio": float(
                get("smart_agent.role_decision_optimization.signature_cache_random_recheck_ratio", 0.10)
            ),
            "enable_model_dual_routing": self._as_bool(
                get("smart_agent.role_decision_optimization.enable_model_dual_routing", False),
                False,
            ),
            "gray_score_lower": float(
                get("smart_agent.role_decision_optimization.gray_score_lower", 0.02)
            ),
            "gray_score_upper": float(
                get("smart_agent.role_decision_optimization.gray_score_upper", 0.08)
            ),
            "default_model_type": str(
                get("smart_agent.role_decision_optimization.default_model_type", "fast")
            ).lower(),
            "gray_model_type": str(
                get("smart_agent.role_decision_optimization.gray_model_type", "smart")
            ).lower(),
        }

    def _buyer_seller_intent_split_cfg(self) -> Dict[str, object]:
        raw_cfg = self.config.get(
            "smart_agent.buyer_seller_intent_split",
            self.config.get("buyer_seller_intent_split", {}),
        )
        if not isinstance(raw_cfg, dict):
            raw_cfg = {}
        enabled = self._as_bool(
            self.config.get(
                "smart_agent.buyer_seller_intent_split.enabled",
                raw_cfg.get("enabled", False),
            ),
            False,
        )
        apply_to_forced = self._as_bool(
            self.config.get(
                "smart_agent.buyer_seller_intent_split.apply_to_forced",
                raw_cfg.get("apply_to_forced", False),
            ),
            False,
        )
        model_type = str(
            self.config.get(
                "smart_agent.buyer_seller_intent_split.model_type",
                raw_cfg.get("model_type", "fast"),
            )
            or "fast"
        ).strip().lower()
        decision_profiles_raw = self.config.get(
            "smart_agent.buyer_seller_intent_split.decision_profiles",
            raw_cfg.get("decision_profiles", ["normal", "smart"]),
        )
        if isinstance(decision_profiles_raw, str):
            decision_profiles = {
                token.strip().lower()
                for token in decision_profiles_raw.split(",")
                if token.strip()
            }
        else:
            decision_profiles = {
                str(token or "").strip().lower()
                for token in list(decision_profiles_raw or [])
                if str(token or "").strip()
            }
        if not decision_profiles:
            decision_profiles = {"normal", "smart"}
        return {
            "enabled": bool(enabled),
            "apply_to_forced": bool(apply_to_forced),
            "model_type": model_type if model_type else "fast",
            "decision_profiles": decision_profiles,
        }

    def _should_split_buyer_seller_intent(
        self,
        *,
        decision_origin: str,
        decision_profile: str,
        decision_payload: Dict,
    ) -> bool:
        cfg = self._buyer_seller_intent_split_cfg()
        if not bool(cfg.get("enabled", False)):
            return False
        if str(decision_origin or "").strip() == "forced_role_mode" and not bool(cfg.get("apply_to_forced", False)):
            return False
        if str(decision_profile or "normal").strip().lower() not in set(
            cfg.get("decision_profiles", {"normal", "smart"})
        ):
            return False
        # Only re-ask ambiguous BUYER_SELLER cases. If the first-stage role decision
        # already gave an explicit chain order, asking again tends to over-correct.
        explicit_chain_mode = str((decision_payload or {}).get("chain_mode", "") or "").strip().lower()
        if explicit_chain_mode in {"buy_first", "sell_first"}:
            return False
        timing_role = str((decision_payload or {}).get("timing_role", "") or "").strip().lower()
        return timing_role not in {TIMING_ROLE_BUY_NOW, TIMING_ROLE_SELL_THEN_BUY}

    @staticmethod
    def _normalize_month_list(raw_value) -> list[int]:
        if raw_value in (None, "", []):
            return []
        if isinstance(raw_value, (list, tuple, set)):
            out = []
            for item in raw_value:
                try:
                    out.append(int(item))
                except Exception:
                    continue
            return sorted({int(x) for x in out if int(x) > 0})
        if isinstance(raw_value, str):
            tokens = [part.strip() for part in raw_value.replace(";", ",").split(",")]
            out = []
            for token in tokens:
                if not token:
                    continue
                try:
                    out.append(int(token))
                except Exception:
                    continue
            return sorted({int(x) for x in out if int(x) > 0})
        try:
            value = int(raw_value)
        except Exception:
            return []
        return [value] if value > 0 else []

    def _initial_liquidity_seed_rate(self) -> float:
        try:
            value = float(self.config.get("market.initial_listing_rate", 0.30))
        except Exception:
            value = 0.30
        return max(0.0, min(1.0, float(value)))

    def _min_holding_months_before_resale(self) -> int:
        try:
            value = int(
                self.config.get(
                    "smart_agent.min_holding_months_before_resale",
                    self.config.get("min_holding_months_before_resale", 12),
                )
            )
        except Exception:
            value = 12
        return max(0, int(value))

    def _init_multi_owner_listings_enabled(self) -> bool:
        return self._as_bool(
            self.config.get("smart_agent.init_multi_owner_listings_enabled", True),
            True,
        )

    def _forced_role_mode_cfg(self, month: int) -> Dict[str, object]:
        raw_cfg = self.config.get(
            "smart_agent.forced_role_mode",
            self.config.get("forced_role_mode", {}),
        )
        if not isinstance(raw_cfg, dict):
            raw_cfg = {}

        enabled = self._as_bool(raw_cfg.get("enabled", False), False)
        apply_months = self._normalize_month_list(
            raw_cfg.get("apply_months", raw_cfg.get("months"))
        )
        if apply_months and int(month) not in set(apply_months):
            enabled = False

        quota_cfg = raw_cfg.get("quota", {}) or {}
        if not isinstance(quota_cfg, dict):
            quota_cfg = {}

        def _to_non_negative_int(value, default: int = 0) -> int:
            try:
                return max(0, int(value))
            except Exception:
                return int(default)

        quota = {
            "BUYER": _to_non_negative_int(quota_cfg.get("buyer", 0)),
            "SELLER": _to_non_negative_int(quota_cfg.get("seller", 0)),
            "BUYER_SELLER": _to_non_negative_int(quota_cfg.get("buyer_seller", 0)),
        }
        selection_policy = str(
            raw_cfg.get("selection_policy", "affordability_inventory_balanced")
        ).strip().lower() or "affordability_inventory_balanced"
        allow_force_locked_buyers = self._as_bool(
            raw_cfg.get("allow_force_locked_buyers", True),
            True,
        )
        return {
            "enabled": bool(enabled),
            "quota": quota,
            "selection_policy": selection_policy,
            "apply_months": apply_months,
            "allow_force_locked_buyers": bool(allow_force_locked_buyers),
            "raw": dict(raw_cfg),
        }

    def _activation_governance_cfg(self, month: int) -> Dict[str, Any]:
        raw_cfg = self.config.get(
            "smart_agent.activation_governance",
            self.config.get("activation_governance", {}),
        )
        if not isinstance(raw_cfg, dict):
            raw_cfg = {}
        forced_cfg = self._forced_role_mode_cfg(month=int(month))
        inferred_mode = "forced" if bool(forced_cfg.get("enabled", False)) else "natural"
        activation_mode = str(raw_cfg.get("activation_mode", inferred_mode) or inferred_mode).strip().lower()
        if activation_mode not in {"forced", "hybrid", "natural"}:
            activation_mode = inferred_mode
        gate_mode = str(raw_cfg.get("gate_mode", "warn") or "warn").strip().lower() or "warn"
        if gate_mode not in {"warn", "pause", "autofill"}:
            gate_mode = "warn"
        return {
            "enabled": self._as_bool(raw_cfg.get("enabled", True), True),
            "activation_mode": activation_mode,
            "gate_mode": gate_mode,
            "profiled_market_required": self._as_bool(raw_cfg.get("profiled_market_required", False), False),
            "hard_bucket_matcher_required": self._as_bool(raw_cfg.get("hard_bucket_matcher_required", False), False),
            "hybrid_floor_enabled": self._as_bool(raw_cfg.get("hybrid_floor_enabled", False), False),
            "hybrid_floor_strategy": str(raw_cfg.get("hybrid_floor_strategy", "bucket_targeted_llm_first") or "bucket_targeted_llm_first"),
            "autofill_supply_floor": max(0, int(raw_cfg.get("autofill_supply_floor", 0) or 0)),
            "autofill_demand_floor": max(0, int(raw_cfg.get("autofill_demand_floor", 0) or 0)),
            "severe_bucket_deficit_ratio": float(raw_cfg.get("severe_bucket_deficit_ratio", 5.0) or 5.0),
            "pause_on_severe_mismatch": self._as_bool(raw_cfg.get("pause_on_severe_mismatch", False), False),
            "emit_bucket_funnel": self._as_bool(raw_cfg.get("emit_bucket_funnel", True), True),
            "raw": dict(raw_cfg),
        }

    def _profiled_market_mode_cfg(self) -> Dict[str, Any]:
        raw_cfg = self.config.get(
            "smart_agent.profiled_market_mode",
            self.config.get("profiled_market_mode", {}),
        )
        if not isinstance(raw_cfg, dict):
            raw_cfg = {}
        enabled = self._as_bool(raw_cfg.get("enabled", False), False)
        experiment_mode = str(
            raw_cfg.get(
                "experiment_mode",
                raw_cfg.get("default_experiment_mode", "abundant"),
            )
        ).strip() or "abundant"
        profile_pack_path = str(raw_cfg.get("profile_pack_path", "") or "").strip()
        profile_pack_inline = raw_cfg.get("profile_pack")
        legacy_story_mode = str(raw_cfg.get("story_mode", "") or "").strip().lower()
        persona_generation_mode = str(
            raw_cfg.get("persona_generation_mode", "code_only")
        ).strip().lower() or "code_only"
        if legacy_story_mode:
            # Keep backward compatibility with old config key but force code-only in this phase.
            persona_generation_mode = "code_only"
        if persona_generation_mode != "code_only":
            logger.warning(
                "profiled_market_mode persona_generation_mode=%s is not supported in this phase; force code_only.",
                persona_generation_mode,
            )
            persona_generation_mode = "code_only"
        background_library_path = str(raw_cfg.get("background_library_path", "") or "").strip()
        return {
            "enabled": bool(enabled),
            "experiment_mode": experiment_mode,
            "persona_generation_mode": persona_generation_mode,
            "profile_pack_path": profile_pack_path,
            "background_library_path": background_library_path,
            "profile_pack_inline": profile_pack_inline if isinstance(profile_pack_inline, dict) else {},
            "raw": dict(raw_cfg),
        }

    def _resolve_profiled_market_pack(self, mode_cfg: Dict[str, Any]) -> Dict[str, Any]:
        if not bool(mode_cfg.get("enabled", False)):
            return {}
        inline_pack = mode_cfg.get("profile_pack_inline") or {}
        if isinstance(inline_pack, dict) and inline_pack:
            return self._normalize_profiled_market_pack_definition(inline_pack)

        pack_path = str(mode_cfg.get("profile_pack_path", "") or "").strip()
        if not pack_path:
            return {}
        resolved = Path(pack_path)
        if not resolved.is_absolute():
            resolved = (Path(__file__).resolve().parents[1] / resolved).resolve()
        cache_key = str(resolved)
        if self._profiled_market_pack_cache is not None and self._profiled_market_pack_cache_key == cache_key:
            return dict(self._profiled_market_pack_cache)
        if not resolved.exists():
            logger.warning(f"profiled_market_mode enabled but profile pack not found: {resolved}")
            return {}
        try:
            payload = yaml.safe_load(resolved.read_text(encoding="utf-8")) or {}
            if not isinstance(payload, dict):
                payload = {}
            pack = payload.get("profiled_market_mode", payload)
            if not isinstance(pack, dict):
                pack = {}
            normalized_pack = self._normalize_profiled_market_pack_definition(pack)
            self._profiled_market_pack_cache = dict(normalized_pack)
            self._profiled_market_pack_cache_key = cache_key
            logger.info(f"profiled_market_mode loaded profile pack: {resolved}")
            return dict(normalized_pack)
        except Exception as exc:
            logger.warning(f"Failed loading profiled_market profile pack {resolved}: {exc}")
            return {}

    def _resolve_profiled_background_library(self, mode_cfg: Dict[str, Any]) -> Dict[str, Any]:
        if not bool(mode_cfg.get("enabled", False)):
            return {}
        lib_path = str(mode_cfg.get("background_library_path", "") or "").strip()
        if not lib_path:
            return {}
        resolved = Path(lib_path)
        if not resolved.is_absolute():
            resolved = (Path(__file__).resolve().parents[1] / resolved).resolve()
        cache_key = str(resolved)
        if (
            self._profiled_background_library_cache is not None
            and self._profiled_background_library_cache_key == cache_key
        ):
            return dict(self._profiled_background_library_cache)
        if not resolved.exists():
            logger.warning("profiled_market_mode background library not found: %s", resolved)
            return {}
        try:
            if resolved.suffix.lower() in {".yaml", ".yml"}:
                payload = yaml.safe_load(resolved.read_text(encoding="utf-8")) or {}
            else:
                payload = json.loads(resolved.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                payload = {}
            self._profiled_background_library_cache = dict(payload)
            self._profiled_background_library_cache_key = cache_key
            logger.info("profiled_market_mode loaded background library: %s", resolved)
            return dict(payload)
        except Exception as exc:
            logger.warning("Failed loading background library %s: %s", resolved, exc)
            return {}

    @staticmethod
    def _safe_int_range(raw_value, default_low: int, default_high: int) -> Tuple[int, int]:
        if isinstance(raw_value, (list, tuple)) and len(raw_value) >= 2:
            try:
                low = int(raw_value[0])
                high = int(raw_value[1])
                if low > high:
                    low, high = high, low
                return low, high
            except Exception:
                pass
        return int(default_low), int(default_high)

    @staticmethod
    def _safe_float(raw_value, default_value: float) -> float:
        try:
            return float(raw_value)
        except Exception:
            return float(default_value)

    @staticmethod
    def _normalize_profiled_zone_target(raw_zone: Any) -> str:
        text = str(raw_zone or "").strip()
        upper = text.upper()
        if upper in {"A", "B"}:
            return upper
        lowered = text.lower()
        if lowered in {"core", "sub_core"}:
            return "A"
        if lowered in {"peripheral", "outer", "suburb"}:
            return "B"
        return text

    @staticmethod
    def _normalize_profiled_preference_type_target(raw_value: Any) -> str:
        text = str(raw_value or "").strip().upper()
        if text in {"JUST", "IMPROVE"}:
            return text
        if text in {"STARTER", "STARTER_UPGRADE"}:
            return "JUST"
        if text in {"LUXURY", "SENIOR_FRIENDLY"}:
            return "IMPROVE"
        return text

    def _normalize_profiled_agent_bucket_definition(
        self,
        raw_bucket: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not isinstance(raw_bucket, dict):
            return {}
        bucket = dict(raw_bucket)
        agent_profile = bucket.get("agent_profile", {}) or {}
        story_profile = bucket.get("story_profile", {}) or {}
        preference_profile = dict(bucket.get("preference_profile", {}) or {})
        runtime_profile = dict(bucket.get("runtime_profile", {}) or {})
        initialization_constraints = dict(bucket.get("initialization_constraints", {}) or {})
        budget_profile = dict(bucket.get("budget_profile", {}) or {})
        if preference_profile.get("target_zone") not in (None, ""):
            preference_profile["target_zone"] = self._normalize_profiled_zone_target(
                preference_profile.get("target_zone")
            )
        if preference_profile.get("property_type_target") not in (None, ""):
            preference_profile["property_type_target"] = self._normalize_profiled_preference_type_target(
                preference_profile.get("property_type_target")
            )
        bucket["agent_profile"] = dict(agent_profile) if isinstance(agent_profile, dict) else {}
        bucket["story_profile"] = dict(story_profile) if isinstance(story_profile, dict) else {}
        bucket["preference_profile"] = preference_profile
        bucket["runtime_profile"] = runtime_profile
        bucket["initialization_constraints"] = initialization_constraints
        bucket["budget_profile"] = budget_profile
        return bucket

    def _normalize_profiled_property_bucket_definition(
        self,
        raw_bucket: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not isinstance(raw_bucket, dict):
            return {}
        bucket = dict(raw_bucket)
        market_profile = dict(bucket.get("market_profile", {}) or {})
        property_profile = dict(bucket.get("property_profile", {}) or {})
        if market_profile:
            bucket.update({k: v for k, v in market_profile.items() if k not in (None, "")})
        zone_value = bucket.get("zone", property_profile.get("zone_tier"))
        if zone_value not in (None, ""):
            bucket["zone"] = self._normalize_profiled_zone_target(zone_value)
        type_bucket = bucket.get("property_type_bucket")
        if type_bucket in (None, ""):
            seg = str(property_profile.get("product_segment", "") or "").strip().lower()
            if seg in {"starter", "starter_upgrade"}:
                type_bucket = "JUST"
            elif seg in {"improve", "luxury", "senior_friendly"}:
                type_bucket = "IMPROVE"
        if type_bucket not in (None, ""):
            bucket["property_type_bucket"] = self._normalize_profiled_preference_type_target(type_bucket)
        school_tier = str(property_profile.get("school_tier", "") or "").strip().lower()
        if bucket.get("is_school_district") is None and school_tier:
            bucket["is_school_district"] = school_tier in {"general_school", "strong_school"}
        bucket["market_profile"] = market_profile
        bucket["property_profile"] = property_profile
        return bucket

    def _normalize_profiled_market_pack_definition(
        self,
        raw_pack: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not isinstance(raw_pack, dict):
            return {}
        pack = dict(raw_pack)
        agent_bucket_defs = pack.get("agent_profile_buckets", {}) or {}
        if isinstance(agent_bucket_defs, dict):
            pack["agent_profile_buckets"] = {
                str(bucket_id): self._normalize_profiled_agent_bucket_definition(raw_bucket)
                for bucket_id, raw_bucket in agent_bucket_defs.items()
                if str(bucket_id).strip()
            }
        property_bucket_defs = pack.get("property_profile_buckets", {}) or {}
        if isinstance(property_bucket_defs, dict):
            pack["property_profile_buckets"] = {
                str(bucket_id): self._normalize_profiled_property_bucket_definition(raw_bucket)
                for bucket_id, raw_bucket in property_bucket_defs.items()
                if str(bucket_id).strip()
            }
        return pack

    def _build_profiled_agent_bucket_assignments(
        self,
        agent_count: int,
        profile_pack: Dict[str, Any],
    ) -> Tuple[List[Optional[str]], Dict[str, Dict[str, Any]]]:
        bucket_defs = profile_pack.get("agent_profile_buckets", {})
        if not isinstance(bucket_defs, dict):
            return [None] * int(agent_count), {}
        expanded: List[str] = []
        normalized_defs: Dict[str, Dict[str, Any]] = {}
        for bucket_id, raw_bucket in bucket_defs.items():
            if not isinstance(raw_bucket, dict):
                continue
            bid = str(bucket_id).strip()
            if not bid:
                continue
            count = max(0, int(raw_bucket.get("count", 0) or 0))
            normalized_defs[bid] = dict(raw_bucket)
            if count > 0:
                expanded.extend([bid] * count)
        random.shuffle(expanded)
        assignments: List[Optional[str]] = [None] * int(agent_count)
        for idx in range(min(len(assignments), len(expanded))):
            assignments[idx] = expanded[idx]
        return assignments, normalized_defs

    def _resolve_profiled_initial_property_target(
        self,
        default_target_props: int,
        bucket_id: Optional[str],
        bucket_defs: Dict[str, Dict[str, Any]],
    ) -> int:
        target_props = max(0, int(default_target_props or 0))
        bid = str(bucket_id or "").strip()
        if not bid:
            return target_props
        bucket = bucket_defs.get(bid, {}) or {}
        if not isinstance(bucket, dict):
            return target_props
        init_constraints = bucket.get("initialization_constraints", {}) or {}
        if bool(init_constraints.get("preserve_no_home", False)):
            return 0
        role_side = str(bucket.get("role_side", "") or "").strip().lower()
        story_profile = bucket.get("story_profile", {}) or {}
        housing_stage = str(story_profile.get("housing_stage", "") or "").strip().lower()
        purchase_motive = str(story_profile.get("purchase_motive_primary", "") or "").strip().lower()
        no_home_stages = {
            "starter_no_home",
            "no_home_first_purchase",
            "starter_entry",
        }
        no_home_motives = {
            "starter_home",
            "starter_no_home",
            "rent_to_buy",
            "new_family_first_home",
        }
        if role_side == "buyer" and (housing_stage in no_home_stages or purchase_motive in no_home_motives):
            return 0
        return target_props

    def _profiled_external_shock_cfg(self) -> Dict[str, Any]:
        raw = self.config.get("simulation.agent.external_shock_operator", {}) or {}
        if not isinstance(raw, dict):
            raw = {}
        income_scale = self._safe_float(
            raw.get(
                "income_scale",
                self.config.get("simulation.agent.income_adjustment_rate", 1.0),
            ),
            1.0,
        )
        cash_scale = self._safe_float(raw.get("cash_scale", income_scale), income_scale)
        max_price_scale = self._safe_float(raw.get("max_price_scale", income_scale), income_scale)
        psych_price_scale = self._safe_float(
            raw.get("psychological_price_scale", max_price_scale),
            max_price_scale,
        )
        payment_tol_scale = self._safe_float(
            raw.get("payment_tolerance_scale", max(0.5, min(1.8, income_scale))),
            1.0,
        )
        down_payment_tol_scale = self._safe_float(
            raw.get("down_payment_tolerance_scale", max(0.5, min(1.8, income_scale))),
            1.0,
        )
        enabled = bool(raw.get("enabled", False)) or abs(float(income_scale) - 1.0) > 1e-9
        return {
            "enabled": bool(enabled),
            "income_scale": max(0.0, float(income_scale)),
            "cash_scale": max(0.0, float(cash_scale)),
            "max_price_scale": max(0.0, float(max_price_scale)),
            "psychological_price_scale": max(0.0, float(psych_price_scale)),
            "payment_tolerance_scale": max(0.1, float(payment_tol_scale)),
            "down_payment_tolerance_scale": max(0.1, float(down_payment_tol_scale)),
        }

    def _apply_profiled_external_shock_operator(self, agent: Agent) -> None:
        shock = self._profiled_external_shock_cfg()
        if not bool(shock.get("enabled", False)):
            return

        agent.monthly_income = max(
            0.0,
            float(agent.monthly_income or 0.0) * float(shock["income_scale"]),
        )
        agent.cash = max(0.0, float(agent.cash or 0.0) * float(shock["cash_scale"]))
        agent.last_month_cash = float(agent.cash)

        pref = getattr(agent, "preference", None)
        if pref is not None:
            for attr, scale_key in (
                ("max_price", "max_price_scale"),
                ("max_affordable_price", "max_price_scale"),
                ("psychological_price", "psychological_price_scale"),
                ("target_buy_price", "psychological_price_scale"),
            ):
                current = float(getattr(pref, attr, 0.0) or 0.0)
                if current > 0.0:
                    setattr(pref, attr, max(0.0, current * float(shock[scale_key])))

        agent.payment_tolerance_ratio = max(
            0.05,
            min(
                0.98,
                float(agent.payment_tolerance_ratio or 0.0)
                * float(shock["payment_tolerance_scale"]),
            ),
        )
        agent.down_payment_tolerance_ratio = max(
            0.05,
            min(
                0.98,
                float(agent.down_payment_tolerance_ratio or 0.0)
                * float(shock["down_payment_tolerance_scale"]),
            ),
        )

    def _apply_profiled_agent_bucket(
        self,
        agent: Agent,
        bucket_id: Optional[str],
        bucket_defs: Dict[str, Dict[str, Any]],
        persona_generation_mode: str,
    ) -> Optional[Dict[str, Any]]:
        if not bucket_id:
            return None
        bucket = bucket_defs.get(bucket_id)
        if not isinstance(bucket, dict):
            return None

        budget_profile = bucket.get("budget_profile", {}) or {}
        if isinstance(budget_profile, dict) and budget_profile:
            cash_low, cash_high = self._safe_int_range(
                budget_profile.get("cash_range"),
                int(agent.cash),
                int(agent.cash),
            )
            income_low, income_high = self._safe_int_range(
                budget_profile.get("income_range"),
                int(agent.monthly_income),
                int(agent.monthly_income),
            )
            agent.cash = float(random.randint(cash_low, cash_high))
            agent.last_month_cash = float(agent.cash)
            agent.monthly_income = float(random.randint(income_low, income_high))
            agent.payment_tolerance_ratio = self._safe_float(
                budget_profile.get("payment_tolerance_ratio", agent.payment_tolerance_ratio),
                agent.payment_tolerance_ratio,
            )
            agent.down_payment_tolerance_ratio = self._safe_float(
                budget_profile.get("down_payment_tolerance_ratio", agent.down_payment_tolerance_ratio),
                agent.down_payment_tolerance_ratio,
            )

        runtime_profile = bucket.get("runtime_profile", {}) or {}
        if isinstance(runtime_profile, dict):
            if runtime_profile.get("agent_type") in {"normal", "smart"}:
                agent.agent_type = str(runtime_profile.get("agent_type"))
            if runtime_profile.get("info_delay_months") is not None:
                try:
                    agent.info_delay_months = max(0, int(runtime_profile.get("info_delay_months")))
                except Exception:
                    pass

        agent_profile = bucket.get("agent_profile", {}) or {}
        setattr(agent, "profile_agent_profile", dict(agent_profile) if isinstance(agent_profile, dict) else {})
        initialization_constraints = bucket.get("initialization_constraints", {}) or {}
        setattr(
            agent,
            "profile_initialization_constraints",
            dict(initialization_constraints) if isinstance(initialization_constraints, dict) else {},
        )

        story_profile = bucket.get("story_profile", {}) or {}
        if isinstance(story_profile, dict) and story_profile:
            for attr in (
                "investment_style",
                "purchase_motive_primary",
                "housing_stage",
                "family_stage",
                "education_path",
                "financial_profile",
                "seller_profile",
            ):
                if story_profile.get(attr) not in (None, ""):
                    setattr(agent.story, attr, str(story_profile.get(attr)))

        preference_profile = bucket.get("preference_profile", {}) or {}
        if isinstance(preference_profile, dict):
            if preference_profile.get("target_zone"):
                agent.preference.target_zone = self._normalize_profiled_zone_target(
                    preference_profile.get("target_zone")
                )
            if preference_profile.get("need_school_district") is not None:
                agent.preference.need_school_district = bool(preference_profile.get("need_school_district"))
            if preference_profile.get("min_bedrooms") is not None:
                try:
                    agent.preference.min_bedrooms = max(1, int(preference_profile.get("min_bedrooms")))
                except Exception:
                    pass
            if preference_profile.get("property_type_target") not in (None, ""):
                setattr(
                    agent.preference,
                    "property_type_target",
                    self._normalize_profiled_preference_type_target(
                        preference_profile.get("property_type_target")
                    ),
                )

        if isinstance(budget_profile, dict):
            max_price_range = budget_profile.get("max_price_range")
            if isinstance(max_price_range, (list, tuple)) and len(max_price_range) >= 2:
                low, high = self._safe_int_range(max_price_range, 0, 0)
                selected = max(0, random.randint(low, high))
                agent.preference.max_price = float(selected)
                agent.preference.max_affordable_price = float(selected)
            target_buy_range = budget_profile.get("target_buy_price_range")
            if isinstance(target_buy_range, (list, tuple)) and len(target_buy_range) >= 2:
                low, high = self._safe_int_range(target_buy_range, 0, 0)
                agent.preference.psychological_price = float(max(0, random.randint(low, high)))

        # Apply external shock translation after bucket persona fields are fully materialized.
        self._apply_profiled_external_shock_operator(agent)

        agent.profile_bucket_id = str(bucket_id)
        return {
            "bucket_id": str(bucket_id),
            "role_side": str(bucket.get("role_side", "") or ""),
            "agent_profile": dict(agent_profile) if isinstance(agent_profile, dict) else {},
            "initialization_constraints": dict(initialization_constraints) if isinstance(initialization_constraints, dict) else {},
            "persona_generation_mode": persona_generation_mode,
            "source": "profiled_market_mode",
        }

    @staticmethod
    def _infer_purchase_motive(housing_need: str) -> str:
        text = str(housing_need or "").strip().lower()
        if any(token in text for token in ("学区", "school")):
            return "education_upgrade"
        if any(token in text for token in ("投资", "invest")):
            return "investment"
        if any(token in text for token in ("改善", "improve", "置换")):
            return "upgrade"
        if any(token in text for token in ("刚需", "just", "首套")):
            return "starter_home"
        return "balanced"

    def _sample_background_entry(
        self,
        background_library: Dict[str, Any],
        bucket_id: Optional[str],
        tier: str,
    ) -> Dict[str, Any]:
        if not isinstance(background_library, dict) or not background_library:
            return {}
        candidates: List[Dict[str, Any]] = []
        by_bucket = background_library.get("by_bucket", {})
        if isinstance(by_bucket, dict) and bucket_id:
            rows = by_bucket.get(str(bucket_id), [])
            if isinstance(rows, list):
                candidates.extend([r for r in rows if isinstance(r, dict)])
        by_tier = background_library.get("by_tier", {})
        if isinstance(by_tier, dict):
            rows = by_tier.get(str(tier), [])
            if isinstance(rows, list):
                candidates.extend([r for r in rows if isinstance(r, dict)])
        rows = background_library.get("entries", [])
        if isinstance(rows, list):
            candidates.extend([r for r in rows if isinstance(r, dict)])
        if not candidates:
            return {}
        return random.choice(candidates)

    def _apply_code_generated_story(
        self,
        agent: Agent,
        template: Dict[str, Any],
        tier: str,
        bucket_id: Optional[str],
        profile_meta: Optional[Dict[str, Any]],
        background_library: Dict[str, Any],
    ) -> None:
        sampled = self._sample_background_entry(background_library, bucket_id=bucket_id, tier=tier)
        occupation = str(
            sampled.get("occupation")
            or template.get("occupation")
            or getattr(agent.story, "occupation", "")
            or "普通从业者"
        )
        background_story = str(
            sampled.get("background_story")
            or template.get("background")
            or getattr(agent.story, "background_story", "")
            or "画像库生成：稳定收入与家庭约束下进行住房决策。"
        )
        purchase_motive = str(
            getattr(agent.story, "purchase_motive_primary", "")
            or sampled.get("purchase_motive_primary")
            or self._infer_purchase_motive(str(template.get("housing_need", "") or ""))
        )
        agent.story.occupation = occupation
        agent.story.background_story = background_story
        agent.story.purchase_motive_primary = purchase_motive
        if not getattr(agent.story, "investment_style", ""):
            agent.story.investment_style = str(sampled.get("investment_style") or "balanced")
        for field in (
            "housing_stage",
            "family_stage",
            "education_path",
            "financial_profile",
            "seller_profile",
        ):
            if not getattr(agent.story, field, ""):
                value = sampled.get(field)
                if value not in (None, ""):
                    setattr(agent.story, field, str(value))
        if profile_meta and not getattr(agent.story, "seller_profile", ""):
            role_side = str(profile_meta.get("role_side", "") or "mixed")
            agent.story.seller_profile = f"profiled:{role_side}"

    @staticmethod
    def _normalize_property_type_bucket(raw_type: Any) -> str:
        text = str(raw_type or "").strip().lower()
        if any(token in text for token in ("luxury", "豪宅", "别墅", "大平层", "顶豪", "preservation")):
            return "LUXURY"
        if any(token in text for token in ("improve", "改善", "large", "大户")):
            return "IMPROVE"
        if any(token in text for token in ("just", "small", "刚需", "小户")):
            return "JUST"
        return "UNKNOWN"

    @staticmethod
    def _range_distance(value: float, low: float, high: float) -> float:
        if low <= value <= high:
            return 0.0
        if value < low:
            return float(low - value)
        return float(value - high)

    def _infer_fallback_canonical_target_bucket_id(
        self,
        prop: Dict[str, Any],
        property_bucket_defs: Dict[str, Dict[str, Any]],
    ) -> str:
        zone = str(prop.get("zone", "") or "").strip().upper()
        school = bool(prop.get("is_school_district", False))
        prop_type_bucket = self._normalize_property_type_bucket(prop.get("property_type"))
        try:
            area = float(prop.get("building_area", 0.0) or 0.0)
        except Exception:
            area = 0.0
        try:
            price = float(prop.get("base_value", 0.0) or 0.0)
        except Exception:
            price = 0.0
        try:
            quality = float(prop.get("quality", 0.0) or 0.0)
        except Exception:
            quality = 0.0
        try:
            bedrooms = float(prop.get("bedrooms", 0.0) or 0.0)
        except Exception:
            bedrooms = 0.0

        best_bucket_id = ""
        best_score = float("-inf")
        for bucket_id, raw_bucket in (property_bucket_defs or {}).items():
            if not isinstance(raw_bucket, dict):
                continue
            bid = str(bucket_id or "").strip()
            if not bid:
                continue
            bucket_zone = str(raw_bucket.get("zone", "") or "").strip().upper()
            bucket_school = raw_bucket.get("is_school_district")
            bucket_type = str(raw_bucket.get("property_type_bucket", "") or "").strip().upper()
            if bucket_zone and zone and bucket_zone != zone:
                continue
            if bucket_school is not None and bool(bucket_school) != school:
                continue
            score = 0.0
            if bucket_zone == zone:
                score += 8.0
            if bucket_school is not None and bool(bucket_school) == school:
                score += 6.0
            if prop_type_bucket == bucket_type:
                score += 6.0
            elif prop_type_bucket == "LUXURY" and bucket_type == "IMPROVE":
                score += 2.0
            elif prop_type_bucket == "UNKNOWN":
                score += 1.0
            else:
                score -= 3.0

            area_low, area_high = self._safe_int_range(raw_bucket.get("building_area_range"), 0, 0)
            price_low, price_high = self._safe_int_range(raw_bucket.get("price_range"), 0, 0)
            quality_low, quality_high = self._safe_int_range(raw_bucket.get("quality_range"), 0, 0)
            bedroom_low, bedroom_high = self._safe_int_range(raw_bucket.get("bedroom_range"), 0, 0)

            if area_high > 0:
                score -= min(6.0, self._range_distance(area, float(area_low), float(area_high)) / 20.0)
            if price_high > 0:
                score -= min(8.0, self._range_distance(price, float(price_low), float(price_high)) / 1_000_000.0)
            if quality_high > 0:
                score -= min(3.0, self._range_distance(quality, float(quality_low), float(quality_high)))
            if bedroom_high > 0:
                score -= min(3.0, self._range_distance(bedrooms, float(bedroom_low), float(bedroom_high)))

            if score > best_score:
                best_score = score
                best_bucket_id = bid
        return best_bucket_id

    @staticmethod
    def _build_runtime_fallback_band_label(
        value: float,
        low: float,
        high: float,
        *,
        prefix: str,
    ) -> str:
        if high <= 0:
            return f"{prefix}_UNK"
        midpoint = (float(low) + float(high)) / 2.0
        if value < float(low):
            return f"{prefix}_BELOW"
        if value > float(high):
            return f"{prefix}_ABOVE"
        if value <= midpoint:
            return f"{prefix}_LOW"
        return f"{prefix}_HIGH"

    def _build_runtime_fallback_bucket_id(
        self,
        *,
        prop: Dict[str, Any],
        fallback_prefix: str,
        canonical_target_bucket_id: str,
        property_bucket_defs: Dict[str, Dict[str, Any]],
    ) -> str:
        zone = str(prop.get("zone", "") or "").strip().upper() or "UNK"
        school = "SCHOOL" if bool(prop.get("is_school_district", False)) else "NOSCHOOL"
        type_bucket = self._normalize_property_type_bucket(prop.get("property_type"))
        base_bucket = str(canonical_target_bucket_id or "").strip()
        if not base_bucket:
            base_bucket = f"{zone}_{school}_{type_bucket}"

        bucket_def = {}
        if canonical_target_bucket_id:
            bucket_def = (property_bucket_defs or {}).get(str(canonical_target_bucket_id), {}) or {}
        area_low, area_high = self._safe_int_range((bucket_def or {}).get("building_area_range"), 0, 0)
        price_low, price_high = self._safe_int_range((bucket_def or {}).get("price_range"), 0, 0)
        try:
            area = float(prop.get("building_area", 0.0) or 0.0)
        except Exception:
            area = 0.0
        try:
            price = float(prop.get("base_value", 0.0) or 0.0)
        except Exception:
            price = 0.0

        area_band = self._build_runtime_fallback_band_label(area, float(area_low), float(area_high), prefix="AREA")
        price_band = self._build_runtime_fallback_band_label(price, float(price_low), float(price_high), prefix="PRICE")
        return f"{fallback_prefix}{base_bucket}_{area_band}_{price_band}"

    def _match_property_bucket(self, prop: Dict[str, Any], bucket: Dict[str, Any]) -> bool:
        zone = str(prop.get("zone", "") or "").upper()
        if str(bucket.get("zone", "") or "").upper() not in {"", zone}:
            return False

        if bucket.get("is_school_district") is not None:
            prop_school = bool(prop.get("is_school_district", False))
            if bool(bucket.get("is_school_district")) != prop_school:
                return False

        price_low, price_high = self._safe_int_range(
            bucket.get("price_range"),
            0,
            10**12,
        )
        price = float(prop.get("base_value", 0.0) or 0.0)
        if price < float(price_low) or price > float(price_high):
            return False

        area_low, area_high = self._safe_int_range(
            bucket.get("building_area_range"),
            0,
            10**6,
        )
        area = float(prop.get("building_area", 0.0) or 0.0)
        if area < float(area_low) or area > float(area_high):
            return False

        quality_low, quality_high = self._safe_int_range(
            bucket.get("quality_range"),
            0,
            100,
        )
        quality = int(prop.get("quality", 0) or 0)
        if quality < int(quality_low) or quality > int(quality_high):
            return False

        type_bucket = str(bucket.get("property_type_bucket", "") or "").strip().upper()
        if type_bucket and type_bucket != "ANY":
            prop_type_bucket = self._normalize_property_type_bucket(prop.get("property_type"))
            if prop_type_bucket != type_bucket:
                return False
        return True

    def _apply_profiled_property_supply_mode(
        self,
        market_properties: List[Dict[str, Any]],
        profile_pack: Dict[str, Any],
        experiment_mode: str,
    ) -> List[Tuple[int, str, str, str, str]]:
        property_bucket_defs = profile_pack.get("property_profile_buckets", {})
        if not isinstance(property_bucket_defs, dict) or not property_bucket_defs:
            return []

        mode = str(experiment_mode or "abundant").strip() or "abundant"
        canonical_policy = profile_pack.get("canonical_bucket_policy", {}) or {}
        fallback_prefix = str(canonical_policy.get("fallback_bucket_prefix", "FALLBACK_SUPPLY_") or "FALLBACK_SUPPLY_")
        strict_required = bool(canonical_policy.get("strict_match_required_for_canonical", False))
        owned_props = [
            p for p in market_properties
            if int(p.get("owner_id", -1) or -1) > 0
        ]
        owned_props.sort(key=lambda item: int(item.get("property_id", 0) or 0))
        # Hard-constrained supply mode: reset all owner-held inventory first,
        # then open only the bucket-selected properties.
        for prop in owned_props:
            prop["status"] = "off_market"
            prop["listing_month"] = 0
            prop.pop("listed_price", None)
            prop.pop("min_price", None)

        bucket_to_props: Dict[str, List[Dict[str, Any]]] = {}
        prop_bucket_rows: List[Tuple[int, str, str, str, str]] = []
        assigned_property_ids = set()
        bucket_deficits: List[Dict[str, Any]] = []

        def _bucket_target(raw_bucket: Dict[str, Any]) -> int:
            target_map = raw_bucket.get("count_by_supply_mode", {}) or {}
            if not isinstance(target_map, dict):
                return 0
            return max(0, int(target_map.get(mode, target_map.get("abundant", 0)) or 0))

        def _bucket_match_mode(bucket: Dict[str, Any], candidates: List[Dict[str, Any]]) -> Tuple[str, List[Dict[str, Any]]]:
            strict = [p for p in candidates if self._match_property_bucket(p, bucket)]
            if strict:
                return "strict", strict
            expect_zone = str(bucket.get("zone", "") or "").upper()
            expect_school = (
                None if bucket.get("is_school_district") is None
                else bool(bucket.get("is_school_district"))
            )
            expect_type = str(bucket.get("property_type_bucket", "") or "").strip().upper()
            relaxed = []
            for p in candidates:
                if str(p.get("zone", "") or "").upper() != expect_zone:
                    continue
                if expect_school is not None and bool(p.get("is_school_district", False)) != expect_school:
                    continue
                if expect_type and expect_type != "ANY":
                    if self._normalize_property_type_bucket(p.get("property_type")) != expect_type:
                        continue
                relaxed.append(p)
            if relaxed:
                return "relaxed_zone_school_type", relaxed
            zone_only = [p for p in candidates if str(p.get("zone", "") or "").upper() == expect_zone]
            if zone_only:
                return "relaxed_zone_only", zone_only
            return "global_pool_fallback", list(candidates)

        def _coerce_property_to_bucket(prop: Dict[str, Any], bucket: Dict[str, Any]) -> None:
            zone = str(bucket.get("zone", "") or "").strip().upper()
            if zone:
                prop["zone"] = zone
            if bucket.get("is_school_district") is not None:
                prop["is_school_district"] = bool(bucket.get("is_school_district"))
            type_bucket = str(bucket.get("property_type_bucket", "") or "").strip().upper()
            if type_bucket == "JUST":
                prop["property_type"] = "just_small"
            elif type_bucket == "IMPROVE":
                prop["property_type"] = "improve_large"
            bedroom_low, bedroom_high = self._safe_int_range(bucket.get("bedroom_range"), 1, 12)
            area_low, area_high = self._safe_int_range(bucket.get("building_area_range"), 20, 500)
            quality_low, quality_high = self._safe_int_range(bucket.get("quality_range"), 1, 5)
            price_low, price_high = self._safe_int_range(bucket.get("price_range"), 100_000, 20_000_000)
            prop["bedrooms"] = int((int(bedroom_low) + int(bedroom_high)) // 2)
            prop["building_area"] = float((float(area_low) + float(area_high)) / 2.0)
            prop["quality"] = int((int(quality_low) + int(quality_high)) // 2)
            prop["base_value"] = float((float(price_low) + float(price_high)) / 2.0)

        for bucket_id, raw_bucket in property_bucket_defs.items():
            if not isinstance(raw_bucket, dict):
                continue
            bid = str(bucket_id).strip()
            if not bid:
                continue
            target_count = _bucket_target(raw_bucket)
            if target_count <= 0:
                continue
            remaining_pool = [
                p for p in owned_props
                if int(p.get("property_id", 0) or 0) not in assigned_property_ids
            ]
            match_mode, matched = _bucket_match_mode(raw_bucket, remaining_pool)
            selected_props = []
            for prop in matched:
                pid = int(prop.get("property_id", 0) or 0)
                if pid <= 0 or pid in assigned_property_ids:
                    continue
                selected_props.append(prop)
                assigned_property_ids.add(pid)
                if len(selected_props) >= target_count:
                    break
            if match_mode != "strict" and not strict_required:
                for prop in selected_props:
                    _coerce_property_to_bucket(prop, raw_bucket)

            hard_filled = 0
            if len(selected_props) < target_count:
                fallback_pool = [
                    p for p in remaining_pool
                    if int(p.get("property_id", 0) or 0) not in assigned_property_ids
                ]
                zone = str(raw_bucket.get("zone", "") or "").upper()
                school = raw_bucket.get("is_school_district")
                type_bucket = str(raw_bucket.get("property_type_bucket", "") or "").strip().upper()

                def _fallback_rank(prop: Dict[str, Any]) -> Tuple[int, int, int, int]:
                    score = 0
                    if zone and str(prop.get("zone", "") or "").upper() == zone:
                        score += 4
                    if school is not None and bool(prop.get("is_school_district", False)) == bool(school):
                        score += 2
                    if type_bucket and type_bucket != "ANY":
                        if self._normalize_property_type_bucket(prop.get("property_type")) == type_bucket:
                            score += 1
                    return (
                        int(score),
                        int(prop.get("quality", 0) or 0),
                        int(prop.get("bedrooms", 0) or 0),
                        -int(prop.get("property_id", 0) or 0),
                    )

                fallback_pool.sort(key=_fallback_rank, reverse=True)
                for prop in fallback_pool:
                    pid = int(prop.get("property_id", 0) or 0)
                    if pid <= 0 or pid in assigned_property_ids:
                        continue
                    if not strict_required:
                        _coerce_property_to_bucket(prop, raw_bucket)
                    selected_props.append(prop)
                    assigned_property_ids.add(pid)
                    hard_filled += 1
                    if len(selected_props) >= target_count:
                        break

            bucket_to_props[bid] = list(selected_props)
            actual_count = int(len(selected_props))
            if actual_count < int(target_count):
                bucket_deficits.append(
                    {
                        "bucket_id": bid,
                        "mode": mode,
                        "target_count": int(target_count),
                        "actual_count": int(actual_count),
                    }
                )

            for idx, prop in enumerate(selected_props, start=1):
                pid = int(prop.get("property_id", 0) or 0)
                prop["status"] = "for_sale"
                prop["listing_month"] = 0
                row_match_mode = match_mode if idx <= (actual_count - hard_filled) else "hard_fill_generated"
                is_canonical = not strict_required or row_match_mode == "strict"
                runtime_bucket_id = bid if is_canonical else self._build_runtime_fallback_bucket_id(
                    prop=prop,
                    fallback_prefix=fallback_prefix,
                    canonical_target_bucket_id=bid,
                    property_bucket_defs=property_bucket_defs,
                )
                prop_bucket_rows.append(
                    (
                        pid,
                        runtime_bucket_id,
                        mode,
                        "profiled_market_mode",
                        json.dumps(
                            {
                                "bucket_id": bid,
                                "runtime_bucket_id": runtime_bucket_id,
                                "selected_for_sale": True,
                                "target_count": int(target_count),
                                "actual_count": int(actual_count),
                                "match_rank": int(idx),
                                "match_mode": row_match_mode,
                                "hard_filled": bool(row_match_mode == "hard_fill_generated"),
                                "bucket_class": "canonical" if is_canonical else "fallback",
                                "canonical_target_bucket_id": bid,
                                "fallback_base_bucket_id": bid if not is_canonical else "",
                            },
                            ensure_ascii=False,
                        ),
                    )
                )

        logger.info(
            "profiled_market_mode property supply applied: "
            f"mode={mode}, buckets={len(bucket_to_props)}, affected_props={len(prop_bucket_rows)}"
        )
        if bucket_deficits:
            logger.warning(
                "profiled_market_mode supply deficits: %s",
                json.dumps(bucket_deficits, ensure_ascii=False),
            )
        return prop_bucket_rows

    def _ensure_profiled_market_tables(self, cursor: sqlite3.Cursor) -> None:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS profiled_market_agent_buckets (
                agent_id INTEGER PRIMARY KEY,
                bucket_id TEXT NOT NULL,
                role_side TEXT,
                source TEXT,
                metadata_json TEXT,
                assigned_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS profiled_market_property_buckets (
                property_id INTEGER PRIMARY KEY,
                bucket_id TEXT NOT NULL,
                supply_mode TEXT,
                source TEXT,
                metadata_json TEXT,
                assigned_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def _flush_profiled_agent_assignments(
        self,
        cursor: sqlite3.Cursor,
        rows: List[Tuple[int, str, str, str, str]],
    ) -> None:
        if not rows:
            return
        self._ensure_profiled_market_tables(cursor)
        cursor.executemany(
            """
            INSERT INTO profiled_market_agent_buckets (
                agent_id, bucket_id, role_side, source, metadata_json
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(agent_id) DO UPDATE SET
                bucket_id=excluded.bucket_id,
                role_side=excluded.role_side,
                source=excluded.source,
                metadata_json=excluded.metadata_json,
                assigned_at=CURRENT_TIMESTAMP
            """,
            rows,
        )
        self.conn.commit()

    def _flush_profiled_property_assignments(
        self,
        cursor: sqlite3.Cursor,
        rows: List[Tuple[int, str, str, str, str]],
    ) -> None:
        if not rows:
            return
        self._ensure_profiled_market_tables(cursor)
        cursor.executemany(
            """
            INSERT INTO profiled_market_property_buckets (
                property_id, bucket_id, supply_mode, source, metadata_json
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(property_id) DO UPDATE SET
                bucket_id=excluded.bucket_id,
                supply_mode=excluded.supply_mode,
                source=excluded.source,
                metadata_json=excluded.metadata_json,
                assigned_at=CURRENT_TIMESTAMP
            """,
            rows,
        )
        self.conn.commit()

    def _replace_profiled_property_assignments(
        self,
        cursor: sqlite3.Cursor,
        rows: List[Tuple[int, str, str, str, str]],
    ) -> None:
        self._ensure_profiled_market_tables(cursor)
        cursor.execute("DELETE FROM profiled_market_property_buckets")
        if rows:
            cursor.executemany(
                """
                INSERT INTO profiled_market_property_buckets (
                    property_id, bucket_id, supply_mode, source, metadata_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                rows,
            )
        self.conn.commit()

    def _load_profiled_agent_bucket_map(self, cursor: sqlite3.Cursor) -> Dict[int, str]:
        try:
            cursor.execute(
                "SELECT agent_id, bucket_id FROM profiled_market_agent_buckets"
            )
        except Exception:
            return {}
        rows = cursor.fetchall() or []
        mapping: Dict[int, str] = {}
        for row in rows:
            try:
                mapping[int(row[0])] = str(row[1] or "")
            except Exception:
                continue
        return mapping

    def _build_runtime_profiled_property_assignments(
        self,
        market_properties: List[Dict[str, Any]],
        profile_pack: Dict[str, Any],
        experiment_mode: str,
    ) -> List[Tuple[int, str, str, str, str]]:
        property_bucket_defs = profile_pack.get("property_profile_buckets", {}) or {}
        if not isinstance(property_bucket_defs, dict) or not property_bucket_defs:
            return []
        canonical_policy = profile_pack.get("canonical_bucket_policy", {}) or {}
        fallback_prefix = str(canonical_policy.get("fallback_bucket_prefix", "FALLBACK_SUPPLY_") or "FALLBACK_SUPPLY_")
        rows: List[Tuple[int, str, str, str, str]] = []
        for prop in sorted(
            market_properties,
            key=lambda item: int(item.get("property_id", 0) or 0),
        ):
            pid = int(prop.get("property_id", 0) or 0)
            owner_id = int(prop.get("owner_id", -1) or -1)
            if pid <= 0 or owner_id <= 0:
                continue
            matched_bucket_id = ""
            for bucket_id, raw_bucket in property_bucket_defs.items():
                if not isinstance(raw_bucket, dict):
                    continue
                bid = str(bucket_id or "").strip()
                if not bid:
                    continue
                if self._match_property_bucket(prop, raw_bucket):
                    matched_bucket_id = bid
                    break
            if not matched_bucket_id:
                zone = str(prop.get("zone", "") or "").strip().upper() or "UNK"
                school = "SCHOOL" if bool(prop.get("is_school_district", False)) else "NOSCHOOL"
                type_bucket = self._normalize_property_type_bucket(prop.get("property_type"))
                canonical_target_bucket_id = self._infer_fallback_canonical_target_bucket_id(
                    prop,
                    property_bucket_defs=property_bucket_defs,
                )
                matched_bucket_id = self._build_runtime_fallback_bucket_id(
                    prop=prop,
                    fallback_prefix=fallback_prefix,
                    canonical_target_bucket_id=canonical_target_bucket_id,
                    property_bucket_defs=property_bucket_defs,
                )
                bucket_class = "fallback"
            else:
                bucket_class = "canonical"
                canonical_target_bucket_id = matched_bucket_id
            rows.append(
                (
                    pid,
                    matched_bucket_id,
                    str(experiment_mode or "abundant"),
                    "profiled_market_mode_runtime_sync",
                    json.dumps(
                        {
                            "bucket_id": matched_bucket_id,
                            "runtime_bucket_id": matched_bucket_id,
                            "canonical_target_bucket_id": canonical_target_bucket_id,
                            "runtime_synced": True,
                            "owner_id": owner_id,
                            "status": str(prop.get("status", "") or ""),
                            "bucket_class": bucket_class,
                            "fallback_base_bucket_id": str(canonical_target_bucket_id or ""),
                        },
                        ensure_ascii=False,
                    ),
                )
            )
        return rows

    @staticmethod
    def _build_runtime_parent_bucket_maps(profile_pack: Dict[str, Any]) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, Dict[str, Any]]]:
        parent_defs = profile_pack.get("runtime_parent_buckets", {}) or {}
        if not isinstance(parent_defs, dict):
            return {}, {}, {}
        demand_child_to_parent: Dict[str, str] = {}
        supply_child_to_parent: Dict[str, str] = {}
        normalized_defs: Dict[str, Dict[str, Any]] = {}
        for parent_bucket_id, raw_def in parent_defs.items():
            if not isinstance(raw_def, dict):
                continue
            pid = str(parent_bucket_id or "").strip()
            if not pid:
                continue
            demand_children = [str(x).strip() for x in (raw_def.get("child_demand_buckets", []) or []) if str(x).strip()]
            supply_children = [str(x).strip() for x in (raw_def.get("child_supply_buckets", []) or []) if str(x).strip()]
            normalized_defs[pid] = {
                "child_demand_buckets": demand_children,
                "child_supply_buckets": supply_children,
            }
            for child in demand_children:
                demand_child_to_parent[child] = pid
            for child in supply_children:
                supply_child_to_parent[child] = pid
        return demand_child_to_parent, supply_child_to_parent, normalized_defs

    def _hybrid_target_budget_floor_pass(
        self,
        agent: Agent,
        bucket_id: str,
        profile_pack: Dict[str, Any],
        min_cash_observer: float,
    ) -> bool:
        if getattr(agent, "owned_properties", []):
            return True
        cash_now = float(getattr(agent, "cash", 0.0) or 0.0)
        income_now = float(getattr(agent, "monthly_income", 0.0) or 0.0)
        bucket_defs = profile_pack.get("agent_profile_buckets", {}) or {}
        bucket_def = bucket_defs.get(str(bucket_id or "").strip(), {}) if isinstance(bucket_defs, dict) else {}
        budget_profile = bucket_def.get("budget_profile", {}) or {}
        cash_range = budget_profile.get("cash_range")
        income_range = budget_profile.get("income_range")
        cash_low, _ = self._safe_int_range(cash_range, 0, 0)
        income_low, _ = self._safe_int_range(income_range, 0, 0)
        if cash_low > 0 or income_low > 0:
            return cash_now >= float(cash_low) and income_now >= float(income_low)
        return cash_now >= float(min_cash_observer)

    def _load_profiled_agent_bucket_details(self, cursor: sqlite3.Cursor) -> Dict[int, Dict[str, Any]]:
        try:
            cursor.execute(
                """
                SELECT agent_id, bucket_id, role_side, metadata_json
                FROM profiled_market_agent_buckets
                """
            )
        except Exception:
            return {}
        rows = cursor.fetchall() or []
        mapping: Dict[int, Dict[str, Any]] = {}
        for row in rows:
            try:
                agent_id = int(row[0])
            except Exception:
                continue
            mapping[agent_id] = {
                "bucket_id": str(row[1] or ""),
                "role_side": str(row[2] or ""),
                "metadata": json.loads(row[3]) if row[3] else {},
            }
        return mapping

    def _load_live_profiled_property_bucket_supply(self, cursor: sqlite3.Cursor) -> Dict[str, int]:
        try:
            cursor.execute(
                """
                SELECT ppb.bucket_id, COUNT(*)
                FROM properties_market pm
                JOIN profiled_market_property_buckets ppb
                  ON ppb.property_id = pm.property_id
                WHERE LOWER(COALESCE(pm.status, ''))='for_sale'
                GROUP BY ppb.bucket_id
                """
            )
        except Exception:
            return {}
        rows = cursor.fetchall() or []
        supply_map: Dict[str, int] = {}
        for row in rows:
            try:
                supply_map[str(row[0] or "")] = int(row[1] or 0)
            except Exception:
                continue
        return supply_map

    @staticmethod
    def _bucket_supply_pressure(ratio: float, eligible_supply_count: int) -> str:
        if int(eligible_supply_count or 0) <= 0:
            return "none"
        if float(ratio or 0.0) > 2.0:
            return "tight"
        if float(ratio or 0.0) < 0.5:
            return "loose"
        return "balanced"

    def _build_profiled_bucket_pressure_map(
        self,
        cursor: sqlite3.Cursor,
        profile_pack: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        if not isinstance(profile_pack, dict) or not profile_pack:
            return {}
        bucket_defs = profile_pack.get("agent_profile_buckets", {}) or {}
        if not isinstance(bucket_defs, dict):
            bucket_defs = {}
        rule_rows = profile_pack.get("bucket_alignment_rules", []) or []
        rule_map = {
            str(item.get("agent_bucket_id", "")).strip(): item
            for item in rule_rows
            if isinstance(item, dict) and str(item.get("agent_bucket_id", "")).strip()
        }
        assignment_details = self._load_profiled_agent_bucket_details(cursor)
        assignment_counts: Dict[str, int] = {}
        for item in assignment_details.values():
            bucket_id = str(item.get("bucket_id", "") or "").strip()
            if not bucket_id:
                continue
            assignment_counts[bucket_id] = int(assignment_counts.get(bucket_id, 0) or 0) + 1
        live_supply_by_bucket = self._load_live_profiled_property_bucket_supply(cursor)

        pressure_map: Dict[str, Dict[str, Any]] = {}
        for bucket_id, bucket_def in bucket_defs.items():
            bid = str(bucket_id or "").strip()
            if not bid or not isinstance(bucket_def, dict):
                continue
            rule = rule_map.get(bid, {}) or {}
            eligible_property_buckets = [
                str(x) for x in (rule.get("eligible_property_buckets", []) or []) if str(x).strip()
            ]
            eligible_supply_count = int(
                sum(int(live_supply_by_bucket.get(prop_bucket, 0) or 0) for prop_bucket in eligible_property_buckets)
            )
            assigned_count = int(assignment_counts.get(bid, int(bucket_def.get("count", 0) or 0)) or 0)
            buyer_to_supply_ratio = float(assigned_count) / float(max(1, eligible_supply_count))
            pressure_map[bid] = {
                "bucket_id": bid,
                "role_side": str(bucket_def.get("role_side", "") or ""),
                "eligible_property_buckets": eligible_property_buckets,
                "eligible_property_bucket_count_this_month": int(eligible_supply_count),
                "buyer_to_supply_ratio": round(float(buyer_to_supply_ratio), 4),
                "bucket_supply_pressure": self._bucket_supply_pressure(
                    ratio=buyer_to_supply_ratio,
                    eligible_supply_count=eligible_supply_count,
                ),
                "assigned_count": assigned_count,
            }
        return pressure_map

    def _build_runtime_parent_bucket_pressure_map(
        self,
        cursor: sqlite3.Cursor,
        profile_pack: Dict[str, Any],
        child_pressure_map: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        _, _, parent_defs = self._build_runtime_parent_bucket_maps(profile_pack)
        if not parent_defs:
            return {}
        live_supply_by_bucket = self._load_live_profiled_property_bucket_supply(cursor)
        parent_map: Dict[str, Dict[str, Any]] = {}
        for parent_bucket_id, parent_def in parent_defs.items():
            demand_children = [str(x).strip() for x in (parent_def.get("child_demand_buckets", []) or []) if str(x).strip()]
            child_rows = [child_pressure_map.get(child_id, {}) for child_id in demand_children if child_pressure_map.get(child_id)]
            if not child_rows:
                continue
            eligible_buckets: List[str] = []
            assigned_count = 0
            role_sides = set()
            for row in child_rows:
                role_side = str(row.get("role_side", "") or "").strip()
                if role_side:
                    role_sides.add(role_side)
                assigned_count += int(row.get("assigned_count", 0) or 0)
                for prop_bucket in (row.get("eligible_property_buckets", []) or []):
                    pbid = str(prop_bucket or "").strip()
                    if pbid and pbid not in eligible_buckets:
                        eligible_buckets.append(pbid)
            eligible_supply_count = int(sum(int(live_supply_by_bucket.get(pbid, 0) or 0) for pbid in eligible_buckets))
            buyer_to_supply_ratio = float(assigned_count) / float(max(1, eligible_supply_count))
            if "buyer" in {s.lower() for s in role_sides}:
                role_side = "buyer"
            elif "buyer_seller" in {s.lower() for s in role_sides}:
                role_side = "buyer_seller"
            else:
                role_side = next(iter(role_sides), "")
            parent_map[parent_bucket_id] = {
                "bucket_id": parent_bucket_id,
                "role_side": role_side,
                "eligible_property_buckets": eligible_buckets,
                "eligible_property_bucket_count_this_month": eligible_supply_count,
                "buyer_to_supply_ratio": round(float(buyer_to_supply_ratio), 4),
                "bucket_supply_pressure": self._bucket_supply_pressure(
                    ratio=buyer_to_supply_ratio,
                    eligible_supply_count=eligible_supply_count,
                ),
                "assigned_count": int(assigned_count),
                "child_bucket_ids": demand_children,
            }
        return parent_map

    def _build_activation_persona_packet(
        self,
        agent: Agent,
        profile_pack: Dict[str, Any],
        bucket_pressure_map: Dict[str, Dict[str, Any]],
        parent_pressure_map: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        bucket_id = str(getattr(agent, "profile_bucket_id", "") or "").strip()
        if not bucket_id:
            return {}
        demand_child_to_parent, _, _ = self._build_runtime_parent_bucket_maps(profile_pack)
        parent_bucket_id = str(demand_child_to_parent.get(bucket_id, "") or "").strip()
        bucket_defs = profile_pack.get("agent_profile_buckets", {}) or {}
        bucket_def = bucket_defs.get(bucket_id, {}) if isinstance(bucket_defs, dict) else {}
        pref_profile = bucket_def.get("preference_profile", {}) or {}
        budget_profile = bucket_def.get("budget_profile", {}) or {}
        pressure = bucket_pressure_map.get(bucket_id, {})
        runtime_pressure = (parent_pressure_map or {}).get(parent_bucket_id, pressure) if parent_bucket_id else pressure
        target_buy_range = budget_profile.get("target_buy_price_range")
        max_price_range = budget_profile.get("max_price_range")
        budget_range = target_buy_range if isinstance(target_buy_range, (list, tuple)) and len(target_buy_range) >= 2 else max_price_range
        if isinstance(budget_range, (list, tuple)) and len(budget_range) >= 2:
            budget_band = f"{int(budget_range[0])}-{int(budget_range[1])}"
        else:
            max_price = float(getattr(getattr(agent, "preference", None), "max_price", 0.0) or 0.0)
            budget_band = f"0-{int(max_price)}" if max_price > 0 else "0-0"
        packet = {
            "profile_bucket_id": bucket_id,
            "runtime_parent_bucket_id": parent_bucket_id,
            "role_side_hint": str(runtime_pressure.get("role_side", bucket_def.get("role_side", "")) or ""),
            "target_zone": str(pref_profile.get("target_zone", getattr(agent.preference, "target_zone", "")) or ""),
            "need_school_district": bool(
                pref_profile.get("need_school_district", getattr(agent.preference, "need_school_district", False))
            ),
            "property_type_target": str(pref_profile.get("property_type_target", "") or ""),
            "budget_band": str(budget_band),
            "eligible_property_bucket_count_this_month": int(
                runtime_pressure.get("eligible_property_bucket_count_this_month", 0) or 0
            ),
            "bucket_supply_pressure": str(runtime_pressure.get("bucket_supply_pressure", "unknown") or "unknown"),
            "buyer_to_supply_ratio": float(runtime_pressure.get("buyer_to_supply_ratio", 0.0) or 0.0),
            "eligible_property_buckets_runtime": list(runtime_pressure.get("eligible_property_buckets", []) or []),
            "info_delay_months": int(getattr(agent, "info_delay_months", 0) or 0),
        }
        agent_profile = bucket_def.get("agent_profile", {}) or {}
        if isinstance(agent_profile, dict):
            for field in (
                "life_stage",
                "household_structure",
                "housing_stage",
                "education_strategy",
                "asset_state",
                "care_burden",
                "location_logic",
                "risk_style",
            ):
                if agent_profile.get(field) not in (None, ""):
                    packet[field] = str(agent_profile.get(field))
        return packet

    def _attach_activation_persona_packets(
        self,
        cursor: sqlite3.Cursor,
        profile_pack: Dict[str, Any],
    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        bucket_pressure_map = self._build_profiled_bucket_pressure_map(cursor, profile_pack)
        parent_pressure_map = self._build_runtime_parent_bucket_pressure_map(cursor, profile_pack, bucket_pressure_map)
        for agent in self.agents:
            packet = self._build_activation_persona_packet(
                agent,
                profile_pack,
                bucket_pressure_map,
                parent_pressure_map=parent_pressure_map,
            )
            setattr(agent, "_activation_persona_packet", packet)
        return bucket_pressure_map, parent_pressure_map

    def _select_hybrid_targeted_candidates(
        self,
        cursor: sqlite3.Cursor,
        *,
        existing_candidates: List[Agent],
        profile_pack: Dict[str, Any],
        min_cash_observer: float,
        targeted_score_threshold: float,
        severe_bucket_deficit_ratio: float,
        max_targeted_total: int,
    ) -> Tuple[List[Agent], Dict[int, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        if not isinstance(profile_pack, dict) or not profile_pack:
            return [], {}, {}
        bucket_pressure_map, parent_pressure_map = self._attach_activation_persona_packets(cursor, profile_pack)
        demand_child_to_parent, _, _ = self._build_runtime_parent_bucket_maps(profile_pack)
        effective_pressure_map = parent_pressure_map if parent_pressure_map else bucket_pressure_map
        if not effective_pressure_map:
            return [], {}, {}

        candidate_id_set = {int(getattr(a, "id", -1)) for a in existing_candidates}
        severe_ratio = max(1.0, float(severe_bucket_deficit_ratio or 5.0))
        target_bucket_ids = {
            bucket_id
            for bucket_id, meta in effective_pressure_map.items()
            if str(meta.get("role_side", "") or "").lower() in {"buyer", "mixed"}
            and int(meta.get("eligible_property_bucket_count_this_month", 0) or 0) > 0
            and float(meta.get("buyer_to_supply_ratio", 0.0) or 0.0) <= severe_ratio
        }
        if not target_bucket_ids:
            return [], {}, bucket_pressure_map

        per_bucket_ranked: Dict[str, List[Tuple[float, float, Agent]]] = {}
        threshold = max(0.005, float(targeted_score_threshold or 0.0))
        for agent in self.agents:
            try:
                agent_id = int(getattr(agent, "id", -1))
            except Exception:
                agent_id = -1
            if agent_id <= 0 or agent_id in candidate_id_set:
                continue
            if hasattr(agent, "role") and getattr(agent, "role", "OBSERVER") in ["BUYER", "SELLER", "BUYER_SELLER"]:
                continue
            child_bucket_id = str(getattr(agent, "profile_bucket_id", "") or "").strip()
            target_bucket_id = str(demand_child_to_parent.get(child_bucket_id, child_bucket_id) or "").strip()
            if target_bucket_id not in target_bucket_ids:
                continue
            if not self._hybrid_target_budget_floor_pass(
                agent=agent,
                bucket_id=child_bucket_id,
                profile_pack=profile_pack,
                min_cash_observer=min_cash_observer,
            ):
                continue
            score = max(0.0, min(1.0, float(calculate_activation_probability(agent))))
            if score < threshold:
                continue
            priority = float(self._buyer_affordability_proxy(agent))
            per_bucket_ranked.setdefault(target_bucket_id, []).append((score, priority, agent))

        if not per_bucket_ranked:
            return [], {}, bucket_pressure_map

        for bucket_id in per_bucket_ranked:
            per_bucket_ranked[bucket_id].sort(
                key=lambda item: (float(item[0]), float(item[1]), -int(item[2].id)),
                reverse=True,
            )

        bucket_count = max(1, len(per_bucket_ranked))
        using_parent_grouping = bool(parent_pressure_map) and bool(demand_child_to_parent)
        per_bucket_cap = 1
        if (not using_parent_grouping) and int(max_targeted_total or 0) > 0:
            per_bucket_cap = max(1, int(max_targeted_total) // bucket_count)
            if int(max_targeted_total) < bucket_count:
                per_bucket_cap = 1

        targeted_agents: List[Agent] = []
        targeted_map: Dict[int, Dict[str, Any]] = {}
        total_cap = max(int(max_targeted_total or 0), bucket_count)
        for bucket_id in sorted(per_bucket_ranked.keys()):
            selected = 0
            meta = effective_pressure_map.get(bucket_id, {})
            for score, _, agent in per_bucket_ranked[bucket_id]:
                if len(targeted_agents) >= total_cap:
                    break
                if selected >= per_bucket_cap:
                    break
                agent_id = int(agent.id)
                tag = {
                    "bucket_id": bucket_id,
                    "parent_bucket_id": bucket_id,
                    "child_bucket_id": str(getattr(agent, "profile_bucket_id", "") or ""),
                    "reason": "hybrid_bucket_targeted_llm",
                    "activation_score": round(float(score), 4),
                    "eligible_property_bucket_count_this_month": int(
                        meta.get("eligible_property_bucket_count_this_month", 0) or 0
                    ),
                    "bucket_supply_pressure": str(meta.get("bucket_supply_pressure", "unknown") or "unknown"),
                    "buyer_to_supply_ratio": float(meta.get("buyer_to_supply_ratio", 0.0) or 0.0),
                }
                setattr(agent, "_hybrid_bucket_targeted_meta", dict(tag))
                targeted_agents.append(agent)
                targeted_map[agent_id] = dict(tag)
                selected += 1
            if len(targeted_agents) >= total_cap:
                break
        return targeted_agents, targeted_map, bucket_pressure_map

    @staticmethod
    def _safe_property_count(agent: Agent) -> int:
        try:
            return int(len(getattr(agent, "owned_properties", []) or []))
        except Exception:
            return 0

    @staticmethod
    def _buyer_affordability_proxy(agent: Agent) -> float:
        cash = float(getattr(agent, "cash", 0.0) or 0.0)
        income = float(getattr(agent, "monthly_income", 0.0) or 0.0)
        debt = float(getattr(agent, "total_debt", 0.0) or 0.0)
        prop_count = AgentService._safe_property_count(agent)
        no_home_bonus = 2_000_000.0 if prop_count <= 0 else 0.0
        return float(cash + income * 36.0 - debt * 0.35 + no_home_bonus)

    @staticmethod
    def _seller_pressure_proxy(agent: Agent) -> float:
        income = max(1.0, float(getattr(agent, "monthly_income", 0.0) or 0.0))
        cash = float(getattr(agent, "cash", 0.0) or 0.0)
        debt = float(getattr(agent, "total_debt", 0.0) or 0.0)
        prop_count = AgentService._safe_property_count(agent)
        cash_buffer_months = float(cash) / float(income)
        pressure = max(0.0, 12.0 - min(cash_buffer_months, 12.0))
        return float(prop_count * 5_000_000.0 + debt * 0.20 + pressure * 100_000.0)

    def _forced_role_sort_key(self, agent: Agent, role: str, selection_policy: str):
        policy = str(selection_policy or "affordability_inventory_balanced").lower()
        prop_count = self._safe_property_count(agent)
        buyer_score = float(self._buyer_affordability_proxy(agent))
        seller_score = float(self._seller_pressure_proxy(agent))
        monthly_income = float(getattr(agent, "monthly_income", 0.0) or 0.0)
        cash = float(getattr(agent, "cash", 0.0) or 0.0)
        agent_id = int(getattr(agent, "id", 0) or 0)

        if policy == "random":
            return (random.random(), -agent_id)
        if policy == "income_desc":
            return (monthly_income, cash, prop_count, -agent_id)
        if policy == "property_count_desc":
            return (prop_count, seller_score, buyer_score, -agent_id)
        if role == "BUYER_SELLER":
            return (buyer_score + prop_count * 500_000.0, seller_score, monthly_income, cash, -agent_id)
        if role == "SELLER":
            return (seller_score, prop_count, cash, monthly_income, -agent_id)
        no_home_bias = 1.0 if prop_count <= 0 else 0.0
        return (no_home_bias, buyer_score, monthly_income, cash, -agent_id)

    @staticmethod
    def _forced_role_life_pressure(agent: Agent, role: str) -> str:
        income = max(1.0, float(getattr(agent, "monthly_income", 0.0) or 0.0))
        cash = float(getattr(agent, "cash", 0.0) or 0.0)
        buffer_months = float(cash) / float(income)
        if role in {"SELLER", "BUYER_SELLER"} and buffer_months < 3.0:
            return "urgent"
        if role == "BUYER" and buffer_months < 6.0:
            return "anxious"
        return "patient"

    @staticmethod
    def _forced_role_risk_mode(agent: Agent) -> str:
        style = str(getattr(agent.story, "investment_style", "balanced") or "balanced").lower()
        if style in {"aggressive", "conservative", "balanced"}:
            return style
        return "balanced"

    def _build_forced_role_decisions(
        self,
        *,
        candidates: List[Agent],
        month: int,
        forced_cfg: Dict[str, object],
    ) -> Tuple[List[Dict], Dict[str, object]]:
        requested_quota = dict((forced_cfg or {}).get("quota", {}) or {})
        selection_policy = str(
            (forced_cfg or {}).get("selection_policy", "affordability_inventory_balanced")
        ).lower()
        allow_force_locked_buyers = bool((forced_cfg or {}).get("allow_force_locked_buyers", True))

        role_pools: Dict[str, Dict[int, Agent]] = {
            "BUYER": {},
            "SELLER": {},
            "BUYER_SELLER": {},
        }
        remaining: Dict[int, Agent] = {}
        locked_allocations: Dict[str, List[int]] = {
            "BUYER": [],
            "SELLER": [],
            "BUYER_SELLER": [],
        }

        for agent in candidates or []:
            try:
                agent_id = int(getattr(agent, "id", 0) or 0)
            except Exception:
                continue
            if agent_id <= 0:
                continue
            remaining[agent_id] = agent

        if allow_force_locked_buyers:
            for agent_id, agent in list(remaining.items()):
                buy_locked = bool(getattr(agent, "_buy_task_locked", False))
                search_exhausted = bool(getattr(agent, "_search_exhausted", False))
                buy_completed = bool(getattr(agent, "buy_completed", 0))
                if not buy_locked or search_exhausted or buy_completed:
                    continue
                locked_role = (
                    "BUYER_SELLER"
                    if bool(getattr(agent, "owned_properties", []) or [])
                    else "BUYER"
                )
                role_pools[locked_role][agent_id] = agent
                locked_allocations[locked_role].append(int(agent_id))
                remaining.pop(agent_id, None)

        for agent_id, agent in remaining.items():
            prop_count = self._safe_property_count(agent)
            role_pools["BUYER"][agent_id] = agent
            if prop_count > 0:
                role_pools["SELLER"][agent_id] = agent
                role_pools["BUYER_SELLER"][agent_id] = agent

        role_order = ["BUYER_SELLER", "SELLER", "BUYER"]
        selected_ids: Dict[str, List[int]] = {
            "BUYER": list(locked_allocations["BUYER"]),
            "SELLER": list(locked_allocations["SELLER"]),
            "BUYER_SELLER": list(locked_allocations["BUYER_SELLER"]),
        }
        allocated_quota: Dict[str, int] = {
            role: int(len(selected_ids.get(role, [])))
            for role in role_order
        }

        taken_ids = set()
        for role in role_order:
            taken_ids.update(int(x) for x in selected_ids.get(role, []))

        for role in role_order:
            role_request = int(requested_quota.get(role, 0) or 0)
            remaining_slots = max(0, role_request - int(allocated_quota.get(role, 0)))
            if remaining_slots <= 0:
                continue
            eligible_agents = [
                agent
                for agent_id, agent in role_pools.get(role, {}).items()
                if int(agent_id) not in taken_ids
            ]
            eligible_agents.sort(
                key=lambda agent_obj: self._forced_role_sort_key(
                    agent_obj,
                    role=role,
                    selection_policy=selection_policy,
                ),
                reverse=True,
            )
            for agent in eligible_agents[:remaining_slots]:
                agent_id = int(getattr(agent, "id", 0) or 0)
                if agent_id <= 0 or agent_id in taken_ids:
                    continue
                selected_ids[role].append(int(agent_id))
                allocated_quota[role] = int(allocated_quota.get(role, 0) or 0) + 1
                taken_ids.add(int(agent_id))

        decisions: List[Dict] = []
        for role in role_order:
            requested = int(requested_quota.get(role, 0) or 0)
            for slot_idx, agent_id in enumerate(selected_ids.get(role, []), start=1):
                agent = role_pools.get(role, {}).get(int(agent_id))
                if agent is None:
                    continue
                buyer_score = round(float(self._buyer_affordability_proxy(agent)), 2)
                seller_score = round(float(self._seller_pressure_proxy(agent)), 2)
                reason = (
                    f"forced_role_mode role={role} slot={slot_idx}/{max(1, requested)} "
                    f"policy={selection_policy} buyer_score={buyer_score} seller_score={seller_score}"
                )
                decisions.append(
                    {
                        "id": int(agent_id),
                        "role": role,
                        "trigger": "forced_role_mode",
                        "reason": reason,
                        "life_pressure": self._forced_role_life_pressure(agent, role),
                        "price_expectation": 1.0,
                        "risk_mode": self._forced_role_risk_mode(agent),
                        "listing_action": "KEEP",
                        "_decision_origin": "forced_role_mode",
                        "_llm_called": False,
                        "_forced_role_mode": True,
                        "_skip_signature_cache": True,
                        "_forced_role_policy": selection_policy,
                        "_forced_role_slot": int(slot_idx),
                    }
                )

        eligible_counts = {
            "BUYER": int(len(role_pools["BUYER"])),
            "SELLER": int(len(role_pools["SELLER"])),
            "BUYER_SELLER": int(len(role_pools["BUYER_SELLER"])),
        }
        shortfalls = {
            role: max(0, int(requested_quota.get(role, 0) or 0) - int(allocated_quota.get(role, 0) or 0))
            for role in role_order
        }
        summary: Dict[str, object] = {
            "enabled": True,
            "month": int(month),
            "selection_policy": selection_policy,
            "requested_quota": {
                "BUYER": int(requested_quota.get("BUYER", 0) or 0),
                "SELLER": int(requested_quota.get("SELLER", 0) or 0),
                "BUYER_SELLER": int(requested_quota.get("BUYER_SELLER", 0) or 0),
            },
            "allocated_quota": {
                "BUYER": int(allocated_quota.get("BUYER", 0) or 0),
                "SELLER": int(allocated_quota.get("SELLER", 0) or 0),
                "BUYER_SELLER": int(allocated_quota.get("BUYER_SELLER", 0) or 0),
            },
            "eligible_counts": eligible_counts,
            "shortfalls": shortfalls,
            "locked_allocations": {
                role: [int(x) for x in ids]
                for role, ids in locked_allocations.items()
            },
            "selected_agent_ids": {
                role: [int(x) for x in ids]
                for role, ids in selected_ids.items()
            },
        }
        self._last_forced_role_summary = dict(summary)
        self._forced_role_history.append(dict(summary))
        logger.info(
            "ForcedRoleMode month=%s policy=%s requested=%s allocated=%s shortfalls=%s",
            int(month),
            selection_policy,
            summary["requested_quota"],
            summary["allocated_quota"],
            summary["shortfalls"],
        )
        return decisions, summary

    @staticmethod
    def _bucketize(value: float, step: float = 1.0, min_v: float = 0.0, max_v: float = 1e12) -> int:
        try:
            v = max(min_v, min(max_v, float(value)))
            return int(v // max(1e-9, float(step)))
        except Exception:
            return 0

    def _build_role_signature(self, agent: Agent, market_trend: str, recent_bulletins: List[Dict]) -> str:
        """
        Build coarse signature for ROLE_DECISION cache reuse.
        Intentionally excludes agent_id to enable cross-agent reuse by cohort.
        """
        marital = str(getattr(agent, "marital_status", "unknown"))
        children = len(getattr(agent, "children_ages", []) or [])
        info_delay = int(getattr(agent, "info_delay_months", 0) or 0)
        monthly_event = str(getattr(agent, "monthly_event", "") or "")
        agent_type = str(getattr(agent, "agent_type", "normal") or "normal").lower()
        lifecycle_key = "|".join(
            build_activation_lifecycle_packet(
                agent,
                month=0,
                min_cash_observer=self._get_min_cash_observer_threshold(),
                holding_lock_months=self._min_holding_months_before_resale(),
            ).get("labels", [])[:6]
        )
        bulletin_head = ""
        if recent_bulletins:
            # Keep only first 2 bulletins to avoid high-churn signature noise.
            top = [str(x) for x in recent_bulletins[:2]]
            bulletin_head = " | ".join(top)
        payload = {
            "income_bucket": self._bucketize(getattr(agent, "monthly_income", 0.0), step=5000, min_v=0.0, max_v=500000),
            "cash_bucket": self._bucketize(getattr(agent, "cash", 0.0), step=100000, min_v=0.0, max_v=20000000),
            "props_bucket": len(getattr(agent, "owned_properties", []) or []),
            "marital": marital,
            "children": int(children),
            "trend": str(market_trend or "STABLE"),
            "event": monthly_event[:48],
            "agent_type": agent_type,
            "delay": info_delay,
            "purchase_motive_primary": str(getattr(agent.story, "purchase_motive_primary", "") or "")[:32],
            "housing_stage": str(getattr(agent.story, "housing_stage", "") or "")[:32],
            "family_stage": str(getattr(agent.story, "family_stage", "") or "")[:32],
            "education_path": str(getattr(agent.story, "education_path", "") or "")[:32],
            "financial_profile": str(getattr(agent.story, "financial_profile", "") or "")[:32],
            "lifecycle_key": lifecycle_key,
            "bulletin_hash": hashlib.sha1(bulletin_head.encode("utf-8", errors="ignore")).hexdigest()[:12],
        }
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()

    def _should_signature_cache_recheck(self, signature: str, month: int, agent_id: int, ratio: float) -> bool:
        ratio = max(0.0, min(1.0, float(ratio or 0.0)))
        if ratio <= 0.0:
            return False
        if ratio >= 1.0:
            return True
        seed = self.config.get("simulation.random_seed", 42)
        salt = f"{seed}:{int(month)}:{int(agent_id)}:{signature}:role_cache_recheck"
        return random.Random(salt).random() < ratio

    def _cache_lookup_role_decision(self, signature: str, month: int, ttl_months: int):
        entry = self._role_signature_cache.get(signature)
        if not entry:
            return None
        cached_month = int(entry.get("month", 0) or 0)
        if int(month) - cached_month > max(0, int(ttl_months)):
            return None
        return entry.get("decision")

    def _cache_store_role_decision(self, signature: str, month: int, decision: Dict):
        if not signature or not isinstance(decision, dict):
            return
        if str(decision.get("role", "OBSERVER") or "OBSERVER").upper() != "OBSERVER":
            return
        # Bound cache size to avoid long-run uncontrolled growth.
        if len(self._role_signature_cache) > 10000:
            # drop oldest-ish 20% by insertion order
            for i, k in enumerate(list(self._role_signature_cache.keys())):
                self._role_signature_cache.pop(k, None)
                if i >= 2000:
                    break
        self._role_signature_cache[signature] = {
            "month": int(month),
            "decision": dict(decision),
        }

    def _table_columns(self, table_name: str) -> set:
        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        return {row[1] for row in cursor.fetchall()}

    def _active_rows_latest(self, cursor) -> List[sqlite3.Row]:
        cols = self._table_columns("active_participants")
        if "month" in cols:
            cursor.execute("""
                SELECT ap.*
                FROM active_participants ap
                JOIN (
                    SELECT agent_id, MAX(month) AS max_month
                    FROM active_participants
                    GROUP BY agent_id
                ) latest
                ON ap.agent_id = latest.agent_id
                AND (
                    ap.month = latest.max_month
                    OR (ap.month IS NULL AND latest.max_month IS NULL)
                )
            """)
            return cursor.fetchall()
        cursor.execute("SELECT * FROM active_participants")
        return cursor.fetchall()

    def _resolve_smart_agent_ids(self, planned_agent_count: int) -> set:
        """Resolve which agent IDs should be labeled as smart for this run."""
        enabled = bool(
            self.config.get("smart_agent_enabled", self.config.get("smart_agent.enabled", False))
        )
        if not enabled or planned_agent_count <= 0:
            return set()

        explicit = self.config.get("smart_agent_count", self.config.get("smart_agent.count", None))
        target_count = None
        try:
            if explicit is not None and str(explicit).strip() != "":
                target_count = int(explicit)
        except Exception:
            target_count = None

        if target_count is None:
            try:
                ratio = float(self.config.get("smart_agent_ratio", self.config.get("smart_agent.ratio", 0.30)))
            except Exception:
                ratio = 0.30
            ratio = max(0.0, min(1.0, ratio))
            target_count = int(round(planned_agent_count * ratio))

        target_count = max(0, min(planned_agent_count, target_count))
        if target_count == 0:
            return set()

        return set(random.sample(range(1, planned_agent_count + 1), target_count))

    def _resolve_info_delay_months(self, is_smart_agent: bool) -> int:
        """
        M14: assign information delay according to the current persona-specific ratios.
        Delay means agent sees older bulletin context by N months.
        """
        rng = random
        return self._resolve_info_delay_months_with_rng(is_smart_agent=is_smart_agent, rng=rng)

    def _resolve_info_delay_months_with_rng(self, is_smart_agent: bool, rng) -> int:
        """
        Resolve information delay using the provided RNG so initialization and resume-refresh
        can stay deterministic under the same seed/config.
        """
        apply_to_normal = bool(
            self.config.get(
                "smart_agent.info_delay_apply_to_normal",
                self.config.get("info_delay_apply_to_normal", False),
            )
        )
        if (not is_smart_agent) and (not apply_to_normal):
            return 0

        enabled = bool(self.config.get("smart_agent.info_delay_enabled", self.config.get("info_delay_enabled", False)))
        if not enabled:
            return 0

        try:
            if is_smart_agent:
                base_ratio = float(
                    self.config.get("smart_agent.info_delay_ratio", self.config.get("info_delay_ratio", 0.5))
                )
            else:
                base_ratio = float(
                    self.config.get(
                        "smart_agent.info_delay_ratio_normal",
                        self.config.get("info_delay_ratio_normal", 0.0),
                    )
                )
        except Exception:
            base_ratio = 0.5 if is_smart_agent else 0.0

        try:
            ratio_multiplier = float(
                self.config.get(
                    "smart_agent.info_delay_ratio_multiplier",
                    self.config.get("info_delay_ratio_multiplier", 1.0),
                )
            )
        except Exception:
            ratio_multiplier = 1.0

        ratio = base_ratio * max(0.0, ratio_multiplier)
        ratio = max(0.0, min(1.0, ratio))
        if rng.random() > ratio:
            return 0

        try:
            min_delay = int(
                self.config.get("smart_agent.info_delay_min_months", self.config.get("info_delay_min_months", 1))
            )
        except Exception:
            min_delay = 1
        min_delay = max(1, min(6, min_delay))

        try:
            max_delay = int(
                self.config.get("smart_agent.info_delay_max_months", self.config.get("info_delay_max_months", 2))
            )
        except Exception:
            max_delay = 2
        max_delay = max(0, min(6, max_delay))
        if max_delay <= 0:
            return 0
        if max_delay < min_delay:
            max_delay = min_delay
        return int(rng.randint(min_delay, max_delay))

    def _deterministic_info_delay_months(self, agent_id: int, is_smart_agent: bool) -> int:
        seed = self.config.get("simulation.random_seed", 42)
        salt = f"{seed}:{int(agent_id)}:{'smart' if is_smart_agent else 'normal'}:info_delay"
        rng = random.Random(salt)
        return self._resolve_info_delay_months_with_rng(is_smart_agent=is_smart_agent, rng=rng)

    def refresh_info_delay_assignments(self) -> Dict[str, int]:
        """
        Recompute M14 delays from the current config.
        Required for resume-from-snapshot runs where agents_static was created under a
        different plan and would otherwise keep stale zero-delay assignments.
        """
        updated_rows = []
        delayed_total = 0
        smart_delayed = 0
        normal_delayed = 0
        for agent in self.agents:
            is_smart = str(getattr(agent, "agent_type", "normal") or "normal").lower() == "smart"
            delay = int(self._deterministic_info_delay_months(int(agent.id), is_smart) or 0)
            agent.info_delay_months = delay
            updated_rows.append((delay, int(agent.id)))
            if delay > 0:
                delayed_total += 1
                if is_smart:
                    smart_delayed += 1
                else:
                    normal_delayed += 1

        if updated_rows and self.is_v2:
            cursor = self.conn.cursor()
            cursor.executemany(
                "UPDATE agents_static SET info_delay_months = ? WHERE agent_id = ?",
                updated_rows,
            )
            self.conn.commit()

        stats = {
            "total_agents": len(self.agents),
            "delayed_total": delayed_total,
            "smart_delayed": smart_delayed,
            "normal_delayed": normal_delayed,
        }
        logger.info(
            "InfoDelayRefresh total=%s delayed=%s smart_delayed=%s normal_delayed=%s",
            stats["total_agents"],
            stats["delayed_total"],
            stats["smart_delayed"],
            stats["normal_delayed"],
        )
        return stats

    def _resolve_smart_finance_scale(self, key: str, default_value: float = 1.0) -> float:
        """Resolve smart finance multiplier with defensive bounds."""
        raw = self.config.get(f"smart_agent.{key}", self.config.get(key, default_value))
        try:
            value = float(raw)
        except Exception:
            value = float(default_value)
        return max(0.1, min(5.0, value))

    def _resolve_init_supply_targets(self, market_properties: List[Dict], planned_agent_count: int) -> Dict[str, int]:
        """
        Resolve minimum tradable-supply targets for initialization.
        Defaults follow the plan's hard-check baseline and can be overridden by config.
        """
        zone_totals: Dict[str, int] = {"A": 0, "B": 0}
        for p in market_properties:
            zone = str(p.get("zone", "")).upper()
            if zone in zone_totals:
                zone_totals[zone] += 1

        def _to_int(key: str, default_value: int) -> int:
            try:
                return int(self.config.get(key, default_value))
            except Exception:
                return int(default_value)

        try:
            owner_ratio = float(self.config.get("smart_agent.init_min_owner_ratio_by_zone", 0.08))
        except Exception:
            owner_ratio = 0.08
        owner_ratio = max(0.0, min(0.5, owner_ratio))
        try:
            listing_ratio = float(self.config.get("smart_agent.init_min_for_sale_ratio_by_zone", 0.02))
        except Exception:
            listing_ratio = 0.02
        listing_ratio = max(0.0, min(0.5, listing_ratio))
        try:
            tradable_ratio = float(self.config.get("smart_agent.init_min_tradable_ratio_total", 0.08))
        except Exception:
            tradable_ratio = 0.08
        tradable_ratio = max(0.0, min(0.5, tradable_ratio))

        owner_floor = max(0, _to_int("smart_agent.init_min_owner_floor_by_zone", 8))
        listing_floor = max(0, _to_int("smart_agent.init_min_for_sale_floor_by_zone", 4))
        tradable_floor = max(0, _to_int("smart_agent.init_min_tradable_floor_total", 10))

        def _zone_ratio(base_value: float, zone: str, key_stem: str, default_floor: float) -> float:
            zone_value = self.config.get(
                f"smart_agent.{key_stem}_zone_{zone.lower()}",
                self.config.get(f"smart_agent.{key_stem}_{zone.lower()}", None),
            )
            if zone_value is None:
                value = max(base_value, default_floor)
            else:
                try:
                    value = float(zone_value)
                except Exception:
                    value = max(base_value, default_floor)
            return max(0.0, min(0.5, float(value)))

        def _zone_floor(base_value: int, zone: str, key_stem: str, default_floor: int) -> int:
            zone_value = self.config.get(
                f"smart_agent.{key_stem}_zone_{zone.lower()}",
                self.config.get(f"smart_agent.{key_stem}_{zone.lower()}", None),
            )
            if zone_value is None:
                value = max(int(base_value), int(default_floor))
            else:
                try:
                    value = int(zone_value)
                except Exception:
                    value = max(int(base_value), int(default_floor))
            return max(0, int(value))

        owner_ratio_a = _zone_ratio(owner_ratio, "A", "init_min_owner_ratio", owner_ratio)
        owner_ratio_b = _zone_ratio(owner_ratio, "B", "init_min_owner_ratio", 0.10)
        listing_ratio_a = _zone_ratio(listing_ratio, "A", "init_min_for_sale_ratio", listing_ratio)
        listing_ratio_b = _zone_ratio(listing_ratio, "B", "init_min_for_sale_ratio", 0.04)

        owner_floor_a = _zone_floor(owner_floor, "A", "init_min_owner_floor", owner_floor)
        owner_floor_b = _zone_floor(owner_floor, "B", "init_min_owner_floor", 10)
        listing_floor_a = _zone_floor(listing_floor, "A", "init_min_for_sale_floor", listing_floor)
        listing_floor_b = _zone_floor(listing_floor, "B", "init_min_for_sale_floor", 8)

        target_a_owner = max(owner_floor_a, int(zone_totals["A"] * owner_ratio_a)) if zone_totals["A"] > 0 else 0
        target_b_owner = max(owner_floor_b, int(zone_totals["B"] * owner_ratio_b)) if zone_totals["B"] > 0 else 0
        target_a_for_sale = max(listing_floor_a, int(zone_totals["A"] * listing_ratio_a)) if zone_totals["A"] > 0 else 0
        target_b_for_sale = max(listing_floor_b, int(zone_totals["B"] * listing_ratio_b)) if zone_totals["B"] > 0 else 0
        target_total_tradable = max(tradable_floor, int(max(0, planned_agent_count) * tradable_ratio))

        return {
            "zone_a_total": zone_totals["A"],
            "zone_b_total": zone_totals["B"],
            "zone_a_owner_target": target_a_owner,
            "zone_b_owner_target": target_b_owner,
            "zone_a_for_sale_target": target_a_for_sale,
            "zone_b_for_sale_target": target_b_for_sale,
            "tradable_total_target": target_total_tradable,
        }

    def _enforce_init_supply_coverage(
        self,
        market_properties: List[Dict],
        property_updates: List[Tuple[int, str, int]],
        planned_agent_count: int,
    ) -> Dict[str, int]:
        """
        Patch initialization coverage to avoid structural zero-supply zones.
        This is setup-time supply hygiene only (does not override runtime LLM decisions).
        """
        if not self.agents or not market_properties:
            return {}

        targets = self._resolve_init_supply_targets(market_properties, planned_agent_count)
        zone_keys = ("A", "B")
        props_by_zone: Dict[str, List[Dict]] = {z: [] for z in zone_keys}
        for prop in market_properties:
            zone = str(prop.get("zone", "")).upper()
            if zone in props_by_zone:
                props_by_zone[zone].append(prop)

        def _preserve_no_home_bucket(agent: Agent) -> bool:
            init_constraints = getattr(agent, "profile_initialization_constraints", {}) or {}
            if bool(init_constraints.get("preserve_no_home", False)):
                return True
            story = getattr(agent, "story", None)
            housing_stage = str(getattr(story, "housing_stage", "") or "").strip().lower()
            purchase_motive = str(getattr(story, "purchase_motive_primary", "") or "").strip().lower()
            no_home_stages = {
                "starter_no_home",
                "no_home_first_purchase",
                "starter_entry",
            }
            no_home_motives = {
                "starter_home",
                "starter_no_home",
                "rent_to_buy",
                "new_family_first_home",
            }
            return housing_stage in no_home_stages or purchase_motive in no_home_motives

        # Keep assignment fair: preferentially assign to agents with fewer holdings.
        def _pick_agent_for_new_property() -> Agent:
            eligible_agents = [a for a in self.agents if not _preserve_no_home_bucket(a)]
            if not eligible_agents:
                eligible_agents = list(self.agents)
            return min(eligible_agents, key=lambda a: len(getattr(a, "owned_properties", []) or []))

        # Ensure each zone has enough owned properties.
        owner_added_by_zone: Dict[str, int] = {z: 0 for z in zone_keys}
        for zone in zone_keys:
            owner_target = int(targets.get(f"zone_{zone.lower()}_owner_target", 0))
            owned_now = sum(1 for p in props_by_zone[zone] if int(p.get("owner_id", -1) or -1) > 0)
            missing = max(0, owner_target - owned_now)
            if missing <= 0:
                continue
            unowned = [p for p in props_by_zone[zone] if int(p.get("owner_id", -1) or -1) <= 0]
            # Prefer lower-priced tail for additional ownership seeding to preserve value-descending flavor.
            unowned.sort(key=lambda x: float(x.get("base_value", 0.0) or 0.0))
            for prop in unowned[:missing]:
                agent = _pick_agent_for_new_property()
                prop["owner_id"] = int(agent.id)
                prop["status"] = "off_market"
                agent.owned_properties.append(prop)
                owner_added_by_zone[zone] += 1
                property_updates.append((int(agent.id), str(prop.get("status", "off_market")), int(prop["property_id"])))

        # Ensure per-zone for_sale floors.
        listed_added_by_zone: Dict[str, int] = {z: 0 for z in zone_keys}
        for zone in zone_keys:
            list_target = int(targets.get(f"zone_{zone.lower()}_for_sale_target", 0))
            listed_now = sum(
                1
                for p in props_by_zone[zone]
                if int(p.get("owner_id", -1) or -1) > 0 and str(p.get("status", "")).lower() == "for_sale"
            )
            missing = max(0, list_target - listed_now)
            if missing <= 0:
                continue
            candidates = [
                p for p in props_by_zone[zone]
                if int(p.get("owner_id", -1) or -1) > 0 and str(p.get("status", "")).lower() != "for_sale"
            ]
            # Prefer more affordable units first to maximize actual match probability.
            candidates.sort(key=lambda x: float(x.get("base_value", 0.0) or 0.0))
            for prop in candidates[:missing]:
                prop["status"] = "for_sale"
                # Keep min/listed anchors explicit at initialization.
                try:
                    base_v = float(prop.get("base_value", prop.get("current_valuation", 0.0)) or 0.0)
                except Exception:
                    base_v = 0.0
                if base_v > 0:
                    prop["min_price"] = float(prop.get("min_price", base_v * 0.95) or base_v * 0.95)
                    prop["listed_price"] = float(prop.get("listed_price", base_v * random.uniform(1.02, 1.10)) or base_v * random.uniform(1.02, 1.10))
                prop["listing_month"] = 0
                listed_added_by_zone[zone] += 1
                property_updates.append((int(prop["owner_id"]), "for_sale", int(prop["property_id"])))

        # Ensure total tradable floor (owner + for_sale) if still too low.
        tradable_target = int(targets.get("tradable_total_target", 0))
        tradable_now = sum(
            1 for p in market_properties
            if int(p.get("owner_id", -1) or -1) > 0 and str(p.get("status", "")).lower() == "for_sale"
        )
        if tradable_now < tradable_target:
            missing = tradable_target - tradable_now
            tradable_candidates = [
                p for p in market_properties
                if int(p.get("owner_id", -1) or -1) > 0 and str(p.get("status", "")).lower() != "for_sale"
            ]
            tradable_candidates.sort(key=lambda x: float(x.get("base_value", 0.0) or 0.0))
            for prop in tradable_candidates[:missing]:
                prop["status"] = "for_sale"
                prop["listing_month"] = 0
                property_updates.append((int(prop["owner_id"]), "for_sale", int(prop["property_id"])))

        # Return post-adjustment snapshot for logging.
        post = {
            "zone_a_owned": sum(1 for p in props_by_zone["A"] if int(p.get("owner_id", -1) or -1) > 0),
            "zone_b_owned": sum(1 for p in props_by_zone["B"] if int(p.get("owner_id", -1) or -1) > 0),
            "zone_a_for_sale": sum(
                1
                for p in props_by_zone["A"]
                if int(p.get("owner_id", -1) or -1) > 0 and str(p.get("status", "")).lower() == "for_sale"
            ),
            "zone_b_for_sale": sum(
                1
                for p in props_by_zone["B"]
                if int(p.get("owner_id", -1) or -1) > 0 and str(p.get("status", "")).lower() == "for_sale"
            ),
            "tradable_total": sum(
                1
                for p in market_properties
                if int(p.get("owner_id", -1) or -1) > 0 and str(p.get("status", "")).lower() == "for_sale"
            ),
            "zone_a_owner_target": int(targets.get("zone_a_owner_target", 0)),
            "zone_b_owner_target": int(targets.get("zone_b_owner_target", 0)),
            "zone_a_for_sale_target": int(targets.get("zone_a_for_sale_target", 0)),
            "zone_b_for_sale_target": int(targets.get("zone_b_for_sale_target", 0)),
            "tradable_target": int(targets.get("tradable_total_target", 0)),
            "owner_added_a": int(owner_added_by_zone["A"]),
            "owner_added_b": int(owner_added_by_zone["B"]),
            "listed_added_a": int(listed_added_by_zone["A"]),
            "listed_added_b": int(listed_added_by_zone["B"]),
        }
        return post

    def _load_init_supply_db_snapshot(self, cursor, targets: Dict[str, int]) -> Dict[str, int]:
        snapshot = {
            "zone_a_owned": 0,
            "zone_b_owned": 0,
            "zone_a_for_sale": 0,
            "zone_b_for_sale": 0,
            "tradable_total": 0,
            "zone_a_owner_target": int(targets.get("zone_a_owner_target", 0)),
            "zone_b_owner_target": int(targets.get("zone_b_owner_target", 0)),
            "zone_a_for_sale_target": int(targets.get("zone_a_for_sale_target", 0)),
            "zone_b_for_sale_target": int(targets.get("zone_b_for_sale_target", 0)),
            "tradable_target": int(targets.get("tradable_total_target", 0)),
        }
        try:
            cursor.execute(
                """
                SELECT ps.zone, COUNT(*)
                FROM properties_market pm
                JOIN properties_static ps ON ps.property_id = pm.property_id
                WHERE pm.owner_id IS NOT NULL AND pm.owner_id != -1
                GROUP BY ps.zone
                """
            )
            for zone, count in cursor.fetchall() or []:
                key = "zone_a_owned" if str(zone).upper() == "A" else "zone_b_owned"
                snapshot[key] = int(count or 0)

            cursor.execute(
                """
                SELECT ps.zone, COUNT(*)
                FROM properties_market pm
                JOIN properties_static ps ON ps.property_id = pm.property_id
                WHERE pm.owner_id IS NOT NULL
                  AND pm.owner_id != -1
                  AND pm.status='for_sale'
                  AND pm.listing_month=0
                GROUP BY ps.zone
                """
            )
            for zone, count in cursor.fetchall() or []:
                key = "zone_a_for_sale" if str(zone).upper() == "A" else "zone_b_for_sale"
                snapshot[key] = int(count or 0)

            snapshot["tradable_total"] = int(snapshot["zone_a_for_sale"] + snapshot["zone_b_for_sale"])
        except Exception as e:
            logger.debug(f"failed to load init supply db snapshot: {e}")
        return snapshot

    def _build_delayed_market_context(self, recent_bulletins, market_trend: str, info_delay_months: int):
        """
        Build delayed bulletin/trend context for M14.
        - recent_bulletins expected chronological list (old -> new)
        - delay N means hide latest N bulletin entries.
        """
        delay = max(0, int(info_delay_months or 0))
        if not recent_bulletins or delay <= 0:
            return recent_bulletins or [], str(market_trend or "STABLE"), 0

        visible = list(recent_bulletins)
        if delay < len(visible):
            visible = visible[:-delay]
        else:
            visible = []

        delayed_trend = "STABLE"
        if visible:
            tail = visible[-1]
            if isinstance(tail, dict):
                delayed_trend = str(tail.get("trend") or "STABLE")
        return visible, delayed_trend, delay

    def _build_delayed_bulletin_text(self, market_bulletin: str, recent_bulletins, info_delay_months: int) -> str:
        """Build seller-facing bulletin text under M14 delay without leaking latest market state."""
        delay = max(0, int(info_delay_months or 0))
        if delay <= 0:
            return str(market_bulletin or "")

        visible, delayed_trend, applied = self._build_delayed_market_context(
            recent_bulletins=recent_bulletins,
            market_trend="STABLE",
            info_delay_months=delay,
        )
        if visible:
            tail = visible[-1] if isinstance(visible[-1], dict) else {}
            v_month = int(tail.get("month", 0) or 0)
            v_avg = float(tail.get("avg_price", 0) or 0)
            v_vol = int(tail.get("volume", 0) or 0)
            return (
                f"【信息延迟视角】你当前只能看到第{v_month}月及之前的市场摘要。"
                f"趋势={delayed_trend}，成交量={v_vol}，均价={v_avg:,.0f}。"
                f"（实际市场可能已滞后{applied}个月）"
            )
        return f"【信息延迟视角】暂无可见公报数据（滞后{applied}个月），趋势按 STABLE 处理。"

    def initialize_agents(self, agent_count: int, market_properties: List[Dict]):
        """批量生成 Agent (V2 Schema)"""
        logger.info("Starting Batch Agent Generation (V2 Schema)...")
        self.agents = []
        cursor = self.conn.cursor()

        name_gen = ChineseNameGenerator(seed=random.randint(0, 10000))

        # 默认配置
        default_tier_config = AGENT_TIER_CONFIG
        default_prop_ownership = default_tier_config["init_params"]
        ordered_tiers = ["ultra_high", "high", "middle", "lower_middle", "low"]

        # 检查是否有用户自定义配置
        user_config = getattr(self.config, 'user_agent_config', None)
        # Also check _config dict in case it was saved deeply
        if not user_config and hasattr(self.config, '_config'):
            user_config = self.config._config.get('user_agent_config')

        if user_config:
            logger.info("Using User Custom Agent Configuration")
            tier_counts = {}
            tier_income_ranges = {}
            tier_prop_ranges = {}
            key_mapping = {'low_mid': 'lower_middle'}
            for u_key, u_data in user_config.items():
                internal_key = key_mapping.get(u_key, u_key)
                tier_counts[internal_key] = u_data['count']
                tier_income_ranges[internal_key] = u_data['income_range']
                tier_prop_ranges[internal_key] = u_data['property_count']
        else:
            logger.info("Using Default Agent Configuration")
            tier_dist = default_tier_config["tier_distribution"]
            total_dist = sum(tier_dist.values())
            tier_counts = {k: int((v / total_dist) * agent_count) for k, v in tier_dist.items()}
            current_sum = sum(tier_counts.values())
            diff = agent_count - current_sum
            if diff > 0:
                tier_counts["middle"] += diff
            tier_income_ranges = {}
            tier_prop_ranges = {}

        planned_total = sum(tier_counts.values())
        self.smart_agent_ids = self._resolve_smart_agent_ids(planned_total)
        if self.smart_agent_ids:
            logger.info(
                f"Smart Agent labeling enabled: {len(self.smart_agent_ids)} / {planned_total} agents"
            )
        else:
            logger.info("Smart Agent labeling disabled (all agents default to normal)")

        # Prepare Personality Weights (Investment Style)
        # Prepare Personality Weights (Investment Style)
        # neg_cfg = getattr(self.config, 'negotiation', {})
        # p_weights = neg_cfg.get('personality_weights', {
        #     'aggressive': 0.30, 'conservative': 0.30,
        #     'balanced': 0.40
        # })
        # p_styles = list(p_weights.keys())
        # p_probs = list(p_weights.values())

        current_id = 1

        # V2 Batches
        batch_static = []
        batch_finance = []
        BATCH_SIZE = 5000
        prop_idx = 0

        property_updates = []
        profiled_assignment_rows: List[Tuple[int, str, str, str, str]] = []
        profiled_property_rows: List[Tuple[int, str, str, str, str]] = []
        profiled_supply_target_total = 0
        profiled_supply_selected_total = 0
        profiled_mode_cfg = self._profiled_market_mode_cfg()
        profiled_pack = self._resolve_profiled_market_pack(profiled_mode_cfg)
        profiled_background_library = self._resolve_profiled_background_library(profiled_mode_cfg)
        profiled_persona_mode = str(
            profiled_mode_cfg.get("persona_generation_mode", "code_only") or "code_only"
        )
        profiled_assignments: List[Optional[str]] = [None] * int(planned_total)
        profiled_bucket_defs: Dict[str, Dict[str, Any]] = {}
        if bool(profiled_mode_cfg.get("enabled", False)) and profiled_pack:
            profiled_assignments, profiled_bucket_defs = self._build_profiled_agent_bucket_assignments(
                agent_count=planned_total,
                profile_pack=profiled_pack,
            )
            assigned_count = sum(1 for item in profiled_assignments if item)
            logger.info(
                "profiled_market_mode enabled: "
                f"mode={profiled_mode_cfg.get('experiment_mode')} "
                f"assigned={assigned_count}/{planned_total} "
                f"persona_mode={profiled_persona_mode}"
            )
        elif bool(profiled_mode_cfg.get("enabled", False)):
            logger.warning("profiled_market_mode enabled but no usable profile pack found; fallback to default init.")

        for tier in ordered_tiers:
            count = tier_counts.get(tier, 0)
            if count == 0:
                continue
            logger.info(f"Generating {count} agents for tier: {tier}")

            for _ in range(count):
                # Basic attrs
                age = random.randint(25, 60)

                # Income Logic
                if user_config:
                    inc_min, inc_max = tier_income_ranges[tier]
                    income = random.randint(inc_min, inc_max)
                else:
                    bounds = default_tier_config["tier_boundaries"]
                    lower_bound = bounds[tier]
                    if tier == "ultra_high":
                        income = random.randint(lower_bound, lower_bound * 5) // 12
                    else:
                        idx = ordered_tiers.index(tier)
                        if idx > 0:
                            upper = bounds[ordered_tiers[idx - 1]]
                        else:
                            upper = lower_bound * 2
                        income = random.randint(lower_bound, upper) // 12

                # Cash Logic
                cash_ratio_range = default_prop_ownership[tier]["cash_ratio"]
                cash_ratio = random.uniform(*cash_ratio_range)
                cash = income * 12 * cash_ratio

                status = random.choice(["single", "married"])
                template = get_template_for_tier(tier, random)
                name = name_gen.generate()

                agent = Agent(
                    id=current_id, name=name, age=age, marital_status=status,
                    cash=float(cash), monthly_income=float(income)
                )
                is_smart = current_id in self.smart_agent_ids
                agent.agent_type = "smart" if is_smart else "normal"
                agent.info_delay_months = self._deterministic_info_delay_months(current_id, is_smart)
                if is_smart:
                    income_scale = self._resolve_smart_finance_scale("smart_income_scale", 1.0)
                    cash_scale = self._resolve_smart_finance_scale("smart_cash_scale", 1.0)
                    agent.monthly_income = float(agent.monthly_income) * income_scale
                    agent.cash = float(agent.cash) * cash_scale
                    agent.last_month_cash = agent.cash
                # Property Allocation First (Tier 6 Fix: Assets before Story)
                if user_config:
                    p_min, p_max = tier_prop_ranges[tier]
                    target_props = random.randint(p_min, p_max)
                else:
                    prop_count_range = default_prop_ownership[tier]["property_count"]
                    target_props = random.randint(*prop_count_range)

                profile_bucket_id = None
                if current_id - 1 < len(profiled_assignments):
                    profile_bucket_id = profiled_assignments[current_id - 1]
                target_props = self._resolve_profiled_initial_property_target(
                    default_target_props=target_props,
                    bucket_id=profile_bucket_id,
                    bucket_defs=profiled_bucket_defs,
                )

                # is_prop_allocated = False
                for _ in range(target_props):
                    if prop_idx < len(market_properties):
                        prop = market_properties[prop_idx]
                        prop['owner_id'] = agent.id
                        
                        # Initialization supply seeding follows the configured listing rate.
                        if random.random() < self._initial_liquidity_seed_rate():
                            prop['status'] = 'for_sale'
                            prop['listing_month'] = 0
                        else:
                            prop['status'] = 'off_market'
                        
                        agent.owned_properties.append(prop)
                        # Update DB tracking tuple: (owner_id, status, property_id)
                        property_updates.append((agent.id, prop['status'], prop['property_id']))
                        prop_idx += 1
                        # is_prop_allocated = True

                profile_meta = self._apply_profiled_agent_bucket(
                    agent=agent,
                    bucket_id=profile_bucket_id,
                    bucket_defs=profiled_bucket_defs,
                    persona_generation_mode=profiled_persona_mode,
                )

                # Keep runtime finance snapshot in sync for downstream hard checks.
                agent.total_assets = float(agent.net_worth)

                # Story is generated by deterministic code path in this governance phase (no LLM generation).
                self._apply_code_generated_story(
                    agent=agent,
                    template=template,
                    tier=tier,
                    bucket_id=profile_bucket_id,
                    profile_meta=profile_meta,
                    background_library=profiled_background_library,
                )

                self.agents.append(agent)
                self.agent_map[agent.id] = agent
                if profile_meta:
                    profiled_assignment_rows.append(
                        (
                            int(agent.id),
                            str(profile_meta.get("bucket_id", "")),
                            str(profile_meta.get("role_side", "")),
                            "profiled_market_mode",
                            json.dumps(profile_meta, ensure_ascii=False),
                        )
                    )

                # V2 Data Pipelining
                s_dict = agent.to_v2_static_dict()
                f_dict = agent.to_v2_finance_dict()

                batch_static.append((
                    s_dict['agent_id'], s_dict['name'], s_dict['birth_year'], s_dict['marital_status'],
                    s_dict['children_ages'], s_dict['occupation'], s_dict['background_story'],
                    s_dict['investment_style'], s_dict['purchase_motive_primary'],
                    s_dict['housing_stage'], s_dict['family_stage'], s_dict['education_path'],
                    s_dict['financial_profile'], s_dict['seller_profile'],
                    s_dict['agent_type'], s_dict['info_delay_months']
                ))

                batch_finance.append((
                    f_dict['agent_id'], f_dict['monthly_income'], f_dict['cash'],
                    f_dict['total_assets'], f_dict['total_debt'], f_dict['mortgage_monthly_payment'],
                    f_dict['net_cashflow'], f_dict['max_affordable_price'],
                    f_dict['psychological_price'], f_dict['payment_tolerance_ratio'],
                    f_dict['down_payment_tolerance_ratio'], f_dict['last_price_update_month'],
                    f_dict['last_price_update_reason']
                ))

                current_id += 1

                if len(batch_static) >= BATCH_SIZE:
                    self._flush_agents(cursor, batch_static, batch_finance)
                    batch_static = []
                    batch_finance = []

        # Flush remaining
        if batch_static:
            self._flush_agents(cursor, batch_static, batch_finance)

        if bool(profiled_mode_cfg.get("enabled", False)) and profiled_pack:
            property_bucket_defs = profiled_pack.get("property_profile_buckets", {}) or {}
            mode_key = str(profiled_mode_cfg.get("experiment_mode", "abundant") or "abundant")
            for raw_bucket in property_bucket_defs.values():
                if not isinstance(raw_bucket, dict):
                    continue
                by_mode = raw_bucket.get("count_by_supply_mode", {}) or {}
                if isinstance(by_mode, dict):
                    profiled_supply_target_total += int(by_mode.get(mode_key, by_mode.get("abundant", 0)) or 0)
            profiled_property_rows = self._apply_profiled_property_supply_mode(
                market_properties=market_properties,
                profile_pack=profiled_pack,
                experiment_mode=mode_key,
            )
            profiled_supply_selected_total = int(len(profiled_property_rows))
            # Rebuild assignment updates to reflect post-bucket supply shaping.
            property_updates = [
                (
                    int(prop.get("owner_id", -1) or -1),
                    str(prop.get("status", "off_market") or "off_market"),
                    int(prop.get("property_id", 0) or 0),
                )
                for prop in market_properties
                if int(prop.get("owner_id", -1) or -1) > 0 and int(prop.get("property_id", 0) or 0) > 0
            ]

        if profiled_assignment_rows:
            self._flush_profiled_agent_assignments(cursor, profiled_assignment_rows)
        if profiled_property_rows:
            self._flush_profiled_property_assignments(cursor, profiled_property_rows)

        # Initialization supply hygiene: avoid structural zero-supply zones.
        coverage_snapshot = self._enforce_init_supply_coverage(
            market_properties=market_properties,
            property_updates=property_updates,
            planned_agent_count=planned_total,
        )
        logger.info(
            "Init supply pre-db patch snapshot: %s",
            json.dumps(coverage_snapshot, ensure_ascii=False),
        )
        init_supply_targets = self._resolve_init_supply_targets(market_properties, planned_total)

        # Flush property updates
        if property_updates:
            logger.info(f"Assigning {len(property_updates)} properties to agents...")
            # Ideally this belongs in MarketService, but AgentService orchestrated allocation.
            # We'll update both tables to be safe for now, or just V2.
            # SimulationRunner update loop did both.
            # Let's stick to V2 (properties_market) and properties (legacy if exists).
            try:
                # Update properties (V1 legacy - optional if we fully removed it)
                # Ensure we only update if table exists? Or just try/except.
                cursor.executemany("UPDATE properties SET owner_id = ?, status = ? WHERE property_id = ?", property_updates)
            except BaseException:
                pass

            cursor.executemany("UPDATE properties_market SET owner_id = ?, status = ? WHERE property_id = ?", property_updates)
            self.conn.commit()

        logger.info(f"Initialization Complete (V2). Generated {len(self.agents)} Agents.")

        # Initial Listings Logic could be here or returned to caller.
        # Let's handle it here to keep initialization self-contained.
        self._create_initial_listings(cursor)
        if bool(profiled_mode_cfg.get("enabled", False)) and profiled_pack:
            profiled_property_rows = self._build_runtime_profiled_property_assignments(
                market_properties=market_properties,
                profile_pack=profiled_pack,
                experiment_mode=str(profiled_mode_cfg.get("experiment_mode", "abundant") or "abundant"),
            )
            self._replace_profiled_property_assignments(cursor, profiled_property_rows)
            profiled_supply_selected_total = int(len(profiled_property_rows))
        init_supply_snapshot = self._load_init_supply_db_snapshot(cursor, init_supply_targets)
        logger.info(
            "Init supply coverage snapshot: "
            f"A_owned={init_supply_snapshot.get('zone_a_owned', 0)} "
            f"B_owned={init_supply_snapshot.get('zone_b_owned', 0)} "
            f"A_for_sale={init_supply_snapshot.get('zone_a_for_sale', 0)} "
            f"B_for_sale={init_supply_snapshot.get('zone_b_for_sale', 0)} "
            f"tradable={init_supply_snapshot.get('tradable_total', 0)} | "
            f"targets(A_owner={init_supply_snapshot.get('zone_a_owner_target', 0)}, "
            f"B_owner={init_supply_snapshot.get('zone_b_owner_target', 0)}, "
            f"A_for_sale={init_supply_snapshot.get('zone_a_for_sale_target', 0)}, "
            f"B_for_sale={init_supply_snapshot.get('zone_b_for_sale_target', 0)}, "
            f"tradable={init_supply_snapshot.get('tradable_target', 0)})"
            f" | profiled(target={profiled_supply_target_total}, selected={profiled_supply_selected_total})"
        )

    def _flush_agents(self, cursor, batch_static, batch_finance):
        for _retry in range(5):
            try:
                cursor.executemany("""
                    INSERT INTO agents_static (
                        agent_id, name, birth_year, marital_status, children_ages, occupation, background_story,
                        investment_style, purchase_motive_primary, housing_stage, family_stage, education_path,
                        financial_profile, seller_profile, agent_type, info_delay_months
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch_static)
                cursor.executemany("""
                    INSERT INTO agents_finance (
                        agent_id, monthly_income, cash, total_assets, total_debt, mortgage_monthly_payment,
                        net_cashflow, max_affordable_price, psychological_price,
                        payment_tolerance_ratio, down_payment_tolerance_ratio,
                        last_price_update_month, last_price_update_reason
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch_finance)
                self.conn.commit()
                break
            except sqlite3.OperationalError as e:
                if "locked" in str(e):
                    import time
                    time.sleep(0.1 * (_retry + 1))
                else:
                    raise
            except Exception as e:
                import logging
                logging.getLogger(__name__).error(f"FATAL INSERT ERROR: {e}")
                logging.getLogger(__name__).error(f"BATCH_STATIC IDS: {[row[0] for row in batch_static]}")
                raise

    def _create_initial_listings(self, cursor):
        """Create initial listings for multi-property owners."""
        try:
            if not self._init_multi_owner_listings_enabled():
                logger.info("Initial multi-owner listings disabled by config.")
                return
            initial_listings = []
            multi_owners = [a for a in self.agents if len(a.owned_properties) > 1]
            for agent in multi_owners[:max(3, len(multi_owners) // 5)]:
                props = sorted(agent.owned_properties, key=lambda x: x.get('base_value', 0))
                prop = props[0]
                listed_price = prop['base_value'] * random.uniform(1.05, 1.15)
                min_price = prop['base_value'] * 0.95
                prop['status'] = 'for_sale'
                prop['listed_price'] = listed_price
                # Tuple for UPDATE properties_market: listed_price, min_price, property_id
                initial_listings.append((listed_price, min_price, prop['property_id']))

            if initial_listings:
                cursor.executemany("""
                     UPDATE properties_market
                     SET status = 'for_sale', listed_price = ?, min_price = ?, listing_month = 0
                     WHERE property_id = ? AND owner_id IS NOT NULL
                 """, initial_listings)
                self.conn.commit()
                logger.info(f"Created {len(initial_listings)} initial listings (V2 properties_market).")
        except Exception as e:
            logger.warning(f"Could not create initial listings: {e}")

    def load_agents_from_db(self):
        """Load agents from DB for resuming."""
        logger.info("Loading agents from DB...")
        conn = self.conn
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Check V2
        try:
            cursor.execute("SELECT * FROM agents_static LIMIT 1")
            self.is_v2 = True
        except BaseException:
            self.is_v2 = False

        if self.is_v2:
            logger.info("Loading from V2 Agents tables...")
            cursor.execute("""
                SELECT s.*, f.*
                FROM agents_static s
                JOIN agents_finance f ON s.agent_id = f.agent_id
            """)
        else:
            logger.info("Loading from V1 Agents table...")
            cursor.execute("SELECT * FROM agents")

        rows = cursor.fetchall()
        self.agents = []
        for row in rows:
            row = dict(row)
            if row.get('agent_id', 0) < 0:
                continue
            age = row.get('age')
            if age is None and row.get('birth_year'):
                age = datetime.datetime.now().year - row['birth_year']

            a = Agent(
                id=row['agent_id'],
                name=row['name'],
                age=age if age else 30,
                marital_status=row['marital_status'],
                cash=float(row['cash']),
                monthly_income=float(row['monthly_income'])
            )
            a.story.occupation = row['occupation']
            a.story.background_story = row['background_story']
            a.story.purchase_motive_primary = row.get('purchase_motive_primary', '') or ''
            a.story.housing_stage = row.get('housing_stage', '') or ''
            a.story.family_stage = row.get('family_stage', '') or ''
            a.story.education_path = row.get('education_path', '') or ''
            a.story.financial_profile = row.get('financial_profile', '') or ''
            a.story.seller_profile = row.get('seller_profile', '') or ''
            a.agent_type = row.get('agent_type', 'normal') or 'normal'
            a.info_delay_months = int(row.get('info_delay_months') or 0)

            if self.is_v2:
                a.story.investment_style = row.get('investment_style', 'balanced')
            else:
                a.story.housing_need = row.get('housing_need', '')
            a.total_assets = float(row.get('total_assets', a.net_worth) or a.net_worth)
            a.total_debt = float(row.get('total_debt', 0.0) or 0.0)
            a.mortgage_monthly_payment = float(row.get('mortgage_monthly_payment', 0.0) or 0.0)
            a.net_cashflow = float(row.get('net_cashflow', a.monthly_income) or a.monthly_income)
            a.payment_tolerance_ratio = float(row.get('payment_tolerance_ratio', 0.45) or 0.45)
            a.down_payment_tolerance_ratio = float(row.get('down_payment_tolerance_ratio', 0.30) or 0.30)

            self.agents.append(a)
            self.agent_map[a.id] = a

        profiled_map = self._load_profiled_agent_bucket_map(cursor)
        if profiled_map:
            for a in self.agents:
                if a.id in profiled_map:
                    a.profile_bucket_id = profiled_map[a.id]
            logger.info(
                f"Loaded profiled_market assignments for {len(profiled_map)} agents."
            )

        # Load active participants info
        self._load_active_participants(cursor)

        logger.info(f"Loaded {len(self.agents)} agents from DB.")

    def _load_active_participants(self, cursor):
        """Load active participants and restore their preference data."""
        if self.is_v2:
            try:
                active_rows = self._active_rows_latest(cursor)
                active_map = {r['agent_id']: dict(r) for r in active_rows}
                for a in self.agents:
                    # Agent objects created from DB do not always carry runtime attrs yet.
                    a.role = getattr(a, 'role', 'OBSERVER') or 'OBSERVER'
                    a.monthly_event = getattr(a, 'monthly_event', None)
                    a.activation_trigger = getattr(a, 'activation_trigger', '') or ''
                    a.school_urgency = int(getattr(a, 'school_urgency', 0) or 0)

                    a_data = active_map.get(a.id)
                    if not a_data:
                        continue

                    # Agent object doesn't have these attrs by default until runtime.
                    a.role = a_data.get('role', 'OBSERVER')
                    if a_data.get('agent_type'):
                        a.agent_type = a_data.get('agent_type')
                    a.monthly_event = a_data.get('llm_intent_summary')
                    a.activation_trigger = a_data.get('activation_trigger', '') or ''
                    a.school_urgency = int(a_data.get('school_urgency') or 0)
                    a.timing_role = a_data.get('timing_role', '') or ''
                    a.decision_urgency = a_data.get('decision_urgency', '') or ''
                    a.lifecycle_summary = a_data.get('lifecycle_summary', '') or ''
                    a.lifecycle_labels = self._loads_lifecycle_labels(a_data.get('lifecycle_labels'))

                    # Restore preference data for buyer-side states so resume can inherit
                    # last-month search context instead of starting from a blank observer shell.
                    if a.role in ['BUYER', 'BUYER_SELLER']:
                        from agent_behavior import calculate_financial_limits
                        from models import AgentPreference

                        real_max_price, psych_price, final_op_max = calculate_financial_limits(a, market=None)
                        a.preference = AgentPreference(
                            target_zone="B",
                            max_price=final_op_max,
                            min_bedrooms=1,
                            need_school_district=False,
                            max_affordable_price=real_max_price,
                            psychological_price=psych_price
                        )

                        if a_data.get('target_zone'):
                            a.preference.target_zone = a_data['target_zone']
                        if a_data.get('max_price'):
                            a.preference.max_price = float(a_data['max_price'])
                        if a_data.get('target_buy_price') is not None:
                            a.preference.target_buy_price = float(a_data['target_buy_price'])
                        else:
                            a.preference.target_buy_price = min(a.preference.max_price, psych_price)
                        a.preference.max_wait_months = int(a_data.get('max_wait_months') or self._get_max_wait_months())
                        a.preference.risk_mode = a_data.get('risk_mode') or "balanced"

                        a.target_buy_price = float(a_data.get('target_buy_price') or a.preference.target_buy_price or 0.0)
                        a.target_sell_price = float(a_data.get('target_sell_price') or 0.0)
                        a.max_wait_months = int(a_data.get('max_wait_months') or self._get_max_wait_months())
                        a.waited_months = int(a_data.get('waited_months') or 0)
                        a.risk_mode = a_data.get('risk_mode') or a.preference.risk_mode
                        a.cooldown_months = int(a_data.get('cooldown_months') or 0)
                        a.consecutive_failures = int(a_data.get('consecutive_failures') or 0)
                        a.chain_mode = a_data.get('chain_mode') or "buy_first"
                        a.sell_completed = int(a_data.get('sell_completed') or 0)
                        a.buy_completed = int(a_data.get('buy_completed') or 0)

                        logger.debug(f"Restored preference for Buyer {a.id}: zone={a.preference.target_zone}, max_price={a.preference.max_price:,.0f}")

            except Exception as e:
                logger.warning(f"Failed to load active participants: {e}")

    def update_financials(self):
        """Monthly financial updates (Income - Expenses)."""
        # Batch update logic
        cursor = self.conn.cursor()
        batch_update = []

        for agent in self.agents:
            # ✅ Phase 3.2: Simplified Financial Update
            # Net Cashflow = Income - Mortgage Payment (no living expense calculation)
            net_cashflow = agent.monthly_income - agent.mortgage_monthly_payment

            # Update Cash
            agent.cash += net_cashflow

            # Store net_cashflow in agent for reference (optional, but good for UI)
            agent.net_cashflow = net_cashflow
            agent.total_assets = float(agent.net_worth)

            batch_update.append((round(agent.cash, 2), round(net_cashflow, 2), round(agent.total_assets, 2), agent.id))

        # Bulk Update DB
        if batch_update:
            cursor.executemany(
                "UPDATE agents_finance SET cash=?, net_cashflow=?, total_assets=? WHERE agent_id=?",
                batch_update,
            )
            self.conn.commit()

    def process_life_events(self, month: int, batch_decision_logs: List):
        """Handle stochastic life events."""
        cursor = self.conn.cursor()
        for agent in self.agents:
            agent.current_life_event = None
        if self.config.life_events:
            life_event_sample_size = int(len(self.agents) * 0.05)
            life_event_candidates = random.sample(self.agents, min(life_event_sample_size, len(self.agents)))

            for agent in life_event_candidates:
                event_result = select_monthly_event(agent, month, self.config)
                if event_result and event_result.get("event"):
                    apply_event_effects(agent, event_result, self.config)
                    agent.current_life_event = str(event_result.get("event") or "")
                    if agent.current_life_event:
                        agent.set_life_event(month, agent.current_life_event)
                    agent.total_assets = float(agent.net_worth)

                    batch_decision_logs.append((
                        agent.id, month, "LIFE_EVENT", event_result["event"],
                        "Stochastic Life Event", json.dumps(event_result), None, bool(event_result.get("llm_called", False))
                    ))

                    # Update DB
                    if self.is_v2:
                        cursor.execute(
                            "UPDATE agents_finance SET cash = ?, total_assets = ? WHERE agent_id = ?",
                            (round(agent.cash, 2), round(agent.total_assets, 2), agent.id),
                        )
                    else:
                        cursor.execute("UPDATE agents SET cash = ? WHERE agent_id = ?", (round(agent.cash, 2), agent.id))

    def update_active_participants(self, month: int, market, batch_decision_logs: List):
        """Manage existing active participants (Timeouts, Exits)."""
        cursor = self.conn.cursor()
        batch_active_delete = []
        buyers = []
        sticky_search_enabled = self._as_bool(
            self.config.get(
                "smart_agent.buyer_sticky_search_across_months_enabled",
                self.config.get("buyer_sticky_search_across_months_enabled", True),
            ),
            True,
        )
        buyer_seller_parallel_search_enabled = self._as_bool(
            self.config.get(
                "smart_agent.buyer_seller_parallel_search_enabled",
                self.config.get("buyer_seller_parallel_search_enabled", True),
            ),
            True,
        )
        # sellers = []  # Although sellers are persistent until sold usually

        if self.is_v2:
            active_rows = self._active_rows_latest(cursor)

            for row in active_rows:
                aid = row['agent_id']
                agent = self.agent_map.get(aid)
                if not agent:
                    continue

                # Sync role info
                agent.role = row['role']
                agent.life_pressure = row['life_pressure']
                agent.risk_mode = row['risk_mode'] if 'risk_mode' in row.keys() else "balanced"
                agent.cooldown_months = int(row['cooldown_months']) if 'cooldown_months' in row.keys() and row['cooldown_months'] is not None else 0
                agent.consecutive_failures = int(row['consecutive_failures']) if 'consecutive_failures' in row.keys() and row['consecutive_failures'] is not None else 0
                agent.chain_mode = row['chain_mode'] if 'chain_mode' in row.keys() and row['chain_mode'] else None
                agent.sell_completed = int(row['sell_completed']) if 'sell_completed' in row.keys() and row['sell_completed'] is not None else 0
                agent.waited_months = int(row['waited_months']) if 'waited_months' in row.keys() and row['waited_months'] is not None else 0
                agent.max_wait_months = int(row['max_wait_months']) if 'max_wait_months' in row.keys() and row['max_wait_months'] is not None else self._get_max_wait_months()
                agent.target_buy_price = float(row['target_buy_price']) if 'target_buy_price' in row.keys() and row['target_buy_price'] is not None else 0.0

                if agent.role in ["BUYER", "BUYER_SELLER"]:
                    if not hasattr(agent, "_must_continue_search"):
                        setattr(agent, "_must_continue_search", True)
                    if not hasattr(agent, "_search_exhausted"):
                        setattr(agent, "_search_exhausted", False)
                    # Buyer Timeout Logic
                    # 🔧 FIX: Read duration from DB (already cumulative), increment by 1 for this month
                    current_duration = row['role_duration']  # This is the duration BEFORE this month starts
                    new_duration = current_duration + 1  # Add 1 for the current month
                    agent.role_duration = new_duration

                    cursor.execute(
                        "UPDATE active_participants SET role_duration = ? WHERE agent_id = ?",
                        (new_duration, agent.id)
                    )

                    # M15: cooldown gate
                    if agent.cooldown_months > 0:
                        next_cooldown = max(0, agent.cooldown_months - 1)
                        cursor.execute(
                            "UPDATE active_participants SET cooldown_months = ? WHERE agent_id = ?",
                            (next_cooldown, agent.id)
                        )
                        agent.cooldown_months = next_cooldown
                        batch_decision_logs.append((
                            aid, month, "FAILURE_RECOVERY", "COOLDOWN",
                            f"cooldown active ({next_cooldown} months left)", None, None, False
                        ))
                        continue

                    # Optional legacy gate:
                    # when disabled (default), BUYER_SELLER can keep searching while selling.
                    if (
                        not buyer_seller_parallel_search_enabled
                        and agent.role == "BUYER_SELLER"
                        and agent.chain_mode == "sell_first"
                        and agent.sell_completed == 0
                    ):
                        batch_decision_logs.append((
                            aid, month, "CHAIN_MODE", "WAIT_SELL_FIRST",
                            "BUYER_SELLER waits for first sale before buying", None, None, False
                        ))
                        continue

                    # 🆕 FIX: Ensure preference is loaded for existing buyers
                    if not hasattr(agent, 'preference') or not agent.preference.target_zone:
                        pass  # Should have been loaded by load_agents_from_db logic

                    if agent.role_duration > 2:
                        should_exit, exit_reason = should_agent_exit_market(agent, market, agent.role_duration)
                        buy_completed = int(row['buy_completed']) if 'buy_completed' in row.keys() and row['buy_completed'] is not None else 0
                        sticky_should_hold = bool(
                            sticky_search_enabled
                            and buy_completed == 0
                            and bool(getattr(agent, "_must_continue_search", True))
                            and not bool(getattr(agent, "_search_exhausted", False))
                        )

                        if should_exit and not sticky_should_hold:
                            agent.role = "OBSERVER"
                            # Append extra None for context_metrics compatibility
                            batch_decision_logs.append((aid, month, "EXIT_DECISION", "OBSERVER", exit_reason, None, None, True))
                            batch_active_delete.append((aid,))
                        else:
                            if should_exit and sticky_should_hold:
                                batch_decision_logs.append((
                                    aid,
                                    month,
                                    "EXIT_DECISION",
                                    "STICKY_SEARCH_KEEP",
                                    "Sticky buyer search: keep active until成交或候选耗尽",
                                    None,
                                    None,
                                    False,
                                ))
                            buyers.append(agent)
                    else:
                        buyers.append(agent)

                elif agent.role == "SELLER":
                    # Sellers handled by listing status mostly, but they are active agents
                    pass

        if batch_active_delete:
            cursor.executemany("DELETE FROM active_participants WHERE agent_id = ?", batch_active_delete)
            self.conn.commit()

        return buyers

    def _get_min_cash_observer_threshold(self) -> float:
        """Get configurable threshold: no-property agents below this cash stay OBSERVER."""
        return float(self.config.get('decision_factors.activation.min_cash_observer_no_property', 500000))

    def _resolve_activation_batch_size(self) -> int:
        raw = self.config.get(
            "smart_agent.activation_batch_size",
            self.config.get("decision_factors.activation.batch_size", 50),
        )
        try:
            value = int(raw)
        except Exception:
            value = 50
        return max(5, min(100, value))

    def _resolve_adaptive_batch_cfg(self) -> Dict[str, float]:
        get = self.config.get
        enabled = self._as_bool(
            get("smart_agent.role_decision_optimization.adaptive_batch_size_enabled", True),
            True,
        )
        min_batch = int(get("smart_agent.role_decision_optimization.adaptive_batch_min", 20))
        max_batch = int(get("smart_agent.role_decision_optimization.adaptive_batch_max", 60))
        step = int(get("smart_agent.role_decision_optimization.adaptive_batch_step", 10))
        slow_seconds = float(
            get("smart_agent.role_decision_optimization.adaptive_latency_slow_seconds", 12.0)
        )
        fast_seconds = float(
            get("smart_agent.role_decision_optimization.adaptive_latency_fast_seconds", 5.0)
        )
        if max_batch < min_batch:
            max_batch = min_batch
        return {
            "enabled": enabled,
            "min_batch": max(5, min_batch),
            "max_batch": max(5, max_batch),
            "step": max(1, step),
            "slow_seconds": max(0.5, slow_seconds),
            "fast_seconds": max(0.1, fast_seconds),
        }

    def _resolve_role_budget_cfg(self) -> Dict[str, float]:
        get = self.config.get
        enabled = self._as_bool(
            get("smart_agent.role_decision_optimization.monthly_budget_enabled", False),
            False,
        )
        budget_calls = int(
            get(
                "smart_agent.role_decision_optimization.monthly_budget_calls",
                get("system.llm.max_calls_per_month", 200),
            )
        )
        min_keep_ratio = float(
            get("smart_agent.role_decision_optimization.budget_min_keep_ratio", 0.10)
        )
        return {
            "enabled": enabled,
            "budget_calls": max(1, budget_calls),
            "min_keep_ratio": max(0.0, min(1.0, min_keep_ratio)),
        }

    def _rollover_role_budget_if_needed(self, month: int):
        month = int(month)
        if self._role_budget_month != month:
            self._role_budget_month = month
            self._role_budget_used_calls = 0

    def _resolve_effective_activation_batch_size(self) -> int:
        base_size = self._resolve_activation_batch_size()
        cfg = self._resolve_adaptive_batch_cfg()
        if not cfg["enabled"]:
            return base_size
        if self._adaptive_activation_batch_size is None:
            self._adaptive_activation_batch_size = max(
                cfg["min_batch"],
                min(cfg["max_batch"], base_size),
            )
        return int(self._adaptive_activation_batch_size)

    def _update_adaptive_activation_batch_size(self, month: int, elapsed_seconds: float, batch_count: int):
        cfg = self._resolve_adaptive_batch_cfg()
        if not cfg["enabled"]:
            return
        if batch_count <= 0:
            return
        current = int(self._resolve_effective_activation_batch_size())
        avg_batch_seconds = float(elapsed_seconds) / max(1, int(batch_count))
        next_size = current
        reason = "hold"
        if avg_batch_seconds >= float(cfg["slow_seconds"]):
            next_size = max(int(cfg["min_batch"]), current - int(cfg["step"]))
            reason = "slow"
        elif avg_batch_seconds <= float(cfg["fast_seconds"]):
            next_size = min(int(cfg["max_batch"]), current + int(cfg["step"]))
            reason = "fast"
        self._adaptive_activation_batch_size = int(next_size)
        logger.info(
            "AdaptiveActivationBatch month=%s avg_batch_sec=%.2f batches=%s size %s->%s reason=%s",
            int(month),
            float(avg_batch_seconds),
            int(batch_count),
            int(current),
            int(next_size),
            reason,
        )

    def _is_activation_hard_only_prefilter(self) -> bool:
        raw = self.config.get(
            "smart_agent.activation_hard_only_prefilter",
            self.config.get("decision_factors.activation.pre_filter.hard_only", True),
        )
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() not in {"0", "false", "no", "off", "n"}

    def _resolve_activation_prefilter_thresholds(self, market_pulse: bool) -> Dict[str, float]:
        if market_pulse:
            min_cash = self.config.get(
                "decision_factors.activation.pre_filter.market_pulse.min_cash",
                100000,
            )
            min_income = self.config.get(
                "decision_factors.activation.pre_filter.market_pulse.min_income",
                15000,
            )
        else:
            min_cash = self.config.get(
                "decision_factors.activation.pre_filter.normal.min_cash",
                300000,
            )
            min_income = self.config.get(
                "decision_factors.activation.pre_filter.normal.min_income",
                20000,
            )
        try:
            min_cash = float(min_cash)
        except Exception:
            min_cash = 300000.0
        try:
            min_income = float(min_income)
        except Exception:
            min_income = 20000.0
        return {
            "min_cash": max(0.0, min_cash),
            "min_income": max(0.0, min_income),
        }

    def _get_max_wait_months(self) -> int:
        raw = self.config.get("smart_agent.max_wait_months", self.config.get("max_wait_months", 6))
        try:
            v = int(raw)
        except Exception:
            v = 6
        return max(1, min(24, v))

    def _resolve_buyer_deadline_months(self, agent: Agent, role_decision: Dict, pref) -> int:
        """
        Resolve buyer purchase deadline horizon (months).
        This is a code-side pacing envelope only; final property choice and bidding still rely on LLM.
        """
        enabled_raw = self.config.get(
            "smart_agent.buyer_deadline_enabled",
            self.config.get("buyer_deadline_enabled", True),
        )
        if isinstance(enabled_raw, bool):
            enabled = enabled_raw
        else:
            enabled = str(enabled_raw).strip().lower() in {"1", "true", "yes", "y", "on"}
        if not enabled:
            return int(self._get_max_wait_months())

        def _read_range(key: str, default_lo: int, default_hi: int) -> Tuple[int, int]:
            raw = self.config.get(key, [default_lo, default_hi])
            lo, hi = default_lo, default_hi
            try:
                if isinstance(raw, (list, tuple)) and len(raw) >= 2:
                    lo, hi = int(raw[0]), int(raw[1])
                elif isinstance(raw, str) and "," in raw:
                    parts = [x.strip() for x in raw.split(",")]
                    if len(parts) >= 2:
                        lo, hi = int(parts[0]), int(parts[1])
            except Exception:
                lo, hi = default_lo, default_hi
            if lo > hi:
                lo, hi = hi, lo
            lo = max(1, lo)
            hi = max(lo, hi)
            return lo, hi

        urgent_range = _read_range("smart_agent.buyer_deadline_urgent_range", 1, 3)
        normal_range = _read_range("smart_agent.buyer_deadline_balanced_range", 3, 6)
        patient_range = _read_range("smart_agent.buyer_deadline_patient_range", 6, 12)

        pressure = str((role_decision or {}).get("life_pressure", getattr(agent, "life_pressure", "patient")) or "patient").lower()
        if pressure in {"urgent", "anxious"}:
            lo, hi = urgent_range
        elif pressure in {"calm", "balanced", "normal"}:
            lo, hi = normal_range
        else:
            lo, hi = patient_range

        motive = str(getattr(getattr(agent, "story", None), "purchase_motive_primary", "") or "").lower()
        family_stage = str(getattr(getattr(agent, "story", None), "family_stage", "") or "").lower()
        school_urgency = int(getattr(agent, "school_urgency", 0) or 0)
        hard_need = motive in {"starter_home", "starter_entry", "education_driven", "chain_replacement"} or bool(
            getattr(pref, "need_school_district", False)
        )
        if school_urgency >= 2 or family_stage in {"junior_school_transition", "senior_school_transition"}:
            hard_need = True

        deadline_months = random.randint(int(lo), int(hi))
        if hard_need:
            deadline_months = max(1, deadline_months - 1)

        # Keep global ceiling/floor compatible with existing max_wait guard.
        ceiling = int(self._get_max_wait_months())
        deadline_months = max(1, min(max(1, ceiling), int(deadline_months)))
        return int(deadline_months)

    def _get_smart_threshold(self, key: str, default_value: float) -> float:
        """Read smart-agent threshold with backward-compatible key fallback."""
        raw = self.config.get(f"smart_agent.{key}", self.config.get(key, default_value))
        try:
            return float(raw)
        except Exception:
            return float(default_value)

    def _evaluate_smart_sell_triggers(self, agent: Agent, market_price_map: Dict[str, float]) -> Dict:
        """
        M11 risk triggers for smart sellers:
        - stop-loss when unrealized drawdown exceeds threshold
        - take-profit when unrealized gain exceeds threshold
        Returns normalized trigger payload for logging and guardrails.
        """
        stop_loss_threshold = max(0.0, self._get_smart_threshold("panic_sell_drawdown", 0.15))
        take_profit_threshold = max(0.0, self._get_smart_threshold("take_profit_threshold", 0.20))

        prop_metrics = []
        for p in agent.owned_properties:
            cost_basis = float(p.get("base_value", 0.0) or 0.0)
            if cost_basis <= 0:
                continue
            zone = p.get("zone", "A")
            current_market_value = float(market_price_map.get(zone, cost_basis) or cost_basis)
            drawdown = max(0.0, (cost_basis - current_market_value) / cost_basis)
            gain = max(0.0, (current_market_value - cost_basis) / cost_basis)
            prop_metrics.append({
                "property_id": int(p.get("property_id", 0)),
                "zone": zone,
                "cost_basis": cost_basis,
                "current_market_value": current_market_value,
                "drawdown": drawdown,
                "gain": gain,
            })

        stop_loss_hits = [m for m in prop_metrics if m["drawdown"] >= stop_loss_threshold]
        take_profit_hits = [m for m in prop_metrics if m["gain"] >= take_profit_threshold]
        # prioritize risk reduction: stop-loss first, then take-profit
        stop_loss_hits.sort(key=lambda x: x["drawdown"], reverse=True)
        take_profit_hits.sort(key=lambda x: x["gain"], reverse=True)

        trigger_mode = None
        trigger_ids = []
        trigger_reason = ""
        if stop_loss_hits:
            trigger_mode = "STOP_LOSS"
            trigger_ids = [x["property_id"] for x in stop_loss_hits]
            worst = stop_loss_hits[0]
            trigger_reason = (
                f"unrealized drawdown {worst['drawdown']:.1%} exceeded threshold {stop_loss_threshold:.1%}"
            )
        elif take_profit_hits:
            trigger_mode = "TAKE_PROFIT"
            trigger_ids = [x["property_id"] for x in take_profit_hits]
            best = take_profit_hits[0]
            trigger_reason = (
                f"unrealized gain {best['gain']:.1%} exceeded threshold {take_profit_threshold:.1%}"
            )

        return {
            "trigger_mode": trigger_mode,
            "trigger_reason": trigger_reason,
            "trigger_property_ids": trigger_ids,
            "stop_loss_threshold": stop_loss_threshold,
            "take_profit_threshold": take_profit_threshold,
            "property_metrics": prop_metrics,
        }

    def _resolve_chain_mode(self, agent: Agent, decision_payload: Dict) -> str:
        forced_mode = str(
            self.config.get("smart_agent.force_chain_mode", self.config.get("force_chain_mode", ""))
        ).lower().strip()
        if forced_mode in ("sell_first", "buy_first"):
            return forced_mode

        timing_role = str(decision_payload.get("timing_role", "") or "").lower().strip()
        if timing_role == TIMING_ROLE_SELL_THEN_BUY:
            return "sell_first"
        if timing_role == TIMING_ROLE_BUY_NOW and len(agent.owned_properties) > 0:
            return "buy_first"

        mode = str(decision_payload.get("chain_mode", "")).lower().strip()
        if mode in ("sell_first", "buy_first"):
            return mode
        # Conservative fallback: low liquidity replacement users sell first.
        if len(agent.owned_properties) > 0 and agent.cash < max(300000, agent.monthly_income * 12):
            return "sell_first"
        return "buy_first"

    @staticmethod
    def _loads_lifecycle_labels(raw_value) -> List[str]:
        if isinstance(raw_value, list):
            return [str(item) for item in raw_value if str(item)]
        text = str(raw_value or "").strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except Exception:
            return [segment.strip() for segment in text.split("|") if segment.strip()]
        if isinstance(parsed, list):
            return [str(item) for item in parsed if str(item)]
        return []

    def _resolve_timing_role(
        self,
        *,
        agent: Agent,
        role_str: str,
        decision_payload: Dict,
        lifecycle_packet: Optional[Dict[str, Any]] = None,
    ) -> str:
        raw = str((decision_payload or {}).get("timing_role", "") or "").strip().lower()
        if raw in VALID_TIMING_ROLES:
            return raw
        chain_mode = str((decision_payload or {}).get("chain_mode", "") or "").strip().lower()
        if role_str == "BUYER_SELLER":
            if chain_mode == "sell_first":
                return TIMING_ROLE_SELL_THEN_BUY
            return TIMING_ROLE_BUY_NOW
        if role_str == "BUYER":
            return TIMING_ROLE_BUY_NOW
        if role_str == "SELLER":
            return TIMING_ROLE_SELL_NOW
        labels = set((lifecycle_packet or {}).get("labels", []) or self._loads_lifecycle_labels((decision_payload or {}).get("lifecycle_labels")))
        demand_labels = {
            "LIFE_SHOCK",
            "SPACE_SQUEEZE",
            "DEADLINE_PRESSURE",
            "LIQUIDITY_READY",
            "CHAIN_BLOCKED",
            "CHAIN_UNLOCKED",
        }
        return TIMING_ROLE_NEED_WAIT if labels & demand_labels else TIMING_ROLE_OBSERVE_WAIT

    @staticmethod
    def _resolve_decision_urgency(decision_payload: Dict) -> str:
        raw = str((decision_payload or {}).get("urgency_level", "") or "").strip().lower()
        if raw in VALID_URGENCY_LEVELS:
            return raw
        return derive_decision_urgency(str((decision_payload or {}).get("life_pressure", "patient") or "patient"))

    def _seller_rewake_cooldown_rounds(self) -> int:
        return max(0, int(self.config.get("smart_agent.seller_rewake_cooldown_rounds", 2) or 2))

    def _seller_signal_lookback_rounds(self) -> int:
        return max(1, int(self.config.get("smart_agent.seller_signal_lookback_rounds", 2) or 2))

    @staticmethod
    def _derive_supply_bucket_id(prop: Dict[str, Any]) -> str:
        zone_bucket = str(prop.get("zone", "") or "UNK").upper()
        school_bucket = "SCHOOL" if bool(prop.get("is_school_district", False)) else "NOSCHOOL"
        property_type = str(prop.get("property_type", "") or "").lower()
        area = float(prop.get("building_area", 0.0) or 0.0)
        if any(token in property_type for token in ("villa", "improve", "改善", "大平层")):
            type_bucket = "IMPROVE"
        elif any(token in property_type for token in ("small", "刚需", "compact", "studio")):
            type_bucket = "JUST"
        else:
            type_bucket = "IMPROVE" if area >= 110.0 else "JUST"
        return f"{zone_bucket}_{school_bucket}_{type_bucket}"

    @staticmethod
    def _bucket_display_label(bucket_id: str) -> str:
        parts = [segment.strip().upper() for segment in str(bucket_id or "").split("_") if segment.strip()]
        if len(parts) >= 3 and parts[0] in {"A", "B"}:
            zone = f"{parts[0]}区"
            school = "学区" if parts[1] == "SCHOOL" else "非学区"
            demand = "刚需" if parts[2] == "JUST" else "改善"
            return f"{zone}/{school}/{demand}"
        return str(bucket_id or "未识别画像")

    def _build_seller_market_signal_packet(
        self,
        cursor: sqlite3.Cursor,
        agent: Agent,
        month: int,
        *,
        holding_lock_months: int,
    ) -> Dict[str, Any]:
        owned_properties = list(getattr(agent, "owned_properties", []) or [])
        if not owned_properties:
            return {}

        property_ids = [
            int(prop.get("property_id", -1) or -1)
            for prop in owned_properties
            if int(prop.get("property_id", -1) or -1) > 0
        ]
        state_map: Dict[int, Dict[str, Any]] = {}
        if property_ids:
            placeholders = ",".join("?" for _ in property_ids)
            try:
                cursor.execute(
                    f"""
                    SELECT pm.property_id, pm.status, pm.current_valuation, pm.listed_price, pm.acquired_month,
                           ps.zone, ps.is_school_district, ps.property_type, ps.building_area
                    FROM properties_market pm
                    LEFT JOIN properties_static ps ON ps.property_id = pm.property_id
                    WHERE pm.property_id IN ({placeholders})
                    """,
                    tuple(property_ids),
                )
                for row in cursor.fetchall() or []:
                    state_map[int(row[0])] = {
                        "status": str(row[1] or ""),
                        "current_valuation": float(row[2] or 0.0),
                        "listed_price": float(row[3] or 0.0),
                        "acquired_month": int(row[4] or 0) if row[4] is not None else 0,
                        "zone": str(row[5] or ""),
                        "is_school_district": bool(row[6]),
                        "property_type": str(row[7] or ""),
                        "building_area": float(row[8] or 0.0),
                    }
            except Exception:
                state_map = {}

        eligible_properties: List[Dict[str, Any]] = []
        for prop in owned_properties:
            property_id = int(prop.get("property_id", -1) or -1)
            merged = dict(prop or {})
            merged.update(state_map.get(property_id, {}))
            status = str(merged.get("status", "off_market") or "off_market").lower()
            acquired_month = int(merged.get("acquired_month", 0) or 0)
            if acquired_month > 0 and (int(month) - acquired_month) < max(1, int(holding_lock_months or 12)):
                continue
            if status in {"for_sale", "pending_settlement", "sold"}:
                continue
            merged["property_id"] = property_id
            merged["bucket_id"] = self._derive_supply_bucket_id(merged)
            eligible_properties.append(merged)

        last_sell_round = 0
        try:
            cursor.execute(
                """
                SELECT MAX(month)
                FROM active_participants
                WHERE agent_id = ?
                  AND timing_role = 'sell_now'
                """,
                (int(agent.id),),
            )
            row = cursor.fetchone()
            last_sell_round = int((row or [0])[0] or 0)
        except Exception:
            last_sell_round = 0

        cooldown_rounds = self._seller_rewake_cooldown_rounds()
        cooldown_active = bool(
            cooldown_rounds > 0
            and last_sell_round > 0
            and (int(month) - int(last_sell_round)) <= int(cooldown_rounds)
        )

        packet: Dict[str, Any] = {
            "eligible_property_ids": [int(item.get("property_id", -1) or -1) for item in eligible_properties],
            "eligible_bucket_ids": sorted(
                {
                    str(item.get("bucket_id", "") or "").strip()
                    for item in eligible_properties
                    if str(item.get("bucket_id", "") or "").strip()
                }
            ),
            "cooldown_rounds": int(cooldown_rounds),
            "last_sell_round": int(last_sell_round),
            "cooldown_active": bool(cooldown_active),
        }
        if cooldown_active:
            packet["cooldown_detail"] = (
                f"上次 sell_now 在第{int(last_sell_round)}回合，冷却要求 {int(cooldown_rounds)} 回合"
            )
            return packet
        if not eligible_properties:
            return packet

        bucket_ids = packet["eligible_bucket_ids"]
        lookback_start = max(1, int(month) - int(self._seller_signal_lookback_rounds()))
        metrics_by_bucket: Dict[str, Dict[str, Any]] = {}
        if bucket_ids:
            placeholders = ",".join("?" for _ in bucket_ids)
            try:
                cursor.execute(
                    f"""
                    SELECT
                        COALESCE(json_extract(match_context, '$.shortlist_item.candidate_bucket_id'), '') AS bucket_id,
                        SUM(CASE WHEN is_valid_bid = 1 THEN 1 ELSE 0 END) AS valid_bids,
                        SUM(CASE WHEN proceeded_to_negotiation = 1 THEN 1 ELSE 0 END) AS negotiations,
                        COUNT(DISTINCT CASE
                            WHEN COALESCE(json_extract(match_context, '$.shortlist_item.heat_state.real_competition_score'), 0) > 0
                            THEN property_id END
                        ) AS real_comp_props,
                        COUNT(DISTINCT CASE
                            WHEN final_outcome IN ('FILLED', 'PENDING_SETTLEMENT')
                            THEN property_id END
                        ) AS filled_props,
                        SUM(CASE WHEN failure_reason = 'NO_ACTIVE_LISTINGS' THEN 1 ELSE 0 END) AS no_active_rows,
                        COUNT(DISTINCT CASE WHEN failure_reason = 'NO_ACTIVE_LISTINGS' THEN buyer_id END) AS waiting_buyers
                    FROM property_buyer_matches
                    WHERE month BETWEEN ? AND ?
                      AND COALESCE(json_extract(match_context, '$.shortlist_item.candidate_bucket_id'), '') IN ({placeholders})
                    GROUP BY COALESCE(json_extract(match_context, '$.shortlist_item.candidate_bucket_id'), '')
                    """,
                    (int(lookback_start), max(int(month) - 1, int(lookback_start)), *bucket_ids),
                )
                for row in cursor.fetchall() or []:
                    bucket_id = str(row[0] or "").strip()
                    if not bucket_id:
                        continue
                    metrics_by_bucket[bucket_id] = {
                        "valid_bids": int(row[1] or 0),
                        "negotiations": int(row[2] or 0),
                        "real_comp_props": int(row[3] or 0),
                        "filled_props": int(row[4] or 0),
                        "no_active_rows": int(row[5] or 0),
                        "waiting_buyers": int(row[6] or 0),
                    }
            except Exception:
                metrics_by_bucket = {}

        best_bucket_id = ""
        best_bucket_metrics: Dict[str, Any] = {}
        best_score = -1.0
        for bucket_id in bucket_ids:
            metrics = dict(metrics_by_bucket.get(bucket_id, {}) or {})
            score = (
                float(metrics.get("valid_bids", 0)) * 1.5
                + float(metrics.get("negotiations", 0)) * 1.5
                + float(metrics.get("real_comp_props", 0)) * 2.0
                + float(metrics.get("filled_props", 0)) * 2.0
                + float(metrics.get("no_active_rows", 0)) * 1.0
            )
            if score > best_score:
                best_score = score
                best_bucket_id = bucket_id
                best_bucket_metrics = metrics

        if best_bucket_id:
            waiting_buyers = int(best_bucket_metrics.get("waiting_buyers", 0) or 0)
            no_active_rows = int(best_bucket_metrics.get("no_active_rows", 0) or 0)
            valid_bids = int(best_bucket_metrics.get("valid_bids", 0) or 0)
            real_comp_props = int(best_bucket_metrics.get("real_comp_props", 0) or 0)
            filled_props = int(best_bucket_metrics.get("filled_props", 0) or 0)
            packet["best_bucket_id"] = best_bucket_id
            packet["best_bucket_display"] = self._bucket_display_label(best_bucket_id)
            packet["best_bucket_metrics"] = best_bucket_metrics
            packet["local_price_push_window"] = bool(
                valid_bids >= 2
                or real_comp_props >= 1
                or filled_props >= 1
            )
            packet["local_price_push_detail"] = (
                f"{packet['best_bucket_display']} 最近存在真实承接: 有效出价{valid_bids}，"
                f"真实竞争房源{real_comp_props}，成交房源{filled_props}"
            )
            packet["scarcity_match_window"] = bool(waiting_buyers >= 1 or no_active_rows >= 2)
            packet["scarcity_detail"] = (
                f"{packet['best_bucket_display']} 当前缺口明显: NO_ACTIVE_LISTINGS={no_active_rows}，等待买家={waiting_buyers}"
            )

        replacement_ready = bool(getattr(agent, "buy_completed", 0)) and bool(eligible_properties)
        if replacement_ready:
            release_bucket = packet.get("best_bucket_display") or self._bucket_display_label(
                str(eligible_properties[0].get("bucket_id", "") or "")
            )
            packet["replacement_old_home_release"] = True
            packet["replacement_release_detail"] = (
                f"已完成买入后仍持有 {len(eligible_properties)} 套可售旧房，可评估释放 {release_bucket}"
            )
        else:
            packet["replacement_old_home_release"] = False

        return packet

    def _resolve_risk_mode(self, agent: Agent, decision_payload: Dict, pref=None) -> str:
        mode = str(decision_payload.get("risk_mode", "")).lower().strip()
        if mode not in ("conservative", "balanced", "aggressive"):
            mode = str(getattr(pref, "risk_mode", "")).lower().strip() if pref else ""
        if mode not in ("conservative", "balanced", "aggressive"):
            style = str(getattr(agent.story, "investment_style", "balanced")).lower()
            mode = "aggressive" if style == "aggressive" else "conservative" if style == "conservative" else "balanced"
        return mode

    def _resolve_target_buy_price(self, pref, price_factor: float) -> float:
        max_price = float(getattr(pref, "max_price", 0) or 0)
        psych_price = float(getattr(pref, "psychological_price", 0) or 0)
        target_from_pref = float(getattr(pref, "target_buy_price", 0) or 0)
        anchor = target_from_pref if target_from_pref > 0 else (psych_price if psych_price > 0 else max_price)
        if anchor <= 0:
            return 0.0
        target = min(max_price, anchor * max(0.8, min(1.2, float(price_factor or 1.0))))
        return max(0.0, target)

    def pre_filter_activation_candidates(
        self,
        agents: List[Agent],
        market_pulse: bool = False,
        min_cash_observer: float = 500000
    ) -> List[Agent]:
        """
        Stage 1 Filter: Rule-based selection of high-potential agents.
        Returns a list of agents who qualify for LLM consideration.
        """
        candidates = []
        
        thresholds = self._resolve_activation_prefilter_thresholds(market_pulse=market_pulse)
        min_cash = float(thresholds["min_cash"])
        min_income = float(thresholds["min_income"])
        hard_only = self._is_activation_hard_only_prefilter()
            
        for agent in agents:
            # 1. Active Participants are skipped (already handled)
            if hasattr(agent, 'role') and agent.role in ['BUYER', 'SELLER', 'BUYER_SELLER']:
                continue

            # 2. Hard Rule (Configurable): no-property + low-cash => skip activation
            if not agent.owned_properties and agent.cash < min_cash_observer:
                continue

            # 3. Existing Property Owners (Potential Sellers)
            if agent.owned_properties:
                candidates.append(agent)
                continue

            # 4. Hard-only mode: keep all no-property agents above observer floor.
            # This avoids over-filtering and preserves possible成交机会.
            if hard_only:
                candidates.append(agent)
                continue

            # 5. Soft thresholds (token-saving mode): High Net Worth / High Income
            if agent.cash > min_cash or agent.monthly_income > min_income:
                candidates.append(agent)
                continue
                
        return candidates

    async def activate_new_agents(
        self,
        month,
        market,
        macro_desc,
        batch_decision_logs,
        market_trend="STABLE",
        market_bulletin="",
        recent_bulletins=[],
    ):
        """Select candidates and run LLM activation."""
        cursor = self.conn.cursor()
        candidates = []
        self._last_forced_role_summary = None
        profiled_mode_cfg = self._profiled_market_mode_cfg()
        profiled_pack = self._resolve_profiled_market_pack(profiled_mode_cfg)
        bucket_pressure_map: Dict[str, Dict[str, Any]] = {}
        if profiled_pack:
            bucket_pressure_map = self._attach_activation_persona_packets(cursor, profiled_pack)

        if self.is_v2:
            # 🆕 Stage 1: Pre-filter using Rule Engine (0 Token Cost)
            market_pulse = False 
            # Check market pulse from bulletin or trend
            # 🆕 Added "超值" and "特价" to capture Developer Fire Sales (Leading Indicator)
            if any(k in market_trend for k in ["PANIC", "BOOM"]) or \
               any(k in market_bulletin for k in ["Crash", "超值", "特价", "Fire Sale"]):
                market_pulse = True
            
            # Also check if we passed a specific pulse flag (TODO: pass from simulation_runner)
            # For now, just rely on strict trend strings
            
            min_cash_observer = self._get_min_cash_observer_threshold()
            candidates = self.pre_filter_activation_candidates(
                self.agents,
                market_pulse,
                min_cash_observer=min_cash_observer
            )
            # Hard include: outbid-locked buyers must stay in buy lane until success/exhaust.
            # They bypass pre-filter skips so post-outbid chain continuity does not break.
            candidate_ids = {int(getattr(a, "id", -1)) for a in candidates}
            for a in self.agents:
                try:
                    a_id = int(getattr(a, "id", -1))
                except Exception:
                    a_id = -1
                if a_id <= 0 or a_id in candidate_ids:
                    continue
                if bool(getattr(a, "_buy_task_locked", False)) and (not bool(getattr(a, "_search_exhausted", False))) and (not bool(getattr(a, "buy_completed", 0))):
                    candidates.append(a)
                    candidate_ids.add(a_id)
        else:
            min_cash_observer = self._get_min_cash_observer_threshold()
        opt_cfg = self._role_opt_cfg()
        stage1_candidate_count = len(candidates)
        low_cut = max(0.0, min(1.0, float(opt_cfg["low_score_observer_threshold"])))
        high_cut = max(low_cut, min(1.0, float(opt_cfg["high_score_candidate_threshold"])))
        freeze_enabled = bool(opt_cfg["enable_observer_freeze"])
        freeze_trigger = max(1, int(opt_cfg["observer_freeze_trigger_streak"]))
        freeze_window = max(1, int(opt_cfg["observer_freeze_window_months"]))
        cache_enabled = bool(opt_cfg["enable_signature_cache"])
        cache_ttl = max(0, int(opt_cfg["signature_cache_ttl_months"]))
        cache_recheck_ratio = max(0.0, min(1.0, float(opt_cfg["signature_cache_random_recheck_ratio"])))
        router_enabled = bool(opt_cfg["enable_uncertainty_router"])

        # ROLE_DECISION pre-routing optimizations:
        # - low-score direct OBSERVER
        # - stable observer freeze window
        # - signature cache reuse (TTL)
        synthetic_decisions = []
        llm_candidate_entries: List[Dict] = []
        hybrid_targeted_map: Dict[int, Dict[str, Any]] = {}
        optimization_counters = {
            "routed_low_score_observer": 0,
            "observer_frozen": 0,
            "signature_cache_hit": 0,
            "budget_downgraded_observer": 0,
            "hybrid_bucket_targeted": 0,
        }

        activation_governance_cfg = self._activation_governance_cfg(month=int(month))
        forced_role_cfg = self._forced_role_mode_cfg(month=int(month))
        if str(activation_governance_cfg.get("activation_mode", "natural") or "natural") != "forced":
            forced_role_cfg = {
                **dict(forced_role_cfg),
                "enabled": False,
            }
        if bool(forced_role_cfg.get("enabled", False)):
            synthetic_decisions, _ = self._build_forced_role_decisions(
                candidates=candidates,
                month=int(month),
                forced_cfg=forced_role_cfg,
            )
            logger.info(
                "ForcedRoleModeActivation month=%s selected=%s candidate_pool=%s",
                int(month),
                int(len(synthetic_decisions)),
                int(len(candidates)),
            )
            candidates = []

        if (
            str(activation_governance_cfg.get("activation_mode", "natural") or "natural") == "hybrid"
            and profiled_pack
        ):
            targeted_agents, hybrid_targeted_map, bucket_pressure_map = self._select_hybrid_targeted_candidates(
                cursor,
                existing_candidates=candidates,
                profile_pack=profiled_pack,
                min_cash_observer=min_cash_observer,
                targeted_score_threshold=max(0.005, float(low_cut) * 0.5 if router_enabled else 0.01),
                severe_bucket_deficit_ratio=float(
                    activation_governance_cfg.get("severe_bucket_deficit_ratio", 5.0) or 5.0
                ),
                max_targeted_total=int(
                    activation_governance_cfg.get("autofill_demand_floor", 0) or 0
                ),
            )
            if targeted_agents:
                existing_ids = {int(getattr(a, "id", -1)) for a in candidates}
                for agent in targeted_agents:
                    if int(getattr(agent, "id", -1)) in existing_ids:
                        continue
                    candidates.append(agent)
                    existing_ids.add(int(getattr(agent, "id", -1)))
                optimization_counters["hybrid_bucket_targeted"] += int(len(targeted_agents))
                logger.info(
                    "HybridActivation month=%s targeted_candidates=%s buckets=%s",
                    int(month),
                    int(len(targeted_agents)),
                    sorted({str(item.get("bucket_id", "")) for item in hybrid_targeted_map.values() if item}),
                )

        holding_lock_months = self._min_holding_months_before_resale()
        for agent in candidates:
            score = max(0.0, min(1.0, float(calculate_activation_probability(agent))))
            signature = self._build_role_signature(agent, market_trend=market_trend, recent_bulletins=recent_bulletins)
            force_buy_lock = bool(getattr(agent, "_buy_task_locked", False)) and (not bool(getattr(agent, "_search_exhausted", False))) and (not bool(getattr(agent, "buy_completed", 0)))
            targeted_meta = hybrid_targeted_map.get(int(agent.id))
            seller_signal_packet = self._build_seller_market_signal_packet(
                cursor,
                agent,
                int(month),
                holding_lock_months=holding_lock_months,
            )
            setattr(agent, "_seller_market_signal_packet", seller_signal_packet)
            lifecycle_packet = build_activation_lifecycle_packet(
                agent,
                month,
                min_cash_observer=min_cash_observer,
                holding_lock_months=holding_lock_months,
                market_signal_packet=seller_signal_packet,
            )
            lifecycle_labels = set(lifecycle_packet.get("labels", []) or [])
            seller_signal_priority = bool(
                {"LOCAL_PRICE_PUSH_WINDOW", "REPLACEMENT_OLD_HOME_RELEASE"} & lifecycle_labels
            ) and "SELLER_REWAKE_COOLDOWN" not in lifecycle_labels
            if force_buy_lock:
                llm_candidate_entries.append({"agent": agent, "score": max(float(score), float(high_cut)), "force_buy_lock": True, "targeted_bucket": bool(targeted_meta)})
                continue
            if seller_signal_priority:
                llm_candidate_entries.append(
                    {
                        "agent": agent,
                        "score": max(float(score), float(low_cut)),
                        "force_buy_lock": False,
                        "targeted_bucket": bool(targeted_meta),
                        "seller_signal_priority": True,
                    }
                )
                setattr(agent, "_role_signature_last", signature)
                continue
            if targeted_meta:
                llm_candidate_entries.append(
                    {
                        "agent": agent,
                        "score": max(float(score), float(low_cut)),
                        "force_buy_lock": False,
                        "targeted_bucket": True,
                    }
                )
                continue

            if router_enabled and score < low_cut:
                synthetic_decisions.append(
                    {
                        "id": int(agent.id),
                        "role": "OBSERVER",
                        "trigger": "pre_router_low_score",
                        "reason": f"Activation score {score:.3f} below low threshold {low_cut:.3f}",
                        "life_pressure": "patient",
                        "price_expectation": 1.0,
                        "risk_mode": "conservative",
                        "_decision_origin": "pre_router_low_score",
                        "_llm_called": False,
                    }
                )
                optimization_counters["routed_low_score_observer"] += 1
                setattr(agent, "_role_signature_last", signature)
                setattr(agent, "_stable_observer_streak", int(getattr(agent, "_stable_observer_streak", 0) or 0) + 1)
                continue

            # Stable observer freeze window: avoid repeating same low-value ROLE_DECISION requests.
            if freeze_enabled:
                freeze_until = int(getattr(agent, "_observer_freeze_until_month", 0) or 0)
                last_sig = getattr(agent, "_role_signature_last", None)
                prev_streak = int(getattr(agent, "_stable_observer_streak", 0) or 0)
                unchanged = (last_sig == signature)
                role_now = str(getattr(agent, "role", "OBSERVER") or "OBSERVER").upper()
                if role_now == "OBSERVER" and unchanged:
                    prev_streak += 1
                elif role_now == "OBSERVER":
                    prev_streak = 1
                else:
                    prev_streak = 0
                setattr(agent, "_stable_observer_streak", prev_streak)
                setattr(agent, "_role_signature_last", signature)

                if role_now == "OBSERVER" and unchanged and int(month) <= freeze_until:
                    synthetic_decisions.append(
                        {
                            "id": int(agent.id),
                            "role": "OBSERVER",
                            "trigger": "observer_freeze_window",
                            "reason": f"Freeze active until month {freeze_until}, signature unchanged",
                            "life_pressure": "patient",
                            "price_expectation": 1.0,
                            "risk_mode": "balanced",
                            "_decision_origin": "observer_freeze_window",
                            "_llm_called": False,
                        }
                    )
                    optimization_counters["observer_frozen"] += 1
                    continue
                if role_now == "OBSERVER" and unchanged and prev_streak >= freeze_trigger:
                    freeze_until = int(month) + freeze_window - 1
                    setattr(agent, "_observer_freeze_until_month", freeze_until)
                    synthetic_decisions.append(
                        {
                            "id": int(agent.id),
                            "role": "OBSERVER",
                            "trigger": "observer_freeze_window",
                            "reason": (
                                f"Stable observer streak={prev_streak} reached trigger={freeze_trigger}; "
                                f"freeze for {freeze_window} month(s)"
                            ),
                            "life_pressure": "patient",
                            "price_expectation": 1.0,
                            "risk_mode": "balanced",
                            "_decision_origin": "observer_freeze_window",
                            "_llm_called": False,
                        }
                    )
                    optimization_counters["observer_frozen"] += 1
                    continue

            # Signature cache reuse with random re-check guard.
            if cache_enabled and (not self._should_signature_cache_recheck(signature, int(month), int(agent.id), cache_recheck_ratio)):
                cached = self._cache_lookup_role_decision(signature, month=int(month), ttl_months=cache_ttl)
                if isinstance(cached, dict):
                    d = dict(cached)
                    d["id"] = int(agent.id)
                    d["reason"] = (
                        f"{d.get('reason', '')} | signature_cache_hit ttl={cache_ttl}"
                    ).strip(" |")
                    d["_decision_origin"] = "signature_cache_hit"
                    d["_llm_called"] = False
                    synthetic_decisions.append(d)
                    optimization_counters["signature_cache_hit"] += 1
                    continue

            # Keep medium/high score cohort for LLM path.
            if (not router_enabled) or score >= low_cut or score >= high_cut:
                llm_candidate_entries.append({"agent": agent, "score": float(score), "force_buy_lock": False, "targeted_bucket": False})

        # Monthly ROLE_DECISION budget gate:
        # if budget is tight, keep higher-score candidates first and downgrade the rest.
        budget_cfg = self._resolve_role_budget_cfg()
        self._rollover_role_budget_if_needed(month=int(month))
        if budget_cfg["enabled"] and llm_candidate_entries:
            protected_entries = [
                row for row in llm_candidate_entries if bool(row.get("force_buy_lock", False)) or bool(row.get("targeted_bucket", False))
            ]
            normal_entries = [
                row for row in llm_candidate_entries if not bool(row.get("force_buy_lock", False)) and not bool(row.get("targeted_bucket", False))
            ]
            # Sort descending by activation score, then stable ID tie-break.
            normal_entries.sort(
                key=lambda x: (float(x.get("score", 0.0)), -int(x["agent"].id)),
                reverse=True,
            )
            budget_calls = int(budget_cfg["budget_calls"])
            remaining_calls = max(0, budget_calls - int(self._role_budget_used_calls))
            # Convert call budget to candidate allowance using current batch size.
            effective_batch_size = max(1, int(self._resolve_effective_activation_batch_size()))
            max_candidates_by_budget = remaining_calls * effective_batch_size

            # Always keep at least a small high-score slice to avoid total freeze.
            min_keep = int(len(normal_entries) * float(budget_cfg["min_keep_ratio"]))
            min_keep = max(0, min(len(normal_entries), min_keep))
            allowed_candidates = max(min_keep, max_candidates_by_budget)
            allowed_candidates = max(0, min(len(normal_entries), allowed_candidates))

            kept_entries = normal_entries[:allowed_candidates]
            downgraded_entries = normal_entries[allowed_candidates:]
            llm_candidate_entries = protected_entries + kept_entries
            if downgraded_entries:
                for row in downgraded_entries:
                    aid = int(row["agent"].id)
                    sc = float(row.get("score", 0.0))
                    synthetic_decisions.append(
                        {
                            "id": aid,
                            "role": "OBSERVER",
                            "trigger": "monthly_budget_throttle",
                            "reason": (
                                f"ROLE_DECISION budget throttle: score={sc:.3f}, "
                                f"remaining_calls={remaining_calls}, budget_calls={budget_calls}"
                            ),
                            "life_pressure": "patient",
                            "price_expectation": 1.0,
                            "risk_mode": "conservative",
                            "_decision_origin": "monthly_budget_throttle",
                            "_llm_called": False,
                        }
                    )
                optimization_counters["budget_downgraded_observer"] += len(downgraded_entries)
            logger.info(
                "RoleDecisionBudget month=%s used_calls=%s/%s remaining_calls=%s kept_candidates=%s downgraded=%s",
                int(month),
                int(self._role_budget_used_calls),
                int(budget_calls),
                int(remaining_calls),
                int(len(llm_candidate_entries)),
                int(optimization_counters["budget_downgraded_observer"]),
            )

        candidates = [row["agent"] for row in llm_candidate_entries]
        logger.info(
            "Activation Candidates (Stage 1+Route): %s / %s Agents | low_score=%s freeze=%s cache=%s budget_downgrade=%s",
            len(candidates),
            len(self.agents),
            optimization_counters["routed_low_score_observer"],
            optimization_counters["observer_frozen"],
            optimization_counters["signature_cache_hit"],
            optimization_counters["budget_downgraded_observer"],
        )

        if not candidates and not synthetic_decisions:
            # Keep return shape stable for caller unpacking:
            # (new_buyers, decisions, bulletin_exposure)
            return [], [], []

        # Async Batch Processing
        llm_score_map = {
            int(row["agent"].id): float(row.get("score", 0.0))
            for row in llm_candidate_entries
        }
        BATCH_SIZE = self._resolve_effective_activation_batch_size()
        smart_candidates = [a for a in candidates if getattr(a, "agent_type", "normal") == "smart"]
        normal_candidates = [a for a in candidates if getattr(a, "agent_type", "normal") != "smart"]

        dual_route_enabled = bool(opt_cfg.get("enable_model_dual_routing", False))
        gray_lower = max(0.0, min(1.0, float(opt_cfg.get("gray_score_lower", 0.02))))
        gray_upper = max(gray_lower, min(1.0, float(opt_cfg.get("gray_score_upper", 0.08))))
        default_model_type = str(opt_cfg.get("default_model_type", "fast")).lower()
        gray_model_type = str(opt_cfg.get("gray_model_type", "smart")).lower()

        def _route_model_for_agent(a: Agent) -> str:
            if not dual_route_enabled:
                return default_model_type
            score = float(llm_score_map.get(int(a.id), 0.0))
            if gray_lower <= score <= gray_upper:
                return gray_model_type
            return default_model_type

        def _build_model_batches(agent_list: List[Agent]) -> Dict[str, List[List[Agent]]]:
            by_model: Dict[str, List[Agent]] = {}
            for a in agent_list:
                model = _route_model_for_agent(a)
                by_model.setdefault(model, []).append(a)
            batched: Dict[str, List[List[Agent]]] = {}
            for model, rows in by_model.items():
                batched[model] = [rows[i:i + BATCH_SIZE] for i in range(0, len(rows), BATCH_SIZE)]
            return batched

        smart_batches_by_model = _build_model_batches(smart_candidates)
        normal_batches_by_model = _build_model_batches(normal_candidates)

        async def process_activation_batches():
            start_ts = time.time()
            tasks = []
            task_meta = []
            # Keep normal-profile prompt, but honor per-agent delay bucket when configured.
            normal_delay_buckets = {}
            for a in normal_candidates:
                delay = int(getattr(a, "info_delay_months", 0) or 0)
                normal_delay_buckets.setdefault(delay, []).append(a)
            for delay, delay_agents in normal_delay_buckets.items():
                local_batches_by_model = _build_model_batches(delay_agents)
                delayed_bulletins, delayed_trend, applied_delay = self._build_delayed_market_context(
                    recent_bulletins,
                    market_trend,
                    delay,
                )
                macro_with_delay = macro_desc
                if applied_delay > 0:
                    macro_with_delay = f"{macro_desc} | 信息延迟视角={applied_delay}个月"
                for model_type, batch_list in local_batches_by_model.items():
                    for batch in batch_list:
                        tasks.append(
                            batched_determine_role_async(
                                batch,
                                month,
                                market,
                                macro_summary=macro_with_delay,
                                market_trend=delayed_trend,
                                recent_bulletins=delayed_bulletins,
                                min_cash_observer=min_cash_observer,
                                decision_profile="normal",
                                model_type=model_type,
                            )
                        )
                        task_meta.append(
                            {
                                "delay": applied_delay,
                                "trend": delayed_trend,
                                "visible_bulletins": len(delayed_bulletins or []),
                                "model_type": model_type,
                            }
                        )

            # M14: split smart batches by info delay buckets
            delay_buckets = {}
            for a in smart_candidates:
                delay = int(getattr(a, "info_delay_months", 0) or 0)
                delay_buckets.setdefault(delay, []).append(a)
            for delay, delay_agents in delay_buckets.items():
                local_batches_by_model = _build_model_batches(delay_agents)
                delayed_bulletins, delayed_trend, applied_delay = self._build_delayed_market_context(
                    recent_bulletins,
                    market_trend,
                    delay,
                )
                macro_with_delay = macro_desc
                if applied_delay > 0:
                    macro_with_delay = f"{macro_desc} | 信息延迟视角={applied_delay}个月"
                for model_type, local_batches in local_batches_by_model.items():
                    for batch in local_batches:
                        tasks.append(
                            batched_determine_role_async(
                                batch,
                                month,
                                market,
                                macro_summary=macro_with_delay,
                                market_trend=delayed_trend,
                                recent_bulletins=delayed_bulletins,
                                min_cash_observer=min_cash_observer,
                                decision_profile="smart",
                                model_type=model_type,
                            )
                        )
                        task_meta.append({
                            "delay": applied_delay,
                            "trend": delayed_trend,
                            "visible_bulletins": len(delayed_bulletins or []),
                            "model_type": model_type,
                        })
            if self._activation_serial_mode:
                logger.info("Running serial LLM activation batches...")
                results = []
                for task in tasks:
                    results.append(await task)
            else:
                results = await asyncio.gather(*tasks)
            elapsed = max(0.0, time.time() - start_ts)
            flattened = []
            for idx, sublist in enumerate(results):
                meta = task_meta[idx] if idx < len(task_meta) else {
                    "delay": 0,
                    "trend": market_trend,
                    "visible_bulletins": len(recent_bulletins or []),
                    "model_type": default_model_type,
                }
                for item in sublist:
                    if isinstance(item, dict):
                        targeted_meta = hybrid_targeted_map.get(int(item.get("id", -1) or -1))
                        if targeted_meta:
                            item["_decision_origin"] = "hybrid_bucket_targeted_llm"
                            item["_hybrid_bucket_targeted"] = True
                            item["_hybrid_bucket_target_meta"] = dict(targeted_meta)
                        item["_info_delay_months"] = int(meta.get("delay", 0))
                        item["_delayed_trend"] = str(meta.get("trend", market_trend))
                        item["_visible_bulletins"] = int(meta.get("visible_bulletins", len(recent_bulletins or [])))
                        item["_role_model_type"] = str(meta.get("model_type", default_model_type))
                    flattened.append(item)
            return flattened, elapsed, len(tasks)

        decisions_flat = []
        if candidates:
            if self._activation_serial_mode:
                logger.info("Running serial LLM activation...")
            else:
                logger.info("Running parallel LLM activation...")
            decisions_flat, activation_elapsed, activation_batches = await process_activation_batches()
            self._role_budget_used_calls += int(activation_batches)
            self._update_adaptive_activation_batch_size(
                month=int(month),
                elapsed_seconds=float(activation_elapsed),
                batch_count=int(activation_batches),
            )
        if synthetic_decisions:
            decisions_flat = synthetic_decisions + decisions_flat
        # Process results

        new_buyers = []
        # new_sellers = []
        batch_active_insert = []
        batch_active_state_update = []
        batch_finance_update = []  # New: Persist Tier 6 finance data
        batch_bulletin_exposure = []
        timing_role_counter: Dict[str, int] = {}

        # Pre-calc property map for fast lookup
        props_map = {p['property_id']: p for p in market.properties}
        buyer_seller_split_cfg = self._buyer_seller_intent_split_cfg()

        for d in decisions_flat:
            a_id = d.get("id")
            role_str = d.get("role", "OBSERVER").upper()
            agent = self.agent_map.get(a_id)
            if not agent:
                continue

            agent_delay = int(getattr(agent, "info_delay_months", 0) or 0)
            delay_from_meta = d.get("_info_delay_months", None)
            if delay_from_meta is None:
                info_delay_months = agent_delay
            else:
                info_delay_months = int(delay_from_meta or 0)
                # Defensive: avoid wiping configured delay when batch metadata unexpectedly drops to 0.
                if info_delay_months <= 0 and agent_delay > 0:
                    info_delay_months = agent_delay
            delayed_trend = str(d.get("_delayed_trend", "STABLE"))
            visible_bulletins = int(d.get("_visible_bulletins", len(recent_bulletins or [])))
            delayed_bulletins, delayed_trend_live, applied_delay = self._build_delayed_market_context(
                recent_bulletins,
                market_trend,
                info_delay_months,
            )
            if delayed_trend_live:
                delayed_trend = str(delayed_trend_live)
            seen_bulletin_month = 0
            if delayed_bulletins:
                try:
                    seen_bulletin_month = int((delayed_bulletins[-1] or {}).get("month", 0) or 0)
                except Exception:
                    seen_bulletin_month = 0
            activation_packet = getattr(agent, "_activation_persona_packet", {}) or {}
            decision_origin = str(d.get("_decision_origin", "llm_batch") or "llm_batch")
            decision_profile = "smart" if str(getattr(agent, "agent_type", "normal") or "normal").lower() == "smart" else "normal"
            d = dict(d or {})
            lifecycle_packet = build_activation_lifecycle_packet(
                agent,
                month,
                min_cash_observer=min_cash_observer,
                holding_lock_months=self._min_holding_months_before_resale(),
                market_signal_packet=getattr(agent, "_seller_market_signal_packet", {}) or {},
            )
            behavior_modifier = build_behavior_modifier(agent, decision_profile, info_delay_months)
            timing_role = self._resolve_timing_role(
                agent=agent,
                role_str=role_str,
                decision_payload=d,
                lifecycle_packet=lifecycle_packet,
            )
            urgency_level = self._resolve_decision_urgency(d)
            d.setdefault("timing_role", timing_role)
            d.setdefault("urgency_level", urgency_level)
            d.setdefault("lifecycle_labels", lifecycle_packet.get("labels", []))
            d.setdefault("lifecycle_summary", lifecycle_packet.get("summary", ""))
            d.setdefault("behavior_modifier", behavior_modifier)
            agent.timing_role = timing_role
            agent.decision_urgency = urgency_level
            agent.lifecycle_labels = list(lifecycle_packet.get("labels", []) or [])
            agent.lifecycle_summary = str(lifecycle_packet.get("summary", "") or "")

            buy_task_locked = bool(getattr(agent, "_buy_task_locked", False))
            buy_task_exhausted = bool(getattr(agent, "_search_exhausted", False))
            buy_task_completed = bool(getattr(agent, "buy_completed", 0))
            if buy_task_locked and (not buy_task_exhausted) and (not buy_task_completed):
                locked_role = "BUYER_SELLER" if bool(getattr(agent, "owned_properties", []) or []) else "BUYER"
                if role_str not in {"BUYER", "BUYER_SELLER"}:
                    d = dict(d or {})
                    d["override_reason_code"] = "OUTBID_BUY_TASK_LOCK"
                    d["original_role"] = str(role_str)
                    d["reason"] = (
                        f"{d.get('reason', '')} | forced keep buy lane after outbid"
                    ).strip(" |")
                    role_str = locked_role
                    d["role"] = role_str
                    preserved_chain_mode = ""
                    if role_str == "BUYER_SELLER":
                        preserved_chain_mode = str(
                            d.get("chain_mode")
                            or getattr(agent, "chain_mode", "")
                            or ""
                        ).strip().lower()
                        if preserved_chain_mode in {"sell_first", "buy_first"}:
                            d["chain_mode"] = preserved_chain_mode
                    if role_str == "BUYER":
                        forced_timing_role = TIMING_ROLE_BUY_NOW
                    elif preserved_chain_mode == "sell_first":
                        forced_timing_role = TIMING_ROLE_SELL_THEN_BUY
                    else:
                        forced_timing_role = TIMING_ROLE_BUY_NOW
                    d["timing_role"] = forced_timing_role
                    timing_role = forced_timing_role
                    d["timing_role"] = timing_role
                    agent.timing_role = timing_role

            if role_str == "BUYER_SELLER" and self._should_split_buyer_seller_intent(
                decision_origin=decision_origin,
                decision_profile=decision_profile,
                decision_payload=d,
            ):
                split_result = await determine_buyer_seller_chain_mode_async(
                    agent,
                    month,
                    market,
                    macro_summary=macro_desc,
                    market_trend=delayed_trend,
                    recent_bulletins=delayed_bulletins,
                    decision_profile=decision_profile,
                    prior_reason=str(d.get("reason", "") or ""),
                    model_type=str(buyer_seller_split_cfg.get("model_type", "fast") or "fast"),
                )
                split_choice = str((split_result or {}).get("chain_mode", "") or "").strip().lower()
                split_reason = str((split_result or {}).get("reason", "") or "").strip()
                split_llm_called = bool((split_result or {}).get("llm_called", True))
                if split_choice not in {"buy_first", "sell_first"}:
                    split_choice = self._resolve_chain_mode(agent, d)
                    split_reason = split_reason or "fallback_to_default_chain_mode"
                d = dict(d or {})
                d["_buyer_seller_split_choice"] = split_choice
                d["_buyer_seller_split_reason"] = split_reason
                d["_buyer_seller_split_llm_called"] = split_llm_called
                d["_buyer_seller_split_applied"] = True
                if split_choice in {"buy_first", "sell_first"}:
                    d["chain_mode"] = split_choice
                chain_metrics = json.dumps(
                    {
                        "role_route_source": decision_origin,
                        "decision_profile": decision_profile,
                        "buyer_seller_split_choice": split_choice,
                        "buyer_seller_split_reason": split_reason,
                        "buyer_seller_split_llm_called": split_llm_called,
                        "profile_bucket_id": str(
                            activation_packet.get("profile_bucket_id", getattr(agent, "profile_bucket_id", "")) or ""
                        ),
                        "runtime_parent_bucket_id": str(
                            activation_packet.get("runtime_parent_bucket_id", "") or ""
                        ),
                    },
                    ensure_ascii=False,
                )
                batch_decision_logs.append(
                    (
                        int(a_id),
                        int(month),
                        "CHAIN_MODE",
                        split_choice.upper(),
                        split_reason or "BUYER_SELLER monthly action split",
                        json.dumps(
                            {
                                "prior_role": "BUYER_SELLER",
                                "decision_origin": decision_origin,
                                "decision_profile": decision_profile,
                            },
                            ensure_ascii=False,
                        ),
                        chain_metrics,
                        split_llm_called,
                    )
                )
                timing_role = self._resolve_timing_role(
                    agent=agent,
                    role_str=role_str,
                    decision_payload=d,
                    lifecycle_packet=lifecycle_packet,
                )
                d["timing_role"] = timing_role
                agent.timing_role = timing_role
            if role_str == "OBSERVER":
                timing_role_counter[str(d.get("timing_role", timing_role) or timing_role)] = (
                    timing_role_counter.get(str(d.get("timing_role", timing_role) or timing_role), 0) + 1
                )
                if self.is_v2:
                    reason_text = d.get('reason', 'No immediate need')
                    trigger_text = str(d.get("trigger", "") or "")
                    llm_called_flag = bool(
                        d.get(
                            "_llm_called",
                            trigger_text not in {
                                "pre_router_low_score",
                                "observer_freeze_window",
                            } and ("signature_cache_hit" not in reason_text),
                        )
                    )
                    observer_context_metrics = json.dumps(
                        {
                            "m14_info_delay_months": info_delay_months,
                            "m14_delayed_trend": delayed_trend,
                            "m14_visible_bulletins": visible_bulletins,
                            "role_route_source": str(d.get("_decision_origin", trigger_text or "observer") or "observer"),
                            "activation_llm_called": bool(llm_called_flag),
                            "profile_bucket_id": str(
                                activation_packet.get("profile_bucket_id", getattr(agent, "profile_bucket_id", "")) or ""
                            ),
                            "runtime_parent_bucket_id": str(
                                activation_packet.get("runtime_parent_bucket_id", "") or ""
                            ),
                            "role_side_hint": str(activation_packet.get("role_side_hint", "") or ""),
                            "hybrid_bucket_targeted": bool(d.get("_hybrid_bucket_targeted", False)),
                            "bucket_supply_pressure": str(activation_packet.get("bucket_supply_pressure", "") or ""),
                            "buyer_seller_split_choice": str(d.get("_buyer_seller_split_choice", "") or ""),
                            "buyer_seller_split_reason": str(d.get("_buyer_seller_split_reason", "") or ""),
                            "buyer_seller_split_llm_called": bool(d.get("_buyer_seller_split_llm_called", False)),
                            "timing_role": str(d.get("timing_role", timing_role) or timing_role),
                            "decision_urgency": str(d.get("urgency_level", urgency_level) or urgency_level),
                            "lifecycle_labels": list(d.get("lifecycle_labels", lifecycle_packet.get("labels", [])) or []),
                            "lifecycle_summary": str(d.get("lifecycle_summary", lifecycle_packet.get("summary", "")) or ""),
                            "behavior_modifier": d.get("behavior_modifier", behavior_modifier),
                        },
                        ensure_ascii=False,
                    )
                    batch_decision_logs.append((
                        a_id, month, "ROLE_DECISION", "OBSERVER",
                        reason_text, json.dumps(d), observer_context_metrics, llm_called_flag
                    ))
                    batch_bulletin_exposure.append(
                        (
                            int(a_id),
                            int(month),
                            "ROLE_DECISION",
                            "OBSERVER",
                            int(info_delay_months),
                            int(visible_bulletins),
                            int(seen_bulletin_month),
                            int(applied_delay),
                            str(delayed_trend),
                            "system_market_bulletin",
                            bool(llm_called_flag),
                        )
                    )
                if cache_enabled and (not bool(d.get("_skip_signature_cache", False))):
                    signature = self._build_role_signature(agent, market_trend=market_trend, recent_bulletins=recent_bulletins)
                    cache_payload = {
                        "role": "OBSERVER",
                        "trigger": str(d.get("trigger", "observer")),
                        "reason": str(d.get("reason", "")),
                        "life_pressure": str(d.get("life_pressure", "patient")),
                        "price_expectation": float(d.get("price_expectation", 1.0) or 1.0),
                        "risk_mode": str(d.get("risk_mode", "balanced")),
                        "timing_role": str(d.get("timing_role", timing_role) or timing_role),
                        "urgency_level": str(d.get("urgency_level", urgency_level) or urgency_level),
                        "lifecycle_labels": list(d.get("lifecycle_labels", lifecycle_packet.get("labels", [])) or []),
                        "lifecycle_summary": str(d.get("lifecycle_summary", lifecycle_packet.get("summary", "")) or ""),
                    }
                    self._cache_store_role_decision(signature, month=int(month), decision=cache_payload)
                continue

            # Configurable hard constraint: no-property + low-cash must remain OBSERVER
            if (not agent.owned_properties) and (agent.cash < min_cash_observer):
                forced_timing_role = TIMING_ROLE_NEED_WAIT if lifecycle_packet.get("labels") else TIMING_ROLE_OBSERVE_WAIT
                d["timing_role"] = forced_timing_role
                timing_role_counter[forced_timing_role] = timing_role_counter.get(forced_timing_role, 0) + 1
                observer_context_metrics = json.dumps(
                    {
                        "m14_info_delay_months": info_delay_months,
                        "m14_delayed_trend": delayed_trend,
                        "m14_visible_bulletins": visible_bulletins,
                        "role_route_source": str(d.get("_decision_origin", "min_cash_observer_guard") or "min_cash_observer_guard"),
                        "activation_llm_called": False,
                        "profile_bucket_id": str(
                            activation_packet.get("profile_bucket_id", getattr(agent, "profile_bucket_id", "")) or ""
                        ),
                        "runtime_parent_bucket_id": str(
                            activation_packet.get("runtime_parent_bucket_id", "") or ""
                        ),
                        "role_side_hint": str(activation_packet.get("role_side_hint", "") or ""),
                        "hybrid_bucket_targeted": bool(d.get("_hybrid_bucket_targeted", False)),
                        "bucket_supply_pressure": str(activation_packet.get("bucket_supply_pressure", "") or ""),
                        "timing_role": forced_timing_role,
                        "decision_urgency": urgency_level,
                        "lifecycle_labels": list(lifecycle_packet.get("labels", []) or []),
                        "lifecycle_summary": str(lifecycle_packet.get("summary", "") or ""),
                    },
                    ensure_ascii=False,
                )
                batch_decision_logs.append((
                    a_id, month, "ROLE_DECISION", "OBSERVER",
                    f"System constrained by min_cash_observer_no_property={min_cash_observer:,.0f}",
                    json.dumps(d), observer_context_metrics, False
                ))
                batch_bulletin_exposure.append(
                    (
                        int(a_id),
                        int(month),
                        "ROLE_DECISION",
                        "OBSERVER",
                        int(info_delay_months),
                        int(visible_bulletins),
                        int(seen_bulletin_month),
                        int(applied_delay),
                        str(delayed_trend),
                        "system_market_bulletin",
                        False,
                    )
                )
                continue

            agent.role = role_str
            agent.role_duration = 1
            agent.life_pressure = d.get("life_pressure", "patient")
            agent.timing_role = str(d.get("timing_role", timing_role) or timing_role)
            agent.decision_urgency = str(d.get("urgency_level", urgency_level) or urgency_level)
            agent.lifecycle_labels = list(d.get("lifecycle_labels", lifecycle_packet.get("labels", [])) or [])
            agent.lifecycle_summary = str(d.get("lifecycle_summary", lifecycle_packet.get("summary", "")) or "")
            timing_role_counter[agent.timing_role] = timing_role_counter.get(agent.timing_role, 0) + 1
            agent.consecutive_failures = 0
            agent.cooldown_months = 0
            agent.waited_months = 0
            agent.sell_completed = 0
            agent.buy_completed = 0

            trigger = d.get("trigger", "Unknown")
            agent.activation_trigger = str(trigger or "")
            decision_origin = str(d.get("_decision_origin", "llm_batch") or "llm_batch")
            decision_llm_called = bool(d.get("_llm_called", True))
            delayed_bulletin_text = self._build_delayed_bulletin_text(
                market_bulletin,
                recent_bulletins,
                info_delay_months,
            )
            if delayed_trend_live:
                delayed_trend = str(delayed_trend_live)

            is_seller = role_str in ["SELLER", "BUYER_SELLER"]
            is_buyer = role_str in ["BUYER", "BUYER_SELLER"]

            metrics = None  # Init metrics

            # Seller Logic
            if is_seller:
                if not agent.owned_properties:
                    if is_buyer:
                        agent.role = "BUYER"
                        role_str = "BUYER"
                        is_seller = False
                    else:
                        agent.role = "OBSERVER"
                        continue
                else:
                    # Generate Listing
                    decision, metrics = self._create_seller_listing(
                        agent,
                        market,
                        month,
                        delayed_trend,
                        delayed_bulletin_text,
                    )
                    seller_metrics = dict(metrics or {})
                    seller_metrics.update(
                        {
                            "seller_persona_snapshot": {
                                "purchase_motive_primary": str(getattr(agent.story, "purchase_motive_primary", "") or ""),
                                "housing_stage": str(getattr(agent.story, "housing_stage", "") or ""),
                                "family_stage": str(getattr(agent.story, "family_stage", "") or ""),
                                "education_path": str(getattr(agent.story, "education_path", "") or ""),
                                "financial_profile": str(getattr(agent.story, "financial_profile", "") or ""),
                                "seller_profile": str(getattr(agent.story, "seller_profile", "") or ""),
                            },
                            "properties_to_sell": list(decision.get("properties_to_sell", []) or []),
                            "pricing_coefficient": float(decision.get("pricing_coefficient", 1.0) or 1.0),
                            "strategy": str(decision.get("strategy", "B") or "B"),
                            "reasoning": str(decision.get("reasoning", "") or ""),
                        }
                    )
                    batch_decision_logs.append((
                        agent.id,
                        month,
                        "LISTING_ACTION",
                        "LIST",
                        str(decision.get("reasoning", "") or "Generated listing strategy"),
                        json.dumps(decision, ensure_ascii=False),
                        json.dumps(seller_metrics, ensure_ascii=False),
                        bool(seller_metrics.get("listing_llm_called", False)),
                    ))

            # Listing state and role are decoupled:
            # a role switch must not auto-withdraw for-sale listings by default.
            listing_action = str((d or {}).get("listing_action", "") or "").strip().upper()
            explicit_withdraw = listing_action in {"WITHDRAW", "UNLIST"}
            role_auto_withdraw_enabled = self._as_bool(
                self.config.get(
                    "smart_agent.role_change_auto_withdraw_enabled",
                    self.config.get("role_change_auto_withdraw_enabled", False),
                ),
                False,
            )
            should_withdraw_listings = bool((not is_seller) and (explicit_withdraw or role_auto_withdraw_enabled))
            if should_withdraw_listings:
                for prop in agent.owned_properties:
                    p_obj = props_map.get(prop['property_id'])
                    if p_obj and p_obj.get('status') == 'for_sale':
                        logger.info(f"Agent {agent.id} (Role: {role_str}) withdrawing Property {p_obj['property_id']} from market.")
                        p_obj['status'] = 'off_market'
                        cursor.execute("UPDATE properties_market SET status='off_market' WHERE property_id=?", (p_obj['property_id'],))
                        withdraw_reason = "explicit_withdraw" if explicit_withdraw else "role_auto_withdraw"
                        withdraw_metrics = {
                            "seller_persona_snapshot": {
                                "purchase_motive_primary": str(getattr(agent.story, "purchase_motive_primary", "") or ""),
                                "housing_stage": str(getattr(agent.story, "housing_stage", "") or ""),
                                "family_stage": str(getattr(agent.story, "family_stage", "") or ""),
                                "education_path": str(getattr(agent.story, "education_path", "") or ""),
                                "financial_profile": str(getattr(agent.story, "financial_profile", "") or ""),
                                "seller_profile": str(getattr(agent.story, "seller_profile", "") or ""),
                            },
                            "withdraw_reason_tags": [
                                f"role:{role_str.lower()}",
                                f"listing:{withdraw_reason}",
                            ],
                            "property_snapshot": {
                                "property_id": int(p_obj.get("property_id", -1) or -1),
                                "zone": str(p_obj.get("zone", "") or ""),
                                "status_before": "for_sale",
                                "listed_price": float(p_obj.get("listed_price", 0.0) or 0.0),
                                "min_price": float(p_obj.get("min_price", 0.0) or 0.0),
                                "building_area": float(p_obj.get("building_area", 0.0) or 0.0),
                                "is_school_district": int(p_obj.get("is_school_district", 0) or 0),
                            },
                        }
                        batch_decision_logs.append((
                            agent.id, month, "LISTING_ACTION", "WITHDRAW",
                            f"Listing withdraw policy={withdraw_reason}", "Withdraw due to explicit listing decision",
                            json.dumps(withdraw_metrics, ensure_ascii=False), False
                        ))

            # Buyer Logic
            if is_buyer:
                # PASS recent_bulletins here!
                pref, reason, b_metrics = await generate_buyer_preference(
                    agent, market, month, macro_desc, delayed_trend,
                    db_conn=self.conn, recent_bulletins=delayed_bulletins
                )
                agent.preference = pref
                family_stage = str(getattr(agent.story, "family_stage", "") or "")
                education_path = str(getattr(agent.story, "education_path", "") or "")
                if education_path == "not_school_sensitive":
                    agent.school_urgency = 0
                elif family_stage == "senior_school_transition":
                    agent.school_urgency = 3
                elif family_stage == "junior_school_transition":
                    agent.school_urgency = 2
                elif family_stage == "primary_school_before_transition":
                    agent.school_urgency = 1
                else:
                    agent.school_urgency = 0
                if reason and d:
                    d['reason'] = f"{d.get('reason', '')} | Pref: {reason}"

                # Merge metrics if seller logic also ran?
                if metrics and b_metrics:
                    metrics.update(b_metrics)
                elif b_metrics:
                    metrics = b_metrics

                price_factor = d.get("price_expectation", 1.0)
                if hasattr(agent.preference, 'max_price'):
                    agent.preference.max_price *= price_factor
                # M12/M13/M15 long-cycle state init + buyer deadline horizon
                buyer_deadline_months = int(self._resolve_buyer_deadline_months(agent, d, pref))
                agent.max_wait_months = int(buyer_deadline_months)
                try:
                    pref.max_wait_months = int(buyer_deadline_months)
                except Exception:
                    pass
                agent.target_buy_price = self._resolve_target_buy_price(pref, price_factor)
                agent.target_sell_price = 0.0
                agent.risk_mode = self._resolve_risk_mode(agent, d, pref=pref)
                agent.chain_mode = self._resolve_chain_mode(agent, d) if role_str == "BUYER_SELLER" else None
                if isinstance(metrics, dict):
                    metrics["buyer_deadline_total_months"] = int(buyer_deadline_months)
                    metrics["buyer_deadline_months_left"] = int(buyer_deadline_months)

                # sell-first replacement users are activated as sellers first in this month.
                if not (role_str == "BUYER_SELLER" and agent.chain_mode == "sell_first" and is_seller):
                    setattr(agent, "_must_continue_search", True)
                    setattr(agent, "_search_exhausted", False)
                    new_buyers.append(agent)

                # Add to finance update batch
                f_dict = agent.to_v2_finance_dict()
                batch_finance_update.append((
                    f_dict['max_affordable_price'],
                    f_dict['psychological_price'],
                    agent.id
                ))

            # Normalize metrics for JSON storage
            metrics_json = json.dumps(metrics) if metrics else None
            # Log Phase 8: context_metrics
            batch_decision_logs.append((
                agent.id, month, "ROLE_DECISION", role_str,
                f"{trigger}: {d.get('reason', '')}", json.dumps(d),
                json.dumps({
                    **(metrics if isinstance(metrics, dict) else {}),
                    "m14_info_delay_months": info_delay_months,
                    "m14_delayed_trend": delayed_trend,
                    "m14_visible_bulletins": visible_bulletins,
                    "role_route_source": decision_origin,
                    "activation_llm_called": bool(decision_llm_called),
                    "activation_score": (
                        round(float(llm_score_map.get(int(agent.id), 0.0)), 4)
                        if int(agent.id) in llm_score_map
                        else None
                    ),
                    "profile_bucket_id": str(
                        activation_packet.get("profile_bucket_id", getattr(agent, "profile_bucket_id", "")) or ""
                    ),
                    "runtime_parent_bucket_id": str(
                        activation_packet.get("runtime_parent_bucket_id", "") or ""
                    ),
                    "role_side_hint": str(activation_packet.get("role_side_hint", "") or ""),
                    "hybrid_bucket_targeted": bool(d.get("_hybrid_bucket_targeted", False)),
                    "bucket_supply_pressure": str(activation_packet.get("bucket_supply_pressure", "") or ""),
                    "buyer_seller_split_choice": str(d.get("_buyer_seller_split_choice", "") or ""),
                    "buyer_seller_split_reason": str(d.get("_buyer_seller_split_reason", "") or ""),
                    "buyer_seller_split_llm_called": bool(d.get("_buyer_seller_split_llm_called", False)),
                    "timing_role": str(d.get("timing_role", timing_role) or timing_role),
                    "decision_urgency": str(d.get("urgency_level", urgency_level) or urgency_level),
                    "lifecycle_labels": list(d.get("lifecycle_labels", lifecycle_packet.get("labels", [])) or []),
                    "lifecycle_summary": str(d.get("lifecycle_summary", lifecycle_packet.get("summary", "")) or ""),
                    "behavior_modifier": d.get("behavior_modifier", behavior_modifier),
                }, ensure_ascii=False),
                bool(decision_llm_called)
            ))
            batch_bulletin_exposure.append(
                (
                    int(agent.id),
                    int(month),
                    "ROLE_DECISION",
                    str(role_str),
                    int(info_delay_months),
                    int(visible_bulletins),
                    int(seen_bulletin_month),
                    int(applied_delay),
                    str(delayed_trend),
                    "system_market_bulletin",
                    bool(decision_llm_called),
                )
            )

            if cache_enabled and (not bool(d.get("_skip_signature_cache", False))):
                signature = self._build_role_signature(agent, market_trend=market_trend, recent_bulletins=recent_bulletins)
                cache_payload = {
                    "role": role_str,
                    "trigger": trigger,
                    "reason": str(d.get("reason", "")),
                    "life_pressure": str(d.get("life_pressure", "patient")),
                    "price_expectation": float(d.get("price_expectation", 1.0) or 1.0),
                    "risk_mode": str(d.get("risk_mode", "balanced")),
                    "chain_mode": d.get("chain_mode"),
                    "timing_role": str(d.get("timing_role", timing_role) or timing_role),
                    "urgency_level": str(d.get("urgency_level", urgency_level) or urgency_level),
                    "lifecycle_labels": list(d.get("lifecycle_labels", lifecycle_packet.get("labels", [])) or []),
                    "lifecycle_summary": str(d.get("lifecycle_summary", lifecycle_packet.get("summary", "")) or ""),
                }
                self._cache_store_role_decision(signature, month=int(month), decision=cache_payload)

            # Persistence Buffer
            if self.is_v2:
                selling_pid = agent.owned_properties[0]['property_id'] if is_seller and agent.owned_properties else None
                target_zone = agent.preference.target_zone if is_buyer and agent.preference else None
                max_price = agent.preference.max_price if is_buyer and agent.preference else None
                target_buy_price = float(getattr(agent, "target_buy_price", 0.0) or 0.0)
                target_sell_price = float(getattr(agent, "target_sell_price", 0.0) or 0.0)
                risk_mode = getattr(agent, "risk_mode", "balanced")
                max_wait_months = int(getattr(agent, "max_wait_months", self._get_max_wait_months()) or self._get_max_wait_months())
                waited_months = int(getattr(agent, "waited_months", 0) or 0)
                cooldown_months = int(getattr(agent, "cooldown_months", 0) or 0)
                consecutive_failures = int(getattr(agent, "consecutive_failures", 0) or 0)
                chain_mode = getattr(agent, "chain_mode", None)
                sell_completed = int(getattr(agent, "sell_completed", 0) or 0)
                buy_completed = int(getattr(agent, "buy_completed", 0) or 0)
                school_urgency = int(getattr(agent, "school_urgency", 0) or 0)
                activation_trigger = str(getattr(agent, "activation_trigger", trigger) or trigger or "")

                batch_active_insert.append((
                    agent.id,
                    role_str,
                    getattr(agent, "agent_type", "normal"),
                    target_zone,
                    max_price,
                    selling_pid,
                    agent.listing.get('min_price') if hasattr(agent, 'listing') and agent.listing else None,
                    agent.listing.get('listed_price') if hasattr(agent, 'listing') and agent.listing else None,
                    agent.life_pressure,
                    d.get('reason', ''),
                    month,
                    1,
                    activation_trigger,
                    school_urgency,
                ))
                batch_active_state_update.append(
                    {
                        "target_buy_price": target_buy_price,
                        "target_sell_price": target_sell_price,
                        "risk_mode": risk_mode,
                        "max_wait_months": max_wait_months,
                        "waited_months": waited_months,
                        "cooldown_months": cooldown_months,
                        "consecutive_failures": consecutive_failures,
                        "chain_mode": chain_mode,
                        "sell_completed": sell_completed,
                        "buy_completed": buy_completed,
                        "timing_role": str(d.get("timing_role", timing_role) or timing_role),
                        "decision_urgency": str(d.get("urgency_level", urgency_level) or urgency_level),
                        "lifecycle_labels": json.dumps(
                            list(d.get("lifecycle_labels", lifecycle_packet.get("labels", [])) or []),
                            ensure_ascii=False,
                        ),
                        "lifecycle_summary": str(d.get("lifecycle_summary", lifecycle_packet.get("summary", "")) or ""),
                        "agent_id": agent.id,
                    }
                )

        if batch_active_insert:
            # Snapshot semantics: keep only one active record per agent.
            agent_ids = [(item[0],) for item in batch_active_insert]
            cursor.executemany("DELETE FROM active_participants WHERE agent_id = ?", agent_ids)

            cols = self._table_columns("active_participants")
            has_month = "month" in cols
            has_agent_type = "agent_type" in cols
            has_activation_trigger = "activation_trigger" in cols
            has_school_urgency = "school_urgency" in cols
            if has_month and has_agent_type and has_activation_trigger and has_school_urgency:
                cursor.executemany("""
                    INSERT OR REPLACE INTO active_participants
                    (agent_id, role, agent_type, target_zone, max_price, selling_property_id,
                     min_price, listed_price, life_pressure, llm_intent_summary, activated_month, role_duration,
                     activation_trigger, school_urgency, month)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    (
                        item[0], item[1], item[2], item[3], item[4], item[5], item[6],
                        item[7], item[8], item[9], item[10], item[11], item[12], item[13], month
                    )
                    for item in batch_active_insert
                ])
            elif has_month and has_agent_type:
                cursor.executemany("""
                    INSERT OR REPLACE INTO active_participants
                    (agent_id, role, agent_type, target_zone, max_price, selling_property_id,
                     min_price, listed_price, life_pressure, llm_intent_summary, activated_month, role_duration, month)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    (
                        item[0], item[1], item[2], item[3], item[4], item[5], item[6],
                        item[7], item[8], item[9], item[10], item[11], month
                    )
                    for item in batch_active_insert
                ])
            elif has_month and not has_agent_type:
                cursor.executemany("""
                    INSERT OR REPLACE INTO active_participants
                    (agent_id, role, target_zone, max_price, selling_property_id,
                     min_price, listed_price, life_pressure, llm_intent_summary, activated_month, role_duration, month)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    (
                        item[0], item[1], item[3], item[4], item[5], item[6],
                        item[7], item[8], item[9], item[10], item[11], month
                    )
                    for item in batch_active_insert
                ])
            elif (not has_month) and has_agent_type:
                cursor.executemany("""
                    INSERT OR REPLACE INTO active_participants
                    (agent_id, role, agent_type, target_zone, max_price, selling_property_id,
                     min_price, listed_price, life_pressure, llm_intent_summary, activated_month, role_duration)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    (
                        item[0], item[1], item[2], item[3], item[4], item[5],
                        item[6], item[7], item[8], item[9], item[10], item[11]
                    )
                    for item in batch_active_insert
                ])
            else:
                cursor.executemany("""
                    INSERT OR REPLACE INTO active_participants
                    (agent_id, role, target_zone, max_price, selling_property_id,
                     min_price, listed_price, life_pressure, llm_intent_summary, activated_month, role_duration)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    (
                        item[0], item[1], item[3], item[4], item[5], item[6],
                        item[7], item[8], item[9], item[10], item[11]
                    )
                    for item in batch_active_insert
                ])

            # Side-car update for long-cycle state columns (M12/M13/M15/M17).
            cols = self._table_columns("active_participants")
            update_cols = [
                "target_buy_price",
                "target_sell_price",
                "risk_mode",
                "max_wait_months",
                "waited_months",
                "cooldown_months",
                "consecutive_failures",
                "chain_mode",
                "sell_completed",
                "buy_completed",
                "timing_role",
                "decision_urgency",
                "lifecycle_labels",
                "lifecycle_summary",
            ]
            available_update_cols = [col for col in update_cols if col in cols]
            if "agent_id" in cols and available_update_cols and batch_active_state_update:
                sql = (
                    "UPDATE active_participants SET "
                    + ", ".join(f"{col}=?" for col in available_update_cols)
                    + " WHERE agent_id=?"
                )
                cursor.executemany(
                    sql,
                    [
                        tuple(item.get(col) for col in available_update_cols) + (item["agent_id"],)
                        for item in batch_active_state_update
                    ],
                )

        # Persist Finance Updates (Tier 6)
        if batch_finance_update:
            cursor.executemany("""
                UPDATE agents_finance
                SET max_affordable_price = ?, psychological_price = ?
                WHERE agent_id = ?
            """, batch_finance_update)

        routing_summary = {
            "total_agents": int(len(self.agents)),
            "stage1_prefilter_candidates": int(stage1_candidate_count),
            "stage1_prefilter_skipped": int(max(0, len(self.agents) - stage1_candidate_count)),
            "llm_candidate_count": int(len(candidates)),
            "targeted_llm_candidate_count": int(sum(1 for row in llm_candidate_entries if bool(row.get("targeted_bucket", False)))),
            "smart_llm_candidates": int(len(smart_candidates)),
            "normal_llm_candidates": int(len(normal_candidates)),
            "synthetic_decision_count": int(len(synthetic_decisions)),
            "activation_mode": str(activation_governance_cfg.get("activation_mode", "natural") or "natural"),
            "targeted_bucket_ids": sorted({str(item.get("bucket_id", "")) for item in hybrid_targeted_map.values() if item}),
            "optimization_counters": {k: int(v) for k, v in optimization_counters.items()},
            "llm_batch_calls": int(locals().get("activation_batches", 0) or 0),
            "activation_batch_size": int(BATCH_SIZE),
            "activation_serial_mode": bool(self._activation_serial_mode),
            "timing_role_counts": {k: int(v) for k, v in sorted(timing_role_counter.items())},
        }
        batch_decision_logs.append(
            (
                0,
                int(month),
                "ROLE_ACTIVATION_ROUTING_SUMMARY",
                "SUMMARY",
                "Activation routing summary",
                None,
                json.dumps(routing_summary, ensure_ascii=False),
                False,
            )
        )
        batch_decision_logs.append(
            (
                0,
                int(month),
                "ROLE_ACTIVATION_GOVERNANCE_SUMMARY",
                str(activation_governance_cfg.get("activation_mode", "natural") or "natural").upper(),
                "Activation governance summary",
                None,
                json.dumps(
                    {
                        "activation_mode": str(activation_governance_cfg.get("activation_mode", "natural") or "natural"),
                        "gate_mode": str(activation_governance_cfg.get("gate_mode", "warn") or "warn"),
                        "profiled_market_required": bool(activation_governance_cfg.get("profiled_market_required", False)),
                        "hard_bucket_matcher_required": bool(activation_governance_cfg.get("hard_bucket_matcher_required", False)),
                        "hybrid_floor_enabled": bool(activation_governance_cfg.get("hybrid_floor_enabled", False)),
                        "hybrid_floor_strategy": str(activation_governance_cfg.get("hybrid_floor_strategy", "bucket_targeted_llm_first") or "bucket_targeted_llm_first"),
                        "autofill_supply_floor": int(activation_governance_cfg.get("autofill_supply_floor", 0) or 0),
                        "autofill_demand_floor": int(activation_governance_cfg.get("autofill_demand_floor", 0) or 0),
                        "emit_bucket_funnel": bool(activation_governance_cfg.get("emit_bucket_funnel", True)),
                    },
                    ensure_ascii=False,
                ),
                False,
            )
        )

        self.conn.commit()

        return new_buyers, decisions_flat, batch_bulletin_exposure

    def _create_seller_listing(self, agent, market, month, market_trend="STABLE", market_bulletin=""):
        """Creates listing and returns (listing_dict, context_metrics)."""
        cursor = self.conn.cursor()
        properties_to_list = []
        strategy_hint = "balanced"
        hold_months_required = self._min_holding_months_before_resale()

        # Calculate strategy first
        zone_prices = {z: market.get_avg_price(z) for z in ["A", "B"]}

        smart_trigger = None
        strategy_input_bulletin = market_bulletin
        if getattr(agent, "agent_type", "normal") == "smart":
            smart_trigger = self._evaluate_smart_sell_triggers(agent, zone_prices)
            if smart_trigger.get("trigger_mode"):
                trigger_note = (
                    f"【风险触发】{smart_trigger['trigger_mode']}: {smart_trigger['trigger_reason']}。"
                    f"触发房源ID={smart_trigger['trigger_property_ids']}。"
                    "请优先处理这些房源，避免回撤扩大。"
                )
                strategy_input_bulletin = f"{market_bulletin}\n{trigger_note}" if market_bulletin else trigger_note

        is_smart_seller = (getattr(agent, "agent_type", "normal") == "smart")
        use_rule_pricing_for_normal = True
        try:
            use_rule_pricing_for_normal = bool(
                self.config.get("smart_agent.normal_seller_rule_pricing_enabled", True)
            )
        except Exception:
            use_rule_pricing_for_normal = True

        listing_llm_called = bool(is_smart_seller or (not use_rule_pricing_for_normal))
        listing_decision_mode = "llm" if listing_llm_called else "rule"

        if (not is_smart_seller) and use_rule_pricing_for_normal:
            decision, metrics = self._build_rule_listing_decision(agent, zone_prices, market_trend)
        else:
            decision, metrics = determine_listing_strategy(
                agent,
                zone_prices,
                strategy_input_bulletin,
                market_trend,
                self.config,
            )
        metrics = dict(metrics or {})
        metrics["listing_llm_called"] = bool(listing_llm_called)
        metrics["listing_decision_mode"] = listing_decision_mode

        target_ids = decision.get("properties_to_sell", [])
        pricing_coefficient = decision.get("pricing_coefficient", 1.0)
        strategy_code = decision.get("strategy", "B")
        strategy_map = {"A": "aggressive", "B": "balanced", "C": "urgent", "D": "hold"}
        strategy_hint = strategy_map.get(strategy_code, "balanced")

        if smart_trigger:
            # M11 guardrail: when stop-loss is hit, enforce at least one triggered property and avoid "hold".
            if smart_trigger.get("trigger_mode") == "STOP_LOSS":
                forced_ids = smart_trigger.get("trigger_property_ids", [])
                if forced_ids:
                    if not target_ids:
                        target_ids = [forced_ids[0]]
                    elif not any(pid in forced_ids for pid in target_ids):
                        target_ids = [forced_ids[0]] + target_ids
                    if strategy_code == "D":
                        strategy_code = "C"
                        strategy_hint = strategy_map.get(strategy_code, "urgent")
                    if float(pricing_coefficient or 1.0) > 0.97:
                        pricing_coefficient = 0.97
                decision["m11_guardrail_applied"] = True
            else:
                decision["m11_guardrail_applied"] = False
            decision["m11_trigger_mode"] = smart_trigger.get("trigger_mode")
            decision["m11_trigger_reason"] = smart_trigger.get("trigger_reason")
            decision["m11_trigger_property_ids"] = smart_trigger.get("trigger_property_ids", [])

        # M16: prevent extreme sell-side dumping in one month.
        # Apply only to smart agents; normal agents keep baseline behavior.
        target_ids = list(dict.fromkeys(target_ids or []))

        blocked_recent_ids: List[int] = []
        eligible_owned_properties: List[Dict[str, Any]] = []
        for owned_prop in list(getattr(agent, "owned_properties", []) or []):
            try:
                pid = int(owned_prop.get("property_id"))
            except Exception:
                continue
            acquired_raw = owned_prop.get("acquired_month")
            acquired_month = None
            if acquired_raw not in (None, ""):
                try:
                    acquired_month = int(acquired_raw)
                except Exception:
                    acquired_month = None
            if (
                acquired_month is not None
                and hold_months_required > 0
                and int(month) - int(acquired_month) < int(hold_months_required)
            ):
                blocked_recent_ids.append(pid)
                continue
            eligible_owned_properties.append(owned_prop)
        eligible_owned_ids = {int(prop.get("property_id")) for prop in eligible_owned_properties if prop.get("property_id") is not None}
        target_ids = [pid for pid in target_ids if int(pid) in eligible_owned_ids]
        decision["min_holding_months_before_resale"] = int(hold_months_required)
        decision["recent_purchase_sell_blocked_property_ids"] = [int(pid) for pid in blocked_recent_ids]
        decision["eligible_sell_property_ids"] = sorted(int(pid) for pid in eligible_owned_ids)

        if getattr(agent, "agent_type", "normal") == "smart":
            try:
                max_sells_per_month = int(
                    self.config.get(
                        "smart_agent.max_sells_per_month",
                        self.config.get("max_sells_per_month", 2),
                    )
                )
            except Exception:
                max_sells_per_month = 2
            max_sells_per_month = max(1, max_sells_per_month)

            trend = str(market_trend or "STABLE").upper()
            downtrend = ("DOWN" in trend) or ("PANIC" in trend)
            try:
                max_downtrend_sells_per_month = int(
                    self.config.get(
                        "smart_agent.max_downtrend_sells_per_month",
                        self.config.get("max_downtrend_sells_per_month", 1),
                    )
                )
            except Exception:
                max_downtrend_sells_per_month = 1
            max_downtrend_sells_per_month = max(1, max_downtrend_sells_per_month)
            effective_sell_cap = min(
                max_sells_per_month,
                max_downtrend_sells_per_month if downtrend else max_sells_per_month,
            )

            if len(target_ids) > effective_sell_cap:
                target_ids = target_ids[:effective_sell_cap]
                decision["m16_sell_cap_applied"] = True
                decision["m16_sell_cap_reason"] = (
                    f"seller listings capped to {effective_sell_cap} in trend {trend}"
                )
                decision["m16_sell_cap"] = effective_sell_cap
            else:
                decision["m16_sell_cap_applied"] = False
                decision["m16_sell_cap"] = effective_sell_cap

            if metrics is None:
                metrics = {}
            if isinstance(metrics, dict):
                metrics.update(
                    {
                        "m16_sell_cap_applied": bool(decision.get("m16_sell_cap_applied", False)),
                        "m16_sell_cap": int(decision.get("m16_sell_cap", effective_sell_cap)),
                        "m16_sell_cap_reason": decision.get("m16_sell_cap_reason", ""),
                        "recent_purchase_sell_blocked_count": int(len(blocked_recent_ids)),
                        "recent_purchase_sell_blocked_property_ids": [int(pid) for pid in blocked_recent_ids],
                        "min_holding_months_before_resale": int(hold_months_required),
                    }
                )

        if metrics is None:
            metrics = {}
        if isinstance(metrics, dict):
            metrics.setdefault("recent_purchase_sell_blocked_count", int(len(blocked_recent_ids)))
            metrics.setdefault("recent_purchase_sell_blocked_property_ids", [int(pid) for pid in blocked_recent_ids])
            metrics.setdefault("min_holding_months_before_resale", int(hold_months_required))
            metrics.setdefault("eligible_sell_property_count", int(len(eligible_owned_ids)))

        if not target_ids and eligible_owned_properties:
            target_ids = [eligible_owned_properties[0]['property_id']]

        decision["properties_to_sell"] = target_ids
        decision["pricing_coefficient"] = pricing_coefficient
        decision["strategy"] = strategy_code

        if smart_trigger:
            metrics = metrics or {}
            metrics.update({
                "m11_trigger_mode": smart_trigger.get("trigger_mode"),
                "m11_trigger_reason": smart_trigger.get("trigger_reason"),
                "m11_trigger_property_ids": smart_trigger.get("trigger_property_ids", []),
                "m11_stop_loss_threshold": smart_trigger.get("stop_loss_threshold"),
                "m11_take_profit_threshold": smart_trigger.get("take_profit_threshold"),
                "m11_guardrail_applied": decision.get("m11_guardrail_applied", False),
            })

        for pid in target_ids:
            p_data = next((p for p in agent.owned_properties if p['property_id'] == pid), None)
            if p_data:
                properties_to_list.append((p_data, pricing_coefficient))

        # Import internally to avoid circular dependency
        from transaction_engine import generate_seller_listing

        for p_data, coeff in properties_to_list:
            try:
                listing = generate_seller_listing(
                    agent,
                    p_data,
                    market,
                    strategy_hint,
                    pricing_coefficient=coeff,
                    config=self.config,
                )
            except TypeError:
                # Backward compatibility for mocked legacy signatures in tests.
                listing = generate_seller_listing(
                    agent,
                    p_data,
                    market,
                    strategy_hint,
                    pricing_coefficient=coeff,
                )
            if not hasattr(agent, 'listing'):
                agent.listing = listing  # Store first for active_participants

            listed_price = max(1.0, float(listing.get("listed_price", 0.0) or 0.0))
            min_price = max(1.0, float(listing.get("min_price", listed_price) or listed_price))
            if min_price > listed_price:
                min_price = listed_price
            deadline_total = int(max(0, int(listing.get("sell_deadline_total_months", 0) or 0)))
            deadline_month = int(month + deadline_total - 1) if deadline_total > 0 else None
            urgency_score = float(max(0.0, min(1.0, float(listing.get("sell_urgency_score", listing.get("urgency", 0.5)) or 0.5))))
            forced_sale_mode = int(1 if bool(listing.get("forced_sale_mode", 0)) else 0)

            # V2 Update
            try:
                cursor.execute(
                    """
                    UPDATE properties_market
                    SET status='for_sale',
                        listed_price=?,
                        min_price=?,
                        listing_month=?,
                        sell_deadline_month=?,
                        sell_deadline_total_months=?,
                        sell_urgency_score=?,
                        forced_sale_mode=?,
                        last_price_update_month=?,
                        last_price_update_reason=?
                    WHERE property_id=?
                    """,
                    (
                        listed_price,
                        min_price,
                        month,
                        deadline_month,
                        deadline_total if deadline_total > 0 else None,
                        urgency_score,
                        forced_sale_mode,
                        month,
                        "Initial Listing",
                        listing['property_id'],
                    ),
                )
            except Exception:
                cursor.execute(
                    "UPDATE properties_market SET status='for_sale', listed_price=?, min_price=?, listing_month=?, "
                    "last_price_update_month=?, last_price_update_reason=? WHERE property_id=?",
                    (listed_price, min_price, month, month, "Initial Listing", listing['property_id']),
                )

        return decision, metrics

    def _build_rule_listing_decision(self, agent, zone_prices: Dict[str, float], market_trend: str):
        """
        Rule-based seller listing for normal agents (token-saving path).
        Smart sellers keep LLM strategy path.
        """
        props = list(getattr(agent, "owned_properties", []) or [])
        if not props:
            return (
                {"strategy": "B", "pricing_coefficient": 1.0, "properties_to_sell": [], "reasoning": "No properties"},
                {"pricing_source": "RULE_NORMAL"},
            )

        trend = str(market_trend or "STABLE").upper()
        downtrend = ("DOWN" in trend) or ("PANIC" in trend) or ("CRASH" in trend)
        uptrend = ("UP" in trend) or ("BOOM" in trend)

        scored = []
        for p in props:
            zone = str(p.get("zone", "A")).upper()
            base_val = float(p.get("base_value", 0.0) or 0.0)
            ref_price = float(zone_prices.get(zone, base_val) or base_val or 1.0)
            gain_ratio = (ref_price - max(1.0, base_val)) / max(1.0, base_val)
            scored.append(
                {
                    "property_id": int(p.get("property_id")),
                    "gain_ratio": float(gain_ratio),
                    "base_value": base_val,
                    "zone": zone,
                }
            )

        # Sell priority:
        # - downtrend: cut weakest performer first
        # - stable/uptrend: realize profit on strongest performer
        if downtrend:
            scored.sort(key=lambda x: (x["gain_ratio"], x["base_value"]))
            strategy = "C"   # urgent
            coeff = 0.95
        elif uptrend:
            scored.sort(key=lambda x: (x["gain_ratio"], x["base_value"]), reverse=True)
            strategy = "A"   # aggressive
            coeff = 1.08
        else:
            scored.sort(key=lambda x: x["base_value"])
            strategy = "B"   # balanced
            coeff = 1.00

        target_ids = [scored[0]["property_id"]] if scored else []
        decision = {
            "strategy": strategy,
            "pricing_coefficient": float(coeff),
            "properties_to_sell": target_ids,
            "reasoning": f"Rule-based normal seller ({trend})",
        }
        metrics = {
            "pricing_source": "RULE_NORMAL",
            "market_trend": trend,
            "chosen_property_id": target_ids[0] if target_ids else None,
            "chosen_gain_ratio": scored[0]["gain_ratio"] if scored else None,
            "coefficient": float(coeff),
        }
        return decision, metrics
