
"""
Transaction Engine: Handles Listings, Matching, Negotiation, and Execution
"""
import asyncio
import hashlib
import json
import logging
import os
import random
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from agent_behavior import decide_negotiation_format, safe_call_llm, safe_call_llm_async
from models import Agent, Market
from mortgage_system import calculate_max_affordable_price, check_affordability
from prompts.system_prompts import (
    SYSTEM_PROMPT_BUYER_NEGOTIATION,
    SYSTEM_PROMPT_SELLER_NEGOTIATION,
    SYSTEM_PROMPT_TRANSACTION_ARBITER,
)
from services.financial_calculator import FinancialCalculator

logger = logging.getLogger(__name__)

# --- Helper: Build Macro Context (Moved from agent_behavior if circular dep, or reimplement) ---


def _stable_prompt_json(payload) -> str:
    """
    Stable JSON for prompt embedding:
    - sort keys to reduce ordering jitter
    - compact separators to reduce token overhead
    """
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _read_bool_config(config, key: str, default: bool) -> bool:
    try:
        raw = config.get(key, default) if config is not None else default
    except Exception:
        raw = default
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _read_int_config(config, key: str, default: int) -> int:
    try:
        raw = config.get(key, default) if config is not None else default
        return int(raw)
    except Exception:
        return int(default)


def _read_float_config(config, key: str, default: float) -> float:
    try:
        raw = config.get(key, default) if config is not None else default
        return float(raw)
    except Exception:
        return float(default)


def _resolve_buyer_match_visible_shortlist_cap(config=None, configured_top_k: int = 5) -> int:
    """
    Cap the LLM-visible shortlist to a small set so that buyer selection stays
    focused and prompt cost remains predictable.
    """
    default_cap = max(3, min(5, int(configured_top_k or 5)))
    try:
        raw_cap = (
            config.get(
                "smart_agent.buyer_match_visible_shortlist_cap",
                config.get("buyer_match_visible_shortlist_cap", default_cap),
            )
            if config
            else default_cap
        )
        cap = int(raw_cap)
    except Exception:
        cap = default_cap
    return max(3, min(5, int(cap)))


def _compute_shortlist_overlap_ratio(previous_ids, current_ids) -> float:
    previous = {int(x) for x in (previous_ids or []) if x is not None}
    current = {int(x) for x in (current_ids or []) if x is not None}
    if not previous or not current:
        return 0.0
    union = previous | current
    if not union:
        return 0.0
    return float(len(previous & current)) / float(len(union))


def _resolve_hot_listing_auto_bidding_mode(
    listing: Dict,
    buyer_count: int,
    config=None,
) -> Optional[str]:
    """
    Process-layer override only: when a listing is clearly hot, route the session
    into a more competitive negotiation format. This never chooses a winner.
    """
    if int(buyer_count or 0) < 2:
        return None
    enabled = _read_bool_config(
        config,
        "smart_agent.hot_listing_auto_bidding_enabled",
        True,
    )
    if not enabled:
        return None
    fake_hot_block_auction_enabled = _read_bool_config(
        config,
        "smart_agent.fake_hot_block_auction_enabled",
        True,
    )
    min_buyers = max(
        2,
        min(8, _read_int_config(config, "smart_agent.hot_listing_auto_bidding_min_buyers", 3)),
    )
    min_recent_competitions = max(
        2,
        min(
            24,
            _read_int_config(
                config,
                "smart_agent.hot_listing_auto_bidding_min_recent_competitions",
                _read_int_config(config, "smart_agent.hot_listing_auto_bidding_min_recent_matches", 8),
            ),
        ),
    )
    min_heat_score = max(
        0.10,
        min(1.0, _read_float_config(config, "smart_agent.hot_listing_auto_bidding_min_heat_score", 0.60)),
    )
    mode = str(
        config.get("smart_agent.hot_listing_auto_bidding_mode", "BATCH")
        if config is not None
        else "BATCH"
    ).strip().upper()
    if mode not in {"BATCH", "CLASSIC"}:
        mode = "BATCH"

    is_fake_hot = bool(
        listing.get("_fake_hot_historical", False)
        or listing.get("_same_month_fake_hot", False)
        or listing.get("_is_fake_hot", False)
    )
    recent_competitions = int(
        listing.get("_recent_competition_count", listing.get("_recent_match_count", 0)) or 0
    )
    recent_commitments = int(listing.get("_recent_commitment_count", 0) or 0)
    hot_score = float(
        listing.get("_real_competition_score", listing.get("_hot_listing_score", 0.0)) or 0.0
    )
    current_interest = max(int(buyer_count or 0), int(listing.get("_current_interest_count", 0) or 0))
    if is_fake_hot and fake_hot_block_auction_enabled:
        return None

    force_auction_enabled = _read_bool_config(
        config,
        "smart_agent.true_competition_force_auction_enabled",
        True,
    )
    force_mode = str(
        config.get("smart_agent.true_competition_force_auction_mode", mode)
        if config is not None
        else mode
    ).strip().upper()
    if force_mode not in {"BATCH", "CLASSIC"}:
        force_mode = mode
    force_min_buyers = max(
        2,
        min(8, _read_int_config(config, "smart_agent.true_competition_force_auction_min_buyers", 2)),
    )
    force_min_commitments = max(
        1,
        min(8, _read_int_config(config, "smart_agent.true_competition_force_auction_min_commitments", 2)),
    )
    force_min_competitions = max(
        1,
        min(12, _read_int_config(config, "smart_agent.true_competition_force_auction_min_competitions", 2)),
    )
    if current_interest < min_buyers:
        return None
    if (
        force_auction_enabled
        and current_interest >= force_min_buyers
        and recent_commitments >= force_min_commitments
        and recent_competitions >= force_min_competitions
    ):
        return force_mode
    if recent_competitions >= min_recent_competitions or hot_score >= min_heat_score:
        return mode
    return None


def _resolve_batch_tie_break_route(
    finalists: List[Dict],
    seller: Agent,
    listing: Dict,
    config=None,
) -> Dict[str, object]:
    """
    Gray-zone routing for near-equal tie-break only. This never replaces the
    seller's ultimate LLM choice; it only chooses fast vs smart model.
    """
    if not finalists:
        return {"model": "fast", "gray_score": 0.0, "reason": "no_finalists"}
    enabled = _read_bool_config(
        config,
        "smart_agent.batch_tie_break_dual_routing_enabled",
        True,
    )
    if not enabled:
        return {"model": "smart", "gray_score": 1.0, "reason": "dual_routing_disabled"}

    prices = [float(x.get("price", 0.0) or 0.0) for x in finalists]
    top_price = max(prices) if prices else 0.0
    low_price = min(prices) if prices else 0.0
    relative_span = 0.0 if top_price <= 0 else max(0.0, min(1.0, (top_price - low_price) / top_price / 0.02))
    smart_buyer_ratio = (
        sum(1 for x in finalists if str(getattr(x.get("buyer"), "agent_type", "normal")).lower() == "smart")
        / max(1, len(finalists))
    )
    seller_smart = 1.0 if str(getattr(seller, "agent_type", "normal")).lower() == "smart" else 0.0
    finalist_count_factor = min(1.0, max(0.0, (len(finalists) - 1) / 3.0))
    hot_score = min(
        1.0,
        max(
            0.0,
            float(listing.get("_real_competition_score", listing.get("_hot_listing_score", 0.0)) or 0.0),
        ),
    )
    current_interest = int(listing.get("_current_interest_count", 0) or 0)
    interest_factor = min(1.0, max(0.0, (current_interest - 1) / 5.0))

    gray_score = (
        0.26 * (1.0 - relative_span)
        + 0.22 * smart_buyer_ratio
        + 0.16 * seller_smart
        + 0.18 * finalist_count_factor
        + 0.10 * hot_score
        + 0.08 * interest_factor
    )
    threshold = max(
        0.10,
        min(0.95, _read_float_config(config, "smart_agent.batch_tie_break_gray_score_threshold", 0.50)),
    )
    if gray_score >= threshold:
        return {
            "model": "smart",
            "gray_score": float(gray_score),
            "reason": f"gray_zone score={gray_score:.3f} threshold={threshold:.3f}",
        }
    return {
        "model": "fast",
        "gray_score": float(gray_score),
        "reason": f"clear_case score={gray_score:.3f} threshold={threshold:.3f}",
    }


def _maybe_reuse_same_month_stop_signal(
    buyer: Agent,
    current_match_month: int,
    retry_attempt: int,
    full_shortlist_ids: List[int],
    visible_shortlist_ids: List[int],
    strategy_profile: str,
    market_trend: str,
    crowd_mode: str,
    config=None,
) -> Optional[Dict[str, object]]:
    """
    Reuse a same-month explicit LLM stop decision when the current shortlist is
    materially the same as the one the buyer already rejected this month.
    This preserves the original LLM choice instead of re-asking the same
    question with near-identical inputs.
    """
    if int(retry_attempt or 0) <= 0:
        return None
    if int(current_match_month or -1) <= 0:
        return None

    last_ctx = dict(getattr(buyer, "_last_buyer_match_context", {}) or {})
    if not last_ctx:
        return None
    if int(last_ctx.get("matching_month", -1) or -1) != int(current_match_month):
        return None
    if not bool(last_ctx.get("llm_called", False)):
        return None
    if not bool(last_ctx.get("stop_search_this_month", False)):
        return None
    if last_ctx.get("selected_property_id") is not None:
        return None

    previous_shortlist_ids = list(
        last_ctx.get("shortlist_property_ids", [])
        or last_ctx.get("shortlist_visible_property_ids", [])
        or []
    )
    overlap_ratio = _compute_shortlist_overlap_ratio(previous_shortlist_ids, full_shortlist_ids)
    try:
        raw_threshold = (
            config.get(
                "smart_agent.same_month_stop_reuse_overlap_threshold",
                config.get("same_month_stop_reuse_overlap_threshold", 0.60),
            )
            if config
            else 0.60
        )
        overlap_threshold = float(raw_threshold)
    except Exception:
        overlap_threshold = 0.60
    overlap_threshold = max(0.20, min(0.95, float(overlap_threshold)))
    if overlap_ratio < overlap_threshold:
        return None

    reused_ctx = dict(last_ctx)
    tags = list(reused_ctx.get("selection_reason_tags", []) or [])
    if "route:same_month_stop_reuse" not in tags:
        tags.append("route:same_month_stop_reuse")
    reused_ctx.update(
        {
            "matching_month": int(current_match_month),
            "strategy_profile": str(strategy_profile),
            "market_trend": str(market_trend),
            "crowd_mode": str(crowd_mode),
            "retry_attempt": int(retry_attempt),
            "shortlist_property_ids": [int(x) for x in full_shortlist_ids],
            "shortlist_visible_property_ids": [int(x) for x in visible_shortlist_ids],
            "shortlist_full_size": int(len(full_shortlist_ids)),
            "shortlist_visible_size": int(len(visible_shortlist_ids)),
            "selected_property_id": None,
            "selected_in_shortlist": False,
            "llm_monthly_intent": "STOP",
            "stop_search_this_month": True,
            "selection_reason_tags": tags,
            "llm_route_model": "reuse",
            "llm_route_reason": f"same_month_stop_reuse overlap={overlap_ratio:.3f}",
            "llm_called": False,
            "same_month_stop_reused": True,
            "same_month_stop_reuse_overlap": float(overlap_ratio),
        }
    )
    return reused_ctx


def build_macro_context(month: int, config=None) -> str:
    """Builds macro-economic context string."""
    # This might need to be imported or reconstructed if agent_behavior usage causes circular import.
    # For now, let's assume it's safe to import IF agent_behavior doesn't import transaction_engine.
    # Actually transaction_engine imports agent_behavior, so it's fine.
    # But wait, I removed it from import list above to check.
    # It was: from agent_behavior import ..., build_macro_context
    # I will reimplement here to be safe and simple.

    risk_free_rate = 0.03
    ltv = 0.7
    if config:
        risk_free_rate = config.market.get('risk_free_rate', 0.03)
        ltv = config.mortgage.get('max_ltv', 0.7)

    try:
        current_month = max(1, int(month))
    except Exception:
        current_month = 1

    return (
        f"【宏观环境·第{current_month}回合（虚拟周期）】"
        f"无风险利率: {risk_free_rate * 100:.1f}%, "
        f"首付比例: {(1 - ltv) * 100:.0f}%"
    )


def _clamp01(v: float) -> float:
    try:
        return max(0.0, min(1.0, float(v)))
    except Exception:
        return 0.0


def _to_bool(raw, default: bool) -> bool:
    if raw is None:
        return bool(default)
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _resolve_batch_bidding_tiebreak_config(config=None) -> Dict[str, float]:
    enabled = True
    max_price_gap_ratio = 0.005
    max_price_gap_abs = 5000.0
    if config is not None:
        try:
            enabled = _to_bool(config.get("smart_agent.batch_b_zone_tiebreak_enabled", enabled), enabled)
        except Exception:
            enabled = True
        try:
            max_price_gap_ratio = float(
                config.get("smart_agent.batch_b_zone_tiebreak_max_gap_ratio", max_price_gap_ratio)
            )
        except Exception:
            max_price_gap_ratio = 0.005
        try:
            max_price_gap_abs = float(
                config.get("smart_agent.batch_b_zone_tiebreak_max_gap_abs", max_price_gap_abs)
            )
        except Exception:
            max_price_gap_abs = 5000.0
    return {
        "enabled": bool(enabled),
        "max_price_gap_ratio": max(0.0, min(0.05, float(max_price_gap_ratio))),
        "max_price_gap_abs": max(0.0, float(max_price_gap_abs)),
    }


def _batch_bid_headroom_ratio(buyer: Agent, bid_price: float, config=None) -> float:
    try:
        max_affordable = float(calculate_max_affordable_price(buyer, config) or 0.0)
    except Exception:
        max_affordable = 0.0
    bid_price = max(1.0, float(bid_price or 0.0))
    if max_affordable <= 0:
        return 1.0
    return max(0.0, (max_affordable - bid_price) / bid_price)


def _sort_batch_bids_for_winner(bids: List[Dict], listing: Dict, config=None) -> List[Dict]:
    """
    Resolve batch-bidding winner order.
    Primary sort remains final bid price desc.
    For B-zone near-tie bids only, prefer lower headroom entrants so that
    equal-price batch sessions do not mechanically favor the strongest budget.
    """
    if not bids:
        return []
    zone = str((listing or {}).get("zone", "") or "").upper()
    tiebreak_cfg = _resolve_batch_bidding_tiebreak_config(config)
    listed_price = float((listing or {}).get("listed_price", 0.0) or 0.0)
    max_gap = max(
        float(tiebreak_cfg["max_price_gap_abs"]),
        listed_price * float(tiebreak_cfg["max_price_gap_ratio"]),
    )

    def _key(entry: Dict):
        price = float(entry.get("price", 0.0) or 0.0)
        buyer = entry.get("buyer")
        if zone == "B" and bool(tiebreak_cfg["enabled"]):
            price_bucket = round(price / max(1.0, max_gap))
            headroom_ratio = _batch_bid_headroom_ratio(buyer, price, config)
            owned_count = 0
            try:
                owned_count = len(getattr(buyer, "properties", []) or [])
            except Exception:
                owned_count = 0
            return (-price_bucket, headroom_ratio, owned_count, int(getattr(buyer, "id", 0)))
        return (-price, int(getattr(buyer, "id", 0)))

    return sorted(list(bids), key=_key)


def _batch_winner_tie_gap(listing: Dict, config=None) -> float:
    cfg = _resolve_batch_bidding_tiebreak_config(config)
    listed_price = float((listing or {}).get("listed_price", 0.0) or 0.0)
    return max(
        float(cfg["max_price_gap_abs"]),
        listed_price * float(cfg["max_price_gap_ratio"]),
    )


def _resolve_batch_tie_break_rebid_config(config=None) -> Dict[str, float]:
    enabled = True
    min_increment_abs = 1000.0
    min_increment_ratio = 0.005
    if config is not None:
        try:
            enabled = _to_bool(config.get("smart_agent.batch_b_zone_extra_rebid_enabled", enabled), enabled)
        except Exception:
            enabled = True
        try:
            min_increment_abs = float(
                config.get("smart_agent.batch_b_zone_extra_rebid_min_increment_abs", min_increment_abs)
            )
        except Exception:
            min_increment_abs = 1000.0
        try:
            min_increment_ratio = float(
                config.get("smart_agent.batch_b_zone_extra_rebid_min_increment_ratio", min_increment_ratio)
            )
        except Exception:
            min_increment_ratio = 0.005
    return {
        "enabled": bool(enabled),
        "min_increment_abs": max(0.0, float(min_increment_abs)),
        "min_increment_ratio": max(0.0, min(0.05, float(min_increment_ratio))),
    }


def _compute_batch_extra_rebid_increment(top_price: float, config=None) -> float:
    cfg = _resolve_batch_tie_break_rebid_config(config)
    return max(float(cfg["min_increment_abs"]), float(top_price) * float(cfg["min_increment_ratio"]))


def _pick_batch_tied_finalists(bids: List[Dict], listing: Dict, config=None) -> List[Dict]:
    ranked = sorted(list(bids), key=lambda x: float(x.get("price", 0.0) or 0.0), reverse=True)
    if not ranked:
        return []
    if str((listing or {}).get("zone", "") or "").upper() != "B":
        return []
    top_price = float(ranked[0].get("price", 0.0) or 0.0)
    tie_gap = _batch_winner_tie_gap(listing, config)
    finalists = [b for b in ranked if abs(float(b.get("price", 0.0) or 0.0) - top_price) <= tie_gap]
    return finalists if len(finalists) >= 2 else []


def _run_batch_tie_break_rebid(
    bids: List[Dict], seller: Agent, listing: Dict, config=None
) -> Tuple[List[Dict], List[Dict]]:
    cfg = _resolve_batch_tie_break_rebid_config(config)
    if not bool(cfg["enabled"]):
        return bids, []
    finalists = _pick_batch_tied_finalists(bids, listing, config)
    if not finalists:
        return bids, []
    route_info = _resolve_batch_tie_break_route(finalists, seller, listing, config)
    top_price = max(float(e.get("price", 0.0) or 0.0) for e in finalists)
    min_raise = _compute_batch_extra_rebid_increment(top_price, config)
    history = []
    updated = []
    finalist_ids = {int(getattr(e.get("buyer"), "id", -1)) for e in finalists}
    for entry in bids:
        buyer = entry.get("buyer")
        buyer_id = int(getattr(buyer, "id", -1))
        if buyer_id not in finalist_ids:
            updated.append(entry)
            continue
        max_budget = float(calculate_max_affordable_price(buyer, config) or 0.0)
        prompt = f"""
        你是买家 {buyer_id}。B区盲拍进入近同价终局加价。
        房产: {listing.get('zone')}区 {listing.get('building_area')}㎡
        当前并列最高有效报价: ¥{top_price:,.0f}
        本轮最小额外加价: ¥{min_raise:,.0f}
        你的当前有效报价: ¥{float(entry.get('price', 0.0) or 0.0):,.0f}
        你的预算上限: ¥{max_budget:,.0f}

        规则：
        - 若继续争取，请给出你愿意承担的真实终局封顶报价，且必须 >= 当前并列最高报价 + 最小额外加价。
        - 这是 sealed-cap 一口价终局，不要机械重复“最低要求值”，除非那就是你真实愿意承担的最高价。
        - 若不愿再加价，可输出 0，保持当前报价不变。
        - 不得超过预算上限。

        输出JSON: {{"bid_price": float, "reason": "..."}}
        """
        resp = safe_call_llm(
            prompt,
            {"bid_price": 0, "reason": "Hold current tied bid"},
            model_type=str(route_info.get("model", "fast") or "fast"),
        )
        raw_bid = normalize_llm_price_scale(float(resp.get("bid_price", 0) or 0.0), float(top_price), float(max_budget))
        if raw_bid > 0:
            clamped = clamp_offer_price(raw_bid, float(listing.get("listed_price", top_price) or top_price), float(max_budget), config=config)
            next_floor = float(top_price) + float(min_raise)
            final_bid = float(clamped)
            is_affordable, _, _ = check_affordability(buyer, final_bid, config)
            if is_affordable and final_bid >= next_floor:
                entry = dict(entry)
                entry["price"] = float(final_bid)
                history.append(
                    {
                        "round": 99,
                        "party": "buyer",
                        "agent_id": buyer_id,
                        "action": "TIE_BREAK_FINAL_BID",
                        "price": float(final_bid),
                        "content": str(resp.get("reason", "") or ""),
                        "model": str(route_info.get("model", "fast") or "fast"),
                        "gray_score": float(route_info.get("gray_score", 0.0) or 0.0),
                        "llm_called": 1,
                    }
                )
        updated.append(entry)
    return updated, history


async def _run_batch_tie_break_rebid_async(
    bids: List[Dict], seller: Agent, listing: Dict, config=None
) -> Tuple[List[Dict], List[Dict]]:
    cfg = _resolve_batch_tie_break_rebid_config(config)
    if not bool(cfg["enabled"]):
        return bids, []
    finalists = _pick_batch_tied_finalists(bids, listing, config)
    if not finalists:
        return bids, []
    route_info = _resolve_batch_tie_break_route(finalists, seller, listing, config)
    top_price = max(float(e.get("price", 0.0) or 0.0) for e in finalists)
    min_raise = _compute_batch_extra_rebid_increment(top_price, config)
    finalist_ids = {int(getattr(e.get("buyer"), "id", -1)) for e in finalists}
    history = []

    async def _rebid(entry: Dict) -> Dict:
        buyer = entry.get("buyer")
        buyer_id = int(getattr(buyer, "id", -1))
        max_budget = float(calculate_max_affordable_price(buyer, config) or 0.0)
        prompt = f"""
        你是买家 {buyer_id}。B区盲拍进入近同价终局加价。
        房产: {listing.get('zone')}区 {listing.get('building_area')}㎡
        当前并列最高有效报价: ¥{top_price:,.0f}
        本轮最小额外加价: ¥{min_raise:,.0f}
        你的当前有效报价: ¥{float(entry.get('price', 0.0) or 0.0):,.0f}
        你的预算上限: ¥{max_budget:,.0f}

        规则：
        - 若继续争取，请给出你愿意承担的真实终局封顶报价，且必须 >= 当前并列最高报价 + 最小额外加价。
        - 这是 sealed-cap 一口价终局，不要机械重复“最低要求值”，除非那就是你真实愿意承担的最高价。
        - 若不愿再加价，可输出 0，保持当前报价不变。
        - 不得超过预算上限。

        输出JSON: {{"bid_price": float, "reason": "..."}}
        """
        resp = await safe_call_llm_async(
            prompt,
            {"bid_price": 0, "reason": "Hold current tied bid"},
            model_type=str(route_info.get("model", "fast") or "fast"),
        )
        raw_bid = normalize_llm_price_scale(float(resp.get("bid_price", 0) or 0.0), float(top_price), float(max_budget))
        out = dict(entry)
        out["_tie_break_reason"] = str(resp.get("reason", "") or "")
        if raw_bid > 0:
            clamped = clamp_offer_price(raw_bid, float(listing.get("listed_price", top_price) or top_price), float(max_budget), config=config)
            next_floor = float(top_price) + float(min_raise)
            final_bid = float(clamped)
            is_affordable, _, _ = check_affordability(buyer, final_bid, config)
            if is_affordable and final_bid >= next_floor:
                out["price"] = float(final_bid)
                out["_tie_break_upgraded"] = True
                return out
        out["_tie_break_upgraded"] = False
        return out

    rebid_map = {}
    if finalists:
        rebid_results = await asyncio.gather(*[_rebid(e) for e in finalists])
        rebid_map = {int(getattr(e.get("buyer"), "id", -1)): e for e in rebid_results}

    updated = []
    for entry in bids:
        buyer_id = int(getattr(entry.get("buyer"), "id", -1))
        rebid = rebid_map.get(buyer_id)
        if not rebid:
            updated.append(entry)
            continue
        entry = dict(rebid)
        if bool(entry.pop("_tie_break_upgraded", False)):
            history.append(
                {
                    "round": 99,
                    "party": "buyer",
                    "agent_id": buyer_id,
                    "action": "TIE_BREAK_FINAL_BID",
                    "price": float(entry.get("price", 0.0) or 0.0),
                    "content": str(entry.pop("_tie_break_reason", "") or ""),
                    "model": str(route_info.get("model", "fast") or "fast"),
                    "gray_score": float(route_info.get("gray_score", 0.0) or 0.0),
                    "llm_called": 1,
                }
            )
        else:
            entry.pop("_tie_break_reason", None)
        updated.append(entry)
    return updated, history


def _resolve_batch_bid_winner(bids: List[Dict], seller: Agent, listing: Dict, config=None) -> Tuple[Dict, List[Dict]]:
    ranked = sorted(list(bids), key=lambda x: float(x.get("price", 0.0) or 0.0), reverse=True)
    if not ranked:
        return {}, []
    if str((listing or {}).get("zone", "") or "").upper() != "B":
        return ranked[0], []
    top_price = float(ranked[0].get("price", 0.0) or 0.0)
    tie_gap = _batch_winner_tie_gap(listing, config)
    finalists = [b for b in ranked if abs(float(b.get("price", 0.0) or 0.0) - top_price) <= tie_gap]
    if len(finalists) <= 1:
        return ranked[0], []
    route_info = _resolve_batch_tie_break_route(finalists, seller, listing, config)

    candidate_rows = []
    for entry in finalists:
        buyer = entry["buyer"]
        price = float(entry.get("price", 0.0) or 0.0)
        candidate_rows.append(
            {
                "buyer_id": int(getattr(buyer, "id", -1)),
                "offer_price": round(price, 2),
                "headroom_ratio": round(_batch_bid_headroom_ratio(buyer, price, config), 4),
                "cash": round(float(getattr(buyer, "cash", 0.0) or 0.0), 2),
                "monthly_income": round(float(getattr(buyer, "monthly_income", 0.0) or 0.0), 2),
                "owned_count": len(getattr(buyer, "properties", []) or []),
                "style": str(getattr(getattr(buyer, "story", None), "investment_style", "") or ""),
            }
        )
    prompt = f"""
    你是卖家 {getattr(seller, 'id', -1)}。当前 B 区房源进入批量竞价的最终近同价决胜。
    房源: {listing.get('zone')}区 {listing.get('building_area')}㎡
    挂牌价: ¥{float(listing.get('listed_price', 0.0) or 0.0):,.0f}
    可接受底价: ¥{float(listing.get('min_price', 0.0) or 0.0):,.0f}

    以下买家的最终有效报价已非常接近，请你决定更愿意把房卖给谁。
    候选:
    {json.dumps(candidate_rows, ensure_ascii=False)}

    只输出 JSON: {{"buyer_id": int, "reason": "..."}}。
    """
    fallback = _sort_batch_bids_for_winner(finalists, listing, config)[0]
    resp = safe_call_llm(
        prompt,
        {"buyer_id": int(getattr(fallback["buyer"], "id", -1)), "reason": "Fallback near-equal batch tie-break"},
        model_type=str(route_info.get("model", "fast") or "fast"),
    )
    try:
        chosen_id = int(resp.get("buyer_id"))
    except Exception:
        chosen_id = int(getattr(fallback["buyer"], "id", -1))
    selected = next((entry for entry in finalists if int(getattr(entry["buyer"], "id", -1)) == chosen_id), fallback)
    history_event = {
        "round": 1,
        "party": "seller",
        "agent_id": int(getattr(seller, "id", -1)),
        "action": "SELLER_TIE_BREAK",
        "price": float(selected.get("price", 0.0) or 0.0),
        "buyer": int(getattr(selected["buyer"], "id", -1)),
        "buyer_id": int(getattr(selected["buyer"], "id", -1)),
        "content": str(resp.get("reason", "") or "Near-equal batch tie-break"),
        "model": str(route_info.get("model", "fast") or "fast"),
        "gray_score": float(route_info.get("gray_score", 0.0) or 0.0),
        "llm_called": 1,
    }
    return selected, [history_event]


async def _resolve_batch_bid_winner_async(bids: List[Dict], seller: Agent, listing: Dict, config=None) -> Tuple[Dict, List[Dict]]:
    ranked = sorted(list(bids), key=lambda x: float(x.get("price", 0.0) or 0.0), reverse=True)
    if not ranked:
        return {}, []
    if str((listing or {}).get("zone", "") or "").upper() != "B":
        return ranked[0], []
    top_price = float(ranked[0].get("price", 0.0) or 0.0)
    tie_gap = _batch_winner_tie_gap(listing, config)
    finalists = [b for b in ranked if abs(float(b.get("price", 0.0) or 0.0) - top_price) <= tie_gap]
    if len(finalists) <= 1:
        return ranked[0], []
    route_info = _resolve_batch_tie_break_route(finalists, seller, listing, config)

    candidate_rows = []
    for entry in finalists:
        buyer = entry["buyer"]
        price = float(entry.get("price", 0.0) or 0.0)
        candidate_rows.append(
            {
                "buyer_id": int(getattr(buyer, "id", -1)),
                "offer_price": round(price, 2),
                "headroom_ratio": round(_batch_bid_headroom_ratio(buyer, price, config), 4),
                "cash": round(float(getattr(buyer, "cash", 0.0) or 0.0), 2),
                "monthly_income": round(float(getattr(buyer, "monthly_income", 0.0) or 0.0), 2),
                "owned_count": len(getattr(buyer, "properties", []) or []),
                "style": str(getattr(getattr(buyer, "story", None), "investment_style", "") or ""),
            }
        )
    prompt = f"""
    你是卖家 {getattr(seller, 'id', -1)}。当前 B 区房源进入批量竞价的最终近同价决胜。
    房源: {listing.get('zone')}区 {listing.get('building_area')}㎡
    挂牌价: ¥{float(listing.get('listed_price', 0.0) or 0.0):,.0f}
    可接受底价: ¥{float(listing.get('min_price', 0.0) or 0.0):,.0f}

    以下买家的最终有效报价已非常接近，请你决定更愿意把房卖给谁。
    候选:
    {json.dumps(candidate_rows, ensure_ascii=False)}

    只输出 JSON: {{"buyer_id": int, "reason": "..."}}。
    """
    fallback = _sort_batch_bids_for_winner(finalists, listing, config)[0]
    resp = await safe_call_llm_async(
        prompt,
        {"buyer_id": int(getattr(fallback["buyer"], "id", -1)), "reason": "Fallback near-equal batch tie-break"},
        model_type=str(route_info.get("model", "fast") or "fast"),
    )
    try:
        chosen_id = int(resp.get("buyer_id"))
    except Exception:
        chosen_id = int(getattr(fallback["buyer"], "id", -1))
    selected = next((entry for entry in finalists if int(getattr(entry["buyer"], "id", -1)) == chosen_id), fallback)
    history_event = {
        "round": 1,
        "party": "seller",
        "agent_id": int(getattr(seller, "id", -1)),
        "action": "SELLER_TIE_BREAK",
        "price": float(selected.get("price", 0.0) or 0.0),
        "buyer": int(getattr(selected["buyer"], "id", -1)),
        "buyer_id": int(getattr(selected["buyer"], "id", -1)),
        "content": str(resp.get("reason", "") or "Near-equal batch tie-break"),
        "model": str(route_info.get("model", "fast") or "fast"),
        "gray_score": float(route_info.get("gray_score", 0.0) or 0.0),
        "llm_called": 1,
    }
    return selected, [history_event]


def _regime_v1_enabled(config=None) -> bool:
    try:
        raw = (
            config.get(
                "smart_agent.regime_engine_v1_enabled",
                config.get("regime_engine_v1_enabled", False),
            )
            if config
            else False
        )
        return _to_bool(raw, False)
    except Exception:
        return False


def _seller_close_priority_score(
    *,
    min_price: float,
    current_price: float,
    candidate_price: float,
    market_condition: str,
    round_index: int,
    total_rounds: int,
) -> float:
    current_price = float(max(1.0, current_price))
    min_price = float(max(1.0, min_price))
    candidate_price = float(max(0.0, candidate_price))
    hold_pressure = _clamp01((current_price - candidate_price) / max(current_price, 1.0)) * 100.0
    floor_gap = _clamp01((candidate_price - min_price) / max(current_price, 1.0)) * 100.0
    round_pressure = _clamp01(float(round_index) / max(1.0, float(total_rounds))) * 100.0
    market_bias = 35.0
    mc = str(market_condition or "").lower()
    if mc == "oversupply":
        market_bias = 70.0
    elif mc == "undersupply":
        market_bias = 25.0
    return (
        hold_pressure * 0.30
        + (100.0 - floor_gap) * 0.20
        + round_pressure * 0.30
        + market_bias * 0.20
    )


def _apply_seller_reject_guardrail(
    *,
    seller_action: str,
    counter_price: float,
    seller_reason: str,
    min_price: float,
    current_price: float,
    candidate_price: float,
    market_condition: str,
    round_index: int,
    total_rounds: int,
    config=None,
) -> Tuple[str, float, str]:
    """
    Regime V1 guardrail (rollback-friendly):
    If seller is high close-priority and offer is already near floor, rewrite hard REJECT to COUNTER.
    """
    if not _regime_v1_enabled(config):
        return seller_action, counter_price, seller_reason
    try:
        enabled = _to_bool(
            config.get(
                "smart_agent.regime_v1_negotiation_convergence_enabled",
                config.get("regime_v1_negotiation_convergence_enabled", True),
            ),
            True,
        )
    except Exception:
        enabled = True
    if not enabled:
        return seller_action, counter_price, seller_reason
    if str(seller_action or "").upper() != "REJECT":
        return seller_action, counter_price, seller_reason

    close_priority = _seller_close_priority_score(
        min_price=float(min_price),
        current_price=float(current_price),
        candidate_price=float(candidate_price),
        market_condition=str(market_condition),
        round_index=int(round_index),
        total_rounds=int(total_rounds),
    )
    try:
        threshold = float(
            config.get(
                "smart_agent.regime_v1_reject_to_counter_close_priority_threshold",
                config.get("regime_v1_reject_to_counter_close_priority_threshold", 68.0),
            )
        )
    except Exception:
        threshold = 68.0
    try:
        near_floor_ratio = float(
            config.get(
                "smart_agent.regime_v1_reject_to_counter_near_floor_ratio",
                config.get("regime_v1_reject_to_counter_near_floor_ratio", 0.98),
            )
        )
    except Exception:
        near_floor_ratio = 0.98
    try:
        final_round_threshold = float(
            config.get(
                "smart_agent.regime_v1_reject_to_counter_final_round_threshold",
                config.get("regime_v1_reject_to_counter_final_round_threshold", 58.0),
            )
        )
    except Exception:
        final_round_threshold = 58.0
    is_final_round = int(round_index) >= int(total_rounds)
    candidate_above_floor = float(candidate_price) >= float(min_price) - 1e-6

    if is_final_round and candidate_above_floor and close_priority >= final_round_threshold:
        target_counter = max(float(min_price), min(float(current_price), float(candidate_price) * 1.01))
        reason = str(seller_reason or "").strip()
        suffix = (
            f"RegimeV1终局收敛护栏触发: 成交优先度={close_priority:.1f}，"
            "最终轮报价已达到底价区间，改为COUNTER以给买方最后确认机会。"
        )
        reason = f"{reason} | {suffix}" if reason else suffix
        return "COUNTER", float(target_counter), reason

    if close_priority < threshold:
        return seller_action, counter_price, seller_reason
    if float(candidate_price) < float(min_price) * max(0.8, min(1.1, near_floor_ratio)):
        return seller_action, counter_price, seller_reason

    target_counter = max(float(min_price), min(float(current_price), float(candidate_price) * 1.01))
    reason = str(seller_reason or "").strip()
    suffix = (
        f"RegimeV1收敛护栏触发: 成交优先度={close_priority:.1f}，"
        "报价已接近可成交区间，改为COUNTER以避免无效谈崩。"
    )
    reason = f"{reason} | {suffix}" if reason else suffix
    return "COUNTER", float(target_counter), reason


def _run_buyer_final_counter_last_look(
    *,
    buyer: Agent,
    seller: Agent,
    current_price: float,
    min_price: float,
    round_index: int,
    total_rounds: int,
    market_hint: str,
    macro_context: str,
    negotiation_log: List[Dict],
) -> Dict:
    buyer_style = getattr(getattr(buyer, "story", None), "negotiation_style", "balanced")
    prompt = f"""
    {macro_context}
    你是买方Agent {buyer.id}。
    当前进入最终收官回应：卖方在最后一轮给出了最终还价。

    【交易背景】
    - 卖方Agent: {seller.id}
    - 当前最终还价: {float(current_price):,.0f}
    - 卖方底价: {float(min_price):,.0f}
    - 你的预算上限: {float(getattr(getattr(buyer, 'preference', None), 'max_price', 0.0) or 0.0):,.0f}
    - 当前轮次: {int(round_index)}/{int(total_rounds)}
    - 市场提示: {market_hint}
    - 你的风格: {buyer_style}

    【谈判历史】
    {_stable_prompt_json(negotiation_log)}

    这是最后一个明确选择点，不会再继续加轮次。
    请只做二选一：
    - ACCEPT: 接受卖方当前最终还价
    - WITHDRAW: 放弃本次交易

    输出JSON:
    {{"action":"ACCEPT"|"WITHDRAW","reason":"..."}}
    """
    return safe_call_llm(
        prompt,
        {"action": "WITHDRAW", "reason": "Final counter last-look fallback"},
        system_prompt=SYSTEM_PROMPT_BUYER_NEGOTIATION,
    )


async def _run_buyer_final_counter_last_look_async(
    *,
    buyer: Agent,
    seller: Agent,
    current_price: float,
    min_price: float,
    round_index: int,
    total_rounds: int,
    market_hint: str,
    macro_context: str,
    negotiation_log: List[Dict],
    llm_model_type: str,
) -> Dict:
    buyer_style = getattr(getattr(buyer, "story", None), "negotiation_style", "balanced")
    prompt = f"""
    {macro_context}
    你是买方Agent {buyer.id}。
    当前进入最终收官回应：卖方在最后一轮给出了最终还价。

    【交易背景】
    - 卖方Agent: {seller.id}
    - 当前最终还价: {float(current_price):,.0f}
    - 卖方底价: {float(min_price):,.0f}
    - 你的预算上限: {float(getattr(getattr(buyer, 'preference', None), 'max_price', 0.0) or 0.0):,.0f}
    - 当前轮次: {int(round_index)}/{int(total_rounds)}
    - 市场提示: {market_hint}
    - 你的风格: {buyer_style}

    【谈判历史】
    {_stable_prompt_json(negotiation_log)}

    这是最后一个明确选择点，不会再继续加轮次。
    请只做二选一：
    - ACCEPT: 接受卖方当前最终还价
    - WITHDRAW: 放弃本次交易

    输出JSON:
    {{"action":"ACCEPT"|"WITHDRAW","reason":"..."}}
    """
    return await safe_call_llm_async(
        prompt,
        {"action": "WITHDRAW", "reason": "Final counter last-look fallback"},
        system_prompt=SYSTEM_PROMPT_BUYER_NEGOTIATION,
        model_type=llm_model_type,
    )


def _build_seller_memory_panel_from_history(history: List[Dict], rounds_window: int = 2) -> str:
    if not history:
        return "- 暂无历史谈判记录。"
    seller_reject = 0
    seller_counter = 0
    seller_accept = 0
    buyer_offer = 0
    buyer_withdraw = 0
    last_round = 0
    for ev in history:
        if not isinstance(ev, dict):
            continue
        try:
            r = int(ev.get("round", 0) or 0)
        except Exception:
            r = 0
        last_round = max(last_round, r)
        party = str(ev.get("party", "")).lower()
        action = str(ev.get("action", "")).upper()
        if party == "buyer":
            if action == "OFFER":
                buyer_offer += 1
            elif action == "WITHDRAW":
                buyer_withdraw += 1
        if party in {"seller", "seller_recheck", "seller_closeout"}:
            if action == "REJECT":
                seller_reject += 1
            elif action == "COUNTER":
                seller_counter += 1
            elif action == "ACCEPT":
                seller_accept += 1
    round_cut = max(1, int(last_round) - int(rounds_window) + 1)
    recent_lines = []
    for ev in history:
        if not isinstance(ev, dict):
            continue
        try:
            r = int(ev.get("round", 0) or 0)
        except Exception:
            r = 0
        if r < round_cut:
            continue
        party = str(ev.get("party", "")).lower()
        action = str(ev.get("action", "")).upper()
        price = float(ev.get("price", 0.0) or 0.0)
        recent_lines.append(f"R{r} {party}:{action}@{price:,.0f}")
    recent_summary = " | ".join(recent_lines[-8:]) if recent_lines else "无"
    return (
        f"- 截至当前，你在本场会话累计 COUNTER={seller_counter}, REJECT={seller_reject}, ACCEPT={seller_accept}\n"
        f"- 买方累计 OFFER={buyer_offer}, WITHDRAW={buyer_withdraw}\n"
        f"- 最近{rounds_window}轮关键轨迹: {recent_summary}"
    )


def _build_seller_negotiation_scorecard(
    *,
    min_price: float,
    current_price: float,
    candidate_price: float,
    market_condition: str,
    round_index: int,
    total_rounds: int,
) -> str:
    current_price = float(max(1.0, current_price))
    min_price = float(max(1.0, min_price))
    candidate_price = float(max(0.0, candidate_price))
    hold_pressure = _clamp01((current_price - candidate_price) / max(current_price, 1.0)) * 100.0
    floor_gap = _clamp01((candidate_price - min_price) / max(current_price, 1.0)) * 100.0
    round_pressure = _clamp01(float(round_index) / max(1.0, float(total_rounds))) * 100.0
    market_bias = 35.0
    mc = str(market_condition or "").lower()
    if mc == "oversupply":
        market_bias = 70.0
    elif mc == "undersupply":
        market_bias = 25.0
    close_priority = (
        hold_pressure * 0.30
        + (100.0 - floor_gap) * 0.20
        + round_pressure * 0.30
        + market_bias * 0.20
    )
    if close_priority >= 70:
        suggestion = "更偏向 ACCEPT 或贴近成交价的 COUNTER"
    elif close_priority >= 45:
        suggestion = "更偏向小步 COUNTER"
    else:
        suggestion = "可维持更强议价立场"
    return (
        f"- 持有拖延压力分: {hold_pressure:.1f}/100\n"
        f"- 底价安全垫分: {floor_gap:.1f}/100（越高表示离底价越远）\n"
        f"- 回合压力分: {round_pressure:.1f}/100\n"
        f"- 市场偏向成交分: {market_bias:.1f}/100\n"
        f"- 总分(成交优先度): {close_priority:.1f}/100；建议: {suggestion}"
    )


def _resolve_batch_bidding_second_round_config(config=None) -> Dict[str, object]:
    """
    Optional second-round rebid for batch bidding.
    This only extends price-discovery rounds; final bid is still produced by LLM.
    """
    def _to_bool(raw, default: bool) -> bool:
        if raw is None:
            return bool(default)
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}

    def _to_int(raw, default: int) -> int:
        try:
            return int(raw)
        except Exception:
            return int(default)

    enabled = True
    min_candidates = 2
    top_n = 3
    max_rounds = 3
    min_increment_ratio = 0.005
    min_increment_abs = 1000.0
    top_n_decay = 1
    rounds_equal_candidate_count_enabled = True
    min_rounds_competition = 4
    rounds_cap = 12
    max_competition_pool = 12
    if config:
        enabled = _to_bool(config.get("smart_agent.batch_bidding_second_round_enabled", True), True)
        min_candidates = _to_int(config.get("smart_agent.batch_bidding_second_round_min_candidates", 2), 2)
        top_n = _to_int(config.get("smart_agent.batch_bidding_second_round_top_n", 3), 3)
        max_rounds = _to_int(config.get("smart_agent.batch_bidding_rebid_max_rounds", 3), 3)
        try:
            min_increment_ratio = float(config.get("smart_agent.batch_bidding_rebid_min_increment_ratio", 0.005))
        except Exception:
            min_increment_ratio = 0.005
        try:
            min_increment_abs = float(config.get("smart_agent.batch_bidding_rebid_min_increment_abs", 1000.0))
        except Exception:
            min_increment_abs = 1000.0
        top_n_decay = _to_int(config.get("smart_agent.batch_bidding_rebid_top_n_decay", 1), 1)
        rounds_equal_candidate_count_enabled = _to_bool(
            config.get("smart_agent.batch_bidding_rounds_equal_candidate_count_enabled", True), True
        )
        min_rounds_competition = _to_int(
            config.get("smart_agent.batch_bidding_min_rounds_on_competition", 4), 4
        )
        rounds_cap = _to_int(config.get("smart_agent.batch_bidding_rounds_cap", 12), 12)
        max_competition_pool = _to_int(config.get("smart_agent.batch_bidding_max_competition_pool", 12), 12)

    return {
        "enabled": bool(enabled),
        "min_candidates": max(2, min(10, int(min_candidates))),
        "top_n": max(2, min(8, int(top_n))),
        "max_rounds": max(2, min(20, int(max_rounds))),
        "min_increment_ratio": max(0.0, min(0.05, float(min_increment_ratio))),
        "min_increment_abs": max(0.0, float(min_increment_abs)),
        "top_n_decay": max(0, min(2, int(top_n_decay))),
        "rounds_equal_candidate_count_enabled": bool(rounds_equal_candidate_count_enabled),
        "min_rounds_competition": max(2, min(20, int(min_rounds_competition))),
        "rounds_cap": max(2, min(20, int(rounds_cap))),
        "max_competition_pool": max(2, min(20, int(max_competition_pool))),
    }

# --- New Negotiation Modes (Phase 5) ---


async def run_batch_bidding_async(seller: Agent, buyers: List[Agent], listing: Dict, market: Market, month: int, config=None, db_conn=None) -> Dict:
    """Mode A: Batch Bidding (Blind Auction) - Async"""
    # history = []
    min_price = listing['min_price']
    try:
        effective_bid_floor_ratio = float(
            config.get("smart_agent.effective_bid_floor_ratio", config.get("effective_bid_floor_ratio", 0.98))
        ) if config else 0.98
    except Exception:
        effective_bid_floor_ratio = 0.98
    effective_bid_floor_ratio = max(0.5, min(1.2, effective_bid_floor_ratio))
    effective_bid_floor = float(min_price) * effective_bid_floor_ratio

    # 1. Buyers Submit Bids (Parallel)
    async def get_buyer_bid(buyer):
        # ✅ Phase 3.1: Calculate real affordability
        max_affordable = calculate_max_affordable_price(buyer, config)

        # ✅ Phase 5.1: Fix Price Logic - Add Context
        valuation = listing.get('initial_value', listing['listed_price'])
        style = buyer.story.investment_style

        prompt = f"""
        你是买家 {buyer.id}。参与房产盲拍（Batch Bidding）。
        房产: {listing['zone']}区 {listing.get('building_area')}㎡
        当前挂牌价: {listing['listed_price']:,.0f}
        **市场估值**: ¥{valuation:,.0f} (参考基准)

        【你的画像】
        - 投资风格: {style} (决定你的溢价意愿)
        - 现金: ¥{buyer.cash:,.0f}
        - 月收入: ¥{buyer.monthly_income:,.0f}
        - **财务极限(Max Cap)**: ¥{max_affordable:,.0f}
        - 卖方底价: ¥{min_price:,.0f}
        - 有效出价下限: ¥{effective_bid_floor:,.0f}（低于此价视为无效）

        【决策逻辑】
        1. 不要无脑出财务极限价！这会让你成为"接盘侠"。
        2. 参考估值和挂牌价，结合你的风格出价：
           - Conservative (保守): 低于或略高于估值 (+0~5%)
           - Balanced (平衡): 适度溢价以确保拿下 (+5~10%)
           - Aggressive (激进): 为拿下心仪房源可大幅溢价 (+10~20%)，但绝不能超过财务极限。

        ⚠️ 硬性约束：出价必须 < ¥{max_affordable:,.0f}。

        请出价（0表示放弃）：
        输出JSON: {{"bid_price": float, "reason": "..."}}
        """
        resp = await safe_call_llm_async(
            prompt,
            {"bid_price": 0, "reason": "Pass"},
            model_type="fast",
        )
        raw_bid_price = float(resp.get("bid_price", 0) or 0.0)
        bid_price = normalize_llm_price_scale(
            raw_bid_price,
            float(listing["listed_price"]),
            float(max_affordable),
        )

        # ✅ Phase 3.1: Validate affordability post-bid
        original_bid = bid_price
        clamp_triggered = False
        if bid_price > 0:
            before = bid_price
            bid_price = clamp_offer_price(
                bid_price,
                float(listing['listed_price']),
                float(max_affordable),
                config=config,
            )
            clamp_triggered = abs(before - bid_price) > 1e-6
        is_valid = True
        if bid_price > 0:
            is_affordable, _, _ = check_affordability(buyer, bid_price, config)
            if not is_affordable:
                logger.warning(
                    f"🚫 买家{buyer.id}出价¥{bid_price:,.0f}超出负担能力"
                    f"（最大可负担¥{max_affordable:,.0f}），标记为无效"
                )
                bid_price = 0  # Mark as invalid bid
                is_valid = False
            elif bid_price < effective_bid_floor:
                logger.info(
                    f"🚫 买家{buyer.id}出价¥{bid_price:,.0f}低于有效下限¥{effective_bid_floor:,.0f}，标记为无效"
                )
                bid_price = 0
                is_valid = False

        return {
            "buyer": buyer,
            "price": bid_price,
            "original_bid": original_bid,
            "is_valid": is_valid,
            "reason": resp.get("reason"),
            "m16_clamp": clamp_triggered,
        }

    tasks = [get_buyer_bid(b) for b in buyers]
    results = await asyncio.gather(*tasks)
    history: List[Dict] = []

    for bid_result in results:
        buyer_obj = bid_result["buyer"]
        final_price = float(bid_result.get("price", 0.0) or 0.0)
        raw_price = float(bid_result.get("original_bid", 0.0) or 0.0)
        if bid_result.get("m16_clamp"):
            history.append(
                {
                    "round": 1,
                    "party": "system",
                    "agent_id": int(getattr(buyer_obj, "id", -1)),
                    "action": "M16_CLAMP",
                    "price": final_price,
                    "raw_bid": raw_price,
                    "content": f"offer clamped from {raw_price:,.0f} to {final_price:,.0f}",
                    "llm_called": 0,
                }
            )
        if final_price > 0:
            action = "BID"
            shown_price = final_price
        elif raw_price > 0:
            action = "INVALID_BID"
            shown_price = raw_price
        else:
            action = "WITHDRAW"
            shown_price = 0.0
        history.append(
            {
                "round": 1,
                "party": "buyer",
                "agent_id": int(getattr(buyer_obj, "id", -1)),
                "action": action,
                "price": float(shown_price),
                "content": str(bid_result.get("reason", "")),
                "llm_called": 1,
            }
        )

    # ✅ Phase 3.3: Record all bids to property_buyer_matches table
    if db_conn:
        cursor = db_conn.cursor()
        for bid_result in results:
            try:
                existing_match = cursor.execute(
                    """
                    SELECT match_id
                    FROM property_buyer_matches
                    WHERE month=? AND property_id=? AND buyer_id=?
                    ORDER BY match_id DESC
                    LIMIT 1
                    """,
                    (
                        int(month),
                        int(listing["property_id"]),
                        int(bid_result["buyer"].id),
                    ),
                ).fetchone()
                if existing_match and existing_match[0] is not None:
                    cursor.execute(
                        """
                        UPDATE property_buyer_matches
                        SET listing_price=CASE
                                WHEN listing_price IS NULL OR listing_price=0 THEN ?
                                ELSE listing_price
                            END,
                            buyer_bid=?,
                            is_valid_bid=MAX(COALESCE(is_valid_bid, 0), ?),
                            proceeded_to_negotiation=MAX(COALESCE(proceeded_to_negotiation, 0), ?)
                        WHERE match_id=?
                        """,
                        (
                            float(listing["listed_price"]),
                            float(bid_result.get("original_bid", 0.0) or 0.0),
                            1 if bid_result["is_valid"] else 0,
                            1 if bid_result["price"] > 0 else 0,
                            int(existing_match[0]),
                        ),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO property_buyer_matches
                        (month, property_id, buyer_id, listing_price, buyer_bid, is_valid_bid, proceeded_to_negotiation)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            month,
                            listing["property_id"],
                            bid_result["buyer"].id,
                            listing["listed_price"],
                            bid_result["original_bid"],
                            1 if bid_result["is_valid"] else 0,
                            1 if bid_result["price"] > 0 else 0,
                        ),
                    )
            except Exception as e:
                logger.error(f"Failed to record bid for buyer {bid_result['buyer'].id}: {e}")
        db_conn.commit()

    # ✅ Phase 3.1: Only filter out zero bids (affordability already checked)
    bids = [r for r in results if r['price'] > 0]

    # 2. Seller Selects Winner
    if not bids:
        soft_interest_count = sum(1 for r in results if float(r.get("original_bid", 0.0) or 0.0) > 0.0)
        return {
            "outcome": "failed",
            "reason": "No valid bids",
            "history": history,
            "fallback_to_classic": bool(soft_interest_count > 0),
        }

    # Multi-round rebid for stronger auction-like price discovery.
    # LLM still produces every buyer bid; code only controls rounds and validity.
    second_round_cfg = _resolve_batch_bidding_second_round_config(config)
    try:
        gate_keep_pool = bool(config.get("smart_agent.gate_force_multi_buyer_pool_enabled", False)) if config else False
    except Exception:
        gate_keep_pool = False
    if second_round_cfg["enabled"] and len(bids) >= int(second_round_cfg["min_candidates"]):
        current_top_n = int(second_round_cfg["top_n"])
        max_rounds = int(second_round_cfg["max_rounds"])
        min_increment_ratio = float(second_round_cfg["min_increment_ratio"])
        top_n_decay = int(second_round_cfg["top_n_decay"])
        # Hard upgrade for high competition:
        # pool size and rounds both scale with buyer count (capped), so we do
        # real multi-round elimination instead of one-shot finishing.
        if bool(second_round_cfg.get("rounds_equal_candidate_count_enabled", True)):
            bids = sorted(bids, key=lambda x: float(x.get("price", 0.0) or 0.0), reverse=True)
            competition_pool = min(len(bids), int(second_round_cfg.get("max_competition_pool", 12)))
            bids = bids[:competition_pool]
            current_top_n = max(2, competition_pool)
            top_n_decay = 0
            max_rounds = max(
                int(max_rounds),
                int(competition_pool),
                int(second_round_cfg.get("min_rounds_competition", 4)),
            )
            max_rounds = min(max_rounds, int(second_round_cfg.get("rounds_cap", 12)))
        for round_no in range(2, max_rounds + 1):
            bids = sorted(bids, key=lambda x: float(x.get("price", 0.0) or 0.0), reverse=True)
            if len(bids) < 2:
                break
            current_top_n = max(2, min(len(bids), current_top_n))
            finalists = bids[:current_top_n]
            leading_price = float(finalists[0]["price"])
            second_price = float(finalists[1]["price"])
            min_raise = max(float(second_round_cfg.get("min_increment_abs", 1000.0)), leading_price * min_increment_ratio)
            gap = max(0.0, leading_price - second_price)
            if (not gate_keep_pool) and gap <= min_raise and round_no > 2:
                history.append(
                    {
                        "round": int(round_no),
                        "party": "system",
                        "agent_id": int(getattr(seller, "id", -1)),
                        "action": "AUCTION_STOP_MIN_GAP",
                        "price": float(leading_price),
                        "content": f"领先价差{gap:,.0f} <= 最小加价{min_raise:,.0f}",
                        "llm_called": 0,
                    }
                )
                break

            async def get_final_bid(entry: Dict):
                buyer = entry["buyer"]
                prev_bid = float(entry.get("price", 0.0) or 0.0)
                max_affordable = calculate_max_affordable_price(buyer, config)
                prompt = f"""
                你是买家 {buyer.id}。盲拍进入第{round_no}轮加价（多轮竞价）。
                房产: {listing['zone']}区 {listing.get('building_area')}㎡
                当前领先报价: ¥{leading_price:,.0f}
                当前第二报价: ¥{second_price:,.0f}
                你上一轮有效报价: ¥{prev_bid:,.0f}
                你的财务极限: ¥{max_affordable:,.0f}
                最小有效加价幅度: ¥{min_raise:,.0f}
                有效出价下限: ¥{effective_bid_floor:,.0f}

                规则：
                - 可退出（出价0）。
                - 若继续，建议报价 >= max(上一轮报价, 当前领先价+最小加价)。
                - 不得超过财务极限。

                输出JSON: {{"bid_price": float, "reason": "..."}}
                """
                resp = await safe_call_llm_async(
                    prompt,
                    {"bid_price": 0, "reason": "Pass"},
                    model_type="fast",
                )
                raw_bid = normalize_llm_price_scale(
                    float(resp.get("bid_price", 0) or 0.0),
                    float(leading_price),
                    float(max_affordable),
                )
                if raw_bid <= 0:
                    if gate_keep_pool:
                        return {
                            "buyer": buyer,
                            "withdraw": False,
                            "prev_bid": prev_bid,
                            "final_bid": float(prev_bid),
                            "reason": str(resp.get("reason", "")),
                        }
                    return {
                        "buyer": buyer,
                        "withdraw": True,
                        "prev_bid": prev_bid,
                        "final_bid": 0.0,
                        "reason": str(resp.get("reason", "")),
                    }
                clamped = clamp_offer_price(raw_bid, float(listing["listed_price"]), float(max_affordable), config=config)
                target_floor = max(prev_bid, leading_price + min_raise)
                final_bid = max(float(clamped), float(target_floor))
                is_affordable, _, _ = check_affordability(buyer, final_bid, config)
                valid = bool(is_affordable and final_bid >= effective_bid_floor)
                if gate_keep_pool and not valid:
                    valid = True
                    final_bid = float(prev_bid)
                return {
                    "buyer": buyer,
                    "withdraw": (not valid),
                    "prev_bid": prev_bid,
                    "final_bid": float(final_bid if valid else 0.0),
                    "reason": str(resp.get("reason", "")),
                }

            final_results = await asyncio.gather(*[get_final_bid(e) for e in finalists])
            rebid_map = {int(r["buyer"].id): r for r in final_results}
            updated_bids = []
            for bid in bids:
                bid_buyer_id = int(bid["buyer"].id)
                rebid = rebid_map.get(bid_buyer_id)
                if rebid is None:
                    updated_bids.append(bid)
                    continue
                if rebid.get("withdraw"):
                    history.append(
                        {
                            "round": int(round_no),
                            "party": "buyer",
                            "agent_id": int(bid["buyer"].id),
                            "action": "FINAL_WITHDRAW",
                            "price": 0.0,
                            "content": rebid.get("reason", ""),
                            "llm_called": 1,
                        }
                    )
                    continue
                new_price = float(rebid.get("final_bid", bid["price"]) or bid["price"])
                history.append(
                    {
                        "round": int(round_no),
                        "party": "buyer",
                        "agent_id": int(bid["buyer"].id),
                        "action": "FINAL_BID",
                        "price": new_price,
                        "content": rebid.get("reason", ""),
                        "llm_called": 1,
                    }
                )
                bid["price"] = new_price
                updated_bids.append(bid)
            bids = updated_bids
            if not bids:
                return {"outcome": "failed", "reason": "No valid bids after final rebid", "history": history}
            current_top_n = max(2, current_top_n - top_n_decay)

    extra_rebid_history = []
    bids, extra_rebid_history = await _run_batch_tie_break_rebid_async(bids, seller, listing, config)
    if extra_rebid_history:
        history.extend(extra_rebid_history)

    # Sort by price desc
    best_bid, tie_break_history = await _resolve_batch_bid_winner_async(bids, seller, listing, config)
    if tie_break_history:
        history.extend(tie_break_history)

    # Seller Final Decision (Auto-accept if > min_price + simple logic, or ask LLM?)
    # For speed, if highest bid > min_price, accept.
    if best_bid['price'] >= min_price:
        history.append({
            "round": 1,
            "party": "seller",
            "agent_id": int(getattr(seller, "id", -1)),
            "action": "WIN_BID",
            "price": best_bid['price'],
            "buyer": best_bid['buyer'].id,
            "buyer_id": int(getattr(best_bid['buyer'], "id", -1)),
            "llm_called": 0,
        })
        if best_bid.get("m16_clamp"):
            history.insert(0, {
                "round": 1,
                "party": "system",
                "agent_id": int(getattr(best_bid['buyer'], "id", -1)),
                "action": "M16_CLAMP",
                "raw_bid": best_bid.get("original_bid"),
                "clamped_bid": best_bid['price'],
                "llm_called": 0,
            })
        return {
            "outcome": "success",
            "buyer_id": best_bid['buyer'].id,
            "final_price": best_bid['price'],
            "mode": "batch_bidding",
            "history": history
        }
    else:
        return {"outcome": "failed", "reason": "Highest bid below min_price"}


def run_batch_bidding(seller: Agent, buyers: List[Agent], listing: Dict, market: Market, config=None) -> Dict:
    """Mode A: Batch Bidding (Blind Auction)"""
    # history = []
    min_price = listing['min_price']
    try:
        effective_bid_floor_ratio = float(
            config.get("smart_agent.effective_bid_floor_ratio", config.get("effective_bid_floor_ratio", 0.98))
        ) if config else 0.98
    except Exception:
        effective_bid_floor_ratio = 0.98
    effective_bid_floor_ratio = max(0.5, min(1.2, effective_bid_floor_ratio))
    effective_bid_floor = float(min_price) * effective_bid_floor_ratio

    # 1. Buyers Submit Bids
    bids = []
    for buyer in buyers:
        max_budget = buyer.preference.max_price
        prompt = f"""
        你是买家 {buyer.id}。参与房产盲拍（Batch Bidding）。
        房产: {listing['zone']}区 {listing.get('building_area')}㎡
        你的预算: {max_budget}
        当前挂牌价: {listing['listed_price']}
        卖方底价: {min_price:,.0f}
        有效出价下限: {effective_bid_floor:,.0f}（低于此价视为无效）

        这是盲拍，只有一次出价机会。价高者得（需高于底价）。

        请出价（0表示放弃）：
        输出JSON: {{"bid_price": float, "reason": "..."}}
        """
        resp = safe_call_llm(
            prompt,
            {"bid_price": 0, "reason": "Pass"},
            model_type="fast",
        )
        raw_bid_price = float(resp.get("bid_price", 0) or 0.0)
        bid_price = normalize_llm_price_scale(
            raw_bid_price,
            float(listing["listed_price"]),
            float(max_budget),
        )

        clamp_triggered = False
        if bid_price > 0:
            before = bid_price
            bid_price = clamp_offer_price(
                bid_price,
                float(listing['listed_price']),
                float(max_budget),
                config=config,
            )
            clamp_triggered = abs(before - bid_price) > 1e-6
        if bid_price > 0 and bid_price <= max_budget and bid_price >= effective_bid_floor:
            bids.append({
                "buyer": buyer,
                "price": bid_price,
                "reason": resp.get("reason"),
                "m16_clamp": clamp_triggered,
                "raw_bid": before if bid_price > 0 else bid_price,
            })

    # 2. Seller Selects Winner
    if not bids:
        return {"outcome": "failed", "reason": "No valid bids"}

    history: List[Dict] = []
    second_round_cfg = _resolve_batch_bidding_second_round_config(config)
    try:
        gate_keep_pool = bool(config.get("smart_agent.gate_force_multi_buyer_pool_enabled", False)) if config else False
    except Exception:
        gate_keep_pool = False
    if second_round_cfg["enabled"] and len(bids) >= int(second_round_cfg["min_candidates"]):
        current_top_n = int(second_round_cfg["top_n"])
        max_rounds = int(second_round_cfg["max_rounds"])
        min_increment_ratio = float(second_round_cfg["min_increment_ratio"])
        top_n_decay = int(second_round_cfg["top_n_decay"])
        # Hard upgrade for high competition:
        # pool size and rounds both scale with buyer count (capped), so we do
        # real multi-round elimination instead of one-shot finishing.
        if bool(second_round_cfg.get("rounds_equal_candidate_count_enabled", True)):
            bids = sorted(bids, key=lambda x: float(x.get("price", 0.0) or 0.0), reverse=True)
            competition_pool = min(len(bids), int(second_round_cfg.get("max_competition_pool", 12)))
            bids = bids[:competition_pool]
            current_top_n = max(2, competition_pool)
            top_n_decay = 0
            max_rounds = max(
                int(max_rounds),
                int(competition_pool),
                int(second_round_cfg.get("min_rounds_competition", 4)),
            )
            max_rounds = min(max_rounds, int(second_round_cfg.get("rounds_cap", 12)))
        for round_no in range(2, max_rounds + 1):
            bids = sorted(bids, key=lambda x: float(x.get("price", 0.0) or 0.0), reverse=True)
            if len(bids) < 2:
                break
            current_top_n = max(2, min(len(bids), current_top_n))
            finalists = bids[: current_top_n]
            leading_price = float(finalists[0]["price"])
            second_price = float(finalists[1]["price"])
            min_raise = max(float(second_round_cfg.get("min_increment_abs", 1000.0)), leading_price * min_increment_ratio)
            gap = max(0.0, leading_price - second_price)
            if (not gate_keep_pool) and gap <= min_raise and round_no > 2:
                history.append(
                    {
                        "round": int(round_no),
                        "party": "system",
                        "agent_id": int(getattr(seller, "id", -1)),
                        "action": "AUCTION_STOP_MIN_GAP",
                        "price": float(leading_price),
                        "content": f"领先价差{gap:,.0f} <= 最小加价{min_raise:,.0f}",
                        "llm_called": 0,
                    }
                )
                break

            rebid_map: Dict[int, Dict] = {}
            for entry in finalists:
                buyer = entry["buyer"]
                prev_bid = float(entry.get("price", 0.0) or 0.0)
                max_budget = float(getattr(getattr(buyer, "preference", None), "max_price", 0.0) or 0.0)
                prompt = f"""
                你是买家 {buyer.id}。盲拍进入第{round_no}轮加价（多轮竞价）。
                房产: {listing['zone']}区 {listing.get('building_area')}㎡
                当前领先报价: ¥{leading_price:,.0f}
                当前第二报价: ¥{second_price:,.0f}
                你上一轮有效报价: ¥{prev_bid:,.0f}
                你的预算上限: ¥{max_budget:,.0f}
                最小有效加价幅度: ¥{min_raise:,.0f}
                有效出价下限: ¥{effective_bid_floor:,.0f}

                规则：
                - 可以退出（出价0）。
                - 若继续，建议报价 >= max(上一轮报价, 当前领先价+最小加价)。
                - 不得超过预算上限。

                输出JSON: {{"bid_price": float, "reason": "..."}}
                """
                resp = safe_call_llm(prompt, {"bid_price": 0, "reason": "Pass"}, model_type="fast")
                raw_bid = normalize_llm_price_scale(
                    float(resp.get("bid_price", 0) or 0.0),
                    float(leading_price),
                    float(max_budget),
                )
                if raw_bid <= 0:
                    if gate_keep_pool:
                        rebid_map[int(buyer.id)] = {
                            "withdraw": False,
                            "final_bid": float(prev_bid),
                            "reason": str(resp.get("reason", "")),
                        }
                    else:
                        rebid_map[int(buyer.id)] = {"withdraw": True, "reason": str(resp.get("reason", ""))}
                    continue
                clamped = clamp_offer_price(raw_bid, float(listing["listed_price"]), float(max_budget), config=config)
                target_floor = max(prev_bid, leading_price + min_raise)
                final_bid = max(float(clamped), float(target_floor))
                valid = bool(final_bid <= max_budget and final_bid >= effective_bid_floor)
                if not valid:
                    if gate_keep_pool:
                        rebid_map[int(buyer.id)] = {
                            "withdraw": False,
                            "final_bid": float(prev_bid),
                            "reason": str(resp.get("reason", "")),
                        }
                    else:
                        rebid_map[int(buyer.id)] = {"withdraw": True, "reason": str(resp.get("reason", ""))}
                    continue
                rebid_map[int(buyer.id)] = {"withdraw": False, "final_bid": final_bid, "reason": str(resp.get("reason", ""))}

            updated_bids = []
            for bid in bids:
                bid_buyer_id = int(bid["buyer"].id)
                rebid = rebid_map.get(bid_buyer_id)
                if rebid is None:
                    updated_bids.append(bid)
                    continue
                if rebid.get("withdraw"):
                    history.append(
                        {
                            "round": int(round_no),
                            "party": "buyer",
                            "agent_id": int(bid["buyer"].id),
                            "action": "FINAL_WITHDRAW",
                            "price": 0.0,
                            "content": rebid.get("reason", ""),
                            "llm_called": 1,
                        }
                    )
                    continue
                new_price = float(rebid.get("final_bid", bid["price"]) or bid["price"])
                history.append(
                    {
                        "round": int(round_no),
                        "party": "buyer",
                        "agent_id": int(bid["buyer"].id),
                        "action": "FINAL_BID",
                        "price": new_price,
                        "content": rebid.get("reason", ""),
                        "llm_called": 1,
                    }
                )
                bid["price"] = new_price
                updated_bids.append(bid)
            bids = updated_bids
            if not bids:
                return {"outcome": "failed", "reason": "No valid bids after final rebid", "history": history}
            current_top_n = max(2, current_top_n - top_n_decay)

    # Sort by price desc
    extra_rebid_history = []
    bids, extra_rebid_history = _run_batch_tie_break_rebid(bids, seller, listing, config)
    if extra_rebid_history:
        history.extend(extra_rebid_history)

    best_bid, tie_break_history = _resolve_batch_bid_winner(bids, seller, listing, config)
    if tie_break_history:
        history.extend(tie_break_history)

    if best_bid['price'] >= min_price:
        history.append({
            "round": 1,
            "party": "seller",
            "agent_id": int(getattr(seller, "id", -1)),
            "action": "WIN_BID",
            "price": best_bid['price'],
            "buyer": best_bid['buyer'].id,
            "buyer_id": int(getattr(best_bid['buyer'], "id", -1)),
            "llm_called": 0,
        })
        if best_bid.get("m16_clamp"):
            history.insert(0, {
                "round": 1,
                "party": "system",
                "agent_id": int(getattr(best_bid['buyer'], "id", -1)),
                "action": "M16_CLAMP",
                "raw_bid": best_bid.get("raw_bid"),
                "clamped_bid": best_bid['price'],
                "llm_called": 0,
            })
        return {
            "outcome": "success",
            "buyer_id": best_bid['buyer'].id,
            "final_price": best_bid['price'],
            "mode": "batch_bidding",
            "history": history
        }
    else:
        return {"outcome": "failed", "reason": "Highest bid below min_price"}


async def run_flash_deal_async(seller: Agent, buyer: Agent, listing: Dict, market: Market) -> Dict:
    """Mode B: Flash Deal (Take it or Leave it) - Async"""
    # 1. Seller sets Flash Price (usually discounted)
    flash_price = listing['listed_price'] * 0.95  # Auto-discount for speed
    if flash_price < listing['min_price']:
        flash_price = listing['min_price']

    # 2. Buyer Decision
    prompt = f"""
    你是买家 {buyer.id}。卖家发起闪电成交（Flash Deal）。
    一口价: {flash_price:,.0f} (原价 {listing['listed_price']:,.0f})

    必须马上决定：接受(ACCEPT) 或 拒绝(REJECT)。
    输出JSON: {{"action": "ACCEPT"|"REJECT", "reason": "..."}}
    """
    resp = await safe_call_llm_async(
        prompt,
        {"action": "REJECT", "reason": "Pass"},
        model_type="fast",
    )
    action = resp.get("action", "REJECT").upper()

    if action == "ACCEPT" and flash_price <= buyer.preference.max_price:
        return {
            "outcome": "success",
            "buyer_id": buyer.id,
            "final_price": flash_price,
            "mode": "flash_deal",
            "history": [
                {
                    "round": 1,
                    "party": "buyer",
                    "agent_id": int(getattr(buyer, "id", -1)),
                    "action": "FLASH_ACCEPT",
                    "price": flash_price,
                }
            ]
        }
    return {"outcome": "failed", "reason": "Buyer rejected flash deal"}


def run_flash_deal(seller: Agent, buyer: Agent, listing: Dict, market: Market) -> Dict:
    """Mode B: Flash Deal (Take it or Leave it)"""
    # 1. Seller sets Flash Price (usually discounted)
    flash_price = listing['listed_price'] * 0.95  # Auto-discount for speed
    if flash_price < listing['min_price']:
        flash_price = listing['min_price']

    # 2. Buyer Decision
    prompt = f"""
    你是买家 {buyer.id}。卖家发起闪电成交（Flash Deal）。
    一口价: {flash_price:,.0f} (原价 {listing['listed_price']:,.0f})

    必须马上决定：接受(ACCEPT) 或 拒绝(REJECT)。
    输出JSON: {{"action": "ACCEPT"|"REJECT", "reason": "..."}}
    """
    resp = safe_call_llm(
        prompt,
        {"action": "REJECT", "reason": "Pass"},
        model_type="fast",
    )
    action = resp.get("action", "REJECT").upper()

    if action == "ACCEPT" and flash_price <= buyer.preference.max_price:
        return {
            "outcome": "success",
            "buyer_id": buyer.id,
            "final_price": flash_price,
            "mode": "flash_deal",
            "history": [
                {
                    "round": 1,
                    "party": "buyer",
                    "agent_id": int(getattr(buyer, "id", -1)),
                    "action": "FLASH_ACCEPT",
                    "price": flash_price,
                }
            ]
        }
    return {"outcome": "failed", "reason": "Buyer rejected flash deal"}


def run_negotiation_session(
    seller: Agent,
    buyers: List[Agent],
    listing: Dict,
    market: Market,
    config=None,
    month: int = 1,
) -> Dict:
    """Main Entry Point for Negotiation Phase"""
    if not buyers:
        return {"outcome": "failed", "reason": "No valid buyers"}

    # 1. Seller Decides Mode
    market_hint = "买家众多" if len(buyers) > 1 else "单一买家"
    mode = decide_negotiation_format(seller, buyers, market_hint)

    # 2. Dispatch
    if mode == "BATCH":
        return run_batch_bidding(seller, buyers, listing, market, config)

    elif mode == "FLASH":
        # Pick one buyer to offer flash deal (e.g. first one)
        target_buyer = buyers[0]
        return run_flash_deal(seller, target_buyer, listing, market)

    else:  # CLASSIC
        # Iterate buyers until one succeeds or all fail
        for buyer in buyers:
            result = negotiate(buyer, seller, listing, market, len(buyers), config, month=month)
            if result['outcome'] == 'success':
                result['buyer_id'] = buyer.id
                result['mode'] = 'classic'
                return result

    return {"outcome": "failed", "reason": "All negotiations failed"}


async def run_negotiation_session_async(seller: Agent, buyers: List[Agent], listing: Dict, market: Market, month: int, config=None, db_conn=None) -> Dict:
    """Async Main Entry Point for Negotiation Phase"""
    if not buyers:
        return {"outcome": "failed", "reason": "No valid buyers"}

    market_hint = "买家众多" if len(buyers) > 1 else "单一买家"
    mode = decide_negotiation_format(seller, buyers, market_hint)
    route_info = _resolve_negotiation_route(seller=seller, buyers=buyers, listing=listing, config=config)
    try:
        classic_competitive_enabled = bool(
            config.get("smart_agent.classic_competitive_enabled", False)
        ) if config else False
    except Exception:
        classic_competitive_enabled = False
    try:
        # Gating helper: when enabled, force multi-buyer sessions through CLASSIC path
        # so we can observe competitive multi-round behavior instead of one-shot batch bids.
        classic_competitive_force_mode = bool(
            config.get("smart_agent.classic_competitive_force_mode", False)
        ) if config else False
    except Exception:
        classic_competitive_force_mode = False

    # Optional gate/testing override: force multi-buyer sessions to BATCH mode.
    # Default is off, and this does not replace LLM decision in normal runs.
    try:
        force_batch_mode = bool(config.get("smart_agent.negotiation_force_batch_mode", False)) if config else False
    except Exception:
        force_batch_mode = False
    if force_batch_mode and len(buyers) > 1:
        mode = "BATCH"
    elif classic_competitive_enabled and classic_competitive_force_mode and len(buyers) > 1:
        mode = "CLASSIC"
    else:
        hot_listing_auto_mode = _resolve_hot_listing_auto_bidding_mode(listing, len(buyers), config)
        if hot_listing_auto_mode and len(buyers) > 1:
            if hot_listing_auto_mode == "CLASSIC" and not classic_competitive_enabled:
                mode = "BATCH"
                route_info = dict(route_info)
                route_info["reason"] = (
                    f"{route_info.get('reason', '')}; hot_listing_auto_bidding=CLASSIC->BATCH"
                ).strip("; ")
            else:
                mode = str(hot_listing_auto_mode)
                route_info = dict(route_info)
                route_info["reason"] = (
                    f"{route_info.get('reason', '')}; hot_listing_auto_bidding={hot_listing_auto_mode}"
                ).strip("; ")

    # Simple Async Implementation: Support Classic Mode primarily for now
    # (Batch and Flash can be added later or reuse sync logic if no LLM calls inside those specific functions yet,
    # but run_batch_bidding DOES use LLM, so they should be async too. For urgency, we map everything to classic async or implement others)

    consolidated_log = []

    if mode == "BATCH":
        route_marker = {
            "round": 0,
            "party": "router",
            "agent_id": int(getattr(seller, "id", -1)),
            "action": "MODEL_ROUTE",
            "model": route_info["model"],
            "gray_score": float(route_info["gray_score"]),
            "content": route_info["reason"],
        }
        # ✅ Phase 3.3: Pass db_conn to record bids
        result = await run_batch_bidding_async(seller, buyers, listing, market, month, config, db_conn)
        if (
            result.get("outcome") == "failed"
            and str(result.get("reason", "") or "") == "No valid bids"
            and bool(result.get("fallback_to_classic", False))
        ):
            fallback_marker = {
                "round": 1,
                "party": "system",
                "agent_id": int(getattr(seller, "id", -1)),
                "action": "BATCH_FALLBACK_CLASSIC",
                "content": "Positive buyer interest existed but all batch bids were invalid; falling back to classic negotiation.",
                "llm_called": 0,
            }
            batch_history = result.get("history", [])
            if not isinstance(batch_history, list):
                batch_history = []

            if classic_competitive_enabled and len(buyers) > 1:
                fallback_result = await _run_classic_competitive_async(
                    seller=seller,
                    buyers=buyers,
                    listing=listing,
                    market=market,
                    month=month,
                    config=config,
                    llm_model_type=route_info["model"],
                )
            else:
                fallback_log = []
                fallback_result = None
                for buyer in buyers:
                    classic_result = await negotiate_async(
                        buyer,
                        seller,
                        listing,
                        market,
                        len(buyers),
                        config,
                        month=month,
                        llm_model_type=route_info["model"],
                    )
                    fallback_log.extend(classic_result.get("history", []))
                    if classic_result.get("outcome") == "success":
                        fallback_result = dict(classic_result)
                        fallback_result["buyer_id"] = buyer.id
                        break
                if fallback_result is None:
                    fallback_result = {
                        "outcome": "failed",
                        "reason": "All negotiations failed",
                        "history": fallback_log,
                        "final_price": 0,
                    }
                else:
                    fallback_result["history"] = fallback_log

            fallback_history = fallback_result.get("history", [])
            if not isinstance(fallback_history, list):
                fallback_history = []
            fallback_result["history"] = [route_marker, fallback_marker] + batch_history + fallback_history
            fallback_result["negotiation_route_model"] = route_info["model"]
            fallback_result["negotiation_route_reason"] = route_info["reason"]
            fallback_result["negotiation_gray_score"] = float(route_info["gray_score"])
            fallback_result["mode"] = "classic"
            return fallback_result

        history = result.get("history", [])
        if isinstance(history, list):
            result["history"] = [route_marker] + history
        else:
            result["history"] = [route_marker]
        result["negotiation_route_model"] = route_info["model"]
        result["negotiation_route_reason"] = route_info["reason"]
        result["negotiation_gray_score"] = float(route_info["gray_score"])
        return result

    elif mode == "FLASH":
        # Pick one buyer to offer flash deal (e.g. first one or random)
        target_buyer = buyers[0]
        result = await run_flash_deal_async(seller, target_buyer, listing, market)
        route_marker = {
            "round": 0,
            "party": "router",
            "agent_id": int(getattr(seller, "id", -1)),
            "action": "MODEL_ROUTE",
            "model": route_info["model"],
            "gray_score": float(route_info["gray_score"]),
            "content": route_info["reason"],
        }
        history = result.get("history", [])
        if isinstance(history, list):
            result["history"] = [route_marker] + history
        else:
            result["history"] = [route_marker]
        result["negotiation_route_model"] = route_info["model"]
        result["negotiation_route_reason"] = route_info["reason"]
        result["negotiation_gray_score"] = float(route_info["gray_score"])
        return result

    elif mode == "CLASSIC":
        route_marker = {
            "round": 0,
            "party": "router",
            "agent_id": int(getattr(seller, "id", -1)),
            "action": "MODEL_ROUTE",
            "model": route_info["model"],
            "gray_score": float(route_info["gray_score"]),
            "content": route_info["reason"],
        }
        consolidated_log.append(route_marker)
        if classic_competitive_enabled and len(buyers) > 1:
            result = await _run_classic_competitive_async(
                seller=seller,
                buyers=buyers,
                listing=listing,
                market=market,
                month=month,
                config=config,
                llm_model_type=route_info["model"],
            )
            session_history = result.get("history", [])
            if isinstance(session_history, list):
                result["history"] = consolidated_log + session_history
            else:
                result["history"] = consolidated_log
            result["mode"] = "classic"
            result["negotiation_route_model"] = route_info["model"]
            result["negotiation_route_reason"] = route_info["reason"]
            result["negotiation_gray_score"] = float(route_info["gray_score"])
            return result

        for buyer in buyers:
            # Await the async negotiate
            result = await negotiate_async(
                buyer,
                seller,
                listing,
                market,
                len(buyers),
                config,
                month=month,
                llm_model_type=route_info["model"],
            )
            consolidated_log.extend(result.get('history', []))

            if result['outcome'] == 'success':
                result['buyer_id'] = buyer.id
                result['mode'] = 'classic'
                result['history'] = consolidated_log  # Preserve prior failed attempts log too
                result['negotiation_route_model'] = route_info["model"]
                result['negotiation_route_reason'] = route_info["reason"]
                result['negotiation_gray_score'] = float(route_info["gray_score"])
                return result
    else:
        # Fallback to sync for unimplemented modes or implement them
        # For this tier, let's just use Classic Async for all or fallback to sync wrapper
        # But to gain performance, we really want async.
        # Let's fallback to CLASSIC async for now to ensure coverage
        route_marker = {
            "round": 0,
            "party": "router",
            "agent_id": int(getattr(seller, "id", -1)),
            "action": "MODEL_ROUTE",
            "model": route_info["model"],
            "gray_score": float(route_info["gray_score"]),
            "content": route_info["reason"],
        }
        consolidated_log.append(route_marker)
        for buyer in buyers:
            result = await negotiate_async(
                buyer,
                seller,
                listing,
                market,
                len(buyers),
                config,
                llm_model_type=route_info["model"],
            )
            consolidated_log.extend(result.get('history', []))

            if result['outcome'] == 'success':
                result['buyer_id'] = buyer.id
                result['mode'] = 'classic'
                result['history'] = consolidated_log
                result['negotiation_route_model'] = route_info["model"]
                result['negotiation_route_reason'] = route_info["reason"]
                result['negotiation_gray_score'] = float(route_info["gray_score"])
                return result

    return {
        "outcome": "failed",
        "reason": "All negotiations failed",
        "history": consolidated_log,
        "negotiation_route_model": route_info["model"],
        "negotiation_route_reason": route_info["reason"],
        "negotiation_gray_score": float(route_info["gray_score"]),
    }

# --- 1. Seller Listing Logic ---


def generate_seller_listing(
    seller: Agent,
    property_data: Dict,
    market: Market,
    strategy_hint: str = "balanced",
    pricing_coefficient: float = None,
    config=None,
) -> Dict:
    """
    Generate seller listing.
    """

    def _normalize_prices(listed: float, minimum: float) -> Tuple[float, float]:
        listed = max(1.0, float(listed or 0.0))
        minimum = max(1.0, float(minimum or 0.0))
        if minimum > listed:
            minimum = listed
        return listed, minimum

    def _cfg_get(key: str, default):
        if config is None:
            return default
        try:
            return config.get(key, default)
        except Exception:
            return default

    def _resolve_seller_deadline(urgency_value: float) -> Tuple[int, float, int]:
        enabled = bool(_cfg_get("smart_agent.seller_deadline_enabled", False))
        if not enabled:
            return 0, float(max(0.0, min(1.0, urgency_value))), 0

        try:
            u = float(urgency_value)
        except Exception:
            u = 0.5
        u = float(max(0.0, min(1.0, u)))

        def _read_pair(key: str, fallback_lo: int, fallback_hi: int) -> Tuple[int, int]:
            raw = _cfg_get(key, [fallback_lo, fallback_hi])
            if isinstance(raw, (list, tuple)) and len(raw) >= 2:
                try:
                    lo = int(raw[0])
                    hi = int(raw[1])
                except Exception:
                    lo, hi = fallback_lo, fallback_hi
            else:
                lo, hi = fallback_lo, fallback_hi
            if hi < lo:
                hi = lo
            return max(1, lo), max(1, hi)

        urgent_lo, urgent_hi = _read_pair("smart_agent.seller_deadline_urgent_range", 1, 3)
        balanced_lo, balanced_hi = _read_pair("smart_agent.seller_deadline_balanced_range", 3, 6)
        patient_lo, patient_hi = _read_pair("smart_agent.seller_deadline_patient_range", 6, 12)

        # Combine explicit strategy hint and urgency score.
        normalized_hint = str(strategy_hint or "").strip().lower()
        if normalized_hint == "urgent" or u >= 0.72:
            lo, hi = urgent_lo, urgent_hi
        elif normalized_hint == "aggressive":
            lo, hi = balanced_lo, balanced_hi
        elif u <= 0.35:
            lo, hi = patient_lo, patient_hi
        else:
            lo, hi = balanced_lo, balanced_hi

        min_cap = int(max(1, _cfg_get("smart_agent.seller_deadline_min_months", 1)))
        max_cap = int(max(min_cap, _cfg_get("smart_agent.seller_deadline_max_months", 12)))
        lo = max(min_cap, min(max_cap, lo))
        hi = max(lo, min(max_cap, hi))
        horizon = random.randint(lo, hi)

        hard_clear = bool(_cfg_get("smart_agent.seller_deadline_hard_clear_enabled", True))
        return int(horizon), float(u), int(1 if hard_clear else 0)

    # Get Market Info
    zone = property_data.get('zone', 'A')  # Default to A if missing
    avg_price = market.get_avg_price(zone)
    if avg_price == 0:
        avg_price = property_data['base_value']

    base_val = property_data['base_value']

    # Tier 3: If coefficient provided, use it directly
    if pricing_coefficient is not None:
        # Apply coefficient based on strategy type
        if strategy_hint == 'aggressive':  # Strategy A: based on valuation
            listed_price = base_val * pricing_coefficient
            min_price = base_val * (pricing_coefficient - 0.05)  # 5% buffer
        elif strategy_hint == 'balanced':  # Strategy B: based on market price
            listed_price = avg_price * pricing_coefficient
            min_price = avg_price * (pricing_coefficient - 0.03)
        elif strategy_hint == 'urgent':  # Strategy C: based on valuation
            listed_price = base_val * pricing_coefficient
            min_price = base_val * (pricing_coefficient - 0.03)
        else:
            listed_price = base_val * pricing_coefficient
            min_price = base_val * 0.95

        listed_price, min_price = _normalize_prices(listed_price, min_price)
        deadline_months, urgency_score, forced_sale_mode = _resolve_seller_deadline(0.5)
        return {
            "property_id": property_data['property_id'],
            "seller_id": seller.id,
            "zone": zone,
            "listed_price": listed_price,
            "min_price": min_price,
            "urgency": 0.5,
            "sell_deadline_total_months": int(deadline_months),
            "sell_urgency_score": float(urgency_score),
            "forced_sale_mode": int(forced_sale_mode),
            "status": "active",
            "reasoning": f"Coefficient {pricing_coefficient:.2f} from LLM strategy"
        }

    # Legacy path: Call LLM if no coefficient (backward compatibility)
    prompt = f"""
    你准备卖房：
    【背景】{seller.story.background_story}
    【卖房动机】{seller.story.selling_motivation}
    【房产】{zone}区，{property_data.get('building_area', 100)}㎡
    【市场均价】{avg_price:,.0f}元
    【估值】{property_data['base_value']:,.0f}元

    【定价策略】{strategy_hint}
    (aggressive=尝试挂高价, balanced=随行就市, urgent=急售降价)

    设定挂牌价和可接受最低价：
    输出JSON：{{"listed_price":..., "min_price":..., "urgency": 0-1, "reasoning":"..."}}
    """

    # Default fallback logic based on strategy
    if strategy_hint == 'aggressive':
        def_list = base_val * 1.15
        def_min = base_val * 1.05
    elif strategy_hint == 'urgent':
        def_list = base_val * 0.95
        def_min = base_val * 0.90
    else:
        def_list = base_val * 1.1
        def_min = base_val * 0.95

    default_listing = {
        "listed_price": def_list,
        "min_price": def_min,
        "urgency": 0.5,
        "reasoning": f"Follow {strategy_hint} strategy"
    }

    result = safe_call_llm(prompt, default_listing, model_type="fast")

    # Ensure numerical validity
    try:
        listed_price = float(result.get("listed_price", default_listing["listed_price"]))
        min_price = float(result.get("min_price", default_listing["min_price"]))
    except BaseException:
        listed_price = default_listing["listed_price"]
        min_price = default_listing["min_price"]

    listed_price, min_price = _normalize_prices(listed_price, min_price)
    try:
        urgency_raw = float(result.get("urgency", 0.5))
    except Exception:
        urgency_raw = 0.5
    deadline_months, urgency_score, forced_sale_mode = _resolve_seller_deadline(urgency_raw)
    return {
        "property_id": property_data['property_id'],
        "seller_id": seller.id,
        "zone": zone,  # 添加zone字段，negotiate需要用它判断市场供需
        "listed_price": listed_price,
        "min_price": min_price,  # Ensure positive and never above listed
        "urgency": urgency_raw,
        "sell_deadline_total_months": int(deadline_months),
        "sell_urgency_score": float(urgency_score),
        "forced_sale_mode": int(forced_sale_mode),
        "status": "active",
        "reasoning": result.get("reasoning", "")
    }

# --- 2. Buyer Matching Logic ---


def get_buyer_monthly_buy_cap(buyer: Agent, config=None) -> int:
    """
    Resolve per-month buy cap.
    - normal agent: always 1
    - smart agent: configurable, default 3
    """
    if getattr(buyer, "agent_type", "normal") != "smart":
        return 1

    cap = 3
    if config:
        try:
            cap = int(config.get("smart_agent.max_buys_per_month", cap))
        except Exception:
            pass
        # Backward-compatible fallback key.
        try:
            cap = int(config.get("max_buys_per_month", cap))
        except Exception:
            pass
    return max(1, cap)


def _m16_guardrails(config=None) -> Dict[str, float]:
    """M16 anti-extreme behavior guardrails."""
    def _get(smart_key: str, fallback_key: str, default):
        if not config:
            return default
        try:
            return config.get(smart_key, config.get(fallback_key, default))
        except Exception:
            return default

    min_offer_ratio = float(_get("smart_agent.min_offer_ratio_to_list", "min_offer_ratio_to_list", 0.75))
    max_overbid_ratio = float(_get("smart_agent.max_overbid_ratio_to_list", "max_overbid_ratio_to_list", 0.15))
    max_rounds_cap = int(_get("smart_agent.max_negotiation_rounds_cap", "max_negotiation_rounds_cap", 4))
    return {
        "min_offer_ratio_to_list": max(0.50, min(0.98, min_offer_ratio)),
        "max_overbid_ratio_to_list": max(0.0, min(0.50, max_overbid_ratio)),
        "max_negotiation_rounds_cap": max(1, min(10, max_rounds_cap)),
    }


def clamp_offer_price(raw_offer: float, listed_price: float, buyer_max: float, config=None) -> float:
    g = _m16_guardrails(config)
    min_offer = listed_price * g["min_offer_ratio_to_list"]
    max_offer = min(float(buyer_max), listed_price * (1.0 + g["max_overbid_ratio_to_list"]))
    if max_offer < min_offer:
        return max_offer
    return max(min_offer, min(max_offer, float(raw_offer)))


def normalize_llm_price_scale(raw_price: float, reference_price: float, buyer_max: float) -> float:
    """
    Normalize obviously scale-truncated LLM prices (e.g. 698 -> 698000).
    This keeps the buyer's price intent while only repairing unit/scale parsing.
    """
    try:
        raw = float(raw_price or 0.0)
        reference = float(reference_price or 0.0)
        cap = float(buyer_max or 0.0)
    except Exception:
        return float(raw_price or 0.0)

    if raw <= 0 or reference < 100000:
        return raw
    if raw >= reference * 0.1:
        return raw

    plausible = []
    upper_bound = cap * 1.05 if cap > 0 else reference * 1.5
    lower_bound = reference * 0.5
    for scale in (1000.0, 10000.0):
        scaled = raw * scale
        if scaled < lower_bound or scaled > upper_bound:
            continue
        plausible.append((abs(scaled - reference), scaled))

    if not plausible:
        return raw
    plausible.sort(key=lambda item: item[0])
    return float(plausible[0][1])


def resolve_strategy_profile(decision_mode: str, risk_mode: str, market_trend: str) -> str:
    """
    M10 strategy library routing:
    - normal path: always 'normal_balanced'
    - smart path: choose strategy by market regime + risk mode
    """
    if decision_mode != "smart":
        return "normal_balanced"

    trend = str(market_trend or "STABLE").upper()
    risk = str(risk_mode or "balanced").lower()
    if "DOWN" in trend or "CRASH" in trend:
        return "smart_downturn_defensive" if risk != "aggressive" else "smart_downturn_opportunistic"
    if "UP" in trend or "BOOM" in trend:
        return "smart_uptrend_momentum" if risk == "aggressive" else "smart_uptrend_selective"
    if "VOLATILE" in trend or "PANIC" in trend:
        return "smart_volatile_barbell"
    return "smart_stable_balanced"


def _resolve_location_scarcity_score(config, zone: str) -> float:
    """
    Resolve location scarcity input score (0~1) for a zone.
    This is a static input factor for candidate ordering, not an outcome metric.
    """
    z = str(zone or "").upper() or "B"
    defaults = {
        "A": {
            "education_resource_density": 0.82,
            "job_accessibility": 0.88,
            "transport_accessibility": 0.85,
            "core_location_scarcity": 0.92,
        },
        "B": {
            "education_resource_density": 0.48,
            "job_accessibility": 0.42,
            "transport_accessibility": 0.50,
            "core_location_scarcity": 0.35,
        },
    }
    metric_weights = {
        "education_resource_density": 0.25,
        "job_accessibility": 0.30,
        "transport_accessibility": 0.20,
        "core_location_scarcity": 0.25,
    }

    zone_defaults = defaults.get(z, defaults["B"])
    zone_cfg = {}
    if config is not None and hasattr(config, "market"):
        try:
            zone_cfg = (
                getattr(config, "market", {})
                .get("zones", {})
                .get(z, {})
            ) or {}
        except Exception:
            zone_cfg = {}
    factors_cfg = zone_cfg.get("location_factors", {}) if isinstance(zone_cfg, dict) else {}

    score = 0.0
    for key, weight in metric_weights.items():
        raw = factors_cfg.get(key, zone_defaults.get(key, 0.5))
        try:
            val = float(raw)
        except Exception:
            val = float(zone_defaults.get(key, 0.5))
        score += max(0.0, min(1.0, val)) * float(weight)
    return max(0.0, min(1.0, float(score)))


def _strategy_score_candidate(
    listing: Dict,
    prop: Dict,
    pref,
    strategy_profile: str,
    bargain_discount_trigger: float = 0.12,
    config=None,
) -> float:
    listed_price = float(listing.get("listed_price", 0.0) or 0.0)
    ref_value = float(
        prop.get("current_valuation", 0.0)
        or prop.get("base_value", 0.0)
        or listed_price
        or 1.0
    )
    discount = (ref_value - listed_price) / max(ref_value, 1.0)
    listing_age_months = max(0.0, float(listing.get("listing_age_months", 0.0) or 0.0))

    edu_need = bool(getattr(pref, "need_school_district", False))
    school_match = 1.0 if bool(prop.get("is_school_district", False)) else 0.0
    area_score = min(1.0, max(0.0, float(prop.get("building_area", 80.0) or 80.0) / 140.0))
    zone_is_a = str(prop.get("zone", "B")).upper() == "A"
    zone_liquidity = 0.95 if zone_is_a else 0.90
    location_scarcity_score = _resolve_location_scarcity_score(config, str(prop.get("zone", "B")))
    budget_cap = max(1.0, float(getattr(pref, "max_price", listed_price) or listed_price or 1.0))
    affordability_headroom = max(0.0, min(1.0, (budget_cap - listed_price) / budget_cap))

    edu_weight = float(getattr(pref, "education_weight", 5) or 5) / 10.0
    comfort_weight = float(getattr(pref, "comfort_weight", 5) or 5) / 10.0
    price_weight = float(getattr(pref, "price_sensitivity", 5) or 5) / 10.0
    location_scarcity_weight_enabled = True
    location_scarcity_weight = 0.12
    if config is not None:
        try:
            raw_enabled = config.get("smart_agent.location_scarcity_weight_enabled", True)
            if isinstance(raw_enabled, bool):
                location_scarcity_weight_enabled = raw_enabled
            else:
                location_scarcity_weight_enabled = str(raw_enabled).strip().lower() in {"1", "true", "yes", "y", "on"}
        except Exception:
            location_scarcity_weight_enabled = True
        try:
            location_scarcity_weight = float(
                config.get("smart_agent.location_scarcity_weight", location_scarcity_weight)
            )
        except Exception:
            location_scarcity_weight = 0.12
    location_scarcity_weight = max(0.0, min(0.40, float(location_scarcity_weight)))
    house_age = None
    try:
        raw_house_age = prop.get("house_age", None)
        if raw_house_age is not None:
            house_age = max(0.0, float(raw_house_age))
        else:
            raw_build_year = prop.get("build_year", None)
            if raw_build_year is not None:
                base_year = datetime.now().year
                if config:
                    base_year = int(config.get("simulation.base_year", base_year))
                house_age = max(0.0, float(base_year - int(raw_build_year)))
    except Exception:
        house_age = None

    # baseline utility
    score = (
        discount * (0.50 + price_weight * 0.50)
        + area_score * (0.20 + comfort_weight * 0.20)
        + affordability_headroom * (0.18 + price_weight * 0.32)
        + zone_liquidity * 0.03
    )
    if location_scarcity_weight_enabled and location_scarcity_weight > 0:
        score += location_scarcity_score * location_scarcity_weight

    if edu_need:
        school_bonus = (0.20 + edu_weight * 0.30)
        school_multiplier = 1.0
        if school_match > 0 and house_age is not None:
            start_year = 10
            old_threshold = 20
            mid_decay = 0.015
            old_decay = 0.030
            floor = 0.35
            tier_mult = 1.0
            if config:
                start_year = int(config.get("smart_agent.school_age_decay_start_year", start_year))
                old_threshold = int(config.get("smart_agent.school_age_old_threshold", old_threshold))
                mid_decay = float(config.get("smart_agent.school_age_decay_rate_mid", mid_decay))
                old_decay = float(config.get("smart_agent.school_age_decay_rate_old", old_decay))
                floor = float(config.get("smart_agent.school_age_value_floor", floor))
                school_tier = int(prop.get("school_tier", 3) or 3)
                tier_mult = float(config.get(f"smart_agent.school_tier_decay_multiplier.tier_{school_tier}", 1.0))
            years_mid = max(0.0, min(float(old_threshold), house_age) - float(start_year))
            years_old = max(0.0, house_age - float(old_threshold))
            decay = (years_mid * mid_decay + years_old * old_decay) * max(0.1, tier_mult)
            school_multiplier = max(float(floor), 1.0 - decay)
        score += school_match * school_bonus * school_multiplier

    if house_age is not None:
        age_penalty_start = 25.0
        age_penalty_rate = 0.008
        age_penalty_cap = 0.25
        if config:
            age_penalty_start = float(config.get("smart_agent.house_age_penalty_start_year", age_penalty_start))
            age_penalty_rate = float(config.get("smart_agent.house_age_penalty_rate", age_penalty_rate))
            age_penalty_cap = float(config.get("smart_agent.house_age_penalty_cap", age_penalty_cap))
        penalty_years = max(0.0, house_age - age_penalty_start)
        age_penalty = min(age_penalty_cap, penalty_years * age_penalty_rate)
        score -= age_penalty * (0.4 + comfort_weight * 0.6)

    # Listing-age opportunity: long-listed homes are more negotiable in practice.
    stale_start = 2.0
    stale_bonus_rate = 0.015
    stale_bonus_cap = 0.15
    if config:
        stale_start = float(config.get("smart_agent.listing_age_stale_start_months", stale_start))
        stale_bonus_rate = float(config.get("smart_agent.listing_age_bonus_rate", stale_bonus_rate))
        stale_bonus_cap = float(config.get("smart_agent.listing_age_bonus_cap", stale_bonus_cap))
    stale_months = max(0.0, listing_age_months - stale_start)
    stale_bonus = min(max(0.0, stale_bonus_cap), stale_months * max(0.0, stale_bonus_rate))
    score += stale_bonus * (0.4 + price_weight * 0.6)

    # regime profile adjustments
    trigger = max(0.0, min(0.5, float(bargain_discount_trigger or 0.12)))
    if strategy_profile in ("smart_downturn_defensive", "smart_downturn_opportunistic"):
        score += discount * 1.00
        if discount >= trigger:
            score += 0.35
        score += (1.0 - zone_liquidity) * 0.05 if strategy_profile == "smart_downturn_opportunistic" else 0.0
    elif strategy_profile in ("smart_uptrend_momentum", "smart_uptrend_selective"):
        score += zone_liquidity * 0.08
        score += school_match * (0.20 if strategy_profile == "smart_uptrend_selective" else 0.10)
        score += discount * 0.15
    elif strategy_profile == "smart_volatile_barbell":
        score += discount * 0.35 + school_match * 0.15 + zone_liquidity * 0.05
    elif strategy_profile == "normal_balanced":
        score += discount * 0.20 + school_match * 0.10

    return float(score)


def _persona_shortlist_adjustment(agent: Agent, pref, listing: Dict, prop: Dict) -> Tuple[float, List[str]]:
    """
    Lightweight persona-aware shortlist bias.
    This only nudges candidate ordering; the final property choice still belongs to the LLM.
    """
    reasons: List[str] = []
    delta = 0.0

    zone = str(prop.get("zone", "") or "").upper()
    listed_price = float(listing.get("listed_price", 0.0) or 0.0)
    budget_cap = max(1.0, float(getattr(pref, "max_price", listed_price) or listed_price or 1.0))
    affordability_headroom = max(0.0, min(1.0, (budget_cap - listed_price) / budget_cap))
    school_match = bool(prop.get("is_school_district", False))

    motive = str(getattr(agent.story, "purchase_motive_primary", "") or "").lower()
    housing_stage = str(getattr(agent.story, "housing_stage", "") or "").lower()
    family_stage = str(getattr(agent.story, "family_stage", "") or "").lower()
    education_path = str(getattr(agent.story, "education_path", "") or "").lower()
    financial_profile = str(getattr(agent.story, "financial_profile", "") or "").lower()
    school_urgency = int(getattr(agent, "school_urgency", 0) or 0)

    starter_like = motive in {"starter_home", "starter_no_home", "rent_to_buy", "new_family_first_home"}
    education_driven = motive == "education_driven"
    asset_allocation = motive == "asset_allocation"
    budget_sensitive = financial_profile in {"cashflow_sensitive", "down_payment_sensitive", "budget_tight"}
    international_route = education_path in {"private_or_international", "international", "private"}
    # school_urgency is normalized to 0~3 in agent profile generation.
    active_school_window = school_urgency >= 2 or family_stage in {"junior_high_window", "senior_high_window"}

    if starter_like or housing_stage in {"no_home_first_purchase", "starter_entry"}:
        if zone == "B":
            delta += 0.22
            reasons.append("刚需上车：B区作为可承受起点")
        if affordability_headroom >= 0.25:
            delta += 0.08
            reasons.append("预算余量更友好")

    if budget_sensitive:
        if affordability_headroom >= 0.20:
            delta += 0.10
            reasons.append("现金流敏感：偏好压力更小的房源")
        if zone == "B":
            delta += 0.06
            reasons.append("预算型画像：B区性价比更高")

    if education_driven and education_path == "public_school_priority" and active_school_window:
        if school_match:
            delta += 0.24
            reasons.append("升学窗口：学区属性加分")
        if zone == "A":
            delta += 0.10
            reasons.append("教育驱动：核心区更有吸引力")
    elif education_driven and (international_route or not active_school_window):
        if school_match:
            delta -= 0.05
            reasons.append("非刚性学区路径：削弱学区溢价")
        if zone == "B" and affordability_headroom >= 0.15:
            delta += 0.05
            reasons.append("教育替代路径：接受B区性价比")

    if asset_allocation:
        if affordability_headroom >= 0.15:
            delta += 0.06
            reasons.append("配置型：看重折价与安全边际")
        if school_match and not active_school_window:
            delta -= 0.04
            reasons.append("配置型：学区并非核心驱动")

    return float(delta), reasons


def _resolve_crowd_behavior_profile(agent: Optional[Agent], pref, config=None) -> Dict[str, object]:
    """
    Resolve how a buyer reacts to crowded listings.
    This profile only nudges shortlist ordering; final selection remains with LLM.
    """
    mode = "neutral"  # avoid | neutral | follow
    reasons: List[str] = []

    motive = str(getattr(getattr(agent, "story", None), "purchase_motive_primary", "") or "").lower()
    investment_style = str(getattr(getattr(agent, "story", None), "investment_style", "") or "").lower()
    education_path = str(getattr(getattr(agent, "story", None), "education_path", "") or "").lower()
    price_sensitivity = int(getattr(pref, "price_sensitivity", 5) or 5)
    school_need = bool(getattr(pref, "need_school_district", False))

    if motive in {"asset_allocation"} or investment_style in {"aggressive", "growth", "momentum"}:
        mode = "follow"
        reasons.append("画像偏进攻：可接受热门竞争")
    elif school_need and "public_school" in education_path:
        mode = "follow"
        reasons.append("教育刚需：优先追逐满足刚需的热门房源")
    elif price_sensitivity >= 7 or motive in {"starter_entry", "cashflow_defensive"}:
        mode = "avoid"
        reasons.append("价格敏感：更倾向避开拥挤房源")
    else:
        mode = "neutral"
        reasons.append("常规画像：拥挤度仅作参考")

    avoid_step = 0.05
    follow_step = 0.03
    pressure_scale = 1.0
    crowd_tolerance_avoid = 1.8
    crowd_tolerance_neutral = 2.8
    crowd_tolerance_follow = 4.5
    precheck_reselect_enabled = True
    precheck_max_reselect_rounds = 1
    ratio_follow = 0.0
    ratio_neutral = 0.0
    ratio_avoid = 0.0

    if config is not None:
        try:
            avoid_step = float(
                config.get(
                    "smart_agent.candidate_crowd_avoid_step",
                    config.get("candidate_crowd_avoid_step", avoid_step),
                )
            )
        except Exception:
            avoid_step = 0.05
        try:
            follow_step = float(
                config.get(
                    "smart_agent.candidate_crowd_follow_step",
                    config.get("candidate_crowd_follow_step", follow_step),
                )
            )
        except Exception:
            follow_step = 0.03
        try:
            pressure_scale = float(
                config.get(
                    "smart_agent.candidate_crowd_pressure_scale",
                    config.get("candidate_crowd_pressure_scale", pressure_scale),
                )
            )
        except Exception:
            pressure_scale = 1.0
        try:
            crowd_tolerance_avoid = float(
                config.get(
                    "smart_agent.candidate_crowd_tolerance_avoid",
                    config.get("candidate_crowd_tolerance_avoid", crowd_tolerance_avoid),
                )
            )
        except Exception:
            crowd_tolerance_avoid = 1.8
        try:
            crowd_tolerance_neutral = float(
                config.get(
                    "smart_agent.candidate_crowd_tolerance_neutral",
                    config.get("candidate_crowd_tolerance_neutral", crowd_tolerance_neutral),
                )
            )
        except Exception:
            crowd_tolerance_neutral = 2.8
        try:
            crowd_tolerance_follow = float(
                config.get(
                    "smart_agent.candidate_crowd_tolerance_follow",
                    config.get("candidate_crowd_tolerance_follow", crowd_tolerance_follow),
                )
            )
        except Exception:
            crowd_tolerance_follow = 4.5
        try:
            raw_precheck = config.get(
                "smart_agent.candidate_crowd_precheck_reselect_enabled",
                config.get("candidate_crowd_precheck_reselect_enabled", True),
            )
            if isinstance(raw_precheck, bool):
                precheck_reselect_enabled = raw_precheck
            else:
                precheck_reselect_enabled = str(raw_precheck).strip().lower() in {"1", "true", "yes", "y", "on"}
        except Exception:
            precheck_reselect_enabled = True
        try:
            precheck_max_reselect_rounds = int(
                config.get(
                    "smart_agent.candidate_crowd_precheck_reselect_rounds",
                    config.get("candidate_crowd_precheck_reselect_rounds", precheck_max_reselect_rounds),
                )
            )
        except Exception:
            precheck_max_reselect_rounds = 1
        try:
            ratio_follow = float(
                config.get(
                    "smart_agent.candidate_crowd_mode_ratio_follow",
                    config.get("candidate_crowd_mode_ratio_follow", ratio_follow),
                )
            )
        except Exception:
            ratio_follow = 0.0
        try:
            ratio_neutral = float(
                config.get(
                    "smart_agent.candidate_crowd_mode_ratio_neutral",
                    config.get("candidate_crowd_mode_ratio_neutral", ratio_neutral),
                )
            )
        except Exception:
            ratio_neutral = 0.0
        try:
            ratio_avoid = float(
                config.get(
                    "smart_agent.candidate_crowd_mode_ratio_avoid",
                    config.get("candidate_crowd_mode_ratio_avoid", ratio_avoid),
                )
            )
        except Exception:
            ratio_avoid = 0.0

    avoid_step = max(0.0, min(0.20, float(avoid_step)))
    follow_step = max(0.0, min(0.20, float(follow_step)))
    pressure_scale = max(0.0, min(3.0, float(pressure_scale)))
    crowd_tolerance_avoid = max(0.5, min(8.0, float(crowd_tolerance_avoid)))
    crowd_tolerance_neutral = max(0.5, min(8.0, float(crowd_tolerance_neutral)))
    crowd_tolerance_follow = max(0.5, min(10.0, float(crowd_tolerance_follow)))
    precheck_max_reselect_rounds = max(0, min(3, int(precheck_max_reselect_rounds)))
    ratio_follow = max(0.0, float(ratio_follow))
    ratio_neutral = max(0.0, float(ratio_neutral))
    ratio_avoid = max(0.0, float(ratio_avoid))

    ratio_sum = float(ratio_follow + ratio_neutral + ratio_avoid)
    if ratio_sum > 0 and agent is not None:
        norm_follow = float(ratio_follow / ratio_sum)
        norm_neutral = float(ratio_neutral / ratio_sum)
        norm_avoid = max(0.0, 1.0 - norm_follow - norm_neutral)
        base_seed = 0
        if config is not None:
            try:
                base_seed = int(config.get("simulation.random_seed", 0) or 0)
            except Exception:
                base_seed = 0
        uid = f"{int(getattr(agent, 'id', 0))}:{base_seed}:crowd_mode_v1"
        raw = hashlib.md5(uid.encode("utf-8")).hexdigest()[:8]
        score = int(raw, 16) / float(16 ** 8 - 1)
        if score < norm_follow:
            mode = "follow"
        elif score < norm_follow + norm_neutral:
            mode = "neutral"
        else:
            mode = "avoid"
        reasons = [f"研究者比例分配：追热{norm_follow:.2f} / 中性{norm_neutral:.2f} / 避拥挤{norm_avoid:.2f}"]

    if mode == "avoid":
        crowd_tolerance_units = crowd_tolerance_avoid
    elif mode == "follow":
        crowd_tolerance_units = crowd_tolerance_follow
    else:
        crowd_tolerance_units = crowd_tolerance_neutral

    return {
        "mode": str(mode),
        "reasons": reasons[:3],
        "avoid_step": float(avoid_step),
        "follow_step": float(follow_step),
        "pressure_scale": float(pressure_scale),
        "tolerance_units": float(crowd_tolerance_units),
        "precheck_reselect_enabled": bool(precheck_reselect_enabled),
        "precheck_max_reselect_rounds": int(precheck_max_reselect_rounds),
        "ratio_follow": float(ratio_follow),
        "ratio_neutral": float(ratio_neutral),
        "ratio_avoid": float(ratio_avoid),
    }


def _build_selection_reason_tags(
    buyer: Agent,
    pref,
    selected_listing: Dict,
    selected_property: Dict,
    shortlist_context: List[Dict],
) -> List[str]:
    """
    Build structured tags from already-available shortlist signals.
    This is evidence metadata only; it does not replace LLM choice/reasoning.
    """
    tags: List[str] = []
    zone = str(selected_property.get("zone", "") or "")
    if zone:
        tags.append(f"zone:{zone}")

    tags.append("school:yes" if bool(selected_property.get("is_school_district", False)) else "school:no")

    for attr_name, prefix in (
        ("purchase_motive_primary", "motive"),
        ("housing_stage", "housing"),
        ("family_stage", "family"),
        ("education_path", "education"),
        ("financial_profile", "finance"),
    ):
        value = str(getattr(buyer.story, attr_name, "") or "").strip()
        if value:
            tags.append(f"{prefix}:{value}")

    selected_id = int(selected_listing.get("property_id", -1) or -1)
    shortlist_entry = next(
        (row for row in shortlist_context if int(row.get("property_id", -1)) == selected_id),
        None,
    )
    for reason in list((shortlist_entry or {}).get("persona_reasons", []) or [])[:3]:
        clean = str(reason or "").strip()
        if clean:
            tags.append(f"bias:{clean}")

    max_price = float(getattr(pref, "max_price", 0.0) or 0.0)
    listed_price = float(selected_listing.get("listed_price", 0.0) or 0.0)
    if max_price > 0 and listed_price > 0:
        budget_ratio = listed_price / max_price
        if budget_ratio <= 0.70:
            tags.append("budget:comfortable")
        elif budget_ratio <= 0.90:
            tags.append("budget:stretch")
        else:
            tags.append("budget:ceiling")

    return list(dict.fromkeys(tags))


def _derive_property_type_bucket(prop: Dict) -> str:
    """
    Coarse type bucket for 8-bucket matching:
    - JUST: starter/compact demand
    - IMPROVE: upgrade/larger demand
    """
    ptype = str(prop.get("property_type", "") or "").lower()
    area = float(prop.get("building_area", 0.0) or 0.0)
    if any(x in ptype for x in ["villa", "improve", "改善", "大平层"]):
        return "IMPROVE"
    if any(x in ptype for x in ["small", "刚需", "compact", "studio"]):
        return "JUST"
    return "IMPROVE" if area >= 110.0 else "JUST"


def _candidate_area_band(prop: Dict) -> str:
    area = float(prop.get("building_area", 0.0) or 0.0)
    if area <= 90.0:
        return "SMALL"
    if area <= 120.0:
        return "MEDIUM"
    if area <= 160.0:
        return "LARGE"
    return "XL"


def _normalize_counterfactual_reason_tag(raw: object) -> str:
    text = str(raw or "").strip().lower().replace("-", "_").replace(" ", "_")
    alias_map = {
        "too_expensive": "too_expensive",
        "price_too_high": "too_expensive",
        "budget_stretch": "too_expensive",
        "no_school": "no_school",
        "school_mismatch": "no_school",
        "wrong_zone": "wrong_zone",
        "zone_mismatch": "wrong_zone",
        "too_large": "size_too_large",
        "size_too_large": "size_too_large",
        "too_small": "size_too_small",
        "size_too_small": "size_too_small",
        "inferior_same_cluster": "inferior_same_cluster",
        "cluster_inferior": "inferior_same_cluster",
    }
    return alias_map.get(text, "")


def _derive_five_factor_contract(
    buyer: Agent,
    pref,
    *,
    buy_deadline_stage: str,
    buy_deadline_months_left: int,
    max_wait_months: int,
) -> Dict[str, object]:
    """
    Strategy-layer contract (no hard decision override):
    3 preferences + 2 guards.
    """
    story = getattr(buyer, "story", None)
    education_path = str(getattr(story, "education_path", "") or "").lower()
    motive = str(getattr(story, "purchase_motive_primary", "") or "").lower()
    financial_profile = str(getattr(story, "financial_profile", "") or "").lower()
    school_urgency = int(getattr(buyer, "school_urgency", 0) or 0)

    need_school = bool(getattr(pref, "need_school_district", False))
    target_zone = str(getattr(pref, "target_zone", "") or "").upper()

    if motive in {"starter_entry", "starter_home", "marriage_home"}:
        type_target = "JUST"
    elif motive in {"upgrade_living", "chain_replacement"}:
        type_target = "IMPROVE"
    else:
        type_target = "JUST" if float(getattr(pref, "max_price", 0.0) or 0.0) <= 3_000_000 else "IMPROVE"

    if need_school and school_urgency >= 2:
        school_must, school_comp = "hard", 0.1
    elif need_school:
        school_must, school_comp = "strong", 0.3
    else:
        school_must, school_comp = "soft", 0.8

    if target_zone in {"A", "B"}:
        zone_must = "strong"
        zone_comp = 0.25 if target_zone == "A" else 0.4
    else:
        zone_must, zone_comp = "soft", 0.8
    if "cash_tight" in financial_profile and target_zone == "A":
        zone_comp = max(zone_comp, 0.55)
    if education_path in {"private_or_international", "international_private_route"} and target_zone == "A":
        school_must, school_comp = "soft", 0.85
        zone_must, zone_comp = "hard", 0.12

    if type_target == "JUST":
        type_must, type_comp = "strong", 0.35
    else:
        type_must, type_comp = ("hard", 0.15) if motive in {"upgrade_living", "chain_replacement"} else ("strong", 0.3)

    return {
        "school": {"must_level": school_must, "compromise": float(school_comp), "target": "SCHOOL" if need_school else "ANY"},
        "zone": {"must_level": zone_must, "compromise": float(zone_comp), "target": target_zone if target_zone in {"A", "B"} else "ANY"},
        "type": {"must_level": type_must, "compromise": float(type_comp), "target": type_target},
        "finance_guard_snapshot": {
            "max_price": float(getattr(pref, "max_price", 0.0) or 0.0),
            "cash": float(getattr(buyer, "cash", 0.0) or 0.0),
            "monthly_income": float(getattr(buyer, "monthly_income", 0.0) or 0.0),
            "financial_profile": str(financial_profile),
        },
        "deadline_pressure_snapshot": {
            "stage": str(buy_deadline_stage),
            "months_left": int(max(0, buy_deadline_months_left)),
            "total_months": int(max_wait_months),
        },
    }


def build_candidate_shortlist(
    candidates: List[Dict],
    properties_map: Dict[int, Dict],
    pref,
    strategy_profile: str,
    top_k: int = 5,
    exploration_slots: int = 1,
    bargain_discount_trigger: float = 0.12,
    config=None,
    agent: Optional[Agent] = None,
    crowd_mode: str = "neutral",
    crowd_avoid_step: float = 0.05,
    crowd_follow_step: float = 0.03,
    pressure_scale: float = 1.0,
    shortlist_offset: int = 0,
) -> List[Dict]:
    pressure_map = {}
    buy_task_locked = False
    if agent is not None:
        pressure_map = getattr(agent, "_candidate_pressure_map", {}) or {}
        buy_task_locked = bool(getattr(agent, "_buy_task_locked", False))
    try:
        pressure_penalty_step = float(
            config.get(
                "smart_agent.candidate_pressure_penalty_step",
                config.get("candidate_pressure_penalty_step", 0.08),
            )
        ) if config else 0.08
    except Exception:
        pressure_penalty_step = 0.08
    try:
        pressure_penalty_cap = int(
            config.get(
                "smart_agent.candidate_pressure_penalty_cap",
                config.get("candidate_pressure_penalty_cap", 4),
            )
        ) if config else 4
    except Exception:
        pressure_penalty_cap = 4
    pressure_penalty_step = max(0.0, min(0.5, pressure_penalty_step))
    pressure_penalty_cap = max(0, min(12, pressure_penalty_cap))
    pressure_scale = max(0.0, min(3.0, float(pressure_scale)))
    crowd_avoid_step = max(0.0, min(0.2, float(crowd_avoid_step)))
    crowd_follow_step = max(0.0, min(0.2, float(crowd_follow_step)))

    try:
        zone_min_slots = int(config.get("smart_agent.candidate_zone_min_slots", 1)) if config else 1
    except Exception:
        zone_min_slots = 1
    try:
        price_band_min_slots = int(config.get("smart_agent.candidate_price_band_min_slots", 1)) if config else 1
    except Exception:
        price_band_min_slots = 1
    try:
        tiebreak_noise = float(config.get("smart_agent.candidate_tiebreak_noise", 0.015)) if config else 0.015
    except Exception:
        tiebreak_noise = 0.015
    try:
        rule_weight = float(config.get("smart_agent.candidate_score_weight_rule", 0.55)) if config else 0.55
    except Exception:
        rule_weight = 0.55
    try:
        unseen_bonus = float(config.get("smart_agent.candidate_unseen_bonus", 0.10)) if config else 0.10
    except Exception:
        unseen_bonus = 0.10
    try:
        target_zone_unseen_bonus = float(config.get("smart_agent.target_zone_unseen_bonus", 0.05)) if config else 0.05
    except Exception:
        target_zone_unseen_bonus = 0.05
    try:
        target_zone_min_slots = int(config.get("smart_agent.target_zone_min_slots", 1)) if config else 1
    except Exception:
        target_zone_min_slots = 1
    try:
        b_zone_shortlist_min_slots = int(config.get("smart_agent.b_zone_shortlist_min_slots", 2)) if config else 2
    except Exception:
        b_zone_shortlist_min_slots = 2

    zone_min_slots = max(0, min(3, zone_min_slots))
    price_band_min_slots = max(0, min(2, price_band_min_slots))
    tiebreak_noise = max(0.0, min(0.08, tiebreak_noise))
    rule_weight = max(0.2, min(0.9, rule_weight))
    dimension_weight = max(0.1, min(0.8, 1.0 - rule_weight))
    unseen_bonus = max(0.0, min(0.30, unseen_bonus))
    target_zone_unseen_bonus = max(0.0, min(0.20, target_zone_unseen_bonus))
    target_zone_min_slots = max(0, min(3, target_zone_min_slots))
    b_zone_shortlist_min_slots = max(0, min(4, b_zone_shortlist_min_slots))
    historical_seen_ids = {
        int(x) for x in (getattr(agent, "_historical_seen_property_ids", []) or [])
        if x is not None
    } if agent is not None else set()
    heat_meta_map = dict(getattr(agent, "_candidate_heat_meta_map", {}) or {}) if agent is not None else {}
    quota_used_map = dict(getattr(agent, "_candidate_monthly_quota_used_map", {}) or {}) if agent is not None else {}
    commitment_map = (
        dict(getattr(agent, "_candidate_monthly_commitment_map", {}) or {}) if agent is not None else {}
    )
    fake_hot_circuit_enabled = _read_bool_config(
        config,
        "smart_agent.candidate_fake_hot_circuit_enabled",
        True,
    )
    fake_hot_historical_match_threshold = max(
        3,
        min(40, _read_int_config(config, "smart_agent.candidate_fake_hot_historical_match_threshold", 10)),
    )
    fake_hot_negotiation_ratio_threshold = max(
        0.0,
        min(1.0, _read_float_config(config, "smart_agent.candidate_fake_hot_negotiation_ratio_threshold", 0.20)),
    )
    fake_hot_commitment_ratio_threshold = max(
        0.0,
        min(
            1.0,
            _read_float_config(
                config,
                "smart_agent.candidate_fake_hot_commitment_ratio_threshold",
                fake_hot_negotiation_ratio_threshold,
            ),
        ),
    )
    fake_hot_same_month_quota_threshold = max(
        2,
        min(20, _read_int_config(config, "smart_agent.candidate_fake_hot_same_month_quota_threshold", 4)),
    )
    fake_hot_same_month_commitment_threshold = max(
        1,
        min(
            12,
            _read_int_config(
                config,
                "smart_agent.candidate_fake_hot_same_month_commitment_threshold",
                2,
            ),
        ),
    )
    fake_hot_score_penalty = max(
        0.0,
        min(1.0, _read_float_config(config, "smart_agent.candidate_fake_hot_score_penalty", 0.28)),
    )
    sibling_rotation_bonus = max(
        0.0,
        min(0.6, _read_float_config(config, "smart_agent.candidate_sibling_rotation_bonus", 0.14)),
    )
    true_competition_sibling_bonus = max(
        0.0,
        min(0.5, _read_float_config(config, "smart_agent.candidate_true_competition_sibling_bonus", 0.10)),
    )
    true_competition_spillover_min_score = max(
        0.1,
        min(1.0, _read_float_config(config, "smart_agent.candidate_true_competition_spillover_min_score", 0.45)),
    )
    fake_hot_pool_preserve_min = max(
        1,
        min(12, _read_int_config(config, "smart_agent.candidate_fake_hot_pool_preserve_min", 3)),
    )
    candidate_two_stage_enabled = _read_bool_config(
        config,
        "smart_agent.candidate_two_stage_enabled",
        True,
    )
    candidate_two_stage_min_stage1_pool = max(
        2,
        min(16, _read_int_config(config, "smart_agent.candidate_two_stage_min_stage1_pool", 4)),
    )
    candidate_two_stage_max_stage2_fill = max(
        0,
        min(12, _read_int_config(config, "smart_agent.candidate_two_stage_max_stage2_fill", 6)),
    )
    candidate_diversity_cluster_cap = max(
        1,
        min(4, _read_int_config(config, "smart_agent.candidate_diversity_cluster_cap", 2)),
    )
    counterfactual_feedback_enabled = _read_bool_config(
        config,
        "smart_agent.candidate_counterfactual_feedback_enabled",
        True,
    )
    counterfactual_reject_penalty = max(
        0.0,
        min(0.8, _read_float_config(config, "smart_agent.candidate_counterfactual_reject_penalty", 0.28)),
    )
    counterfactual_same_cluster_penalty = max(
        0.0,
        min(0.6, _read_float_config(config, "smart_agent.candidate_counterfactual_same_cluster_penalty", 0.12)),
    )
    counterfactual_reason_penalty = max(
        0.0,
        min(0.6, _read_float_config(config, "smart_agent.candidate_counterfactual_reason_penalty", 0.08)),
    )

    def _price_band(listing: Dict) -> str:
        listed_price = float(listing.get("listed_price", 0.0) or 0.0)
        budget_cap = max(1.0, float(getattr(pref, "max_price", listed_price) or listed_price or 1.0))
        ratio = listed_price / budget_cap
        if ratio <= 0.55:
            return "LOW"
        if ratio <= 0.85:
            return "MID"
        return "HIGH"

    def _diversity_cluster_key(listing: Dict, prop: Dict) -> str:
        zone_bucket = str(prop.get("zone", "") or "").upper() or "UNK"
        school_bucket = "SCHOOL" if bool(prop.get("is_school_district", False)) else "NOSCHOOL"
        type_bucket = _derive_property_type_bucket(prop)
        area_bucket = _candidate_area_band(prop)
        price_bucket = _price_band(listing)
        return f"{zone_bucket}:{school_bucket}:{type_bucket}:{area_bucket}:{price_bucket}"

    last_ctx = dict(getattr(agent, "_last_buyer_match_context", {}) or {}) if agent is not None else {}
    explicit_reject_ids: Set[int] = set()
    rejected_feedback_map: Dict[int, Dict[str, object]] = {}
    rejected_cluster_feedback: Dict[str, List[Dict[str, object]]] = {}
    if counterfactual_feedback_enabled and last_ctx:
        for entry in list(last_ctx.get("rejected_property_feedback", []) or [])[:6]:
            try:
                rejected_pid = int(entry.get("property_id"))
            except Exception:
                continue
            rejected_prop = properties_map.get(rejected_pid, {}) or {}
            if not rejected_prop:
                continue
            feedback_listed_price = float(entry.get("listed_price", 0.0) or 0.0)
            feedback_prop = dict(rejected_prop)
            feedback_prop["building_area"] = float(
                entry.get("building_area", rejected_prop.get("building_area", 0.0)) or 0.0
            )
            feedback_prop["zone"] = str(entry.get("zone", rejected_prop.get("zone", "")) or "")
            feedback_prop["is_school_district"] = bool(
                entry.get("is_school_district", rejected_prop.get("is_school_district", False))
            )
            feedback = {
                "property_id": rejected_pid,
                "reason": str(entry.get("reason", "") or ""),
                "reason_tag": _normalize_counterfactual_reason_tag(entry.get("reason_tag")),
                "listed_price": feedback_listed_price,
                "building_area": float(feedback_prop.get("building_area", 0.0) or 0.0),
                "zone": str(feedback_prop.get("zone", "") or ""),
                "is_school_district": bool(feedback_prop.get("is_school_district", False)),
                # Always recompute with the current cluster definition so that
                # older stored feedback remains compatible after bucket refactors.
                "cluster_key": _diversity_cluster_key(
                    {"property_id": rejected_pid, "listed_price": feedback_listed_price},
                    feedback_prop,
                ),
            }
            explicit_reject_ids.add(rejected_pid)
            rejected_feedback_map[rejected_pid] = feedback
            rejected_cluster_feedback.setdefault(str(feedback["cluster_key"]), []).append(feedback)

    def _persist_pool_stats(
        *,
        selected_rows: List[Dict],
        ranked_size: int,
        stage1_pool_size: int,
        blocked_fake_hot_count: int,
        stage2_fill_size: int,
    ) -> List[Dict]:
        if agent is not None:
            setattr(
                agent,
                "_last_candidate_pool_stats",
                {
                    "fake_hot_circuit_enabled": bool(fake_hot_circuit_enabled),
                    "fake_hot_blocked_pool_count": int(blocked_fake_hot_count),
                    "ranked_candidate_count": int(ranked_size),
                    "stage1_pool_size": int(stage1_pool_size),
                    "stage2_fill_size": int(stage2_fill_size),
                    "candidate_two_stage_enabled": bool(candidate_two_stage_enabled),
                    "candidate_two_stage_min_stage1_pool": int(candidate_two_stage_min_stage1_pool),
                    "candidate_two_stage_max_stage2_fill": int(candidate_two_stage_max_stage2_fill),
                    "selected_size": int(len(selected_rows or [])),
                },
            )
        return selected_rows

    def _candidate_heat_state(listing: Dict) -> Dict[str, object]:
        pid = int(listing.get("property_id", -1))
        meta = dict(heat_meta_map.get(pid, {}) or {})
        recent_exposure_count = int(
            meta.get("recent_exposure_count", meta.get("recent_match_count", 0)) or 0
        )
        recent_commitment_count = int(meta.get("recent_commitment_count", 0) or 0)
        recent_competition_count = int(meta.get("recent_competition_count", 0) or 0)
        recent_negotiations = int(meta.get("recent_negotiation_count", 0) or 0)
        recent_transactions = int(meta.get("recent_transaction_count", 0) or 0)
        if recent_exposure_count > 0:
            recent_commitment_ratio = float(
                meta.get(
                    "recent_commitment_ratio",
                    recent_commitment_count / max(1, recent_exposure_count),
                )
                or 0.0
            )
            recent_neg_ratio = float(
                meta.get(
                    "recent_negotiation_ratio",
                    recent_negotiations / max(1, recent_exposure_count),
                )
                or 0.0
            )
        else:
            recent_commitment_ratio = 0.0
            recent_neg_ratio = 0.0
        same_month_quota_used = int(quota_used_map.get(pid, 0) or 0)
        same_month_commitment_count = int(commitment_map.get(pid, 0) or 0)
        historical_fake_hot = bool(
            meta.get("fake_hot_historical", False)
            or (
                recent_exposure_count >= fake_hot_historical_match_threshold
                and recent_commitment_ratio <= fake_hot_commitment_ratio_threshold
                and recent_neg_ratio <= fake_hot_negotiation_ratio_threshold
                and recent_transactions <= 0
            )
        )
        same_month_fake_hot = bool(
            same_month_commitment_count >= fake_hot_same_month_commitment_threshold
            or same_month_quota_used >= fake_hot_same_month_quota_threshold
        )
        hot_listing_score = float(meta.get("hot_listing_score", 0.0) or 0.0)
        real_competition_score = float(meta.get("real_competition_score", hot_listing_score) or hot_listing_score)
        return {
            "recent_match_count": int(recent_exposure_count),
            "recent_exposure_count": int(recent_exposure_count),
            "recent_commitment_count": int(recent_commitment_count),
            "recent_competition_count": int(recent_competition_count),
            "recent_negotiation_count": int(recent_negotiations),
            "recent_transaction_count": int(recent_transactions),
            "recent_commitment_ratio": float(recent_commitment_ratio),
            "recent_negotiation_ratio": float(recent_neg_ratio),
            "same_month_quota_used": int(same_month_quota_used),
            "same_month_commitment_count": int(same_month_commitment_count),
            "historical_fake_hot": bool(historical_fake_hot),
            "same_month_fake_hot": bool(same_month_fake_hot),
            "is_fake_hot": bool(historical_fake_hot or same_month_fake_hot),
            "hot_listing_score": float(hot_listing_score),
            "real_competition_score": float(real_competition_score),
        }

    spillover_context = dict(getattr(agent, "substitute_spillover_context", {}) or {}) if agent is not None else {}
    spillover_month = int(getattr(agent, "substitute_spillover_month", -1) or -1) if agent is not None else -1
    current_matching_month = int(getattr(agent, "_current_matching_month", -1) or -1) if agent is not None else -1

    def _candidate_spillover_bonus(listing: Dict, prop: Dict) -> Tuple[float, Dict[str, object]]:
        if not spillover_context:
            return 0.0, {"enabled": False}
        if spillover_month < 0:
            return 0.0, {"enabled": False}
        if current_matching_month > 0 and spillover_month != current_matching_month:
            return 0.0, {"enabled": False, "stale": True}
        try:
            pid = int(listing.get("property_id", -1) or -1)
        except Exception:
            pid = -1
        source_pid = int(spillover_context.get("source_property_id", -1) or -1)
        if pid <= 0 or pid == source_pid:
            return 0.0, {"enabled": False, "same_source": True}

        zone = str(prop.get("zone", listing.get("zone", "")) or "").upper()
        is_school = bool(prop.get("is_school_district", listing.get("is_school_district", False)))
        property_type = str(prop.get("property_type", "") or "").upper() or "UNK"
        try:
            area = float(prop.get("building_area", 0.0) or 0.0)
        except Exception:
            area = 0.0
        try:
            listed_price = float(listing.get("listed_price", listing.get("price", 0.0)) or 0.0)
        except Exception:
            listed_price = 0.0

        similarity = 0.0
        reasons: List[str] = []
        if zone and zone == str(spillover_context.get("zone", "") or "").upper():
            similarity += 0.28
            reasons.append("same_zone")
        if bool(is_school) == bool(spillover_context.get("is_school_district", False)):
            similarity += 0.18
            reasons.append("same_school_flag")
        if property_type == str(spillover_context.get("property_type", "") or "").upper():
            similarity += 0.18
            reasons.append("same_property_type")

        if area > 0.0:
            if _candidate_area_band(prop) == str(spillover_context.get("area_band", "UNK") or "UNK"):
                similarity += 0.16
                reasons.append("same_area_band")
            else:
                similarity += 0.06
                reasons.append("near_area_band")
        if listed_price > 0.0:
            if _price_band(listing) == str(spillover_context.get("price_band", "UNK") or "UNK"):
                similarity += 0.12
                reasons.append("same_price_band")
            source_price = float(spillover_context.get("listed_price", 0.0) or 0.0)
            if source_price > 0.0:
                gap_ratio = abs(listed_price - source_price) / max(source_price, 1.0)
                if gap_ratio <= 0.10:
                    similarity += 0.16
                    reasons.append("near_price")
                elif gap_ratio <= 0.20:
                    similarity += 0.08
                    reasons.append("mid_price")
        similarity = max(0.0, min(1.0, float(similarity)))
        if similarity <= 0.0:
            return 0.0, {"enabled": True, "similarity": 0.0, "reasons": reasons}

        priority_weight = max(0.0, min(1.5, float(spillover_context.get("priority_weight", 0.0) or 0.0)))
        competition_strength = max(1.0, float(spillover_context.get("competition_strength", 1.0) or 1.0))
        competition_factor = min(1.0, competition_strength / 2.0)
        bonus = float(similarity * priority_weight * competition_factor)
        return max(0.0, min(1.25, bonus)), {
            "enabled": True,
            "similarity": float(round(similarity, 4)),
            "priority_weight": float(round(priority_weight, 4)),
            "competition_factor": float(round(competition_factor, 4)),
            "source_property_id": int(source_pid),
            "reasons": reasons,
        }

    def _diversity_key(listing: Dict) -> str:
        pid = int(listing.get("property_id", -1))
        prop = properties_map.get(pid, {})
        return _diversity_cluster_key(listing, prop)

    def _zone_of(listing: Dict) -> str:
        pid = int(listing.get("property_id", -1))
        prop = properties_map.get(pid, {})
        return str(prop.get("zone", "")).upper() or "UNK"

    def _resolve_preference_weights() -> Dict[str, float]:
        motive = str(getattr(getattr(agent, "story", None), "purchase_motive_primary", "") or "").lower()
        profile: Dict[str, Dict[str, float]] = {
            "starter_entry": {"school": 0.12, "zone": 0.24, "comfort": 0.16, "price": 0.36, "finance": 0.12},
            "upgrade_living": {"school": 0.14, "zone": 0.24, "comfort": 0.32, "price": 0.16, "finance": 0.14},
            "education_driven": {"school": 0.46, "zone": 0.20, "comfort": 0.14, "price": 0.10, "finance": 0.10},
            "asset_allocation": {"school": 0.10, "zone": 0.16, "comfort": 0.12, "price": 0.24, "finance": 0.38},
            "intl_education_substitute": {"school": 0.08, "zone": 0.20, "comfort": 0.15, "price": 0.22, "finance": 0.35},
            "chain_replacement": {"school": 0.12, "zone": 0.20, "comfort": 0.28, "price": 0.22, "finance": 0.18},
            "cashflow_defensive": {"school": 0.08, "zone": 0.16, "comfort": 0.16, "price": 0.30, "finance": 0.30},
        }
        return profile.get(motive, {"school": 0.16, "zone": 0.20, "comfort": 0.22, "price": 0.24, "finance": 0.18})

    def _dimension_scores(listing: Dict, prop: Dict) -> Dict[str, float]:
        listed_price = float(listing.get("listed_price", 0.0) or 0.0)
        budget_cap = max(1.0, float(getattr(pref, "max_price", listed_price) or listed_price or 1.0))
        ref_value = float(
            listing.get("assessed_price", 0.0)
            or prop.get("current_valuation", 0.0)
            or prop.get("base_value", 0.0)
            or listed_price
            or 1.0
        )
        discount = (ref_value - listed_price) / max(ref_value, 1.0)
        affordability_headroom = max(0.0, min(1.0, (budget_cap - listed_price) / budget_cap))
        price_score = max(0.0, min(1.0, 0.5 + discount + affordability_headroom * 0.5))

        target_zone = str(getattr(pref, "target_zone", "") or "").upper()
        zone = str(prop.get("zone", "") or "").upper()
        base_zone_score = 1.0 if (target_zone and zone == target_zone) else (0.72 if not target_zone else 0.45)
        location_scarcity_score = _resolve_location_scarcity_score(config, zone)
        zone_score = max(0.0, min(1.0, base_zone_score * 0.7 + location_scarcity_score * 0.3))

        area = float(prop.get("building_area", 80.0) or 80.0)
        area_score = min(1.0, max(0.0, area / 140.0))
        house_age = None
        raw_build_year = prop.get("build_year", None)
        if raw_build_year is not None:
            try:
                base_year = datetime.now().year
                if config:
                    base_year = int(config.get("simulation.base_year", base_year))
                house_age = max(0.0, float(base_year - int(raw_build_year)))
            except Exception:
                house_age = None
        age_penalty = 0.0
        if house_age is not None:
            age_penalty = min(0.4, max(0.0, (house_age - 20.0) * 0.01))
        comfort_score = max(0.0, min(1.0, area_score - age_penalty + 0.25))

        need_school = bool(getattr(pref, "need_school_district", False))
        school_match = 1.0 if bool(prop.get("is_school_district", False)) else 0.0
        school_score = school_match if need_school else (0.70 + school_match * 0.20)
        school_score = max(0.0, min(1.0, school_score))

        rental_yield = float(listing.get("rental_yield", prop.get("rental_yield", 0.0)) or 0.0)
        finance_score = max(0.0, min(1.0, rental_yield / 0.06))

        return {
            "school": float(school_score),
            "zone": float(zone_score),
            "comfort": float(comfort_score),
            "price": float(price_score),
            "finance": float(finance_score),
            "location_scarcity": float(location_scarcity_score),
        }

    try:
        pressure_bonus_step = float(
            config.get(
                "smart_agent.candidate_pressure_bonus_step",
                config.get("candidate_pressure_bonus_step", 0.05),
            )
        ) if config else 0.05
    except Exception:
        pressure_bonus_step = 0.05
    try:
        pressure_bonus_cap = float(
            config.get(
                "smart_agent.candidate_pressure_bonus_cap",
                config.get("candidate_pressure_bonus_cap", 3.0),
            )
        ) if config else 3.0
    except Exception:
        pressure_bonus_cap = 3.0
    pressure_bonus_step = max(0.0, min(0.3, pressure_bonus_step))
    pressure_bonus_cap = max(0.0, min(12.0, pressure_bonus_cap))

    def _pressure_units(listing: Dict) -> float:
        pid = int(listing.get("property_id", -1))
        try:
            pressure = float(pressure_map.get(pid, 0) or 0)
        except Exception:
            pressure = 0.0
        # Positive pressure => penalty (热门房源降权)
        if pressure >= 0:
            return float(min(max(0.0, pressure), float(pressure_penalty_cap)))
        # Negative pressure => bonus (冷门房源加权)
        return -float(min(abs(pressure), float(pressure_bonus_cap)))

    def _pressure_penalty(listing: Dict) -> float:
        pressure_units = _pressure_units(listing)
        if pressure_units >= 0:
            return pressure_units * pressure_penalty_step * pressure_scale
        return pressure_units * pressure_bonus_step * pressure_scale

    def _crowd_style_adjustment(listing: Dict) -> float:
        pressure_units = _pressure_units(listing)
        if pressure_units <= 0:
            return 0.0
        mode = str(crowd_mode or "neutral").lower()
        if mode == "avoid":
            return -pressure_units * crowd_avoid_step * pressure_scale
        if mode == "follow":
            return pressure_units * crowd_follow_step * pressure_scale
        return 0.0

    def shortlist_value_score(listing: Dict) -> float:
        pid = int(listing.get("property_id", -1))
        prop = properties_map.get(pid, {})
        listed_price = float(listing.get("listed_price", 0.0) or 0.0)
        ref_value = float(
            listing.get("assessed_price", 0.0)
            or prop.get("current_valuation", 0.0)
            or prop.get("base_value", 0.0)
            or listed_price
            or 1.0
        )
        budget_cap = max(1.0, float(getattr(pref, "max_price", listed_price) or listed_price or 1.0))
        bargain_ratio = (ref_value - listed_price) / max(ref_value, 1.0)
        affordability_headroom = max(0.0, min(1.0, (budget_cap - listed_price) / budget_cap))
        zone = str(prop.get("zone", "") or "").upper()
        target_zone = str(getattr(pref, "target_zone", "") or "").upper()
        location_scarcity = _resolve_location_scarcity_score(config, zone)
        location_bonus_weight = 0.20
        cross_zone_discount_threshold = 0.20
        if config is not None:
            try:
                location_bonus_weight = float(
                    config.get("smart_agent.shortlist_location_bonus_weight", location_bonus_weight)
                )
            except Exception:
                location_bonus_weight = 0.20
            try:
                cross_zone_discount_threshold = float(
                    config.get("smart_agent.cross_zone_discount_threshold", cross_zone_discount_threshold)
                )
            except Exception:
                cross_zone_discount_threshold = 0.20
        location_bonus_weight = max(0.0, min(0.50, float(location_bonus_weight)))
        cross_zone_discount_threshold = max(0.0, min(0.60, float(cross_zone_discount_threshold)))
        same_zone_or_no_pref = (not target_zone) or (zone == target_zone)
        cross_zone_bargain_ok = bargain_ratio >= cross_zone_discount_threshold
        is_developer = int(listing.get("owner_id", 0) or 0) == -1
        if is_developer:
            developer_bonus = 0.12 if (same_zone_or_no_pref or cross_zone_bargain_ok) else 0.02
        else:
            developer_bonus = 0.0
        recovery_bonus = 0.0
        if buy_task_locked:
            pressure_units = max(0.0, float(_pressure_units(listing)))
            price_stretch = listed_price / max(1.0, budget_cap)
            recovery_bonus += affordability_headroom * 0.35
            recovery_bonus -= min(6.0, pressure_units) * 0.08
            if price_stretch > 0.92:
                recovery_bonus -= min(0.30, (price_stretch - 0.92) * 1.5)
        return (
            bargain_ratio
            + affordability_headroom * 0.75
            + developer_bonus
            + location_scarcity * location_bonus_weight
            + recovery_bonus
        )

    ranked = []
    scoring_map: Dict[int, Dict[str, object]] = {}
    preference_weights = _resolve_preference_weights()
    for listing in candidates:
        prop = properties_map.get(listing.get("property_id"))
        if not prop:
            continue
        pid = int(listing.get("property_id", -1))
        zone_value = _zone_of(listing)
        is_historically_unseen = pid not in historical_seen_ids if historical_seen_ids else False
        unseen_discovery_bonus = float(unseen_bonus) if is_historically_unseen else 0.0
        if is_historically_unseen and zone_value == str(getattr(pref, "target_zone", "") or "").upper():
            unseen_discovery_bonus += float(target_zone_unseen_bonus)
        zone_bucket = str(prop.get("zone", "") or "").upper() or "UNK"
        school_bucket = "SCHOOL" if bool(prop.get("is_school_district", False)) else "NOSCHOOL"
        type_bucket = _derive_property_type_bucket(prop)
        bucket_id = f"{zone_bucket}_{school_bucket}_{type_bucket}"
        diversity_cluster_key = _diversity_cluster_key(listing, prop)
        heat_state = _candidate_heat_state(listing)
        raw_score = _strategy_score_candidate(
            listing,
            prop,
            pref,
            strategy_profile,
            bargain_discount_trigger=bargain_discount_trigger,
            config=config,
        )
        dim_scores = _dimension_scores(listing, prop)
        weighted_dim_score = float(
            sum(float(preference_weights.get(k, 0.0)) * float(dim_scores.get(k, 0.0)) for k in preference_weights.keys())
        )
        persona_bonus, _ = _persona_shortlist_adjustment(agent, pref, listing, prop) if agent else (0.0, [])
        substitute_spillover_bonus, substitute_spillover_meta = _candidate_spillover_bonus(listing, prop)
        crowd_adjust = _crowd_style_adjustment(listing)
        noise = random.uniform(-tiebreak_noise, tiebreak_noise) if tiebreak_noise > 0 else 0.0
        counterfactual_penalty = 0.0
        counterfactual_penalty_reasons: List[str] = []
        if counterfactual_feedback_enabled:
            if pid in explicit_reject_ids:
                counterfactual_penalty += float(counterfactual_reject_penalty)
                counterfactual_penalty_reasons.append("explicit_reject")
            if diversity_cluster_key in rejected_cluster_feedback:
                counterfactual_penalty += float(counterfactual_same_cluster_penalty)
                counterfactual_penalty_reasons.append("same_cluster_reject")
                tags_in_cluster = {
                    str(item.get("reason_tag", "") or "")
                    for item in rejected_cluster_feedback.get(diversity_cluster_key, [])
                    if str(item.get("reason_tag", "") or "")
                }
                listed_price = float(listing.get("listed_price", 0.0) or 0.0)
                area = float(prop.get("building_area", 0.0) or 0.0)
                if "too_expensive" in tags_in_cluster:
                    rejected_prices = [
                        float(item.get("listed_price", 0.0) or 0.0)
                        for item in rejected_cluster_feedback.get(diversity_cluster_key, [])
                        if float(item.get("listed_price", 0.0) or 0.0) > 0.0
                    ]
                    if rejected_prices and listed_price >= min(rejected_prices) * 0.97:
                        counterfactual_penalty += float(counterfactual_reason_penalty)
                        counterfactual_penalty_reasons.append("reason:too_expensive")
                if "no_school" in tags_in_cluster and not bool(prop.get("is_school_district", False)):
                    counterfactual_penalty += float(counterfactual_reason_penalty)
                    counterfactual_penalty_reasons.append("reason:no_school")
                if "wrong_zone" in tags_in_cluster:
                    target_zone = str(getattr(pref, "target_zone", "") or "").upper()
                    if target_zone and zone_bucket != target_zone:
                        counterfactual_penalty += float(counterfactual_reason_penalty)
                        counterfactual_penalty_reasons.append("reason:wrong_zone")
                if "size_too_large" in tags_in_cluster:
                    rejected_areas = [
                        float(item.get("building_area", 0.0) or 0.0)
                        for item in rejected_cluster_feedback.get(diversity_cluster_key, [])
                        if float(item.get("building_area", 0.0) or 0.0) > 0.0
                    ]
                    if rejected_areas and area >= min(rejected_areas) * 0.95:
                        counterfactual_penalty += float(counterfactual_reason_penalty)
                        counterfactual_penalty_reasons.append("reason:size_too_large")
                if "size_too_small" in tags_in_cluster:
                    rejected_areas = [
                        float(item.get("building_area", 0.0) or 0.0)
                        for item in rejected_cluster_feedback.get(diversity_cluster_key, [])
                        if float(item.get("building_area", 0.0) or 0.0) > 0.0
                    ]
                    if rejected_areas and area <= max(rejected_areas) * 1.05:
                        counterfactual_penalty += float(counterfactual_reason_penalty)
                        counterfactual_penalty_reasons.append("reason:size_too_small")
        adjusted_score = (
            float(raw_score) * rule_weight
            + float(weighted_dim_score) * dimension_weight
            + float(persona_bonus)
            + float(substitute_spillover_bonus)
            + float(crowd_adjust)
            + float(unseen_discovery_bonus)
            - float(counterfactual_penalty)
            - _pressure_penalty(listing)
            + float(noise)
        )
        if bool(heat_state.get("is_fake_hot", False)):
            adjusted_score -= float(fake_hot_score_penalty)
        ranked.append(
            {
                "score": float(adjusted_score),
                "listing": listing,
                "key": _diversity_key(listing),
                "value_score": float(shortlist_value_score(listing)),
                "zone": _zone_of(listing),
                "price_band": _price_band(listing),
                "bucket_id": bucket_id,
                "heat_state": heat_state,
            }
        )
        scoring_map[pid] = {
            "composite_score": float(adjusted_score),
            "raw_rule_score": float(raw_score),
            "dimension_utility": float(weighted_dim_score),
            "dimension_scores": dim_scores,
            "weights": dict(preference_weights),
            "substitute_spillover_bonus": round(float(substitute_spillover_bonus), 4),
            "substitute_spillover_meta": dict(substitute_spillover_meta),
            "market_spillover_bonus": 0.0,
            "price_band": _price_band(listing),
                "zone": _zone_of(listing),
                "crowd_mode": str(crowd_mode),
                "crowd_pressure_units": round(float(_pressure_units(listing)), 4),
                "crowd_adjustment": round(float(crowd_adjust), 4),
                "historically_unseen": bool(is_historically_unseen),
                "unseen_discovery_bonus": round(float(unseen_discovery_bonus), 4),
                "counterfactual_penalty": round(float(counterfactual_penalty), 4),
                "counterfactual_penalty_reasons": list(dict.fromkeys(counterfactual_penalty_reasons)),
                "bucket_id": str(bucket_id),
                "candidate_bucket_key": str(_diversity_key(listing)),
                "diversity_cluster_key": str(diversity_cluster_key),
                "heat_state": dict(heat_state),
            }

    ranked.sort(key=lambda x: x["score"], reverse=True)
    if ranked:
        bucket_heat_groups: Dict[str, List[Dict[str, object]]] = {}
        for item in ranked:
            bucket_heat_groups.setdefault(str(item.get("bucket_id", "UNK_NOSCHOOL_JUST")), []).append(item)
        for bucket_items in bucket_heat_groups.values():
            has_fake_hot = any(bool((item.get("heat_state") or {}).get("is_fake_hot", False)) for item in bucket_items)
            has_cool_sibling = any(not bool((item.get("heat_state") or {}).get("is_fake_hot", False)) for item in bucket_items)
            if has_fake_hot and has_cool_sibling and sibling_rotation_bonus > 0.0:
                for item in bucket_items:
                    if not bool((item.get("heat_state") or {}).get("is_fake_hot", False)):
                        item["score"] = float(item.get("score", 0.0) or 0.0) + float(sibling_rotation_bonus)
                        try:
                            sibling_pid = int((item.get("listing") or {}).get("property_id", -1) or -1)
                        except Exception:
                            sibling_pid = -1
                        if sibling_pid > 0:
                            meta = scoring_map.get(sibling_pid, {})
                            prev_bonus = float(meta.get("market_spillover_bonus", 0.0) or 0.0)
                            meta["market_spillover_bonus"] = round(prev_bonus + float(sibling_rotation_bonus), 4)
                            scoring_map[sibling_pid] = meta
            anchor_true_heat = max(
                float((item.get("heat_state") or {}).get("real_competition_score", 0.0) or 0.0)
                for item in bucket_items
            )
            if anchor_true_heat >= float(true_competition_spillover_min_score) and true_competition_sibling_bonus > 0.0:
                for item in bucket_items:
                    heat_state = dict(item.get("heat_state", {}) or {})
                    if bool(heat_state.get("is_fake_hot", False)):
                        continue
                    item_true_heat = float(heat_state.get("real_competition_score", 0.0) or 0.0)
                    if item_true_heat >= anchor_true_heat - 0.02:
                        continue
                    cooling_headroom = max(0.0, anchor_true_heat - item_true_heat)
                    spillover_bonus = float(true_competition_sibling_bonus * min(1.0, cooling_headroom / 0.35))
                    if spillover_bonus <= 0.0:
                        continue
                    item["score"] = float(item.get("score", 0.0) or 0.0) + spillover_bonus
                    try:
                        sibling_pid = int((item.get("listing") or {}).get("property_id", -1) or -1)
                    except Exception:
                        sibling_pid = -1
                    if sibling_pid > 0:
                        meta = scoring_map.get(sibling_pid, {})
                        prev_bonus = float(meta.get("market_spillover_bonus", 0.0) or 0.0)
                        meta["market_spillover_bonus"] = round(prev_bonus + spillover_bonus, 4)
                        meta["market_spillover_anchor_heat"] = round(float(anchor_true_heat), 4)
                        scoring_map[sibling_pid] = meta
            bucket_items.sort(
                key=lambda item: (
                    1 if bool((item.get("heat_state") or {}).get("is_fake_hot", False)) and has_cool_sibling else 0,
                    -float(item.get("score", 0.0) or 0.0),
                )
            )
        ranked = [
            item
            for _bucket_id, items in sorted(
                bucket_heat_groups.items(),
                key=lambda kv: float(kv[1][0].get("score", 0.0) or 0.0) if kv[1] else -999.0,
                reverse=True,
            )
            for item in items
        ]
        ranked.sort(key=lambda x: float(x.get("score", 0.0) or 0.0), reverse=True)
    if ranked and int(shortlist_offset or 0) > 0:
        offset = int(shortlist_offset) % len(ranked)
        if offset > 0:
            ranked = ranked[offset:] + ranked[:offset]
    if agent is not None:
        setattr(agent, "_last_candidate_scoring_map", scoring_map)
    core_k = max(1, int(top_k))
    if not ranked:
        return _persist_pool_stats(
            selected_rows=[],
            ranked_size=0,
            stage1_pool_size=0,
            blocked_fake_hot_count=0,
            stage2_fill_size=0,
        )

    selected = []
    selected_ids = set()
    pool_limit = max(core_k * 4, 12)
    provisional_pool = ranked[:pool_limit]
    bucket_pool_map: Dict[str, List[Dict[str, object]]] = {}
    for item in provisional_pool:
        bucket_pool_map.setdefault(str(item.get("bucket_id", "UNK_NOSCHOOL_JUST")), []).append(item)
    pool = []
    blocked_fake_hot_items: List[Dict[str, object]] = []
    fake_hot_blocked_pool_count = 0
    for item in provisional_pool:
        heat_state = dict(item.get("heat_state", {}) or {})
        bucket_id = str(item.get("bucket_id", "UNK_NOSCHOOL_JUST"))
        siblings = bucket_pool_map.get(bucket_id, [])
        has_cool_sibling = any(
            not bool((sib.get("heat_state") or {}).get("is_fake_hot", False))
            and int((sib.get("listing") or {}).get("property_id", -1)) != int((item.get("listing") or {}).get("property_id", -1))
            for sib in siblings
        )
        if (
            bool(fake_hot_circuit_enabled)
            and bool(heat_state.get("is_fake_hot", False))
            and bool(has_cool_sibling)
        ):
            fake_hot_blocked_pool_count += 1
            blocked_fake_hot_items.append(item)
            continue
        pool.append(item)
    min_preserve_target = max(core_k, int(fake_hot_pool_preserve_min))
    if len(pool) < min_preserve_target:
        for item in blocked_fake_hot_items:
            pool.append(item)
            if len(pool) >= min_preserve_target:
                break
    if len(pool) < core_k:
        pool = list(provisional_pool)
    buckets = {}
    for item in pool:
        buckets.setdefault(item["key"], []).append(item)
    bucket_keys = sorted(
        buckets.keys(),
        key=lambda k: buckets[k][0]["score"] if buckets.get(k) else -999.0,
        reverse=True,
    )
    bucket_idx = {k: 0 for k in bucket_keys}

    def _append_from_item(item: Dict[str, object]) -> bool:
        listing = dict(item["listing"] or {})
        pid = int(listing.get("property_id", -1))
        if pid in selected_ids:
            return False
        cluster_key = str(item.get("key", "UNK:UNK"))
        if cluster_counts.get(cluster_key, 0) >= int(candidate_diversity_cluster_cap):
            return False
        listing["candidate_bucket_id"] = str(item.get("bucket_id", "UNK_NOSCHOOL_JUST"))
        listing["candidate_bucket_key"] = str(item.get("key", "UNK:UNK"))
        selected.append(listing)
        selected_ids.add(pid)
        cluster_counts[cluster_key] = int(cluster_counts.get(cluster_key, 0) + 1)
        return True

    cluster_counts: Dict[str, int] = {}

    if zone_min_slots > 0:
        zone_groups: Dict[str, List[Dict[str, object]]] = {}
        for item in pool:
            zone_groups.setdefault(str(item.get("zone", "UNK")), []).append(item)
        target_zone = str(getattr(pref, "target_zone", "") or "").upper()
        effective_target_zone_min_slots = int(target_zone_min_slots)
        if target_zone == "B":
            effective_target_zone_min_slots = max(effective_target_zone_min_slots, int(b_zone_shortlist_min_slots))
        if target_zone in zone_groups and effective_target_zone_min_slots > 0:
            quota = 0
            for item in zone_groups.get(target_zone, []):
                if len(selected) >= core_k:
                    break
                if _append_from_item(item):
                    quota += 1
                if quota >= effective_target_zone_min_slots:
                    break
        zone_order = sorted(
            zone_groups.keys(),
            key=lambda z: zone_groups[z][0]["score"] if zone_groups.get(z) else -999.0,
            reverse=True,
        )
        for zone_key in zone_order:
            quota = 0
            for item in zone_groups.get(zone_key, []):
                if len(selected) >= core_k:
                    break
                if _append_from_item(item):
                    quota += 1
                if quota >= zone_min_slots:
                    break

    if price_band_min_slots > 0 and len(selected) < core_k:
        band_groups: Dict[str, List[Dict[str, object]]] = {"LOW": [], "MID": [], "HIGH": []}
        for item in pool:
            band_groups.setdefault(str(item.get("price_band", "MID")), []).append(item)
        band_order = sorted(
            band_groups.keys(),
            key=lambda b: band_groups[b][0]["score"] if band_groups.get(b) else -999.0,
            reverse=True,
        )
        for band in band_order:
            quota = 0
            for item in band_groups.get(band, []):
                if len(selected) >= core_k:
                    break
                if _append_from_item(item):
                    quota += 1
                if quota >= price_band_min_slots:
                    break

    while len(selected) < core_k and bucket_keys:
        progressed = False
        for key in bucket_keys:
            idx = bucket_idx.get(key, 0)
            candidates_in_bucket = buckets.get(key, [])
            if idx >= len(candidates_in_bucket):
                continue
            bucket_idx[key] = idx + 1
            progressed = _append_from_item(candidates_in_bucket[idx]) or progressed
            if len(selected) >= core_k:
                break
        if not progressed:
            break

    if len(selected) < core_k:
        for item in ranked:
            _append_from_item(item)
            if len(selected) >= core_k:
                break
    if len(selected) < core_k:
        for item in ranked:
            listing = dict(item["listing"] or {})
            pid = int(listing.get("property_id", -1))
            if pid in selected_ids:
                continue
            listing["candidate_bucket_id"] = str(item.get("bucket_id", "UNK_NOSCHOOL_JUST"))
            listing["candidate_bucket_key"] = str(item.get("key", "UNK:UNK"))
            selected.append(listing)
            selected_ids.add(pid)
            if len(selected) >= core_k:
                break

    extra = max(0, int(exploration_slots))
    if candidate_two_stage_enabled:
        extra = min(int(extra), int(candidate_two_stage_max_stage2_fill))
    key_counter = {}
    for listing in selected:
        key_counter[_diversity_key(listing)] = int(key_counter.get(_diversity_key(listing), 0)) + 1
    dominant_ratio = (max(key_counter.values()) / max(1, len(selected))) if key_counter else 0.0
    if dominant_ratio >= 0.8:
        extra = min(4, extra + 1)
    if extra <= 0 or len(ranked) <= core_k:
        return _persist_pool_stats(
            selected_rows=selected,
            ranked_size=len(ranked),
            stage1_pool_size=len(pool),
            blocked_fake_hot_count=fake_hot_blocked_pool_count,
            stage2_fill_size=0,
        )

    remainder = []
    represented_keys = set(key_counter.keys())
    for item in ranked:
        listing = item["listing"]
        pid = int(listing.get("property_id", -1))
        if pid in selected_ids:
            continue
        key = item["key"]
        unseen_key_bonus = 0.12 if key not in represented_keys else 0.0
        value_score = float(item["value_score"])
        adjusted = value_score + unseen_key_bonus - _pressure_penalty(listing)
        remainder.append((adjusted, listing))

    remainder.sort(key=lambda x: x[0], reverse=True)
    for _, listing in remainder[:extra]:
        selected.append(listing)
        selected_ids.add(int(listing.get("property_id", -1)))

    selected_zones = {
        str(properties_map.get(int(x.get("property_id", -1)), {}).get("zone", "")).upper()
        for x in selected
        if x.get("property_id") is not None
    }
    need_school = bool(getattr(pref, "need_school_district", False))
    price_sensitive = float(getattr(pref, "price_sensitivity", 5) or 5)

    if len(selected_zones) == 1 and not need_school and price_sensitive >= 5:
        alternate = []
        primary_zone = next(iter(selected_zones)) if selected_zones else ""
        for item in ranked:
            listing = item["listing"]
            pid = int(listing.get("property_id", -1))
            if pid in selected_ids:
                continue
            prop = properties_map.get(pid, {})
            zone = str(prop.get("zone", "")).upper()
            if not zone or zone == primary_zone:
                continue
            alternate.append((shortlist_value_score(listing), listing))
        alternate.sort(key=lambda x: x[0], reverse=True)
        if alternate:
            selected.append(alternate[0][1])
    return _persist_pool_stats(
        selected_rows=selected,
        ranked_size=len(ranked),
        stage1_pool_size=len(pool),
        blocked_fake_hot_count=fake_hot_blocked_pool_count,
        stage2_fill_size=max(0, len(selected) - core_k),
    )


def compute_dynamic_preference_weights(
    decision_mode: str,
    market_trend: str,
    base_edu_weight: int,
    base_price_sensitivity: int,
    props_info: List[Dict],
) -> Dict[str, object]:
    """
    Dynamic preference re-weighting for smart buyers.
    - Normal path: unchanged
    - Smart path: combine market regime + developer discount signal
    """
    edu = max(0, min(10, int(base_edu_weight)))
    price = max(0, min(10, int(base_price_sensitivity)))
    hint_parts = []

    if decision_mode != "smart":
        return {
            "education_weight": edu,
            "price_sensitivity": price,
            "hint": "普通路径：保持原始权重。",
        }

    trend = str(market_trend or "STABLE").upper()
    # regime shift first
    if "DOWN" in trend or "CRASH" in trend:
        edu = max(1, edu - 1)
        price = min(10, price + 1)
        hint_parts.append("下行市场：教育执念-1，价格敏感+1。")
    elif "UP" in trend or "BOOM" in trend:
        edu = min(10, edu + 1)
        price = max(1, price - 1)
        hint_parts.append("上行市场：教育执念+1，价格敏感-1。")
    elif "VOLATILE" in trend or "PANIC" in trend:
        price = min(10, price + 1)
        hint_parts.append("波动市场：价格敏感+1。")
    else:
        hint_parts.append("平稳市场：保持基线。")

    # developer fire-sale signal second
    dev_prices = [float(c.get("price", 0.0) or 0.0) for c in props_info if str(c.get("is_developer", "")).startswith("Yes")]
    non_dev_prices = [float(c.get("price", 0.0) or 0.0) for c in props_info if str(c.get("is_developer", "")).startswith("No")]
    if dev_prices and non_dev_prices:
        avg_dev = sum(dev_prices) / len(dev_prices)
        avg_non_dev = sum(non_dev_prices) / len(non_dev_prices)
        if avg_non_dev > 0:
            discount_ratio = (avg_dev - avg_non_dev) / avg_non_dev
            relax = 2 if discount_ratio <= -0.20 else 1 if discount_ratio <= -0.10 else 0
            if relax > 0:
                edu = max(1, edu - relax)
                price = min(10, price + relax)
                hint_parts.append(
                    f"开发商折价{discount_ratio:+.1%}：教育执念-{relax}，价格敏感+{relax}。"
                )

    return {
        "education_weight": edu,
        "price_sensitivity": price,
        "hint": " ".join(hint_parts) if hint_parts else "保持原始权重。",
    }


def match_property_for_buyer(
    buyer: Agent,
    listings: List[Dict],
    properties_map: Dict[int, Dict],
    ignore_zone: bool = False,
    decision_mode: str = "normal",
    market_trend: str = "STABLE",
    config=None,
    excluded_property_ids: Optional[Set[int]] = None,
    retry_attempt: int = 0,
    top_k_boost: int = 0,
    crowd_hard_exclude_override: Optional[bool] = None,
) -> Optional[Dict]:
    """
    Find the best matching property for a buyer from active listings.
    listings: List of listing dicts (from property_listings table)
    properties_map: property_id -> property_data dict (full details)
    ignore_zone: If True, skip zone matching (for desperation fallback)
    """
    pref = buyer.preference
    candidates = []
    is_smart = (decision_mode == "smart")
    excluded_property_ids = set(int(x) for x in (excluded_property_ids or set()) if x is not None)
    current_match_month = int(getattr(buyer, "_current_matching_month", -1) or -1)
    crowd_profile = _resolve_crowd_behavior_profile(buyer, pref, config=config)
    crowd_mode = str(crowd_profile.get("mode", "neutral") or "neutral")

    # M12: target-price waiting gate (only for smart path).
    target_buy_price = float(getattr(buyer, "target_buy_price", getattr(pref, "target_buy_price", 0.0)) or 0.0)
    waited_months = int(getattr(buyer, "waited_months", 0) or 0)
    max_wait_months = int(getattr(buyer, "max_wait_months", getattr(pref, "max_wait_months", 6)) or 6)
    risk_mode = str(getattr(buyer, "risk_mode", getattr(pref, "risk_mode", "balanced"))).lower()
    strategy_profile = resolve_strategy_profile(decision_mode, risk_mode, market_trend)
    try:
        bargain_discount_trigger = float(
            config.get("smart_agent.bargain_discount_trigger", config.get("bargain_discount_trigger", 0.12))
        ) if config else 0.12
    except Exception:
        bargain_discount_trigger = 0.12
    try:
        configured_top_k = int(
            getattr(buyer, "_candidate_top_k_override", 0)
            or (config.get("smart_agent.candidate_top_k", 5) if config else 5)
        )
    except Exception:
        configured_top_k = 5
    try:
        exploration_slots = int(
            getattr(buyer, "_candidate_exploration_slots_override", 0)
            or (config.get("smart_agent.candidate_exploration_slots", 1) if config else 1)
        )
    except Exception:
        exploration_slots = 1
    configured_top_k = max(1, min(12, configured_top_k))
    visible_shortlist_cap = _resolve_buyer_match_visible_shortlist_cap(
        config=config,
        configured_top_k=configured_top_k,
    )
    force_full_visible_pool_enabled = True
    try:
        raw_force_full_visible_pool = (
            config.get(
                "smart_agent.candidate_force_full_visible_pool_enabled",
                config.get("candidate_force_full_visible_pool_enabled", True),
            )
            if config
            else True
        )
        if isinstance(raw_force_full_visible_pool, bool):
            force_full_visible_pool_enabled = raw_force_full_visible_pool
        else:
            force_full_visible_pool_enabled = str(raw_force_full_visible_pool).strip().lower() in {
                "1", "true", "yes", "y", "on"
            }
    except Exception:
        force_full_visible_pool_enabled = True
    try:
        shortlist_rotation_step = int(
            config.get(
                "smart_agent.candidate_shortlist_rotation_step",
                config.get("candidate_shortlist_rotation_step", max(1, configured_top_k)),
            )
        ) if config else max(1, configured_top_k)
    except Exception:
        shortlist_rotation_step = max(1, configured_top_k)
    shortlist_rotation_step = max(1, min(24, int(shortlist_rotation_step)))
    shortlist_offset = int(retry_attempt or 0) * int(shortlist_rotation_step) + int(len(excluded_property_ids))
    if int(top_k_boost or 0) > 0:
        configured_top_k = max(1, min(12, configured_top_k + int(top_k_boost)))
    exploration_slots = max(0, min(3, exploration_slots))
    setattr(buyer, "_last_strategy_profile", strategy_profile)
    risk_tolerance = {"conservative": 0.01, "balanced": 0.03, "aggressive": 0.06}.get(risk_mode, 0.03)
    strict_target_cap = target_buy_price * (1.0 + risk_tolerance) if target_buy_price > 0 else 0.0
    relaxed_target_cap = strict_target_cap
    if is_smart and target_buy_price > 0 and waited_months >= max_wait_months:
        overflow = waited_months - max_wait_months + 1
        relaxed_target_cap = target_buy_price * (1.0 + risk_tolerance + min(0.20, 0.02 * overflow))
        relaxed_target_cap = min(relaxed_target_cap, float(getattr(pref, "max_price", relaxed_target_cap)))

    # print(f"\n=== DEBUG Buyer {buyer.id} Matching ===")
    # print(f"Buyer Zone: {pref.target_zone}, Max Price: {pref.max_price:,.0f} (IgnoreZone={ignore_zone})")
    # print(f"Received {len(listings)} listings for matching")

    needs_school = bool(getattr(pref, 'need_school_district', False))
    school_urgency = int(getattr(buyer, "school_urgency", 0) or 0)
    education_path = str(getattr(getattr(buyer, "story", None), "education_path", "") or "").lower()
    purchase_motive = str(getattr(getattr(buyer, "story", None), "purchase_motive_primary", "") or "").lower()
    strict_school_requirement = bool(
        needs_school and (
            school_urgency >= 2
            or "public_school" in education_path
            or purchase_motive == "education_driven"
        )
    )
    waited_months = max(0, int(waited_months))
    max_wait_months = max(1, int(max_wait_months))
    deadline_progress = float(waited_months) / float(max_wait_months) if max_wait_months > 0 else 0.0
    buy_deadline_months_left = max(0, int(max_wait_months - waited_months))
    if deadline_progress >= 1.0:
        buy_deadline_stage = "overdue"
    elif deadline_progress >= 0.75:
        buy_deadline_stage = "late"
    elif deadline_progress >= 0.40:
        buy_deadline_stage = "mid"
    else:
        buy_deadline_stage = "early"
    force_zone_flexible = buy_deadline_stage in {"late", "overdue"}
    force_school_relax = buy_deadline_stage in {"late", "overdue"}
    factor_contract = _derive_five_factor_contract(
        buyer,
        pref,
        buy_deadline_stage=buy_deadline_stage,
        buy_deadline_months_left=buy_deadline_months_left,
        max_wait_months=max_wait_months,
    )

    def _bucket_id_for_listing(listing_data: Dict) -> str:
        try:
            pid = int(listing_data.get("property_id", -1))
        except Exception:
            pid = -1
        prop = properties_map.get(pid, {})
        zone_bucket = str(prop.get("zone", "") or "").upper() or "UNK"
        school_bucket = "SCHOOL" if bool(prop.get("is_school_district", False)) else "NOSCHOOL"
        type_bucket = _derive_property_type_bucket(prop)
        return f"{zone_bucket}_{school_bucket}_{type_bucket}"

    def _bucket_distribution(rows: List[Dict]) -> Dict[str, int]:
        dist: Dict[str, int] = {}
        for row in rows or []:
            bid = str(row.get("candidate_bucket_id", "") or "").strip() or _bucket_id_for_listing(row)
            dist[bid] = int(dist.get(bid, 0)) + 1
        return dist

    def _collect_candidates(relax_school_requirement: bool = False) -> List[Dict]:
        candidate_rows: List[Dict] = []
        for listing in listings:
            try:
                pid = int(listing.get("property_id", -1))
            except Exception:
                pid = -1
            if pid in excluded_property_ids:
                continue
            prop = properties_map.get(listing['property_id'])
            if not prop:
                continue
            owner_val = listing.get("owner_id", listing.get("seller_id", None))
            try:
                owner_id = int(owner_val) if owner_val is not None else None
            except Exception:
                owner_id = None
            if owner_id is not None and owner_id >= 0 and int(owner_id) == int(buyer.id):
                continue

            is_developer = (listing.get('owner_id') == -1)

            if not ignore_zone and not force_zone_flexible and pref.target_zone and prop['zone'] != pref.target_zone:
                if not is_developer:
                    continue

            if listing['listed_price'] > pref.max_price * 1.2:
                continue

            if is_smart and target_buy_price > 0:
                current_cap = strict_target_cap if waited_months < max_wait_months else relaxed_target_cap
                if listing['listed_price'] > current_cap:
                    continue

            min_beds = getattr(pref, 'min_bedrooms', 1)
            if not is_developer and prop.get('bedrooms', 999) < min_beds:
                continue

            if not is_developer and needs_school and not force_school_relax and not prop.get('is_school_district', False):
                if not relax_school_requirement:
                    continue

            candidate_rows.append(listing)
        return candidate_rows

    def _collect_visible_for_sale() -> List[Dict]:
        visible_rows: List[Dict] = []
        for listing in listings:
            try:
                pid = int(listing.get("property_id", -1))
            except Exception:
                pid = -1
            if pid in excluded_property_ids:
                continue
            prop = properties_map.get(listing.get('property_id'))
            if not prop:
                continue
            owner_val = listing.get("owner_id", listing.get("seller_id", None))
            try:
                owner_id = int(owner_val) if owner_val is not None else None
            except Exception:
                owner_id = None
            if owner_id is not None and owner_id >= 0 and int(owner_id) == int(buyer.id):
                continue
            visible_rows.append(listing)
        return visible_rows

    def _diagnose_no_candidate_filters() -> Dict[str, int]:
        stats = {
            "visible_pool": 0,
            "excluded_self_or_hidden": 0,
            "blocked_zone": 0,
            "blocked_price_soft_cap": 0,
            "blocked_target_cap": 0,
            "blocked_bedrooms": 0,
            "blocked_school": 0,
            "passed_all_hard_filters": 0,
        }
        current_cap = strict_target_cap if waited_months < max_wait_months else relaxed_target_cap
        min_beds = getattr(pref, "min_bedrooms", 1)
        for listing in listings:
            try:
                pid = int(listing.get("property_id", -1))
            except Exception:
                pid = -1
            if pid in excluded_property_ids:
                stats["excluded_self_or_hidden"] += 1
                continue
            prop = properties_map.get(listing.get("property_id"))
            if not prop:
                stats["excluded_self_or_hidden"] += 1
                continue
            owner_val = listing.get("owner_id", listing.get("seller_id", None))
            try:
                owner_id = int(owner_val) if owner_val is not None else None
            except Exception:
                owner_id = None
            if owner_id is not None and owner_id >= 0 and int(owner_id) == int(buyer.id):
                stats["excluded_self_or_hidden"] += 1
                continue

            stats["visible_pool"] += 1
            is_developer = (listing.get("owner_id") == -1)

            if not ignore_zone and not force_zone_flexible and pref.target_zone and prop["zone"] != pref.target_zone:
                if not is_developer:
                    stats["blocked_zone"] += 1
                    continue

            if listing["listed_price"] > pref.max_price * 1.2:
                stats["blocked_price_soft_cap"] += 1
                continue

            if is_smart and target_buy_price > 0 and listing["listed_price"] > current_cap:
                stats["blocked_target_cap"] += 1
                continue

            if not is_developer and prop.get("bedrooms", 999) < min_beds:
                stats["blocked_bedrooms"] += 1
                continue

            if not is_developer and needs_school and not force_school_relax and not prop.get("is_school_district", False):
                stats["blocked_school"] += 1
                continue

            stats["passed_all_hard_filters"] += 1
        return stats

    visible_for_sale_pool = _collect_visible_for_sale()
    candidates = _collect_candidates(relax_school_requirement=False)
    school_requirement_relaxed = False
    if not candidates and needs_school and (not strict_school_requirement or force_school_relax):
        soft_relax_enabled = True
        if config is not None:
            raw_soft_relax = config.get("smart_agent.candidate_soft_relax_school_enabled", True)
            if isinstance(raw_soft_relax, bool):
                soft_relax_enabled = raw_soft_relax
            else:
                soft_relax_enabled = str(raw_soft_relax).strip().lower() in {"1", "true", "yes", "y", "on"}
        if soft_relax_enabled:
            candidates = _collect_candidates(relax_school_requirement=True)
            school_requirement_relaxed = bool(candidates)

    if not candidates:
        no_candidate_stats = _diagnose_no_candidate_filters()
        buyer._last_buyer_match_context = {
            "matching_month": int(current_match_month),
            "strategy_profile": str(strategy_profile),
            "market_trend": str(market_trend),
            "visible_for_sale_count": int(len(visible_for_sale_pool)),
            "visible_for_sale_property_ids": [int(x.get("property_id", -1)) for x in visible_for_sale_pool if x.get("property_id") is not None][:200],
            "eligible_candidate_count": int(len(candidates)),
            "shortlist_property_ids": [],
            "selected_property_id": None,
            "selection_reason": "no_candidates_after_soft_relax" if (needs_school and (not strict_school_requirement or force_school_relax)) else "no_candidates_after_hard_filters",
            "selection_reason_tags": ["route:no_candidates"],
            "no_candidate_filter_counts": no_candidate_stats,
            "no_candidate_primary_blockers": [
                key
                for key, _value in sorted(
                    (
                        (k, v)
                        for k, v in no_candidate_stats.items()
                        if k.startswith("blocked_") and int(v or 0) > 0
                    ),
                    key=lambda item: (-int(item[1]), item[0]),
                )[:3]
            ],
            "llm_monthly_intent": "NO_CANDIDATE",
            "stop_search_this_month": False,
            "llm_route_model": "none",
            "llm_route_reason": "no_candidates",
            "llm_gray_score": 0.0,
            "llm_called": False,
            "school_requirement_relaxed": bool(school_requirement_relaxed),
            "buy_deadline_stage": str(buy_deadline_stage),
            "buy_deadline_months_left": int(buy_deadline_months_left),
            "buy_deadline_total_months": int(max_wait_months),
            "crowd_mode": str(crowd_mode),
            "crowd_profile_reasons": list(crowd_profile.get("reasons", []) or []),
            "retry_attempt": int(retry_attempt),
            "excluded_property_count": int(len(excluded_property_ids)),
            "factor_contract": factor_contract,
            "bucket_plan": {
                "mode": "EMPTY",
                "main_quota": 0,
                "secondary_quota": 0,
                "explore_quota": 0,
                "total_shortlist_size": 0,
            },
            "bucket_distribution": {},
            "pipeline_stage_trace": [
                "collect_visible_pool",
                "apply_hard_filters",
                "no_candidates_exit",
            ],
        }
        if is_smart and target_buy_price > 0:
            setattr(buyer, "_blocked_by_target_price", True)
        return None
    setattr(buyer, "_blocked_by_target_price", False)

    # 5. M10 strategy shortlist: keep both baseline list and crowd-aware list.
    baseline_shortlist = build_candidate_shortlist(
        candidates=candidates,
        properties_map=properties_map,
        agent=buyer,
        pref=pref,
        strategy_profile=strategy_profile,
        top_k=configured_top_k,
        exploration_slots=exploration_slots,
        bargain_discount_trigger=bargain_discount_trigger,
        config=config,
        crowd_mode="neutral",
        crowd_avoid_step=0.0,
        crowd_follow_step=0.0,
        pressure_scale=0.0,
        shortlist_offset=shortlist_offset,
    )
    shortlist = build_candidate_shortlist(
        candidates=candidates,
        properties_map=properties_map,
        agent=buyer,
        pref=pref,
        strategy_profile=strategy_profile,
        top_k=configured_top_k,
        exploration_slots=exploration_slots,
        bargain_discount_trigger=bargain_discount_trigger,
        config=config,
        crowd_mode=crowd_mode,
        crowd_avoid_step=float(crowd_profile.get("avoid_step", 0.05) or 0.05),
        crowd_follow_step=float(crowd_profile.get("follow_step", 0.03) or 0.03),
        pressure_scale=float(crowd_profile.get("pressure_scale", 1.0) or 1.0),
        shortlist_offset=shortlist_offset,
    )

    pressure_map_for_prompt = getattr(buyer, "_candidate_pressure_map", {}) or {}
    crowd_tolerance_units = float(crowd_profile.get("tolerance_units", 2.8) or 2.8)
    precheck_reselect_enabled = bool(crowd_profile.get("precheck_reselect_enabled", True))
    precheck_max_reselect_rounds = int(crowd_profile.get("precheck_max_reselect_rounds", 1) or 1)

    def _crowd_level(pressure_units: float) -> str:
        units = float(max(0.0, pressure_units))
        if units >= 4.0:
            return "HIGH"
        if units >= 2.0:
            return "MID"
        return "LOW"

    def _estimate_overlap_buyers(pressure_units: float) -> int:
        units = float(max(0.0, pressure_units))
        return max(1, int(round(units)) + 1)

    def _crowd_meta_for_listing(listing_data: Dict) -> Dict[str, object]:
        try:
            units = float(pressure_map_for_prompt.get(int(listing_data.get("property_id", -1)), 0.0) or 0.0)
        except Exception:
            units = 0.0
        est_overlap = _estimate_overlap_buyers(units)
        return {
            "pressure_units": round(float(units), 2),
            "crowd_level": _crowd_level(units),
            "estimated_overlap_buyers": int(est_overlap),
            "within_tolerance": bool(units <= crowd_tolerance_units + 1e-9),
        }

    # Hard rule (requested): listings above crowd tolerance are directly excluded
    # from LLM decision set. LLM only decides among allowed listings.
    if crowd_hard_exclude_override is not None:
        crowd_hard_exclude_enabled = bool(crowd_hard_exclude_override)
    else:
        try:
            raw_hard_exclude = (
                config.get(
                    "smart_agent.candidate_crowd_hard_exclude_enabled",
                    config.get("candidate_crowd_hard_exclude_enabled", True),
                )
                if config
                else True
            )
            if isinstance(raw_hard_exclude, bool):
                crowd_hard_exclude_enabled = raw_hard_exclude
            else:
                crowd_hard_exclude_enabled = str(raw_hard_exclude).strip().lower() in {"1", "true", "yes", "y", "on"}
        except Exception:
            crowd_hard_exclude_enabled = True

    raw_shortlist = list(shortlist)
    shortlist_filtered_out_count = 0
    crowd_floor_min_keep = 1
    crowd_floor_fallback_applied = False
    crowd_floor_fallback_added = 0
    try:
        crowd_floor_min_keep = int(
            config.get(
                "smart_agent.candidate_crowd_min_shortlist_keep",
                config.get("candidate_crowd_min_shortlist_keep", 1),
            )
        ) if config else 1
    except Exception:
        crowd_floor_min_keep = 1
    crowd_floor_min_keep = max(1, min(4, int(crowd_floor_min_keep)))
    if crowd_hard_exclude_enabled:
        shortlist_under_threshold = [
            c for c in raw_shortlist
            if bool(_crowd_meta_for_listing(c).get("within_tolerance", True))
        ]
        shortlist_filtered_out_count = max(0, len(raw_shortlist) - len(shortlist_under_threshold))
        shortlist = list(shortlist_under_threshold)

        # 机制固定：拥挤约束用于降权，不允许把候选集合一刀切清空。
        # 若硬剔除后不足最小候选数，则从原 shortlist 中按“拥挤最轻”回补到保底数。
        if raw_shortlist and len(shortlist) < crowd_floor_min_keep:
            existing_ids = {
                int(c.get("property_id"))
                for c in shortlist
                if c.get("property_id") is not None
            }
            candidates_for_floor = []
            for c in raw_shortlist:
                try:
                    pid = int(c.get("property_id"))
                except Exception:
                    continue
                if pid in existing_ids:
                    continue
                crowd_units = float(_crowd_meta_for_listing(c).get("pressure_units", 0.0) or 0.0)
                candidates_for_floor.append((crowd_units, pid, c))
            candidates_for_floor.sort(key=lambda x: (float(x[0]), int(x[1])))
            needed = max(0, int(crowd_floor_min_keep) - int(len(shortlist)))
            topups = [item[2] for item in candidates_for_floor[:needed]]
            if topups:
                shortlist.extend(topups)
                crowd_floor_fallback_applied = True
                crowd_floor_fallback_added = int(len(topups))

        if not shortlist:
            buyer._last_buyer_match_context = {
                "matching_month": int(current_match_month),
                "strategy_profile": str(strategy_profile),
                "market_trend": str(market_trend),
                "visible_for_sale_count": int(len(visible_for_sale_pool)),
                "visible_for_sale_property_ids": [
                    int(x.get("property_id", -1))
                    for x in visible_for_sale_pool
                    if x.get("property_id") is not None
                ][:200],
                "eligible_candidate_count": int(len(candidates)),
                "shortlist_property_ids": [],
                "shortlist_raw_property_ids": [int(x.get("property_id", -1)) for x in raw_shortlist if x.get("property_id") is not None],
                "shortlist_raw_size": int(len(raw_shortlist)),
                "crowd_hard_filtered_out_count": int(shortlist_filtered_out_count),
                "selected_property_id": None,
                "selection_reason": "all_shortlist_over_crowd_tolerance",
                "selection_reason_tags": ["route:crowd_hard_excluded_all"],
                "llm_monthly_intent": "CONTINUE",
                "stop_search_this_month": False,
                "llm_route_model": "none",
                "llm_route_reason": "crowd_hard_excluded_all",
                "llm_gray_score": 0.0,
                "llm_called": False,
                "school_requirement_relaxed": bool(school_requirement_relaxed),
                "buy_deadline_stage": str(buy_deadline_stage),
                "buy_deadline_months_left": int(buy_deadline_months_left),
                "buy_deadline_total_months": int(max_wait_months),
                "crowd_hard_exclude_enabled": bool(crowd_hard_exclude_enabled),
                "crowd_floor_min_keep": int(crowd_floor_min_keep),
                "crowd_floor_fallback_applied": bool(crowd_floor_fallback_applied),
                "crowd_floor_fallback_added": int(crowd_floor_fallback_added),
                "crowd_mode": str(crowd_mode),
                "crowd_profile_reasons": list(crowd_profile.get("reasons", []) or [])[:3],
                "retry_attempt": int(retry_attempt),
                "excluded_property_count": int(len(excluded_property_ids)),
                "factor_contract": factor_contract,
                "bucket_plan": {
                    "mode": "ALL_EXCLUDED_BY_CROWD",
                    "main_quota": 0,
                    "secondary_quota": 0,
                    "explore_quota": 0,
                    "total_shortlist_size": 0,
                },
                "bucket_distribution": _bucket_distribution(raw_shortlist),
                "pipeline_stage_trace": [
                    "collect_visible_pool",
                    "apply_hard_filters",
                    "rank_shortlist",
                    "apply_crowd_hard_exclude",
                    "crowd_excluded_all_exit",
                ],
            }
            return None

    full_baseline_shortlist = list(baseline_shortlist)
    full_shortlist = list(shortlist)
    if int(visible_shortlist_cap) > 0:
        shortlist = list(full_shortlist[:visible_shortlist_cap])
    visible_shortlist_ids = [
        int(c.get("property_id", -1))
        for c in shortlist
        if c.get("property_id") is not None
    ]
    full_shortlist_ids = [
        int(c.get("property_id", -1))
        for c in full_shortlist
        if c.get("property_id") is not None
    ]
    reused_stop_ctx = _maybe_reuse_same_month_stop_signal(
        buyer=buyer,
        current_match_month=current_match_month,
        retry_attempt=retry_attempt,
        full_shortlist_ids=full_shortlist_ids,
        visible_shortlist_ids=visible_shortlist_ids,
        strategy_profile=strategy_profile,
        market_trend=market_trend,
        crowd_mode=crowd_mode,
        config=config,
    )
    if reused_stop_ctx:
        buyer._last_buyer_match_context = reused_stop_ctx
        logger.info(
            "💡 [买方初筛复用] 买家 %s(%s) 复用本月已明确的停搜决定，不再重复询问相似房单。",
            buyer.name,
            buyer.id,
        )
        return None

    candidate_scoring_map = getattr(buyer, "_last_candidate_scoring_map", {}) or {}
    candidate_pool_stats = getattr(buyer, "_last_candidate_pool_stats", {}) or {}
    fake_hot_circuit_enabled = bool(candidate_pool_stats.get("fake_hot_circuit_enabled", False))
    fake_hot_blocked_pool_count = int(candidate_pool_stats.get("fake_hot_blocked_pool_count", 0) or 0)
    candidate_two_stage_enabled = bool(candidate_pool_stats.get("candidate_two_stage_enabled", False))
    candidate_stage1_pool_size = int(candidate_pool_stats.get("stage1_pool_size", 0) or 0)
    candidate_stage2_fill_size = int(candidate_pool_stats.get("stage2_fill_size", 0) or 0)

    # helper to format prop for prompt
    def format_prop(listing_data):
        p = properties_map.get(listing_data['property_id'])
        crowd_meta = _crowd_meta_for_listing(listing_data)
        score_meta = candidate_scoring_map.get(int(listing_data.get("property_id", -1)), {})
        return {
            "id": listing_data['property_id'],
            "zone": p['zone'],
            "area": p['building_area'],
            "build_year": p.get('build_year'),
            "listing_age_months": int(listing_data.get('listing_age_months', 0) or 0),
            "price": listing_data['listed_price'],
            "school": "Yes" if p.get('is_school_district') else "No",
            "type": p.get('property_type', 'N/A'),
            "is_developer": "Yes (新房/特价)" if listing_data.get('owner_id') == -1 else "No (二手)",
            "crowd_pressure_units": float(crowd_meta["pressure_units"]),
            "crowd_level": str(crowd_meta["crowd_level"]),
            "estimated_overlap_buyers": int(crowd_meta["estimated_overlap_buyers"]),
            "within_crowd_tolerance": bool(crowd_meta["within_tolerance"]),
            "diversity_cluster_key": str(score_meta.get("diversity_cluster_key", "")),
        }

    props_info = [format_prop(c) for c in shortlist]
    baseline_props_info = [format_prop(c) for c in full_baseline_shortlist]
    shortlist_context = []
    for c in shortlist:
        pid = int(c.get("property_id", -1))
        prop = properties_map.get(pid, {})
        persona_bonus, persona_reasons = _persona_shortlist_adjustment(buyer, pref, c, prop)
        score_meta = candidate_scoring_map.get(pid, {})
        shortlist_context.append(
            {
                "property_id": pid,
                "zone": str(prop.get("zone", "")),
                "listed_price": float(c.get("listed_price", 0.0) or 0.0),
                "is_school_district": bool(prop.get("is_school_district", False)),
                "persona_bonus": round(float(persona_bonus), 4),
                "persona_reasons": persona_reasons[:3],
                "candidate_bucket_id": str(c.get("candidate_bucket_id", "") or _bucket_id_for_listing(c)),
                "candidate_bucket_key": str(c.get("candidate_bucket_key", "")),
                "dimension_scores": score_meta.get("dimension_scores", {}),
                "score_components": {
                    "raw_rule_score": round(float(score_meta.get("raw_rule_score", 0.0) or 0.0), 4),
                    "dimension_utility": round(float(score_meta.get("dimension_utility", 0.0) or 0.0), 4),
                    "substitute_spillover_bonus": round(float(score_meta.get("substitute_spillover_bonus", 0.0) or 0.0), 4),
                    "market_spillover_bonus": round(float(score_meta.get("market_spillover_bonus", 0.0) or 0.0), 4),
                    "composite_score": round(float(score_meta.get("composite_score", 0.0) or 0.0), 4),
                },
                "heat_state": score_meta.get("heat_state", {}),
                "substitute_spillover_meta": score_meta.get("substitute_spillover_meta", {}),
                "bucket_id": str(score_meta.get("bucket_id", c.get("candidate_bucket_id", "") or _bucket_id_for_listing(c))),
                "diversity_cluster_key": str(score_meta.get("diversity_cluster_key", "")),
                "crowd_pressure_units": float(_crowd_meta_for_listing(c).get("pressure_units", 0.0)),
                "crowd_level": str(_crowd_meta_for_listing(c).get("crowd_level", "LOW")),
                "estimated_overlap_buyers": int(_crowd_meta_for_listing(c).get("estimated_overlap_buyers", 1)),
                "within_crowd_tolerance": bool(_crowd_meta_for_listing(c).get("within_tolerance", True)),
            }
        )

    shortlist_bucket_distribution = _bucket_distribution(shortlist)
    shortlist_size = int(len(shortlist))
    shortlist_full_size = int(len(full_shortlist))
    main_quota = min(3, shortlist_size)
    secondary_quota = min(2, max(0, shortlist_size - main_quota))
    explore_quota = max(0, shortlist_size - main_quota - secondary_quota)
    pipeline_stage_trace = [
        "collect_visible_pool",
        "apply_hard_filters",
        "rank_shortlist",
        "apply_crowd_hard_exclude",
        "llm_decision",
        "post_select_guard",
    ]

    # Dynamic re-weight under market regime + developer discount (smart path only)
    base_edu_weight = int(getattr(pref, 'education_weight', 5))
    base_price_sensitivity = int(getattr(pref, 'price_sensitivity', 5))
    dynamic_weights = compute_dynamic_preference_weights(
        decision_mode=decision_mode,
        market_trend=market_trend,
        base_edu_weight=base_edu_weight,
        base_price_sensitivity=base_price_sensitivity,
        props_info=props_info,
    )
    effective_edu_weight = int(dynamic_weights["education_weight"])
    effective_price_sensitivity = int(dynamic_weights["price_sensitivity"])
    dynamic_hint = str(dynamic_weights["hint"])
    setattr(
        buyer,
        "_last_dynamic_weights",
        {
            "base_education_weight": base_edu_weight,
            "effective_education_weight": effective_edu_weight,
            "base_price_sensitivity": base_price_sensitivity,
            "effective_price_sensitivity": effective_price_sensitivity,
            "market_trend": market_trend,
            "strategy_profile": strategy_profile,
            "dynamic_hint": dynamic_hint,
        },
    )

    if getattr(pref, 'need_school_district', False):
        if effective_edu_weight >= 8:
            school_pref_text = "强烈偏好学区，但在极端价差下可有限妥协。"
        elif effective_edu_weight >= 5:
            school_pref_text = "偏好学区，可在高性价比情境下权衡取舍。"
        else:
            school_pref_text = "轻度偏好学区，价格优势可优先。"
    else:
        school_pref_text = "无学区偏好。"

    decision_profile = "聪明策略" if is_smart else "普通策略"
    profile_hint = (
        "你可以在显著折价时更积极权衡学区与价格。"
        if is_smart
        else "你保持常规谨慎，不做激进扫货。"
    )

    retry_hint = (
        f"这是你本月第 {int(retry_attempt) + 1} 次选房尝试。"
        if int(retry_attempt or 0) > 0
        else "这是你本月首次选房。"
    )
    if buy_deadline_stage == "early":
        deadline_hint = "早期阶段：可按原偏好慢慢挑选。"
    elif buy_deadline_stage == "mid":
        deadline_hint = "中期阶段：建议适度放宽非核心偏好，提升成交概率。"
    elif buy_deadline_stage == "late":
        deadline_hint = "临近到期：请以成交为优先，可明显放宽学区/区域执念。"
    else:
        deadline_hint = "已超期：本月应以买到房为第一目标，优先成交。"
    visible_for_sale_count = int(len(visible_for_sale_pool))
    eligible_candidate_count = int(len(candidates))
    prompt = f"""
    你是买家 {buyer.name} (ID:{buyer.id})。
    【决策路径】{decision_profile}
    【策略库命中】{strategy_profile}
    【市场状态】{market_trend}
    【核心需求】{buyer.story.housing_need}
    【预算上限】{pref.max_price / 10000:.0f}万
    【基础偏好】核心区域: {pref.target_zone}, 学区倾向: {school_pref_text}
    【市场线索】挂牌越久通常议价空间越大，请把挂牌时长纳入考虑。
    【在售可见池】当前你可见到的在售房源共 {visible_for_sale_count} 套（平台全量可见）。
    【本轮可行动候选】满足你基本条件的候选共 {eligible_candidate_count} 套。
    【本轮状态】{retry_hint}
    【交易协议（买家执行版，必须遵守）】
    - 本系统按“单套房”逐个处理，先到先得，不做月底统一分房。
    - 同一套房同一个月最多只有 1 个有效待交割主订单（pending_settlement）。
    - 若本轮落败/失败，系统会让你继续尝试下一套（直到候选耗尽或达到重试上限）。
    - 下单前和交割前都会做资金/DTI/费用校验，不达标不会成交。
    - 你现在只需为本轮选择 1 套，不要假设月底会重新统一分配。
    【8桶候选机制（计划书35.29.3）】
    - 候选按 8 桶组织：A/B × 学区/非学区 × 刚需/改善。
    - 本轮桶分布（候选集）: {json.dumps(shortlist_bucket_distribution, ensure_ascii=False)}
    - 本轮配额（主/次/探索）: {main_quota}/{secondary_quota}/{explore_quota}
    - 失败后的放松顺序固定：房型 -> 区位 -> 学区；每轮只放松一个因素。
    - 资金与时间约束永不放松。
    【购房期限】总窗口 {max_wait_months} 个月，已等待 {waited_months} 个月，剩余 {buy_deadline_months_left} 个月，阶段={buy_deadline_stage}
    【期限提示】{deadline_hint}
    【拥挤偏好】{crowd_mode}；原因: {", ".join(list(crowd_profile.get("reasons", []) or [])[:3])}
    【拥挤容忍阈值】{crowd_tolerance_units:.2f}（候选里 crowd_pressure_units 超过阈值说明竞争更激烈）
    【硬规则】超出拥挤阈值的房源已被系统剔除，你不能选择它们。
    
    【深层效用权重 (0-10分)】(决定你的妥协尺度)
    - 教育执念 (education_weight): {effective_edu_weight} (原始:{base_edu_weight})
    - 舒适度执念 (comfort_weight): {getattr(pref, 'comfort_weight', 5)}
    - 价格敏感度 (price_sensitivity): {effective_price_sensitivity} (原始:{base_price_sensitivity})
    - 动态情境: {dynamic_hint}
    - 路径提示: {profile_hint}

    【原始Top候选】(不考虑拥挤度，只看基本匹配)
    {json.dumps(baseline_props_info[:3], indent=2, ensure_ascii=False)}

    【拥挤修正后Top候选】(考虑当前拥挤度后的排序)
    {json.dumps(props_info, indent=2, ensure_ascii=False)}

    请结合你的【深层效用权重】和【拥挤偏好】，在“高溢价的学区房”与“超低价的非学区房/特价房”之间做出最符合你偏好的权衡。
    如果你看到某套房太拥挤，也可以主动转去次优房源；如果你愿意追热门，也可以保留热门选择。
    若你坚持选择超过“拥挤容忍阈值”的房子，请在 reason 里说明坚持原因。
    如果不满意，可以不选 (null)。
    输出JSON: {{
        "selected_property_id": int|null,
        "backup_property_ids": [int, ...],
        "rejected_property_feedback": [
            {{"property_id": int, "reason_tag": "too_expensive|no_school|wrong_zone|size_too_large|size_too_small|inferior_same_cluster", "reason": "..."}} 
        ],
        "thought_bubble": "1-2句内心独白，体现你在价格、学区等权重间的挣扎与最终妥协，用于在大屏气泡显示",
        "reason": "...",
        "monthly_intent": "CONTINUE"|"STOP"
    }}
    """

    # Conservative fallback: abstain instead of forcing cheapest when LLM output is invalid.
    default_resp = {
        "selected_property_id": None,
        "backup_property_ids": [],
        "rejected_property_feedback": [],
        "thought_bubble": "信息不足，先观望。",
        "reason": "Default no-pick",
        "monthly_intent": "CONTINUE",
    }
    def _resolve_buyer_match_route() -> tuple[str, str, float]:
        """
        Route only chooses model (fast/smart), never substitutes LLM decision.
        """
        if not is_smart:
            return "fast", "normal_profile_fast_path", 0.0

        enabled_raw = True
        if config is not None:
            try:
                enabled_raw = config.get("smart_agent.buyer_match_dual_routing_enabled", True)
            except Exception:
                enabled_raw = True
        enabled = bool(enabled_raw) if isinstance(enabled_raw, bool) else str(enabled_raw).strip().lower() in {
            "1", "true", "yes", "y", "on"
        }
        if not enabled:
            return "smart", "dual_routing_disabled", 1.0

        prices = [float(c.get("listed_price", 0.0) or 0.0) for c in shortlist]
        if not prices:
            return "fast", "empty_shortlist", 0.0
        p_min = max(1.0, min(prices))
        p_max = max(prices)
        p_span = (p_max - p_min) / p_min if p_min > 0 else 0.0
        budget = float(getattr(pref, "max_price", 0.0) or 0.0)
        budget_gap = 1.0 if budget <= 0 else abs(((sum(prices) / len(prices)) - budget) / max(1.0, budget))

        school_vals = {int(bool(properties_map.get(int(c.get("property_id", -1)), {}).get("is_school_district", False))) for c in shortlist}
        zone_vals = {str(properties_map.get(int(c.get("property_id", -1)), {}).get("zone", "")) for c in shortlist}
        school_need = bool(getattr(pref, "need_school_district", False))
        school_mixed = 1.0 if school_need and len(school_vals) > 1 else 0.0
        zone_mixed = 1.0 if len(zone_vals) > 1 else 0.0
        try:
            small_clear_case_cap = int(
                config.get(
                    "smart_agent.buyer_match_small_clear_case_cap",
                    config.get("buyer_match_small_clear_case_cap", 4),
                )
            ) if config else 4
        except Exception:
            small_clear_case_cap = 4
        try:
            small_clear_case_price_span_cap = float(
                config.get(
                    "smart_agent.buyer_match_small_clear_case_price_span_cap",
                    config.get("buyer_match_small_clear_case_price_span_cap", 0.08),
                )
            ) if config else 0.08
        except Exception:
            small_clear_case_price_span_cap = 0.08
        try:
            small_clear_case_budget_gap_cap = float(
                config.get(
                    "smart_agent.buyer_match_small_clear_case_budget_gap_cap",
                    config.get("buyer_match_small_clear_case_budget_gap_cap", 0.10),
                )
            ) if config else 0.10
        except Exception:
            small_clear_case_budget_gap_cap = 0.10
        small_clear_case_cap = max(2, min(5, int(small_clear_case_cap)))
        small_clear_case_price_span_cap = max(0.01, min(0.20, float(small_clear_case_price_span_cap)))
        small_clear_case_budget_gap_cap = max(0.01, min(0.25, float(small_clear_case_budget_gap_cap)))
        if (
            len(shortlist) <= int(small_clear_case_cap)
            and len(zone_vals) <= 1
            and (not school_need or len(school_vals) <= 1)
            and p_span <= float(small_clear_case_price_span_cap)
            and budget_gap <= float(small_clear_case_budget_gap_cap)
        ):
            return "fast", "small_homogeneous_shortlist", 0.0
        trend_uncertainty = 1.0 if str(market_trend or "").upper() in {"STABLE", "DOWN"} else 0.4
        shortlist_complexity = min(1.0, max(0.0, (len(shortlist) - 1) / 4.0))

        gray_score = (
            0.28 * min(1.0, p_span / 0.25)
            + 0.24 * min(1.0, budget_gap / 0.18)
            + 0.20 * school_mixed
            + 0.12 * zone_mixed
            + 0.10 * trend_uncertainty
            + 0.06 * shortlist_complexity
        )
        threshold = 0.55
        if config is not None:
            try:
                threshold = float(config.get("smart_agent.buyer_match_gray_score_threshold", 0.55))
            except Exception:
                threshold = 0.55
        threshold = max(0.1, min(0.95, threshold))

        if gray_score >= threshold:
            return "smart", f"gray_zone score={gray_score:.3f} threshold={threshold:.3f}", float(gray_score)
        return "fast", f"clear_case score={gray_score:.3f} threshold={threshold:.3f}", float(gray_score)

    route_model, route_reason, gray_score = _resolve_buyer_match_route()
    llm_called_flag = True
    mock_mode = str(os.getenv("LLM_MOCK_MODE", "false")).strip().lower() in {"1", "true", "yes", "y", "on"}
    mock_stub_select_enabled = False
    if config is not None:
        try:
            mock_stub_select_enabled = _to_bool(
                config.get(
                    "smart_agent.mock_stub_select_enabled",
                    config.get("mock_stub_select_enabled", False),
                ),
                False,
            )
        except Exception:
            mock_stub_select_enabled = False
    if mock_mode and mock_stub_select_enabled and shortlist:
        # Deterministic no-LLM stub for structure/funnel experiments:
        # pick a stable low-crowd + value candidate from shortlist.
        def _stub_rank(item: Dict) -> Tuple[float, float, float, int]:
            crowd_meta = _crowd_meta_for_listing(item)
            pressure = float(crowd_meta.get("pressure_units", 0.0) or 0.0)
            try:
                listed_price = float(item.get("listed_price", 0.0) or 0.0)
            except Exception:
                listed_price = 0.0
            try:
                bedrooms = float(item.get("bedrooms", 0.0) or 0.0)
            except Exception:
                bedrooms = 0.0
            return (-pressure, bedrooms, -listed_price, -int(item.get("property_id", -1) or -1))

        chosen_stub = sorted(list(shortlist), key=_stub_rank, reverse=True)[0]
        result = {
            "selected_property_id": int(chosen_stub.get("property_id", -1)),
            "thought_bubble": "规则桩决策：优先低拥挤且匹配度高的候选。",
            "reason": "Mock stub: deterministic shortlist selection",
            "monthly_intent": "CONTINUE",
        }
        route_model = "stub"
        route_reason = "mock_stub_select_enabled"
        gray_score = 0.0
        llm_called_flag = False
    else:
        result = safe_call_llm(prompt, default_resp, model_type=route_model)
    selected_id = result.get("selected_property_id")
    raw_backup_ids = list(result.get("backup_property_ids", []) or [])
    raw_rejected_feedback = list(result.get("rejected_property_feedback", []) or [])
    thought_bubble = result.get("thought_bubble", "...")
    reason = result.get("reason", "未提供理由")
    monthly_intent = str(result.get("monthly_intent", "CONTINUE") or "CONTINUE").strip().upper()
    if monthly_intent not in {"CONTINUE", "STOP"}:
        monthly_intent = "CONTINUE"
    selected_in_shortlist = bool(selected_id) and any(int(c.get("property_id", -1)) == int(selected_id) for c in shortlist)

    # Enforce hard rule even if LLM returns an excluded id.
    if selected_id is not None and not selected_in_shortlist:
        selected_id = None
        reason = "selected_property_excluded_by_crowd_hard_rule"
        thought_bubble = "这套房竞争太拥挤，先换下一套。"

    shortlist_prop_index = {
        int(item.get("property_id", -1)): dict(item)
        for item in shortlist_context
        if item.get("property_id") is not None
    }
    backup_property_ids: List[int] = []
    for raw_pid in raw_backup_ids:
        try:
            backup_pid = int(raw_pid)
        except Exception:
            continue
        if backup_pid == int(selected_id or -1):
            continue
        if backup_pid not in shortlist_prop_index:
            continue
        if backup_pid in backup_property_ids:
            continue
        backup_property_ids.append(backup_pid)
        if len(backup_property_ids) >= 2:
            break

    rejected_property_feedback: List[Dict[str, object]] = []
    seen_rejected_ids: Set[int] = set()
    for entry in raw_rejected_feedback:
        if not isinstance(entry, dict):
            continue
        try:
            rejected_pid = int(entry.get("property_id"))
        except Exception:
            continue
        if rejected_pid == int(selected_id or -1):
            continue
        if rejected_pid in seen_rejected_ids or rejected_pid not in shortlist_prop_index:
            continue
        shortlist_meta = shortlist_prop_index[rejected_pid]
        rejected_property_feedback.append(
            {
                "property_id": rejected_pid,
                "reason_tag": _normalize_counterfactual_reason_tag(entry.get("reason_tag")),
                "reason": str(entry.get("reason", "") or ""),
                "cluster_key": str(shortlist_meta.get("diversity_cluster_key", "") or ""),
                "listed_price": float(shortlist_meta.get("listed_price", 0.0) or 0.0),
                "building_area": float(shortlist_meta.get("area", 0.0) or 0.0),
                "zone": str(shortlist_meta.get("zone", "") or ""),
                "is_school_district": bool(shortlist_meta.get("is_school_district", False)),
            }
        )
        seen_rejected_ids.add(rejected_pid)
        if len(rejected_property_feedback) >= 3:
            break


    crowd_guard_action = "none"
    crowd_guard_rounds = 0

    def _find_shortlist_listing(pid: Optional[int]) -> Dict:
        if pid is None:
            return {}
        try:
            pid_int = int(pid)
        except Exception:
            return {}
        return next((c for c in shortlist if int(c.get("property_id", -1)) == pid_int), {})

    selected_listing = _find_shortlist_listing(selected_id)
    selected_crowd_units = float(_crowd_meta_for_listing(selected_listing).get("pressure_units", 0.0)) if selected_listing else 0.0
    selected_estimated_overlap = int(_crowd_meta_for_listing(selected_listing).get("estimated_overlap_buyers", 1)) if selected_listing else 0
    low_crowd_alternative_count = int(
        sum(
            1
            for c in shortlist
            if int(c.get("property_id", -1)) != int(selected_listing.get("property_id", -1) or -1)
            and float(_crowd_meta_for_listing(c).get("pressure_units", 0.0)) <= crowd_tolerance_units + 1e-9
        )
    )
    crowd_over_threshold = bool(selected_listing) and float(selected_crowd_units) > crowd_tolerance_units + 1e-9

    if (
        selected_listing
        and crowd_over_threshold
        and low_crowd_alternative_count > 0
        and precheck_reselect_enabled
        and crowd_mode != "follow"
        and precheck_max_reselect_rounds > 0
        and _should_run_buyer_crowd_reselect(
            route_model=str(route_model),
            selected_crowd_units=float(selected_crowd_units),
            crowd_tolerance_units=float(crowd_tolerance_units),
            low_crowd_alternative_count=int(low_crowd_alternative_count),
            retry_attempt=int(retry_attempt),
            config=config,
        )
    ):
        alternatives_payload = []
        for c in shortlist:
            pid = int(c.get("property_id", -1))
            if pid == int(selected_listing.get("property_id", -1)):
                continue
            crowd_meta = _crowd_meta_for_listing(c)
            if float(crowd_meta.get("pressure_units", 0.0)) <= crowd_tolerance_units + 1e-9:
                alternatives_payload.append(
                    {
                        "id": pid,
                        "zone": c.get("zone"),
                        "price": float(c.get("listed_price", 0.0) or 0.0),
                        "crowd_pressure_units": float(crowd_meta.get("pressure_units", 0.0)),
                        "estimated_overlap_buyers": int(crowd_meta.get("estimated_overlap_buyers", 1)),
                    }
                )
        alternatives_payload = alternatives_payload[:5]

        for guard_round in range(int(precheck_max_reselect_rounds)):
            if not alternatives_payload:
                break
            guard_default = {
                "action": "KEEP",
                "selected_property_id": int(selected_listing.get("property_id", -1)),
                "reason": "保持原选择",
            }
            guard_prompt = f"""
            你刚选择了房源 {int(selected_listing.get("property_id", -1))}，该房源拥挤度为 {selected_crowd_units:.2f}，
            已超过你的拥挤容忍阈值 {crowd_tolerance_units:.2f}。
            你可以继续坚持，也可以切换到拥挤度更低的备选房源，或本轮放弃。

            当前低拥挤备选:
            {json.dumps(alternatives_payload, ensure_ascii=False, indent=2)}

            输出JSON:
            {{
              "action":"KEEP"|"SWITCH"|"WITHDRAW",
              "selected_property_id": int|null,
              "reason":"..."
            }}
            """
            guard_resp = safe_call_llm(guard_prompt, guard_default, model_type=route_model)
            guard_action = str(guard_resp.get("action", "KEEP") or "KEEP").strip().upper()
            guard_reason = str(guard_resp.get("reason", "") or "").strip()
            crowd_guard_rounds += 1
            if guard_action == "WITHDRAW":
                selected_id = None
                selected_listing = {}
                selected_crowd_units = 0.0
                selected_estimated_overlap = 0
                low_crowd_alternative_count = 0
                crowd_over_threshold = False
                crowd_guard_action = "withdraw"
                reason = guard_reason or reason
                thought_bubble = thought_bubble or "竞争太激烈，先观望。"
                break
            if guard_action == "SWITCH":
                guard_sid = guard_resp.get("selected_property_id")
                try:
                    guard_sid_int = int(guard_sid) if guard_sid is not None else None
                except Exception:
                    guard_sid_int = None
                switched = _find_shortlist_listing(guard_sid_int)
                if switched:
                    selected_id = int(switched.get("property_id"))
                    selected_listing = switched
                    selected_crowd_units = float(_crowd_meta_for_listing(switched).get("pressure_units", 0.0))
                    selected_estimated_overlap = int(_crowd_meta_for_listing(switched).get("estimated_overlap_buyers", 1))
                    crowd_over_threshold = bool(selected_crowd_units > crowd_tolerance_units + 1e-9)
                    crowd_guard_action = "switch"
                    reason = guard_reason or reason
                    if not crowd_over_threshold:
                        break
                    continue
            crowd_guard_action = "keep"
            reason = guard_reason or reason
            break

    selected_in_shortlist = bool(selected_id) and any(int(c.get("property_id", -1)) == int(selected_id) for c in shortlist)
    selected_listing = _find_shortlist_listing(selected_id)
    selected_property = properties_map.get(int(selected_id), {}) if selected_id else {}

    buyer._last_buyer_match_context = {
        "matching_month": int(current_match_month),
        "persona_snapshot": {
            "purchase_motive_primary": str(getattr(buyer.story, "purchase_motive_primary", "") or ""),
            "housing_stage": str(getattr(buyer.story, "housing_stage", "") or ""),
            "family_stage": str(getattr(buyer.story, "family_stage", "") or ""),
            "education_path": str(getattr(buyer.story, "education_path", "") or ""),
            "financial_profile": str(getattr(buyer.story, "financial_profile", "") or ""),
            "target_zone": str(getattr(pref, "target_zone", "") or ""),
            "max_price": float(getattr(pref, "max_price", 0.0) or 0.0),
            "school_urgency": int(getattr(buyer, "school_urgency", 0) or 0),
        },
        "strategy_profile": str(strategy_profile),
        "crowd_mode": str(crowd_mode),
        "crowd_profile_reasons": list(crowd_profile.get("reasons", []) or [])[:3],
        "crowd_hard_exclude_enabled": bool(crowd_hard_exclude_enabled),
        "crowd_floor_min_keep": int(crowd_floor_min_keep),
        "crowd_floor_fallback_applied": bool(crowd_floor_fallback_applied),
        "crowd_floor_fallback_added": int(crowd_floor_fallback_added),
        "fake_hot_circuit_enabled": bool(fake_hot_circuit_enabled),
        "fake_hot_blocked_pool_count": int(fake_hot_blocked_pool_count),
        "crowd_tolerance_units": float(crowd_tolerance_units),
        "selected_crowd_units": float(selected_crowd_units),
        "selected_estimated_overlap_buyers": int(selected_estimated_overlap),
        "crowd_over_threshold": bool(crowd_over_threshold),
        "crowd_guard_action": str(crowd_guard_action),
        "crowd_guard_rounds": int(crowd_guard_rounds),
        "low_crowd_alternative_count": int(low_crowd_alternative_count),
        "retry_attempt": int(retry_attempt),
        "excluded_property_count": int(len(excluded_property_ids)),
        "factor_contract": factor_contract,
        "bucket_plan": {
            "mode": "MAIN_SECONDARY_EXPLORE",
            "main_quota": int(main_quota),
            "secondary_quota": int(secondary_quota),
            "explore_quota": int(explore_quota),
            "total_shortlist_size": int(shortlist_size),
            "total_shortlist_full_size": int(shortlist_full_size),
            "candidate_two_stage_enabled": bool(candidate_two_stage_enabled),
            "stage1_pool_size": int(candidate_stage1_pool_size),
            "stage2_fill_size": int(candidate_stage2_fill_size),
        },
        "bucket_distribution": shortlist_bucket_distribution,
        "pipeline_stage_trace": pipeline_stage_trace,
        "dynamic_weights": {
            "base_education_weight": base_edu_weight,
            "effective_education_weight": effective_edu_weight,
            "base_price_sensitivity": base_price_sensitivity,
            "effective_price_sensitivity": effective_price_sensitivity,
        },
        "market_trend": str(market_trend),
        "baseline_shortlist_property_ids": [int(c.get("property_id", -1)) for c in full_baseline_shortlist if c.get("property_id") is not None],
        "shortlist_property_ids": [int(c.get("property_id", -1)) for c in full_shortlist if c.get("property_id") is not None],
        "shortlist_visible_property_ids": [int(c.get("property_id", -1)) for c in shortlist if c.get("property_id") is not None],
        "shortlist_full_size": int(shortlist_full_size),
        "shortlist_visible_size": int(shortlist_size),
        "buyer_match_visible_shortlist_cap": int(visible_shortlist_cap),
        "shortlist": shortlist_context,
        "shortlist_crowd_snapshot": [
            {
                "property_id": int(c.get("property_id", -1)),
                "crowd_pressure_units": float(_crowd_meta_for_listing(c).get("pressure_units", 0.0)),
                "estimated_overlap_buyers": int(_crowd_meta_for_listing(c).get("estimated_overlap_buyers", 1)),
                "crowd_level": str(_crowd_meta_for_listing(c).get("crowd_level", "LOW")),
                "within_tolerance": bool(_crowd_meta_for_listing(c).get("within_tolerance", True)),
            }
            for c in shortlist
        ],
        "school_requirement_relaxed": bool(school_requirement_relaxed),
        "buy_deadline_stage": str(buy_deadline_stage),
        "buy_deadline_months_left": int(buy_deadline_months_left),
        "buy_deadline_total_months": int(max_wait_months),
        "visible_for_sale_count": int(len(visible_for_sale_pool)),
        "visible_for_sale_property_ids": [int(x.get("property_id", -1)) for x in visible_for_sale_pool if x.get("property_id") is not None][:200],
        "eligible_candidate_count": int(len(candidates)),
        "force_full_visible_pool_enabled": bool(force_full_visible_pool_enabled),
        "shortlist_rotation_step": int(shortlist_rotation_step),
        "shortlist_offset": int(shortlist_offset),
        "selected_property_id": int(selected_id) if selected_id else None,
        "selected_diversity_cluster_key": str(
            next(
                (
                    item.get("diversity_cluster_key", "")
                    for item in shortlist_context
                    if int(item.get("property_id", -1)) == int(selected_id or -1)
                ),
                "",
            )
            or ""
        ),
        "selected_in_shortlist": bool(selected_in_shortlist),
        "backup_property_ids": [int(x) for x in backup_property_ids],
        "rejected_property_feedback": rejected_property_feedback,
        "selection_reason": str(reason),
        "llm_monthly_intent": str(monthly_intent),
        "stop_search_this_month": bool((not selected_id) and monthly_intent == "STOP"),
        "selection_reason_tags": _build_selection_reason_tags(
            buyer=buyer,
            pref=pref,
            selected_listing=selected_listing,
            selected_property=selected_property,
            shortlist_context=shortlist_context,
        ) if selected_id else [],
        "thought_bubble": str(thought_bubble),
        "llm_route_model": str(route_model),
        "llm_route_reason": str(route_reason),
        "llm_gray_score": float(gray_score),
        "llm_called": bool(llm_called_flag),
    }

    if selected_id:
        logger.info(f"💡 [买方初筛] 买家 {buyer.name}({buyer.id}) 选中房产 {selected_id}。内心戏:【{thought_bubble}】")
        for c in shortlist:
            if c['property_id'] == selected_id:
                return c
    else:
        logger.info(f"💡 [买方初筛] 买家 {buyer.name}({buyer.id}) 放弃所有房源。内心戏:【{thought_bubble}】")

    # Fallback/Logic for explicit None
    if selected_id is None:
        return None
    
    return None


def _resolve_cross_month_excluded_ids(buyer: Agent) -> Set[int]:
    current_match_month = int(getattr(buyer, "_current_matching_month", -1) or -1)
    previous_month_attempted_ids: Set[int] = set()
    if current_match_month > 1:
        attempted_history = getattr(buyer, "_attempted_property_ids_by_month", {}) or {}
        if isinstance(attempted_history, dict):
            prev_month_raw = attempted_history.get(int(current_match_month - 1), [])
            for pid in (prev_month_raw or []):
                try:
                    previous_month_attempted_ids.add(int(pid))
                except Exception:
                    continue
    repriced_reentry_ids: Set[int] = set()
    for pid in (getattr(buyer, "_repriced_reentry_property_ids", []) or []):
        try:
            repriced_reentry_ids.add(int(pid))
        except Exception:
            continue
    if repriced_reentry_ids:
        previous_month_attempted_ids = {
            int(pid) for pid in previous_month_attempted_ids
            if int(pid) not in repriced_reentry_ids
        }
    return previous_month_attempted_ids


def match_properties_for_buyer(
    buyer: Agent,
    listings: List[Dict],
    properties_map: Dict[int, Dict],
    config=None,
    ignore_zone: bool = False,
    market_trend: str = "STABLE",
) -> List[Dict]:
    """
    Return one or multiple property selections for a buyer.
    - normal agent: at most 1
    - smart agent: up to configured monthly cap
    """
    cap = get_buyer_monthly_buy_cap(buyer, config=config)
    decision_mode = "smart" if getattr(buyer, "agent_type", "normal") == "smart" else "normal"
    buy_task_locked = bool(getattr(buyer, "_buy_task_locked", False))

    buyer_story = getattr(buyer, "story", None)
    buyer_pref = getattr(buyer, "preference", None)
    buyer_motive = str(getattr(buyer_story, "primary_motivation", "") or "").strip().lower()
    need_school_district = bool(getattr(buyer_pref, "need_school_district", False))
    hard_need_motives = {"starter_entry", "education_driven", "chain_replacement"}
    is_hard_need_buyer = buyer_motive in hard_need_motives or need_school_district
    reflow_blocked_ids = set(
        int(x)
        for x in (getattr(buyer, "_reflow_blocked_property_ids", set()) or set())
        if x is not None
    )
    reflow_active = bool(reflow_blocked_ids)
    waited_months = max(0, int(getattr(buyer, "waited_months", 0) or 0))
    max_wait_months = max(1, int(getattr(buyer, "max_wait_months", getattr(buyer_pref, "max_wait_months", 6)) or 6))
    deadline_progress = float(waited_months) / float(max_wait_months) if max_wait_months > 0 else 0.0
    if deadline_progress >= 1.0:
        buy_deadline_stage = "overdue"
    elif deadline_progress >= 0.75:
        buy_deadline_stage = "late"
    elif deadline_progress >= 0.40:
        buy_deadline_stage = "mid"
    else:
        buy_deadline_stage = "early"

    selection_cap = int(cap)
    # Human-like fallback chain for normal buyers: best/second/backup (bounded, opt-in).
    if decision_mode != "smart":
        try:
            normal_backup_slots = int(
                config.get("smart_agent.normal_buyer_backup_slots", config.get("normal_buyer_backup_slots", 0))
            ) if config else 0
        except Exception:
            normal_backup_slots = 0
        normal_backup_slots = max(0, min(6, normal_backup_slots))
        selection_cap = max(1, 1 + normal_backup_slots)

    try:
        monthly_retry_attempts = int(
            config.get("smart_agent.monthly_retry_attempts", config.get("monthly_retry_attempts", 1))
        ) if config else 1
    except Exception:
        monthly_retry_attempts = 1
    try:
        retry_top_k_step = int(
            config.get(
                "smart_agent.monthly_retry_expand_top_k_step",
                config.get("monthly_retry_expand_top_k_step", 1),
            )
        ) if config else 1
    except Exception:
        retry_top_k_step = 1
    # Reflow pass: diversify stronger after outbid in same month.
    if reflow_active:
        try:
            reflow_retry_bonus = int(
                config.get(
                    "smart_agent.same_month_outbid_reflow_retry_bonus",
                    config.get("same_month_outbid_reflow_retry_bonus", 1),
                )
            ) if config else 1
        except Exception:
            reflow_retry_bonus = 1
        try:
            reflow_topk_step_bonus = int(
                config.get(
                    "smart_agent.same_month_outbid_reflow_top_k_step_bonus",
                    config.get("same_month_outbid_reflow_top_k_step_bonus", 1),
                )
            ) if config else 1
        except Exception:
            reflow_topk_step_bonus = 1
        try:
            reflow_selection_cap_bonus = int(
                config.get(
                    "smart_agent.same_month_outbid_reflow_selection_cap_bonus",
                    config.get("same_month_outbid_reflow_selection_cap_bonus", 1),
                )
            ) if config else 1
        except Exception:
            reflow_selection_cap_bonus = 1
        monthly_retry_attempts = int(monthly_retry_attempts) + max(0, int(reflow_retry_bonus))
        retry_top_k_step = int(retry_top_k_step) + max(0, int(reflow_topk_step_bonus))
        selection_cap = int(selection_cap) + max(0, int(reflow_selection_cap_bonus))
    try:
        retry_ignore_zone_last_attempt_raw = (
            config.get(
                "smart_agent.monthly_retry_ignore_zone_last_attempt",
                config.get("monthly_retry_ignore_zone_last_attempt", True),
            )
            if config
            else True
        )
        if isinstance(retry_ignore_zone_last_attempt_raw, bool):
            retry_ignore_zone_last_attempt = retry_ignore_zone_last_attempt_raw
        else:
            retry_ignore_zone_last_attempt = str(retry_ignore_zone_last_attempt_raw).strip().lower() in {
                "1", "true", "yes", "y", "on"
            }
    except Exception:
        retry_ignore_zone_last_attempt = True
    try:
        crowd_hard_gate_enabled_raw = (
            config.get(
                "smart_agent.candidate_crowd_hard_gate_enabled",
                config.get("candidate_crowd_hard_gate_enabled", True),
            )
            if config
            else True
        )
        if isinstance(crowd_hard_gate_enabled_raw, bool):
            crowd_hard_gate_enabled = crowd_hard_gate_enabled_raw
        else:
            crowd_hard_gate_enabled = str(crowd_hard_gate_enabled_raw).strip().lower() in {
                "1", "true", "yes", "y", "on"
            }
    except Exception:
        crowd_hard_gate_enabled = True
    try:
        crowd_hard_gate_modes_raw = (
            config.get(
                "smart_agent.candidate_crowd_hard_gate_modes",
                config.get("candidate_crowd_hard_gate_modes", "avoid,neutral,follow"),
            )
            if config
            else "avoid,neutral,follow"
        )
        crowd_hard_gate_modes = {
            s.strip().lower()
            for s in str(crowd_hard_gate_modes_raw).replace(";", ",").split(",")
            if s and s.strip()
        }
    except Exception:
        crowd_hard_gate_modes = {"avoid", "neutral", "follow"}
    if not crowd_hard_gate_modes:
        crowd_hard_gate_modes = {"avoid", "neutral", "follow"}
    # Reflow pass: force avoid re-crowding no matter profile.
    try:
        reflow_force_crowd_avoid_enabled_raw = (
            config.get(
                "smart_agent.same_month_outbid_reflow_force_crowd_avoid_enabled",
                config.get("same_month_outbid_reflow_force_crowd_avoid_enabled", True),
            )
            if config
            else True
        )
        if isinstance(reflow_force_crowd_avoid_enabled_raw, bool):
            reflow_force_crowd_avoid_enabled = reflow_force_crowd_avoid_enabled_raw
        else:
            reflow_force_crowd_avoid_enabled = str(reflow_force_crowd_avoid_enabled_raw).strip().lower() in {
                "1", "true", "yes", "y", "on"
            }
    except Exception:
        reflow_force_crowd_avoid_enabled = True
    reflow_force_crowd_avoid = bool(reflow_active and reflow_force_crowd_avoid_enabled)
    monthly_retry_attempts = max(0, min(12, int(monthly_retry_attempts)))
    retry_top_k_step = max(0, min(3, int(retry_top_k_step)))
    if reflow_active:
        monthly_retry_attempts = max(monthly_retry_attempts, 4)
        selection_cap = max(selection_cap, 2)
    if buy_task_locked:
        monthly_retry_attempts = max(monthly_retry_attempts, 6)
        selection_cap = max(selection_cap, 3)

    # Buyer deadline staged relaxation:
    # late/overdue buyers increase retries and candidate breadth to avoid "stuck without deal".
    if buy_deadline_stage == "late":
        monthly_retry_attempts = max(monthly_retry_attempts, 4)
        selection_cap = max(selection_cap, 3)
        retry_ignore_zone_last_attempt = True
    elif buy_deadline_stage == "overdue":
        monthly_retry_attempts = max(monthly_retry_attempts, 8)
        selection_cap = max(selection_cap, 4)
        retry_ignore_zone_last_attempt = True
        # For overdue buyers, crowd hard gate should not block all options.
        crowd_hard_gate_enabled = False

    def _to_bool(raw, default: bool) -> bool:
        if raw is None:
            return bool(default)
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}

    def _to_int(raw, default: int) -> int:
        try:
            return int(raw)
        except Exception:
            return int(default)

    hard_need_dynamic_attempts_enabled = _to_bool(
        config.get(
            "smart_agent.hard_need_dynamic_attempts_enabled",
            config.get("hard_need_dynamic_attempts_enabled", True),
        ) if config else True,
        True,
    )
    hard_need_min_total_attempts = max(
        1,
        min(
            20,
            _to_int(
                config.get(
                    "smart_agent.hard_need_min_total_attempts",
                    config.get("hard_need_min_total_attempts", 8),
                ) if config else 8,
                8,
            ),
        ),
    )
    hard_need_max_total_attempts = max(
        hard_need_min_total_attempts,
        min(
            80,
            _to_int(
                config.get(
                    "smart_agent.hard_need_max_total_attempts",
                    config.get("hard_need_max_total_attempts", 40),
                ) if config else 40,
                40,
            ),
        ),
    )
    hard_need_exhaust_untried_enabled = _to_bool(
        config.get(
            "smart_agent.hard_need_exhaust_untried_enabled",
            config.get("hard_need_exhaust_untried_enabled", True),
        ) if config else True,
        True,
    )
    hard_need_extra_selection_slots = max(
        0,
        min(
            6,
            _to_int(
                config.get(
                    "smart_agent.hard_need_extra_selection_slots",
                    config.get("hard_need_extra_selection_slots", 1),
                ) if config else 1,
                1,
            ),
        ),
    )

    if decision_mode != "smart" and hard_need_dynamic_attempts_enabled and is_hard_need_buyer:
        selection_cap = max(1, min(8, int(selection_cap) + int(hard_need_extra_selection_slots)))

    base_total_attempt_budget = max(1, int(selection_cap) * (int(monthly_retry_attempts) + 1))
    total_attempt_budget = int(base_total_attempt_budget)
    if hard_need_dynamic_attempts_enabled and is_hard_need_buyer:
        boosted_budget = int(base_total_attempt_budget) + int(hard_need_extra_selection_slots) * (int(monthly_retry_attempts) + 1)
        boosted_budget = max(int(hard_need_min_total_attempts), boosted_budget)
        boosted_budget = min(int(hard_need_max_total_attempts), boosted_budget)
        total_attempt_budget = max(int(base_total_attempt_budget), int(boosted_budget))
    if hard_need_dynamic_attempts_enabled and is_hard_need_buyer and hard_need_exhaust_untried_enabled:
        unique_property_ids = set()
        for l in listings:
            try:
                pid = int(l.get("property_id"))
            except Exception:
                pid = None
            if pid is not None:
                unique_property_ids.add(pid)
        unique_listing_budget = max(1, len(unique_property_ids))
        total_attempt_budget = max(int(total_attempt_budget), int(unique_listing_budget))
    if buy_deadline_stage == "late":
        total_attempt_budget = max(int(total_attempt_budget), int(base_total_attempt_budget) + 8)
    elif buy_deadline_stage == "overdue":
        total_attempt_budget = max(int(total_attempt_budget), int(base_total_attempt_budget) + 16)

    current_match_month = int(getattr(buyer, "_current_matching_month", -1) or -1)
    previous_month_attempted_ids: Set[int] = set()
    current_month_seen_ids: Set[int] = set()
    attempted_history = getattr(buyer, "_attempted_property_ids_by_month", {}) or {}
    if current_match_month > 1:
        if isinstance(attempted_history, dict):
            prev_month_raw = attempted_history.get(int(current_match_month - 1), [])
            for pid in (prev_month_raw or []):
                try:
                    previous_month_attempted_ids.add(int(pid))
                except Exception:
                    continue
    if current_match_month > 0 and isinstance(attempted_history, dict):
        current_month_raw = attempted_history.get(int(current_match_month), [])
        for pid in (current_month_raw or []):
            try:
                current_month_seen_ids.add(int(pid))
            except Exception:
                continue
    for pid in (getattr(buyer, "_same_month_seen_property_ids", []) or []):
        try:
            current_month_seen_ids.add(int(pid))
        except Exception:
            continue
    repriced_reentry_ids: Set[int] = set()
    for pid in (getattr(buyer, "_repriced_reentry_property_ids", []) or []):
        try:
            repriced_reentry_ids.add(int(pid))
        except Exception:
            continue
    if repriced_reentry_ids:
        previous_month_attempted_ids = {
            int(pid) for pid in previous_month_attempted_ids
            if int(pid) not in repriced_reentry_ids
        }
        current_month_seen_ids = {
            int(pid) for pid in current_month_seen_ids
            if int(pid) not in repriced_reentry_ids
        }

    selected = []
    selected_ids = set()
    attempted_ids: Set[int] = set(previous_month_attempted_ids) | set(current_month_seen_ids)
    attempted_this_month: Set[int] = set()
    retry_trace: List[Dict[str, object]] = []
    attempts_spent = 0
    llm_stop_signals = 0
    same_month_abandon_cycles = 0
    try:
        regime_buyer_rhythm_enabled = _to_bool(
            config.get(
                "smart_agent.regime_v1_buyer_search_rhythm_enabled",
                config.get("regime_v1_buyer_search_rhythm_enabled", True),
            )
            if config
            else True,
            True,
        ) and _regime_v1_enabled(config)
    except Exception:
        regime_buyer_rhythm_enabled = False
    try:
        same_month_max_abandon_cycles = int(
            config.get(
                "smart_agent.regime_v1_same_month_max_abandon_cycles",
                config.get("regime_v1_same_month_max_abandon_cycles", 2),
            )
        ) if config else 2
    except Exception:
        same_month_max_abandon_cycles = 2
    same_month_max_abandon_cycles = max(1, min(6, int(same_month_max_abandon_cycles)))
    if buy_task_locked:
        same_month_max_abandon_cycles = max(int(same_month_max_abandon_cycles), 4)

    try:
        hard_need_stop_after_llm_stop = int(
            config.get(
                "smart_agent.hard_need_stop_after_llm_stop_signals",
                config.get("hard_need_stop_after_llm_stop_signals", 1),
            )
        ) if config else 1
    except Exception:
        hard_need_stop_after_llm_stop = 1
    hard_need_stop_after_llm_stop = max(1, min(3, int(hard_need_stop_after_llm_stop)))
    if buy_task_locked:
        hard_need_stop_after_llm_stop = max(int(hard_need_stop_after_llm_stop), 3)

    for _ in range(selection_cap):
        if attempts_spent >= total_attempt_budget:
            break
        chosen = None
        for attempt in range(monthly_retry_attempts + 1):
            if attempts_spent >= total_attempt_budget:
                retry_trace.append(
                    {
                        "attempt": int(attempt),
                        "result": "attempt_budget_exhausted",
                        "attempts_spent": int(attempts_spent),
                        "attempt_budget": int(total_attempt_budget),
                    }
                )
                break
            remaining = []
            for l in listings:
                try:
                    lp = int(l.get("property_id"))
                except Exception:
                    lp = None
                if lp is None:
                    continue
                if lp in selected_ids or lp in attempted_ids:
                    continue
                remaining.append(l)
            if not remaining:
                break
            apply_ignore_zone = bool(ignore_zone or (buy_deadline_stage in {"late", "overdue"}))
            if (not apply_ignore_zone) and retry_ignore_zone_last_attempt and attempt == monthly_retry_attempts and monthly_retry_attempts > 0:
                apply_ignore_zone = True
            attempts_spent += 1
            chosen = match_property_for_buyer(
                buyer,
                remaining,
                properties_map,
                ignore_zone=apply_ignore_zone,
                decision_mode=decision_mode,
                market_trend=market_trend,
                config=config,
                excluded_property_ids=attempted_ids,
                retry_attempt=attempt,
                top_k_boost=attempt * retry_top_k_step,
                crowd_hard_exclude_override=(
                    False if buy_deadline_stage == "overdue" else None
                ),
            )
            if not chosen:
                retry_trace.append(
                    {
                        "attempt": int(attempt),
                        "result": "no_selection",
                        "remaining_candidates": int(len(remaining)),
                        "ignore_zone": bool(apply_ignore_zone),
                        "attempts_spent": int(attempts_spent),
                        "attempt_budget": int(total_attempt_budget),
                    }
                )
                continue
            chosen_ctx = dict(getattr(buyer, "_last_buyer_match_context", {}) or {})
            chosen_crowd_mode = str(chosen_ctx.get("crowd_mode", "neutral") or "neutral").strip().lower()
            chosen_crowd_over_threshold = bool(chosen_ctx.get("crowd_over_threshold", False))
            chosen_low_crowd_alternatives = int(chosen_ctx.get("low_crowd_alternative_count", 0) or 0)
            if (
                crowd_hard_gate_enabled
                and chosen_crowd_over_threshold
                and (chosen_crowd_mode in crowd_hard_gate_modes or reflow_force_crowd_avoid)
            ):
                try:
                    overcrowded_pid = int(chosen.get("property_id"))
                    attempted_ids.add(overcrowded_pid)
                    attempted_this_month.add(overcrowded_pid)
                except Exception:
                    overcrowded_pid = None
                retry_trace.append(
                    {
                        "attempt": int(attempt),
                        "result": "crowd_over_threshold_retry",
                        "property_id": overcrowded_pid,
                        "crowd_mode": str(chosen_crowd_mode),
                        "low_crowd_alternatives": int(chosen_low_crowd_alternatives),
                        "ignore_zone": bool(apply_ignore_zone),
                        "attempts_spent": int(attempts_spent),
                        "attempt_budget": int(total_attempt_budget),
                    }
                )
                chosen = None
                continue
            exposed_ids = []
            for exposed_pid in (chosen_ctx.get("shortlist_visible_property_ids", []) or []):
                try:
                    exposed_ids.append(int(exposed_pid))
                except Exception:
                    continue
            for exposed_pid in exposed_ids:
                attempted_ids.add(int(exposed_pid))
                attempted_this_month.add(int(exposed_pid))
            try:
                attempted_ids.add(int(chosen.get("property_id")))
                attempted_this_month.add(int(chosen.get("property_id")))
            except Exception:
                pass
            retry_trace.append(
                {
                    "attempt": int(attempt),
                    "result": "selected",
                    "property_id": int(chosen.get("property_id", -1)),
                    "ignore_zone": bool(apply_ignore_zone),
                    "attempts_spent": int(attempts_spent),
                    "attempt_budget": int(total_attempt_budget),
                }
            )
            break

        if not chosen:
            last_ctx = dict(getattr(buyer, "_last_buyer_match_context", {}) or {})
            exposed_ids = []
            for exposed_pid in (last_ctx.get("shortlist_visible_property_ids", []) or []):
                try:
                    exposed_ids.append(int(exposed_pid))
                except Exception:
                    continue
            for exposed_pid in exposed_ids:
                attempted_ids.add(int(exposed_pid))
                attempted_this_month.add(int(exposed_pid))
            llm_stop_now = bool(last_ctx.get("stop_search_this_month", False))
            structural_no_candidate = bool(
                last_ctx
                and not bool(last_ctx.get("llm_called", False))
                and str(last_ctx.get("selection_reason", "") or "").strip().lower().startswith("no_candidates_after_")
            )
            if llm_stop_now:
                llm_stop_signals += 1
                same_month_abandon_cycles += 1
            elif structural_no_candidate:
                retry_trace.append(
                    {
                        "attempt": int(len(retry_trace) + 1),
                        "result": "structural_no_candidate_defer_next_month",
                        "selection_reason": str(last_ctx.get("selection_reason", "") or ""),
                        "attempts_spent": int(attempts_spent),
                        "attempt_budget": int(total_attempt_budget),
                    }
                )
                break
            elif bool(last_ctx):
                # no selection with context present, still count as one abandon-like cycle
                same_month_abandon_cycles += 1
            if regime_buyer_rhythm_enabled and same_month_abandon_cycles >= same_month_max_abandon_cycles:
                retry_trace.append(
                    {
                        "attempt": int(len(retry_trace) + 1),
                        "result": "regime_v1_stop_same_month_loop",
                        "same_month_abandon_cycles": int(same_month_abandon_cycles),
                        "same_month_max_abandon_cycles": int(same_month_max_abandon_cycles),
                        "attempts_spent": int(attempts_spent),
                        "attempt_budget": int(total_attempt_budget),
                    }
                )
                break
            if (
                hard_need_dynamic_attempts_enabled
                and is_hard_need_buyer
                and attempts_spent < total_attempt_budget
            ):
                has_untried_remaining = False
                for l in listings:
                    try:
                        lp = int(l.get("property_id"))
                    except Exception:
                        lp = None
                    if lp is None:
                        continue
                    if lp in selected_ids or lp in attempted_ids:
                        continue
                    has_untried_remaining = True
                    break
                if has_untried_remaining and llm_stop_signals < hard_need_stop_after_llm_stop:
                    retry_trace.append(
                        {
                            "attempt": int(len(retry_trace) + 1),
                            "result": "hard_need_continue_search",
                            "attempts_spent": int(attempts_spent),
                            "attempt_budget": int(total_attempt_budget),
                            "llm_stop_signals": int(llm_stop_signals),
                        }
                    )
                    continue
            break
        pid = chosen.get("property_id")
        try:
            pid_int = int(pid)
        except Exception:
            pid_int = pid
        if pid_int in selected_ids:
            break
        chosen_copy = dict(chosen)
        chosen_copy["candidate_rank"] = int(len(selected) + 1)
        chosen_copy["_selection_snapshot"] = dict(getattr(buyer, "_last_buyer_match_context", {}) or {})
        chosen_copy["_selection_retry_trace"] = list(getattr(buyer, "_last_buyer_match_retry_trace", []) or [])
        chosen_copy["_selection_retry_budget"] = dict(getattr(buyer, "_last_buyer_match_retry_budget", {}) or {})
        selected.append(chosen_copy)
        selected_ids.add(pid_int)
        try:
            attempted_ids.add(int(pid_int))
            attempted_this_month.add(int(pid_int))
        except Exception:
            pass

    all_visible_ids: Set[int] = set()
    for l in listings:
        try:
            all_visible_ids.add(int(l.get("property_id")))
        except Exception:
            continue
    remaining_untried_ids = set(int(x) for x in all_visible_ids if x not in set(int(y) for y in attempted_ids))
    search_exhausted_this_month = (len(selected) == 0 and len(remaining_untried_ids) == 0)

    setattr(
        buyer,
        "_last_buyer_match_retry_budget",
        {
            "is_hard_need_buyer": bool(is_hard_need_buyer),
            "attempts_spent": int(attempts_spent),
            "attempt_budget": int(total_attempt_budget),
            "llm_stop_signals": int(llm_stop_signals),
            "same_month_abandon_cycles": int(same_month_abandon_cycles),
            "same_month_max_abandon_cycles": int(same_month_max_abandon_cycles),
            "hard_need_stop_after_llm_stop_signals": int(hard_need_stop_after_llm_stop),
            "selection_cap": int(selection_cap),
            "monthly_retry_attempts": int(monthly_retry_attempts),
            "hard_need_dynamic_attempts_enabled": bool(hard_need_dynamic_attempts_enabled),
            "reflow_active": bool(reflow_active),
            "reflow_force_crowd_avoid": bool(reflow_force_crowd_avoid),
            "buy_deadline_stage": str(buy_deadline_stage),
            "buy_deadline_months_left": int(max(0, max_wait_months - waited_months)),
            "buy_deadline_total_months": int(max_wait_months),
            "cross_month_excluded_count": int(len(previous_month_attempted_ids)),
            "search_exhausted_this_month": bool(search_exhausted_this_month),
            "remaining_untried_count": int(len(remaining_untried_ids)),
        },
    )
    setattr(buyer, "_last_buyer_match_retry_trace", retry_trace)

    # Cross-month traversal memory:
    # Keep track of this month's attempted properties so next month can avoid revisiting them.
    if current_match_month > 0:
        attempted_history = getattr(buyer, "_attempted_property_ids_by_month", {}) or {}
        if not isinstance(attempted_history, dict):
            attempted_history = {}
        existing_current = set()
        for pid in (attempted_history.get(int(current_match_month), []) or []):
            try:
                existing_current.add(int(pid))
            except Exception:
                continue
        if attempted_this_month:
            existing_current |= set(int(x) for x in attempted_this_month if x is not None)
            attempted_history[int(current_match_month)] = sorted(existing_current)
        # Retain only recent months to bound memory footprint.
        min_month_to_keep = int(current_match_month) - 3
        attempted_history = {
            int(m): v
            for m, v in attempted_history.items()
            if int(m) >= min_month_to_keep
        }
        setattr(buyer, "_attempted_property_ids_by_month", attempted_history)

    return selected

# --- 3. Negotiation Logic (Phase 2.2 & P3) ---


def get_market_condition(market: Market, zone: str, potential_buyers_count: int) -> str:
    """
    Determine market condition based on Supply/Demand Ratio.
    Ratio = Active Listings / Potential Buyers
    """
    listings = [p for p in market.properties if p['status'] == 'for_sale' and p['zone'] == zone]
    listing_count = len(listings)

    # Avoid division by zero
    buyer_count = max(potential_buyers_count, 1)

    ratio = listing_count / buyer_count

    # Thresholds
    if ratio > 1.5:
        return "oversupply"      # 供过于求 (买方市场)
    elif ratio < 0.7:
        return "undersupply"     # 供不应求 (卖方市场)
    else:
        return "balanced"        # 供需平衡


def _resolve_arbiter_mode(config=None) -> Dict[str, object]:
    enabled = False
    strict_range = True
    prefer_midpoint = True
    if config:
        enabled = bool(config.get("negotiation.arbiter_mode.enabled", False))
        strict_range = bool(config.get("negotiation.arbiter_mode.strict_price_range", True))
        prefer_midpoint = bool(config.get("negotiation.arbiter_mode.prefer_midpoint_fallback", True))
    return {
        "enabled": enabled,
        "strict_price_range": strict_range,
        "prefer_midpoint_fallback": prefer_midpoint,
    }


def _resolve_negotiation_routing_config(config=None) -> Dict[str, float]:
    enabled = True
    threshold = 0.55
    if config:
        try:
            enabled_raw = config.get("smart_agent.negotiation_dual_routing_enabled", True)
            enabled = enabled_raw if isinstance(enabled_raw, bool) else str(enabled_raw).strip().lower() in {
                "1", "true", "yes", "y", "on"
            }
        except Exception:
            enabled = True
        try:
            threshold = float(config.get("smart_agent.negotiation_gray_score_threshold", 0.55))
        except Exception:
            threshold = 0.55
    return {
        "enabled": bool(enabled),
        "threshold": max(0.1, min(0.95, float(threshold))),
    }


def _resolve_negotiation_route(seller: Agent, buyers: List[Agent], listing: Dict, config=None) -> Dict[str, object]:
    """
    Choose LLM model route only. Never replaces negotiation decisions.
    """
    route_cfg = _resolve_negotiation_routing_config(config)
    if not buyers:
        return {"model": "fast", "gray_score": 0.0, "reason": "no_buyers"}
    if not route_cfg["enabled"]:
        return {"model": "smart", "gray_score": 1.0, "reason": "dual_routing_disabled"}

    seller_smart = str(getattr(seller, "agent_type", "normal")).lower() == "smart"
    buyer_types = [str(getattr(b, "agent_type", "normal")).lower() for b in buyers]
    smart_buyer_ratio = (
        sum(1 for t in buyer_types if t == "smart") / max(1, len(buyer_types))
    )
    current_price = float(listing.get("listed_price", 0.0) or 0.0)
    min_price = float(listing.get("min_price", current_price) or current_price)
    price_flex = 0.0 if current_price <= 0 else max(0.0, min(1.0, (current_price - min_price) / current_price))
    buyer_max_values = [
        float(getattr(getattr(b, "preference", None), "max_price", 0.0) or 0.0)
        for b in buyers
    ]
    valid_budgets = [v for v in buyer_max_values if v > 0]
    budget_dispersion = 0.0
    if valid_budgets:
        bmin = min(valid_budgets)
        bmax = max(valid_budgets)
        budget_dispersion = 0.0 if bmin <= 0 else max(0.0, min(1.0, (bmax - bmin) / bmin / 1.2))
    buyer_count_factor = min(1.0, (len(buyers) - 1) / 6.0)

    gray_score = (
        0.34 * smart_buyer_ratio
        + 0.22 * (1.0 if seller_smart else 0.0)
        + 0.20 * budget_dispersion
        + 0.14 * buyer_count_factor
        + 0.10 * min(1.0, price_flex / 0.15)
    )
    threshold = float(route_cfg["threshold"])
    if gray_score >= threshold:
        return {
            "model": "smart",
            "gray_score": float(gray_score),
            "reason": f"gray_zone score={gray_score:.3f} threshold={threshold:.3f}",
        }
    return {
        "model": "fast",
        "gray_score": float(gray_score),
        "reason": f"clear_case score={gray_score:.3f} threshold={threshold:.3f}",
    }


def _should_run_buyer_crowd_reselect(
    *,
    route_model: str,
    selected_crowd_units: float,
    crowd_tolerance_units: float,
    low_crowd_alternative_count: int,
    retry_attempt: int,
    config=None,
) -> bool:
    """
    Run the extra reselect prompt only for genuine gray/extreme crowd cases.
    This never replaces the buyer's original choice with code; it only decides
    whether an extra LLM recheck is worth spending tokens on.
    """
    if low_crowd_alternative_count <= 0:
        return False
    tolerance = float(crowd_tolerance_units or 0.0)
    selected_units = float(selected_crowd_units or 0.0)
    if selected_units <= tolerance + 1e-9:
        return False

    try:
        fast_extreme_enabled = bool(
            config.get(
                "smart_agent.candidate_crowd_precheck_fast_extreme_enabled",
                config.get("candidate_crowd_precheck_fast_extreme_enabled", True),
            )
        ) if config else True
    except Exception:
        fast_extreme_enabled = True
    try:
        severe_multiplier = float(
            config.get(
                "smart_agent.candidate_crowd_precheck_fast_extreme_multiplier",
                config.get("candidate_crowd_precheck_fast_extreme_multiplier", 1.60),
            )
        ) if config else 1.60
    except Exception:
        severe_multiplier = 1.60
    try:
        fast_min_alternatives = int(
            config.get(
                "smart_agent.candidate_crowd_precheck_fast_extreme_min_alternatives",
                config.get("candidate_crowd_precheck_fast_extreme_min_alternatives", 3),
            )
        ) if config else 3
    except Exception:
        fast_min_alternatives = 3
    try:
        smart_max_retry = int(
            config.get(
                "smart_agent.candidate_crowd_precheck_smart_max_retry_attempt",
                config.get("candidate_crowd_precheck_smart_max_retry_attempt", 1),
            )
        ) if config else 1
    except Exception:
        smart_max_retry = 1
    try:
        fast_max_retry = int(
            config.get(
                "smart_agent.candidate_crowd_precheck_fast_max_retry_attempt",
                config.get("candidate_crowd_precheck_fast_max_retry_attempt", 0),
            )
        ) if config else 0
    except Exception:
        fast_max_retry = 0

    retry_i = int(retry_attempt or 0)
    if str(route_model or "").lower() == "smart":
        return retry_i <= max(0, smart_max_retry)

    if not fast_extreme_enabled:
        return False

    extreme_crowd = selected_units >= max(tolerance * max(1.05, severe_multiplier), tolerance + 1.0)
    return (
        extreme_crowd
        and int(low_crowd_alternative_count or 0) >= max(1, fast_min_alternatives)
        and retry_i <= max(0, fast_max_retry)
    )


def _should_run_competitive_seller_recheck(
    *,
    route_model: str,
    bid_count: int,
    spread_ratio: float,
    round_index: int,
    total_rounds: int,
    config=None,
) -> bool:
    """
    Only spend an extra seller recheck call on genuine gray or tightly clustered
    competitive sessions.
    """
    if int(round_index) >= int(total_rounds):
        return False
    if int(bid_count) < 2:
        return False
    if str(route_model or "").lower() == "smart":
        return True

    try:
        fast_enabled = bool(
            config.get(
                "smart_agent.classic_competitive_seller_recheck_fast_enabled",
                config.get("classic_competitive_seller_recheck_fast_enabled", True),
            )
        ) if config else True
    except Exception:
        fast_enabled = True
    if not fast_enabled:
        return False
    try:
        max_spread = float(
            config.get(
                "smart_agent.classic_competitive_seller_recheck_fast_spread_cap",
                config.get("classic_competitive_seller_recheck_fast_spread_cap", 0.03),
            )
        ) if config else 0.03
    except Exception:
        max_spread = 0.03
    try:
        min_buyers = int(
            config.get(
                "smart_agent.classic_competitive_seller_recheck_fast_min_buyers",
                config.get("classic_competitive_seller_recheck_fast_min_buyers", 4),
            )
        ) if config else 4
    except Exception:
        min_buyers = 4

    return int(bid_count) >= max(2, min_buyers) and float(spread_ratio or 0.0) <= max(0.0, max_spread)


def _arbiter_negotiate_sync(
    buyer: Agent,
    seller: Agent,
    listing: Dict,
    market_condition: str,
    market_hint: str,
    macro_context: str,
    effective_bid_floor: float,
    config=None,
) -> Dict:
    """
    Optional judge mode: one-shot LLM verdict to reduce multi-round token cost.
    Returns same shape as negotiate().
    """
    current_price = float(listing.get("listed_price", 0.0) or 0.0)
    min_price = float(listing.get("min_price", current_price) or current_price)
    buyer_max = float(getattr(buyer.preference, "max_price", 0.0) or 0.0)
    lower = max(min_price, effective_bid_floor)
    upper = min(current_price, buyer_max)
    if upper < lower:
        return {"outcome": "failed", "reason": "Arbiter: no overlap", "history": [], "final_price": 0}

    prompt = f"""
    你是房地产交易仲裁员，只做一次裁决，不要多轮谈判。
    {macro_context}
    【市场】{market_condition} | {market_hint}
    【买方】
    - id={buyer.id}
    - 背景={buyer.story.background_story}
    - 预算上限={buyer_max:,.0f}
    - 风格={getattr(buyer.story, 'negotiation_style', 'balanced')}
    【卖方】
    - id={seller.id}
    - 背景={seller.story.background_story}
    - 底价={min_price:,.0f}
    - 当前报价={current_price:,.0f}
    - 风格={getattr(seller.story, 'negotiation_style', 'balanced')}
    【有效价格区间】[{lower:,.0f}, {upper:,.0f}]

    输出JSON:
    {{
      "outcome":"DEAL"|"NO_DEAL",
      "final_price": 数字或0,
      "reason":"一句话说明"
    }}
    """
    default_resp = {"outcome": "NO_DEAL", "final_price": 0, "reason": "No agreement"}
    resp = safe_call_llm(prompt, default_resp, model_type="fast")

    outcome = str(resp.get("outcome", "NO_DEAL")).upper()
    raw_price = float(resp.get("final_price", 0) or 0.0)
    mode_cfg = _resolve_arbiter_mode(config)
    if mode_cfg["strict_price_range"]:
        if raw_price < lower or raw_price > upper:
            raw_price = (lower + upper) / 2.0 if mode_cfg["prefer_midpoint_fallback"] else 0.0

    if outcome == "DEAL" and raw_price > 0:
        final_price = max(lower, min(upper, raw_price))
        return {
            "outcome": "success",
            "final_price": final_price,
            "history": [
                {
                    "round": 1,
                    "party": "arbiter",
                    "agent_id": int(getattr(seller, "id", -1)),
                    "buyer_id": int(getattr(buyer, "id", -1)),
                    "action": "DEAL",
                    "price": final_price,
                    "content": str(resp.get("reason", "")),
                }
            ],
        }
    return {
        "outcome": "failed",
        "reason": f"Arbiter: {resp.get('reason', 'no deal')}",
        "history": [
            {
                "round": 1,
                "party": "arbiter",
                "agent_id": int(getattr(seller, "id", -1)),
                "buyer_id": int(getattr(buyer, "id", -1)),
                "action": "NO_DEAL",
                "price": 0,
                "content": str(resp.get("reason", "")),
            }
        ],
        "final_price": 0,
    }


async def _arbiter_negotiate_async(
    buyer: Agent,
    seller: Agent,
    listing: Dict,
    market_condition: str,
    market_hint: str,
    macro_context: str,
    effective_bid_floor: float,
    llm_model_type: str = "fast",
    config=None,
) -> Dict:
    current_price = float(listing.get("listed_price", 0.0) or 0.0)
    min_price = float(listing.get("min_price", current_price) or current_price)
    buyer_max = float(getattr(buyer.preference, "max_price", 0.0) or 0.0)
    lower = max(min_price, effective_bid_floor)
    upper = min(current_price, buyer_max)
    if upper < lower:
        return {"outcome": "failed", "reason": "Arbiter: no overlap", "history": [], "final_price": 0}

    prompt = f"""
    你是房地产交易仲裁员，只做一次裁决，不要多轮谈判。
    {macro_context}
    【市场】{market_condition} | {market_hint}
    【买方】id={buyer.id}, 预算上限={buyer_max:,.0f}, 风格={getattr(buyer.story, 'negotiation_style', 'balanced')}
    【卖方】id={seller.id}, 底价={min_price:,.0f}, 当前报价={current_price:,.0f}, 风格={getattr(seller.story, 'negotiation_style', 'balanced')}
    【有效价格区间】[{lower:,.0f}, {upper:,.0f}]
    输出JSON: {{"outcome":"DEAL"|"NO_DEAL","final_price":0,"reason":"..."}}
    """
    default_resp = {"outcome": "NO_DEAL", "final_price": 0, "reason": "No agreement"}
    resp = await safe_call_llm_async(
        prompt,
        default_resp,
        system_prompt=SYSTEM_PROMPT_TRANSACTION_ARBITER,
        model_type=llm_model_type,
    )

    outcome = str(resp.get("outcome", "NO_DEAL")).upper()
    raw_price = float(resp.get("final_price", 0) or 0.0)
    mode_cfg = _resolve_arbiter_mode(config)
    if mode_cfg["strict_price_range"]:
        if raw_price < lower or raw_price > upper:
            raw_price = (lower + upper) / 2.0 if mode_cfg["prefer_midpoint_fallback"] else 0.0

    if outcome == "DEAL" and raw_price > 0:
        final_price = max(lower, min(upper, raw_price))
        return {
            "outcome": "success",
            "final_price": final_price,
            "history": [
                {
                    "round": 1,
                    "party": "arbiter",
                    "agent_id": int(getattr(seller, "id", -1)),
                    "buyer_id": int(getattr(buyer, "id", -1)),
                    "action": "DEAL",
                    "price": final_price,
                    "content": str(resp.get("reason", "")),
                }
            ],
        }
    return {
        "outcome": "failed",
        "reason": f"Arbiter: {resp.get('reason', 'no deal')}",
        "history": [
            {
                "round": 1,
                "party": "arbiter",
                "agent_id": int(getattr(seller, "id", -1)),
                "buyer_id": int(getattr(buyer, "id", -1)),
                "action": "NO_DEAL",
                "price": 0,
                "content": str(resp.get("reason", "")),
            }
        ],
        "final_price": 0,
    }


async def _run_classic_competitive_async(
    seller: Agent,
    buyers: List[Agent],
    listing: Dict,
    market: Market,
    month: int = 1,
    config=None,
    llm_model_type: str = "fast",
) -> Dict:
    """
    CLASSIC mode minimal enhancement:
    - Keep LLM as decision maker for buyer/seller actions
    - Use code only for affordability validation and loop control
    """
    if not buyers:
        return {"outcome": "failed", "reason": "No buyer in competitive session", "history": [], "final_price": 0}

    neg_cfg = config.negotiation if config else {}
    rounds_range = neg_cfg.get("rounds_range", [2, 3])
    market_conds = neg_cfg.get("market_conditions", {})
    current_price = float(listing.get("listed_price", 0.0) or 0.0)
    min_price = float(listing.get("min_price", current_price) or current_price)
    try:
        effective_bid_floor_ratio = float(
            config.get("smart_agent.effective_bid_floor_ratio", config.get("effective_bid_floor_ratio", 0.98))
        ) if config else 0.98
    except Exception:
        effective_bid_floor_ratio = 0.98
    effective_bid_floor = max(0.0, min_price * max(0.5, min(1.2, effective_bid_floor_ratio)))

    market_condition = get_market_condition(market, str(listing.get("zone", "A")), len(buyers))
    cond_cfg = market_conds.get(market_condition, {})
    lowball_ratio = float(cond_cfg.get("buyer_lowball", 0.90))
    market_hint = str(cond_cfg.get("llm_hint", "【市场供需平衡】供需相当，价格理性。"))

    rounds = min(random.randint(*rounds_range), int(_m16_guardrails(config)["max_negotiation_rounds_cap"]))
    try:
        max_active_buyers = int(config.get("smart_agent.classic_competitive_max_active_buyers", 6)) if config else 6
    except Exception:
        max_active_buyers = 6
    max_active_buyers = max(2, min(20, max_active_buyers))

    macro_context = build_macro_context(month, config)
    history: List[Dict] = []
    active_buyers: List[Agent] = list(buyers)
    last_round_offers: Dict[int, float] = {}
    best_offer_so_far: Optional[Dict] = None

    for r in range(1, rounds + 1):
        if not active_buyers:
            return {"outcome": "failed", "reason": "All buyers withdrew", "history": history, "final_price": 0}

        round_market_hint = (
            f"{market_hint}\n【本轮事实】参与买家数: {len(active_buyers)}; 当前卖方要价: {current_price:,.0f}; "
            f"卖方底价: {min_price:,.0f}; 有效出价下限: {effective_bid_floor:,.0f}"
        )

        async def _buyer_offer_task(buyer: Agent):
            buyer_max = float(getattr(getattr(buyer, "preference", None), "max_price", 0.0) or 0.0)
            prev_offer = float(last_round_offers.get(int(buyer.id), current_price * lowball_ratio) or 0.0)
            suggested_floor = max(effective_bid_floor, min(current_price, prev_offer))
            prompt = f"""
            {macro_context}
            你是买方Agent {buyer.id}，同房源多人竞争，第{r}/{rounds}轮。

            【交易背景】
            - 你的预算上限: {buyer_max:,.0f}
            - 卖方当前要价: {current_price:,.0f}
            - 卖方底价: {min_price:,.0f}
            - 有效出价下限: {effective_bid_floor:,.0f}
            - 你的上轮报价: {prev_offer:,.0f}

            【市场提示】
            {round_market_hint}

            请输出JSON:
            {{"action":"OFFER"|"WITHDRAW","offer_price":0,"reason":"..."}}
            """
            resp = await safe_call_llm_async(
                prompt,
                {"action": "WITHDRAW", "offer_price": 0, "reason": "LLM Error"},
                system_prompt=SYSTEM_PROMPT_BUYER_NEGOTIATION,
                model_type=llm_model_type,
            )
            action = str(resp.get("action", "WITHDRAW") or "WITHDRAW").upper()
            raw_offer = float(resp.get("offer_price", 0) or 0.0)

            if action != "OFFER":
                return {
                    "buyer": buyer,
                    "action": "WITHDRAW",
                    "offer_price": 0.0,
                    "raw_offer": raw_offer,
                    "clamped": False,
                    "valid_bid": False,
                    "reason": str(resp.get("reason", "")),
                }

            offer = clamp_offer_price(raw_offer, current_price, buyer_max, config=config)
            clamped = abs(offer - raw_offer) > 1e-6
            if offer > buyer_max:
                return {
                    "buyer": buyer,
                    "action": "WITHDRAW",
                    "offer_price": 0.0,
                    "raw_offer": raw_offer,
                    "clamped": clamped,
                    "valid_bid": False,
                    "reason": "Offer above max budget",
                }
            is_affordable, _, _ = check_affordability(buyer, offer, config)
            valid_bid = bool(is_affordable and offer >= effective_bid_floor and offer > 0)
            return {
                "buyer": buyer,
                "action": "OFFER" if valid_bid else "WITHDRAW",
                "offer_price": float(offer if valid_bid else 0.0),
                "raw_offer": raw_offer,
                "clamped": clamped,
                "valid_bid": valid_bid,
                "reason": str(resp.get("reason", "")),
            }

        buyer_results = await asyncio.gather(*[_buyer_offer_task(b) for b in active_buyers])
        this_round_bids = []
        still_active: List[Agent] = []

        for bres in buyer_results:
            buyer = bres["buyer"]
            if bres.get("clamped"):
                history.append({
                    "round": r,
                    "party": "system",
                    "agent_id": int(getattr(buyer, "id", -1)),
                    "action": "M16_CLAMP",
                    "price": float(bres.get("offer_price", 0.0) or 0.0),
                    "content": (
                        f"offer clamped from {float(bres.get('raw_offer', 0.0)):,.0f} "
                        f"to {float(bres.get('offer_price', 0.0)):,.0f}"
                    ),
                })
            history.append({
                "round": r,
                "party": "buyer",
                "agent_id": int(getattr(buyer, "id", -1)),
                "action": str(bres.get("action", "WITHDRAW")),
                "price": float(bres.get("offer_price", 0.0) or 0.0),
                "content": str(bres.get("reason", "")),
            })

            if str(bres.get("action")) == "OFFER" and bool(bres.get("valid_bid")):
                this_round_bids.append({
                    "buyer": buyer,
                    "price": float(bres.get("offer_price", 0.0)),
                })
                still_active.append(buyer)
                last_round_offers[int(buyer.id)] = float(bres.get("offer_price", 0.0))

        if not this_round_bids:
            if best_offer_so_far is not None:
                best_buyer = best_offer_so_far["buyer"]
                best_price = float(best_offer_so_far["price"])
                if best_price < float(min_price):
                    history.append({
                        "round": r,
                        "party": "seller_closeout",
                        "agent_id": int(getattr(seller, "id", -1)),
                        "action": "REJECT",
                        "price": float(best_price),
                        "content": "closeout_auto_reject_below_min_price",
                        "llm_called": 0,
                    })
                    return {"outcome": "failed", "reason": "Best historical bid below min price", "history": history, "final_price": 0}
                closeout_prompt = f"""
                你是卖方Agent {seller.id}。当前第{r}/{rounds}轮没有新增有效报价。
                历史最优有效报价来自买家 {int(getattr(best_buyer, "id", -1))}，价格 {best_price:,.0f}。
                你的底价是 {min_price:,.0f}。

                请做收官决策（只允许二选一）：
                {{"action":"ACCEPT"|"REJECT","reason":"..."}}
                """
                closeout_resp = await safe_call_llm_async(
                    closeout_prompt,
                    {"action": "REJECT", "reason": "No fresh valid bids"},
                    system_prompt=SYSTEM_PROMPT_SELLER_NEGOTIATION,
                    model_type=llm_model_type,
                )
                closeout_action = str(closeout_resp.get("action", "REJECT") or "REJECT").upper()
                history.append({
                    "round": r,
                    "party": "seller_closeout",
                    "agent_id": int(getattr(seller, "id", -1)),
                    "action": closeout_action,
                    "price": float(best_price),
                    "content": str(closeout_resp.get("reason", "")),
                })
                if closeout_action == "ACCEPT" and best_price >= min_price:
                    return {
                        "outcome": "success",
                        "buyer_id": int(getattr(best_buyer, "id", -1)),
                        "final_price": float(best_price),
                        "mode": "classic",
                        "history": history,
                    }
            return {"outcome": "failed", "reason": "No valid bids in round", "history": history, "final_price": 0}

        this_round_bids.sort(key=lambda x: x["price"], reverse=True)
        prices = [float(x["price"]) for x in this_round_bids]
        top_price = prices[0]
        second_price = prices[1] if len(prices) > 1 else prices[0]
        median_price = prices[len(prices) // 2]

        # Research rule: listing is a transaction standard in for_sale state.
        # If highest effective bid already reaches current asking price, auto-close
        # this round to avoid non-natural "above-list yet rejected" behavior.
        try:
            auto_accept_at_list_enabled = bool(
                config.get(
                    "smart_agent.classic_competitive_auto_accept_at_list_enabled",
                    config.get("classic_competitive_auto_accept_at_list_enabled", True),
                )
            ) if config else True
        except Exception:
            auto_accept_at_list_enabled = True
        if auto_accept_at_list_enabled and top_price >= float(current_price):
            winner = this_round_bids[0]
            final_price = float(winner["price"])
            history.append({
                "round": r,
                "party": "system",
                "agent_id": int(getattr(seller, "id", -1)),
                "action": "AUTO_ACCEPT_AT_LIST",
                "price": float(final_price),
                "content": (
                    "最高有效报价已达到/超过挂牌价，"
                    "按挂牌成交规则自动进入成交。"
                ),
                "llm_called": 0,
            })
            if final_price < min_price:
                return {
                    "outcome": "failed",
                    "reason": "Auto-accept blocked: final price below min price",
                    "history": history,
                    "final_price": 0,
                }
            return {
                "outcome": "success",
                "buyer_id": int(getattr(winner["buyer"], "id", -1)),
                "final_price": float(final_price),
                "mode": "classic",
                "history": history,
            }

        if best_offer_so_far is None or float(top_price) > float(best_offer_so_far["price"]):
            best_offer_so_far = {
                "buyer": this_round_bids[0]["buyer"],
                "price": float(top_price),
                "round": int(r),
            }

        seller_memory_panel = _build_seller_memory_panel_from_history(history)
        seller_scorecard = _build_seller_negotiation_scorecard(
            min_price=float(min_price),
            current_price=float(current_price),
            candidate_price=float(top_price),
            market_condition=str(market_condition),
            round_index=int(r),
            total_rounds=int(rounds),
        )
        seller_prompt = f"""
        {macro_context}
        你是卖方Agent {seller.id}，同房源多人竞争，第{r}/{rounds}轮。

        【事实摘要（仅真实可见信息）】
        - 本轮有效报价人数: {len(this_round_bids)}
        - 最高报价: {top_price:,.0f}
        - 次高报价: {second_price:,.0f}
        - 中位报价: {median_price:,.0f}
        - 你当前要价: {current_price:,.0f}
        - 你的底价: {min_price:,.0f}
        - 你的近两个月卖房记忆面板:
        {seller_memory_panel}
        - 成交决策打分卡:
        {seller_scorecard}

        【市场提示】
        {round_market_hint}

        【行动提醒】
        - 若首轮有多名有效报价且最高价与次高价差距不大，优先考虑 HOLD 或 COUNTER 观察下一轮竞争。
        - 若最高价已显著高于次高价且满足你的目标，可 ACCEPT。
        - 若所有报价明显偏离你的预期，可 REJECT。
        - 如果你在本轮选择 REJECT，但当前总分已经偏高（成交优先），你必须解释为什么依然拒绝。

        请输出JSON:
        {{"action":"ACCEPT"|"COUNTER"|"HOLD"|"REJECT","counter_price":0,"reason":"..."}}
        """
        seller_resp = await safe_call_llm_async(
            seller_prompt,
            {"action": "REJECT", "counter_price": 0, "reason": "LLM Error"},
            system_prompt=SYSTEM_PROMPT_SELLER_NEGOTIATION,
            model_type=llm_model_type,
        )
        seller_action = str(seller_resp.get("action", "REJECT") or "REJECT").upper()
        counter_price = float(seller_resp.get("counter_price", current_price) or current_price)

        # Recheck early one-shot decisions once (LLM still decides), to increase
        # signal for multi-round competition in high-interest sessions.
        if (
            seller_action in {"ACCEPT", "REJECT"}
            and _should_run_competitive_seller_recheck(
                route_model=str(route_info.get("model", "")),
                bid_count=int(len(this_round_bids)),
                spread_ratio=float(abs(top_price - second_price) / max(top_price, 1.0)),
                round_index=int(r),
                total_rounds=int(rounds),
                config=config,
            )
        ):
            spread_ratio = abs(top_price - second_price) / max(top_price, 1.0)
            recheck_prompt = f"""
            你正在复核第{r}/{rounds}轮决策（多人竞争同一房源）。
            已有事实：
            - 有效报价人数: {len(this_round_bids)}
            - 最高报价: {top_price:,.0f}
            - 次高报价: {second_price:,.0f}
            - 价差比例: {spread_ratio:.2%}
            - 当前要价: {current_price:,.0f}
            - 底价: {min_price:,.0f}

            你刚才选择: {seller_action}
            若继续 {seller_action}，会立即结束本轮竞争。
            请再次确认是否维持，或改为 COUNTER/HOLD。

            输出JSON:
            {{"action":"ACCEPT"|"COUNTER"|"HOLD"|"REJECT","counter_price":0,"reason":"..."}}
            """
            recheck_resp = await safe_call_llm_async(
                recheck_prompt,
                {"action": seller_action, "counter_price": counter_price, "reason": "Keep previous decision"},
                system_prompt=SYSTEM_PROMPT_SELLER_NEGOTIATION,
                model_type=llm_model_type,
            )
            recheck_action = str(recheck_resp.get("action", seller_action) or seller_action).upper()
            recheck_counter = float(recheck_resp.get("counter_price", counter_price) or counter_price)
            history.append({
                "round": r,
                "party": "seller_recheck",
                "agent_id": int(getattr(seller, "id", -1)),
                "action": recheck_action,
                "price": float(recheck_counter),
                "content": str(recheck_resp.get("reason", "")),
            })
            seller_action = recheck_action
            counter_price = recheck_counter

        seller_action, counter_price, patched_reason = _apply_seller_reject_guardrail(
            seller_action=seller_action,
            counter_price=float(counter_price),
            seller_reason=str(seller_resp.get("reason", "")),
            min_price=float(min_price),
            current_price=float(current_price),
            candidate_price=float(top_price),
            market_condition=str(market_condition),
            round_index=int(r),
            total_rounds=int(rounds),
            config=config,
        )
        if str(patched_reason or "").strip() != str(seller_resp.get("reason", "") or "").strip():
            seller_resp["reason"] = patched_reason

        if seller_action == "COUNTER":
            current_price = max(min_price, counter_price)
        elif seller_action == "ACCEPT":
            # Keep current_price for logging;成交价取本轮最高有效报价。
            current_price = float(current_price)

        history.append({
            "round": r,
            "party": "seller",
            "agent_id": int(getattr(seller, "id", -1)),
            "action": seller_action,
            "price": float(current_price),
            "content": str(seller_resp.get("reason", "")),
        })

        if seller_action == "ACCEPT":
            winner = this_round_bids[0]
            final_price = float(winner["price"])
            if final_price < min_price:
                return {
                    "outcome": "failed",
                    "reason": "Seller accepted below min price (blocked by rule)",
                    "history": history,
                    "final_price": 0,
                }
            return {
                "outcome": "success",
                "buyer_id": int(getattr(winner["buyer"], "id", -1)),
                "final_price": float(final_price),
                "mode": "classic",
                "history": history,
            }

        if seller_action == "REJECT":
            return {"outcome": "failed", "reason": "Seller rejected", "history": history, "final_price": 0}

        # HOLD / COUNTER: keep competitive set, cap active buyers to avoid explosion.
        offer_rank = {int(x["buyer"].id): float(x["price"]) for x in this_round_bids}
        threshold = max(effective_bid_floor, min(current_price, top_price * 0.88))
        filtered = [
            b for b in still_active
            if float(getattr(getattr(b, "preference", None), "max_price", 0.0) or 0.0) >= threshold
        ]
        if not filtered:
            filtered = [this_round_bids[0]["buyer"]]
        filtered.sort(key=lambda b: offer_rank.get(int(b.id), 0.0), reverse=True)
        active_buyers = filtered[:max_active_buyers]

    if best_offer_so_far is not None:
        best_buyer = best_offer_so_far["buyer"]
        best_price = float(best_offer_so_far["price"])
        if best_price < float(min_price):
            history.append({
                "round": rounds,
                "party": "seller_closeout",
                "agent_id": int(getattr(seller, "id", -1)),
                "action": "REJECT",
                "price": float(best_price),
                "content": "closeout_auto_reject_below_min_price",
                "llm_called": 0,
            })
            return {"outcome": "failed", "reason": "Best historical bid below min price", "history": history, "final_price": 0}
        closeout_prompt = f"""
        你是卖方Agent {seller.id}。已达到最大轮次 {rounds}。
        历史最优有效报价来自买家 {int(getattr(best_buyer, "id", -1))}，价格 {best_price:,.0f}。
        你的底价是 {min_price:,.0f}。

        请做最终收官决策（只允许二选一）：
        {{"action":"ACCEPT"|"REJECT","reason":"..."}}
        """
        closeout_resp = await safe_call_llm_async(
            closeout_prompt,
            {"action": "REJECT", "reason": "Max rounds reached"},
            system_prompt=SYSTEM_PROMPT_SELLER_NEGOTIATION,
            model_type=llm_model_type,
        )
        closeout_action = str(closeout_resp.get("action", "REJECT") or "REJECT").upper()
        history.append({
            "round": rounds,
            "party": "seller_closeout",
            "agent_id": int(getattr(seller, "id", -1)),
            "action": closeout_action,
            "price": float(best_price),
            "content": str(closeout_resp.get("reason", "")),
        })
        if closeout_action == "ACCEPT" and best_price >= min_price:
            return {
                "outcome": "success",
                "buyer_id": int(getattr(best_buyer, "id", -1)),
                "final_price": float(best_price),
                "mode": "classic",
                "history": history,
            }
    return {"outcome": "failed", "reason": "Max rounds reached", "history": history, "final_price": 0}


def negotiate(
    buyer: Agent,
    seller: Agent,
    listing: Dict,
    market: Market,
    potential_buyers_count: int = 10,
    config=None,
    month: int = 1,
) -> Dict:
    """
    LLM-driven negotiation with Market Context, Configurable Rounds, and Personality.
    """
    # 1. Configuration & Context Setup
    neg_cfg = config.negotiation if config else {}
    rounds_range = neg_cfg.get('rounds_range', [2, 3])
    gap_threshold = neg_cfg.get('heuristic_gap_threshold', 0.20)
    market_conds = neg_cfg.get('market_conditions', {})

    current_price = listing['listed_price']
    min_price = listing['min_price']
    try:
        effective_bid_floor_ratio = float(
            config.get("smart_agent.effective_bid_floor_ratio", config.get("effective_bid_floor_ratio", 0.98))
        ) if config else 0.98
    except Exception:
        effective_bid_floor_ratio = 0.98
    effective_bid_floor = max(0.0, float(min_price) * max(0.5, min(1.2, effective_bid_floor_ratio)))

    # 2. Heuristic Pre-check (Fail early if gap is too large)
    buyer_max = buyer.preference.max_price
    # Check gap between listed price and buyer max
    # If listed_price is significantly higher than buyer_max, skip
    price_gap = (current_price - buyer_max) / current_price

    # Also check min_price vs buyer_max
    if min_price > buyer_max * (1 + gap_threshold * 2):
        return {"outcome": "failed", "reason": f"Pre-check: Price gap {price_gap:.1%} too large", "history": [], "final_price": 0}

    # 3. Market Condition & Strategy
    market_condition = get_market_condition(market, listing['zone'], potential_buyers_count)

    cond_cfg = market_conds.get(market_condition, {})
    lowball_ratio = cond_cfg.get('buyer_lowball', 0.90)
    market_hint = cond_cfg.get('llm_hint', "【市场供需平衡】供需相当，价格理性。")

    # Macro Environment Context
    macro_context = build_macro_context(month, config)

    arbiter_cfg = _resolve_arbiter_mode(config)
    if arbiter_cfg["enabled"]:
        return _arbiter_negotiate_sync(
            buyer=buyer,
            seller=seller,
            listing=listing,
            market_condition=market_condition,
            market_hint=market_hint,
            macro_context=macro_context,
            effective_bid_floor=effective_bid_floor,
            config=config,
        )

    # history = []
    rounds = min(random.randint(*rounds_range), int(_m16_guardrails(config)["max_negotiation_rounds_cap"]))

    # Starting offer based on configuration
    buyer_offer_price = current_price * lowball_ratio

    negotiation_log = []

    # Agent Styling
    buyer_style = getattr(buyer.story, 'negotiation_style', 'balanced')
    seller_style = getattr(seller.story, 'negotiation_style', 'balanced')

    style_prompts = {
        "aggressive": "你是个激进派。大幅杀价/坐地起价，一言不合就退出，绝不吃亏。",
        "conservative": "你是个保守派。谨慎出价，坚守底线，不轻易冒进。",
        "balanced": "你是个理性派。寻求双赢，愿意适度妥协以达成交易。",
        "desperate": "你是个急迫派。为了快速成交，愿意大幅让步。"
    }

    for r in range(1, rounds + 1):
        # --- Buyer Turn ---
        buyer_floor_lock_mode = bool(r > 1 and float(current_price) <= float(min_price) + 1e-6)
        buyer_floor_lock_hint = (
            "\n        【关键提示】卖方当前报价已经到其系统底价，继续低于该价格出价不会再推动成交。你现在应在“接受当前报价”与“退出本次交易”之间做最终判断。"
            if buyer_floor_lock_mode
            else ""
        )
        buyer_output_schema = (
            '{"action": "ACCEPT"|"WITHDRAW", "reason": "..."}'
            if buyer_floor_lock_mode
            else '{"action": "OFFER"|"ACCEPT"|"WITHDRAW", "offer_price": 0, "reason": "..."}'
        )
        buyer_action_guidance = (
            "- ACCEPT: 接受报价\n"
            "        - WITHDRAW: 放弃 (如果价格太高或你不愿在底价成交)\n"
            "        - 不要继续给低于卖方底价的新报价。"
            if buyer_floor_lock_mode
            else
            "- OFFER: 出价 (可低于、等于或高于当前报价；请按市场状态灵活决策，不要机械压价)\n"
            "        - ACCEPT: 接受报价\n"
            "        - WITHDRAW: 放弃 (如果价格太高或对方太顽固)"
        )
        buyer_prompt = f"""
        {macro_context}
        你是买方Agent {buyer.id}，第{r}/{rounds}轮谈判。
        【你的风格】{buyer_style} - {style_prompts.get(buyer_style, "")}

        【交易背景】
        - 你的预算上限: {buyer.preference.max_price:,.0f}
        - 卖方当前报价: {current_price:,.0f}
        - 卖方底价: {min_price:,.0f}
        - 有效出价下限: {effective_bid_floor:,.0f}
        - 你的上轮出价: {buyer_offer_price:,.0f}

        【市场提示】{market_hint}{buyer_floor_lock_hint}

        【谈判历史】
        {_stable_prompt_json(negotiation_log)}

        决定行动 (请遵循你的风格):
        {buyer_action_guidance}

        市场适配要求:
        - 若当前是卖方市场(undersupply)，允许并鼓励为提高成交概率给出等于或高于当前报价的出价。
        - 若当前是买方市场(oversupply)，可给出明显低于当前报价的出价以测试卖方让步空间。
        - 若当前是均衡市场，优先给出接近市场锚点的理性报价。

        输出JSON: {buyer_output_schema}
        """
        buyer_resp = safe_call_llm(
            buyer_prompt,
            {"action": "WITHDRAW", "offer_price": 0, "reason": "LLM Error"},
            system_prompt=SYSTEM_PROMPT_BUYER_NEGOTIATION,
        )
        buyer_action = buyer_resp.get("action", "WITHDRAW")

        # Validate logic
        if buyer_floor_lock_mode and str(buyer_action).upper() == "OFFER":
            raw_offer = normalize_llm_price_scale(
                float(buyer_resp.get("offer_price", buyer_offer_price) or buyer_offer_price),
                float(current_price),
                float(buyer.preference.max_price),
            )
            if raw_offer >= float(current_price):
                buyer_action = "ACCEPT"
                buyer_offer_price = float(current_price)
            else:
                buyer_action = "WITHDRAW"
                buyer_offer_price = float(raw_offer)
                normalized_reason = str(buyer_resp.get("reason", "") or "").strip()
                suffix = "Rule guard: seller is already at min_price, invalid new offer normalized to WITHDRAW."
                buyer_resp["reason"] = f"{normalized_reason} | {suffix}" if normalized_reason else suffix
        elif buyer_action == "OFFER":
            raw_offer = normalize_llm_price_scale(
                float(buyer_resp.get("offer_price", buyer_offer_price) or buyer_offer_price),
                float(current_price),
                float(buyer.preference.max_price),
            )
            buyer_offer_price = clamp_offer_price(
                raw_offer,
                float(current_price),
                float(buyer.preference.max_price),
                config=config,
            )
            if abs(raw_offer - buyer_offer_price) > 1e-6:
                negotiation_log.append({
                    "round": r,
                    "party": "system",
                    "agent_id": int(getattr(buyer, "id", -1)),
                    "action": "M16_CLAMP",
                    "price": buyer_offer_price,
                    "content": f"offer clamped from {raw_offer:,.0f} to {buyer_offer_price:,.0f}",
                })
            # Enforce constraints
            if buyer_offer_price >= current_price:
                buyer_action = "ACCEPT"
                buyer_offer_price = current_price
            if buyer_offer_price > buyer.preference.max_price:
                buyer_action = "WITHDRAW"

        negotiation_log.append({
            "round": r,
            "party": "buyer",
            "agent_id": int(getattr(buyer, "id", -1)),
            "action": buyer_action,
            "price": buyer_offer_price,
            "content": buyer_resp.get("reason", "")
        })

        if buyer_action == "WITHDRAW":
            return {"outcome": "failed", "reason": "Buyer withdrew", "history": negotiation_log, "final_price": 0}
        if buyer_action == "ACCEPT":
            if float(current_price) < float(min_price):
                return {
                    "outcome": "failed",
                    "reason": "Buyer accepted below min price (blocked by rule)",
                    "history": negotiation_log,
                    "final_price": 0,
                }
            return {"outcome": "success", "final_price": current_price, "history": negotiation_log}

    # --- Seller Turn ---
        seller_memory_panel = _build_seller_memory_panel_from_history(negotiation_log)
        seller_scorecard = _build_seller_negotiation_scorecard(
            min_price=float(min_price),
            current_price=float(current_price),
            candidate_price=float(buyer_offer_price),
            market_condition=str(market_condition),
            round_index=int(r),
            total_rounds=int(rounds),
        )
        seller_output_schema = (
            '{{"action": "ACCEPT"|"REJECT", "reason": "..."}}'
            if is_final_round
            else '{{"action": "ACCEPT"|"COUNTER"|"REJECT", "counter_price": 0, "reason": "..."}}'
        )
        seller_prompt = f"""
        {macro_context}
        你是卖方Agent {seller.id}，第{r}/{rounds}轮谈判。
        【你的风格】{seller_style} - {style_prompts.get(seller_style, "")}

        【交易背景】
        - 你的挂牌底价(系统约束): {min_price:,.0f}
        - 买方最新出价: {buyer_offer_price:,.0f}
        - 当前你的报价: {current_price:,.0f}

        【市场提示】{market_hint}
        {'【趋势建议】市场上涨中，可以坚守价格或适当提价。' if market_condition == 'undersupply' else ''}
        {'【趋势建议】市场低迷，建议适度灵活，避免流拍。' if market_condition == 'oversupply' else ''}

        【谈判历史】
        {_stable_prompt_json(negotiation_log)}

        决定行动 (请遵循你的风格):
        - ACCEPT: 接受买方出价 (如果高于底价或你是急迫型)
        - COUNTER: 还价 (可上调、持平或下调，但必须说明理由并与市场状态一致)
        - REJECT: 拒绝 (价格太低且无意让步)

        市场适配要求:
        - 卖方市场可更强势，允许维持高价或小幅上调。
        - 买方市场应更重视成交概率，倾向通过下调或实质性让步促成交易。
        - 均衡市场优先小步协商，避免极端报价。

        输出JSON: {seller_output_schema}
        """
        seller_resp = safe_call_llm(
            seller_prompt,
            {"action": "REJECT", "counter_price": 0, "reason": "LLM Error"},
            system_prompt=SYSTEM_PROMPT_SELLER_NEGOTIATION,
        )
        seller_action = seller_resp.get("action", "REJECT")
        seller_counter_raw = float(seller_resp.get("counter_price", current_price) or current_price)
        seller_action, seller_counter_raw, patched_reason = _apply_seller_reject_guardrail(
            seller_action=str(seller_action),
            counter_price=float(seller_counter_raw),
            seller_reason=str(seller_resp.get("reason", "")),
            min_price=float(min_price),
            current_price=float(current_price),
            candidate_price=float(buyer_offer_price),
            market_condition=str(market_condition),
            round_index=int(r),
            total_rounds=int(rounds),
            config=config,
        )
        seller_resp["action"] = str(seller_action)
        if str(patched_reason or "").strip() != str(seller_resp.get("reason", "") or "").strip():
            seller_resp["reason"] = patched_reason
        if seller_action == "COUNTER":
            current_price = max(float(min_price), float(seller_counter_raw))
            # Validation
            if current_price <= buyer_offer_price:
                seller_action = "ACCEPT"
                current_price = buyer_offer_price

        if seller_action == "ACCEPT" and float(buyer_offer_price) < float(min_price):
            invalid_accept_reason = str(seller_resp.get("reason", "") or "").strip()
            if not is_final_round:
                seller_action = "COUNTER"
                seller_resp["action"] = "COUNTER"
                current_price = float(min_price)
                suffix = "Rule guard: buyer offer below min price, convert invalid ACCEPT to COUNTER@min_price."
            else:
                seller_action = "REJECT"
                seller_resp["action"] = "REJECT"
                suffix = "Rule guard: buyer offer below min price, final round cannot accept below floor."
            seller_resp["reason"] = f"{invalid_accept_reason} | {suffix}" if invalid_accept_reason else suffix

        negotiation_log.append({
            "round": r,
            "party": "seller",
            "agent_id": int(getattr(seller, "id", -1)),
            "action": seller_action,
            "price": current_price,
            "content": seller_resp.get("reason", "")
        })

        if seller_action == "ACCEPT":
            if float(buyer_offer_price) < float(min_price):
                return {
                    "outcome": "failed",
                    "reason": "Seller accepted below min price (blocked by rule)",
                    "history": negotiation_log,
                    "final_price": 0,
                }
            return {"outcome": "success", "final_price": buyer_offer_price, "history": negotiation_log}
        if seller_action == "COUNTER" and is_final_round:
            buyer_last_look = _run_buyer_final_counter_last_look(
                buyer=buyer,
                seller=seller,
                current_price=float(current_price),
                min_price=float(min_price),
                round_index=int(r),
                total_rounds=int(rounds),
                market_hint=str(market_hint),
                macro_context=str(macro_context),
                negotiation_log=list(negotiation_log),
            )
            buyer_final_action = str(buyer_last_look.get("action", "WITHDRAW") or "WITHDRAW").upper()
            negotiation_log.append({
                "round": r,
                "party": "buyer_final",
                "agent_id": int(getattr(buyer, "id", -1)),
                "action": buyer_final_action,
                "price": float(current_price),
                "content": buyer_last_look.get("reason", ""),
            })
            if buyer_final_action == "ACCEPT":
                return {"outcome": "success", "final_price": float(current_price), "history": negotiation_log}
            return {
                "outcome": "failed",
                "reason": "Buyer withdrew after seller counter",
                "history": negotiation_log,
                "final_price": 0,
            }
        if seller_action == "REJECT":
            return {"outcome": "failed", "reason": "Seller rejected", "history": negotiation_log, "final_price": 0}

    return {"outcome": "failed", "reason": "Max rounds reached", "history": negotiation_log, "final_price": 0}


async def negotiate_async(
    buyer: Agent,
    seller: Agent,
    listing: Dict,
    market: Market,
    potential_buyers_count: int = 10,
    config=None,
    month: int = 1,
    llm_model_type: str = "fast",
) -> Dict:
    """
    Async version of negotiate.
    """
    # 1. Configuration & Context Setup
    neg_cfg = config.negotiation if config else {}
    rounds_range = neg_cfg.get('rounds_range', [2, 3])
    gap_threshold = neg_cfg.get('heuristic_gap_threshold', 0.20)
    market_conds = neg_cfg.get('market_conditions', {})

    current_price = listing['listed_price']
    min_price = listing['min_price']
    try:
        effective_bid_floor_ratio = float(
            config.get("smart_agent.effective_bid_floor_ratio", config.get("effective_bid_floor_ratio", 0.98))
        ) if config else 0.98
    except Exception:
        effective_bid_floor_ratio = 0.98
    effective_bid_floor = max(0.0, float(min_price) * max(0.5, min(1.2, effective_bid_floor_ratio)))

    # 2. Heuristic Pre-check
    buyer_max = buyer.preference.max_price
    price_gap = (current_price - buyer_max) / current_price

    if min_price > buyer_max * (1 + gap_threshold * 2):
        return {"outcome": "failed", "reason": f"Pre-check: Price gap {price_gap:.1%} too large", "history": [], "final_price": 0}

    # 3. Market Condition & Strategy
    market_condition = get_market_condition(market, listing['zone'], potential_buyers_count)
    cond_cfg = market_conds.get(market_condition, {})
    lowball_ratio = cond_cfg.get('buyer_lowball', 0.90)
    market_hint = cond_cfg.get('llm_hint', "【市场供需平衡】供需相当，价格理性。")

    macro_context = build_macro_context(month, config)
    arbiter_cfg = _resolve_arbiter_mode(config)
    if arbiter_cfg["enabled"]:
        return await _arbiter_negotiate_async(
            buyer=buyer,
            seller=seller,
            listing=listing,
            market_condition=market_condition,
            market_hint=market_hint,
            macro_context=macro_context,
            effective_bid_floor=effective_bid_floor,
            llm_model_type=llm_model_type,
            config=config,
        )

    negotiation_log = []
    rounds = min(random.randint(*rounds_range), int(_m16_guardrails(config)["max_negotiation_rounds_cap"]))
    buyer_offer_price = current_price * lowball_ratio

    buyer_style = getattr(buyer.story, 'negotiation_style', 'balanced')
    seller_style = getattr(seller.story, 'negotiation_style', 'balanced')

    style_prompts = {
        "aggressive": "你是个激进派。大幅杀价/坐地起价，一言不合就退出，绝不吃亏。",
        "conservative": "你是个保守派。谨慎出价，坚守底线，不轻易冒进。",
        "balanced": "你是个理性派。寻求双赢，愿意适度妥协以达成交易。",
        "desperate": "你是个急迫派。为了快速成交，愿意大幅让步。"
    }

    for r in range(1, rounds + 1):
        is_final_round = (r == rounds)
        final_round_hint = "\n        ⚡️【最后通牒】这是最后这一轮谈判。如果达不成一致，交易将失败。请慎重决策！" if is_final_round else ""
        buyer_floor_lock_mode = bool(r > 1 and float(current_price) <= float(min_price) + 1e-6)
        buyer_floor_lock_hint = (
            "\n        【关键提示】卖方当前报价已经到其系统底价，继续低于该价格出价不会再推动成交。你现在应在“接受当前报价”与“退出本次交易”之间做最终判断。"
            if buyer_floor_lock_mode
            else ""
        )
        buyer_output_schema = (
            '{"action": "ACCEPT"|"WITHDRAW", "reason": "..."}'
            if buyer_floor_lock_mode
            else '{"action": "OFFER"|"ACCEPT"|"WITHDRAW", "offer_price": 0, "reason": "..."}'
        )
        buyer_action_guidance = (
            "- ACCEPT: 接受报价\n"
            "        - WITHDRAW: 放弃 (如果价格太高或你不愿在底价成交)\n"
            "        - 不要继续给低于卖方底价的新报价。"
            if buyer_floor_lock_mode
            else
            "- OFFER: 出价 (可低于、等于或高于当前报价；请按市场状态灵活决策，不要机械压价)\n"
            "        - ACCEPT: 接受报价\n"
            "        - WITHDRAW: 放弃 (如果价格太高或对方太顽固)"
        )

        # --- Buyer Turn ---
        buyer_prompt = f"""
        {macro_context}
        你是买方Agent {buyer.id}，第{r}/{rounds}轮谈判。
        【你的风格】{buyer_style} - {style_prompts.get(buyer_style, "")}

        【交易背景】
        - 你的预算上限: {buyer.preference.max_price:,.0f}
        - 卖方当前报价: {current_price:,.0f}
        - 卖方底价: {min_price:,.0f}
        - 有效出价下限: {effective_bid_floor:,.0f}
        - 你的上轮出价: {buyer_offer_price:,.0f}

        【市场提示】{market_hint}{final_round_hint}{buyer_floor_lock_hint}

        【谈判历史】
        {_stable_prompt_json(negotiation_log)}

        决定行动 (请遵循你的风格):
        {buyer_action_guidance}

        市场适配要求:
        - 若当前是卖方市场(undersupply)，允许并鼓励为提高成交概率给出等于或高于当前报价的出价。
        - 若当前是买方市场(oversupply)，可给出明显低于当前报价的出价以测试卖方让步空间。
        - 若当前是均衡市场，优先给出接近市场锚点的理性报价。

        输出JSON: {buyer_output_schema}
        """
        buyer_resp = await safe_call_llm_async(
            buyer_prompt,
            {"action": "WITHDRAW", "offer_price": 0, "reason": "LLM Error"},
            system_prompt=SYSTEM_PROMPT_BUYER_NEGOTIATION,
            model_type=llm_model_type,
        )
        buyer_action = buyer_resp.get("action", "WITHDRAW")

        if buyer_floor_lock_mode and str(buyer_action).upper() == "OFFER":
            raw_offer = normalize_llm_price_scale(
                float(buyer_resp.get("offer_price", buyer_offer_price) or buyer_offer_price),
                float(current_price),
                float(buyer.preference.max_price),
            )
            if raw_offer >= float(current_price):
                buyer_action = "ACCEPT"
                buyer_offer_price = float(current_price)
            else:
                buyer_action = "WITHDRAW"
                buyer_offer_price = float(raw_offer)
                normalized_reason = str(buyer_resp.get("reason", "") or "").strip()
                suffix = "Rule guard: seller is already at min_price, invalid new offer normalized to WITHDRAW."
                buyer_resp["reason"] = f"{normalized_reason} | {suffix}" if normalized_reason else suffix
        elif buyer_action == "OFFER":
            raw_offer = normalize_llm_price_scale(
                float(buyer_resp.get("offer_price", buyer_offer_price) or buyer_offer_price),
                float(current_price),
                float(buyer.preference.max_price),
            )
            buyer_offer_price = clamp_offer_price(
                raw_offer,
                float(current_price),
                float(buyer.preference.max_price),
                config=config,
            )
            if abs(raw_offer - buyer_offer_price) > 1e-6:
                negotiation_log.append({
                    "round": r,
                    "party": "system",
                    "agent_id": int(getattr(buyer, "id", -1)),
                    "action": "M16_CLAMP",
                    "price": buyer_offer_price,
                    "content": f"offer clamped from {raw_offer:,.0f} to {buyer_offer_price:,.0f}",
                })
            if buyer_offer_price >= current_price:
                buyer_action = "ACCEPT"
                buyer_offer_price = current_price
            if buyer_offer_price > buyer.preference.max_price:
                buyer_action = "WITHDRAW"

        negotiation_log.append({
            "round": r,
            "party": "buyer",
            "agent_id": int(getattr(buyer, "id", -1)),
            "action": buyer_action,
            "price": buyer_offer_price,
            "content": buyer_resp.get("reason", "")
        })

        if buyer_action == "WITHDRAW":
            return {"outcome": "failed", "reason": "Buyer withdrew", "history": negotiation_log, "final_price": 0}
        if buyer_action == "ACCEPT":
            if float(current_price) < float(min_price):
                return {
                    "outcome": "failed",
                    "reason": "Buyer accepted below min price (blocked by rule)",
                    "history": negotiation_log,
                    "final_price": 0,
                }
            return {"outcome": "success", "final_price": current_price, "history": negotiation_log}

        # --- Seller Turn ---
        seller_final_hint = "\n        ⚡️【最后通牒】这是买家的最终出价。必须决定：接受(ACCEPT) 或 拒绝(REJECT 导致交易失败)。不建议再还价。" if is_final_round else ""
        seller_memory_panel = _build_seller_memory_panel_from_history(negotiation_log)
        seller_scorecard = _build_seller_negotiation_scorecard(
            min_price=float(min_price),
            current_price=float(current_price),
            candidate_price=float(buyer_offer_price),
            market_condition=str(market_condition),
            round_index=int(r),
            total_rounds=int(rounds),
        )

        seller_output_schema = (
            '{{"action": "ACCEPT"|"REJECT", "reason": "..."}}'
            if is_final_round
            else '{{"action": "ACCEPT"|"COUNTER"|"REJECT", "counter_price": 0, "reason": "..."}}'
        )
        seller_prompt = f"""
        {macro_context}
        你是卖方Agent {seller.id}，第{r}/{rounds}轮谈判。
        【你的风格】{seller_style} - {style_prompts.get(seller_style, "")}

        【交易背景】
        - 你的挂牌底价(系统约束): {min_price:,.0f}
        - 买方最新出价: {buyer_offer_price:,.0f}
        - 当前你的报价: {current_price:,.0f}
        - 你的近两个月卖房记忆面板:
        {seller_memory_panel}
        - 成交决策打分卡:
        {seller_scorecard}

        【市场提示】{market_hint}{seller_final_hint}
        {'【趋势建议】市场上涨中，可以坚守价格或适当提价。' if market_condition == 'undersupply' else ''}
        {'【趋势建议】市场低迷，建议适度灵活，避免流拍。' if market_condition == 'oversupply' else ''}

        【谈判历史】
        {_stable_prompt_json(negotiation_log)}

        决定行动 (请遵循你的风格):
        - ACCEPT: 接受买方出价
        - COUNTER: 还价 (可上调、持平或下调，但必须说明理由并与市场状态一致)
        - REJECT: 拒绝

        市场适配要求:
        - 卖方市场可更强势，允许维持高价或小幅上调。
        - 买方市场应更重视成交概率，倾向通过下调或实质性让步促成交易。
        - 均衡市场优先小步协商，避免极端报价。
        - 如果你准备 REJECT，但打分卡显示成交优先度较高，请明确解释“为什么不接受或不让步”。

        输出JSON: {seller_output_schema}
        """
        seller_resp = await safe_call_llm_async(
            seller_prompt,
            {"action": "REJECT", "counter_price": 0, "reason": "LLM Error"},
            system_prompt=SYSTEM_PROMPT_SELLER_NEGOTIATION,
            model_type=llm_model_type,
        )
        seller_action = seller_resp.get("action", "REJECT")
        seller_counter_raw = float(seller_resp.get("counter_price", current_price) or current_price)
        seller_action, seller_counter_raw, patched_reason = _apply_seller_reject_guardrail(
            seller_action=str(seller_action),
            counter_price=float(seller_counter_raw),
            seller_reason=str(seller_resp.get("reason", "")),
            min_price=float(min_price),
            current_price=float(current_price),
            candidate_price=float(buyer_offer_price),
            market_condition=str(market_condition),
            round_index=int(r),
            total_rounds=int(rounds),
            config=config,
        )
        seller_resp["action"] = str(seller_action)
        if str(patched_reason or "").strip() != str(seller_resp.get("reason", "") or "").strip():
            seller_resp["reason"] = patched_reason
        if seller_action == "COUNTER":
            current_price = max(float(min_price), float(seller_counter_raw))
            if current_price <= buyer_offer_price:
                seller_action = "ACCEPT"
                current_price = buyer_offer_price

        if seller_action == "ACCEPT" and float(buyer_offer_price) < float(min_price):
            invalid_accept_reason = str(seller_resp.get("reason", "") or "").strip()
            if not is_final_round:
                seller_action = "COUNTER"
                seller_resp["action"] = "COUNTER"
                current_price = float(min_price)
                suffix = "Rule guard: buyer offer below min price, convert invalid ACCEPT to COUNTER@min_price."
            else:
                seller_action = "REJECT"
                seller_resp["action"] = "REJECT"
                suffix = "Rule guard: buyer offer below min price, final round cannot accept below floor."
            seller_resp["reason"] = f"{invalid_accept_reason} | {suffix}" if invalid_accept_reason else suffix

        negotiation_log.append({
            "round": r,
            "party": "seller",
            "agent_id": int(getattr(seller, "id", -1)),
            "action": seller_action,
            "price": current_price,
            "content": seller_resp.get("reason", "")
        })

        if seller_action == "ACCEPT":
            if float(buyer_offer_price) < float(min_price):
                return {
                    "outcome": "failed",
                    "reason": "Seller accepted below min price (blocked by rule)",
                    "history": negotiation_log,
                    "final_price": 0,
                }
            return {"outcome": "success", "final_price": buyer_offer_price, "history": negotiation_log}
        if seller_action == "COUNTER" and is_final_round:
            buyer_last_look = await _run_buyer_final_counter_last_look_async(
                buyer=buyer,
                seller=seller,
                current_price=float(current_price),
                min_price=float(min_price),
                round_index=int(r),
                total_rounds=int(rounds),
                market_hint=str(market_hint),
                macro_context=str(macro_context),
                negotiation_log=list(negotiation_log),
                llm_model_type=llm_model_type,
            )
            buyer_final_action = str(buyer_last_look.get("action", "WITHDRAW") or "WITHDRAW").upper()
            negotiation_log.append({
                "round": r,
                "party": "buyer_final",
                "agent_id": int(getattr(buyer, "id", -1)),
                "action": buyer_final_action,
                "price": float(current_price),
                "content": buyer_last_look.get("reason", ""),
            })
            if buyer_final_action == "ACCEPT":
                return {"outcome": "success", "final_price": float(current_price), "history": negotiation_log}
            return {
                "outcome": "failed",
                "reason": "Buyer withdrew after seller counter",
                "history": negotiation_log,
                "final_price": 0,
            }
        if seller_action == "REJECT":
            return {"outcome": "failed", "reason": "Seller rejected", "history": negotiation_log, "final_price": 0}

    return {"outcome": "failed", "reason": "Max rounds reached", "history": negotiation_log, "final_price": 0}

# --- 4. Transaction Execution Logic ---


def execute_transaction(
    buyer: Agent,
    seller: Agent,
    property_data: Dict,
    final_price: float,
    market: Market = None,
    config=None,
    skip_affordability_check: bool = False,
    transaction_month: int | None = None,
) -> Optional[Dict]:
    """
    Execute transaction: Transfer ownership, handle money (Cash + Mortgage).
    Returns transaction record dict or None if failed.
    """
    # Reset transient failure marker from previous attempts.
    try:
        setattr(buyer, "_last_tx_error_code", None)
    except Exception:
        pass

    # 1. Financial Check (Double check)
    buyer_costs = FinancialCalculator.calculate_transaction_costs(final_price, config=config, side="buyer")
    seller_costs = FinancialCalculator.calculate_transaction_costs(final_price, config=config, side="seller")
    buyer_total_cost = float(buyer_costs.get("total", 0.0))
    seller_total_cost = float(seller_costs.get("total", 0.0))
    if skip_affordability_check:
        mortgage_cfg = config.mortgage if config else {}
        down_ratio = float(mortgage_cfg.get("down_payment_ratio", 0.3))
        down_payment = max(0.0, float(final_price) * down_ratio)
        loan_amount = max(0.0, float(final_price) - down_payment)
        # Keep a minimal cash floor check to avoid negative-cash artifacts.
        required_cash_now = down_payment + buyer_total_cost
        if float(getattr(buyer, "cash", 0.0) or 0.0) < float(required_cash_now):
            try:
                setattr(buyer, "_last_tx_error_code", "CASH_SHORTFALL_PREQUALIFIED")
            except Exception:
                pass
            logger.error(
                f"Transaction Failed: Buyer {buyer.id} cash shortfall under prequalified settlement "
                f"(need={required_cash_now:,.0f}, have={buyer.cash:,.0f})"
            )
            return None
    else:
        is_affordable, down_payment, loan_amount = check_affordability(
            buyer,
            final_price,
            config,
            extra_cash_needed=buyer_total_cost,
        )
        if not is_affordable:
            try:
                setattr(buyer, "_last_tx_error_code", "AFFORDABILITY_CHECK_FAILED")
            except Exception:
                pass
            logger.error(
                f"Transaction Failed: Buyer {buyer.id} affordability check failed "
                f"(price={final_price:,.0f}, cash={buyer.cash:,.0f}, income={buyer.monthly_income:,.0f})"
            )
            return None

    # 2. Transfer Money
    # Buyer pays down payment + buyer-side transaction costs
    buyer.cash -= (down_payment + buyer_total_cost)

    # Seller receives net proceeds after seller-side transaction costs
    seller.cash += max(0.0, final_price - seller_total_cost)

    # 3. Handle Mortgage
    # Simple mortgage: Add to total_debt, calculate monthly payment
    buyer.total_debt += loan_amount

    # Calculate monthly payment
    # Assume 30 years, interest rate from macro or config
    interest_rate = 0.045  # Default 4.5%
    if market and hasattr(market, "average_mortgage_rate"):
        interest_rate = market.average_mortgage_rate

    years = 30
    monthly_rate = interest_rate / 12
    num_payments = years * 12

    if monthly_rate > 0:
        monthly_payment = loan_amount * (monthly_rate * (1 + monthly_rate)**num_payments) / ((1 + monthly_rate)**num_payments - 1)
    else:
        monthly_payment = loan_amount / num_payments

    buyer.mortgage_monthly_payment += monthly_payment

    # 4. Transfer Ownership
    # Remove from Seller
    # Find property in seller's list
    # Use ID to match
    pid = property_data['property_id']
    seller.owned_properties = [p for p in seller.owned_properties if p['property_id'] != pid]

    # Add to Buyer
    # Update property data
    new_prop_data = property_data.copy()
    new_prop_data['owner_id'] = buyer.id
    new_prop_data['status'] = 'off_market'
    new_prop_data['last_transaction_price'] = final_price
    if transaction_month is not None:
        new_prop_data['acquired_month'] = int(transaction_month)
    # Inherit or reset other fields? base_value might update to transaction price?
    # Usually base_value tracks market value, transaction price is history.
    # Let's update base_value to reflect market recognition?
    # Or keep it separate. Let's keep base_value as is (market dictates it next month).

    buyer.owned_properties.append(new_prop_data)

    # Update Market Object (Global State) if needed
    # market.properties is the source of truth for some lookups
    # props_map or market.properties should be updated.
    # We update the dict object in place if possible, assuming property_data is a reference to the one in market.properties
    property_data['owner_id'] = buyer.id
    property_data['status'] = 'off_market'
    property_data['last_transaction_price'] = final_price
    if transaction_month is not None:
        property_data['acquired_month'] = int(transaction_month)

    logger.info(f"Transaction Executed: Unit {pid} sold from {seller.name}({seller.id}) to {buyer.name}({buyer.id}) @ {final_price:,.0f}")

    return {
        "price": final_price,
        "down_payment": down_payment,
        "loan_amount": loan_amount,
        "buyer_transaction_cost": buyer_total_cost,
        "seller_transaction_cost": seller_total_cost,
        "buyer_id": buyer.id,
        "seller_id": seller.id,
        "property_id": pid
    }


def handle_failed_negotiation(seller: Agent, listing: Dict, market: Market, potential_buyers_count: int = 0) -> bool:
    """
    Handle failed negotiation.
    Seller might lower price if desperate or market is cold.
    Returns True if listing was modified (e.g. price cut).
    """
    # Simple Logic:
    # If no buyers, cut price.
    # If buyers but failed, maybe cut price a little?

    # Check patience/desperation
    # We can check how long it's been listed? listing['listing_month']

    is_desperate = False
    if hasattr(seller, 'life_pressure') and seller.life_pressure == "urgent":
        is_desperate = True

    price_cut = 0.0

    if potential_buyers_count == 0:
        # No interest: Cut price
        price_cut = 0.05  # 5% cut
        if is_desperate:
            price_cut = 0.10

    else:
        # Had interest but failed
        # Maybe price too high?
        # Cut smaller
        price_cut = 0.02
        if is_desperate:
            price_cut = 0.05

    if price_cut > 0:
        old_price = listing['listed_price']
        old_min = float(listing.get('min_price', old_price * 0.95) or (old_price * 0.95))
        ratio = old_min / old_price if old_price else 0.95
        if ratio <= 0 or ratio > 1.0:
            ratio = 0.95
        new_price = old_price * (1 - price_cut)
        listing['listed_price'] = new_price
        listing['min_price'] = min(new_price, max(1.0, new_price * ratio))


        logger.info(f"Seller {seller.id} lowered price of {listing['property_id']} by {price_cut:.0%} to {new_price:,.0f} after failed negotiation.")
        return True

    return False


def developer_quick_sale(buyer: Agent, listing: Dict, config=None, offered_price: float = None) -> Dict:
    """
    Developer property sale logic.
    Developer sells at listed price if buyer can afford it.
    """
    # check_affordability is imported at top of file
    candidate_price = float(offered_price) if offered_price is not None else float(listing['listed_price'])
    min_price = float(listing.get("min_price", listing["listed_price"]) or listing["listed_price"])
    buyer_cap = float(getattr(getattr(buyer, "preference", None), "max_price", candidate_price) or candidate_price)
    price = clamp_offer_price(candidate_price, float(listing['listed_price']), buyer_cap, config=config)
    price = max(float(min_price), float(price))
    m16_clamp = abs(candidate_price - price) > 1e-6

    # Backward-compatible fee handling:
    # - If caller explicitly configures precheck_include_tax_and_fee, respect it.
    # - Otherwise, enable fee-aware check for "full" buyer profiles (has preference.max_price),
    #   while keeping legacy lightweight checks for minimal mock buyers in old tests.
    include_fees = None
    if config is not None:
        try:
            include_fees = config.get("smart_agent.precheck_include_tax_and_fee", None)
        except Exception:
            include_fees = None
    if include_fees is None:
        include_fees = bool(getattr(getattr(buyer, "preference", None), "max_price", None) is not None)
    include_fees = bool(include_fees)

    buyer_total_cost = 0.0
    if include_fees:
        buyer_costs = FinancialCalculator.calculate_transaction_costs(price, config=config, side="buyer")
        buyer_total_cost = float(buyer_costs.get("total", 0.0))
    # Check affordability
    is_affordable, _, _ = check_affordability(
        buyer,
        price,
        config,
        extra_cash_needed=buyer_total_cost,
    )

    if is_affordable:
        return {
            "outcome": "success",
            "final_price": price,
            "buyer_id": buyer.id,
            "reason": "Affordable developer property",
            "history": (
                [{"action": "M16_CLAMP", "raw_bid": candidate_price, "clamped_bid": price}] if m16_clamp else []
            ) + [{"action": "BUY", "price": price, "reason": "Developer Sale"}]
        }
    else:
        return {
            "outcome": "failed",
            "final_price": 0,
            "reason": "Not affordable",
            "history": []
        }


