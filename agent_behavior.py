# =========== Copyright 2023 @ CAMEL-AI.org. All Rights Reserved. ===========
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =========== Copyright 2023 @ CAMEL-AI.org. All Rights Reserved. ===========

"""
Core Logic for Agent Behavior (LLM Driven)
"""
import json
import os
import random
from enum import Enum
from typing import Dict, List, Tuple

from config.settings import MORTGAGE_CONFIG
from models import Agent, AgentStory, Market
from prompts.buyer_prompts import BUYER_PREFERENCE_TEMPLATE
from prompts.seller_prompts import (
    LISTING_STRATEGY_TEMPLATE,
    PRICE_ADJUSTMENT_TEMPLATE,
    PRICE_ADJUSTMENT_TEMPLATE_NORMAL,
)
from prompts.system_prompts import (
    SYSTEM_PROMPT_SELLER_REPRICING,
    SYSTEM_PROMPT_STORY_WRITER,
)

# --- Phase 8: Financial Calculator & New Prompts ---
from services.financial_calculator import FinancialCalculator

# --- LLM Integration ---
from utils.llm_client import safe_call_llm, safe_call_llm_async

# --- 1. Story Generation ---


def _derive_family_stage(agent: Agent) -> str:
    children = sorted(getattr(agent, "children_ages", []) or [])
    if not children:
        return "single_or_couple_no_children" if agent.marital_status != "married" else "married_no_children"

    oldest = children[-1]
    if oldest <= 5:
        return "young_children"
    if oldest <= 9:
        return "primary_school_before_transition"
    if oldest <= 12:
        return "junior_school_transition"
    if oldest <= 15:
        return "senior_school_transition"
    return "post_school_pressure"


def _stable_prompt_json(payload) -> str:
    """Stable JSON string for prompts to reduce non-essential token jitter."""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


TIMING_ROLE_BUY_NOW = "buy_now"
TIMING_ROLE_SELL_NOW = "sell_now"
TIMING_ROLE_SELL_THEN_BUY = "sell_then_buy"
TIMING_ROLE_NEED_WAIT = "need_wait"
TIMING_ROLE_OBSERVE_WAIT = "observe_wait"

VALID_TIMING_ROLES = {
    TIMING_ROLE_BUY_NOW,
    TIMING_ROLE_SELL_NOW,
    TIMING_ROLE_SELL_THEN_BUY,
    TIMING_ROLE_NEED_WAIT,
    TIMING_ROLE_OBSERVE_WAIT,
}

VALID_URGENCY_LEVELS = {"high", "medium", "low"}

_LIFE_SHOCK_KEYWORDS = (
    "结婚",
    "新婚",
    "生子",
    "生娃",
    "怀孕",
    "离婚",
    "搬家",
    "跨城",
    "调动",
    "失业",
    "裁员",
    "大病",
    "重病",
    "老人同住",
)
_DEADLINE_KEYWORDS = ("学区", "入学", "报名", "租约", "到期", "倒计时", "置换", "换房窗口")
_SPACE_PRESSURE_KEYWORDS = ("改善", "换房", "拥挤", "多孩", "二胎", "三胎", "老人同住", "分房")
_ELDERLY_KEYWORDS = ("老人", "父母同住", "赡养", "三代同堂")


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    normalized = str(text or "")
    return any(token in normalized for token in keywords)


def _append_lifecycle_label(labels: List[str], details: List[str], label: str, detail: str) -> None:
    if label not in labels:
        labels.append(label)
        if detail:
            details.append(f"{label}:{detail}")


def derive_decision_urgency(life_pressure: str) -> str:
    normalized = str(life_pressure or "").strip().lower()
    if normalized == "urgent":
        return "high"
    if normalized in {"calm", "opportunistic", "balanced"}:
        return "medium"
    return "low"


def build_behavior_modifier(agent: Agent, decision_profile: str, info_delay_months: int) -> Dict[str, str]:
    profile = str(decision_profile or getattr(agent, "agent_type", "normal") or "normal").strip().lower()
    delay = max(0, int(info_delay_months or 0))
    if profile == "smart":
        return {
            "profile": "smart",
            "visibility": "real_time_structured" if delay <= 0 else f"delayed_{delay}m_but_structured",
            "processing_anchor": "valuation_tradeoff_opportunity_cost",
            "timing_bias": "earlier_or_countercyclical",
            "stability_bias": "stable",
        }
    return {
        "profile": "normal",
        "visibility": "lagged_fragmented" if delay > 0 else "fragmented_realtime",
        "processing_anchor": "headline_social_proof_life_pressure",
        "timing_bias": "lagged_or_following",
        "stability_bias": "swayed",
    }


def build_activation_lifecycle_packet(
    agent: Agent,
    month: int,
    *,
    min_cash_observer: float = 500000.0,
    holding_lock_months: int = 12,
    market_signal_packet: Dict[str, object] | None = None,
) -> Dict[str, object]:
    labels: List[str] = []
    detail_lines: List[str] = []
    market_signal_packet = dict(
        market_signal_packet
        or getattr(agent, "_seller_market_signal_packet", {})
        or {}
    )
    family_stage = str(getattr(getattr(agent, "story", None), "family_stage", "") or "")
    housing_stage = str(getattr(getattr(agent, "story", None), "housing_stage", "") or "")
    education_path = str(getattr(getattr(agent, "story", None), "education_path", "") or "")
    financial_profile = str(getattr(getattr(agent, "story", None), "financial_profile", "") or "")
    purchase_motive = str(getattr(getattr(agent, "story", None), "purchase_motive_primary", "") or "")
    housing_need = str(getattr(getattr(agent, "story", None), "housing_need", "") or "")
    current_event = str(getattr(agent, "current_life_event", "") or "")
    life_events = getattr(agent, "life_events", {}) or {}
    recent_events: List[str] = []
    for candidate in (
        current_event,
        str(life_events.get(month, "") or ""),
        str(life_events.get(max(0, month - 1), "") or ""),
        str(life_events.get(0, "") or ""),
    ):
        candidate = candidate.strip()
        if candidate and candidate not in recent_events:
            recent_events.append(candidate)
    recent_event_text = " | ".join(recent_events)

    if recent_event_text and _contains_any(recent_event_text, _LIFE_SHOCK_KEYWORDS):
        _append_lifecycle_label(labels, detail_lines, "LIFE_SHOCK", recent_events[0])

    household_size = 1
    if str(getattr(agent, "marital_status", "") or "").lower() == "married":
        household_size += 1
    children = list(getattr(agent, "children_ages", []) or [])
    household_size += len(children)
    if _contains_any(housing_need, _ELDERLY_KEYWORDS) or _contains_any(family_stage, ("elder", "sandwich", "grown_children_with_parents")):
        household_size += 1

    owned_properties = list(getattr(agent, "owned_properties", []) or [])
    livable_areas = [float(prop.get("building_area", 0.0) or 0.0) for prop in owned_properties if float(prop.get("building_area", 0.0) or 0.0) > 0]
    primary_area = max(livable_areas) if livable_areas else 0.0
    area_per_person = (primary_area / household_size) if household_size > 0 and primary_area > 0 else 0.0
    space_squeeze = False
    if household_size >= 3 and primary_area > 0 and area_per_person < 22:
        space_squeeze = True
    if _contains_any(housing_need, _SPACE_PRESSURE_KEYWORDS):
        space_squeeze = True
    if family_stage in {"young_family", "young_children", "primary_school_before_transition"} and household_size >= 3 and primary_area > 0 and area_per_person < 28:
        space_squeeze = True
    if housing_stage == "owner_upgrade" and family_stage in {"young_family", "young_children"}:
        space_squeeze = True
    if space_squeeze:
        detail = f"household={household_size}, area_per_person={area_per_person:.1f}" if area_per_person > 0 else f"household={household_size}, need={housing_need[:24]}"
        _append_lifecycle_label(labels, detail_lines, "SPACE_SQUEEZE", detail)

    deadline_pressure = False
    if family_stage in {"primary_school_before_transition", "junior_school_transition", "senior_school_transition"}:
        deadline_pressure = True
    if education_path and education_path != "not_school_sensitive":
        deadline_pressure = deadline_pressure or _contains_any(education_path, ("school", "学区", "priority", "district"))
    if _contains_any(housing_need, _DEADLINE_KEYWORDS) or _contains_any(recent_event_text, _DEADLINE_KEYWORDS):
        deadline_pressure = True
    if deadline_pressure:
        detail = family_stage or education_path or housing_need[:24]
        _append_lifecycle_label(labels, detail_lines, "DEADLINE_PRESSURE", detail)

    monthly_income = float(getattr(agent, "monthly_income", 0.0) or 0.0)
    cash = float(getattr(agent, "cash", 0.0) or 0.0)
    liquidity_ready = False
    if not owned_properties:
        liquidity_ready = cash >= max(float(min_cash_observer or 0.0) * 1.2, monthly_income * 12.0)
    else:
        liquidity_ready = cash >= max(300000.0, monthly_income * 10.0)
    if liquidity_ready:
        _append_lifecycle_label(labels, detail_lines, "LIQUIDITY_READY", f"cash={cash:,.0f}")

    replacement_candidate = bool(owned_properties) and (
        housing_stage in {"owner_upgrade", "multi_property_holder"}
        or purchase_motive in {"upgrade_living", "education_driven", "asset_reallocation"}
        or _contains_any(housing_need, ("换房", "改善", "学区"))
    )
    if replacement_candidate and cash < max(300000.0, monthly_income * 12.0):
        _append_lifecycle_label(labels, detail_lines, "CHAIN_BLOCKED", f"cash={cash:,.0f}")
    elif replacement_candidate and (cash >= max(600000.0, monthly_income * 18.0) or bool(getattr(agent, "sell_completed", 0))):
        _append_lifecycle_label(labels, detail_lines, "CHAIN_UNLOCKED", f"cash={cash:,.0f}")

    for prop in owned_properties:
        acquired_month = int(prop.get("acquired_month", 0) or 0)
        if acquired_month > 0 and (month - acquired_month) < max(1, int(holding_lock_months or 12)):
            months_held = max(0, month - acquired_month)
            _append_lifecycle_label(
                labels,
                detail_lines,
                "RECENTLY_PURCHASED_LOCKED",
                f"property={int(prop.get('property_id', -1) or -1)}, held={months_held}m",
            )
            break

    if market_signal_packet:
        cooldown_active = bool(market_signal_packet.get("cooldown_active", False))
        if cooldown_active:
            _append_lifecycle_label(
                labels,
                detail_lines,
                "SELLER_REWAKE_COOLDOWN",
                str(market_signal_packet.get("cooldown_detail", "recent seller rewake")).strip() or "recent seller rewake",
            )
        else:
            if bool(market_signal_packet.get("local_price_push_window", False)):
                _append_lifecycle_label(
                    labels,
                    detail_lines,
                    "LOCAL_PRICE_PUSH_WINDOW",
                    str(market_signal_packet.get("local_price_push_detail", "recent local real-bid pressure")).strip()
                    or "recent local real-bid pressure",
                )
            if bool(market_signal_packet.get("replacement_old_home_release", False)):
                _append_lifecycle_label(
                    labels,
                    detail_lines,
                    "REPLACEMENT_OLD_HOME_RELEASE",
                    str(market_signal_packet.get("replacement_release_detail", "replacement old home can be released")).strip()
                    or "replacement old home can be released",
                )
            if bool(market_signal_packet.get("scarcity_match_window", False)):
                _append_lifecycle_label(
                    labels,
                    detail_lines,
                    "SCARCITY_MATCH_WINDOW",
                    str(market_signal_packet.get("scarcity_detail", "demand gap matches owned supply")).strip()
                    or "demand gap matches owned supply",
                )

    if financial_profile in {"cashflow_sensitive", "payment_sensitive", "income_stressed"} and "CHAIN_BLOCKED" not in labels and owned_properties:
        _append_lifecycle_label(labels, detail_lines, "CHAIN_BLOCKED", financial_profile)

    if {"LIFE_SHOCK", "DEADLINE_PRESSURE"} & set(labels):
        entry_window = "immediate_window"
    elif {
        "SPACE_SQUEEZE",
        "CHAIN_BLOCKED",
        "CHAIN_UNLOCKED",
        "LIQUIDITY_READY",
        "LOCAL_PRICE_PUSH_WINDOW",
        "REPLACEMENT_OLD_HOME_RELEASE",
        "SCARCITY_MATCH_WINDOW",
    } & set(labels):
        entry_window = "watch_window"
    else:
        entry_window = "background_window"

    summary = " | ".join(detail_lines) if detail_lines else "无明显生命周期窗口标签"
    return {
        "labels": labels,
        "detail_lines": detail_lines,
        "summary": summary,
        "entry_window": entry_window,
        "recent_events": recent_events,
        "household_size": household_size,
        "primary_area": primary_area,
        "area_per_person": round(area_per_person, 2) if area_per_person > 0 else 0.0,
        "market_signal_packet": market_signal_packet,
        "market_signal_labels": [
            label
            for label in (
                "LOCAL_PRICE_PUSH_WINDOW",
                "REPLACEMENT_OLD_HOME_RELEASE",
                "SCARCITY_MATCH_WINDOW",
                "SELLER_REWAKE_COOLDOWN",
            )
            if label in labels
        ],
        "market_signal_summary": " | ".join(
            line
            for line in detail_lines
            if any(
                line.startswith(prefix)
                for prefix in (
                    "LOCAL_PRICE_PUSH_WINDOW:",
                    "REPLACEMENT_OLD_HOME_RELEASE:",
                    "SCARCITY_MATCH_WINDOW:",
                    "SELLER_REWAKE_COOLDOWN:",
                )
            )
        ),
    }


def _derive_housing_stage(agent: Agent) -> str:
    owned = len(getattr(agent, "owned_properties", []) or [])
    if owned <= 0:
        return "starter_no_home"
    if owned == 1:
        return "owner_upgrade"
    return "multi_property_holder"


def _derive_purchase_motive(agent: Agent, family_stage: str, housing_stage: str) -> str:
    if family_stage in {"junior_school_transition", "senior_school_transition"}:
        return "education_driven"
    if housing_stage == "starter_no_home":
        return "starter_home"
    if housing_stage == "owner_upgrade":
        return "upgrade_living"
    if getattr(agent, "monthly_income", 0) >= 120000 or len(getattr(agent, "owned_properties", []) or []) >= 2:
        return "asset_allocation"
    return "balanced_living"


def _derive_education_path(agent: Agent, family_stage: str) -> str:
    income = float(getattr(agent, "monthly_income", 0) or 0)
    assets = float(getattr(agent, "total_assets", getattr(agent, "cash", 0)) or 0)
    if family_stage in {"single_or_couple_no_children", "married_no_children", "post_school_pressure"}:
        return "not_school_sensitive"
    if income >= 120000 or assets >= 8000000:
        return "private_or_international_optional"
    if family_stage in {"junior_school_transition", "senior_school_transition"}:
        return "public_school_district_priority"
    return "public_school_preferred"


def _derive_financial_profile(agent: Agent) -> str:
    income = float(getattr(agent, "monthly_income", 0) or 0)
    cash = float(getattr(agent, "cash", 0) or 0)
    if income <= 0:
        return "income_stressed"
    cash_buffer_months = cash / max(income, 1.0)
    if income >= 100000 and cash_buffer_months >= 24:
        return "cash_rich_high_income"
    if cash_buffer_months < 6:
        return "down_payment_sensitive"
    if cash_buffer_months < 12:
        return "cashflow_sensitive"
    return "balanced_finance"


def _derive_seller_profile(agent: Agent, housing_stage: str) -> str:
    owned = len(getattr(agent, "owned_properties", []) or [])
    if owned >= 3:
        return "portfolio_rebalancer"
    if housing_stage == "owner_upgrade":
        return "owner_occupier_upgrade"
    if getattr(agent, "monthly_income", 0) <= 8000 and getattr(agent, "cash", 0) <= 300000:
        return "cash_pressure_seller"
    return "patient_holder"


def _derive_tolerance_ratios(agent: Agent, financial_profile: str):
    income = float(getattr(agent, "monthly_income", 0) or 0)
    cash = float(getattr(agent, "cash", 0) or 0)
    owned = len(getattr(agent, "owned_properties", []) or [])
    payment_ratio = 0.45
    down_ratio = 0.30

    if financial_profile == "income_stressed":
        payment_ratio, down_ratio = 0.35, 0.35
    elif financial_profile == "down_payment_sensitive":
        payment_ratio, down_ratio = 0.40, 0.22
    elif financial_profile == "cashflow_sensitive":
        payment_ratio, down_ratio = 0.42, 0.25
    elif financial_profile == "cash_rich_high_income":
        payment_ratio, down_ratio = 0.50, 0.40

    if income >= 120000 and owned >= 2 and cash >= 3000000:
        payment_ratio = max(payment_ratio, 0.48)
        down_ratio = max(down_ratio, 0.35)

    return round(payment_ratio, 3), round(down_ratio, 3)


def _school_urgency_score(agent: Agent) -> int:
    family_stage = getattr(agent.story, "family_stage", "") or ""
    education_path = getattr(agent.story, "education_path", "") or ""
    if education_path == "not_school_sensitive":
        return 0
    if family_stage == "senior_school_transition":
        return 3
    if family_stage == "junior_school_transition":
        return 2
    if family_stage == "primary_school_before_transition":
        return 1
    return 0


def _starter_home_priority(agent: Agent) -> float:
    motive = str(getattr(agent.story, "purchase_motive_primary", "") or "").lower()
    housing_stage = str(getattr(agent.story, "housing_stage", "") or "").lower()
    if motive == "starter_home":
        return 1.0
    if housing_stage in {"starter_no_home", "rent_to_buy", "new_family_first_home"}:
        return 0.8
    if getattr(agent, "owns_home", False) is False:
        return 0.6
    return 0.0


def _persona_snapshot(agent: Agent) -> Dict[str, object]:
    story = getattr(agent, "story", None)
    return {
        "purchase_motive_primary": str(getattr(story, "purchase_motive_primary", "") or ""),
        "housing_stage": str(getattr(story, "housing_stage", "") or ""),
        "family_stage": str(getattr(story, "family_stage", "") or ""),
        "education_path": str(getattr(story, "education_path", "") or ""),
        "financial_profile": str(getattr(story, "financial_profile", "") or ""),
        "seller_profile": str(getattr(story, "seller_profile", "") or ""),
        "payment_tolerance_ratio": float(getattr(story, "payment_tolerance_ratio", 0.0) or 0.0),
        "down_payment_tolerance_ratio": float(getattr(story, "down_payment_tolerance_ratio", 0.0) or 0.0),
    }


def _buyer_school_need(agent: Agent) -> bool:
    family_stage = str(getattr(agent.story, "family_stage", "") or "").lower()
    education_path = str(getattr(agent.story, "education_path", "") or "").lower()
    motive = str(getattr(agent.story, "purchase_motive_primary", "") or "").lower()
    school_urgency = _school_urgency_score(agent)
    if education_path == "public_school_district_priority" and school_urgency >= 2:
        return True
    if motive == "education_driven" and education_path != "international_private_route":
        return True
    return agent.story.education_need != "无" or agent.has_children_near_school_age()


def _buyer_zone_baseline(agent: Agent, real_max_price: float, zone_a_avg: float, zone_b_avg: float) -> str:
    motive = str(getattr(agent.story, "purchase_motive_primary", "") or "").lower()
    housing_stage = str(getattr(agent.story, "housing_stage", "") or "").lower()
    family_stage = str(getattr(agent.story, "family_stage", "") or "").lower()
    financial_profile = str(getattr(agent.story, "financial_profile", "") or "").lower()
    education_path = str(getattr(agent.story, "education_path", "") or "").lower()
    school_need = _buyer_school_need(agent)

    zone_a_pressure = zone_a_avg / max(real_max_price, 1.0)
    zone_b_pressure = zone_b_avg / max(real_max_price, 1.0)
    starter_priority = _starter_home_priority(agent)

    if motive in {"starter_home", "marriage_home"} or housing_stage in {
        "starter_no_home", "rent_to_buy", "new_family_first_home"
    }:
        if zone_b_pressure <= 1.15:
            return "B"

    if financial_profile in {"cash_tight", "mortgage_sensitive", "down_payment_sensitive", "cashflow_sensitive"}:
        if zone_b_pressure <= 1.25:
            return "B"

    if school_need and education_path == "public_school_district_priority" and zone_a_pressure <= 1.05:
        return "A"

    if motive == "asset_allocation" and zone_a_pressure <= 1.2:
        return "A"

    if family_stage in {"children_far_from_school_window", "single_or_couple_no_children"} and starter_priority >= 0.4:
        return "B"

    if zone_a_pressure <= 0.78:
        return "A"
    if zone_b_pressure <= 1.30:
        return "B"
    return "A" if zone_a_pressure <= zone_b_pressure else "B"


def generate_agent_story(agent: Agent, config=None, occupation_hint: str = None) -> AgentStory:
    """
    Generate background story and structured attributes for a new agent.
    """
    # 1. Investment Style (Personality) Selection
    weights = {'balanced': 0.4}  # default
    if config:
        weights = config.negotiation.get('personality_weights', {
            'aggressive': 0.30, 'conservative': 0.30,
            'balanced': 0.40
        })

    styles = list(weights.keys())
    probs = list(weights.values())
    investment_style = random.choices(styles, weights=probs, k=1)[0]

    # Logic Consistency Fix (Tier 6)
    prop_count = len(agent.owned_properties)
    has_properties = prop_count > 0
    total_asset_est = agent.cash + sum(p['current_valuation'] for p in agent.owned_properties) if has_properties else agent.cash
    family_stage = _derive_family_stage(agent)
    housing_stage = _derive_housing_stage(agent)
    purchase_motive_primary = _derive_purchase_motive(agent, family_stage, housing_stage)
    education_path = _derive_education_path(agent, family_stage)
    financial_profile = _derive_financial_profile(agent)
    seller_profile = _derive_seller_profile(agent, housing_stage)
    payment_tolerance_ratio, down_payment_tolerance_ratio = _derive_tolerance_ratios(agent, financial_profile)

    occ_str = f"建议职业: {occupation_hint}" if occupation_hint else ""

    prompt = f"""
    为这个Agent生成背景故事：
    【基础信息】
    年龄：{agent.age}
    婚姻：{agent.marital_status}
    月收入：{agent.monthly_income:,.0f}
    现金：{agent.cash:,.0f}
    {occ_str}
    【关键资产】
    持有房产数量：{prop_count} 套
    总资产预估：{total_asset_est:,.0f}

    【强制约束】
    1. 若持有房产({prop_count} > 0)，严禁在 story/housing_need 中描述为“无房刚需”、“首次置业”或“租房居住”。必须描述为“改善型需求”或“投资客”。
    2. 若现金充裕(>100w)且有房，严禁描述为“积蓄不多”。
    3. 住房需求(housing_need)的可选值：刚需(仅限无房), 改善(有房但小), 投资(有钱有房), 学区(有娃).
    4. 你还需要额外输出以下结构化画像字段：
       - purchase_motive_primary: starter_home / upgrade_living / education_driven / asset_allocation / balanced_living
       - housing_stage: starter_no_home / owner_upgrade / multi_property_holder
       - family_stage: single_or_couple_no_children / married_no_children / young_children / primary_school_before_transition / junior_school_transition / senior_school_transition / post_school_pressure
       - education_path: not_school_sensitive / public_school_preferred / public_school_district_priority / private_or_international_optional
       - financial_profile: income_stressed / down_payment_sensitive / cashflow_sensitive / balanced_finance / cash_rich_high_income
       - seller_profile: cash_pressure_seller / owner_occupier_upgrade / portfolio_rebalancer / patient_holder
    5. 学区偏好不能简单等于“有孩子”或“高收入”。只有在关键升学窗口，公办学区优先才应明显增强。
    6. 无房首次上车群体应允许偏向总价更低、压力更小的B区房，而不是默认执着A区。

    请包含：occupation(职业), career_outlook(职业前景), family_plan(家庭规划), education_need(教育需求), housing_need(住房需求), selling_motivation(卖房动机), background_story(3-5句故事).

    另外，请为该人物设定一个投资风格 (investment_style)，可选值:
    - aggressive (激进): 愿意承担风险，追求高回报
    - conservative (保守): 厌恶风险，追求本金安全
    - balanced (平衡): 权衡风险与收益
    (建议风格: {investment_style})

    输出JSON格式。
    """

    default_story = AgentStory(
        occupation=occupation_hint if occupation_hint else "普通职员",
        career_outlook="稳定",
        family_plan="暂无",
        education_need="无",
        housing_need="刚需",
        selling_motivation="无",
        background_story="普通工薪阶层。",
        investment_style="balanced",
        purchase_motive_primary=purchase_motive_primary,
        housing_stage=housing_stage,
        family_stage=family_stage,
        education_path=education_path,
        financial_profile=financial_profile,
        seller_profile=seller_profile,
    )

    result = safe_call_llm(
        prompt,
        default_story,
        system_prompt=SYSTEM_PROMPT_STORY_WRITER,
        model_type="fast",
    )
    agent.payment_tolerance_ratio = payment_tolerance_ratio
    agent.down_payment_tolerance_ratio = down_payment_tolerance_ratio

    # If result is dict (success), map to AgentStory
    if isinstance(result, dict):
        return AgentStory(
            occupation=result.get("occupation", "自由职业"),
            career_outlook=result.get("career_outlook", "未知"),
            family_plan=result.get("family_plan", "未知"),
            education_need=result.get("education_need", "无"),
            housing_need=result.get("housing_need", "刚需"),
            selling_motivation=result.get("selling_motivation", "无"),
            background_story=result.get("background_story", "平凡的一生。"),
            investment_style=result.get("investment_style", investment_style),
            purchase_motive_primary=result.get("purchase_motive_primary", purchase_motive_primary),
            housing_stage=result.get("housing_stage", housing_stage),
            family_stage=result.get("family_stage", family_stage),
            education_path=result.get("education_path", education_path),
            financial_profile=result.get("financial_profile", financial_profile),
            seller_profile=result.get("seller_profile", seller_profile),
        )
    return result


def determine_psychological_price(agent: Agent, market_avg_price: float, market_trend: str) -> float:
    """
    Calculate psychological price based on agent personality and market trend.
    Returns the price/sqm or total price depending on input market_avg_price.
    Assumes market_avg_price is TOTAL price for a typical unit in target zone.
    """
    style = agent.story.investment_style

    # Coefficients
    #          Bear    Bull    Stable
    # Aggr     0.80    1.10    1.02
    # Cons     0.70    1.05    0.98
    # Bal      0.90    1.02    1.00

    coeffs = {
        "aggressive": {"UP": 1.10, "DOWN": 0.80, "PANIC": 0.70, "STABLE": 1.02},
        "conservative": {"UP": 1.05, "DOWN": 0.70, "PANIC": 0.60, "STABLE": 0.95},
        "balanced": {"UP": 1.02, "DOWN": 0.90, "PANIC": 0.80, "STABLE": 1.00}
    }

    # Map trend string if needed (assuming "UP", "DOWN", "STABLE", "PANIC")
    # market_trend usually comes from MarketBulletin or MarketService
    trend = market_trend.upper()
    if trend not in coeffs["balanced"]:
        trend = "STABLE"

    coeff = coeffs.get(style, coeffs["balanced"]).get(trend, 1.0)

    return market_avg_price * coeff


def calculate_financial_limits(agent, market=None, market_trend="STABLE"):
    """
    Sync helper to calculate max_affordable_price and psychological_price.
    Used for rehydration without LLM.
    Returns (real_max_price, psych_price, final_operational_max)
    """
    from mortgage_system import calculate_max_affordable

    # Zone Averages
    zone_b_avg = market.get_avg_price("B") if market else 2000000

    # Affordability
    existing_payment = float(
        getattr(agent, 'mortgage_monthly_payment', getattr(agent, 'monthly_payment', 0)) or 0
    )
    real_max_price = calculate_max_affordable(agent.cash, agent.monthly_income, existing_payment)

    psych_price = determine_psychological_price(agent, zone_b_avg, market_trend)
    final_operational_max = real_max_price

    return real_max_price, psych_price, final_operational_max


async def generate_buyer_preference(agent, market, current_month, macro_summary, market_trend, db_conn=None, recent_bulletins=None):
    """
    Tier 7.2: Generate buyer preference with Comparative Logic & Market Memory.
    Returns: (BuyerPreference, thought_process_str, context_metrics)
    """
    from models import BuyerPreference
    from mortgage_system import calculate_max_affordable, calculate_monthly_payment

    # 1. Config & Attributes
    risk_free_rate = 0.03  # Default
    if hasattr(market, 'config') and market.config:
        risk_free_rate = market.config.market.get('risk_free_rate', 0.03)

    # Zone Averages
    zone_a_avg = market.get_avg_price("A") if market else 100000
    zone_b_avg = market.get_avg_price("B") if market else 50000

    # Affordability
    existing_payment = float(
        getattr(agent, 'mortgage_monthly_payment', getattr(agent, 'monthly_payment', 0)) or 0
    )
    real_max_price = calculate_max_affordable(agent.cash, agent.monthly_income, existing_payment)

    psych_price = determine_psychological_price(agent, zone_b_avg, market_trend)
    final_operational_max = real_max_price

    # Zone Logic
    # Keep this as a soft recommendation only. Buyers should prefer B-zone as a
    # starter-home path more often, unless their life stage and education path
    # justify a stronger A-zone pull.
    has_school_need = _buyer_school_need(agent)
    default_zone = _buyer_zone_baseline(agent, real_max_price, zone_a_avg, zone_b_avg)

    # 2. Query Developer Properties (Fix for cross-zone matching)
    developer_properties = [p for p in market.properties if p.get('owner_id') == -1 and p.get('status') == 'for_sale']
    
    # Calculate developer property stats by zone
    dev_stats = {}
    for zone in ['A', 'B']:
        zone_dev_props = [p for p in developer_properties if p.get('zone') == zone]
        if zone_dev_props:
            avg_price = sum(p.get('listed_price', 0) for p in zone_dev_props) / len(zone_dev_props)
            avg_unit_price = sum(p.get('price_per_sqm', 0) for p in zone_dev_props) / len(zone_dev_props)
            dev_stats[zone] = {
                'count': len(zone_dev_props),
                'avg_price': avg_price,
                'avg_unit_price': avg_unit_price
            }
    
    # Calculate discount percentage vs. market average
    market_avg_price = (zone_a_avg + zone_b_avg) / 2
    for zone, stats in dev_stats.items():
        discount_pct = ((stats['avg_price'] - market_avg_price) / market_avg_price) * 100
        stats['discount_pct'] = discount_pct

    # Dynamic market signal for weight adjustment
    min_dev_discount_pct = 0.0
    if dev_stats:
        min_dev_discount_pct = min(v.get('discount_pct', 0.0) for v in dev_stats.values())
    deep_discount = min_dev_discount_pct <= -20
    mild_discount = min_dev_discount_pct <= -10
    
    # 3. Market Memory / History Construction
    history_text = "【近期市场走势】\n(暂无历史数据)"
    if recent_bulletins:
        history_lines = []
        for b in recent_bulletins:
            # Format: Month X: Price Y, Vol Z, Trend T
            history_lines.append(f"- 月份{b['month']}: 均价{b['avg_price']:,.0f}, 成交{b['volume']}, 趋势{b['trend']}")
        history_text = "【近期市场走势】\n" + "\n".join(history_lines)

    # 4. Construct Developer Property Info Text
    dev_info_text = ""
    if dev_stats:
        dev_info_text = "\n\n【🏗️ 开发商特供房源】\n"
        for zone, stats in dev_stats.items():
            indicator = "🔥超值!" if stats['discount_pct'] < -20 else "✅优惠" if stats['discount_pct'] < 0 else ""
            dev_info_text += f"- {zone}区: {stats['count']}套，均价¥{stats['avg_price']:,.0f}，单价¥{stats['avg_unit_price']:,.0f}/㎡，较市场{stats['discount_pct']:+.0f}% {indicator}\n"
    
    # 5. Financial Calculations (Phase 8)
    # Estimate rental yield for the target zone (Avg Rent / Avg Price)
    # This assumes we have average rent data or can estimate it
    # For now, let's look up a typical rental yield from market props if possible, or mock it
    # We can fetch avg unit price and avg rental price from properties_market if DB passed?
    # Simpler: use properties in market object

    target_zone_props = [p for p in market.properties if p['zone'] == default_zone]
    avg_price = zone_a_avg if default_zone == 'A' else zone_b_avg
    avg_rent = 0
    if target_zone_props:
        # Simple avg of rental_price if exists, else estimate
        # Assuming rental_price is populated Phase 7
        total_rent = sum(p.get('rental_price', p['base_value'] * 0.0015) for p in target_zone_props)  # Fallback 1.5% yield monthly? No 1.5/12%
        avg_rent = total_rent / len(target_zone_props)

    # If no data, use rough 2% annual yield estimate
    if avg_rent == 0:
        avg_rent = avg_price * 0.02 / 12

    rental_yield = FinancialCalculator.calculate_rental_yield(avg_price, avg_rent)

    # Calculate estimated monthly payment for a max price purchase
    est_loan = real_max_price * 0.7  # Assuming 30% down
    annual_rate = MORTGAGE_CONFIG.get('annual_interest_rate', 0.05)
    est_monthly_payment = calculate_monthly_payment(est_loan, annual_rate, 30)  # 30 years

    dti = 0
    if agent.monthly_income > 0:
        dti = est_monthly_payment / agent.monthly_income

    affordability_warning = ""
    if dti > 0.5:
        affordability_warning = f"警告: 预计月供占收入 {dti:.1%}，压力巨大！"

    # 5.5 Life Stage Hints (Phase 2.5)
    family_stage = str(getattr(agent.story, "family_stage", "") or "")
    housing_stage = str(getattr(agent.story, "housing_stage", "") or "")
    financial_profile = str(getattr(agent.story, "financial_profile", "") or "")
    purchase_motive_primary = str(getattr(agent.story, "purchase_motive_primary", "") or "")
    education_path = str(getattr(agent.story, "education_path", "") or "")
    life_stage_hints = (
        f"【画像提示】主购房动机={purchase_motive_primary}；住房阶段={housing_stage}；"
        f"家庭阶段={family_stage}；教育路径={education_path}；财务结构={financial_profile}。"
    )
    if has_school_need:
        life_stage_hints += " 当前确有教育窗口，学区因素可以显著提高，但不要无视预算约束。"
    elif _starter_home_priority(agent) >= 0.8:
        life_stage_hints += " 当前更像刚需上车，应优先考虑可承受总价、现金流安全和B区上车机会。"
    elif purchase_motive_primary == "asset_allocation":
        life_stage_hints += " 当前更像资产配置，应看重流动性、收益性与持有成本，不应机械追逐学区。"

    # 6. Construct Prompt using Template
    # 6. Layered baseline for education weight (profile-aware)
    style = getattr(agent.story, 'investment_style', 'balanced')
    if has_school_need:
        edu_min, edu_max, edu_base = 7, 10, 8
    elif purchase_motive_primary == "starter_home":
        edu_min, edu_max, edu_base = 1, 4, 2
    elif education_path == "international_private_route":
        edu_min, edu_max, edu_base = 1, 3, 2
    elif style == "aggressive":
        edu_min, edu_max, edu_base = 2, 5, 3
    elif style == "conservative":
        edu_min, edu_max, edu_base = 4, 7, 5
    else:
        edu_min, edu_max, edu_base = 3, 6, 4

    # Dynamic contextual adjustment under developer fire sale
    edu_relax = 2 if deep_discount else 1 if mild_discount else 0
    effective_edu_base = max(1, edu_base - edu_relax)
    effective_edu_min = max(1, edu_min - edu_relax)
    effective_edu_max = max(effective_edu_min, edu_max)
    suggested_price_sensitivity = min(10, 5 + edu_relax)

    prompt = BUYER_PREFERENCE_TEMPLATE.format(
        background=agent.story.background_story,
        investment_style=agent.story.investment_style,
        cash=agent.cash,
        income=agent.monthly_income,
        max_price=real_max_price,
        macro_summary=macro_summary,
        market_trend=market_trend,
        risk_free_rate=risk_free_rate,
        history_text=history_text,
        dev_info_text=dev_info_text,  # New: Developer property info
        life_stage_hints=life_stage_hints,  # New: V5 negotiation deep core
        default_zone=default_zone,
        zone_a_avg=zone_a_avg,
        zone_b_avg=zone_b_avg,
        rental_yield=rental_yield,
        est_monthly_payment=est_monthly_payment,
        dti=dti,
        affordability_warning=affordability_warning,
        layered_education_hint=(
            f"【权重分层建议】你的教育执念建议区间: {effective_edu_min}-{effective_edu_max} "
            f"(基线{edu_base}, 特价修正-{edu_relax})。"
        ),
        dynamic_tradeoff_hint=(
            f"【情境动态调权】开发商最低折价约 {min_dev_discount_pct:+.1f}% ，"
            f"可适度提升价格敏感度（建议 price_sensitivity≈{suggested_price_sensitivity}），"
            f"但需保持核心区与地段稀缺性的基本优先级。"
            if dev_stats else "【情境动态调权】当前无开发商特价信号，保持原始权重偏好。"
        )
    )

    # Prepare Context Metrics for Logging
    context_metrics = {
        "risk_free_rate": risk_free_rate,
        "est_rental_yield": rental_yield,
        "yield_gap": rental_yield - risk_free_rate,
        "est_monthly_payment": est_monthly_payment,
        "dti_ratio": dti,
        "real_max_price": real_max_price
    }

    # Call LLM
    from utils.llm_client import safe_call_llm_async

    default_data = {
        "target_zone": default_zone,
        "max_price": final_operational_max,
        "target_buy_price": min(final_operational_max, psych_price),
        "max_wait_months": 6,
        "risk_mode": "balanced",
        "min_bedrooms": 1,
        "education_weight": effective_edu_base,
        "comfort_weight": 5,
        "price_sensitivity": suggested_price_sensitivity,
        "investment_motivation": "medium",
        "strategy_reason": "Default logic due to error"
    }

    data = await safe_call_llm_async(prompt, default_return=default_data, model_type="smart")

    # Parse Result
    try:
        # Determine school need from agent story/family
        need_school = has_school_need
        llm_edu_weight = int(data.get("education_weight", effective_edu_base))
        llm_price_sensitivity = int(data.get("price_sensitivity", suggested_price_sensitivity))
        # Clamp with layered profile range; allow mild relax under special-sale context.
        bounded_edu_weight = max(effective_edu_min, min(effective_edu_max, llm_edu_weight))
        bounded_price_sensitivity = max(1, min(10, llm_price_sensitivity))

        # Phase 7.2 Enhancement: Return explicit structure
        pref = BuyerPreference(
            target_zone=data.get("target_zone", default_zone),
            target_price_range=(0, data.get("max_price", final_operational_max)),
            min_bedrooms=data.get("min_bedrooms", 1),
            need_school_district=need_school,
            max_affordable_price=real_max_price,
            psychological_price=psych_price,
            education_weight=bounded_edu_weight,
            comfort_weight=data.get("comfort_weight", 5),
            price_sensitivity=bounded_price_sensitivity
        )
        pref.max_price = data.get("max_price", final_operational_max)
        # Optional long-cycle fields (M12/M17)
        try:
            pref.target_buy_price = float(data.get("target_buy_price", min(pref.max_price, psych_price)))
        except Exception:
            pref.target_buy_price = min(pref.max_price, psych_price)
        pref.target_buy_price = max(0.0, min(pref.max_price, pref.target_buy_price))
        try:
            pref.max_wait_months = int(data.get("max_wait_months", 6))
        except Exception:
            pref.max_wait_months = 6
        pref.max_wait_months = max(1, min(12, pref.max_wait_months))
        risk_mode_raw = str(data.get("risk_mode", "balanced")).lower()
        if risk_mode_raw not in ("conservative", "balanced", "aggressive"):
            risk_mode_raw = "balanced"
        pref.risk_mode = risk_mode_raw

        reason = data.get("strategy_reason", "LLM Decision")

        # Return Tuple 3: Pref, Reason, ContextMetrics
        return pref, reason, context_metrics

    except Exception as e:
        # logging import might be needed if not in scope
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Failed to parse LLM buyer preference: {e}")
        # Fallback
        # Determine school need from agent story/family (Safe check)
        try:
            need_school = agent.story.education_need != "无" or agent.has_children_near_school_age()
        except BaseException:
            need_school = False

        pref = BuyerPreference(
            target_zone=default_zone,
            target_price_range=(0, final_operational_max),
            min_bedrooms=1,
            need_school_district=need_school,
            max_affordable_price=real_max_price,
            psychological_price=psych_price,
            education_weight=8 if need_school else 3,
            comfort_weight=5,
            price_sensitivity=5
        )
        pref.max_price = final_operational_max
        pref.target_buy_price = min(final_operational_max, psych_price)
        pref.max_wait_months = 6
        pref.risk_mode = "balanced"
        return pref, "Fallback decision", context_metrics


def generate_real_thought(agent: Agent, trigger: str, market: Market) -> str:
    """
    Generate a human-readable thought process.
    """
    # Ensure market prices are accessible
    try:
        zone_a_price = market.get_avg_price("A")
        # zone_b_price = market.get_avg_price("B")
    except BaseException:
        zone_a_price = 0
        # zone_b_price = 0

    # prompt = f"""
    # 你是Agent {agent.id}。
    # 【背景】{agent.story.background_story}
    # 【触发】{trigger}
    # 【市场】A区均价{zone_a_price:,.0f}，B区均价{zone_b_price:,.0f}
    #
    # 请思考你的决策（简短一段话）：
    # """
    # For now, return a formatted string. Real LLM would yield varied text.
    return f"我是{agent.story.occupation}，看到{trigger}，考虑到当前A区均价{zone_a_price / 10000:.0f}万，我决定..."

# --- 2. Event System ---


def select_monthly_event(agent: Agent, month: int, config=None) -> dict:
    """
    Select a life event for the agent for this month.
    """
    event_pool = []
    if config:
        event_pool = config.life_events.get('pool', [])

    if not event_pool:
        # Fallback or return None
        return {"event": None, "reasoning": "No event pool or config", "llm_called": False}

    trigger_prob = 0.25
    llm_reasoning_enabled = True
    if config:
        trigger_prob = float(config.get("life_events.monthly_event_trigger_prob", 0.25))
        llm_reasoning_enabled = bool(config.get("life_events.llm_reasoning_enabled", True))
    trigger_prob = max(0.0, min(1.0, trigger_prob))

    # Phase 1: cheap code-side trigger gate (0 token when not triggered)
    if random.random() > trigger_prob:
        return {"event": None, "reasoning": "Code gate: no life event this month", "llm_called": False}

    # Phase 2: code-side weighted event draw
    weighted_events = []
    weights = []
    for e in event_pool:
        if not isinstance(e, dict) or "event" not in e:
            continue
        raw_weight = e.get("probability", None)
        if raw_weight is None:
            raw_weight = (
                abs(float(e.get("cash_change", 0.0) or 0.0))
                + abs(float(e.get("buy_tendency", 0.0) or 0.0))
                + abs(float(e.get("sell_tendency", 0.0) or 0.0))
                + 0.05
            )
        weight = max(0.001, float(raw_weight))
        weighted_events.append(e)
        weights.append(weight)

    if not weighted_events:
        return {"event": None, "reasoning": "No valid life events", "llm_called": False}

    selected = random.choices(weighted_events, weights=weights, k=1)[0]
    selected_event = selected.get("event")
    default_reason = f"Code-selected event: {selected_event}"

    # Phase 3: optional LLM explanation only (decision already made by code)
    if not llm_reasoning_enabled:
        return {"event": selected_event, "reasoning": default_reason, "llm_called": False}

    prompt = f"""
    你是Agent {agent.id} 的生活事件解释器。
    【背景】{agent.story.background_story}
    【本月】第{month}月
    【已触发事件】{selected_event}
    请用一句话解释这个事件为何在本月发生（不改变事件本身）。
    输出JSON：{{"reasoning":"..."}}
    """
    llm_resp = safe_call_llm(prompt, {"reasoning": default_reason}, model_type="fast")
    reasoning = llm_resp.get("reasoning", default_reason) if isinstance(llm_resp, dict) else default_reason
    return {"event": selected_event, "reasoning": reasoning, "llm_called": True}


def apply_event_effects(agent: Agent, event_data: dict, config=None):
    """
    Apply the financial effects of an event.
    """
    event_name = event_data.get("event")
    if not event_name:
        return

    event_pool = []
    if config:
        event_pool = config.life_events.get('pool', [])

    event_config = next((e for e in event_pool if e["event"] == event_name), None)
    if event_config:
        cash_change_pct = event_config["cash_change"]
        agent.cash *= (1 + cash_change_pct)
        agent.set_life_event(0, event_name)  # Using 0 as current month placeholder or pass actual month
        # print(f"Agent {agent.id} experienced {event_name}, cash changed by {cash_change_pct*100}%")


def determine_listing_strategy(agent: Agent, market_price_map: Dict[str, float], market_bulletin: str = "", market_trend: str = "STABLE", config=None) -> tuple[dict, dict]:
    """
    For multi-property owners, decide which properties to sell and the pricing strategy.
    Returns: (DecisionDict, ContextMetrics)
    """
    props_info = []
    total_holding_cost = 0

    for p in agent.owned_properties:
        zone = p.get('zone', 'A')
        current_market_value = market_price_map.get(zone, p['base_value'])

        # Calculate holding cost
        # Assuming existing mortgage info is stored or estimated
        # Simplified: estimate mortgage based on loan amount?
        # For now, let's use a standard estimate from FinancialCalculator
        holding_cost = FinancialCalculator.calculate_holding_cost(agent, p, mortgage_payment=0)  # Need real mortgage data validation in later phase
        # Actually agent.mortgage_monthly_payment is total. We can amortize?
        # Let's simple check if property is rented.

        total_holding_cost += holding_cost

        props_info.append({
            "id": p['property_id'],
            "zone": zone,
            "base_value": p['base_value'],
            "est_market_value": current_market_value,
            "holding_cost": holding_cost
        })

    # Psychological Anchor
    psych_advice = ""
    comp_min_price = 0
    if props_info:
        # Use first property as reference
        ref_val = props_info[0]['est_market_value']
        psych_val = determine_psychological_price(agent, ref_val, market_trend)
        psych_advice = f"【参考心理价】基于你的风格({agent.story.investment_style})和市场({market_trend})，建议关注 {psych_val:,.0f} 附近的价位。"
        comp_min_price = ref_val * 0.95  # Mock competitor price 5% lower

    # Financial Context
    risk_free_rate = 0.03
    if config:
        risk_free_rate = config.market.get('risk_free_rate', 0.03)

    total_property_value = sum(p['est_market_value'] for p in props_info)
    potential_bank_interest = total_property_value * risk_free_rate

    # Construct Prompt
    prompt = LISTING_STRATEGY_TEMPLATE.format(
        agent_id=agent.id,
        background=agent.story.background_story,
        investment_style=agent.story.investment_style,
        cash=agent.cash,
        income=agent.monthly_income,
        monthly_payment=getattr(agent, 'mortgage_monthly_payment', 0),
        life_pressure=getattr(agent, 'life_pressure', 'patient'),
        props_info_json=json.dumps(props_info, indent=2, ensure_ascii=False),
        market_bulletin=market_bulletin if market_bulletin else "【市场信息】暂无市场公报",
        psych_advice=psych_advice,
        total_holding_cost=total_holding_cost,
        risk_free_rate=risk_free_rate,
        potential_bank_interest=potential_bank_interest,
        comp_min_price=comp_min_price
    )

    seller_profile = str(getattr(agent.story, "seller_profile", "") or "")
    housing_stage = str(getattr(agent.story, "housing_stage", "") or "")
    family_stage = str(getattr(agent.story, "family_stage", "") or "")
    financial_profile = str(getattr(agent.story, "financial_profile", "") or "")
    purchase_motive_primary = str(getattr(agent.story, "purchase_motive_primary", "") or "")
    seller_persona_hint = (
        f"\n【卖方画像补充】\n"
        f"- 卖方画像: {seller_profile}\n"
        f"- 住房阶段: {housing_stage}\n"
        f"- 家庭阶段: {family_stage}\n"
        f"- 财务结构: {financial_profile}\n"
        f"- 主购房动机: {purchase_motive_primary}\n"
        "- 如果你是改善型自住者，卖房目标更可能是换房或释放资金，而不是纯粹逐利。\n"
        "- 如果你是现金压力卖家，应优先考虑去化速度与回款确定性。\n"
        "- 如果你是资产配置/组合调整型，应考虑持有成本、流动性和再配置效率。\n"
    )
    prompt += seller_persona_hint

    # Prepare metrics for logging
    context_metrics = {
        "total_holding_cost": total_holding_cost,
        "potential_bank_interest": potential_bank_interest,
        "comp_min_price": comp_min_price,
        "risk_free_rate": risk_free_rate,
        "persona_snapshot": _persona_snapshot(agent),
        "owned_property_count": int(len(getattr(agent, "owned_properties", []) or [])),
        "property_candidates": [
            {
                "property_id": int(p["id"]),
                "zone": str(p["zone"]),
                "base_value": float(p["base_value"]),
                "est_market_value": float(p["est_market_value"]),
                "holding_cost": float(p["holding_cost"]),
            }
            for p in props_info
        ],
    }

    # Default: Sell the cheapest one with balanced strategy
    sorted_props = sorted(props_info, key=lambda x: x['base_value'])
    default_resp = {
        "strategy": "B",
        "pricing_coefficient": 1.0,
        "properties_to_sell": [sorted_props[0]['id']] if sorted_props else [],
        "reasoning": "Default balanced strategy"
    }

    decision = safe_call_llm(prompt, default_resp, model_type="fast")
    return decision, context_metrics


def decide_negotiation_format(seller: Agent, interested_buyers: List[Agent], market_info: str) -> str:
    """
    Decide the negotiation format based on seller's situation and market interest.
    Options: 'classic', 'batch_bidding', 'flash_deal'
    """
    buyer_count = len(interested_buyers)
    if buyer_count == 0:
        return "classic"

    prompt = f"""
    你是卖家 {seller.id}。
    【背景】{seller.story.background_story}
    【性格】{seller.story.investment_style}
    【市场环境】{market_info}
    【当前状况】有 {buyer_count} 位买家对你的房产感兴趣。

    请选择谈判方式：
    1. CLASSIC: 传统谈判 (一个个谈，稳妥)
    2. BATCH: 盲拍/批量竞价 (仅当买家>1时可选，适合市场火热，价高者得)
    3. FLASH: 闪电成交 (一口价甩卖，适合急需用钱或市场冷清，需降价换速度)

    输出JSON: {{"format": "CLASSIC"|"BATCH"|"FLASH", "reasoning": "..."}}
    """
    # Default fallback: CLASSIC
    default_resp = {"format": "CLASSIC", "reasoning": "Default safe choice"}

    result = safe_call_llm(prompt, default_resp, model_type="fast")
    fmt = result.get("format", "CLASSIC").upper()

    # Enforce logic: Batch requires > 1 buyer
    if fmt == "BATCH" and buyer_count < 2:
        return "CLASSIC"

    if fmt not in ["CLASSIC", "BATCH", "FLASH"]:
        return "CLASSIC"

    return fmt


async def decide_price_adjustment(
    agent_id: int,
    agent_name: str,
    investment_style: str,
    property_id: int,
    current_price: float,
    listing_duration: int,
    market_trend: str,
    db_conn,
    recent_demand_context: Dict | None = None,
    sell_deadline_month: int | None = None,
    sell_deadline_total_months: int | None = None,
    current_month: int | None = None,
    model_type: str = "fast",
    force_raise_only: bool = False,
    decision_profile: str = "normal",
    info_delay_months: int = 0,
    observed_market_trend: str | None = None,
) -> tuple[dict, dict]:
    """
    LLM decides whether to adjust price for a property that has been listed for too long.
    Returns: (DecisionDict, ContextMetrics)
    """

    # Fetch agent background
    cursor = db_conn.cursor()
    cursor.execute(
        """
        SELECT
            background_story,
            purchase_motive_primary,
            housing_stage,
            family_stage,
            education_path,
            financial_profile,
            seller_profile
        FROM agents_static
        WHERE agent_id = ?
        """,
        (agent_id,),
    )
    row = cursor.fetchone()
    background = row[0] if row else "普通投资者"
    persona_snapshot = {
        "purchase_motive_primary": str(row[1] if row and len(row) > 1 else ""),
        "housing_stage": str(row[2] if row and len(row) > 2 else ""),
        "family_stage": str(row[3] if row and len(row) > 3 else ""),
        "education_path": str(row[4] if row and len(row) > 4 else ""),
        "financial_profile": str(row[5] if row and len(row) > 5 else ""),
        "seller_profile": str(row[6] if row and len(row) > 6 else ""),
    }
    cursor.execute(
        """
        SELECT payment_tolerance_ratio, down_payment_tolerance_ratio
        FROM agents_finance
        WHERE agent_id = ?
        """,
        (agent_id,),
    )
    finance_row = cursor.fetchone()
    if finance_row:
        persona_snapshot["payment_tolerance_ratio"] = float(finance_row[0] or 0.0)
        persona_snapshot["down_payment_tolerance_ratio"] = float(finance_row[1] or 0.0)
    property_row = None
    try:
        cursor.execute(
            """
            SELECT ps.zone, ps.is_school_district, ps.building_area, pm.status
            FROM properties_static ps
            LEFT JOIN properties_market pm ON ps.property_id = pm.property_id
            WHERE ps.property_id = ?
            """,
            (property_id,),
        )
        property_row = cursor.fetchone()
    except Exception:
        try:
            cursor.execute(
                """
                SELECT zone, 0 AS is_school_district, 0 AS building_area, status
                FROM properties_market
                WHERE property_id = ?
                """,
                (property_id,),
            )
            property_row = cursor.fetchone()
        except Exception:
            property_row = None
    property_snapshot = {
        "property_id": int(property_id),
        "zone": str(property_row[0] if property_row else ""),
        "is_school_district": bool(int(property_row[1] or 0)) if property_row else False,
        "building_area": float(property_row[2] if property_row and property_row[2] is not None else 0.0),
        "market_status": str(property_row[3] if property_row and len(property_row) > 3 and property_row[3] else ""),
    }

    def _clamp01(v: float) -> float:
        return max(0.0, min(1.0, float(v)))

    # Calculate Psych Price
    mock_agent = Agent(id=agent_id)
    mock_agent.story = AgentStory(investment_style=investment_style)
    psych_price = determine_psychological_price(
        mock_agent,  # Mock agent wrapper for function
        current_price,
        market_trend
    )
    psych_advice = f"【参考建议】心理价位约 {psych_price:,.0f} (基于风格{investment_style})"

    # Data-grounded context (avoid systematic "always cheaper competitor" bias)
    accumulated_holding_cost = current_price * 0.005 * listing_duration  # 0.5% per month holding cost
    demand_ctx = dict(recent_demand_context or {})
    lookback = max(1, int(demand_ctx.get("lookback_months", 1) or 1))
    matches = int(demand_ctx.get("matches", 0) or 0)
    valid_bids = int(demand_ctx.get("valid_bids", 0) or 0)
    negotiation_entries = int(demand_ctx.get("negotiation_entries", 0) or 0)
    outbid_losses = int(demand_ctx.get("outbid_losses", 0) or 0)
    demand_proxy = matches + 2 * valid_bids + 2 * negotiation_entries + outbid_losses
    daily_views = max(1, int(round(demand_proxy / lookback)))

    comp_ref_price = current_price
    comp_min_price = current_price
    try:
        zone = str(property_snapshot.get("zone", "") or "")
        school_flag = int(1 if property_snapshot.get("is_school_district", False) else 0)
        # Prefer robust benchmark (median) over global minimum to avoid
        # systematic "always overpriced" bias for higher-quality listings.
        def _fetch_comp_prices(match_school: bool):
            if match_school:
                cursor.execute(
                    """
                    SELECT pm.listed_price
                    FROM properties_market pm
                    JOIN properties_static ps ON ps.property_id = pm.property_id
                    WHERE pm.status='for_sale'
                      AND pm.property_id <> ?
                      AND (? = '' OR ps.zone = ?)
                      AND ps.is_school_district = ?
                    ORDER BY pm.listed_price ASC
                    """,
                    (int(property_id), zone, zone, school_flag),
                )
            else:
                cursor.execute(
                    """
                    SELECT pm.listed_price
                    FROM properties_market pm
                    JOIN properties_static ps ON ps.property_id = pm.property_id
                    WHERE pm.status='for_sale'
                      AND pm.property_id <> ?
                      AND (? = '' OR ps.zone = ?)
                    ORDER BY pm.listed_price ASC
                    """,
                    (int(property_id), zone, zone),
                )
            return [float(x[0]) for x in (cursor.fetchall() or []) if x and x[0] is not None]

        prices = _fetch_comp_prices(match_school=True)
        if not prices:
            prices = _fetch_comp_prices(match_school=False)

        if prices:
            comp_min_price = float(prices[0])
            mid = len(prices) // 2
            if len(prices) % 2 == 0:
                comp_ref_price = (prices[mid - 1] + prices[mid]) / 2.0
            else:
                comp_ref_price = prices[mid]
        else:
            comp_min_price = float(current_price)
            comp_ref_price = float(current_price)
    except Exception:
        comp_min_price = float(current_price)
        comp_ref_price = float(current_price)

    price_diff = float(current_price) - float(comp_ref_price)
    premium_ratio = 0.0
    if float(comp_ref_price) > 0:
        premium_ratio = float(price_diff) / float(comp_ref_price)
    if price_diff >= 0:
        price_diff_signed = f"+¥{abs(price_diff):,.0f}"
    else:
        price_diff_signed = f"-¥{abs(price_diff):,.0f}"
    recent_demand_summary = (
        "近月热度一般，暂无明显抢单信号。"
        if not demand_ctx
        else (
            f"热度={demand_ctx.get('band', 'LOW')} "
            f"(score={float(demand_ctx.get('score', 0.0)):.2f})，"
            f"近{int(demand_ctx.get('lookback_months', 1))}月匹配{int(demand_ctx.get('matches', 0))}、"
            f"有效出价{int(demand_ctx.get('valid_bids', 0))}、"
            f"进谈判{int(demand_ctx.get('negotiation_entries', 0))}、"
            f"outbid失败{int(demand_ctx.get('outbid_losses', 0))}。"
        )
    )
    raise_gain_3pct = float(current_price) * 0.03
    cut_loss_3pct = float(current_price) * 0.03
    next_month_holding_cost = float(current_price) * 0.005
    recent_valid_bids = int(demand_ctx.get("valid_bids", 0) or 0)
    current_interest_buyers = int(demand_ctx.get("interest_buyers", demand_ctx.get("matches", 0)) or 0)
    recent_negotiations = int(demand_ctx.get("negotiation_entries", 0) or 0)
    recent_outbid_losses = int(demand_ctx.get("outbid_losses", 0) or 0)
    best_valid_bid = float(demand_ctx.get("best_valid_bid", 0.0) or 0.0)
    lead_gap = float(best_valid_bid - float(current_price)) if best_valid_bid > 0 else 0.0
    lead_gap_ratio_pct = 0.0
    if float(current_price) > 0:
        lead_gap_ratio_pct = (float(lead_gap) / float(current_price)) * 100.0
    lead_gap_signed = (
        f"+¥{abs(float(lead_gap)):,.0f}"
        if float(lead_gap) >= 0
        else f"-¥{abs(float(lead_gap)):,.0f}"
    )
    demand_band = str(demand_ctx.get("band", "LOW") or "LOW").upper()
    style_norm = str(investment_style or "").strip().lower()
    month_now = int(current_month) if current_month is not None else None
    deadline_month = int(sell_deadline_month) if sell_deadline_month is not None else None
    deadline_total = int(max(0, sell_deadline_total_months or 0))
    months_left = None
    if month_now is not None and deadline_month is not None:
        months_left = int(deadline_month - month_now + 1)
    if months_left is None and deadline_total > 0:
        months_left = int(max(1, deadline_total - int(listing_duration)))
    if months_left is None:
        months_left = 999
    deadline_pressure = "低"
    deadline_note = "你可按市场情况自由调价。"
    deadline_penalty_note = (
        "到期未成交会进入系统托管清仓：持续降价直到真实买家成交，"
        "并按规则对卖家额外扣罚资金。"
    )
    if months_left <= 1:
        deadline_pressure = "极高（本月必须完成出售）"
        deadline_note = (
            "你已到售出期限最后一个月。请不要选择维持(A)或撤牌(D)，"
            "应给出明显调价动作，让房源本月尽快成交。"
        )
    elif months_left <= 2:
        deadline_pressure = "高（窗口即将结束）"
        deadline_note = "建议优先考虑可成交动作，避免继续拖延。"
    elif months_left <= 4:
        deadline_pressure = "中"
        deadline_note = "可适度提价或降价，但需兼顾成交速度。"

    # Seller memory panel (last 2 months) for continuity of decisions.
    lookback_start = None
    if month_now is not None:
        lookback_start = max(1, int(month_now) - 1)
    recent_adjust_counts = {"A": 0, "B": 0, "C": 0, "D": 0, "E": 0, "F": 0}
    recent_orders = 0
    recent_filled = 0
    recent_failed = 0
    recent_zone_closed = 0
    recent_zone_filled = 0
    try:
        if lookback_start is not None:
            cursor.execute(
                """
                SELECT decision, COUNT(*)
                FROM decision_logs
                WHERE agent_id = ?
                  AND event_type = 'PRICE_ADJUSTMENT'
                  AND month BETWEEN ? AND ?
                GROUP BY decision
                """,
                (int(agent_id), int(lookback_start), int(month_now)),
            )
            for d, cnt in (cursor.fetchall() or []):
                key = str(d or "").strip().upper()
                if key in recent_adjust_counts:
                    recent_adjust_counts[key] = int(cnt or 0)
            cursor.execute(
                """
                SELECT
                    COUNT(*),
                    SUM(CASE WHEN status='filled' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN status IN ('cancelled','breached') THEN 1 ELSE 0 END)
                FROM transaction_orders
                WHERE seller_id = ?
                  AND created_month BETWEEN ? AND ?
                """,
                (int(agent_id), int(lookback_start), int(month_now)),
            )
            row = cursor.fetchone() or (0, 0, 0)
            recent_orders = int(row[0] or 0)
            recent_filled = int(row[1] or 0)
            recent_failed = int(row[2] or 0)
            zone_now = str(property_snapshot.get("zone", "") or "")
            if zone_now:
                cursor.execute(
                    """
                    SELECT
                        SUM(CASE WHEN o.status='filled' THEN 1 ELSE 0 END),
                        SUM(CASE WHEN o.status IN ('filled','cancelled','breached') THEN 1 ELSE 0 END)
                    FROM transaction_orders o
                    JOIN properties_static ps ON ps.property_id=o.property_id
                    WHERE ps.zone = ?
                      AND o.created_month BETWEEN ? AND ?
                    """,
                    (zone_now, int(lookback_start), int(month_now)),
                )
                zrow = cursor.fetchone() or (0, 0)
                recent_zone_filled = int(zrow[0] or 0)
                recent_zone_closed = int(zrow[1] or 0)
    except Exception:
        pass

    recent_fill_rate = (recent_filled / recent_orders) if recent_orders > 0 else 0.0
    recent_zone_fill_rate = (
        recent_zone_filled / recent_zone_closed
        if recent_zone_closed > 0
        else 0.0
    )
    seller_memory_panel = (
        f"- 近2个月你的调价动作统计: A{recent_adjust_counts['A']} / B{recent_adjust_counts['B']} / "
        f"C{recent_adjust_counts['C']} / D{recent_adjust_counts['D']} / "
        f"E{recent_adjust_counts['E']} / F{recent_adjust_counts['F']}\n"
        f"- 近2个月你发起订单: {recent_orders}，已成交: {recent_filled}，失败/违约: {recent_failed}，"
        f"你自己的闭环成交率: {recent_fill_rate:.1%}\n"
        f"- 同期本区(区={property_snapshot.get('zone','?')})闭环成交率: {recent_zone_fill_rate:.1%}\n"
        f"- 当前这套房已挂牌: {listing_duration}个月，剩余售出期限: "
        f"{months_left if months_left < 900 else '未设定'}个月"
    )

    # Explainable scorecard (0-100): high means stronger need to prioritize closing.
    holding_pressure_score = _clamp01(
        next_month_holding_cost / max(1.0, float(current_price) * 0.015)
    ) * 100.0
    sale_prob_proxy = _clamp01(
        0.30 * min(1.0, matches / 4.0)
        + 0.35 * min(1.0, valid_bids / 2.0)
        + 0.20 * min(1.0, negotiation_entries / 2.0)
        + 0.15 * min(1.0, max(0.0, best_valid_bid) / max(1.0, current_price))
    ) * 100.0
    # One-month hold opportunity-cost (coarse, auditable):
    # 1) next-month carrying cost
    # 2) expected lost pricing power if the sale window weakens while waiting
    market_cool_risk = _clamp01(0.6 * (premium_ratio + 0.05) + 0.4 * (1.0 - sale_prob_proxy / 100.0))
    expected_price_slip_if_wait = max(0.0, float(current_price) * (0.005 + 0.02 * market_cool_risk))
    hold_opportunity_cost_1m = max(0.0, float(next_month_holding_cost) + expected_price_slip_if_wait)
    liquidity_risk_score = 100.0 - sale_prob_proxy
    pricing_gap_pressure_score = _clamp01((premium_ratio + 0.02) / 0.25) * 100.0
    if months_left >= 900:
        deadline_pressure_score = 35.0
    elif months_left <= 1:
        deadline_pressure_score = 100.0
    elif months_left == 2:
        deadline_pressure_score = 85.0
    elif months_left == 3:
        deadline_pressure_score = 70.0
    elif months_left == 4:
        deadline_pressure_score = 55.0
    else:
        deadline_pressure_score = 40.0

    score_weights = {
        "holding_pressure": 0.30,
        "liquidity_risk": 0.30,
        "pricing_gap_pressure": 0.20,
        "deadline_pressure": 0.20,
    }
    seller_close_priority_score = (
        holding_pressure_score * score_weights["holding_pressure"]
        + liquidity_risk_score * score_weights["liquidity_risk"]
        + pricing_gap_pressure_score * score_weights["pricing_gap_pressure"]
        + deadline_pressure_score * score_weights["deadline_pressure"]
    )
    if seller_close_priority_score >= 70:
        score_suggestion = "优先考虑B/C（降价成交）"
    elif seller_close_priority_score >= 45:
        score_suggestion = "优先考虑A/B（小步调整）"
    else:
        score_suggestion = "可考虑A/E（维持或温和提价）"
    seller_scorecard = (
        f"- 持有压力分: {holding_pressure_score:.1f}/100（下月持有成本越高越应尽快成交）\n"
        f"- 流动性风险分: {liquidity_risk_score:.1f}/100（近期有效报价越少，风险越高）\n"
        f"- 定价偏离压力分: {pricing_gap_pressure_score:.1f}/100（高于可比成交越多，越应让价）\n"
        f"- 期限压力分: {deadline_pressure_score:.1f}/100（剩余月份越少，越应成交）\n"
        f"- 总分(成交优先度): {seller_close_priority_score:.1f}/100；建议动作: {score_suggestion}"
    )

    # Behavior anchor is advisory only; final action remains LLM-owned.
    style_steer_hint = "【行为风格提示】按你的性格自由决策，可忽略本提示。"
    if style_norm == "aggressive":
        if demand_band in {"HIGH", "MEDIUM"} and premium_ratio <= 0.08:
            style_steer_hint = (
                "【行为风格提示】你偏激进。近期需求不弱且当前溢价不高，"
                "可考虑小幅提价(E)或明显提价(F)筛选高购买力买家。"
            )
        elif demand_band == "LOW" and listing_duration >= 3:
            style_steer_hint = (
                "【行为风格提示】你偏激进。需求偏弱且已滞销，"
                "可优先考虑B/C降价试成交；若选择撤牌(D)需给出明确依据。"
            )
    elif style_norm == "conservative":
        if demand_band == "LOW":
            style_steer_hint = (
                "【行为风格提示】你偏保守。需求偏弱时优先控风险，"
                "应在A维持与B小幅降价之间谨慎权衡，不建议无理由撤牌。"
            )
        elif demand_band in {"HIGH", "MEDIUM"}:
            style_steer_hint = (
                "【行为风格提示】你偏保守。即使需求偏热，也可先A维持或E小幅提价，不必激进追价。"
            )
    else:
        if demand_band in {"HIGH", "MEDIUM"}:
            style_steer_hint = (
                "【行为风格提示】你偏平衡。需求偏热且价格不高时可评估E/F；"
                "若竞争转弱再回到A或D。"
            )
        elif demand_band == "LOW" and listing_duration >= 4:
            style_steer_hint = (
                "【行为风格提示】你偏平衡。滞销时间拉长且需求偏弱时，"
                "可优先考虑B/C降价提高成交概率，而非盲目加价或长期撤牌。"
            )

    profile = str(decision_profile or "normal").strip().lower()
    visible_market_trend = str(observed_market_trend or market_trend or "STABLE")
    prompt_kwargs = dict(
        agent_name=agent_name,
        investment_style=investment_style,
        background=background,
        property_id=property_id,
        listing_duration=listing_duration,
        current_price=current_price,
        market_trend=market_trend,
        observed_market_trend=visible_market_trend,
        info_delay_months=max(0, int(info_delay_months or 0)),
        deadline_total_months=deadline_total if deadline_total > 0 else "未设定",
        months_left=months_left if months_left < 900 else "未设定",
        deadline_pressure=deadline_pressure,
        deadline_penalty_note=deadline_penalty_note,
        deadline_note=deadline_note,
        psych_advice=psych_advice,
        accumulated_holding_cost=accumulated_holding_cost,
        daily_views=daily_views,
        comp_min_price=comp_min_price,
        comp_ref_price=comp_ref_price,
        price_diff_signed=price_diff_signed,
        recent_demand_summary=recent_demand_summary,
        raise_gain_3pct=raise_gain_3pct,
        cut_loss_3pct=cut_loss_3pct,
        next_month_holding_cost=next_month_holding_cost,
        premium_ratio_pct=round(premium_ratio * 100.0, 2),
        current_interest_buyers=current_interest_buyers,
        current_valid_bids=recent_valid_bids,
        lead_gap_signed=lead_gap_signed,
        lead_gap_ratio_pct=round(float(lead_gap_ratio_pct), 2),
        best_valid_bid=float(best_valid_bid),
        recent_valid_bids=recent_valid_bids,
        recent_negotiations=recent_negotiations,
        recent_outbid_losses=recent_outbid_losses,
        hold_opportunity_cost_1m=hold_opportunity_cost_1m,
        seller_memory_panel=seller_memory_panel,
        seller_scorecard=seller_scorecard,
    )
    if profile == "smart":
        prompt = PRICE_ADJUSTMENT_TEMPLATE.format(**prompt_kwargs)
        prompt += (
            "\n【中性提醒】请做“收益-成交概率-持有成本”平衡，不要求固定策略。"
            "若你认为提价/降价都不合理，可维持或撤牌。"
        )
    else:
        prompt = PRICE_ADJUSTMENT_TEMPLATE_NORMAL.format(**prompt_kwargs)
        prompt += (
            "\n【普通代理人提醒】你只能根据自己看见的有限信号和周边气氛做判断，"
            "不要把自己当成精算型卖家。"
        )
    prompt += f"\n{style_steer_hint}"
    if bool(force_raise_only):
        prompt += (
            "\n【RegimeV1前置提价仲裁】本轮只允许提价动作。"
            "\n你必须在 E(小幅提价) 或 F(明显提价) 中二选一。"
            "\n请不要返回 A/B/C/D。"
        )

    context_metrics = {
        "accumulated_holding_cost": accumulated_holding_cost,
        "daily_views": daily_views,
        "comp_min_price": comp_min_price,
        "comp_ref_price": comp_ref_price,
        "price_gap": price_diff,
        "premium_ratio": premium_ratio,
        "raise_gain_3pct": raise_gain_3pct,
        "cut_loss_3pct": cut_loss_3pct,
        "next_month_holding_cost": next_month_holding_cost,
        "hold_opportunity_cost_1m": float(hold_opportunity_cost_1m),
        "expected_price_slip_if_wait": float(expected_price_slip_if_wait),
        "market_cool_risk": float(round(market_cool_risk, 6)),
        "current_interest_buyers": int(current_interest_buyers),
        "current_valid_bids": int(recent_valid_bids),
        "best_valid_bid": float(best_valid_bid),
        "lead_gap": float(lead_gap),
        "lead_gap_ratio_pct": float(lead_gap_ratio_pct),
        "recent_demand_context": demand_ctx,
        "demand_band": demand_band,
        "sell_deadline_month": int(deadline_month) if deadline_month is not None else None,
        "sell_deadline_total_months": int(deadline_total) if deadline_total > 0 else None,
        "deadline_months_left": int(months_left) if months_left < 900 else None,
        "deadline_pressure": deadline_pressure,
        "deadline_penalty_note": deadline_penalty_note,
        "style_steer_hint": style_steer_hint,
        "seller_memory_panel": seller_memory_panel,
        "seller_scorecard_text": seller_scorecard,
        "decision_profile": profile,
        "observed_market_trend": visible_market_trend,
        "info_delay_months": int(max(0, int(info_delay_months or 0))),
        "seller_scorecard": {
            "holding_pressure_score": float(round(holding_pressure_score, 4)),
            "liquidity_risk_score": float(round(liquidity_risk_score, 4)),
            "pricing_gap_pressure_score": float(round(pricing_gap_pressure_score, 4)),
            "deadline_pressure_score": float(round(deadline_pressure_score, 4)),
            "close_priority_score": float(round(seller_close_priority_score, 4)),
            "weights": score_weights,
            "score_suggestion": score_suggestion,
        },
        "regime_v1_force_raise_only": bool(force_raise_only),
        "seller_recent_memory": {
            "lookback_start_month": int(lookback_start) if lookback_start is not None else None,
            "lookback_end_month": int(month_now) if month_now is not None else None,
            "price_adjust_counts": recent_adjust_counts,
            "orders": int(recent_orders),
            "filled": int(recent_filled),
            "failed": int(recent_failed),
            "seller_fill_rate": float(round(recent_fill_rate, 6)),
            "zone_fill_rate": float(round(recent_zone_fill_rate, 6)),
        },
        "persona_snapshot": persona_snapshot,
        "property_snapshot": property_snapshot,
    }

    default_return = {
        "action": "E" if bool(force_raise_only) else "A",
        "coefficient": 1.03 if bool(force_raise_only) else 1.00,
        "reason": "热盘前置仲裁默认小幅提价" if bool(force_raise_only) else "默认维持原价"
    }

    result = await safe_call_llm_async(
        prompt,
        default_return,
        system_prompt=SYSTEM_PROMPT_SELLER_REPRICING,
        model_type=str(model_type or "fast")
    )

    def _cfg_bool(key: str, default: bool) -> bool:
        try:
            raw = config.get(key, default)
        except Exception:
            raw = default
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}

    def _cfg_float(key: str, default: float) -> float:
        try:
            return float(config.get(key, default))
        except Exception:
            return float(default)

    def _cfg_int(key: str, default: int) -> int:
        try:
            return int(config.get(key, default))
        except Exception:
            return int(default)

    recheck_called = False
    hard_guard_called = False
    hard_guard_forced = False
    regime_enabled = _cfg_bool(
        "smart_agent.regime_engine_v1_enabled",
        _cfg_bool("regime_engine_v1_enabled", False),
    )
    hot_recheck_enabled = _cfg_bool(
        "smart_agent.regime_v1_price_reconsider_enabled",
        _cfg_bool("regime_v1_price_reconsider_enabled", True),
    )
    hard_raise_guard_enabled = _cfg_bool(
        "smart_agent.regime_v1_raise_release_hard_guard_enabled",
        _cfg_bool("regime_v1_raise_release_hard_guard_enabled", True),
    )
    if bool(force_raise_only):
        hard_raise_guard_enabled = True
    hot_min_valid_bids = max(
        1,
        _cfg_int("smart_agent.regime_v1_raise_release_min_valid_bids", 2),
    )
    hot_min_outbid_losses = max(
        0,
        _cfg_int("smart_agent.regime_v1_raise_release_min_outbid_losses", 1),
    )
    hot_max_close_priority_score = _cfg_float(
        "smart_agent.regime_v1_raise_release_max_close_priority_score",
        50.0,
    )

    hot_signal = bool(force_raise_only) or (
        str(demand_band).upper() in {"HIGH", "MEDIUM"}
        and int(recent_valid_bids) >= int(hot_min_valid_bids)
        and (
            int(recent_outbid_losses) >= int(hot_min_outbid_losses)
            or float(lead_gap) > 0.0
        )
        and float(seller_close_priority_score) <= float(hot_max_close_priority_score)
    )
    if regime_enabled and hot_recheck_enabled:
        action_now = str(result.get("action", "A") or "A").strip().upper()
        if hot_signal and action_now in {"A", "B", "C", "D"}:
            recheck_called = True
            recheck_prompt = (
                prompt
                + "\n\n【RegimeV1热盘复核】"
                + f"\n你当前动作是 {action_now}。"
                + f"\n若本月不执行提价(E/F)，预计1个月机会成本约 ¥{hold_opportunity_cost_1m:,.0f}。"
                + "\n当前有效报价与outbid信号显示买方竞争仍在。"
                + "\n请复核是否改为 E/F 以筛选高购买力买家。"
                + "\n若仍坚持 A/B/C/D，必须明确写出你为什么接受这笔机会成本。"
                + "\n返回JSON格式不变。"
            )
            reconsidered = await safe_call_llm_async(
                recheck_prompt,
                result,
                system_prompt=SYSTEM_PROMPT_SELLER_REPRICING,
                model_type=str(model_type or "fast"),
            )
            if isinstance(reconsidered, dict):
                result = reconsidered

    # Hard-guard (rollback-able): in hot market signal, if model still avoids E/F,
    # trigger a final raise-only arbitration. LLM still decides between E/F.
    if regime_enabled and hard_raise_guard_enabled and hot_signal:
        action_now = str(result.get("action", "A") or "A").strip().upper()
        if action_now not in {"E", "F"}:
            hard_guard_called = True
            fallback_raise = {
                "action": "E",
                "coefficient": max(1.01, min(1.05, _cfg_float("smart_agent.regime_v1_raise_release_default_e_coeff", 1.03))),
                "reason": "热盘信号下启用提价仲裁，默认温和提价。",
            }
            arb_prompt = (
                prompt
                + "\n\n【RegimeV1热盘强制仲裁】"
                + "\n你现在只能在 E(小幅提价) / F(明显提价) 二选一。"
                + f"\n当前有效报价={int(recent_valid_bids)}，outbid={int(recent_outbid_losses)}，"
                + f"成交优先度={float(seller_close_priority_score):.1f}。"
                + f"\n若本月不提价，预计1个月机会成本约 ¥{hold_opportunity_cost_1m:,.0f}。"
                + "\n请输出 JSON，action 只能是 E 或 F。"
            )
            arb_result = await safe_call_llm_async(
                arb_prompt,
                fallback_raise,
                system_prompt=SYSTEM_PROMPT_SELLER_REPRICING,
                model_type=str(model_type or "smart"),
            )
            if isinstance(arb_result, dict):
                result = arb_result
            action_now = str(result.get("action", "A") or "A").strip().upper()
            if action_now not in {"E", "F"}:
                hard_guard_forced = True
                result = dict(fallback_raise)

    # Keep coefficient inside action-compatible bounds for auditability.
    action_final = str(result.get("action", "A") or "A").strip().upper()
    try:
        coeff_final = float(result.get("coefficient", 1.0) or 1.0)
    except Exception:
        coeff_final = 1.0
    if action_final == "E":
        coeff_final = max(1.01, min(1.05, coeff_final))
    elif action_final == "F":
        coeff_final = max(1.05, min(1.12, coeff_final))
    result["coefficient"] = float(coeff_final)

    # Calculate new price
    coefficient = result.get("coefficient", 1.0)
    new_price = current_price * coefficient

    result["new_price"] = new_price
    context_metrics["regime_v1_hot_recheck_called"] = bool(recheck_called)
    context_metrics["regime_v1_hot_signal"] = bool(hot_signal)
    context_metrics["regime_v1_raise_hard_guard_called"] = bool(hard_guard_called)
    context_metrics["regime_v1_raise_hard_guard_forced"] = bool(hard_guard_forced)

    return result, context_metrics

# --- 3. Role Determination ---


class AgentRole(Enum):
    BUYER = "buyer"
    SELLER = "seller"
    BUYER_SELLER = "buyer_seller"
    OBSERVER = "observer"


def determine_role(agent: Agent, month: int, market: Market) -> Tuple[AgentRole, str]:
    """
    Determine the agent's role (Buyer/Seller/Observer) for the month.
    """
    # 1. Context Hints (No Hard Constraints)
    hints = []
    if agent.cash < agent.monthly_income * 2 and agent.owned_properties:
        hints.append("【资金预警】现金流紧张（不足2个月），请认真考虑是否需要变现资产。")
    if agent.cash > agent.monthly_income * 60 and not agent.owned_properties:
        hints.append("【存款充裕】存款超过5年收入，长期持有现金可能贬值，建议考虑置业。")

    hint_str = "\n".join(hints)

    # 2. LLM Decision
    prompt = f"""
    你是Agent {agent.id}。
    【背景】{agent.story.background_story}
    【本月事件】{agent.monthly_event}

    {hint_str}

    判断角色（BUYER/SELLER/OBSERVER）：
    输出JSON：{{"role": "...", "reasoning": "..."}}
    """

    result = safe_call_llm(
        prompt,
        {"role": "OBSERVER", "reasoning": "Default wait"},
        model_type="fast",
    )
    role_str = result.get("role", "OBSERVER").upper()
    role_map = {
        "BUYER": AgentRole.BUYER,
        "SELLER": AgentRole.SELLER,
        "OBSERVER": AgentRole.OBSERVER
    }

    role = role_map.get(role_str, AgentRole.OBSERVER)

    # Enforce Hard Constraint: No property = Cannot be SELLER
    if role == AgentRole.SELLER and not agent.owned_properties:
        role = AgentRole.OBSERVER
        result["reasoning"] = (result.get("reasoning", "") + " [System Corrected: No property to sell]").strip()

    return role, result.get("reasoning", "")

# --- 4. Batch Activation Logic (Million Agent Scale) ---


def calculate_activation_probability(agent: Agent) -> float:
    """
    Calculate the probability (0.0 - 1.0) that an agent becomes active (Buyer/Seller) this month.
    """
    base_prob = 0.003  # 0.3% base rate

    # Weights configuration
    weights = {
        "school_urgency_high": 0.14,
        "school_urgency_mid": 0.08,
        "starter_home": 0.09,
        "recently_married": 0.08,
        "owner_upgrade": 0.06,
        "multi_property_holder": 0.05,
        "asset_allocation": 0.04,
        "high_wealth_no_property": 0.12,
        "owner_liquidity_pressure": 0.10,
        "owner_income_stressed": 0.07,
        "income_stressed_no_home": -0.08,
        "cash_poor_no_home": -0.18,
    }

    prob_score = base_prob
    owns_property = bool(agent.owned_properties)

    school_urgency = _school_urgency_score(agent)
    if school_urgency >= 3:
        prob_score += weights["school_urgency_high"]
    elif school_urgency >= 2:
        prob_score += weights["school_urgency_mid"]

    # 2. Marriage / starter-home phase
    if agent.marital_status == "married" and 25 <= agent.age <= 35:
        prob_score += weights["recently_married"]
    if _starter_home_priority(agent):
        prob_score += weights["starter_home"]

    # 3. Persona-stage effects
    if getattr(agent.story, "housing_stage", "") == "owner_upgrade":
        prob_score += weights["owner_upgrade"]
    if getattr(agent.story, "purchase_motive_primary", "") == "asset_allocation":
        prob_score += weights["asset_allocation"]

    if len(agent.owned_properties) > 1:
        prob_score += weights["multi_property_holder"]

    if agent.cash > 2000000 and not owns_property:
        prob_score += weights["high_wealth_no_property"]

    financial_profile = str(getattr(agent.story, "financial_profile", "") or "")
    if agent.cash < 50000:
        if owns_property:
            # 这里只做“进入 LLM 候选”的预路由加权，不替代理人决定一定卖房。
            prob_score += weights["owner_liquidity_pressure"]
        else:
            prob_score += weights["cash_poor_no_home"]
    if financial_profile == "income_stressed":
        if owns_property:
            prob_score += weights["owner_income_stressed"]
        else:
            prob_score += weights["income_stressed_no_home"]

    return max(0.0, min(1.0, prob_score))


# --- Constant System Prompt for Caching ---
BATCH_ROLE_SYSTEM_PROMPT = """你是一个房地产市场模拟引擎。
【任务】判断Agent本月是否进入住房决策窗口，并给出“本月时点角色”。
【规则】
1. 默认角色为 OBSERVER (无操作)
2. 角色定义:
   - BUYER: 刚需或投资买入
   - SELLER: 变现或置换卖出
   - BUYER_SELLER: 置换需求 (既买又卖)
3. **重要限制**:
   - 只有持有房产 (props > 0) 才能成为 SELLER 或 BUYER_SELLER。
   - 现金不足阈值由输入参数指定；低于阈值且无房产者只能是 OBSERVER。
   - **推理约束**: 若无房产(props=0)，严禁在 reasoning 中虚构“卖掉名下房产”/“卖老破小”。资金来源必须描述为“卖掉外省老家房产”或“父母资助”。
4. 你必须先判断 timing_role，再给出 role。timing_role 只能是：
   - buy_now: 本月立即买 / 立即推进买入
   - sell_now: 本月立即卖 / 立即挂牌或变现
   - sell_then_buy: 本月先卖后买
   - need_wait: 有住房需求，但本月暂不进场
   - observe_wait: 继续观望，本月没有明确入场动作
5. role 与 timing_role 必须一致：
   - timing_role=buy_now -> role 只能是 BUYER 或 BUYER_SELLER
   - timing_role=sell_now -> role 必须是 SELLER
   - timing_role=sell_then_buy -> role 必须是 BUYER_SELLER，且 chain_mode=sell_first
   - timing_role in {need_wait, observe_wait} -> role 必须是 OBSERVER
6. lifecycle_labels / lifecycle_summary 是“本月决策窗口”的上下文提示，只能帮助你判断，不允许机械套标签下结论。
7. smart / normal 的差异只是同一决策窗口内的信息面与判断风格差异，不是需求来源本身。
8. 输出JSON列表，包含所有产生变化的Agent。
9. 每个条目包含：
   - id
   - role (BUYER/SELLER/BUYER_SELLER)
   - timing_role
   - trigger (触发原因)
   - urgency_level: "high"|"medium"|"low"
   - life_pressure: "urgent"(迫切), "patient"(耐心), "opportunistic"(投机)
   - price_expectation: 浮点数 (1.0-1.2)
   - chain_mode: 仅当 role=BUYER_SELLER 时填写 "sell_first" 或 "buy_first"
   - risk_mode: "conservative"|"balanced"|"aggressive"
   - listing_action: "KEEP"|"WITHDRAW"（可选；若未给则默认KEEP，不会因角色变化自动撤盘）

输出示例：
[
    {"id": 101, "role": "BUYER", "timing_role": "buy_now", "trigger": "婚房刚需", "urgency_level": "high", "life_pressure": "urgent", "price_expectation": 1.1},
    {"id": 102, "role": "SELLER", "timing_role": "sell_now", "trigger": "资金周转", "urgency_level": "high", "life_pressure": "urgent", "price_expectation": 0.95}
]"""

BUYER_SELLER_CHAIN_SYSTEM_PROMPT = """你在帮助一个“既可能买也可能卖”的家庭做本月行动决策。
【任务】
只判断这个家庭本月更像下面哪一种：
1. buy_first：本月先继续找房、推进买入。
2. sell_first：本月先挂牌/先卖旧房，再考虑买入。

【重要边界】
1. 你只能决定“本月先做什么”，不能替系统执行成交。
2. 你必须结合这个家庭自己的现金、收入、已有住房、生活阶段和市场环境。
3. 若家庭无房，不可选择 sell_first；若已有房，也不能机械默认 sell_first。
4. “是否本月继续观望”已经在上一层激活阶段判断过；既然进入这一层，就只允许在 buy_first / sell_first 中二选一。

【输出格式】
输出严格 JSON 对象：
{"chain_mode":"buy_first","reason":"..."}
其中 chain_mode 只能是 buy_first / sell_first
"""


def _build_market_memory_one_liner(market_trend: str, recent_bulletins: List[str] = None) -> str:
    """
    Compress bulletin context into one line with risk tags.
    This keeps decision-critical signals while reducing prompt tokens.
    """
    trend = str(market_trend or "STABLE").upper()
    bullets = [str(x).strip() for x in (recent_bulletins or []) if str(x).strip()]
    if not bullets:
        return f"Trend={trend} | Heat=MEDIUM | RiskTags=NONE | Key=暂无新增冲击"

    joined = " | ".join(bullets[:3]).lower()
    risk_tags = []
    tag_rules = [
        ("CREDIT_TIGHT", ["credit", "信贷", "收紧", "加息", "利率上调"]),
        ("LIQUIDITY_STRESS", ["liquidity", "流动性", "冻结", "去化慢", "成交低"]),
        ("INCOME_STRESS", ["降薪", "失业", "income shock", "裁员", "工资下调"]),
        ("FOMO", ["fomo", "抢房", "连涨", "追涨", "拥挤"]),
        ("FIRE_SALE", ["特价", "fire sale", "折价", "抛售", "骨折价"]),
    ]
    for tag, keys in tag_rules:
        if any(k in joined for k in keys):
            risk_tags.append(tag)
    if not risk_tags:
        risk_tags.append("NONE")

    if "UP" in trend or "BOOM" in trend:
        heat = "HIGH"
    elif "DOWN" in trend or "PANIC" in trend or "CRASH" in trend:
        heat = "LOW"
    else:
        heat = "MEDIUM"

    key_event = bullets[0].replace("\n", " ").strip()
    if len(key_event) > 72:
        key_event = key_event[:69] + "..."

    return f"Trend={trend} | Heat={heat} | RiskTags={','.join(risk_tags)} | Key={key_event}"


def batched_determine_role(
    agents: list[Agent],
    month: int,
    market: Market,
    macro_summary: str = "平稳",
    min_cash_observer: float = 500000
) -> list[dict]:
    """
    Batch process agents to determine roles using a single LLM call per batch.
    """
    if not agents:
        return []

    # Construct Batch Data
    agent_summaries = []
    for a in agents:
        persona_packet = getattr(a, "_activation_persona_packet", {}) or {}
        info_delay = int(persona_packet.get("info_delay_months", getattr(a, "info_delay_months", 0)) or 0)
        lifecycle_packet = build_activation_lifecycle_packet(
            a,
            month,
            min_cash_observer=min_cash_observer,
        )
        summary = {
            "id": a.id,
            "age": a.age,
            "income": a.monthly_income,
            "cash": a.cash,
            "props": len(a.owned_properties),
            "background": a.story.background_story[:50] + "...",
            "need": a.story.housing_need,
            "style": a.story.investment_style,
            "motive": getattr(a.story, "purchase_motive_primary", ""),
            "housing_stage": getattr(a.story, "housing_stage", ""),
            "family_stage": getattr(a.story, "family_stage", ""),
            "education_path": getattr(a.story, "education_path", ""),
            "financial_profile": getattr(a.story, "financial_profile", ""),
            "profile_bucket_id": persona_packet.get("profile_bucket_id", getattr(a, "profile_bucket_id", "")),
            "role_side_hint": persona_packet.get("role_side_hint", ""),
            "target_zone": persona_packet.get("target_zone", getattr(a.preference, "target_zone", "")),
            "need_school_district": bool(
                persona_packet.get(
                    "need_school_district",
                    getattr(a.preference, "need_school_district", False),
                )
            ),
            "property_type_target": persona_packet.get("property_type_target", ""),
            "budget_band": persona_packet.get("budget_band", ""),
            "eligible_property_bucket_count_this_month": persona_packet.get(
                "eligible_property_bucket_count_this_month",
                0,
            ),
            "bucket_supply_pressure": persona_packet.get("bucket_supply_pressure", ""),
            "buyer_to_supply_ratio": persona_packet.get("buyer_to_supply_ratio", 0.0),
            "info_delay_months": info_delay,
            "lifecycle_labels": lifecycle_packet["labels"],
            "lifecycle_summary": lifecycle_packet["summary"],
            "entry_window": lifecycle_packet["entry_window"],
            "recent_events": lifecycle_packet["recent_events"],
            "market_signal_labels": lifecycle_packet.get("market_signal_labels", []),
            "market_signal_summary": lifecycle_packet.get("market_signal_summary", ""),
            "behavior_modifier": build_behavior_modifier(a, getattr(a, "agent_type", "normal"), info_delay),
        }
        agent_summaries.append(summary)

    # Dynamic part follows static system prompt
    prompt = f"""
    【当前宏观环境】{macro_summary}
    【规则参数】无房且现金 < {min_cash_observer:,.0f} 元的Agent只能是OBSERVER

    【待处理Agent列表】({len(agents)}人):
    {_stable_prompt_json(agent_summaries)}
    """

    default_response = []

    # Use global system prompt for caching
    response = safe_call_llm(
        prompt,
        default_response,
        system_prompt=BATCH_ROLE_SYSTEM_PROMPT,
        model_type="fast",
    )

    if not isinstance(response, list):
        return []

    return response


async def batched_determine_role_async(
    agents: list[Agent],
    month: int,
    market: Market,
    macro_summary: str = "平稳",
    market_trend: str = "STABLE",
    recent_bulletins: list[str] = None,
    min_cash_observer: float = 500000,
    decision_profile: str = "normal",
    model_type: str = "fast",
) -> list[dict]:
    """
    Async Batch process agents to determine roles. Optimizes prompt caching.
    """
    if not agents:
        return []

    # Offline/CI-friendly mode: return deterministic "mock LLM" decisions.
    # This keeps the pipeline runnable when real LLM network calls are unavailable,
    # while preserving the project rule that "role choice is LLM-layer output".
    mock_mode = str(os.getenv("LLM_MOCK_MODE", "false")).strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }
    if mock_mode:
        results: list[dict] = []
        trend = str(market_trend or "STABLE").upper()
        for a in agents:
            has_props = bool(getattr(a, "owned_properties", None))
            cash = float(getattr(a, "cash", 0) or 0)
            income = float(getattr(a, "monthly_income", 0) or 0)
            housing_need = str(getattr(getattr(a, "story", None), "housing_need", "") or "")
            housing_stage = str(getattr(getattr(a, "story", None), "housing_stage", "") or "")
            family_stage = str(getattr(getattr(a, "story", None), "family_stage", "") or "")
            investment_style = str(getattr(getattr(a, "story", None), "investment_style", "balanced") or "balanced").lower()
            chain_mode = None
            lifecycle_packet = build_activation_lifecycle_packet(
                a,
                month,
                min_cash_observer=min_cash_observer,
            )
            lifecycle_labels = list(lifecycle_packet["labels"])

            urgent_tokens = ("学", "入学", "结婚", "生", "二胎", "三胎", "换房", "改善", "赡养", "养老", "分家", "裂变")
            is_urgent = any(t in housing_need for t in urgent_tokens) or any(t in family_stage for t in urgent_tokens)
            deadline_pressure = "DEADLINE_PRESSURE" in lifecycle_labels or is_urgent
            chain_blocked = "CHAIN_BLOCKED" in lifecycle_labels
            liquidity_ready = "LIQUIDITY_READY" in lifecycle_labels

            if (not has_props) and cash < float(min_cash_observer or 0):
                role = "OBSERVER"
                trigger = "cash_insufficient"
                reason = "现金低于观望底线，暂时无法入场。"
                timing_role = TIMING_ROLE_NEED_WAIT if lifecycle_labels else TIMING_ROLE_OBSERVE_WAIT
                life_pressure = "urgent" if deadline_pressure else "patient"
            elif has_props and ("upgrade" in housing_stage or "改善" in housing_need or "换房" in housing_need):
                # Owners with clear upgrade intent: buy-sell linkage.
                role = "BUYER_SELLER"
                trigger = "upgrade_cycle"
                if chain_blocked or (not liquidity_ready and not deadline_pressure):
                    timing_role = TIMING_ROLE_SELL_THEN_BUY
                    chain_mode = "sell_first"
                    reason = "已持房且有改善/置换动机，但当前资金链更像先卖后买。"
                else:
                    timing_role = TIMING_ROLE_BUY_NOW
                    chain_mode = "buy_first"
                    reason = "已持房且有改善/置换动机，本月会先继续推进买入。"
                life_pressure = "urgent" if deadline_pressure else "opportunistic"
            elif has_props and cash < float(min_cash_observer or 0) * 1.2 and income < 18000:
                role = "SELLER"
                trigger = "cash_pressure"
                reason = "已持房但现金/收入压力偏大，倾向于先卖房释放资金。"
                timing_role = TIMING_ROLE_SELL_NOW
                life_pressure = "urgent"
            else:
                # No property + above observer floor, or owners without strong sell trigger.
                if (not has_props) and (deadline_pressure or liquidity_ready or income >= 20000):
                    role = "BUYER"
                    timing_role = TIMING_ROLE_BUY_NOW
                    trigger = "life_event" if deadline_pressure else "budget_ready"
                    reason = "具备基本入场能力，且本月窗口已打开。"
                    life_pressure = "urgent" if deadline_pressure else "opportunistic"
                    # In a hot (seller) market, conservative profiles may wait.
                    if trend in {"PANIC_UP", "HOT", "SELLER"} and investment_style == "conservative" and not deadline_pressure:
                        role = "OBSERVER"
                        timing_role = TIMING_ROLE_NEED_WAIT
                        trigger = "wait_for_cooldown"
                        reason = "市场偏热且不紧迫，倾向继续观望等待。"
                        life_pressure = "patient"
                else:
                    role = "OBSERVER"
                    timing_role = TIMING_ROLE_NEED_WAIT if lifecycle_labels else TIMING_ROLE_OBSERVE_WAIT
                    trigger = "choose_to_wait"
                    reason = "当前存在需求但窗口未完全打开，或仍需继续等待。"
                    life_pressure = "patient"

            risk_mode = "aggressive" if investment_style == "aggressive" else "conservative" if investment_style == "conservative" else "balanced"
            urgency_level = (
                "high"
                if timing_role in {TIMING_ROLE_BUY_NOW, TIMING_ROLE_SELL_NOW, TIMING_ROLE_SELL_THEN_BUY} and deadline_pressure
                else "medium"
                if timing_role in {TIMING_ROLE_BUY_NOW, TIMING_ROLE_SELL_NOW, TIMING_ROLE_SELL_THEN_BUY, TIMING_ROLE_NEED_WAIT}
                else "low"
            )
            results.append(
                {
                    "id": int(getattr(a, "id", -1) or -1),
                    "role": role,
                    "timing_role": timing_role,
                    "trigger": trigger,
                    "reason": reason,
                    "urgency_level": urgency_level,
                    "life_pressure": life_pressure,
                    "price_expectation": 1.0,
                    "chain_mode": chain_mode if role == "BUYER_SELLER" else None,
                    "risk_mode": risk_mode,
                    "listing_action": "KEEP",
                    "lifecycle_labels": lifecycle_labels,
                    "lifecycle_summary": lifecycle_packet["summary"],
                    "behavior_modifier": build_behavior_modifier(a, decision_profile, int(getattr(a, "info_delay_months", 0) or 0)),
                    "_decision_origin": "mock_llm_activation",
                    "_llm_called": False,
                }
            )
        return results

    # Construct Batch Data
    agent_summaries = []
    for a in agents:
        info_delay = int(getattr(a, "info_delay_months", 0) or 0)
        lifecycle_packet = build_activation_lifecycle_packet(
            a,
            month,
            min_cash_observer=min_cash_observer,
        )
        summary = {
            "id": a.id,
            "age": a.age,
            "income": a.monthly_income,
            "cash": a.cash,
            "props": len(a.owned_properties),
            "background": a.story.background_story[:50] + "...",
            "need": a.story.housing_need,
            "style": a.story.investment_style,
            "motive": getattr(a.story, "purchase_motive_primary", ""),
            "housing_stage": getattr(a.story, "housing_stage", ""),
            "family_stage": getattr(a.story, "family_stage", ""),
            "education_path": getattr(a.story, "education_path", ""),
            "financial_profile": getattr(a.story, "financial_profile", ""),
            "info_delay_months": info_delay,
            "lifecycle_labels": lifecycle_packet["labels"],
            "lifecycle_summary": lifecycle_packet["summary"],
            "entry_window": lifecycle_packet["entry_window"],
            "recent_events": lifecycle_packet["recent_events"],
            "market_signal_labels": lifecycle_packet.get("market_signal_labels", []),
            "market_signal_summary": lifecycle_packet.get("market_signal_summary", ""),
            "behavior_modifier": build_behavior_modifier(a, decision_profile, info_delay),
        }
        agent_summaries.append(summary)

    bulletin_text = _build_market_memory_one_liner(market_trend, recent_bulletins)

    profile_hint = (
        "你获取的信息更完整，会结合预算、热度、竞争强度和风险收益做权衡，但仍需遵守系统硬约束。"
        if decision_profile == "smart"
        else "你是信息有限的普通家庭/普通业主，只根据眼前生活压力、身边传闻和滞后的市场印象判断，容易跟随周围气氛，不做复杂机会成本推演。"
    )
    profile_detail = (
        "- 若为 smart 路径：可以系统比较预算、热度、竞争强度与风险收益，再决定买、卖还是观望。"
        if decision_profile == "smart"
        else "- 若为 normal 路径：请把“近期市场动态”当作零散传闻和滞后印象，而不是完整研究报告。普通代理人更看重生活事件、现金压力、周边人是否在买卖，可能跟随氛围，但不要无中生有。"
    )

    prompt = f"""
    【当前宏观环境】{macro_summary} (市场趋势: {market_trend})
    【决策路径】{decision_profile}
    【路径提示】{profile_hint}

    【近期市场动态 (Market Memory, One-Liner)】
    {bulletin_text}

    【规则参数】
    - 无房且现金 < {min_cash_observer:,.0f} 元者只能是 OBSERVER。

    【任务】
    请分析以下Agent列表，根据他们的财务状况、需求、宏观环境和近期市场动态，判断每个人本月是否进入住房决策窗口，并输出 timing_role + role。
    - buy_now: 本月立即买/推进买入。对应 role=BUYER 或 BUYER_SELLER。
    - sell_now: 本月立即卖/推进卖出。对应 role=SELLER。
    - sell_then_buy: 本月先卖后买。对应 role=BUYER_SELLER，且 chain_mode=sell_first。
    - need_wait: 有住房需求，但本月不进场。对应 role=OBSERVER。
    - observe_wait: 继续观望。对应 role=OBSERVER。
    - 若提供了 `profile_bucket_id / role_side_hint / target_zone / need_school_district / property_type_target / budget_band / eligible_property_bucket_count_this_month / bucket_supply_pressure`，
      请把它们当作该人本月所处的结构化画像与可见供需上下文；它们用于帮助你理解这类人通常会关注什么房、面临什么供需压力，但不能替代你对个人是否激活的判断。
    - `lifecycle_labels / lifecycle_summary / entry_window` 是本月生命周期窗口提示；请先判断“窗口是否打开”，再判断角色。
    - `market_signal_labels / market_signal_summary` 是卖方侧供给续航提示，只能作为“是否进入卖方窗口”的辅助事实，不允许机械见到标签就直接输出 SELLER。
    - `behavior_modifier` 描述 smart/normal 在同一决策窗口内的可见信息、处理参照系、行动时点与稳定性差异。
    {profile_detail}

    【重要约束】
    1. 若Agent无房产(props=0)，严禁在 reasoning 中虚构“卖掉郊区房产”或“卖掉名下房产”等理由。若需描述资金来源，必须描述为“卖掉外省老家房产”或“父母资助”。
    2. 无房产者不可成为 SELLER。
    3. 若 timing_role=need_wait 或 observe_wait，role 必须是 OBSERVER。
    4. 若 timing_role=sell_then_buy，role 必须是 BUYER_SELLER，且 chain_mode=sell_first。

    【输出要求】
    请输出严格的JSON列表格式，包含每个Agent的决策：
    [
      {{"id": 123, "role": "BUYER", "timing_role": "buy_now", "trigger": "life_event", "reason": "...", "urgency_level": "high", "life_pressure": "calm", "price_expectation": 1.0, "risk_mode": "balanced", "listing_action": "KEEP"}},
      {{"id": 456, "role": "BUYER_SELLER", "timing_role": "sell_then_buy", "trigger": "换房", "reason": "...", "urgency_level": "high", "life_pressure": "urgent", "price_expectation": 1.02, "chain_mode": "sell_first", "risk_mode": "conservative", "listing_action": "KEEP"}}
    ]

    【待处理Agent列表】({len(agents)}人):
    {_stable_prompt_json(agent_summaries)}
    """

    default_response = []

    # Use global system prompt for caching
    response = await safe_call_llm_async(
        prompt,
        default_response,
        system_prompt=BATCH_ROLE_SYSTEM_PROMPT,
        model_type=model_type,
    )

    if not isinstance(response, list):
        return []

    return response


async def determine_buyer_seller_chain_mode_async(
    agent: Agent,
    month: int,
    market: Market,
    *,
    macro_summary: str = "平稳",
    market_trend: str = "STABLE",
    recent_bulletins: list | None = None,
    decision_profile: str = "normal",
    prior_reason: str = "",
    model_type: str = "fast",
) -> Dict:
    """LLM decides whether an already-activated BUYER_SELLER should buy first or sell first this month."""
    mock_mode = str(os.getenv("LLM_MOCK_MODE", "false")).strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }
    if mock_mode:
        housing_need = str(getattr(getattr(agent, "story", None), "housing_need", "") or "")
        family_stage = str(getattr(getattr(agent, "story", None), "family_stage", "") or "")
        purchase_motive = str(getattr(getattr(agent, "story", None), "purchase_motive_primary", "") or "")
        investment_style = str(
            getattr(getattr(agent, "story", None), "investment_style", "balanced") or "balanced"
        ).lower()
        cash = float(getattr(agent, "cash", 0.0) or 0.0)
        income = float(getattr(agent, "monthly_income", 0.0) or 0.0)
        urgent_tokens = ("学", "入学", "结婚", "生", "二胎", "三胎", "换房", "改善", "赡养", "养老", "分家", "裂变")
        is_urgent = any(t in housing_need for t in urgent_tokens) or any(t in family_stage for t in urgent_tokens)
        has_school_window = family_stage in {"primary_school_before_transition", "junior_school_transition", "senior_school_transition"}
        trend = str(market_trend or "STABLE").upper()

        if cash < max(300000.0, income * 10.0):
            choice = "sell_first"
            reason = "现金缓冲偏薄，先卖旧房更稳妥。"
        elif trend in {"PANIC_UP", "HOT", "SELLER"} and investment_style == "conservative" and not is_urgent:
            choice = "sell_first"
            reason = "市场偏热且并不紧迫，先卖旧房、保住资金弹性更稳妥。"
        elif has_school_window or purchase_motive in {"education_driven", "upgrade_living"}:
            choice = "buy_first"
            reason = "家庭阶段或改善任务更急，本月会先继续看房推进。"
        else:
            choice = "sell_first"
            reason = "更像置换型家庭，先卖后买更符合当前节奏。"

        return {
            "chain_mode": choice,
            "reason": reason,
            "llm_called": False,
            "decision_profile": str(decision_profile or "normal"),
            "mock_mode": True,
        }

    bulletin_text = _build_market_memory_one_liner(market_trend, recent_bulletins)
    owned_props = []
    for prop in list(getattr(agent, "owned_properties", []) or [])[:3]:
        owned_props.append(
            {
                "property_id": int(prop.get("property_id", -1) or -1),
                "zone": str(prop.get("zone", "") or ""),
                "base_value": float(prop.get("base_value", 0.0) or 0.0),
                "status": str(prop.get("status", "") or ""),
                "is_school_district": int(prop.get("is_school_district", 0) or 0),
            }
        )
    summary = {
        "id": int(getattr(agent, "id", -1) or -1),
        "month": int(month),
        "age": int(getattr(agent, "age", 0) or 0),
        "cash": float(getattr(agent, "cash", 0.0) or 0.0),
        "income": float(getattr(agent, "monthly_income", 0.0) or 0.0),
        "owned_properties": owned_props,
        "housing_need": str(getattr(agent.story, "housing_need", "") or ""),
        "purchase_motive_primary": str(getattr(agent.story, "purchase_motive_primary", "") or ""),
        "housing_stage": str(getattr(agent.story, "housing_stage", "") or ""),
        "family_stage": str(getattr(agent.story, "family_stage", "") or ""),
        "education_path": str(getattr(agent.story, "education_path", "") or ""),
        "financial_profile": str(getattr(agent.story, "financial_profile", "") or ""),
        "investment_style": str(getattr(agent.story, "investment_style", "balanced") or "balanced"),
        "prior_role_reason": str(prior_reason or ""),
    }
    profile_hint = (
        "你获取的信息更完整，会比较资金链、卖旧房难度、看房紧迫度和市场热度。"
        if decision_profile == "smart"
        else "你是信息有限的普通家庭/普通业主，更看重眼前生活压力、周围氛围和滞后的市场印象。"
    )
    prompt = f"""
【当前宏观环境】{macro_summary} (市场趋势: {market_trend})
【决策路径】{decision_profile}
【路径提示】{profile_hint}

【近期市场动态 (Market Memory, One-Liner)】
{bulletin_text}

【任务】
这个家庭已经被判断为“既可能买也可能卖”的置换型/联动型角色。
请你只判断：它本月应该更像 buy_first 还是 sell_first。

【判断要点】
1. 如果现金缓冲薄、旧房不卖很难接新房，倾向 sell_first。
2. 如果生活任务很急（如上学窗口、明显改善刚需），且资金还能撑住，可能 buy_first。
3. 这个家庭已经通过上一层激活判断，本题不再回答“是否观望”，只能在 buy_first / sell_first 中二选一。
4. 不要机械地“有房就先卖”；也不要机械地“想换房就一定马上买”。

【待判断家庭】
{_stable_prompt_json(summary)}
"""
    default_choice = "sell_first" if float(getattr(agent, "cash", 0.0) or 0.0) < max(300000.0, float(getattr(agent, "monthly_income", 0.0) or 0.0) * 12.0) else "buy_first"
    default_resp = {
        "chain_mode": default_choice,
        "reason": "Fallback chain-mode decision",
    }
    resp = await safe_call_llm_async(
        prompt,
        default_resp,
        system_prompt=BUYER_SELLER_CHAIN_SYSTEM_PROMPT,
        model_type=model_type,
    )
    chain_mode = str((resp or {}).get("chain_mode", default_choice) or default_choice).strip().lower()
    if chain_mode not in {"buy_first", "sell_first"}:
        chain_mode = default_choice
    return {
        "chain_mode": chain_mode,
        "reason": str((resp or {}).get("reason", "") or ""),
        "llm_called": True,
        "decision_profile": str(decision_profile or "normal"),
        "mock_mode": False,
    }


# --- 5. Open Role Evaluation (LLM-Driven Free Strategy) ---

def open_role_evaluation(agent: Agent, month: int, market: Market, history_context: str = "") -> Dict:
    """
    开放式角色评估 - 让LLM自由决定Agent本月策略

    Args:
        agent: Agent对象
        month: 当前月份
        market: Agent对象
        history_context: Agent历史行为记录（用于保持一致性）

    Returns:
        dict: {"role": str, "action_description": str, "target_zone": str|None,
               "price_expectation": float|None, "urgency": float, "reasoning": str}
    """
    # from mortgage_system import calculate_max_affordable

    # 计算真实购买力
    # existing_payment = getattr(agent, 'monthly_payment', 0)
    # max_affordable = calculate_max_affordable(agent.cash, agent.monthly_income, existing_payment)

    # 获取市场状态
    # properties = getattr(market, 'properties', [])
    # supply = len([p for p in properties if p.get('status') == 'for_sale'])
    # total_props = len(properties)
    # demand_estimate = max(1, int(total_props * 0.08))

    # if supply > demand_estimate * 1.2:
    #     supply_demand_desc = "供过于求（买方市场）"
    # elif supply < demand_estimate * 0.8:
    #     supply_demand_desc = "供不应求（卖方市场）"
    # else:
    #     supply_demand_desc = "供需平衡"

    return {"role": "OBSERVER", "reasoning": "Placeholder"}


def should_agent_exit_market(agent: Agent, market: Market, duration_months: int) -> Tuple[bool, str]:
    """
    Determine if an active agent (Buyer/Seller) should exit due to fatigue or market conditions.
    Returns: (should_exit, reason)
    """
    # Base probability increases with duration
    base_exit_prob = min(0.1 * duration_months, 0.8)

    # Check patience based on "life pressure"
    pressure = getattr(agent, 'life_pressure', 'patient')
    if pressure == 'urgent' and duration_months > 2:
        return True, "Urgent need unmet, giving up"
    if pressure == 'anxious' and duration_months > 4:
        return True, "Anxiety overwhelmed patience"

    # Random roll
    if random.random() < base_exit_prob:
        return True, f"Market fatigue after {duration_months} months"

    return False, ""
