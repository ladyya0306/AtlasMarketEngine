import asyncio
import datetime
import html
import json
import os
import sqlite3
import threading
from typing import Any, Dict, List, Optional
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import yaml

from config.agent_tiers import AGENT_TIER_CONFIG
from config.config_loader import SimulationConfig
from real_estate_demo_v2_1 import (
    _choose_experiment_mode,
    _choose_profile_pack,
    _derive_agent_count_from_supply,
    _estimate_listing_rate,
    _scale_role_defaults,
    apply_scholar_release_config,
    build_scaled_profile_pack_from_snapshot,
    load_release_supply_snapshot_options,
)
from simulation_runner import SimulationRunner


class StartSimulationRequest(BaseModel):
    agent_count: Optional[int] = Field(default=None, ge=1)
    months: Optional[int] = Field(default=None, ge=1)
    seed: Optional[int] = None
    resume: bool = False
    night_run: bool = False
    config_path: str = "config/baseline.yaml"
    night_plan_path: Optional[str] = None
    preplanned_interventions: Optional[List[Dict[str, Any]]] = None
    db_path: Optional[str] = None
    startup_overrides: Optional["StartupOverrides"] = None


class StartupTierConfig(BaseModel):
    tier: str
    count: int = Field(ge=0)
    income_min: int = Field(ge=0)
    income_max: int = Field(ge=0)
    property_min: int = Field(ge=0)
    property_max: int = Field(ge=0)


class StartupZoneConfig(BaseModel):
    zone: str
    price_min: float = Field(gt=0)
    price_max: float = Field(gt=0)
    rent_per_sqm: float = Field(gt=0)


class StartupOverrides(BaseModel):
    use_release_supply_controls: Optional[bool] = None
    fixed_supply_snapshot_id: Optional[str] = None
    market_goal: Optional[str] = None
    demand_multiplier: Optional[float] = Field(default=None, ge=0.1, le=2.0)
    property_count: Optional[int] = Field(default=None, ge=1)
    agent_tiers: Optional[List[StartupTierConfig]] = None
    zones: Optional[List[StartupZoneConfig]] = None
    min_cash_observer_threshold: Optional[int] = Field(default=None, ge=0)
    base_year: Optional[int] = Field(default=None, ge=1900, le=2100)
    income_adjustment_rate: Optional[float] = Field(default=None, gt=0)
    enable_intervention_panel: Optional[bool] = None
    market_pulse_enabled: Optional[bool] = None
    market_pulse_seed_ratio: Optional[float] = Field(default=None, ge=0, le=1)
    down_payment_ratio: Optional[float] = Field(default=None, gt=0, lt=1)
    max_dti_ratio: Optional[float] = Field(default=None, gt=0, lt=1)
    annual_interest_rate: Optional[float] = Field(default=None, gt=0, lt=1)
    effective_bid_floor_ratio: Optional[float] = Field(default=None, ge=0.5, le=1.2)
    precheck_liquidity_buffer_months: Optional[int] = Field(default=None, ge=0, le=36)
    precheck_include_tax_and_fee: Optional[bool] = None


try:
    StartSimulationRequest.model_rebuild()
except AttributeError:
    StartSimulationRequest.update_forward_refs()


class RuntimeControlsRequest(BaseModel):
    down_payment_ratio: Optional[float] = Field(default=None, gt=0, lt=1)
    annual_interest_rate: Optional[float] = Field(default=None, gt=0, lt=1)
    max_dti_ratio: Optional[float] = Field(default=None, gt=0, lt=1)
    market_pulse_enabled: Optional[bool] = None
    macro_override_mode: Optional[str] = None
    negotiation_quote_stream_enabled: Optional[bool] = None
    negotiation_quote_filter_mode: Optional[str] = None
    negotiation_quote_mode: Optional[str] = None
    negotiation_quote_turn_limit: Optional[int] = Field(default=None, ge=1, le=20)
    negotiation_quote_char_limit: Optional[int] = Field(default=None, ge=20, le=500)


class PopulationInterventionRequest(BaseModel):
    count: int = Field(ge=1)
    tier: str
    template: Optional[str] = None
    income_multiplier: Optional[float] = Field(default=None, gt=0)
    income_multiplier_min: Optional[float] = Field(default=None, gt=0)
    income_multiplier_max: Optional[float] = Field(default=None, gt=0)


class TierIncomeAdjustment(BaseModel):
    tier: str
    pct_change: float = Field(gt=-1, lt=10)


class IncomeInterventionRequest(BaseModel):
    pct_change: Optional[float] = Field(default=None, gt=-1, lt=10)
    target_tier: str = "all"
    tier_adjustments: Optional[List[TierIncomeAdjustment]] = None


class DeveloperSupplyInterventionRequest(BaseModel):
    count: int = Field(ge=1, le=100)
    zone: str
    template: Optional[str] = None
    price_per_sqm: Optional[float] = Field(default=None, gt=0)
    size: Optional[float] = Field(default=None, gt=0)
    school_units: Optional[int] = Field(default=None, ge=0)
    build_year: Optional[int] = Field(default=None, ge=1900, le=2100)


class ScenarioPresetRequest(BaseModel):
    preset: str


class ForensicRequest(BaseModel):
    db_path: Optional[str] = None


CONFIG_SCHEMA_FIELDS = [
    {
        "key": "simulation.agent_count",
        "label": "Agent Count",
        "type": "integer",
        "group": "simulation",
        "editable_phase": "startup_only",
        "description": "Total agents seeded when a new run starts.",
        "min": 1,
        "max": 100000,
        "step": 1,
    },
    {
        "key": "simulation.months",
            "label": "Simulation Rounds (virtual cycles)",
        "type": "integer",
        "group": "simulation",
        "editable_phase": "startup_only",
            "description": "Total number of virtual market rounds to simulate.",
        "min": 1,
        "max": 240,
        "step": 1,
    },
    {
        "key": "simulation.random_seed",
        "label": "Random Seed",
        "type": "integer",
        "group": "simulation",
        "editable_phase": "startup_only",
        "description": "Seed used for reproducible run initialization.",
        "step": 1,
    },
    {
        "key": "mortgage.down_payment_ratio",
        "label": "Down Payment Ratio",
        "type": "number",
        "group": "financing",
        "editable_phase": "between_steps",
        "description": "Required down payment ratio for mortgage precheck.",
        "min": 0.05,
        "max": 0.95,
        "step": 0.01,
    },
    {
        "key": "mortgage.annual_interest_rate",
        "label": "Annual Interest Rate",
        "type": "number",
        "group": "financing",
        "editable_phase": "between_steps",
        "description": "Annual mortgage rate used by affordability checks.",
        "min": 0.001,
        "max": 0.5,
        "step": 0.001,
    },
    {
        "key": "mortgage.max_dti_ratio",
        "label": "Max DTI Ratio",
        "type": "number",
        "group": "financing",
        "editable_phase": "between_steps",
        "description": "Upper debt-to-income ratio allowed during mortgage screening.",
        "min": 0.1,
        "max": 0.95,
        "step": 0.01,
    },
    {
        "key": "macro_environment.override_mode",
        "label": "Macro Override Mode",
        "type": "enum",
        "group": "macro",
        "editable_phase": "between_steps",
        "description": "Override the round-based macro schedule with a fixed market sentiment.",
        "options": ["", "optimistic", "stable", "pessimistic"],
    },
    {
        "key": "market_pulse.enabled",
        "label": "Market Pulse Enabled",
        "type": "boolean",
        "group": "macro",
        "editable_phase": "between_steps",
        "description": "Enable mortgage stress cycle and delinquency monitoring.",
    },
    {
        "key": "negotiation.quote_stream_enabled",
        "label": "Negotiation Quote Stream",
        "type": "boolean",
        "group": "negotiation",
        "editable_phase": "between_steps",
        "description": "Emit limited quote events alongside negotiation summaries.",
    },
    {
        "key": "negotiation.quote_filter_mode",
        "label": "Quote Filter Mode",
        "type": "enum",
        "group": "negotiation",
        "editable_phase": "between_steps",
        "description": "Select which negotiations are allowed to emit quote events.",
        "options": ["all", "focused", "heated_only", "high_value_only"],
    },
    {
        "key": "negotiation.quote_mode",
        "label": "Negotiation Quote Mode",
        "type": "enum",
        "group": "negotiation",
        "editable_phase": "between_steps",
        "description": "Choose whether negotiations emit summaries, clipped quotes, or full turn replay.",
        "options": ["off", "summary", "limited_quotes", "full_quotes"],
    },
    {
        "key": "negotiation.quote_turn_limit",
        "label": "Quote Turn Limit",
        "type": "integer",
        "group": "negotiation",
        "editable_phase": "between_steps",
        "description": "Maximum number of negotiation turns exposed to the frontend replay.",
        "min": 1,
        "max": 20,
        "step": 1,
    },
    {
        "key": "negotiation.quote_char_limit",
        "label": "Quote Char Limit",
        "type": "integer",
        "group": "negotiation",
        "editable_phase": "between_steps",
        "description": "Character limit for each negotiation turn snippet sent to the frontend.",
        "min": 20,
        "max": 500,
        "step": 1,
    },
    {
        "key": "decision_factors.activation.base_probability",
        "label": "Activation Base Probability",
        "type": "number",
        "group": "market_dynamics",
        "editable_phase": "startup_only",
        "description": "Base per-round activation probability before funnel routing.",
        "min": 0.0,
        "max": 1.0,
        "step": 0.001,
    },
    {
        "key": "decision_factors.activation.batch_size",
        "label": "Activation Batch Size",
        "type": "integer",
        "group": "performance",
        "editable_phase": "startup_only",
        "description": "LLM batch size used during activation role routing.",
        "min": 1,
        "max": 200,
        "step": 1,
    },
    {
        "key": "smart_agent.ratio",
        "label": "Smart Agent Ratio",
        "type": "number",
        "group": "agents",
        "editable_phase": "startup_only",
        "description": "Share of smart agents when explicit count is not set.",
        "min": 0.0,
        "max": 1.0,
        "step": 0.01,
    },
    {
        "key": "smart_agent.max_buys_per_month",
        "label": "Smart Agent Max Buys",
        "type": "integer",
        "group": "agents",
        "editable_phase": "startup_only",
        "description": "Upper bound on smart-agent buy orders per round.",
        "min": 0,
        "max": 20,
        "step": 1,
    },
    {
        "key": "market.initial_listing_rate",
        "label": "Initial Listing Rate",
        "type": "number",
        "group": "supply",
        "editable_phase": "startup_only",
        "description": "Initial share of properties listed when market is seeded.",
        "min": 0.0,
        "max": 1.0,
        "step": 0.01,
    },
    {
        "key": "market.zones.A.base_price_per_sqm",
        "label": "Zone A Base Price Per Sqm",
        "type": "integer",
        "group": "supply",
        "editable_phase": "readonly",
        "description": "Baseline pricing anchor for Zone A property generation.",
        "step": 100,
    },
    {
        "key": "market.zones.B.base_price_per_sqm",
        "label": "Zone B Base Price Per Sqm",
        "type": "integer",
        "group": "supply",
        "editable_phase": "readonly",
        "description": "Baseline pricing anchor for Zone B property generation.",
        "step": 100,
    },
    {
        "key": "simulation.agent.savings_rate",
        "label": "Agent Savings Rate",
        "type": "number",
        "group": "simulation",
        "editable_phase": "readonly",
        "description": "Baseline per-round savings rate used when seeding agent cash flow.",
        "min": 0.0,
        "max": 1.0,
        "step": 0.01,
    },
    {
        "key": "simulation.agent.income_adjustment_rate",
        "label": "Income Adjustment Rate",
        "type": "number",
        "group": "simulation",
        "editable_phase": "readonly",
        "description": "Global income multiplier applied during initial simulation seeding.",
        "min": 0.0,
        "max": 5.0,
        "step": 0.01,
    },
    {
        "key": "simulation.enable_intervention_panel",
        "label": "CLI Intervention Panel",
        "type": "boolean",
        "group": "simulation",
        "editable_phase": "readonly",
            "description": "Whether the round-end CLI intervention panel is enabled.",
    },
    {
        "key": "simulation.base_year",
        "label": "Simulation Base Year",
        "type": "integer",
        "group": "simulation",
        "editable_phase": "readonly",
        "description": "Reference year used for property age calculations.",
        "step": 1,
    },
    {
        "key": "simulation.min_transactions_gate",
        "label": "Min Transactions Gate",
        "type": "integer",
        "group": "simulation",
        "editable_phase": "readonly",
        "description": "Minimum transaction gate used by regression scenarios.",
        "step": 1,
    },
    {
        "key": "simulation.low_tx_auto_relax_enabled",
        "label": "Low Tx Auto Relax",
        "type": "boolean",
        "group": "simulation",
        "editable_phase": "readonly",
        "description": "Automatically relax candidate pools when low transaction runs are detected.",
    },
    {
        "key": "market.panic_sell_threshold",
        "label": "Panic Sell Threshold",
        "type": "number",
        "group": "market_dynamics",
        "editable_phase": "readonly",
        "description": "Three-round price decline threshold that triggers panic sell logic.",
        "min": -1.0,
        "max": 0.0,
        "step": 0.01,
    },
    {
        "key": "decision_factors.activation.macro_volatility",
        "label": "Macro Volatility",
        "type": "number",
        "group": "market_dynamics",
        "editable_phase": "readonly",
        "description": "Baseline macro volatility used by the activation funnel.",
        "min": 0.0,
        "max": 1.0,
        "step": 0.01,
    },
    {
        "key": "decision_factors.activation.risk_free_rate",
        "label": "Risk Free Rate",
        "type": "number",
        "group": "market_dynamics",
        "editable_phase": "readonly",
        "description": "Reference risk-free rate exposed to activation and investment logic.",
        "min": 0.0,
        "max": 1.0,
        "step": 0.001,
    },
    {
        "key": "decision_factors.activation.rental.zone_a_rent_per_sqm",
        "label": "Zone A Rent Per Sqm",
        "type": "integer",
        "group": "market_dynamics",
        "editable_phase": "readonly",
        "description": "Monthly rent baseline per sqm for Zone A rental context.",
        "step": 1,
    },
    {
        "key": "decision_factors.activation.rental.zone_b_rent_per_sqm",
        "label": "Zone B Rent Per Sqm",
        "type": "integer",
        "group": "market_dynamics",
        "editable_phase": "readonly",
        "description": "Monthly rent baseline per sqm for Zone B rental context.",
        "step": 1,
    },
    {
        "key": "decision_factors.buyer_timeout_months",
        "label": "Buyer Timeout Rounds",
        "type": "integer",
        "group": "market_dynamics",
        "editable_phase": "readonly",
        "description": "Buyer timeout window before demand leaves the market.",
        "step": 1,
    },
    {
        "key": "decision_factors.listing_stale_months",
        "label": "Listing Stale Rounds",
        "type": "integer",
        "group": "market_dynamics",
        "editable_phase": "readonly",
        "description": "How long listings remain stale before strategic adjustments kick in.",
        "step": 1,
    },
    {
        "key": "decision_factors.auto_price_cut_rate",
        "label": "Auto Price Cut Rate",
        "type": "number",
        "group": "market_dynamics",
        "editable_phase": "readonly",
        "description": "Automatic stale-listing price cut rate.",
        "min": 0.0,
        "max": 1.0,
        "step": 0.01,
    },
    {
        "key": "life_events.monthly_event_trigger_prob",
        "label": "Life Event Trigger Probability Per Round",
        "type": "number",
        "group": "life_events",
        "editable_phase": "readonly",
        "description": "Per-round probability of triggering a life event before optional LLM reasoning.",
        "min": 0.0,
        "max": 1.0,
        "step": 0.01,
    },
    {
        "key": "life_events.llm_reasoning_enabled",
        "label": "Life Event LLM Reasoning",
        "type": "boolean",
        "group": "life_events",
        "editable_phase": "readonly",
        "description": "Whether triggered life events request LLM-generated explanations.",
    },
    {
        "key": "property_allocation.strategy",
        "label": "Property Allocation Strategy",
        "type": "enum",
        "group": "allocation",
        "editable_phase": "readonly",
        "description": "Strategy used when allocating initial properties across agents.",
        "options": ["value_descending"],
    },
    {
        "key": "transaction_costs.buyer.brokerage_ratio",
        "label": "Buyer Brokerage Ratio",
        "type": "number",
        "group": "transaction_costs",
        "editable_phase": "readonly",
        "description": "Buyer-side brokerage fee ratio applied during settlement.",
        "min": 0.0,
        "max": 1.0,
        "step": 0.001,
    },
    {
        "key": "transaction_costs.buyer.tax_ratio",
        "label": "Buyer Tax Ratio",
        "type": "number",
        "group": "transaction_costs",
        "editable_phase": "readonly",
        "description": "Buyer-side transaction tax ratio.",
        "min": 0.0,
        "max": 1.0,
        "step": 0.001,
    },
    {
        "key": "transaction_costs.seller.brokerage_ratio",
        "label": "Seller Brokerage Ratio",
        "type": "number",
        "group": "transaction_costs",
        "editable_phase": "readonly",
        "description": "Seller-side brokerage fee ratio applied during settlement.",
        "min": 0.0,
        "max": 1.0,
        "step": 0.001,
    },
    {
        "key": "transaction_costs.seller.tax_ratio",
        "label": "Seller Tax Ratio",
        "type": "number",
        "group": "transaction_costs",
        "editable_phase": "readonly",
        "description": "Seller-side transaction tax ratio.",
        "min": 0.0,
        "max": 1.0,
        "step": 0.001,
    },
    {
        "key": "system.llm.max_calls_per_month",
        "label": "LLM Max Calls Per Round",
        "type": "integer",
        "group": "system",
        "editable_phase": "readonly",
        "description": "Per-round LLM budget ceiling used by the simulation runtime.",
        "step": 1,
    },
    {
        "key": "system.llm.enable_caching",
        "label": "LLM Caching Enabled",
        "type": "boolean",
        "group": "system",
        "editable_phase": "readonly",
        "description": "Whether the LLM client cache is enabled by baseline config.",
    },
    {
        "key": "system.llm.max_concurrency_smart",
        "label": "Smart LLM Concurrency",
        "type": "integer",
        "group": "system",
        "editable_phase": "readonly",
        "description": "Concurrency ceiling for smart-model LLM calls.",
        "step": 1,
    },
    {
        "key": "system.llm.max_concurrency_fast",
        "label": "Fast LLM Concurrency",
        "type": "integer",
        "group": "system",
        "editable_phase": "readonly",
        "description": "Concurrency ceiling for fast-model LLM calls.",
        "step": 1,
    },
    {
        "key": "system.output.log_level",
        "label": "Output Log Level",
        "type": "enum",
        "group": "system",
        "editable_phase": "readonly",
        "description": "Baseline logging level used for simulation output.",
        "options": ["DEBUG", "INFO", "WARNING", "ERROR"],
    },
    {
        "key": "smart_agent.enabled",
        "label": "Smart Agent Enabled",
        "type": "boolean",
        "group": "agents",
        "editable_phase": "readonly",
        "description": "Whether smart-agent strategy routing is enabled.",
    },
    {
        "key": "smart_agent.count",
        "label": "Smart Agent Count",
        "type": "integer",
        "group": "agents",
        "editable_phase": "readonly",
        "description": "Explicit smart-agent count override when provided.",
        "step": 1,
    },
    {
        "key": "smart_agent.max_sells_per_month",
        "label": "Smart Agent Max Sells Per Round",
        "type": "integer",
        "group": "agents",
        "editable_phase": "readonly",
        "description": "Upper bound on smart-agent sell orders per round.",
        "step": 1,
    },
    {
        "key": "smart_agent.bid_aggressiveness",
        "label": "Bid Aggressiveness",
        "type": "number",
        "group": "agents",
        "editable_phase": "readonly",
        "description": "Baseline aggressiveness factor used by smart-agent bidding.",
        "min": 0.0,
        "max": 1.0,
        "step": 0.01,
    },
    {
        "key": "smart_agent.order_ttl_days",
        "label": "Order TTL Days",
        "type": "integer",
        "group": "agents",
        "editable_phase": "readonly",
        "description": "Lifetime of smart-agent orders before they expire.",
        "step": 1,
    },
    {
        "key": "smart_agent.deposit_ratio",
        "label": "Deposit Ratio",
        "type": "number",
        "group": "agents",
        "editable_phase": "readonly",
        "description": "Deposit ratio reserved when smart-agent orders are placed.",
        "min": 0.0,
        "max": 1.0,
        "step": 0.01,
    },
    {
        "key": "smart_agent.candidate_top_k",
        "label": "Candidate Top K",
        "type": "integer",
        "group": "agents",
        "editable_phase": "readonly",
        "description": "Top-K candidate pool size for smart-agent property matching.",
        "step": 1,
    },
    {
        "key": "smart_agent.leverage_cap",
        "label": "Leverage Cap",
        "type": "number",
        "group": "agents",
        "editable_phase": "readonly",
        "description": "Maximum leverage ratio allowed by smart-agent capital gates.",
        "min": 0.0,
        "max": 2.0,
        "step": 0.01,
    },
    {
        "key": "market_pulse.seed_existing_mortgage_ratio",
        "label": "Seed Existing Mortgage Ratio",
        "type": "number",
        "group": "pulse",
        "editable_phase": "readonly",
        "description": "Share of households seeded with existing mortgages in pulse mode.",
        "min": 0.0,
        "max": 1.0,
        "step": 0.01,
    },
    {
        "key": "market_pulse.seed_rate_base",
        "label": "Seed Mortgage Base Rate",
        "type": "number",
        "group": "pulse",
        "editable_phase": "readonly",
        "description": "Base interest rate for seeded legacy mortgages in pulse mode.",
        "min": 0.0,
        "max": 1.0,
        "step": 0.001,
    },
    {
        "key": "market_pulse.seed_loan_age_min_months",
        "label": "Seed Loan Age Min Periods",
        "type": "integer",
        "group": "pulse",
        "editable_phase": "readonly",
        "description": "Minimum age for seeded existing mortgage books.",
        "step": 1,
    },
    {
        "key": "market_pulse.seed_loan_age_max_months",
        "label": "Seed Loan Age Max Periods",
        "type": "integer",
        "group": "pulse",
        "editable_phase": "readonly",
        "description": "Maximum age for seeded existing mortgage books.",
        "step": 1,
    },
]

CONFIG_SCHEMA_GROUPS = {
    "simulation": "Simulation Setup",
    "financing": "Financing",
    "macro": "Macro / Pulse",
    "negotiation": "Negotiation Observability",
    "market_dynamics": "Market Dynamics",
    "performance": "Performance",
    "agents": "Agent Profile",
    "supply": "Supply Baseline",
    "life_events": "Life Events",
    "allocation": "Property Allocation",
    "transaction_costs": "Transaction Costs",
    "system": "System / LLM Runtime",
    "pulse": "Market Pulse",
}


SCENARIO_PRESETS = {
    "starter_demand_push": {
        "label": "starter demand push",
        "description": "Lower barriers, add first-home buyers, lift lower-middle income, inject entry-level supply, and focus quotes on heated starter bargaining.",
        "controls": {
            "down_payment_ratio": 0.22,
            "annual_interest_rate": 0.032,
            "macro_override_mode": "optimistic",
            "negotiation_quote_stream_enabled": True,
            "negotiation_quote_filter_mode": "heated_only",
        },
        "population": {
            "count": 6,
            "tier": "lower_middle",
            "template": "young_first_home",
        },
        "income": {
            "tier_adjustments": [
                {"tier": "lower_middle", "pct_change": 0.08},
                {"tier": "middle", "pct_change": 0.04},
            ]
        },
        "developer": {
            "count": 4,
            "zone": "B",
            "template": "b_entry_level",
        },
    },
    "upgrade_cycle": {
        "label": "upgrade cycle",
        "description": "Support middle-class upgrade demand with balanced A-zone supply, stable macro tone, and highlight high-value negotiations.",
        "controls": {
            "down_payment_ratio": 0.28,
            "annual_interest_rate": 0.036,
            "macro_override_mode": "stable",
            "negotiation_quote_stream_enabled": True,
            "negotiation_quote_filter_mode": "high_value_only",
        },
        "population": {
            "count": 5,
            "tier": "middle",
            "template": "middle_upgrade",
        },
        "income": {
            "tier_adjustments": [
                {"tier": "middle", "pct_change": 0.06},
                {"tier": "upper_middle", "pct_change": 0.05},
            ]
        },
        "developer": {
            "count": 3,
            "zone": "A",
            "template": "mixed_balanced",
        },
    },
    "investor_cooldown": {
        "label": "investor cooldown",
        "description": "Tighten financing, cut investor incomes, keep premium supply visible, and focus quotes on stressed negotiations.",
        "controls": {
            "down_payment_ratio": 0.35,
            "annual_interest_rate": 0.045,
            "macro_override_mode": "pessimistic",
            "market_pulse_enabled": False,
            "negotiation_quote_stream_enabled": True,
            "negotiation_quote_filter_mode": "focused",
        },
        "population": {
            "count": 3,
            "tier": "high",
            "template": "capital_investor",
        },
        "income": {
            "tier_adjustments": [
                {"tier": "high", "pct_change": -0.08},
                {"tier": "ultra_high", "pct_change": -0.12},
            ]
        },
        "developer": {
            "count": 2,
            "zone": "A",
            "template": "a_district_premium",
        },
    },
}


class SimulationRuntime:
    def __init__(self):
        self._lock = threading.Lock()
        self.runner: Optional[SimulationRunner] = None
        self.auto_running = False
        self.night_plan_path: Optional[str] = None
        self.night_run_thread: Optional[threading.Thread] = None

    @staticmethod
    def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        for key, value in override.items():
            if isinstance(value, dict) and isinstance(base.get(key), dict):
                SimulationRuntime._deep_merge(base[key], value)
            else:
                base[key] = value
        return base

    def _apply_night_plan(self, config: SimulationConfig, night_plan_path: Optional[str]) -> Optional[str]:
        plan_path_str = str(night_plan_path or "").strip()
        if not plan_path_str:
            return None
        plan_path = Path(plan_path_str)
        if not plan_path.exists():
            raise HTTPException(status_code=400, detail=f"Night run plan not found: {plan_path_str}")
        try:
            with plan_path.open("r", encoding="utf-8") as handle:
                overlay = yaml.safe_load(handle) or {}
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Failed to read night run plan: {exc}") from exc
        if not isinstance(overlay, dict):
            raise HTTPException(status_code=400, detail="Night run plan root must be a mapping.")
        self._deep_merge(config._config, overlay)
        return str(plan_path).replace("\\", "/")

    @staticmethod
    def _normalize_startup_tier_key(tier: str) -> str:
        key = str(tier or "").strip().lower()
        mapping = {
            "low_mid": "lower_middle",
            "lower_middle": "lower_middle",
            "mid_low": "lower_middle",
            "upper_middle": "upper_middle",
            "middle": "middle",
            "high": "high",
            "ultra_high": "ultra_high",
            "low": "low",
        }
        return mapping.get(key, key)

    def _apply_release_startup_overrides(
        self,
        config: SimulationConfig,
        overrides: StartupOverrides,
        *,
        months: int,
        seed: int,
        preplanned_interventions: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        options = load_release_supply_snapshot_options()
        if not options:
            raise HTTPException(status_code=500, detail="Release supply snapshots are not available.")
        by_id = {item["snapshot_id"]: item for item in options}
        recommended_snapshot_id = "spindle_medium" if "spindle_medium" in by_id else options[0]["snapshot_id"]
        snapshot_id = str(overrides.fixed_supply_snapshot_id or recommended_snapshot_id).strip()
        if snapshot_id not in by_id:
            raise HTTPException(status_code=400, detail=f"Unsupported fixed_supply_snapshot_id: {snapshot_id}")

        market_goal = str(overrides.market_goal or "balanced").strip().lower() or "balanced"
        if market_goal not in {"balanced", "buyer_market", "seller_market"}:
            raise HTTPException(status_code=400, detail=f"Unsupported market_goal: {market_goal}")

        demand_multiplier = float(
            overrides.demand_multiplier
            if overrides.demand_multiplier is not None
            else {"balanced": 1.00, "buyer_market": 0.80, "seller_market": 1.30}[market_goal]
        )
        supply_snapshot = dict(by_id[snapshot_id])
        property_count = int(supply_snapshot.get("total_selected_supply", 0) or 0)
        requested_agent_count = _derive_agent_count_from_supply(property_count, demand_multiplier)
        scaled_profile_pack, demand_bucket_plan = build_scaled_profile_pack_from_snapshot(
            base_profile_pack_path=str(
                supply_snapshot.get("profile_pack_path") or _choose_profile_pack(market_goal)
            ),
            snapshot_payload=dict(supply_snapshot.get("snapshot_payload") or {}),
            target_agent_total=requested_agent_count,
        )
        effective_agent_count = int(
            demand_bucket_plan.get("effective_agent_count", requested_agent_count) or requested_agent_count
        )
        effective_demand_multiplier = float(effective_agent_count) / float(max(1, property_count))
        role_defaults = _scale_role_defaults(effective_agent_count, market_goal)
        buyer_quota = max(0, int(role_defaults.get("BUYER", 0) or 0))
        seller_quota = min(
            max(0, int(role_defaults.get("SELLER", 0) or 0)),
            max(0, effective_agent_count - buyer_quota),
        )
        buyer_seller_quota = min(
            max(0, int(role_defaults.get("BUYER_SELLER", 0) or 0)),
            max(0, effective_agent_count - buyer_quota - seller_quota),
        )
        target_r_order_hint = {"balanced": 1.00, "buyer_market": 0.70, "seller_market": 1.30}[market_goal]
        listing_plan = _estimate_listing_rate(
            property_count=property_count,
            buyer_quota=buyer_quota,
            buyer_seller_quota=buyer_seller_quota,
            target_r_order_hint=target_r_order_hint,
        )
        scholar_inputs = {
            "market_goal": market_goal,
            "months": int(months),
            "agent_count": int(effective_agent_count),
            "property_count": int(property_count),
            "demand_multiplier": float(demand_multiplier),
            "effective_demand_multiplier": float(effective_demand_multiplier),
            "supply_snapshot": supply_snapshot,
            "profile_pack_inline": scaled_profile_pack,
            "demand_bucket_plan": demand_bucket_plan,
            "buyer_quota": int(buyer_quota),
            "seller_quota": int(seller_quota),
            "buyer_seller_quota": int(buyer_seller_quota),
            "target_r_order_hint": float(target_r_order_hint),
            "income_multiplier": float(overrides.income_adjustment_rate or 1.0),
            "force_role_months": min(int(months), 3),
            "profiled_market_mode": True,
            "hard_bucket_matcher": True,
            "enable_intervention_panel": bool(overrides.enable_intervention_panel)
            if overrides.enable_intervention_panel is not None
            else False,
            "open_startup_intervention_menu": False,
            "profile_pack_path": str(
                supply_snapshot.get("profile_pack_path") or _choose_profile_pack(market_goal)
            ),
            "experiment_mode": str(
                supply_snapshot.get("experiment_mode") or _choose_experiment_mode(market_goal)
            ),
            "listing_plan": listing_plan,
            "preplanned_interventions": list(preplanned_interventions or []),
            "seed": int(seed),
        }
        apply_scholar_release_config(config, scholar_inputs, start_month=1)
        return {
            "release_startup": {
                "snapshot_id": snapshot_id,
                "market_goal": market_goal,
                "requested_demand_multiplier": float(demand_multiplier),
                "effective_demand_multiplier": float(effective_demand_multiplier),
                "requested_agent_count": int(requested_agent_count),
                "effective_agent_count": int(effective_agent_count),
                "demand_bucket_plan": demand_bucket_plan,
            },
            "agent_count": int(effective_agent_count),
            "property_count": int(property_count),
        }

    def _apply_startup_overrides(
        self,
        config: SimulationConfig,
        overrides: Optional[StartupOverrides],
        *,
        months: int,
        seed: int,
        preplanned_interventions: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        if overrides is None:
            return {}

        applied: Dict[str, Any] = {}
        use_release_supply_controls = bool(
            overrides.use_release_supply_controls
            or overrides.fixed_supply_snapshot_id
            or overrides.demand_multiplier is not None
        )

        if use_release_supply_controls:
            applied.update(
                self._apply_release_startup_overrides(
                    config,
                    overrides,
                    months=months,
                    seed=seed,
                    preplanned_interventions=preplanned_interventions,
                )
            )

        if (not use_release_supply_controls) and overrides.property_count is not None:
            config._config["user_property_count"] = int(overrides.property_count)
            applied["property_count"] = int(overrides.property_count)

        if (not use_release_supply_controls) and overrides.agent_tiers:
            user_agent_config: Dict[str, Dict[str, Any]] = {}
            total_agents = 0
            for tier in overrides.agent_tiers:
                tier_key = self._normalize_startup_tier_key(tier.tier)
                if tier.income_max < tier.income_min:
                    raise HTTPException(status_code=400, detail=f"Invalid income range for tier {tier_key}.")
                if tier.property_max < tier.property_min:
                    raise HTTPException(status_code=400, detail=f"Invalid property range for tier {tier_key}.")
                user_agent_config[tier_key] = {
                    "count": int(tier.count),
                    "income_range": [int(tier.income_min), int(tier.income_max)],
                    "property_count": [int(tier.property_min), int(tier.property_max)],
                }
                total_agents += int(tier.count)
            config._config["user_agent_config"] = user_agent_config
            applied["agent_tiers"] = user_agent_config
            applied["agent_count"] = total_agents

        if (not use_release_supply_controls) and overrides.zones:
            zone_payload: Dict[str, Dict[str, float]] = {}
            for zone in overrides.zones:
                zone_key = str(zone.zone or "").strip().upper()
                if zone_key not in {"A", "B"}:
                    raise HTTPException(status_code=400, detail=f"Unsupported zone {zone.zone}.")
                if zone.price_max < zone.price_min:
                    raise HTTPException(status_code=400, detail=f"Invalid price range for zone {zone_key}.")
                config.update(f"market.zones.{zone_key}.price_per_sqm_range.min", float(zone.price_min))
                config.update(f"market.zones.{zone_key}.price_per_sqm_range.max", float(zone.price_max))
                rent_key = "zone_a_rent_per_sqm" if zone_key == "A" else "zone_b_rent_per_sqm"
                config.update(f"market.rental.{rent_key}", float(zone.rent_per_sqm))
                zone_payload[zone_key] = {
                    "price_min": float(zone.price_min),
                    "price_max": float(zone.price_max),
                    "rent_per_sqm": float(zone.rent_per_sqm),
                }
            applied["zones"] = zone_payload

        if overrides.min_cash_observer_threshold is not None:
            config.update(
                "decision_factors.activation.min_cash_observer_no_property",
                int(overrides.min_cash_observer_threshold),
            )
            applied["min_cash_observer_threshold"] = int(overrides.min_cash_observer_threshold)

        if overrides.base_year is not None:
            config.update("simulation.base_year", int(overrides.base_year))
            applied["base_year"] = int(overrides.base_year)

        if overrides.income_adjustment_rate is not None:
            config.update("simulation.agent.income_adjustment_rate", float(overrides.income_adjustment_rate))
            applied["income_adjustment_rate"] = float(overrides.income_adjustment_rate)

        if overrides.enable_intervention_panel is not None:
            config.update("simulation.enable_intervention_panel", bool(overrides.enable_intervention_panel))
            applied["enable_intervention_panel"] = bool(overrides.enable_intervention_panel)

        if overrides.market_pulse_enabled is not None:
            config.update("market_pulse.enabled", bool(overrides.market_pulse_enabled))
            applied["market_pulse_enabled"] = bool(overrides.market_pulse_enabled)

        if overrides.market_pulse_seed_ratio is not None:
            config.update("market_pulse.seed_existing_mortgage_ratio", float(overrides.market_pulse_seed_ratio))
            applied["market_pulse_seed_ratio"] = float(overrides.market_pulse_seed_ratio)

        if overrides.down_payment_ratio is not None:
            config.update("mortgage.down_payment_ratio", float(overrides.down_payment_ratio))
            applied["down_payment_ratio"] = float(overrides.down_payment_ratio)

        if overrides.max_dti_ratio is not None:
            config.update("mortgage.max_dti_ratio", float(overrides.max_dti_ratio))
            applied["max_dti_ratio"] = float(overrides.max_dti_ratio)

        if overrides.annual_interest_rate is not None:
            config.update("mortgage.annual_interest_rate", float(overrides.annual_interest_rate))
            applied["annual_interest_rate"] = float(overrides.annual_interest_rate)

        if overrides.effective_bid_floor_ratio is not None:
            config.update("smart_agent.effective_bid_floor_ratio", float(overrides.effective_bid_floor_ratio))
            applied["effective_bid_floor_ratio"] = float(overrides.effective_bid_floor_ratio)

        if overrides.precheck_liquidity_buffer_months is not None:
            config.update("smart_agent.precheck_liquidity_buffer_months", int(overrides.precheck_liquidity_buffer_months))
            applied["precheck_liquidity_buffer_months"] = int(overrides.precheck_liquidity_buffer_months)

        if overrides.precheck_include_tax_and_fee is not None:
            config.update("smart_agent.precheck_include_tax_and_fee", bool(overrides.precheck_include_tax_and_fee))
            applied["precheck_include_tax_and_fee"] = bool(overrides.precheck_include_tax_and_fee)

        return applied

    def _has_active_run(self) -> bool:
        if self.runner is None:
            return False
        return self.runner.status in {"initialized", "running", "paused"}

    def start(self, req: StartSimulationRequest):
        with self._lock:
            if self._has_active_run():
                raise HTTPException(status_code=409, detail="A simulation is already active.")

            if self.runner is not None:
                try:
                    self.runner.close()
                except Exception:
                    pass
                self.runner = None
            self.auto_running = False
            self.night_plan_path = None

            config = SimulationConfig(req.config_path)
            provisional_months = req.months or int(config.get("simulation.months", 12))
            provisional_seed = req.seed if req.seed is not None else config.get("simulation.random_seed", 42)
            if provisional_seed is None:
                provisional_seed = 42
            applied_night_plan = self._apply_night_plan(config, req.night_plan_path if req.night_run else None)
            applied_startup = self._apply_startup_overrides(
                config,
                req.startup_overrides,
                months=int(provisional_months),
                seed=int(provisional_seed),
                preplanned_interventions=req.preplanned_interventions,
            )
            config._config["_applied_startup_overrides"] = applied_startup
            config._config["_applied_night_plan"] = applied_night_plan
            if req.preplanned_interventions is not None:
                config.update("simulation.preplanned_interventions", req.preplanned_interventions)
                config._config["_inline_preplanned_interventions"] = req.preplanned_interventions
            if req.night_run:
                config.update("simulation.enable_intervention_panel", False)

            derived_agent_count = applied_startup.get("agent_count")
            agent_count = req.agent_count or derived_agent_count or int(config.get("simulation.agent_count", 50))
            months = req.months or int(config.get("simulation.months", 12))
            seed = req.seed if req.seed is not None else config.get("simulation.random_seed", 42)
            if seed is None:
                seed = 42

            self.runner = SimulationRunner(
                agent_count=agent_count,
                months=months,
                seed=int(seed),
                resume=req.resume,
                config=config,
                db_path=req.db_path,
            )
            self.runner.initialize()
            self.auto_running = bool(req.night_run)
            self.night_plan_path = applied_night_plan
            status = self.runner.get_status()
            status["run_mode"] = "night_run" if self.auto_running else "manual"
            status["night_plan_path"] = applied_night_plan
            return status

    def _step_locked(self):
        if self.runner is None:
            raise HTTPException(status_code=409, detail="No simulation has been started.")

        if self.runner.status == "completed":
            self.auto_running = False
            self.night_plan_path = None
            raise HTTPException(status_code=409, detail="Simulation already completed.")

        if self.runner.status == "failed":
            self.auto_running = False
            self.night_plan_path = None
            raise HTTPException(status_code=409, detail="Simulation is in failed state.")

        try:
            summary = self.runner.run_one_month()
        except RuntimeError as exc:
            if self.runner is not None and self.runner.status == "completed":
                self.auto_running = False
                self.night_plan_path = None
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except Exception as exc:
            self.auto_running = False
            self.night_plan_path = None
            raise HTTPException(status_code=500, detail=str(exc)) from exc

        status = self.runner.get_status()
        status["run_mode"] = "night_run" if self.auto_running else "manual"
        if status.get("status") in {"completed", "failed"}:
            self.auto_running = False
            self.night_plan_path = None
            status["run_mode"] = "manual"
        status["night_plan_path"] = self.night_plan_path
        return {
            "month_result": summary,
            "status": status,
        }

    def step(self):
        with self._lock:
            if self.auto_running:
                raise HTTPException(status_code=409, detail="Night run is in progress. Manual stepping is disabled.")
            return self._step_locked()

    def step_auto(self):
        with self._lock:
            return self._step_locked()

    def set_progress_callback(self, callback):
        with self._lock:
            if self.runner is not None:
                self.runner.set_progress_callback(callback)

    def status(self):
        with self._lock:
            if self.runner is None:
                return {
                    "status": "idle",
                    "initialized": False,
                    "current_month": 0,
                    "total_months": 0,
                    "remaining_months": 0,
                    "db_path": None,
                    "run_dir": None,
                    "last_error": None,
                    "started_at": None,
                    "completed_at": None,
                    "last_month_summary": None,
                    "final_summary": None,
                    "stage_snapshot": {
                        "focus_lane": "generated",
                        "counts": {
                            "generatedAgents": 0,
                            "generatedProperties": 0,
                            "activations": 0,
                            "listings": 0,
                            "matches": 0,
                            "negotiations": 0,
                            "successes": 0,
                            "failures": 0,
                        },
                        "nodes": [],
                    },
                    "stage_replay_events": [],
                    "runtime_controls": None,
                    "run_mode": "manual",
                    "night_plan_path": None,
                }
            status = self.runner.get_status()
            status["run_mode"] = "night_run" if self.auto_running else "manual"
            status["night_plan_path"] = self.night_plan_path
            return status

    def get_controls(self):
        with self._lock:
            if self.runner is None:
                raise HTTPException(status_code=409, detail="No simulation has been started.")
            return self.runner.get_runtime_controls()

    def update_controls(self, req: RuntimeControlsRequest):
        with self._lock:
            if self.runner is None:
                raise HTTPException(status_code=409, detail="No simulation has been started.")
            if self.runner.status not in {"initialized", "paused"}:
                raise HTTPException(status_code=409, detail="Controls can only be changed between round steps.")
            try:
                controls = self.runner.apply_runtime_controls(
                    down_payment_ratio=req.down_payment_ratio,
                    annual_interest_rate=req.annual_interest_rate,
                    max_dti_ratio=req.max_dti_ratio,
                    market_pulse_enabled=req.market_pulse_enabled,
                    macro_override_mode=req.macro_override_mode,
                    negotiation_quote_stream_enabled=req.negotiation_quote_stream_enabled,
                    negotiation_quote_filter_mode=req.negotiation_quote_filter_mode,
                    negotiation_quote_mode=req.negotiation_quote_mode,
                    negotiation_quote_turn_limit=req.negotiation_quote_turn_limit,
                    negotiation_quote_char_limit=req.negotiation_quote_char_limit,
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            return {
                "controls": controls,
                "status": self.runner.get_status(),
            }

    def _ensure_intervention_window(self):
        if self.runner is None:
            raise HTTPException(status_code=409, detail="No simulation has been started.")
        if self.runner.status not in {"initialized", "paused"}:
            raise HTTPException(status_code=409, detail="Interventions can only be applied between round steps.")

    def add_population(self, req: PopulationInterventionRequest):
        with self._lock:
            self._ensure_intervention_window()
            try:
                result = self.runner.add_population_intervention(
                    count=req.count,
                    tier=req.tier,
                    template=req.template,
                    income_multiplier=req.income_multiplier,
                    income_multiplier_min=req.income_multiplier_min,
                    income_multiplier_max=req.income_multiplier_max,
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            return {"result": result, "status": self.runner.get_status()}

    def apply_income_intervention(self, req: IncomeInterventionRequest):
        with self._lock:
            self._ensure_intervention_window()
            try:
                result = self.runner.apply_income_intervention(
                    pct_change=req.pct_change,
                    target_tier=req.target_tier,
                    tier_adjustments=req.tier_adjustments,
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            return {"result": result, "status": self.runner.get_status()}

    def inject_developer_supply(self, req: DeveloperSupplyInterventionRequest):
        with self._lock:
            self._ensure_intervention_window()
            try:
                result = self.runner.inject_developer_supply_intervention(
                    count=req.count,
                    zone=req.zone,
                    template=req.template,
                    price_per_sqm=req.price_per_sqm,
                    size=req.size,
                    school_units=req.school_units,
                    build_year=req.build_year,
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            return {"result": result, "status": self.runner.get_status()}

    def apply_scenario_preset(self, req: ScenarioPresetRequest):
        with self._lock:
            self._ensure_intervention_window()
            preset = str(req.preset or "").strip().lower()
            if preset not in SCENARIO_PRESETS:
                raise HTTPException(status_code=400, detail="Unknown preset. Use starter_demand_push, upgrade_cycle, or investor_cooldown.")

            spec = SCENARIO_PRESETS[preset]
            controls = self.runner.apply_runtime_controls(**spec["controls"])
            population_result = self.runner.add_population_intervention(**spec["population"])
            income_result = self.runner.apply_income_intervention(**spec["income"])
            developer_result = self.runner.inject_developer_supply_intervention(**spec["developer"])
            preset_history_entry = self.runner.record_scenario_preset(preset)
            return {
                "preset": preset,
                "controls": controls,
                "population_result": population_result,
                "income_result": income_result,
                "developer_result": developer_result,
                "preset_history_entry": preset_history_entry,
                "status": self.runner.get_status(),
            }


class WebSocketManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._clients = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        with self._lock:
            self._clients.append(websocket)

    def disconnect(self, websocket: WebSocket):
        with self._lock:
            self._clients = [client for client in self._clients if client is not websocket]

    async def broadcast(self, message):
        with self._lock:
            clients = list(self._clients)

        stale_clients = []
        for client in clients:
            try:
                await client.send_json(message)
            except Exception:
                stale_clients.append(client)

        if stale_clients:
            with self._lock:
                self._clients = [client for client in self._clients if client not in stale_clients]


runtime = SimulationRuntime()
ws_manager = WebSocketManager()
app = FastAPI(title="Visual Real Estate API", version="0.1.0")
app.mount("/web", StaticFiles(directory="web"), name="web")


def _event_envelope(event_type: str, payload: dict, month: int = 0, phase: str = "system"):
    run_id = "idle"
    if runtime.runner is not None:
        run_id = str(runtime.runner._run_dir).split("\\")[-1].split("/")[-1] or "run"
    return {
        "event_id": f"{run_id}:{event_type}:{month}:{threading.get_ident()}",
        "run_id": run_id,
        "month": int(month),
        "phase": phase,
        "event_type": event_type,
        "ts": datetime.datetime.now().isoformat(),
        "payload": payload,
        "source": "api_server",
        "schema_version": "v1",
    }


def _failure_event_payload(message: str):
    status = runtime.status()
    current_month = int(status.get("current_month", 0) or 0)
    next_month = current_month + 1 if status.get("status") != "completed" else current_month
    return _event_envelope(
        "RUN_FAILED",
        {
            "error_code": "SIMULATION_RUN_FAILED",
            "message": str(message),
            "recoverable": False,
            "status": status,
        },
        month=next_month,
        phase="system",
    )


def _current_config_source() -> SimulationConfig:
    if runtime.runner is not None and getattr(runtime.runner, "config", None) is not None:
        return runtime.runner.config
    return SimulationConfig("config/baseline.yaml")


def _normalize_schema_value(value, field_type: str):
    if value is None:
        return None
    if field_type == "boolean":
        return bool(value)
    if field_type == "integer":
        try:
            return int(value)
        except Exception:
            return value
    if field_type == "number":
        try:
            return float(value)
        except Exception:
            return value
    return value


def _build_config_schema():
    config = _current_config_source()
    baseline_config = SimulationConfig("config/baseline.yaml")
    parameters = []
    for item in CONFIG_SCHEMA_FIELDS:
        current_value = _normalize_schema_value(config.get(item["key"], None), item["type"])
        default_value = _normalize_schema_value(baseline_config.get(item["key"], None), item["type"])
        field = {
            **item,
            "group_label": CONFIG_SCHEMA_GROUPS.get(item["group"], item["group"]),
            "default": default_value,
            "current_value": current_value,
        }
        parameters.append(field)
    return {
        "config_path": str(getattr(config, "config_path", "config/baseline.yaml")),
        "groups": [
            {"id": group_id, "label": label}
            for group_id, label in CONFIG_SCHEMA_GROUPS.items()
            if any(item["group"] == group_id for item in CONFIG_SCHEMA_FIELDS)
        ],
        "parameters": parameters,
        "startup_defaults": _build_startup_defaults(baseline_config),
    }


def _build_startup_defaults(config: SimulationConfig):
    boundaries = AGENT_TIER_CONFIG.get("tier_boundaries", {})
    wealth = AGENT_TIER_CONFIG.get("init_params", {})

    def annual_to_monthly(value):
        return int(round(float(value) / 12.0))

    ordered_tiers = ["ultra_high", "high", "middle", "lower_middle", "low"]
    tier_defaults = []
    total_agents = int(config.get("simulation.agent_count", 50) or 50)
    dist = config.get("agent_tiers.distribution", {}) or {}
    assigned = 0
    for tier in ordered_tiers:
        if tier == "low":
            count = max(0, total_agents - assigned)
        else:
            ratio = float(dist.get(tier, 0))
            count = int(round(total_agents * ratio / 100.0))
            assigned += count
        lower = int(boundaries.get(tier, 0))
        if tier == "ultra_high":
            upper = max(lower, annual_to_monthly(lower * 3.6))
        else:
            next_idx = ordered_tiers.index(tier) - 1
            next_tier = ordered_tiers[next_idx] if next_idx >= 0 else None
            next_boundary = int(boundaries.get(next_tier, lower * 2)) if next_tier else lower * 3
            upper = max(lower, annual_to_monthly(next_boundary))
        income_min = annual_to_monthly(lower)
        income_max = annual_to_monthly(upper)
        prop_range = wealth.get(tier, {}).get("property_count", (0, 0))
        tier_defaults.append(
            {
                "tier": tier,
                "count": int(count),
                "income_min": int(income_min),
                "income_max": int(income_max),
                "property_min": int(prop_range[0]),
                "property_max": int(prop_range[1]),
            }
        )

    min_properties = sum(item["count"] * item["property_min"] for item in tier_defaults)
    max_properties = sum(item["count"] * item["property_max"] for item in tier_defaults)
    suggested_property_count = max(max_properties, int(round(max_properties * 1.2))) if max_properties else max(1, min_properties)

    zone_a = config.get_zone_price_range("A")
    zone_b = config.get_zone_price_range("B")
    return {
        "agent_count": total_agents,
        "property_count": int(config.get("user_property_count", suggested_property_count) or suggested_property_count),
        "months": int(config.get("simulation.months", 12) or 12),
        "seed": config.get("simulation.random_seed", 42) if config.get("simulation.random_seed", 42) is not None else 42,
        "base_year": int(config.get("simulation.base_year", 2026) or 2026),
        "income_adjustment_rate": float(config.get("simulation.agent.income_adjustment_rate", 1.0) or 1.0),
        "down_payment_ratio": float(config.get("mortgage.down_payment_ratio", 0.3) or 0.3),
        "max_dti_ratio": float(config.get("mortgage.max_dti_ratio", 0.5) or 0.5),
        "annual_interest_rate": float(config.get("mortgage.annual_interest_rate", 0.035) or 0.035),
        "enable_intervention_panel": bool(config.get("simulation.enable_intervention_panel", True)),
        "market_pulse_enabled": bool(config.get("market_pulse.enabled", False)),
        "market_pulse_seed_ratio": float(config.get("market_pulse.seed_existing_mortgage_ratio", 0.55) or 0.55),
        "effective_bid_floor_ratio": float(config.get("smart_agent.effective_bid_floor_ratio", 0.98) or 0.98),
        "precheck_liquidity_buffer_months": int(config.get("smart_agent.precheck_liquidity_buffer_months", 3) or 3),
        "precheck_include_tax_and_fee": bool(config.get("smart_agent.precheck_include_tax_and_fee", True)),
        "min_cash_observer_threshold": int(config.get("decision_factors.activation.min_cash_observer_no_property", 500000) or 500000),
        "zones": [
            {
                "zone": "A",
                "price_min": int(zone_a.get("min", 32000)),
                "price_max": int(zone_a.get("max", 40000)),
                "rent_per_sqm": float(config.get("market.rental.zone_a_rent_per_sqm", 100) or 100),
            },
            {
                "zone": "B",
                "price_min": int(zone_b.get("min", 10000)),
                "price_max": int(zone_b.get("max", 20000)),
                "rent_per_sqm": float(config.get("market.rental.zone_b_rent_per_sqm", 60) or 60),
            },
        ],
        "agent_tiers": tier_defaults,
        "release_startup": _build_release_startup_defaults(),
    }


def _build_release_startup_defaults():
    options = load_release_supply_snapshot_options()
    by_id = {item["snapshot_id"]: item for item in options}
    recommended_snapshot_id = "spindle_medium" if "spindle_medium" in by_id else (options[0]["snapshot_id"] if options else "")
    return {
        "enabled": bool(options),
        "recommended_snapshot_id": recommended_snapshot_id,
        "default_market_goal": "balanced",
        "demand_multiplier_range": {"min": 0.10, "max": 2.00},
        "default_demand_multiplier_by_goal": {
            "balanced": 1.00,
            "buyer_market": 0.80,
            "seller_market": 1.30,
        },
        "default_preplanned_interventions": [
            {"action_type": "income_shock", "month": 2, "pct_change": -0.10, "target_tier": "all"},
            {"action_type": "developer_supply", "month": 2, "zone": "A", "count": 3, "template": "mixed_balanced"},
            {"action_type": "supply_cut", "month": 3, "zone": "A", "count": 2},
        ],
        "supply_snapshots": [
            {
                "snapshot_id": str(item.get("snapshot_id", "")),
                "display_name": str(item.get("display_name", "")),
                "family_label": str(item.get("family_label", "")),
                "structure_family": str(item.get("structure_family", "")),
                "recommended_use": str(item.get("recommended_use", "")),
                "total_selected_supply": int(item.get("total_selected_supply", 0) or 0),
                "snapshot_status": str(item.get("snapshot_status", "")),
                "startup_characteristics": str(item.get("startup_characteristics", "")),
                "speed_tradeoff": str(item.get("speed_tradeoff", "")),
                "accuracy_tradeoff": str(item.get("accuracy_tradeoff", "")),
                "minimum_demand_multiplier": float(item.get("minimum_demand_multiplier", 0.0) or 0.0),
                "demand_bucket_count": int(item.get("demand_bucket_count", 0) or 0),
                "supply_bucket_count": int(
                    ((((item.get("snapshot_payload") or {}).get("governance_snapshot") or {}).get("supply_library") or {}).get("bucket_count", 0)
                    or 0
                )),
            }
            for item in options
        ],
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/")
def index():
    return FileResponse("web/index.html")


async def _broadcast_run_started(status: Dict[str, Any]):
    await ws_manager.broadcast(
        _event_envelope(
            "RUN_STARTED",
            {"status": status},
            month=int(status.get("current_month", 0)),
            phase="month_start",
        )
    )
    if runtime.runner is not None:
        generation_events = await run_in_threadpool(runtime.runner.get_generation_events)
        for event in generation_events:
            await ws_manager.broadcast(event)


def _progress_event_payload(progress: Dict[str, Any]):
    status = progress.get("status", {}) or {}
    month = int(progress.get("month", status.get("current_month", 0) or 0) or 0)
    phase = str(progress.get("detail", {}).get("phase") or progress.get("stage") or "system")
    return _event_envelope(
        "RUN_PROGRESS",
        {
            "stage": progress.get("stage"),
            "message": progress.get("message"),
            "detail": progress.get("detail", {}) or {},
            "status": status,
        },
        month=month,
        phase=phase,
    )


def _make_progress_callback(loop: asyncio.AbstractEventLoop):
    def _callback(progress: Dict[str, Any]):
        asyncio.run_coroutine_threadsafe(
            ws_manager.broadcast(_progress_event_payload(progress)),
            loop,
        )

    return _callback


async def _broadcast_step_result(result: Dict[str, Any]):
    month = int(result["month_result"]["month"])
    month_events = []
    bulletin_event = None
    if runtime.runner is not None:
        bulletin_event = await run_in_threadpool(runtime.runner.get_bulletin_event, month)
        month_events = await run_in_threadpool(runtime.runner.get_month_events, month)

    if bulletin_event is not None:
        await ws_manager.broadcast(bulletin_event)

    for event in month_events:
        await ws_manager.broadcast(event)

    await ws_manager.broadcast(
        _event_envelope(
            "MONTH_END",
            {
                "month_result": result["month_result"],
                "status": result["status"],
            },
            month=month,
            phase="month_end",
        )
    )
    if result["status"].get("status") == "completed":
        await ws_manager.broadcast(
            _event_envelope(
                "RUN_FINISHED",
                {
                    "status": result["status"],
                    "month_result": result["month_result"],
                    "final_summary": result["status"].get("final_summary"),
                },
                month=month,
                phase="month_end",
            )
        )


async def _night_run_loop():
    loop = asyncio.get_running_loop()
    try:
        while True:
            runtime.set_progress_callback(_make_progress_callback(loop))
            try:
                result = await run_in_threadpool(runtime.step_auto)
            except HTTPException as exc:
                if "completed" not in str(exc.detail).lower():
                    await ws_manager.broadcast(_failure_event_payload(exc.detail))
                break
            except Exception as exc:
                await ws_manager.broadcast(_failure_event_payload(str(exc)))
                break
            finally:
                runtime.set_progress_callback(None)
            await _broadcast_step_result(result)
            if result["status"].get("status") == "completed":
                break
    finally:
        runtime.set_progress_callback(None)
        runtime.night_run_thread = None


def _start_night_run_background():
    def _runner():
        asyncio.run(_night_run_loop())

    thread = threading.Thread(target=_runner, daemon=True, name="night-run-loop")
    runtime.night_run_thread = thread
    thread.start()


@app.post("/start")
async def start_simulation(req: StartSimulationRequest):
    try:
        status = await run_in_threadpool(runtime.start, req)
    except HTTPException as exc:
        await ws_manager.broadcast(_failure_event_payload(exc.detail))
        raise
    except Exception as exc:
        await ws_manager.broadcast(_failure_event_payload(str(exc)))
        raise
    await _broadcast_run_started(status)
    return status


@app.post("/night-run/start")
async def start_night_run(req: StartSimulationRequest):
    req.night_run = True
    if not req.night_plan_path:
        req.night_plan_path = "config/night_run_example.yaml"
    try:
        status = await run_in_threadpool(runtime.start, req)
    except HTTPException as exc:
        await ws_manager.broadcast(_failure_event_payload(exc.detail))
        raise
    except Exception as exc:
        await ws_manager.broadcast(_failure_event_payload(str(exc)))
        raise
    await _broadcast_run_started(status)
    _start_night_run_background()
    return status


@app.post("/step")
async def step_simulation():
    loop = asyncio.get_running_loop()
    runtime.set_progress_callback(_make_progress_callback(loop))
    try:
        result = await run_in_threadpool(runtime.step)
    except HTTPException as exc:
        await ws_manager.broadcast(_failure_event_payload(exc.detail))
        raise
    except Exception as exc:
        await ws_manager.broadcast(_failure_event_payload(str(exc)))
        raise
    finally:
        runtime.set_progress_callback(None)
    await _broadcast_step_result(result)
    return result


@app.get("/status")
def get_status():
    return runtime.status()


@app.get("/runs")
def list_runs():
    results_root = Path("results")
    runs: List[Dict[str, object]] = []
    seen_db_paths = set()
    if not results_root.exists():
        results_root.mkdir(parents=True, exist_ok=True)

    for db_file in sorted(results_root.glob("run_*/simulation.db"), reverse=True):
        run_dir = db_file.parent
        seen_db_paths.add(str(db_file.resolve()))
        metadata_path = run_dir / "metadata.json"
        metadata: Dict[str, object] = {}
        if metadata_path.exists():
            try:
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            except Exception:
                metadata = {}
        status = "unknown"
        current_month = 0
        completed_months = 0
        transactions_total = 0
        can_resume = False
        try:
            conn = sqlite3.connect(str(db_file))
            cur = conn.cursor()
            completed_months = _q1(cur, "SELECT COALESCE(MAX(month), 0) FROM market_bulletin")
            current_month = completed_months
            transactions_total = _q1(cur, "SELECT COUNT(*) FROM transactions")
            target_months = int(metadata.get("months") or 0)
            if completed_months <= 0:
                status = "initialized"
            elif target_months > 0 and completed_months >= target_months:
                status = "completed"
            else:
                status = "paused"
            can_resume = status in {"initialized", "paused"}
        except Exception:
            status = "unknown"
        finally:
            try:
                conn.close()
            except Exception:
                pass
        runs.append(
            {
                "run_id": run_dir.name,
                "run_dir": str(run_dir).replace("\\", "/"),
                "db_path": str(db_file).replace("\\", "/"),
                "created_at": metadata.get("created_at"),
                "agent_count": metadata.get("agent_count"),
                "months": metadata.get("months"),
                "seed": metadata.get("seed"),
                "status": status,
                "current_month": current_month,
                "completed_months": completed_months,
                "transactions_total": transactions_total,
                "can_resume": can_resume,
            }
        )

    current_runner = runtime.runner
    current_db = getattr(current_runner, "db_path", None)
    if current_runner is not None and current_db:
        current_db_path = Path(str(current_db))
        resolved_current = str(current_db_path.resolve()) if current_db_path.exists() else str(current_db_path)
        if resolved_current not in seen_db_paths:
            snapshot = current_runner.get_status()
            transactions_total = 0
            try:
                cur = current_runner.conn.cursor()
                transactions_total = _q1(cur, "SELECT COUNT(*) FROM transactions")
            except Exception:
                transactions_total = 0
            runs.insert(
                0,
                {
                    "run_id": current_db_path.parent.name,
                    "run_dir": str(current_db_path.parent).replace("\\", "/"),
                    "db_path": str(current_db_path).replace("\\", "/"),
                    "created_at": getattr(current_runner, "started_at", None),
                    "agent_count": int(getattr(current_runner, "agent_count", 0) or 0),
                    "months": int(getattr(current_runner, "months", 0) or 0),
                    "seed": getattr(current_runner, "seed", None),
                    "status": snapshot.get("status", "unknown"),
                    "current_month": int(snapshot.get("current_month", 0) or 0),
                    "completed_months": int(snapshot.get("current_month", 0) or 0),
                    "transactions_total": int(transactions_total or 0),
                    "can_resume": bool(snapshot.get("status") in {"initialized", "paused"}),
                },
            )
    return {"runs": runs}


@app.get("/db-observer")
def get_db_observer(db_path: Optional[str] = None):
    target_db = _find_observer_db_path(db_path)
    return _collect_db_observer_snapshot(target_db)


@app.get("/db-observer/view", response_class=HTMLResponse)
def view_db_observer(db_path: Optional[str] = None):
    target_db = _find_observer_db_path(db_path)
    data = _collect_db_observer_snapshot(target_db)
    run = data.get("run", {}) or {}
    metadata = data.get("metadata", {}) or {}
    counts = data.get("table_counts", {}) or {}
    latest_records = data.get("latest_records", {}) or {}
    latest_bulletin = (latest_records.get("market_bulletin") or [{}])[0] or {}

    def _card(label: str, value: object) -> str:
        return (
            "<article class='card stat-card'>"
            f"<div class='muted'>{html.escape(label)}</div>"
            f"<strong>{html.escape(str(value))}</strong>"
            "</article>"
        )

    def _render_rows(rows: List[Dict[str, object]]) -> str:
        if not rows:
            return "<div class='muted'>暂无记录</div>"
        rendered = []
        for row in rows[:10]:
            rendered.append(
                "<pre class='record'>"
                f"{html.escape(json.dumps(row, ensure_ascii=False, indent=2, default=str))}"
                "</pre>"
            )
        return "".join(rendered)

    stats_html = "".join(
        [
            _card("当前回合（虚拟周期）", data.get("latest_month", 0)),
            _card("Decision Logs", counts.get("decision_logs", 0)),
            _card("Transactions", counts.get("transactions", 0)),
            _card("Orders", counts.get("transaction_orders", 0)),
            _card("Negotiations", counts.get("negotiations", 0)),
            _card("Active Participants", counts.get("active_participants", 0)),
        ]
    )
    sections_html = "".join(
        f"""
        <section class="card">
          <h2>{html.escape(title)}</h2>
          {_render_rows(latest_records.get(key) or [])}
        </section>
        """
        for key, title in [
            ("market_bulletin", "最近市场公报"),
            ("transactions", "最近成交记录"),
            ("transaction_orders", "最近订单记录"),
            ("negotiations", "最近谈判记录"),
            ("active_participants", "最近活跃参与者"),
            ("decision_logs", "最近决策日志"),
        ]
    )
    return f"""
    <!doctype html>
    <html lang="zh-CN">
    <head>
      <meta charset="utf-8">
      <title>Research DB Observer</title>
      <style>
        body {{ font-family: 'Segoe UI','PingFang SC',sans-serif; margin: 0; background: #0b1210; color: #eff5ef; }}
        main {{ max-width: 1240px; margin: 0 auto; padding: 30px 24px 80px; }}
        h1, h2 {{ margin: 0 0 12px; }}
        p {{ line-height: 1.6; }}
        .muted {{ color: #9fb3a5; }}
        .grid {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; }}
        .stats {{ grid-template-columns: repeat(6, minmax(0, 1fr)); margin: 20px 0 24px; }}
        .card {{ border: 1px solid rgba(152,196,169,0.16); border-radius: 18px; background: rgba(255,255,255,0.03); padding: 16px; box-sizing: border-box; }}
        .stat-card strong {{ display: block; margin-top: 8px; font-size: 22px; }}
        .banner {{ margin-bottom: 18px; padding: 14px 16px; border-radius: 16px; background: rgba(91,143,255,0.10); color: #d5e5ff; }}
        .record {{ margin: 0 0 10px; white-space: pre-wrap; word-break: break-word; border-radius: 14px; padding: 12px; background: rgba(0,0,0,0.24); border: 1px solid rgba(255,255,255,0.06); }}
        .meta-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px; margin-bottom: 18px; }}
        @media (max-width: 1100px) {{
          .stats {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }}
          .grid, .meta-grid {{ grid-template-columns: 1fr; }}
        }}
      </style>
    </head>
    <body>
      <main>
        <h1>Research DB Observer</h1>
        <p class="muted">数据库观测页（最小版）。所有数字和记录都直接来自 <code>simulation.db</code>，不是前端缓存。</p>
        <div class="banner">
          <strong>当前数据库</strong><br>
          Run: {html.escape(str(run.get("run_id") or "-"))} · DB: {html.escape(str(run.get("db_path") or "-"))}
        </div>
        <section class="card" style="margin-bottom:18px;">
          <p>{html.escape(_external_db_notice(run.get("db_path") or "-"))}</p>
          <p class="muted">{html.escape(_external_round_notice())}</p>
        </section>
        <section class="meta-grid">
          <article class="card">
            <h2>运行元信息</h2>
            <pre class="record">{html.escape(json.dumps(metadata, ensure_ascii=False, indent=2, default=str))}</pre>
          </article>
          <article class="card">
            <h2>最新公报口径</h2>
            <pre class="record">{html.escape(json.dumps(latest_bulletin, ensure_ascii=False, indent=2, default=str))}</pre>
          </article>
        </section>
        <section class="grid stats">{stats_html}</section>
        <section class="grid">{sections_html}</section>
      </main>
    </body>
    </html>
    """


def _find_observer_db_path(candidate: Optional[str] = None) -> Path:
    if candidate:
        db_path = Path(candidate)
        if not db_path.exists():
            raise HTTPException(status_code=404, detail=f"DB not found: {candidate}")
        return db_path
    if runtime.runner is not None and runtime.runner.db_path:
        db_path = Path(str(runtime.runner.db_path))
        if db_path.exists():
            return db_path
    results_root = Path("results")
    for db_file in sorted(results_root.glob("run_*/simulation.db"), reverse=True):
        return db_file
    raise HTTPException(status_code=404, detail="No simulation DB found.")


def _find_forensic_db_path(candidate: Optional[str] = None) -> Path:
    try:
        return _find_observer_db_path(candidate)
    except HTTPException as exc:
        raise HTTPException(status_code=404, detail="No runnable simulation DB found.") from exc


def _q1(cur, sql: str, params: tuple = ()) -> int:
    row = cur.execute(sql, params).fetchone()
    return int((row[0] if row else 0) or 0)


def _safe_fetch_rows(cur, sql: str, params: tuple = ()) -> List[Dict[str, object]]:
    try:
        rows = cur.execute(sql, params).fetchall()
    except sqlite3.OperationalError:
        return []
    return [dict(row) for row in rows]


def _safe_q1(cur, sql: str, params: tuple = ()) -> int:
    try:
        return _q1(cur, sql, params)
    except sqlite3.OperationalError:
        return 0


def _external_round_notice() -> str:
    return "本项目对外展示中的“回合”是虚拟市场周期；若个别输出仍出现“月份/month”，也应按回合机制理解。"


def _external_db_notice(db_path: Optional[object]) -> str:
    return f"当前数据库位置：{str(db_path or '-')}"


def _read_run_metadata(run_dir: Path) -> Dict[str, object]:
    metadata_path = run_dir / "metadata.json"
    if not metadata_path.exists():
        return {}
    try:
        return json.loads(metadata_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _collect_db_observer_snapshot(db_path: Path) -> Dict[str, object]:
    run_dir = db_path.parent
    metadata = _read_run_metadata(run_dir)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        latest_month = _safe_q1(cur, "SELECT COALESCE(MAX(month), 0) FROM market_bulletin")
        table_counts = {
            "agents": _safe_q1(cur, "SELECT COUNT(*) FROM agents"),
            "properties_market": _safe_q1(cur, "SELECT COUNT(*) FROM properties_market"),
            "active_participants": _safe_q1(cur, "SELECT COUNT(*) FROM active_participants"),
            "decision_logs": _safe_q1(cur, "SELECT COUNT(*) FROM decision_logs"),
            "negotiations": _safe_q1(cur, "SELECT COUNT(*) FROM negotiations"),
            "transaction_orders": _safe_q1(cur, "SELECT COUNT(*) FROM transaction_orders"),
            "transactions": _safe_q1(cur, "SELECT COUNT(*) FROM transactions"),
            "market_bulletin": _safe_q1(cur, "SELECT COUNT(*) FROM market_bulletin"),
        }
        latest_records = {
            "decision_logs": _safe_fetch_rows(
                cur,
                """
                SELECT id, month, agent_id, action, llm_called, thought_process
                FROM decision_logs
                ORDER BY id DESC
                LIMIT 20
                """,
            ),
            "transactions": _safe_fetch_rows(
                cur,
                """
                SELECT id, month, buyer_id, seller_id, property_id, final_price
                FROM transactions
                ORDER BY id DESC
                LIMIT 20
                """,
            ),
            "transaction_orders": _safe_fetch_rows(
                cur,
                """
                SELECT order_id, month, buyer_id, seller_id, property_id, status, agreed_price, close_month
                FROM transaction_orders
                ORDER BY order_id DESC
                LIMIT 20
                """,
            ),
            "negotiations": _safe_fetch_rows(
                cur,
                """
                SELECT id, month, buyer_id, seller_id, property_id, success, final_price, rounds
                FROM negotiations
                ORDER BY id DESC
                LIMIT 20
                """,
            ),
            "active_participants": _safe_fetch_rows(
                cur,
                """
                SELECT id, month, agent_id, role, status
                FROM active_participants
                ORDER BY id DESC
                LIMIT 20
                """,
            ),
            "market_bulletin": _safe_fetch_rows(
                cur,
                """
                SELECT month, transaction_volume, avg_price, avg_unit_price, active_listings
                FROM market_bulletin
                ORDER BY month DESC
                LIMIT 12
                """,
            ),
        }
        return {
            "run": {
                "run_id": run_dir.name,
                "run_dir": str(run_dir).replace("\\", "/"),
                "db_path": str(db_path).replace("\\", "/"),
            },
            "metadata": metadata,
            "latest_month": latest_month,
            "table_counts": table_counts,
            "latest_records": latest_records,
            "generated_at": datetime.datetime.now().isoformat(),
            "data_source": "sqlite_db",
        }
    finally:
        conn.close()


def _collect_zero_tx_diagnostics(db_path: Path) -> Dict[str, object]:
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    try:
        max_month = _q1(cur, "SELECT COALESCE(MAX(month),0) FROM market_bulletin")
        tx = _q1(cur, "SELECT COUNT(*) FROM transactions")
        active_buyers = _q1(cur, "SELECT COUNT(*) FROM active_participants WHERE role IN ('BUYER','BUYER_SELLER')")
        for_sale = _q1(cur, "SELECT COUNT(*) FROM properties_market WHERE status='for_sale'")
        pending_orders = _q1(cur, "SELECT COUNT(*) FROM transaction_orders WHERE status='pending'")
        pending_settle = _q1(cur, "SELECT COUNT(*) FROM transaction_orders WHERE status='pending_settlement'")
        filled_orders = _q1(cur, "SELECT COUNT(*) FROM transaction_orders WHERE status='filled'")
        canceled_orders = _q1(cur, "SELECT COUNT(*) FROM transaction_orders WHERE status='cancelled'")
        precheck_reject = _q1(cur, "SELECT COUNT(*) FROM decision_logs WHERE event_type='ORDER_PRECHECK' AND decision='REJECT'")
        invalid_bid = _q1(cur, "SELECT COUNT(*) FROM decision_logs WHERE event_type='BID_VALIDATION' AND decision='INVALID_BID'")
        no_valid_bids = _q1(cur, "SELECT COUNT(*) FROM negotiations WHERE success=0 AND reason LIKE '%No valid bids%'")
        affordability_fail = _q1(cur, "SELECT COUNT(*) FROM transaction_orders WHERE close_reason LIKE 'Settlement failed:%'")
        reasons_rows = cur.execute(
            """
            SELECT reason, COUNT(*)
            FROM decision_logs
            WHERE event_type='ORDER_PRECHECK' AND decision='REJECT'
            GROUP BY reason
            ORDER BY COUNT(*) DESC
            """
        ).fetchall()
        monthly_rows = cur.execute(
            "SELECT month, transaction_volume, precheck_reject_count, invalid_bid_count FROM market_bulletin ORDER BY month"
        ).fetchall()
    finally:
        conn.close()

    return {
        "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "max_month": max_month,
        "transactions_total": tx,
        "active_buyers": active_buyers,
        "for_sale_listings": for_sale,
        "pending_orders": pending_orders,
        "pending_settlement_orders": pending_settle,
        "filled_orders": filled_orders,
        "cancelled_orders": canceled_orders,
        "precheck_reject_total": precheck_reject,
        "invalid_bid_total": invalid_bid,
        "negotiation_no_valid_bids": no_valid_bids,
        "settlement_affordability_fails": affordability_fail,
        "precheck_reasons": [{"reason": str(reason or ""), "count": int(count or 0)} for reason, count in reasons_rows],
        "monthly_metrics": [
            {
                "month": int(month),
                "tx": int(tx_count or 0),
                "precheck_reject_count": int(precheck_count or 0),
                "invalid_bid_count": int(invalid_count or 0),
            }
            for month, tx_count, precheck_count, invalid_count in monthly_rows
        ],
    }


def _write_zero_tx_diagnostics(run_dir: Path, data: Dict[str, object]) -> Dict[str, str]:
    md_path = run_dir / "zero_tx_diagnostics.md"
    json_path = run_dir / "zero_tx_diagnostics.json"
    json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    md_lines = [
        "# 0成交诊断报告",
        "",
        f"- {_external_db_notice(run_dir / 'simulation.db')}",
        f"- {_external_round_notice()}",
        f"- generated_at: `{data['generated_at']}`",
        f"- max_month: `{data['max_month']}`",
        f"- transactions_total: `{data['transactions_total']}`",
        "",
        "## 核心链路计数",
        f"- active_buyers: `{data['active_buyers']}`",
        f"- for_sale_listings: `{data['for_sale_listings']}`",
        f"- pending_orders: `{data['pending_orders']}`",
        f"- pending_settlement_orders: `{data['pending_settlement_orders']}`",
        f"- filled_orders: `{data['filled_orders']}`",
        f"- cancelled_orders: `{data['cancelled_orders']}`",
        "",
        "## 阻断指标",
        f"- precheck_reject_total: `{data['precheck_reject_total']}`",
        f"- invalid_bid_total: `{data['invalid_bid_total']}`",
        f"- negotiation_no_valid_bids: `{data['negotiation_no_valid_bids']}`",
        f"- settlement_affordability_fails: `{data['settlement_affordability_fails']}`",
    ]
    md_path.write_text("\n".join(md_lines), encoding="utf-8")
    return {
        "markdown_path": str(md_path).replace("\\", "/"),
        "json_path": str(json_path).replace("\\", "/"),
    }


@app.post("/forensics/zero-tx")
def run_zero_tx_forensics(req: ForensicRequest):
    db_path = _find_forensic_db_path(req.db_path)
    run_dir = db_path.parent
    data = _collect_zero_tx_diagnostics(db_path)
    artifacts = _write_zero_tx_diagnostics(run_dir, data)
    metadata: Dict[str, object] = {}
    metadata_path = run_dir / "metadata.json"
    if metadata_path.exists():
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except Exception:
            metadata = {}
    return {
        "run_id": run_dir.name,
        "run_dir": str(run_dir).replace("\\", "/"),
        "db_path": str(db_path).replace("\\", "/"),
        "artifacts": artifacts,
        "metadata": metadata,
        "report": data,
    }


@app.get("/forensics/zero-tx/download")
def download_zero_tx_forensics(db_path: Optional[str] = None, format: str = "json"):
    target_db = _find_forensic_db_path(db_path)
    run_dir = target_db.parent
    data = _collect_zero_tx_diagnostics(target_db)
    artifacts = _write_zero_tx_diagnostics(run_dir, data)
    normalized = str(format or "json").strip().lower()
    if normalized == "md":
        return FileResponse(artifacts["markdown_path"], media_type="text/markdown", filename="zero_tx_diagnostics.md")
    return FileResponse(artifacts["json_path"], media_type="application/json", filename="zero_tx_diagnostics.json")


@app.get("/forensics/zero-tx/view", response_class=HTMLResponse)
def view_zero_tx_forensics(db_path: Optional[str] = None):
    target_db = _find_forensic_db_path(db_path)
    run_dir = target_db.parent
    data = _collect_zero_tx_diagnostics(target_db)
    _write_zero_tx_diagnostics(run_dir, data)
    reasons_html = "".join(
        f"<li>{html.escape(str(item.get('reason') or '-'))} <strong>{int(item.get('count') or 0)}</strong></li>"
        for item in data.get("precheck_reasons", [])[:6]
    ) or "<li>暂无明显阻断原因</li>"
    monthly_html = "".join(
        f"<tr><td>R{int(item.get('month') or 0)}</td><td>{int(item.get('tx') or 0)}</td><td>{int(item.get('precheck_reject_count') or 0)}</td><td>{int(item.get('invalid_bid_count') or 0)}</td></tr>"
        for item in data.get("monthly_metrics", [])
    ) or "<tr><td colspan='4'>暂无回合指标</td></tr>"
    return f"""
    <!doctype html>
    <html lang="zh-CN">
    <head>
      <meta charset="utf-8">
      <title>0成交诊断报告</title>
      <style>
        body {{ font-family: 'Segoe UI','PingFang SC',sans-serif; margin: 0; background: #0c1411; color: #eef6f0; }}
        main {{ max-width: 1100px; margin: 0 auto; padding: 32px 24px 60px; }}
        h1, h2 {{ margin: 0 0 14px; }}
        .grid {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 14px; margin: 18px 0 24px; }}
        .card {{ border: 1px solid rgba(152,196,169,0.16); border-radius: 18px; background: rgba(255,255,255,0.03); padding: 16px; }}
        .muted {{ color: #9db2a6; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 10px 12px; border-bottom: 1px solid rgba(255,255,255,0.08); text-align: left; }}
        ul {{ margin: 0; padding-left: 18px; }}
      </style>
    </head>
    <body>
      <main>
        <h1>0成交诊断报告</h1>
        <p class="muted">Run: {html.escape(run_dir.name)} · generated_at: {html.escape(str(data.get("generated_at") or "-"))}</p>
        <section class="card" style="margin-bottom:16px;">
          <p>{html.escape(_external_db_notice(target_db))}</p>
          <p class="muted">{html.escape(_external_round_notice())}</p>
        </section>
        <section class="grid">
          <article class="card"><div class="muted">总成交</div><strong>{int(data.get("transactions_total") or 0)}</strong></article>
          <article class="card"><div class="muted">活跃买家</div><strong>{int(data.get("active_buyers") or 0)}</strong></article>
          <article class="card"><div class="muted">挂牌数</div><strong>{int(data.get("for_sale_listings") or 0)}</strong></article>
          <article class="card"><div class="muted">预检拒绝</div><strong>{int(data.get("precheck_reject_total") or 0)}</strong></article>
        </section>
        <section class="card">
          <h2>阻断原因</h2>
          <ul>{reasons_html}</ul>
        </section>
        <section class="card" style="margin-top:16px;">
          <h2>回合指标（虚拟周期）</h2>
          <table>
            <thead><tr><th>回合</th><th>成交</th><th>预检拒绝</th><th>无效出价</th></tr></thead>
            <tbody>{monthly_html}</tbody>
          </table>
        </section>
      </main>
    </body>
    </html>
    """


@app.get("/report/final")
def get_final_report():
    if runtime.runner is None:
        raise HTTPException(status_code=409, detail="No simulation has been started.")
    return runtime.runner.get_export_report()


@app.get("/report/parameter-assumption")
def get_parameter_assumption_report():
    if runtime.runner is None:
        raise HTTPException(status_code=409, detail="No simulation has been started.")
    return runtime.runner.build_parameter_assumption_report()


@app.get("/report/parameter-assumption/download")
def download_parameter_assumption_report(format: str = "json"):
    if runtime.runner is None:
        raise HTTPException(status_code=409, detail="No simulation has been started.")
    artifacts = runtime.runner.write_parameter_assumption_report()
    normalized = str(format or "json").strip().lower()
    if normalized == "md":
        return FileResponse(
            artifacts["markdown_path"],
            media_type="text/markdown",
            filename="parameter_assumption_report.md",
        )
    return FileResponse(
        artifacts["json_path"],
        media_type="application/json",
        filename="parameter_assumption_report.json",
    )


@app.get("/report/parameter-assumption/view", response_class=HTMLResponse)
def view_parameter_assumption_report():
    if runtime.runner is None:
        raise HTTPException(status_code=409, detail="No simulation has been started.")
    report = runtime.runner.build_parameter_assumption_report()
    experiment = report.get("experiment_info", {}) or {}
    parameter_rows = report.get("parameter_rows", []) or []
    income_tiers = ((report.get("role_structure", {}) or {}).get("income_tiers") or [])[:8]
    latest_results = report.get("latest_results", {}) or {}

    rows_html = "".join(
        f"""
        <tr>
          <td><code>{html.escape(str(row.get('parameter_key') or '-'))}</code></td>
          <td>{html.escape(str(row.get('label') or '-'))}</td>
          <td>{html.escape(str(row.get('current_value') or '-'))}</td>
          <td>{html.escape(str(row.get('parameter_category') or '-'))}</td>
          <td>{html.escape(str(row.get('source_category') or '-'))}</td>
          <td>{html.escape(str(row.get('why_set') or '-'))}</td>
          <td>{'是' if row.get('is_key') else '否'}</td>
          <td>{html.escape(str(row.get('confidence') or '-'))}</td>
        </tr>
        """
        for row in parameter_rows
    ) or "<tr><td colspan='8'>暂无参数说明</td></tr>"

    tier_html = "".join(
        f"""
        <tr>
          <td>{html.escape(str(row.get('tier') or '-'))}</td>
          <td>{int(row.get('count') or 0)}</td>
          <td>{html.escape(str(row.get('income_min') or '-'))}</td>
          <td>{html.escape(str(row.get('income_max') or '-'))}</td>
          <td>{html.escape(str(row.get('property_min') or '-'))}</td>
          <td>{html.escape(str(row.get('property_max') or '-'))}</td>
        </tr>
        """
        for row in income_tiers
    ) or "<tr><td colspan='6'>暂无收入结构</td></tr>"

    return f"""
    <!doctype html>
    <html lang="zh-CN">
    <head>
      <meta charset="utf-8">
      <title>参数与假设说明表</title>
      <style>
        body {{ font-family: 'Segoe UI','PingFang SC',sans-serif; margin: 0; background: #0b1210; color: #eef6f0; }}
        main {{ max-width: 1320px; margin: 0 auto; padding: 30px 24px 80px; }}
        h1, h2 {{ margin: 0 0 14px; }}
        .muted {{ color: #9db2a6; }}
        .card {{ border: 1px solid rgba(152,196,169,0.16); border-radius: 18px; background: rgba(255,255,255,0.03); padding: 16px; margin-bottom: 16px; }}
        .grid {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 14px; margin-bottom: 18px; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 10px 12px; border-bottom: 1px solid rgba(255,255,255,0.08); text-align: left; vertical-align: top; }}
        code {{ color: #cfe3ff; }}
        pre {{ white-space: pre-wrap; word-break: break-word; }}
        @media (max-width: 1100px) {{ .grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }} }}
      </style>
    </head>
    <body>
      <main>
        <h1>参数与假设说明表</h1>
        <p class="muted">这份说明表由系统根据当前 run 自动生成，用于记录启动参数、关键假设和本轮结果摘要。</p>
        <section class="card">
          <p>{html.escape(_external_db_notice(experiment.get('db_path') or '-'))}</p>
          <p class="muted">{html.escape(_external_round_notice())}</p>
        </section>
        <section class="grid">
          <article class="card"><div class="muted">Run ID</div><strong>{html.escape(str(experiment.get('run_id') or '-'))}</strong></article>
          <article class="card"><div class="muted">Agent 数量</div><strong>{html.escape(str(experiment.get('agent_count') or '-'))}</strong></article>
          <article class="card"><div class="muted">模拟回合数（虚拟周期）</div><strong>{html.escape(str(experiment.get('months') or '-'))}</strong></article>
          <article class="card"><div class="muted">随机种子</div><strong>{html.escape(str(experiment.get('seed') or '-'))}</strong></article>
        </section>
        <section class="card">
          <h2>参数总表</h2>
          <table>
            <thead><tr><th>参数键</th><th>中文名称</th><th>当前值</th><th>参数类别</th><th>来源类别</th><th>为什么这样设</th><th>是否关键</th><th>当前信心</th></tr></thead>
            <tbody>{rows_html}</tbody>
          </table>
        </section>
        <section class="card">
          <h2>收入结构</h2>
          <table>
            <thead><tr><th>收入档</th><th>人数</th><th>收入下限</th><th>收入上限</th><th>拥房最小值</th><th>拥房最大值</th></tr></thead>
            <tbody>{tier_html}</tbody>
          </table>
        </section>
        <section class="card">
          <h2>本轮结果摘要</h2>
          <pre>{html.escape(json.dumps(latest_results, ensure_ascii=False, indent=2, default=str))}</pre>
        </section>
      </main>
    </body>
    </html>
    """


@app.get("/report/final/view", response_class=HTMLResponse)
def view_final_report():
    if runtime.runner is None:
        raise HTTPException(status_code=409, detail="No simulation has been started.")

    report = runtime.runner.get_export_report()
    run = report.get("run", {})
    public_notes = report.get("public_notes", {}) or {}
    runtime_controls = report.get("runtime_controls", {})
    last_month_summary = report.get("last_month_summary", {}) or {}
    final_summary = report.get("final_summary", {})
    month_reviews = report.get("month_reviews", [])
    latest_bulletin = html.escape(str(last_month_summary.get("bulletin_excerpt") or "No market bulletin available."))
    top_agents = final_summary.get("top_agents", []) or []
    key_properties = final_summary.get("key_properties", []) or []
    failure_reasons = final_summary.get("failure_reasons", []) or []
    interventions = final_summary.get("interventions", []) or []
    preset_entries = [entry for entry in interventions if entry.get("event_type") == "SCENARIO_PRESET_APPLIED"]
    control_entries = [
        entry
        for entry in interventions
        if entry.get("event_type") in {"CONTROLS_UPDATED", "SCENARIO_PRESET_APPLIED"}
    ]

    top_agent_cards = "".join(
        f"""
        <article class="mini-card">
          <strong>{html.escape(str(item.get('name') or f"Agent {item.get('agent_id', '-')}"))}</strong>
          <span class="mini-pill">{html.escape(str(item.get('agent_type') or 'normal'))}</span>
          <p>Activations {int(item.get('activations', 0) or 0)} · Deals {int(item.get('deals', 0) or 0)} · Failures {int(item.get('failures', 0) or 0)}</p>
        </article>
        """
        for item in top_agents[:3]
    ) or '<article class="mini-card"><p>No top agent summary available.</p></article>'

    key_property_cards = "".join(
        f"""
        <article class="mini-card">
          <strong>Property {int(item.get('property_id', 0) or 0)}</strong>
          <span class="mini-pill">{html.escape(str(item.get('zone') or '?'))}</span>
          <p>{html.escape(str(item.get('property_type') or 'Property'))}</p>
          <p>Listings {int(item.get('listings', 0) or 0)} · Attempts {int(item.get('attempts', 0) or 0)} · Deals {int(item.get('deals', 0) or 0)}</p>
        </article>
        """
        for item in key_properties[:3]
    ) or '<article class="mini-card"><p>No key property summary available.</p></article>'

    failure_reason_cards = "".join(
        f"""
        <article class="mini-card">
          <strong>{html.escape(str(item.get('reason') or item.get('status') or 'Unknown'))}</strong>
          <span class="mini-pill">{html.escape(str(item.get('status') or 'n/a'))}</span>
          <p>Count {int(item.get('count', 0) or 0)}</p>
        </article>
        """
        for item in failure_reasons[:3]
    ) or '<article class="mini-card"><p>No failure reason summary available.</p></article>'

    chart_points = []
    for item in month_reviews:
        month = int(item.get("month", 0) or 0)
        tx = float(item.get("transactions", 0) or 0)
        avg_price = float(item.get("avg_transaction_price", 0) or 0)
        failures = float(item.get("failed_negotiations", 0) or 0)
        activations = float(len((item.get("month_review", {}) or {}).get("top_agents", [])) or 0)
        success_rate = (tx / max(tx + failures, 1.0)) * 100.0
        chart_points.append(
            {
                "month": month,
                "transactions": tx,
                "avg_price": avg_price,
                "failures": failures,
                "activations": activations,
                "success_rate": success_rate,
            }
        )

    def _polyline(values, width, height, top_pad, bottom_pad):
        if not values:
            return ""
        if len(values) == 1:
            x = width / 2
            y = height - bottom_pad
            return f"M{x:.1f} {y:.1f} L{x:.1f} {y:.1f}"
        max_value = max(values) or 1.0
        min_value = min(values)
        span = max(max_value - min_value, 1.0)
        usable_height = max(height - top_pad - bottom_pad, 1)
        step_x = width / max(len(values) - 1, 1)
        points = []
        for index, value in enumerate(values):
            x = step_x * index
            ratio = (value - min_value) / span
            y = height - bottom_pad - (usable_height * ratio)
            points.append(f"{x:.1f},{y:.1f}")
        return "M " + " L ".join(points)

    def _bars(values, width, height, top_pad, bottom_pad):
        if not values:
            return ""
        max_value = max(values) or 1.0
        usable_height = max(height - top_pad - bottom_pad, 1)
        gap = 10
        bar_width = max((width - gap * max(len(values) - 1, 0)) / max(len(values), 1), 14)
        bars = []
        for index, value in enumerate(values):
            x = index * (bar_width + gap)
            bar_height = (float(value) / max_value) * usable_height if max_value else 0
            y = height - bottom_pad - bar_height
            bars.append(
                f"<rect class='bar' x='{x:.1f}' y='{y:.1f}' width='{bar_width:.1f}' height='{bar_height:.1f}' rx='7'></rect>"
            )
        return "".join(bars)

    chart_width = 720
    chart_height = 220
    chart_svg = ""
    if chart_points:
        tx_values = [item["transactions"] for item in chart_points]
        avg_values = [item["avg_price"] for item in chart_points]
        fail_values = [item["failures"] for item in chart_points]
        rate_values = [item["success_rate"] for item in chart_points]
        chart_svg = f"""
        <svg class="report-chart" viewBox="0 0 {chart_width} {chart_height}" role="img" aria-label="Round market chart">
          <line class="grid-line" x1="0" y1="188" x2="{chart_width}" y2="188"></line>
          <line class="grid-line" x1="0" y1="132" x2="{chart_width}" y2="132"></line>
          <line class="grid-line" x1="0" y1="76" x2="{chart_width}" y2="76"></line>
          <g class="bar-layer">{_bars(tx_values, chart_width, chart_height, 18, 32)}</g>
          <path class="line avg-line" d="{_polyline(avg_values, chart_width, chart_height, 18, 32)}"></path>
          <path class="line fail-line" d="{_polyline(fail_values, chart_width, chart_height, 18, 32)}"></path>
          <path class="line rate-line" d="{_polyline(rate_values, chart_width, chart_height, 18, 32)}"></path>
        </svg>
        """

    month_cards = "".join(
        f"""
        <article class="card month-card">
          <div class="month-head">
            <h3>Round {item.get('month', '-')}</h3>
            <span class="month-pill">{int(item.get('transactions', 0) or 0)} deals</span>
          </div>
          <p>Avg Transaction Price: ¥{float(item.get('avg_transaction_price', 0) or 0):,.0f}</p>
          <p>Failed Negotiations: {int(item.get('failed_negotiations', 0) or 0)}</p>
          <p>Top Agents: {len(item.get('month_review', {}).get('top_agents', []))}</p>
          <p>Key Properties: {len(item.get('month_review', {}).get('key_properties', []))}</p>
          <p>Failure Reasons: {len(item.get('month_review', {}).get('failure_reasons', []))}</p>
          <p>Interventions: {len(item.get('month_review', {}).get('interventions', []))}</p>
          <p class="muted">Bulletin: {html.escape(str(item.get('bulletin_excerpt') or 'No bulletin excerpt.'))}</p>
        </article>
        """
        for item in month_reviews
    ) or '<article class="card"><p>No round reviews available.</p></article>'
    preset_timeline = "".join(
        f"<li>Round {entry.get('month', 0)} · {html.escape(str(entry.get('payload', {}).get('preset', 'unknown')))}</li>"
        for entry in preset_entries
    ) or "<li>No scenario preset applied.</li>"
    preset_impact_cards = "".join(
        f"""
        <article class="mini-card">
          <strong>Round {int(entry.get('month', 0) or 0)} · {html.escape(str(entry.get('payload', {}).get('preset', 'unknown')))}</strong>
          <p>{html.escape(str(entry.get('message') or 'Scenario preset applied.'))}</p>
        </article>
        """
        for entry in preset_entries[-4:]
    ) or '<article class="mini-card"><p>No preset impact recorded.</p></article>'
    controls_timeline = "".join(
        f"""
        <article class="mini-card">
          <strong>Round {int(entry.get('month', 0) or 0)} · {html.escape(str(entry.get('event_type') or 'CONTROL_EVENT'))}</strong>
          <p>{html.escape(str(entry.get('message') or 'Control state updated.'))}</p>
        </article>
        """
        for entry in control_entries[-6:]
    ) or '<article class="mini-card"><p>No control changes recorded.</p></article>'

    return f"""
    <!doctype html>
    <html lang="zh-CN">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>Simulation Final Report</title>
      <style>
        body {{
          margin: 0;
          font-family: "Segoe UI", sans-serif;
          background: #111714;
          color: #eef6f0;
        }}
        .page {{
          max-width: 1080px;
          margin: 0 auto;
          padding: 32px 20px 48px;
          display: grid;
          gap: 18px;
        }}
        .hero, .card {{
          border-radius: 18px;
          border: 1px solid rgba(152, 196, 169, 0.18);
          background: rgba(255, 255, 255, 0.04);
          padding: 18px;
        }}
        .grid {{
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
          gap: 14px;
        }}
        .grid-wide {{
          display: grid;
          grid-template-columns: 1.2fr 0.8fr;
          gap: 14px;
        }}
        h1, h2, h3, p, ul {{ margin: 0; }}
        ul {{ padding-left: 18px; }}
        .muted {{ color: #b7c6bd; }}
        .hero h1 {{ margin-bottom: 8px; }}
        .metric {{
          font-size: 28px;
          font-weight: 700;
          margin-top: 8px;
        }}
        .bulletin {{
          line-height: 1.5;
        }}
        .report-chart {{
          width: 100%;
          height: auto;
          margin-top: 14px;
          overflow: visible;
        }}
        .grid-line {{
          stroke: rgba(255,255,255,0.1);
          stroke-width: 1;
        }}
        .bar {{
          fill: rgba(113, 214, 167, 0.34);
        }}
        .line {{
          fill: none;
          stroke-width: 3;
          stroke-linecap: round;
          stroke-linejoin: round;
        }}
        .avg-line {{ stroke: #f1c77c; }}
        .fail-line {{ stroke: #f08989; }}
        .rate-line {{ stroke: #70b4ff; }}
        .legend {{
          display: flex;
          flex-wrap: wrap;
          gap: 10px;
          margin-top: 12px;
          color: #c7d7ce;
          font-size: 13px;
        }}
        .legend span::before {{
          content: "";
          display: inline-block;
          width: 10px;
          height: 10px;
          border-radius: 999px;
          margin-right: 6px;
          vertical-align: middle;
        }}
        .legend .tx::before {{ background: rgba(113, 214, 167, 0.7); }}
        .legend .avg::before {{ background: #f1c77c; }}
        .legend .fail::before {{ background: #f08989; }}
        .legend .rate::before {{ background: #70b4ff; }}
        .month-card {{
          display: grid;
          gap: 8px;
        }}
        .summary-stacks {{
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
          gap: 14px;
        }}
        .mini-stack {{
          display: grid;
          gap: 10px;
        }}
        .mini-card {{
          border-radius: 14px;
          padding: 14px;
          background: rgba(255, 255, 255, 0.035);
          border: 1px solid rgba(152, 196, 169, 0.14);
          display: grid;
          gap: 6px;
        }}
        .mini-card strong {{
          font-size: 15px;
        }}
        .mini-pill {{
          justify-self: start;
          border-radius: 999px;
          padding: 3px 8px;
          background: rgba(112, 180, 255, 0.12);
          color: #97c7ff;
          font-size: 11px;
          text-transform: uppercase;
        }}
        .month-head {{
          display: flex;
          justify-content: space-between;
          gap: 10px;
          align-items: center;
        }}
        .month-pill {{
          border-radius: 999px;
          padding: 4px 10px;
          background: rgba(113, 214, 167, 0.14);
          color: #9be2c2;
          font-size: 12px;
        }}
        @media (max-width: 800px) {{
          .grid-wide {{
            grid-template-columns: 1fr;
          }}
        }}
      </style>
    </head>
    <body>
      <main class="page">
        <section class="hero">
          <h1>Simulation Final Report</h1>
          <p class="muted">Run {html.escape(str(run.get('run_id', '-')))} · Status {html.escape(str(run.get('status', '-')))} · Completed Round {run.get('completed_month', 0)}</p>
          <p>{html.escape(_external_db_notice(public_notes.get('db_path') or run.get('db_path') or '-'))}</p>
          <p class="muted">{html.escape(str(public_notes.get('round_interpretation') or _external_round_notice()))}</p>
        </section>
        <section class="grid">
          <article class="card">
            <h2>Run Snapshot</h2>
            <p>Agent Count: {run.get('agent_count', 0)}</p>
            <p>Total Rounds: {run.get('total_months', 0)}</p>
            <p>Started: {html.escape(str(run.get('started_at', '-')))}</p>
            <p>Completed: {html.escape(str(run.get('completed_at', '-')))}</p>
          </article>
          <article class="card">
            <h2>Final Summary</h2>
            <p>Top Agents: {len(final_summary.get('top_agents', []))}</p>
            <p>Key Properties: {len(final_summary.get('key_properties', []))}</p>
            <p>Failure Reasons: {len(final_summary.get('failure_reasons', []))}</p>
            <p>Interventions: {len(final_summary.get('interventions', []))}</p>
            <div class="metric">{len(month_reviews)} rounds reviewed</div>
          </article>
          <article class="card">
            <h2>Preset Timeline</h2>
            <ul>{preset_timeline}</ul>
          </article>
        </section>
        <section class="grid-wide">
          <article class="card">
            <h2>Market Bulletin</h2>
            <p class="bulletin">{latest_bulletin}</p>
            <div class="metric">Avg Transaction Price: ¥{float(last_month_summary.get('avg_transaction_price', 0) or 0):,.0f}</div>
            <p class="muted">Market Pulse: {html.escape(str(runtime_controls.get('market_pulse_enabled', False)))}</p>
          </article>
          <article class="card">
            <h2>Market Pulse</h2>
            <p>Transactions: {int(last_month_summary.get('transactions', 0) or 0)}</p>
            <p>Failed Negotiations: {int(last_month_summary.get('failed_negotiations', 0) or 0)}</p>
            <p>Buyer Count: {int(last_month_summary.get('buyer_count', 0) or 0)}</p>
            <p>Active Listings: {int(last_month_summary.get('active_listing_count', 0) or 0)}</p>
          </article>
        </section>
        <section class="card">
          <h2>Round Market Chart</h2>
          {chart_svg or '<p class="muted">No chart data available.</p>'}
          <div class="legend">
            <span class="tx">Transactions</span>
            <span class="avg">Avg Price</span>
            <span class="fail">Failed Negotiations</span>
            <span class="rate">Success Rate</span>
          </div>
        </section>
        <section class="card">
          <h2>Summary Highlights</h2>
          <div class="summary-stacks">
            <div class="mini-stack">
              <h3>Top Agents</h3>
              {top_agent_cards}
            </div>
            <div class="mini-stack">
              <h3>Key Properties</h3>
              {key_property_cards}
            </div>
            <div class="mini-stack">
              <h3>Failure Reasons</h3>
              {failure_reason_cards}
            </div>
          </div>
        </section>
        <section class="grid-wide">
          <article class="card">
            <h2>Controls Timeline</h2>
            <div class="mini-stack">{controls_timeline}</div>
          </article>
          <article class="card">
            <h2>Preset Impact</h2>
            <div class="mini-stack">{preset_impact_cards}</div>
          </article>
        </section>
        <section>
          <h2>Round Reviews</h2>
          <div class="grid">{month_cards}</div>
        </section>
      </main>
    </body>
    </html>
    """


@app.get("/controls")
def get_controls():
    return runtime.get_controls()


@app.get("/config/schema")
def get_config_schema():
    return _build_config_schema()


@app.get("/presets")
def get_presets():
    return {
        "presets": [
            {
                "id": preset_id,
                "label": spec["label"],
                "description": spec["description"],
                "population_template": spec.get("population", {}).get("template"),
                "population_count": spec.get("population", {}).get("count"),
                "developer_template": spec.get("developer", {}).get("template"),
                "developer_count": spec.get("developer", {}).get("count"),
                "income_strategy": (
                    "tier_adjustments"
                    if spec.get("income", {}).get("tier_adjustments")
                    else "pct_change"
                ),
                "controls_preview": {
                    "down_payment_ratio": spec.get("controls", {}).get("down_payment_ratio"),
                    "annual_interest_rate": spec.get("controls", {}).get("annual_interest_rate"),
                    "market_pulse_enabled": spec.get("controls", {}).get("market_pulse_enabled"),
                    "macro_override_mode": spec.get("controls", {}).get("macro_override_mode"),
                },
                "negotiation_quote_stream_enabled": bool(spec.get("controls", {}).get("negotiation_quote_stream_enabled", False)),
                "negotiation_quote_filter_mode": str(spec.get("controls", {}).get("negotiation_quote_filter_mode", "all") or "all"),
            }
            for preset_id, spec in SCENARIO_PRESETS.items()
        ]
    }


@app.post("/controls")
async def update_controls(req: RuntimeControlsRequest):
    result = await run_in_threadpool(runtime.update_controls, req)
    status = result.get("status", {})
    await ws_manager.broadcast(
        _event_envelope(
            "CONTROLS_UPDATED",
            {
                "controls": result.get("controls", {}),
                "status": status,
            },
            month=int(status.get("current_month", 0) or 0),
            phase="system",
        )
    )
    return result


@app.post("/interventions/population/add")
async def add_population_intervention(req: PopulationInterventionRequest):
    result = await run_in_threadpool(runtime.add_population, req)
    status = result.get("status", {})
    for event in result.get("result", {}).get("generated_events", []):
        await ws_manager.broadcast(event)
    await ws_manager.broadcast(
        _event_envelope(
            "POPULATION_ADDED",
            {
                "result": result.get("result", {}),
                "status": status,
            },
            month=int(status.get("current_month", 0) or 0),
            phase="system",
        )
    )
    return result


@app.post("/interventions/income")
async def apply_income_intervention(req: IncomeInterventionRequest):
    result = await run_in_threadpool(runtime.apply_income_intervention, req)
    status = result.get("status", {})
    await ws_manager.broadcast(
        _event_envelope(
            "INCOME_SHOCK_APPLIED",
            {
                "result": result.get("result", {}),
                "status": status,
            },
            month=int(status.get("current_month", 0) or 0),
            phase="system",
        )
    )
    return result


@app.post("/interventions/developer-supply")
async def inject_developer_supply(req: DeveloperSupplyInterventionRequest):
    result = await run_in_threadpool(runtime.inject_developer_supply, req)
    status = result.get("status", {})
    for event in result.get("result", {}).get("generated_events", []):
        await ws_manager.broadcast(event)
    for event in result.get("result", {}).get("listed_events", []):
        await ws_manager.broadcast(event)
    await ws_manager.broadcast(
        _event_envelope(
            "DEVELOPER_SUPPLY_INJECTED",
            {
                "result": result.get("result", {}),
                "status": status,
            },
            month=int(status.get("current_month", 0) or 0),
            phase="system",
        )
    )
    return result


@app.post("/presets/apply")
async def apply_scenario_preset(req: ScenarioPresetRequest):
    result = await run_in_threadpool(runtime.apply_scenario_preset, req)
    status = result.get("status", {})
    await ws_manager.broadcast(
        _event_envelope(
            "CONTROLS_UPDATED",
            {
                "controls": result.get("controls", {}),
                "status": status,
            },
            month=int(status.get("current_month", 0) or 0),
            phase="system",
        )
    )
    for event in result.get("population_result", {}).get("generated_events", []):
        await ws_manager.broadcast(event)
    await ws_manager.broadcast(
        _event_envelope(
            "POPULATION_ADDED",
            {
                "result": result.get("population_result", {}),
                "status": status,
            },
            month=int(status.get("current_month", 0) or 0),
            phase="system",
        )
    )
    await ws_manager.broadcast(
        _event_envelope(
            "INCOME_SHOCK_APPLIED",
            {
                "result": result.get("income_result", {}),
                "status": status,
            },
            month=int(status.get("current_month", 0) or 0),
            phase="system",
        )
    )
    for event in result.get("developer_result", {}).get("generated_events", []):
        await ws_manager.broadcast(event)
    for event in result.get("developer_result", {}).get("listed_events", []):
        await ws_manager.broadcast(event)
    await ws_manager.broadcast(
        _event_envelope(
            "DEVELOPER_SUPPLY_INJECTED",
            {
                "result": result.get("developer_result", {}),
                "status": status,
            },
            month=int(status.get("current_month", 0) or 0),
            phase="system",
        )
    )
    await ws_manager.broadcast(
        _event_envelope(
            "SCENARIO_PRESET_APPLIED",
            {
                "preset": result.get("preset"),
                "status": status,
            },
            month=int(status.get("current_month", 0) or 0),
            phase="system",
        )
    )
    return result


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        await websocket.send_json(_event_envelope("STATUS_SNAPSHOT", {"status": runtime.status()}, phase="system"))
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)
