#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
研究实验批跑脚本

用途：
1. 以程序方式运行第一轮控制变量实验，避免人工在 CLI 中重复输入。
2. 自动计时、导出结果、汇总 run 目录与核心产物路径。

说明：
- 默认使用真实 LLM 模式，不主动打开 LLM_MOCK_MODE。
- 优先服务于研究测试，不替代网页展示入口。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import sqlite3
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import project_manager
from config.config_loader import SimulationConfig
from scripts.export_results import export_data
from simulation_runner import SimulationRunner
from utils.llm_client import reset_llm_runtime_state


SEEDS = [101, 202, 303]


BASELINE_EXPERIMENT: Dict[str, Any] = {
    "months": 3,
    "agent_count": 120,
    "property_count": 144,
    "min_cash_observer_threshold": 500000,
    "base_year": 2026,
    "income_adjustment_rate": 1.00,
    "mortgage": {
        "down_payment_ratio": 0.30,
        "max_dti_ratio": 0.50,
        "annual_interest_rate": 0.035,
    },
    "smart_agent": {
        "effective_bid_floor_ratio": 0.93,
        "precheck_liquidity_buffer_months": 3,
        "precheck_include_tax_and_fee": True,
        "price_adjustment_llm_min_duration": 1,
        "price_adjustment_monthly_llm_cap": 120,
        "price_adjustment_model_type": "fast",
        "price_adjustment_high_heat_model_type": "smart",
        "info_delay_enabled": False,
        "info_delay_ratio": 0.50,
        "info_delay_ratio_multiplier": 1.0,
        "info_delay_min_months": 1,
        "info_delay_apply_to_normal": False,
        "info_delay_ratio_normal": 0.0,
        "info_delay_max_months": 2,
        "price_adjustment_heat_medium_threshold": 0.16,
        "price_adjustment_heat_high_threshold": 0.30,
        "price_adjustment_heat_zone_mix_weight": 0.30,
        "price_adjustment_heat_property_floor_on_zone_hot": 0.06,
        "price_adjustment_heat_zone_match_norm": 28.0,
        "price_adjustment_heat_zone_valid_bid_norm": 18.0,
        "price_adjustment_heat_zone_negotiation_norm": 14.0,
        "price_adjustment_heat_zone_outbid_norm": 10.0,
        # Regime V1 aggressive raise-release (rollback-able by toggling these switches)
        "regime_engine_v1_enabled": True,
        "regime_v1_price_reconsider_enabled": True,
        "regime_v1_raise_release_preforce_enabled": True,
        "regime_v1_raise_release_force_sample_enabled": True,
        "regime_v1_raise_release_force_sample_ratio": 0.10,
        "regime_v1_raise_release_force_sample_min_hits_per_month": 1,
        "regime_v1_raise_release_routing_mode": "ratio",
        "regime_v1_raise_release_topk_enabled": False,
        "regime_v1_raise_release_topk": 0,
        "regime_v1_raise_release_min_valid_bids_filter": 1,
        "regime_v1_raise_release_min_outbid_filter": 1,
        "regime_v1_raise_release_min_negotiation_filter": 1,
        "regime_v1_raise_release_exclude_cold_zero_bid": True,
        "regime_v1_raise_release_require_competition_evidence": True,
        "regime_v1_raise_release_zero_valid_bid_streak_block_months": 2,
        "price_adjustment_llm_min_calls_per_month": 2,
        "regime_v1_raise_release_hard_guard_enabled": True,
        "regime_v1_raise_release_min_valid_bids": 1,
        "regime_v1_raise_release_min_outbid_losses": 0,
        "regime_v1_raise_release_max_close_priority_score": 80.0,
        "regime_v1_hot_signal_lag_compensation_enabled": True,
        "regime_v1_hot_signal_lag_compensation_medium_delta_base": 0.03,
        "regime_v1_hot_signal_lag_compensation_medium_delta_scale": 0.04,
        "regime_v1_hot_signal_lag_compensation_high_delta_base": 0.02,
        "regime_v1_hot_signal_lag_compensation_high_delta_scale": 0.03,
        "regime_v1_raise_release_default_e_coeff": 1.05,
        "regime_v1_raise_force_rule_path_enabled": True,
        "regime_v1_raise_force_rule_coeff": 1.05,
        "regime_v1_raise_force_all_early_llm": True,
        "regime_v1_raise_release_cap_reserved": 10,
        "regime_v1_raise_release_force_llm_min_duration": 1,
        "classic_queue_high_headroom_penalty_enabled": True,
        "classic_queue_high_headroom_threshold_ratio": 0.18,
        "classic_queue_high_headroom_penalty_span": 0.30,
        "classic_queue_high_headroom_penalty_max": 0.36,
        "classic_queue_b_zone_entry_boost": 0.24,
        "classic_queue_b_zone_entry_max_headroom_ratio": 0.22,
        "classic_queue_b_zone_entry_min_budget_fit": 0.40,
        "classic_queue_b_zone_entry_max_owned_properties": 1,
        "classic_competitive_enabled": False,
        "classic_competitive_force_mode": False,
        "classic_competitive_max_active_buyers": 6,
        "normal_buyer_backup_slots": 0,
        "buyer_pending_order_cap": 1,
    },
    "market_pulse": {
        "enabled": False,
        "seed_existing_mortgage_ratio": 0.55,
    },
    "enable_intervention_panel": False,
    "zone_price_ranges": {
        "A": {"min": 32000, "max": 40000},
        "B": {"min": 10000, "max": 20000},
    },
    "zone_rents": {
        "A": 100,
        "B": 60,
    },
    "user_agent_config": {
        "ultra_high": {"count": 6, "income_range": (150000, 300000), "property_count": (2, 5)},
        "high": {"count": 18, "income_range": (80000, 150000), "property_count": (1, 3)},
        "middle": {"count": 36, "income_range": (40000, 80000), "property_count": (0, 1)},
        "low_mid": {"count": 30, "income_range": (20000, 40000), "property_count": (0, 1)},
        "low": {"count": 30, "income_range": (8000, 20000), "property_count": (0, 0)},
    },
}


def deep_merge(base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    result = deepcopy(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


FIRST_ROUND_GROUPS: Dict[str, Dict[str, Any]] = {
    "A": {
        "name": "A_基准组",
        "overrides": {},
    },
    "B": {
        "name": "B_样本量影响组",
        "overrides": {
            "agent_count": 240,
            "property_count": 288,
            "user_agent_config": {
                "ultra_high": {"count": 12},
                "high": {"count": 36},
                "middle": {"count": 72},
                "low_mid": {"count": 60},
                "low": {"count": 60},
            },
        },
    },
    "C": {
        "name": "C_收入结构下沉组",
        "overrides": {
            "user_agent_config": {
                "ultra_high": {"count": 4},
                "high": {"count": 12},
                "middle": {"count": 24},
                "low_mid": {"count": 32},
                "low": {"count": 48},
            },
        },
    },
    "D": {
        "name": "D_供给增加组",
        "overrides": {
            "property_count": 180,
        },
    },
    "E": {
        "name": "E_信息情绪机制组",
        "overrides": {
            "market_pulse": {
                "enabled": True,
                "seed_existing_mortgage_ratio": 0.55,
            },
        },
    },
    "C2": {
        "name": "C2_收入结构下沉复核组_6个月",
        "overrides": {
            "months": 6,
            "user_agent_config": {
                "ultra_high": {"count": 4},
                "high": {"count": 12},
                "middle": {"count": 24},
                "low_mid": {"count": 32},
                "low": {"count": 48},
            },
        },
    },
    "D2": {
        "name": "D2_供给增加复核组_6个月",
        "overrides": {
            "months": 6,
            "property_count": 180,
        },
    },
    "D3": {
        "name": "D3_供给增加修正后复核组_6个月",
        "overrides": {
            "months": 6,
            "property_count": 180,
        },
    },
    "C3": {
        "name": "C3_收入结构画像修正短测组_3个月",
        "overrides": {
            "months": 3,
            "user_agent_config": {
                "ultra_high": {"count": 4},
                "high": {"count": 12},
                "middle": {"count": 24},
                "low_mid": {"count": 32},
                "low": {"count": 48},
            },
        },
    },
    "D4": {
        "name": "D4_供给增加画像修正短测组_3个月",
        "overrides": {
            "months": 3,
            "property_count": 180,
        },
    },
    "C4": {
        "name": "C4_收入结构画像修正复核组_6个月",
        "overrides": {
            "months": 6,
            "user_agent_config": {
                "ultra_high": {"count": 4},
                "high": {"count": 12},
                "middle": {"count": 24},
                "low_mid": {"count": 32},
                "low": {"count": 48},
            },
        },
    },
    "D5": {
        "name": "D5_供给增加画像修正复核组_6个月",
        "overrides": {
            "months": 6,
            "property_count": 180,
        },
    },
    # 第二轮：市场公报接收/时滞机制（其余条件尽量固定）
    "R2A": {
        "name": "R2A_公报即时接收_对照组",
        "overrides": {
            "months": 6,
            "property_count": 180,
            "market_pulse": {
                "enabled": True,
                "seed_existing_mortgage_ratio": 0.55,
            },
            "smart_agent": {
                "info_delay_enabled": False,
                "info_delay_ratio": 0.00,
                "info_delay_max_months": 0,
            },
        },
    },
    "R2B": {
        "name": "R2B_公报延迟接收_中等时滞组",
        "overrides": {
            "months": 6,
            "property_count": 180,
            "market_pulse": {
                "enabled": True,
                "seed_existing_mortgage_ratio": 0.55,
            },
            "smart_agent": {
                "info_delay_enabled": True,
                "info_delay_ratio": 0.68,
                "info_delay_ratio_multiplier": 1.60,
                "info_delay_min_months": 1,
                "info_delay_apply_to_normal": True,
                "info_delay_ratio_normal": 0.35,
                "info_delay_max_months": 3,
            },
        },
    },
    "R2C": {
        "name": "R2C_公报延迟接收_高时滞组",
        "overrides": {
            "months": 6,
            "property_count": 180,
            "market_pulse": {
                "enabled": True,
                "seed_existing_mortgage_ratio": 0.55,
            },
            "smart_agent": {
                "info_delay_enabled": True,
                "info_delay_ratio": 0.80,
                "info_delay_ratio_multiplier": 1.30,
                "info_delay_min_months": 2,
                "info_delay_max_months": 3,
            },
        },
    },
    "G1R": {
        "name": "G1R_少房多买_竞价门禁",
        "overrides": {
            "months": 1,
            "agent_count": 36,
            "property_count": 40,
            "smart_agent": {
                "classic_competitive_enabled": True,
                "classic_competitive_force_mode": True,
                "classic_competitive_max_active_buyers": 12,
                "normal_buyer_backup_slots": 1,
                "buyer_pending_order_cap": 2,
                # 触发验证专用：放大同房竞争和扩轮触发概率
                "batch_bidding_second_round_enabled": True,
                "batch_bidding_second_round_min_candidates": 2,
                "batch_bidding_second_round_top_n": 8,
                "batch_bidding_rounds_equal_candidate_count_enabled": True,
                "batch_bidding_min_rounds_on_competition": 4,
                "batch_bidding_rebid_max_rounds": 6,
                "batch_bidding_rounds_cap": 12,
                "batch_bidding_max_competition_pool": 12,
                # 触发验证专用：收紧拥挤容忍阈值，便于验证 hard-pass 逻辑
                "candidate_crowd_pressure_scale": 1.60,
                "candidate_crowd_tolerance_avoid": 0.60,
                "candidate_crowd_tolerance_neutral": 0.95,
                "candidate_crowd_tolerance_follow": 1.40,
            },
            "user_agent_config": {
                "ultra_high": {"count": 2, "income_range": (150000, 260000), "property_count": (2, 3)},
                "high": {"count": 6, "income_range": (80000, 150000), "property_count": (1, 2)},
                "middle": {"count": 12, "income_range": (40000, 80000), "property_count": (0, 1)},
                "low_mid": {"count": 8, "income_range": (20000, 40000), "property_count": (0, 0)},
                "low": {"count": 8, "income_range": (8000, 20000), "property_count": (0, 0)},
            },
        },
    },
    "G1P": {
        "name": "G1P_少房多买_卖方抬价观察",
        "overrides": {
            "months": 1,
            "agent_count": 36,
            "property_count": 40,
            "smart_agent": {
                # Keep competitive settings, but do not force CLASSIC path.
                # Let seller route naturally choose batch/classic by context.
                "classic_competitive_enabled": True,
                "classic_competitive_force_mode": False,
                "classic_competitive_max_active_buyers": 8,
                "normal_buyer_backup_slots": 1,
                "buyer_pending_order_cap": 2,
            },
            "user_agent_config": {
                "ultra_high": {"count": 2, "income_range": (150000, 260000), "property_count": (2, 3)},
                "high": {"count": 6, "income_range": (80000, 150000), "property_count": (1, 2)},
                "middle": {"count": 12, "income_range": (40000, 80000), "property_count": (0, 1)},
                "low_mid": {"count": 8, "income_range": (20000, 40000), "property_count": (0, 0)},
                "low": {"count": 8, "income_range": (8000, 20000), "property_count": (0, 0)},
            },
        },
    },
    "G2R": {
        "name": "G2R_多房少买_去化门禁",
        "overrides": {
            "months": 1,
            "agent_count": 36,
            "property_count": 72,
            "smart_agent": {
                "classic_competitive_enabled": False,
                "normal_buyer_backup_slots": 1,
                "buyer_pending_order_cap": 2,
            },
            "user_agent_config": {
                "ultra_high": {"count": 2, "income_range": (150000, 300000), "property_count": (3, 5)},
                "high": {"count": 8, "income_range": (80000, 150000), "property_count": (2, 4)},
                "middle": {"count": 12, "income_range": (40000, 80000), "property_count": (1, 2)},
                "low_mid": {"count": 8, "income_range": (20000, 40000), "property_count": (0, 1)},
                "low": {"count": 6, "income_range": (8000, 20000), "property_count": (0, 0)},
            },
        },
    },
    "G2T": {
        "name": "G2T_多房少买_B区后段承接调优",
        "overrides": {
            "months": 1,
            "agent_count": 36,
            "property_count": 72,
            "smart_agent": {
                "classic_competitive_enabled": False,
                "normal_buyer_backup_slots": 2,
                "buyer_pending_order_cap": 3,
                "classic_queue_high_headroom_penalty_enabled": True,
                "classic_queue_high_headroom_threshold_ratio": 0.12,
                "classic_queue_high_headroom_penalty_span": 0.45,
                "classic_queue_high_headroom_penalty_max": 0.50,
                "classic_queue_b_zone_entry_boost": 0.38,
                "classic_queue_b_zone_entry_max_headroom_ratio": 0.16,
                "classic_queue_b_zone_entry_min_budget_fit": 0.32,
                "classic_queue_b_zone_entry_max_owned_properties": 1,
            },
            "user_agent_config": {
                "ultra_high": {"count": 2, "income_range": (150000, 300000), "property_count": (3, 5)},
                "high": {"count": 8, "income_range": (80000, 150000), "property_count": (2, 4)},
                "middle": {"count": 12, "income_range": (40000, 80000), "property_count": (1, 2)},
                "low_mid": {"count": 8, "income_range": (20000, 40000), "property_count": (0, 1)},
                "low": {"count": 6, "income_range": (8000, 20000), "property_count": (0, 0)},
            },
        },
    },
    "I1": {
        "name": "I1_融合组_少房多买_风格与调价",
        "overrides": {
            "months": 3,
            "agent_count": 60,
            "property_count": 44,
            "smart_agent": {
                "price_adjustment_llm_min_duration": 2,
                "price_adjustment_monthly_llm_cap": 160,
                "normal_seller_rule_pricing_enabled": False,
                "price_adjustment_heat_medium_threshold": 0.14,
                "price_adjustment_heat_high_threshold": 0.30,
                "classic_competitive_enabled": True,
                "classic_competitive_force_mode": False,
                "classic_competitive_max_active_buyers": 8,
                "normal_buyer_backup_slots": 1,
                "buyer_pending_order_cap": 2,
            },
            "personality_weights": {
                "aggressive": 0.55,
                "conservative": 0.20,
                "balanced": 0.25,
            },
            "user_agent_config": {
                "ultra_high": {"count": 4, "income_range": (150000, 280000), "property_count": (2, 4)},
                "high": {"count": 10, "income_range": (80000, 150000), "property_count": (1, 3)},
                "middle": {"count": 18, "income_range": (40000, 80000), "property_count": (0, 1)},
                "low_mid": {"count": 14, "income_range": (20000, 40000), "property_count": (0, 1)},
                "low": {"count": 14, "income_range": (8000, 20000), "property_count": (0, 0)},
            },
        },
    },
    "I1B": {
        "name": "I1B_少房多买_B区后段收敛修正版",
        "overrides": {
            "months": 3,
            "agent_count": 60,
            "property_count": 44,
            "smart_agent": {
                "price_adjustment_llm_min_duration": 2,
                "price_adjustment_monthly_llm_cap": 160,
                "normal_seller_rule_pricing_enabled": False,
                "price_adjustment_heat_medium_threshold": 0.14,
                "price_adjustment_heat_high_threshold": 0.30,
                "classic_competitive_enabled": True,
                "classic_competitive_force_mode": False,
                "classic_competitive_max_active_buyers": 8,
                # B区后段收敛：给刚需更多进入后段机会，抑制高预算买家长期挤占
                "normal_buyer_backup_slots": 2,
                "buyer_pending_order_cap": 3,
                "classic_queue_high_headroom_penalty_enabled": True,
                "classic_queue_high_headroom_threshold_ratio": 0.12,
                "classic_queue_high_headroom_penalty_span": 0.45,
                "classic_queue_high_headroom_penalty_max": 0.46,
                "classic_queue_b_zone_entry_boost": 0.30,
                "classic_queue_b_zone_entry_max_headroom_ratio": 0.18,
                "classic_queue_b_zone_entry_min_budget_fit": 0.34,
                "classic_queue_b_zone_entry_max_owned_properties": 1,
            },
            "personality_weights": {
                "aggressive": 0.55,
                "conservative": 0.20,
                "balanced": 0.25,
            },
            "user_agent_config": {
                "ultra_high": {"count": 4, "income_range": (150000, 280000), "property_count": (2, 4)},
                "high": {"count": 10, "income_range": (80000, 150000), "property_count": (1, 3)},
                "middle": {"count": 18, "income_range": (40000, 80000), "property_count": (0, 1)},
                "low_mid": {"count": 14, "income_range": (20000, 40000), "property_count": (0, 1)},
                "low": {"count": 14, "income_range": (8000, 20000), "property_count": (0, 0)},
            },
        },
    },
    "I1C": {
        "name": "I1C_少房多买_B区后段收敛温和版",
        "overrides": {
            "months": 3,
            "agent_count": 60,
            "property_count": 44,
            "smart_agent": {
                "price_adjustment_llm_min_duration": 2,
                "price_adjustment_monthly_llm_cap": 160,
                "normal_seller_rule_pricing_enabled": False,
                "price_adjustment_heat_medium_threshold": 0.14,
                "price_adjustment_heat_high_threshold": 0.30,
                "classic_competitive_enabled": True,
                "classic_competitive_force_mode": False,
                "classic_competitive_max_active_buyers": 8,
                "normal_buyer_backup_slots": 2,
                "buyer_pending_order_cap": 2,
                # 温和参数：不过度抑制高预算，也不给B区过强硬推
                "classic_queue_high_headroom_penalty_enabled": True,
                "classic_queue_high_headroom_threshold_ratio": 0.15,
                "classic_queue_high_headroom_penalty_span": 0.35,
                "classic_queue_high_headroom_penalty_max": 0.28,
                "classic_queue_b_zone_entry_boost": 0.16,
                "classic_queue_b_zone_entry_max_headroom_ratio": 0.20,
                "classic_queue_b_zone_entry_min_budget_fit": 0.45,
                "classic_queue_b_zone_entry_max_owned_properties": 1,
            },
            "personality_weights": {
                "aggressive": 0.55,
                "conservative": 0.20,
                "balanced": 0.25,
            },
            "user_agent_config": {
                "ultra_high": {"count": 4, "income_range": (150000, 280000), "property_count": (2, 4)},
                "high": {"count": 10, "income_range": (80000, 150000), "property_count": (1, 3)},
                "middle": {"count": 18, "income_range": (40000, 80000), "property_count": (0, 1)},
                "low_mid": {"count": 14, "income_range": (20000, 40000), "property_count": (0, 1)},
                "low": {"count": 14, "income_range": (8000, 20000), "property_count": (0, 0)},
            },
        },
    },
    "I1D": {
        "name": "I1D_少房多买_B区后段收敛微调版",
        "overrides": {
            "months": 3,
            "agent_count": 60,
            "property_count": 44,
            "market": {
                # I1D 收敛门禁：给 B 区保留可承接的供给底线，避免极冷样本
                "initial_listing_rate": 0.10,
                "zones": {
                    "B": {
                        # 让 B 区低/中价房占比更高，降低“全挤在少量同价位房源”的概率
                        "supply_band_ratio": {
                            "low": 0.55,
                            "mid": 0.35,
                            "high": 0.10,
                        },
                        # 保留少量可被教育偏好买家接受的房源，不做极端全学区化
                        "school_district_ratio_by_band": {
                            "low": 0.12,
                            "mid": 0.20,
                            "high": 0.35,
                        },
                    }
                },
            },
            "smart_agent": {
                "price_adjustment_llm_min_duration": 2,
                "price_adjustment_monthly_llm_cap": 160,
                "normal_seller_rule_pricing_enabled": False,
                "price_adjustment_heat_medium_threshold": 0.10,
                "price_adjustment_heat_high_threshold": 0.22,
                "classic_competitive_enabled": True,
                "classic_competitive_force_mode": False,
                "classic_competitive_max_active_buyers": 14,
                "normal_buyer_backup_slots": 3,
                "buyer_pending_order_cap": 4,
                "order_ttl_days": 45,
                # 在 I1C 基础上小步收敛，避免 I1B 那种过冲
                "classic_queue_high_headroom_penalty_enabled": True,
                "classic_queue_high_headroom_threshold_ratio": 0.14,
                "classic_queue_high_headroom_penalty_span": 0.35,
                "classic_queue_high_headroom_penalty_max": 0.36,
                "classic_queue_b_zone_entry_boost": 0.24,
                "classic_queue_b_zone_entry_max_headroom_ratio": 0.20,
                "classic_queue_b_zone_entry_min_budget_fit": 0.45,
                "classic_queue_b_zone_entry_max_owned_properties": 1,
                "classic_queue_b_zone_non_school_penalty_enabled": True,
                "classic_queue_b_zone_non_school_min_headroom_ratio": 0.30,
                "classic_queue_b_zone_non_school_penalty_span": 0.55,
                "classic_queue_b_zone_non_school_penalty_max": 0.26,
                "classic_queue_b_zone_non_school_owned_threshold": 1,
                "batch_bidding_second_round_enabled": True,
                "batch_bidding_second_round_min_candidates": 2,
                "batch_bidding_second_round_top_n": 8,
                "batch_bidding_rounds_equal_candidate_count_enabled": True,
                "batch_bidding_min_rounds_on_competition": 4,
                "batch_bidding_rebid_max_rounds": 6,
                "batch_bidding_rounds_cap": 12,
                "batch_bidding_max_competition_pool": 12,
                # 触发验证专用：收紧拥挤容忍阈值，便于验证 hard-pass 逻辑
                "candidate_crowd_pressure_scale": 1.80,
                "candidate_crowd_tolerance_avoid": 0.60,
                "candidate_crowd_tolerance_neutral": 0.90,
                "candidate_crowd_tolerance_follow": 1.30,
            },
            "personality_weights": {
                "aggressive": 0.55,
                "conservative": 0.20,
                "balanced": 0.25,
            },
            "user_agent_config": {
                "ultra_high": {"count": 4, "income_range": (150000, 280000), "property_count": (2, 4)},
                "high": {"count": 10, "income_range": (80000, 150000), "property_count": (1, 3)},
                "middle": {"count": 18, "income_range": (40000, 80000), "property_count": (0, 1)},
                "low_mid": {"count": 14, "income_range": (20000, 40000), "property_count": (0, 1)},
                "low": {"count": 14, "income_range": (8000, 20000), "property_count": (0, 0)},
            },
        },
    },
    "I1E": {
        "name": "I1E_少房多买_B区挤出小步收敛门禁",
        "overrides": {
            "months": 3,
            "agent_count": 60,
            "property_count": 44,
            "market": {
                # 沿用 I1D 的供给底线，保证可比性
                "initial_listing_rate": 0.10,
                "zones": {
                    "B": {
                        "supply_band_ratio": {"low": 0.55, "mid": 0.35, "high": 0.10},
                        "school_district_ratio_by_band": {"low": 0.12, "mid": 0.20, "high": 0.35},
                    }
                },
            },
            "smart_agent": {
                "price_adjustment_llm_min_duration": 2,
                "price_adjustment_monthly_llm_cap": 160,
                "normal_seller_rule_pricing_enabled": False,
                "price_adjustment_heat_medium_threshold": 0.10,
                "price_adjustment_heat_high_threshold": 0.22,
                "classic_competitive_enabled": True,
                "classic_competitive_force_mode": False,
                "classic_competitive_max_active_buyers": 8,
                "normal_buyer_backup_slots": 3,
                "buyer_pending_order_cap": 4,
                "order_ttl_days": 45,
                # 相比 I1D 只做小步：略加强“高预算挤占”惩罚 + 略抬B区刚需承接
                "classic_queue_high_headroom_penalty_enabled": True,
                "classic_queue_high_headroom_threshold_ratio": 0.12,
                "classic_queue_high_headroom_penalty_span": 0.40,
                "classic_queue_high_headroom_penalty_max": 0.42,
                "classic_queue_b_zone_entry_boost": 0.28,
                "classic_queue_b_zone_entry_max_headroom_ratio": 0.20,
                "classic_queue_b_zone_entry_min_budget_fit": 0.42,
                "classic_queue_b_zone_entry_max_owned_properties": 1,
                "classic_queue_b_zone_non_school_penalty_enabled": True,
                "classic_queue_b_zone_non_school_min_headroom_ratio": 0.28,
                "classic_queue_b_zone_non_school_penalty_span": 0.60,
                "classic_queue_b_zone_non_school_penalty_max": 0.30,
                "classic_queue_b_zone_non_school_owned_threshold": 1,
                "batch_bidding_second_round_enabled": True,
                "batch_bidding_second_round_min_candidates": 2,
                "batch_bidding_second_round_top_n": 4,
            },
            "personality_weights": {
                "aggressive": 0.55,
                "conservative": 0.20,
                "balanced": 0.25,
            },
            "user_agent_config": {
                "ultra_high": {"count": 4, "income_range": (150000, 280000), "property_count": (2, 4)},
                "high": {"count": 10, "income_range": (80000, 150000), "property_count": (1, 3)},
                "middle": {"count": 18, "income_range": (40000, 80000), "property_count": (0, 1)},
                "low_mid": {"count": 14, "income_range": (20000, 40000), "property_count": (0, 1)},
                "low": {"count": 14, "income_range": (8000, 20000), "property_count": (0, 0)},
            },
        },
    },
    "I1F": {
        "name": "I1F_少房多买_候选分散与成交回拉门禁",
        "overrides": {
            "months": 3,
            "agent_count": 60,
            "property_count": 44,
            "market": {
                # 保持 I1D/I1E 的供给底线，确保可比
                "initial_listing_rate": 0.10,
                "zones": {
                    "B": {
                        "supply_band_ratio": {"low": 0.55, "mid": 0.35, "high": 0.10},
                        "school_district_ratio_by_band": {"low": 0.12, "mid": 0.20, "high": 0.35},
                    }
                },
            },
            "smart_agent": {
                "price_adjustment_llm_min_duration": 2,
                "price_adjustment_monthly_llm_cap": 160,
                "normal_seller_rule_pricing_enabled": False,
                "price_adjustment_heat_medium_threshold": 0.10,
                "price_adjustment_heat_high_threshold": 0.22,
                "classic_competitive_enabled": True,
                "classic_competitive_force_mode": False,
                "classic_competitive_max_active_buyers": 8,
                # 候选分散：多给一个候选位 + 多一个探索位
                "candidate_top_k": 6,
                "candidate_exploration_slots": 2,
                "normal_buyer_backup_slots": 3,
                "buyer_pending_order_cap": 4,
                "order_ttl_days": 45,
                # 比 I1E 略放松高预算惩罚，避免压缩总成交
                "classic_queue_high_headroom_penalty_enabled": True,
                "classic_queue_high_headroom_threshold_ratio": 0.14,
                "classic_queue_high_headroom_penalty_span": 0.36,
                "classic_queue_high_headroom_penalty_max": 0.34,
                # 保留 B 区承接，但强度略回收
                "classic_queue_b_zone_entry_boost": 0.24,
                "classic_queue_b_zone_entry_max_headroom_ratio": 0.22,
                "classic_queue_b_zone_entry_min_budget_fit": 0.40,
                "classic_queue_b_zone_entry_max_owned_properties": 1,
                "classic_queue_b_zone_non_school_penalty_enabled": True,
                "classic_queue_b_zone_non_school_min_headroom_ratio": 0.28,
                "classic_queue_b_zone_non_school_penalty_span": 0.60,
                "classic_queue_b_zone_non_school_penalty_max": 0.26,
                "classic_queue_b_zone_non_school_owned_threshold": 1,
                "batch_bidding_second_round_enabled": True,
                "batch_bidding_second_round_min_candidates": 2,
                "batch_bidding_second_round_top_n": 4,
            },
            "personality_weights": {
                "aggressive": 0.55,
                "conservative": 0.20,
                "balanced": 0.25,
            },
            "user_agent_config": {
                "ultra_high": {"count": 4, "income_range": (150000, 280000), "property_count": (2, 4)},
                "high": {"count": 10, "income_range": (80000, 150000), "property_count": (1, 3)},
                "middle": {"count": 18, "income_range": (40000, 80000), "property_count": (0, 1)},
                "low_mid": {"count": 14, "income_range": (20000, 40000), "property_count": (0, 1)},
                "low": {"count": 14, "income_range": (8000, 20000), "property_count": (0, 0)},
            },
        },
    },
    "I2": {
        "name": "I2_融合组_多房少买_风格与去化",
        "overrides": {
            "months": 3,
            "agent_count": 60,
            "property_count": 110,
            "decision_factors": {
                "activation": {
                    # 避免多房少买场景出现“整月无有效买家匹配”的冷样本
                    # 仅提高触发活跃的机会，不替LLM做买卖决策
                    "base_probability": 0.006,
                    "min_cash_observer_no_property": 350000,
                    "pre_filter": {
                        "normal": {
                            "min_cash": 150000,
                            "min_income": 12000,
                        }
                    },
                }
            },
            "life_events": {
                "monthly_event_trigger_prob": 0.32,
            },
            "smart_agent": {
                "price_adjustment_llm_min_duration": 2,
                "price_adjustment_monthly_llm_cap": 220,
                "normal_seller_rule_pricing_enabled": False,
                "price_adjustment_heat_medium_threshold": 0.10,
                "price_adjustment_heat_high_threshold": 0.22,
                "classic_competitive_enabled": False,
                "normal_buyer_backup_slots": 3,
                "buyer_pending_order_cap": 4,
                "order_ttl_days": 45,
                "classic_queue_high_headroom_penalty_enabled": True,
                "classic_queue_high_headroom_threshold_ratio": 0.12,
                "classic_queue_high_headroom_penalty_span": 0.45,
                "classic_queue_high_headroom_penalty_max": 0.50,
                "classic_queue_b_zone_entry_boost": 0.38,
                "classic_queue_b_zone_entry_max_headroom_ratio": 0.16,
                "classic_queue_b_zone_entry_min_budget_fit": 0.32,
                "classic_queue_b_zone_entry_max_owned_properties": 1,
                "classic_queue_b_zone_non_school_penalty_enabled": True,
                "classic_queue_b_zone_non_school_min_headroom_ratio": 0.22,
                "classic_queue_b_zone_non_school_penalty_span": 0.45,
                "classic_queue_b_zone_non_school_penalty_max": 0.30,
                "classic_queue_b_zone_non_school_owned_threshold": 1,
                "batch_bidding_second_round_enabled": True,
                "batch_bidding_second_round_min_candidates": 2,
                "batch_bidding_second_round_top_n": 4,
            },
            "personality_weights": {
                "aggressive": 0.55,
                "conservative": 0.20,
                "balanced": 0.25,
            },
            "user_agent_config": {
                "ultra_high": {"count": 4, "income_range": (150000, 300000), "property_count": (3, 5)},
                "high": {"count": 12, "income_range": (80000, 150000), "property_count": (2, 4)},
                "middle": {"count": 18, "income_range": (40000, 80000), "property_count": (1, 2)},
                "low_mid": {"count": 14, "income_range": (20000, 40000), "property_count": (0, 1)},
                "low": {"count": 12, "income_range": (8000, 20000), "property_count": (0, 0)},
            },
        },
    },
    "I1G": {
        "name": "I1G_少房多买_E稳定触发门禁",
        "overrides": {
            "months": 3,
            "agent_count": 60,
            "property_count": 44,
            "market": {
                # 维持 I1D/I1E 的供给底线
                "initial_listing_rate": 0.10,
                "zones": {
                    "B": {
                        "supply_band_ratio": {"low": 0.55, "mid": 0.35, "high": 0.10},
                        "school_district_ratio_by_band": {"low": 0.12, "mid": 0.20, "high": 0.35},
                    }
                },
            },
            "smart_agent": {
                # 保留 I1E 的 B 区承接参数，不在此门禁改动队列侧
                "price_adjustment_llm_min_duration": 1,
                "price_adjustment_monthly_llm_cap": 180,
                "normal_seller_rule_pricing_enabled": False,
                "price_adjustment_heat_medium_threshold": 0.10,
                "price_adjustment_heat_high_threshold": 0.20,
                "classic_competitive_enabled": True,
                "classic_competitive_force_mode": False,
                "classic_competitive_max_active_buyers": 8,
                "normal_buyer_backup_slots": 3,
                "buyer_pending_order_cap": 4,
                "order_ttl_days": 45,
                "classic_queue_high_headroom_penalty_enabled": True,
                "classic_queue_high_headroom_threshold_ratio": 0.12,
                "classic_queue_high_headroom_penalty_span": 0.40,
                "classic_queue_high_headroom_penalty_max": 0.42,
                "classic_queue_b_zone_entry_boost": 0.28,
                "classic_queue_b_zone_entry_max_headroom_ratio": 0.20,
                "classic_queue_b_zone_entry_min_budget_fit": 0.42,
                "classic_queue_b_zone_entry_max_owned_properties": 1,
                "classic_queue_b_zone_non_school_penalty_enabled": True,
                "classic_queue_b_zone_non_school_min_headroom_ratio": 0.28,
                "classic_queue_b_zone_non_school_penalty_span": 0.60,
                "classic_queue_b_zone_non_school_penalty_max": 0.30,
                "classic_queue_b_zone_non_school_owned_threshold": 1,
                "batch_bidding_second_round_enabled": True,
                "batch_bidding_second_round_min_candidates": 2,
                "batch_bidding_second_round_top_n": 4,
            },
            "personality_weights": {
                # 仅小步提高激进风格占比，提高 E 出现概率
                "aggressive": 0.60,
                "conservative": 0.18,
                "balanced": 0.22,
            },
            "user_agent_config": {
                "ultra_high": {"count": 4, "income_range": (150000, 280000), "property_count": (2, 4)},
                "high": {"count": 10, "income_range": (80000, 150000), "property_count": (1, 3)},
                "middle": {"count": 18, "income_range": (40000, 80000), "property_count": (0, 1)},
                "low_mid": {"count": 14, "income_range": (20000, 40000), "property_count": (0, 1)},
                "low": {"count": 14, "income_range": (8000, 20000), "property_count": (0, 0)},
            },
        },
    },
    "I1H": {
        "name": "I1H_少房多买_B区挤出占比收敛门禁",
        "overrides": {
            "months": 3,
            "agent_count": 60,
            "property_count": 48,
            "market": {
                # 小步提高供给，优先给 B 区低/中总价盘，降低同房源挤兑
                "initial_listing_rate": 0.12,
                "zones": {
                    "B": {
                        "supply_band_ratio": {"low": 0.62, "mid": 0.30, "high": 0.08},
                        "school_district_ratio_by_band": {"low": 0.10, "mid": 0.18, "high": 0.32},
                    }
                },
            },
            "smart_agent": {
                "price_adjustment_llm_min_duration": 2,
                "price_adjustment_monthly_llm_cap": 160,
                "normal_seller_rule_pricing_enabled": False,
                "price_adjustment_heat_medium_threshold": 0.10,
                "price_adjustment_heat_high_threshold": 0.22,
                "classic_competitive_enabled": True,
                "classic_competitive_force_mode": False,
                # Promote key trigger path from gate-only to regular I1H runs:
                # force multi-buyer pooling + batch route, while keeping affordability precheck ON.
                "force_multi_buyer_pool_enabled": True,
                "force_min_buyers": 8,
                "gate_force_multi_buyer_pool_enabled": True,
                "gate_force_min_buyers": 10,
                "negotiation_force_batch_mode": True,
                "classic_competitive_max_active_buyers": 8,
                "normal_buyer_backup_slots": 3,
                "buyer_pending_order_cap": 4,
                "order_ttl_days": 45,
                "candidate_force_full_visible_pool_enabled": True,
                "candidate_crowd_hard_exclude_enabled": False,
                "candidate_crowd_precheck_reselect_enabled": True,
                # 保持与 I1E 接近，仅微调，不做剧烈惩罚
                "classic_queue_high_headroom_penalty_enabled": True,
                "classic_queue_high_headroom_threshold_ratio": 0.13,
                "classic_queue_high_headroom_penalty_span": 0.38,
                "classic_queue_high_headroom_penalty_max": 0.36,
                "classic_queue_b_zone_entry_boost": 0.30,
                "classic_queue_b_zone_entry_max_headroom_ratio": 0.22,
                "classic_queue_b_zone_entry_min_budget_fit": 0.40,
                "classic_queue_b_zone_entry_max_owned_properties": 1,
                "classic_queue_b_zone_non_school_penalty_enabled": True,
                "classic_queue_b_zone_non_school_min_headroom_ratio": 0.28,
                "classic_queue_b_zone_non_school_penalty_span": 0.60,
                "classic_queue_b_zone_non_school_penalty_max": 0.28,
                "classic_queue_b_zone_non_school_owned_threshold": 1,
                "batch_bidding_second_round_enabled": True,
                "batch_bidding_second_round_min_candidates": 2,
                "batch_bidding_second_round_top_n": 4,
                "batch_bidding_rounds_equal_candidate_count_enabled": True,
                "batch_bidding_min_rounds_on_competition": 4,
                "batch_bidding_rebid_max_rounds": 8,
                "batch_bidding_rounds_cap": 12,
            },
            "personality_weights": {
                "aggressive": 0.55,
                "conservative": 0.20,
                "balanced": 0.25,
            },
            "user_agent_config": {
                "ultra_high": {"count": 4, "income_range": (150000, 280000), "property_count": (2, 4)},
                "high": {"count": 10, "income_range": (80000, 150000), "property_count": (1, 3)},
                "middle": {"count": 18, "income_range": (40000, 80000), "property_count": (0, 1)},
                "low_mid": {"count": 14, "income_range": (20000, 40000), "property_count": (0, 1)},
                "low": {"count": 14, "income_range": (8000, 20000), "property_count": (0, 0)},
            },
        },
    },
    "I1J": {
        "name": "I1J_刚需上车能力门禁_2个月双seed",
        "overrides": {
            "months": 2,
            "agent_count": 80,
            "property_count": 96,
            "min_cash_observer_threshold": 200000,
            "zone_price_ranges": {
                "A": {"min": 28000, "max": 38000},
                "B": {"min": 9000, "max": 16000},
            },
            "market": {
                "initial_listing_rate": 0.22,
                "zones": {
                    "B": {
                        "supply_band_ratio": {"low": 0.68, "mid": 0.27, "high": 0.05},
                        "school_district_ratio_by_band": {"low": 0.25, "mid": 0.35, "high": 0.45},
                    }
                },
            },
            "smart_agent": {
                "price_adjustment_llm_min_duration": 2,
                "price_adjustment_monthly_llm_cap": 120,
                "normal_seller_rule_pricing_enabled": False,
                "classic_competitive_enabled": True,
                "classic_competitive_force_mode": False,
                "classic_competitive_max_active_buyers": 8,
                "normal_buyer_backup_slots": 4,
                "buyer_pending_order_cap": 3,
                "order_ttl_days": 45,
                "candidate_force_full_visible_pool_enabled": True,
                "candidate_crowd_hard_exclude_enabled": False,
                "candidate_crowd_precheck_reselect_enabled": True,
                "candidate_top_k": 6,
                "candidate_exploration_slots": 2,
                "classic_queue_high_headroom_penalty_enabled": True,
                "classic_queue_high_headroom_threshold_ratio": 0.12,
                "classic_queue_high_headroom_penalty_span": 0.36,
                "classic_queue_high_headroom_penalty_max": 0.35,
                "classic_queue_b_zone_entry_boost": 0.28,
                "classic_queue_b_zone_entry_max_headroom_ratio": 0.24,
                "classic_queue_b_zone_entry_min_budget_fit": 0.38,
                "classic_queue_b_zone_entry_max_owned_properties": 1,
                "batch_bidding_second_round_enabled": True,
                "batch_bidding_second_round_min_candidates": 2,
                "batch_bidding_second_round_top_n": 4,
                "batch_bidding_rounds_equal_candidate_count_enabled": True,
                "batch_bidding_min_rounds_on_competition": 3,
                "batch_bidding_rebid_max_rounds": 6,
                "batch_bidding_rounds_cap": 10,
            },
            "personality_weights": {
                "aggressive": 0.50,
                "conservative": 0.20,
                "balanced": 0.30,
            },
            "user_agent_config": {
                "ultra_high": {"count": 4, "income_range": (150000, 280000), "property_count": (1, 3)},
                "high": {"count": 10, "income_range": (80000, 150000), "property_count": (1, 2)},
                "middle": {"count": 22, "income_range": (40000, 80000), "property_count": (0, 1)},
                "low_mid": {"count": 20, "income_range": (20000, 40000), "property_count": (0, 0)},
                "low": {"count": 24, "income_range": (8000, 20000), "property_count": (0, 0)},
            },
        },
    },
    "I1T": {
        "name": "I1T_强竞争触发门禁_多买家少挂牌",
        "overrides": {
            "months": 1,
            "agent_count": 20,
            "property_count": 3,
            "zone_price_ranges": {
                # 门禁用低价带，确保至少 3+ 买家能够进入同房竞争，稳定触发多轮机制
                "A": {"min": 1200, "max": 2500},
                "B": {"min": 1200, "max": 2500},
            },
            "mortgage": {
                "down_payment_ratio": 0.10,
                "max_dti_ratio": 0.80,
                "annual_interest_rate": 0.030,
            },
            "market": {
                # 挂牌保底至少会形成多个在售，且明显供不应求
                "initial_listing_rate": 1.0,
                "zones": {
                    "A": {
                        "school_district_ratio_by_band": {"low": 1.0, "mid": 1.0, "high": 1.0},
                    },
                    "B": {
                        # 压缩到低/中价带，放大同价位重叠竞争
                        "supply_band_ratio": {"low": 0.72, "mid": 0.24, "high": 0.04},
                        "school_district_ratio_by_band": {"low": 1.0, "mid": 1.0, "high": 1.0},
                    }
                },
            },
            "smart_agent": {
                "price_adjustment_llm_min_duration": 1,
                "price_adjustment_monthly_llm_cap": 200,
                "normal_seller_rule_pricing_enabled": False,
                "classic_competitive_enabled": True,
                "classic_competitive_force_mode": False,
                "force_multi_buyer_pool_enabled": True,
                "force_min_buyers": 10,
                "negotiation_force_batch_mode": True,
                "classic_competitive_max_active_buyers": 8,
                "normal_buyer_backup_slots": 0,
                "buyer_pending_order_cap": 1,
                "order_ttl_days": 45,
                # 触发“多人竞争=>多轮”
                "batch_bidding_second_round_enabled": True,
                "batch_bidding_second_round_min_candidates": 2,
                "batch_bidding_second_round_top_n": 5,
                "batch_bidding_rounds_equal_candidate_count_enabled": True,
                "batch_bidding_min_rounds_on_competition": 4,
                "batch_bidding_rebid_max_rounds": 8,
                "batch_bidding_rebid_min_increment_abs": 1.0,
                "batch_bidding_rounds_cap": 12,
                "batch_bidding_max_competition_pool": 12,
                # 门禁触发：先让多人进同房竞争，再看是否进入多轮。
                "candidate_force_full_visible_pool_enabled": True,
                "candidate_shortlist_rotation_step": 0,
                "candidate_top_k": 1,
                "candidate_exploration_slots": 0,
                "candidate_pressure_penalty_step": 0.0,
                "candidate_pressure_penalty_cap": 0.0,
                "candidate_pressure_bonus_step": 0.0,
                "candidate_pressure_bonus_cap": 0.0,
                "candidate_crowd_hard_exclude_enabled": False,
                "candidate_crowd_precheck_reselect_enabled": False,
                "candidate_crowd_pressure_scale": 1.10,
                "candidate_crowd_tolerance_avoid": 0.95,
                "candidate_crowd_tolerance_neutral": 0.98,
                "candidate_crowd_tolerance_follow": 1.00,
                "gate_force_multi_buyer_pool_enabled": True,
                "gate_force_min_buyers": 10,
                # 适度压制高预算碾压，避免一边倒秒杀
                "classic_queue_high_headroom_penalty_enabled": True,
                "classic_queue_high_headroom_threshold_ratio": 0.10,
                "classic_queue_high_headroom_penalty_span": 0.35,
                "classic_queue_high_headroom_penalty_max": 0.40,
            },
            "personality_weights": {
                "aggressive": 0.52,
                "conservative": 0.20,
                "balanced": 0.28,
            },
            "user_agent_config": {
                "ultra_high": {"count": 1, "income_range": (150000, 280000), "property_count": (2, 3)},
                "high": {"count": 3, "income_range": (80000, 150000), "property_count": (1, 2)},
                "middle": {"count": 6, "income_range": (40000, 80000), "property_count": (0, 1)},
                "low_mid": {"count": 5, "income_range": (20000, 40000), "property_count": (0, 1)},
                "low": {"count": 5, "income_range": (8000, 20000), "property_count": (0, 0)},
            },
        },
    },
}


def materialize_agent_config(raw: Dict[str, Dict[str, Any]], baseline: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for tier_key, base_item in baseline.items():
        patch = raw.get(tier_key, {})
        merged[tier_key] = {
            "count": int(patch.get("count", base_item["count"])),
            "income_range": tuple(patch.get("income_range", base_item["income_range"])),
            "property_count": tuple(patch.get("property_count", base_item["property_count"])),
        }
    return merged


def build_experiment_config(group_key: str, seed: int) -> Dict[str, Any]:
    group = FIRST_ROUND_GROUPS[group_key]
    merged = deep_merge(BASELINE_EXPERIMENT, group["overrides"])
    merged["group_key"] = group_key
    merged["group_name"] = group["name"]
    merged["seed"] = seed
    merged["user_agent_config"] = materialize_agent_config(
        merged["user_agent_config"],
        BASELINE_EXPERIMENT["user_agent_config"],
    )
    return merged


def _build_init_snapshot_signature(exp: Dict[str, Any], seed: int) -> str:
    payload = {
        "seed": int(seed),
        "agent_count": int(exp["agent_count"]),
        "property_count": int(exp["property_count"]),
        "user_agent_config": exp["user_agent_config"],
        "zone_price_ranges": exp["zone_price_ranges"],
        "zone_rents": exp["zone_rents"],
        "base_year": int(exp["base_year"]),
        "seed_existing_mortgage_ratio": float(exp["market_pulse"]["seed_existing_mortgage_ratio"]),
        "mortgage": exp["mortgage"],
        # Snapshot content depends on initialization-time agent fields.
        # Include smart_agent config to prevent cross-group reuse (e.g., R2A vs R2B info_delay).
        "smart_agent_init": exp.get("smart_agent", {}),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()


def ensure_init_snapshot(
    exp: Dict[str, Any],
    seed: int,
    snapshot_root: Path,
) -> Tuple[Path, Path, Path]:
    """
    Build or reuse month-0 initialization snapshot:
    - agents/properties/mortgage seed are generated once
    - later experiment runs copy this DB and resume from month1
    """
    signature = _build_init_snapshot_signature(exp, seed)
    snapshot_dir = snapshot_root / f"init_{signature[:12]}_s{int(seed)}_a{int(exp['agent_count'])}_p{int(exp['property_count'])}"
    snapshot_db = snapshot_dir / "simulation.db"
    snapshot_cfg = snapshot_dir / "config.yaml"
    snapshot_meta = snapshot_dir / "snapshot_meta.json"

    def _snapshot_healthy(db_path: Path, expected_agents: int, expected_props: int) -> bool:
        if not db_path.exists():
            return False
        try:
            conn = sqlite3.connect(str(db_path))
            cur = conn.cursor()
            agents = int(cur.execute("SELECT COUNT(*) FROM agents_static").fetchone()[0] or 0)
            props = int(cur.execute("SELECT COUNT(*) FROM properties_static").fetchone()[0] or 0)
            conn.close()
        except Exception:
            return False
        # allow +1 developer/system agent, but reject obviously broken snapshots
        min_agents = max(1, int(expected_agents * 0.9))
        return agents >= min_agents and props >= int(expected_props)

    if snapshot_db.exists() and snapshot_cfg.exists():
        if _snapshot_healthy(snapshot_db, int(exp["agent_count"]), int(exp["property_count"])):
            return snapshot_dir, snapshot_cfg, snapshot_db
        # bad snapshot: rebuild in place
        try:
            shutil.rmtree(snapshot_dir, ignore_errors=True)
        except Exception:
            pass

    snapshot_dir.mkdir(parents=True, exist_ok=True)
    baseline_cfg = ROOT / "config" / "baseline.yaml"
    if baseline_cfg.exists():
        shutil.copy2(str(baseline_cfg), str(snapshot_cfg))
    else:
        snapshot_cfg.write_text("# baseline missing\n", encoding="utf-8")

    config = SimulationConfig(str(snapshot_cfg))
    apply_overrides(config, exp, seed)
    config.save()

    runner = SimulationRunner(
        agent_count=int(exp["agent_count"]),
        months=int(exp["months"]),
        seed=int(seed),
        resume=False,
        config=config,
        db_path=str(snapshot_db),
    )
    try:
        runner.initialize()
    finally:
        runner.close()

    snapshot_meta.write_text(
        json.dumps(
            {
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "signature": signature,
                "seed": int(seed),
                "agent_count": int(exp["agent_count"]),
                "property_count": int(exp["property_count"]),
                "snapshot_dir": str(snapshot_dir.resolve()),
                "db_path": str(snapshot_db.resolve()),
                "config_path": str(snapshot_cfg.resolve()),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return snapshot_dir, snapshot_cfg, snapshot_db


def apply_overrides(config: SimulationConfig, exp: Dict[str, Any], seed: int) -> None:
    config.update("simulation.random_seed", int(seed))
    config.update("simulation.base_year", int(exp["base_year"]))
    config.update("simulation.enable_intervention_panel", bool(exp["enable_intervention_panel"]))
    config.update("simulation.agent.income_adjustment_rate", float(exp["income_adjustment_rate"]))
    config.update(
        "decision_factors.activation.min_cash_observer_no_property",
        int(exp["min_cash_observer_threshold"]),
    )
    config.update("mortgage.down_payment_ratio", float(exp["mortgage"]["down_payment_ratio"]))
    config.update("mortgage.max_dti_ratio", float(exp["mortgage"]["max_dti_ratio"]))
    config.update("mortgage.annual_interest_rate", float(exp["mortgage"]["annual_interest_rate"]))
    market_overrides = exp.get("market", {}) or {}
    if isinstance(market_overrides, dict):
        if "initial_listing_rate" in market_overrides:
            config.update("market.initial_listing_rate", float(market_overrides["initial_listing_rate"]))
        zones_override = market_overrides.get("zones", {}) or {}
        if isinstance(zones_override, dict):
            for zone, zone_patch in zones_override.items():
                if not isinstance(zone_patch, dict):
                    continue
                ratio_patch = zone_patch.get("supply_band_ratio", {}) or {}
                if isinstance(ratio_patch, dict):
                    for band in ("low", "mid", "high"):
                        if band in ratio_patch:
                            config.update(
                                f"market.zones.{zone}.supply_band_ratio.{band}",
                                float(ratio_patch[band]),
                            )
                school_patch = zone_patch.get("school_district_ratio_by_band", {}) or {}
                if isinstance(school_patch, dict):
                    for band in ("low", "mid", "high"):
                        if band in school_patch:
                            config.update(
                                f"market.zones.{zone}.school_district_ratio_by_band.{band}",
                                float(school_patch[band]),
                            )
    config.update(
        "smart_agent.effective_bid_floor_ratio",
        float(exp["smart_agent"]["effective_bid_floor_ratio"]),
    )
    config.update(
        "smart_agent.precheck_liquidity_buffer_months",
        int(exp["smart_agent"]["precheck_liquidity_buffer_months"]),
    )
    config.update(
        "smart_agent.precheck_include_tax_and_fee",
        bool(exp["smart_agent"]["precheck_include_tax_and_fee"]),
    )
    config.update(
        "smart_agent.price_adjustment_llm_min_duration",
        int(exp["smart_agent"].get("price_adjustment_llm_min_duration", 5)),
    )
    config.update(
        "smart_agent.price_adjustment_monthly_llm_cap",
        int(exp["smart_agent"].get("price_adjustment_monthly_llm_cap", 20)),
    )
    config.update(
        "smart_agent.price_adjustment_model_type",
        str(exp["smart_agent"].get("price_adjustment_model_type", "fast")),
    )
    config.update(
        "smart_agent.price_adjustment_high_heat_model_type",
        str(exp["smart_agent"].get("price_adjustment_high_heat_model_type", "smart")),
    )
    config.update(
        "smart_agent.info_delay_enabled",
        bool(exp["smart_agent"].get("info_delay_enabled", False)),
    )
    config.update(
        "smart_agent.info_delay_ratio",
        float(exp["smart_agent"].get("info_delay_ratio", 0.5)),
    )
    config.update(
        "smart_agent.info_delay_max_months",
        int(exp["smart_agent"].get("info_delay_max_months", 2)),
    )
    config.update(
        "smart_agent.info_delay_ratio_multiplier",
        float(exp["smart_agent"].get("info_delay_ratio_multiplier", 1.0)),
    )
    config.update(
        "smart_agent.info_delay_min_months",
        int(exp["smart_agent"].get("info_delay_min_months", 1)),
    )
    config.update(
        "smart_agent.info_delay_apply_to_normal",
        bool(exp["smart_agent"].get("info_delay_apply_to_normal", False)),
    )
    config.update(
        "smart_agent.info_delay_ratio_normal",
        float(exp["smart_agent"].get("info_delay_ratio_normal", 0.0)),
    )
    config.update(
        "smart_agent.price_adjustment_heat_medium_threshold",
        float(exp["smart_agent"].get("price_adjustment_heat_medium_threshold", 0.16)),
    )
    config.update(
        "smart_agent.price_adjustment_heat_high_threshold",
        float(exp["smart_agent"].get("price_adjustment_heat_high_threshold", 0.36)),
    )
    config.update(
        "smart_agent.price_adjustment_heat_zone_mix_weight",
        float(exp["smart_agent"].get("price_adjustment_heat_zone_mix_weight", 0.35)),
    )
    config.update(
        "smart_agent.price_adjustment_heat_property_floor_on_zone_hot",
        float(exp["smart_agent"].get("price_adjustment_heat_property_floor_on_zone_hot", 0.14)),
    )
    config.update(
        "smart_agent.price_adjustment_heat_zone_match_norm",
        float(exp["smart_agent"].get("price_adjustment_heat_zone_match_norm", 16.0)),
    )
    config.update(
        "smart_agent.price_adjustment_heat_zone_valid_bid_norm",
        float(exp["smart_agent"].get("price_adjustment_heat_zone_valid_bid_norm", 10.0)),
    )
    config.update(
        "smart_agent.price_adjustment_heat_zone_negotiation_norm",
        float(exp["smart_agent"].get("price_adjustment_heat_zone_negotiation_norm", 8.0)),
    )
    config.update(
        "smart_agent.price_adjustment_heat_zone_outbid_norm",
        float(exp["smart_agent"].get("price_adjustment_heat_zone_outbid_norm", 6.0)),
    )
    config.update(
        "smart_agent.regime_engine_v1_enabled",
        bool(exp["smart_agent"].get("regime_engine_v1_enabled", True)),
    )
    config.update(
        "smart_agent.regime_v1_price_reconsider_enabled",
        bool(exp["smart_agent"].get("regime_v1_price_reconsider_enabled", True)),
    )
    config.update(
        "smart_agent.regime_v1_raise_release_preforce_enabled",
        bool(exp["smart_agent"].get("regime_v1_raise_release_preforce_enabled", True)),
    )
    config.update(
        "smart_agent.regime_v1_raise_release_force_sample_enabled",
        bool(exp["smart_agent"].get("regime_v1_raise_release_force_sample_enabled", True)),
    )
    config.update(
        "smart_agent.regime_v1_raise_release_force_sample_ratio",
        float(exp["smart_agent"].get("regime_v1_raise_release_force_sample_ratio", 0.25)),
    )
    config.update(
        "smart_agent.regime_v1_raise_release_force_sample_min_hits_per_month",
        int(exp["smart_agent"].get("regime_v1_raise_release_force_sample_min_hits_per_month", 0)),
    )
    config.update(
        "smart_agent.regime_v1_raise_release_routing_mode",
        str(exp["smart_agent"].get("regime_v1_raise_release_routing_mode", "ratio")),
    )
    config.update(
        "smart_agent.regime_v1_raise_release_topk_enabled",
        bool(exp["smart_agent"].get("regime_v1_raise_release_topk_enabled", False)),
    )
    config.update(
        "smart_agent.regime_v1_raise_release_topk",
        int(exp["smart_agent"].get("regime_v1_raise_release_topk", 0)),
    )
    config.update(
        "smart_agent.regime_v1_raise_release_min_valid_bids_filter",
        int(exp["smart_agent"].get("regime_v1_raise_release_min_valid_bids_filter", 1)),
    )
    config.update(
        "smart_agent.regime_v1_raise_release_min_outbid_filter",
        int(exp["smart_agent"].get("regime_v1_raise_release_min_outbid_filter", 1)),
    )
    config.update(
        "smart_agent.regime_v1_raise_release_min_negotiation_filter",
        int(exp["smart_agent"].get("regime_v1_raise_release_min_negotiation_filter", 1)),
    )
    config.update(
        "smart_agent.regime_v1_raise_release_exclude_cold_zero_bid",
        bool(exp["smart_agent"].get("regime_v1_raise_release_exclude_cold_zero_bid", True)),
    )
    config.update(
        "smart_agent.price_adjustment_llm_min_calls_per_month",
        int(exp["smart_agent"].get("price_adjustment_llm_min_calls_per_month", 0)),
    )
    config.update(
        "smart_agent.regime_v1_raise_release_hard_guard_enabled",
        bool(exp["smart_agent"].get("regime_v1_raise_release_hard_guard_enabled", True)),
    )
    config.update(
        "smart_agent.regime_v1_raise_release_min_valid_bids",
        int(exp["smart_agent"].get("regime_v1_raise_release_min_valid_bids", 1)),
    )
    config.update(
        "smart_agent.regime_v1_raise_release_min_outbid_losses",
        int(exp["smart_agent"].get("regime_v1_raise_release_min_outbid_losses", 0)),
    )
    config.update(
        "smart_agent.regime_v1_raise_release_max_close_priority_score",
        float(exp["smart_agent"].get("regime_v1_raise_release_max_close_priority_score", 80.0)),
    )
    config.update(
        "smart_agent.regime_v1_hot_signal_lag_compensation_enabled",
        bool(exp["smart_agent"].get("regime_v1_hot_signal_lag_compensation_enabled", False)),
    )
    config.update(
        "smart_agent.regime_v1_hot_signal_lag_compensation_medium_delta_base",
        float(exp["smart_agent"].get("regime_v1_hot_signal_lag_compensation_medium_delta_base", 0.03)),
    )
    config.update(
        "smart_agent.regime_v1_hot_signal_lag_compensation_medium_delta_scale",
        float(exp["smart_agent"].get("regime_v1_hot_signal_lag_compensation_medium_delta_scale", 0.04)),
    )
    config.update(
        "smart_agent.regime_v1_hot_signal_lag_compensation_high_delta_base",
        float(exp["smart_agent"].get("regime_v1_hot_signal_lag_compensation_high_delta_base", 0.02)),
    )
    config.update(
        "smart_agent.regime_v1_hot_signal_lag_compensation_high_delta_scale",
        float(exp["smart_agent"].get("regime_v1_hot_signal_lag_compensation_high_delta_scale", 0.03)),
    )
    config.update(
        "smart_agent.regime_v1_raise_release_default_e_coeff",
        float(exp["smart_agent"].get("regime_v1_raise_release_default_e_coeff", 1.05)),
    )
    config.update(
        "smart_agent.regime_v1_raise_force_rule_path_enabled",
        bool(exp["smart_agent"].get("regime_v1_raise_force_rule_path_enabled", True)),
    )
    config.update(
        "smart_agent.regime_v1_raise_force_rule_coeff",
        float(exp["smart_agent"].get("regime_v1_raise_force_rule_coeff", 1.05)),
    )
    config.update(
        "smart_agent.regime_v1_raise_force_all_early_llm",
        bool(exp["smart_agent"].get("regime_v1_raise_force_all_early_llm", True)),
    )
    config.update(
        "smart_agent.regime_v1_raise_release_cap_reserved",
        int(exp["smart_agent"].get("regime_v1_raise_release_cap_reserved", 10)),
    )
    config.update(
        "smart_agent.regime_v1_raise_release_force_llm_min_duration",
        int(exp["smart_agent"].get("regime_v1_raise_release_force_llm_min_duration", 1)),
    )
    config.update(
        "smart_agent.classic_queue_high_headroom_penalty_enabled",
        bool(exp["smart_agent"].get("classic_queue_high_headroom_penalty_enabled", True)),
    )
    config.update(
        "smart_agent.classic_queue_high_headroom_threshold_ratio",
        float(exp["smart_agent"].get("classic_queue_high_headroom_threshold_ratio", 0.35)),
    )
    config.update(
        "smart_agent.classic_queue_high_headroom_penalty_span",
        float(exp["smart_agent"].get("classic_queue_high_headroom_penalty_span", 0.70)),
    )
    config.update(
        "smart_agent.classic_queue_high_headroom_penalty_max",
        float(exp["smart_agent"].get("classic_queue_high_headroom_penalty_max", 0.16)),
    )
    config.update(
        "smart_agent.classic_queue_b_zone_entry_boost",
        float(exp["smart_agent"].get("classic_queue_b_zone_entry_boost", 0.08)),
    )
    config.update(
        "smart_agent.classic_queue_b_zone_entry_max_headroom_ratio",
        float(exp["smart_agent"].get("classic_queue_b_zone_entry_max_headroom_ratio", 0.25)),
    )
    config.update(
        "smart_agent.classic_queue_b_zone_entry_min_budget_fit",
        float(exp["smart_agent"].get("classic_queue_b_zone_entry_min_budget_fit", 0.55)),
    )
    config.update(
        "smart_agent.classic_queue_b_zone_entry_max_owned_properties",
        int(exp["smart_agent"].get("classic_queue_b_zone_entry_max_owned_properties", 1)),
    )
    config.update(
        "smart_agent.classic_queue_b_zone_non_school_penalty_enabled",
        bool(exp["smart_agent"].get("classic_queue_b_zone_non_school_penalty_enabled", True)),
    )
    config.update(
        "smart_agent.classic_queue_b_zone_non_school_min_headroom_ratio",
        float(exp["smart_agent"].get("classic_queue_b_zone_non_school_min_headroom_ratio", 0.35)),
    )
    config.update(
        "smart_agent.classic_queue_b_zone_non_school_penalty_span",
        float(exp["smart_agent"].get("classic_queue_b_zone_non_school_penalty_span", 0.60)),
    )
    config.update(
        "smart_agent.classic_queue_b_zone_non_school_penalty_max",
        float(exp["smart_agent"].get("classic_queue_b_zone_non_school_penalty_max", 0.10)),
    )
    config.update(
        "smart_agent.classic_queue_b_zone_non_school_owned_threshold",
        int(exp["smart_agent"].get("classic_queue_b_zone_non_school_owned_threshold", 1)),
    )
    config.update(
        "smart_agent.batch_bidding_second_round_enabled",
        bool(exp["smart_agent"].get("batch_bidding_second_round_enabled", True)),
    )
    config.update(
        "smart_agent.batch_bidding_second_round_min_candidates",
        int(exp["smart_agent"].get("batch_bidding_second_round_min_candidates", 2)),
    )
    config.update(
        "smart_agent.batch_bidding_second_round_top_n",
        int(exp["smart_agent"].get("batch_bidding_second_round_top_n", 3)),
    )
    config.update(
        "smart_agent.batch_bidding_rounds_equal_candidate_count_enabled",
        bool(exp["smart_agent"].get("batch_bidding_rounds_equal_candidate_count_enabled", False)),
    )
    config.update(
        "smart_agent.batch_bidding_min_rounds_on_competition",
        int(exp["smart_agent"].get("batch_bidding_min_rounds_on_competition", 3)),
    )
    config.update(
        "smart_agent.batch_bidding_rebid_max_rounds",
        int(exp["smart_agent"].get("batch_bidding_rebid_max_rounds", 3)),
    )
    config.update(
        "smart_agent.batch_bidding_rebid_min_increment_abs",
        float(exp["smart_agent"].get("batch_bidding_rebid_min_increment_abs", 1000.0)),
    )
    config.update(
        "smart_agent.batch_bidding_rounds_cap",
        int(exp["smart_agent"].get("batch_bidding_rounds_cap", 6)),
    )
    config.update(
        "smart_agent.batch_bidding_max_competition_pool",
        int(exp["smart_agent"].get("batch_bidding_max_competition_pool", 6)),
    )
    config.update(
        "smart_agent.candidate_crowd_pressure_scale",
        float(exp["smart_agent"].get("candidate_crowd_pressure_scale", 1.0)),
    )
    config.update(
        "smart_agent.candidate_crowd_tolerance_avoid",
        float(exp["smart_agent"].get("candidate_crowd_tolerance_avoid", 0.65)),
    )
    config.update(
        "smart_agent.candidate_crowd_tolerance_neutral",
        float(exp["smart_agent"].get("candidate_crowd_tolerance_neutral", 0.8)),
    )
    config.update(
        "smart_agent.candidate_crowd_tolerance_follow",
        float(exp["smart_agent"].get("candidate_crowd_tolerance_follow", 0.95)),
    )
    config.update(
        "smart_agent.classic_competitive_enabled",
        bool(exp["smart_agent"].get("classic_competitive_enabled", False)),
    )
    config.update(
        "smart_agent.classic_competitive_force_mode",
        bool(exp["smart_agent"].get("classic_competitive_force_mode", False)),
    )
    config.update(
        "smart_agent.classic_competitive_max_active_buyers",
        int(exp["smart_agent"].get("classic_competitive_max_active_buyers", 6)),
    )
    config.update(
        "smart_agent.normal_buyer_backup_slots",
        int(exp["smart_agent"].get("normal_buyer_backup_slots", 0)),
    )
    config.update(
        "smart_agent.buyer_pending_order_cap",
        int(exp["smart_agent"].get("buyer_pending_order_cap", 1)),
    )
    config.update(
        "smart_agent.normal_seller_rule_pricing_enabled",
        bool(exp["smart_agent"].get("normal_seller_rule_pricing_enabled", True)),
    )
    config.update(
        "smart_agent.negotiation_force_batch_mode",
        bool(exp["smart_agent"].get("negotiation_force_batch_mode", False)),
    )
    config.update(
        "smart_agent.candidate_force_full_visible_pool_enabled",
        bool(exp["smart_agent"].get("candidate_force_full_visible_pool_enabled", False)),
    )
    config.update(
        "smart_agent.candidate_shortlist_rotation_step",
        int(exp["smart_agent"].get("candidate_shortlist_rotation_step", 0)),
    )
    config.update(
        "smart_agent.candidate_top_k",
        int(exp["smart_agent"].get("candidate_top_k", 5)),
    )
    config.update(
        "smart_agent.candidate_exploration_slots",
        int(exp["smart_agent"].get("candidate_exploration_slots", 1)),
    )
    config.update(
        "smart_agent.candidate_crowd_hard_exclude_enabled",
        bool(exp["smart_agent"].get("candidate_crowd_hard_exclude_enabled", True)),
    )
    config.update(
        "smart_agent.candidate_crowd_precheck_reselect_enabled",
        bool(exp["smart_agent"].get("candidate_crowd_precheck_reselect_enabled", True)),
    )
    config.update(
        "smart_agent.candidate_pressure_penalty_step",
        float(exp["smart_agent"].get("candidate_pressure_penalty_step", 0.08)),
    )
    config.update(
        "smart_agent.candidate_pressure_penalty_cap",
        float(exp["smart_agent"].get("candidate_pressure_penalty_cap", 0.30)),
    )
    config.update(
        "smart_agent.candidate_pressure_bonus_step",
        float(exp["smart_agent"].get("candidate_pressure_bonus_step", 0.05)),
    )
    config.update(
        "smart_agent.candidate_pressure_bonus_cap",
        float(exp["smart_agent"].get("candidate_pressure_bonus_cap", 0.20)),
    )
    config.update(
        "smart_agent.gate_force_multi_buyer_pool_enabled",
        bool(exp["smart_agent"].get("gate_force_multi_buyer_pool_enabled", False)),
    )
    config.update(
        "smart_agent.gate_force_min_buyers",
        int(exp["smart_agent"].get("gate_force_min_buyers", 6)),
    )
    config.update(
        "smart_agent.force_multi_buyer_pool_enabled",
        bool(exp["smart_agent"].get("force_multi_buyer_pool_enabled", False)),
    )
    config.update(
        "smart_agent.force_min_buyers",
        int(exp["smart_agent"].get("force_min_buyers", 6)),
    )
    personality_weights = exp.get("personality_weights")
    if isinstance(personality_weights, dict) and personality_weights:
        normalized_weights: Dict[str, float] = {}
        for key in ("aggressive", "conservative", "balanced"):
            if key in personality_weights:
                normalized_weights[key] = float(personality_weights[key])
        if normalized_weights:
            total = float(sum(v for v in normalized_weights.values() if v > 0))
            if total > 0:
                normalized_weights = {k: float(v) / total for k, v in normalized_weights.items()}
                config.update("negotiation.personality_weights", normalized_weights)
    config.update("market_pulse.enabled", bool(exp["market_pulse"]["enabled"]))
    config.update(
        "market_pulse.seed_existing_mortgage_ratio",
        float(exp["market_pulse"]["seed_existing_mortgage_ratio"]),
    )
    # Research/night batch: keep LLM budget focused on market decisions.
    # These are narrative/annotation calls and are not required for control-variable validation.
    config.update("life_events.llm_reasoning_enabled", False)
    config.update("system.market_bulletin.post_settlement_llm_analysis_enabled", False)
    config.update("system.market_bulletin.model_type", "fast")
    config.update("reporting.enable_end_reports", False)
    config.update("system.reporting.portrait_model_type", "fast")
    for zone, price_cfg in exp["zone_price_ranges"].items():
        config.update(f"market.zones.{zone}.price_per_sqm_range.min", int(price_cfg["min"]))
        config.update(f"market.zones.{zone}.price_per_sqm_range.max", int(price_cfg["max"]))
    for zone, rent in exp["zone_rents"].items():
        if zone == "A":
            config.update("market.rental.zone_a_rent_per_sqm", float(rent))
        elif zone == "B":
            config.update("market.rental.zone_b_rent_per_sqm", float(rent))

    config._config["user_agent_config"] = exp["user_agent_config"]
    config._config["user_property_count"] = int(exp["property_count"])
    config._config["enable_llm_portraits"] = False
    config._config["research_experiment"] = {
        "group_key": exp["group_key"],
        "group_name": exp["group_name"],
        "seed": seed,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    # reporting_service currently reads this as attribute fallback.
    setattr(config, "enable_llm_portraits", False)
    config.save()


def run_single_experiment(
    group_key: str,
    seed: int,
    routing_enabled: bool | None = None,
    buyer_match_threshold: float | None = None,
    negotiation_threshold: float | None = None,
    use_init_snapshot: bool = False,
    init_snapshot_root: Optional[Path] = None,
    months_override: int | None = None,
    enable_raise_routing_cli: bool = False,
    raise_routing_mode: str | None = None,
    raise_routing_topk: int | None = None,
    raise_routing_min_valid_bids: int | None = None,
    raise_routing_min_outbid: int | None = None,
    raise_routing_min_negotiations: int | None = None,
    location_scarcity_weight: float | None = None,
    shortlist_location_bonus_weight: float | None = None,
    cross_zone_discount_threshold: float | None = None,
) -> Dict[str, Any]:
    # Prevent in-process cache/retry state from leaking across runs.
    reset_llm_runtime_state(reset_breaker=True)

    exp = build_experiment_config(group_key, seed)
    if months_override is not None:
        exp["months"] = int(max(1, months_override))
        # For short mechanism gates (e.g. 1-2 months), we must allow at least
        # one price-adjustment decision to reach LLM; otherwise E-path cannot
        # be validated at all. This only applies to explicit months override.
        if int(exp["months"]) <= 2:
            cur_min = int(exp["smart_agent"].get("price_adjustment_llm_min_duration", 5))
            exp["smart_agent"]["price_adjustment_llm_min_duration"] = min(cur_min, 1)
    if enable_raise_routing_cli:
        smart = exp.setdefault("smart_agent", {})
        if raise_routing_mode is not None:
            smart["regime_v1_raise_release_routing_mode"] = str(raise_routing_mode).strip().lower()
        if raise_routing_topk is not None:
            smart["regime_v1_raise_release_topk_enabled"] = True
            smart["regime_v1_raise_release_topk"] = int(max(0, raise_routing_topk))
        if raise_routing_min_valid_bids is not None:
            smart["regime_v1_raise_release_min_valid_bids_filter"] = int(max(0, raise_routing_min_valid_bids))
        if raise_routing_min_outbid is not None:
            smart["regime_v1_raise_release_min_outbid_filter"] = int(max(0, raise_routing_min_outbid))
        if raise_routing_min_negotiations is not None:
            smart["regime_v1_raise_release_min_negotiation_filter"] = int(max(0, raise_routing_min_negotiations))
    proj_dir, config_path, db_path = project_manager.create_new_project("config/baseline.yaml")
    run_dir = Path(proj_dir).resolve()
    snapshot_dir = None
    if use_init_snapshot:
        snapshot_root = init_snapshot_root or (ROOT / "results" / "init_snapshots")
        snapshot_dir, snapshot_cfg, snapshot_db = ensure_init_snapshot(exp, seed, snapshot_root=snapshot_root)
        shutil.copy2(str(snapshot_db), str(db_path))
        # Start from snapshot config to avoid baseline drift between snapshot and run.
        shutil.copy2(str(snapshot_cfg), str(config_path))

    config = SimulationConfig(config_path)
    apply_overrides(config, exp, seed)
    if routing_enabled is not None:
        config.update("smart_agent.buyer_match_dual_routing_enabled", bool(routing_enabled))
        config.update("smart_agent.negotiation_dual_routing_enabled", bool(routing_enabled))
    if buyer_match_threshold is not None:
        config.update("smart_agent.buyer_match_gray_score_threshold", float(buyer_match_threshold))
    if negotiation_threshold is not None:
        config.update("smart_agent.negotiation_gray_score_threshold", float(negotiation_threshold))
    if location_scarcity_weight is not None:
        config.update(
            "smart_agent.location_scarcity_weight",
            float(max(0.0, min(0.40, location_scarcity_weight))),
        )
    if shortlist_location_bonus_weight is not None:
        config.update(
            "smart_agent.shortlist_location_bonus_weight",
            float(max(0.0, min(0.80, shortlist_location_bonus_weight))),
        )
    if cross_zone_discount_threshold is not None:
        config.update(
            "smart_agent.cross_zone_discount_threshold",
            float(max(0.0, min(0.60, cross_zone_discount_threshold))),
        )
    config.save()

    started_at = datetime.now()
    t0 = time.perf_counter()
    status = "success"
    error_message = None
    try:
        runner = SimulationRunner(
            agent_count=int(exp["agent_count"]),
            months=int(exp["months"]),
            seed=int(seed),
            resume=bool(use_init_snapshot),
            config=config,
            db_path=db_path,
        )
        runner.run()
        export_data(db_path=db_path, output_dir=proj_dir)
        # Defensive guard: SimulationRunner may swallow runtime exceptions and only log them.
        run_log = run_dir / "run.log"
        if run_log.exists():
            try:
                tail = "\n".join(run_log.read_text(encoding="utf-8", errors="ignore").splitlines()[-400:])
            except Exception:
                tail = ""
            if "Simulation Error:" in tail or "Traceback (most recent call last):" in tail:
                status = "failed"
                error_message = "Simulation Error detected in run.log"
    except Exception as exc:  # pragma: no cover - runtime capture
        status = "failed"
        error_message = repr(exc)
    elapsed_seconds = round(time.perf_counter() - t0, 2)

    def _evaluate_quality_gate(db_file: str, gk: str) -> Dict[str, Any]:
        """
        Quality gate for avoiding false conclusions from "cold samples".
        This does NOT modify LLM decisions; it only marks run evidence quality.
        """
        gate = {
            "enabled": False,
            "passed": True,
            "reason": "gate_disabled",
            "metrics": {},
        }
        if gk not in {"I2", "R2A", "R2B"}:
            return gate
        gate["enabled"] = True
        try:
            conn = sqlite3.connect(db_file)
            cur = conn.cursor()
            buyer_match_logs = int(
                (cur.execute("SELECT COUNT(*) FROM decision_logs WHERE event_type='BUYER_MATCH'").fetchone() or [0])[0] or 0
            )
            buyer_match_with_selection = int(
                (
                    cur.execute(
                        "SELECT COUNT(*) FROM decision_logs "
                        "WHERE event_type='BUYER_MATCH' "
                        "AND reason LIKE 'selected=[%' "
                        "AND reason<>'selected=[]'"
                    ).fetchone()
                    or [0]
                )[0]
                or 0
            )
            matches = int((cur.execute("SELECT COUNT(*) FROM property_buyer_matches").fetchone() or [0])[0] or 0)
            transactions = int((cur.execute("SELECT COUNT(*) FROM transactions").fetchone() or [0])[0] or 0)
            pa_total = int(
                (
                    cur.execute(
                        "SELECT COUNT(*) FROM decision_logs WHERE event_type='PRICE_ADJUSTMENT'"
                    ).fetchone()
                    or [0]
                )[0]
                or 0
            )
            ef_total = int(
                (
                    cur.execute(
                        "SELECT COUNT(*) FROM decision_logs "
                        "WHERE event_type='PRICE_ADJUSTMENT' AND decision IN ('E','F')"
                    ).fetchone()
                    or [0]
                )[0]
                or 0
            )
            pa_llm_total = int(
                (
                    cur.execute(
                        "SELECT COUNT(*) FROM decision_logs "
                        "WHERE event_type='PRICE_ADJUSTMENT' AND COALESCE(llm_called,0)=1"
                    ).fetchone()
                    or [0]
                )[0]
                or 0
            )
            ef_llm_total = int(
                (
                    cur.execute(
                        "SELECT COUNT(*) FROM decision_logs "
                        "WHERE event_type='PRICE_ADJUSTMENT' AND decision IN ('E','F') "
                        "AND COALESCE(llm_called,0)=1"
                    ).fetchone()
                    or [0]
                )[0]
                or 0
            )
            conn.close()
        except Exception as exc:
            gate["passed"] = False
            gate["reason"] = f"gate_check_error:{exc}"
            gate["metrics"] = {}
            return gate

        gate["metrics"] = {
            "buyer_match_logs": buyer_match_logs,
            "buyer_match_with_selection": buyer_match_with_selection,
            "matches": matches,
            "transactions": transactions,
            "price_adjustment_total": pa_total,
            "ef_total": ef_total,
            "price_adjustment_llm_total": pa_llm_total,
            "ef_llm_total": ef_llm_total,
        }
        if gk in {"R2A", "R2B"}:
            # Dual gate:
            # 1) total E/F must be non-zero
            # 2) E/F among llm-called price-adjustments must also be non-zero
            # This avoids false "pass" caused by non-LLM fallback-only outcomes.
            if pa_total <= 0:
                gate["passed"] = False
                gate["reason"] = "no_price_adjustment_rows"
            elif ef_total <= 0:
                gate["passed"] = False
                gate["reason"] = "ef_zero_total"
            elif pa_llm_total <= 0:
                gate["passed"] = False
                gate["reason"] = "no_llm_price_adjustment_rows"
            elif ef_llm_total <= 0:
                gate["passed"] = False
                gate["reason"] = "ef_zero_in_llm_subset"
            else:
                gate["passed"] = True
                gate["reason"] = "ok_dual_gate"
        else:
            # I2 keeps original buyer-matching gate.
            if buyer_match_logs <= 0 or buyer_match_with_selection <= 0 or matches <= 0:
                gate["passed"] = False
                gate["reason"] = "cold_sample_no_effective_buyer_matching"
            else:
                gate["passed"] = True
                gate["reason"] = "ok"
        return gate

    quality_gate = _evaluate_quality_gate(db_path, group_key)
    marker_payload = {
        "group_key": group_key,
        "group_name": exp["group_name"],
        "seed": seed,
        "months": int(exp["months"]),
        "agent_count": int(exp["agent_count"]),
        "property_count": int(exp["property_count"]),
        "market_pulse_enabled": bool(exp["market_pulse"]["enabled"]),
        "info_delay_enabled": bool(exp["smart_agent"].get("info_delay_enabled", False)),
        "info_delay_ratio": float(exp["smart_agent"].get("info_delay_ratio", 0.5)),
        "info_delay_max_months": int(exp["smart_agent"].get("info_delay_max_months", 2)),
        "month_end_bulletin_llm_analysis": False,
        "end_reports_enabled": False,
        "status": status,
        "started_at": started_at.isoformat(timespec="seconds"),
        "elapsed_seconds": elapsed_seconds,
        "db_path": str(Path(db_path).resolve()),
        "config_path": str(Path(config_path).resolve()),
        "init_snapshot_used": bool(use_init_snapshot),
        "init_snapshot_dir": str(snapshot_dir.resolve()) if snapshot_dir else None,
        "quality_gate_enabled": bool(quality_gate.get("enabled", False)),
        "quality_gate_passed": bool(quality_gate.get("passed", True)),
        "quality_gate_reason": str(quality_gate.get("reason", "")),
        "quality_gate_metrics": dict(quality_gate.get("metrics", {})),
    }
    (run_dir / "experiment_marker.json").write_text(
        json.dumps(marker_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (run_dir / "experiment_marker.md").write_text(
        "\n".join(
            [
                "# 实验标记",
                "",
                f"- 组别: {exp['group_name']} ({group_key})",
                f"- 种子: {seed}",
                f"- 月数: {int(exp['months'])}",
                f"- Agent 数: {int(exp['agent_count'])}",
                f"- 房产数: {int(exp['property_count'])}",
                f"- Market Pulse: {'开' if exp['market_pulse']['enabled'] else '关'}",
                f"- 信息时滞开关: {'开' if exp['smart_agent'].get('info_delay_enabled', False) else '关'}",
                f"- 信息时滞比例: {float(exp['smart_agent'].get('info_delay_ratio', 0.5))}",
                f"- 信息最大滞后月数: {int(exp['smart_agent'].get('info_delay_max_months', 2))}",
                "- 月末市场公报LLM点评: 关（降耗）",
                "- 终局Agent报告: 关（降耗）",
                f"- 初始化快照复用: {'开' if use_init_snapshot else '关'}",
                f"- 初始化快照目录: {snapshot_dir.resolve() if snapshot_dir else '-'}",
                f"- 质量门禁启用: {'开' if quality_gate.get('enabled', False) else '关'}",
                f"- 质量门禁通过: {'是' if quality_gate.get('passed', True) else '否'}",
                f"- 门禁原因: {quality_gate.get('reason', '')}",
                f"- 门禁指标: {json.dumps(quality_gate.get('metrics', {}), ensure_ascii=False)}",
                f"- 状态: {status}",
                f"- 开始时间: {started_at.isoformat(timespec='seconds')}",
                f"- 耗时(秒): {elapsed_seconds}",
                f"- 数据库: {Path(db_path).resolve()}",
                f"- 配置: {Path(config_path).resolve()}",
                "",
            ]
        ),
        encoding="utf-8",
    )

    return {
        "group_key": group_key,
        "group_name": exp["group_name"],
        "seed": seed,
        "status": status,
        "error": error_message,
        "started_at": started_at.isoformat(timespec="seconds"),
        "elapsed_seconds": elapsed_seconds,
        "run_dir": str(run_dir),
        "config_path": str(Path(config_path).resolve()),
        "db_path": str(Path(db_path).resolve()),
        "months": int(exp["months"]),
        "agent_count": int(exp["agent_count"]),
        "property_count": int(exp["property_count"]),
        "market_pulse_enabled": bool(exp["market_pulse"]["enabled"]),
        "info_delay_enabled": bool(exp["smart_agent"].get("info_delay_enabled", False)),
        "info_delay_ratio": float(exp["smart_agent"].get("info_delay_ratio", 0.5)),
        "info_delay_max_months": int(exp["smart_agent"].get("info_delay_max_months", 2)),
        "month_end_bulletin_llm_analysis": False,
        "end_reports_enabled": False,
        "routing_enabled": routing_enabled,
        "buyer_match_gray_score_threshold": buyer_match_threshold,
        "negotiation_gray_score_threshold": negotiation_threshold,
        "init_snapshot_used": bool(use_init_snapshot),
        "init_snapshot_dir": str(snapshot_dir.resolve()) if snapshot_dir else None,
        "quality_gate_enabled": bool(quality_gate.get("enabled", False)),
        "quality_gate_passed": bool(quality_gate.get("passed", True)),
        "quality_gate_reason": str(quality_gate.get("reason", "")),
        "quality_gate_metrics": dict(quality_gate.get("metrics", {})),
    }


def write_batch_summary(batch_dir: Path, runs: List[Dict[str, Any]]) -> None:
    batch_dir.mkdir(parents=True, exist_ok=True)
    summary_json = batch_dir / "batch_summary.json"
    summary_md = batch_dir / "batch_summary.md"

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_count": len(runs),
        "success_count": sum(1 for item in runs if item["status"] == "success"),
        "failed_count": sum(1 for item in runs if item["status"] != "success"),
        "runs": runs,
    }
    summary_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# 研究实验批跑结果",
        "",
        f"- 生成时间: {payload['generated_at']}",
        f"- 总轮数: {payload['run_count']}",
        f"- 成功轮数: {payload['success_count']}",
        f"- 失败轮数: {payload['failed_count']}",
        "",
        "| 组别 | 种子 | 状态 | 耗时(秒) | Agent数 | 房产数 | Market Pulse | 运行目录 |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    for item in runs:
        lines.append(
            f"| {item['group_name']} | {item['seed']} | {item['status']} | {item['elapsed_seconds']} | "
            f"{item['agent_count']} | {item['property_count']} | "
            f"{'开' if item['market_pulse_enabled'] else '关'} | {item['run_dir']} |"
        )
        if item.get("quality_gate_enabled", False):
            lines.append(
                f"- 质量门禁: {'通过' if item.get('quality_gate_passed', True) else '未通过'} "
                f"({item.get('quality_gate_reason', '')}) "
                f"{json.dumps(item.get('quality_gate_metrics', {}), ensure_ascii=False)}"
            )
        if item["error"]:
            lines.extend(["", f"- 错误: `{item['error']}`", ""])
    summary_md.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_zone_chain_summary(batch_dir: Path) -> None:
    """自动生成批次链路摘要（含漏斗/动机分层/B区诊断）。"""
    analyzer_script = ROOT / "scripts" / "analyze_experiment_batch.py"
    if not analyzer_script.exists():
        print(f"[warn] 分析脚本不存在，跳过: {analyzer_script}")
        return
    try:
        subprocess.run(
            [sys.executable, str(analyzer_script), str(batch_dir)],
            check=True,
            cwd=str(ROOT),
        )
        print(f"批次链路摘要已写入: {batch_dir / 'zone_chain_summary.md'}")
    except Exception as exc:
        print(f"[warn] 批次链路摘要生成失败: {exc}")


def write_stage_trend_report(batch_dir: Path) -> None:
    """自动生成量价主判断报告。"""
    report_script = ROOT / "scripts" / "generate_stage_trend_report.py"
    if not report_script.exists():
        print(f"[warn] 量价报告脚本不存在，跳过: {report_script}")
        return
    try:
        subprocess.run(
            [sys.executable, str(report_script), str(batch_dir)],
            check=True,
            cwd=str(ROOT),
        )
        print(f"阶段量价报告已写入: {batch_dir / 'stage_trend_report.md'}")
    except Exception as exc:
        print(f"[warn] 阶段量价报告生成失败: {exc}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="运行研究批跑实验")
    parser.add_argument("--group", choices=list(FIRST_ROUND_GROUPS.keys()), help="只运行指定组别")
    parser.add_argument(
        "--groups",
        nargs="+",
        choices=list(FIRST_ROUND_GROUPS.keys()),
        help="按给定顺序运行多个组别",
    )
    parser.add_argument("--seed", type=int, choices=SEEDS, help="只运行指定种子")
    parser.add_argument("--seeds", nargs="+", type=int, choices=SEEDS, help="运行指定多个种子（覆盖 --seed/--repeats）")
    parser.add_argument("--repeats", type=int, default=3, help="每组重复次数，最大 3")
    parser.add_argument("--all-groups", action="store_true", help="运行第一轮全部 5 组实验")
    parser.add_argument(
        "--out-dir",
        default=None,
        help="批次汇总输出目录（默认按规范自动命名）",
    )
    parser.add_argument(
        "--routing-enabled",
        choices=["true", "false"],
        help="是否启用买方初筛+谈判灰区路由（覆盖组配置）",
    )
    parser.add_argument(
        "--buyer-match-threshold",
        type=float,
        help="买方初筛灰区阈值（覆盖组配置）",
    )
    parser.add_argument(
        "--negotiation-threshold",
        type=float,
        help="谈判灰区阈值（覆盖组配置）",
    )
    parser.add_argument(
        "--use-init-snapshot",
        action="store_true",
        help="复用初始化快照（跳过每轮Agent/房产重新生成）",
    )
    parser.add_argument(
        "--init-snapshot-root",
        default=str(ROOT / "results" / "init_snapshots"),
        help="初始化快照目录（默认 results/init_snapshots）",
    )
    parser.add_argument(
        "--months-override",
        type=int,
        help="覆盖组内默认模拟月数（用于机制短测门禁）",
    )
    parser.add_argument(
        "--fail-on-quality-gate",
        action="store_true",
        help="若质量门禁启用且未通过，则批次返回失败（用于门禁阶段自动中止后续长跑）",
    )
    parser.add_argument(
        "--enable-raise-routing-cli",
        action="store_true",
        help="启用提价路由CLI参数覆盖（建议稳定性通过后再启用）",
    )
    parser.add_argument(
        "--raise-routing-mode",
        choices=["ratio", "topk"],
        help="提价导流模式：比例抽样或先筛后排序取前K",
    )
    parser.add_argument(
        "--raise-routing-topk",
        type=int,
        help="topk 模式下的前K名额",
    )
    parser.add_argument(
        "--raise-routing-min-valid-bids",
        type=int,
        help="先筛条件：有效报价最小值",
    )
    parser.add_argument(
        "--raise-routing-min-outbid",
        type=int,
        help="先筛条件：被抢单最小值",
    )
    parser.add_argument(
        "--raise-routing-min-negotiations",
        type=int,
        help="先筛条件：谈判次数最小值",
    )
    parser.add_argument(
        "--location-scarcity-weight",
        type=float,
        help="地段稀缺度权重覆盖（0.0~0.4）",
    )
    parser.add_argument(
        "--shortlist-location-bonus-weight",
        type=float,
        help="候选清单地段加分权重覆盖（0.0~0.8）",
    )
    parser.add_argument(
        "--cross-zone-discount-threshold",
        type=float,
        help="跨区折价阈值覆盖（0.0~0.6）",
    )
    return parser.parse_args()


def _compress_numeric_values(values: List[int]) -> str:
    if not values:
        return "none"
    ordered = sorted(set(int(v) for v in values))
    return "s" + "".join(str(v) for v in ordered)


def build_default_batch_out_dir(
    plan: List[Dict[str, int]],
    use_init_snapshot: bool,
    buyer_match_threshold: float | None,
    negotiation_threshold: float | None,
) -> Path:
    """
    命名规范:
    {stage}_{track}_{topic}_{groups}_{seeds}_{yyyymmdd_hhmmss}
    """
    stage = "p1"
    track = "snap" if use_init_snapshot else "fresh"
    topic = "research"
    if buyer_match_threshold is not None and negotiation_threshold is not None:
        topic = f"th{buyer_match_threshold:.2f}_{negotiation_threshold:.2f}".replace(".", "p")
    elif buyer_match_threshold is not None:
        topic = f"th{buyer_match_threshold:.2f}".replace(".", "p")
    elif negotiation_threshold is not None:
        topic = f"thn{negotiation_threshold:.2f}".replace(".", "p")

    group_keys = [str(item["group"]).lower() for item in plan]
    unique_groups = []
    for g in group_keys:
        if g not in unique_groups:
            unique_groups.append(g)
    groups_part = "".join(unique_groups) if unique_groups else "nogroup"
    seeds_part = _compress_numeric_values([int(item["seed"]) for item in plan])
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_name = f"{stage}_{track}_{topic}_{groups_part}_{seeds_part}_{stamp}"
    return ROOT / "results" / "experiment_batches" / batch_name


def build_run_plan(args: argparse.Namespace) -> List[Dict[str, int]]:
    if args.seeds:
        chosen_seeds = [int(s) for s in args.seeds]
    elif args.seed is not None:
        chosen_seeds = [int(args.seed)]
    else:
        max_repeats = max(1, min(int(args.repeats), len(SEEDS)))
        chosen_seeds = SEEDS[:max_repeats]

    if args.group and args.seed and (not args.seeds):
        return [{"group": args.group, "seed": args.seed}]

    if args.groups:
        plan: List[Dict[str, int]] = []
        for group_key in args.groups:
            for seed in chosen_seeds:
                plan.append({"group": group_key, "seed": seed})
        return plan

    if args.group:
        return [{"group": args.group, "seed": seed} for seed in chosen_seeds]

    if args.all_groups:
        plan: List[Dict[str, int]] = []
        for group_key in ["A", "B", "C", "D", "E"]:
            for seed in chosen_seeds:
                plan.append({"group": group_key, "seed": seed})
        return plan

    # 默认只跑最关键的两轮基准组，便于先看耗时和稳定性
    return [{"group": "A", "seed": 101}, {"group": "A", "seed": 202}]


def main() -> int:
    args = parse_args()
    plan = build_run_plan(args)
    routing_enabled = None
    if args.routing_enabled is not None:
        routing_enabled = str(args.routing_enabled).strip().lower() == "true"
    buyer_match_threshold = args.buyer_match_threshold
    negotiation_threshold = args.negotiation_threshold
    init_snapshot_root = Path(args.init_snapshot_root).resolve()
    months_override = args.months_override
    enable_raise_routing_cli = bool(args.enable_raise_routing_cli)
    raise_routing_mode = args.raise_routing_mode
    raise_routing_topk = args.raise_routing_topk
    raise_routing_min_valid_bids = args.raise_routing_min_valid_bids
    raise_routing_min_outbid = args.raise_routing_min_outbid
    raise_routing_min_negotiations = args.raise_routing_min_negotiations
    location_scarcity_weight = args.location_scarcity_weight
    shortlist_location_bonus_weight = args.shortlist_location_bonus_weight
    cross_zone_discount_threshold = args.cross_zone_discount_threshold
    if args.out_dir:
        out_dir = Path(args.out_dir)
    else:
        out_dir = build_default_batch_out_dir(
            plan=plan,
            use_init_snapshot=bool(args.use_init_snapshot),
            buyer_match_threshold=buyer_match_threshold,
            negotiation_threshold=negotiation_threshold,
        )
    if args.use_init_snapshot:
        print(f"- 初始化快照复用: 开 ({init_snapshot_root})")
    else:
        print("- 初始化快照复用: 关")
    if months_override is not None:
        print(f"- 月数覆盖: {int(months_override)}")
    if args.fail_on_quality_gate:
        print("- 质量门禁失败即中止: 开")
    if enable_raise_routing_cli:
        print("- 提价路由CLI覆盖: 开（稳定后建议启用）")
    elif any(
        v is not None
        for v in (
            raise_routing_mode,
            raise_routing_topk,
            raise_routing_min_valid_bids,
            raise_routing_min_outbid,
            raise_routing_min_negotiations,
        )
    ):
        print("- 提价路由CLI覆盖: 关（已忽略相关参数；如需生效请加 --enable-raise-routing-cli）")
    print(f"- 批次输出目录: {out_dir}")

    print("研究实验批跑计划：")
    for item in plan:
        print(f"- {FIRST_ROUND_GROUPS[item['group']]['name']} / seed {item['seed']}")
    if routing_enabled is not None:
        print(f"- 路由开关覆盖: {routing_enabled}")
    if buyer_match_threshold is not None:
        print(f"- 买方初筛阈值覆盖: {buyer_match_threshold}")
    if negotiation_threshold is not None:
        print(f"- 谈判阈值覆盖: {negotiation_threshold}")
    if enable_raise_routing_cli:
        if raise_routing_mode is not None:
            print(f"- 提价导流模式覆盖: {raise_routing_mode}")
        if raise_routing_topk is not None:
            print(f"- 提价导流TopK覆盖: {raise_routing_topk}")
        if raise_routing_min_valid_bids is not None:
            print(f"- 提价导流筛选(有效报价)覆盖: {raise_routing_min_valid_bids}")
        if raise_routing_min_outbid is not None:
            print(f"- 提价导流筛选(被抢单)覆盖: {raise_routing_min_outbid}")
        if raise_routing_min_negotiations is not None:
            print(f"- 提价导流筛选(谈判次数)覆盖: {raise_routing_min_negotiations}")
    if location_scarcity_weight is not None:
        print(f"- 地段稀缺度权重覆盖: {location_scarcity_weight}")
    if shortlist_location_bonus_weight is not None:
        print(f"- 候选清单地段加分权重覆盖: {shortlist_location_bonus_weight}")
    if cross_zone_discount_threshold is not None:
        print(f"- 跨区折价阈值覆盖: {cross_zone_discount_threshold}")

    results: List[Dict[str, Any]] = []
    for item in plan:
        print(
            f"\n=== 开始运行 {FIRST_ROUND_GROUPS[item['group']]['name']} / seed {item['seed']} ===",
            flush=True,
        )
        run_result = run_single_experiment(
            item["group"],
            item["seed"],
            routing_enabled=routing_enabled,
            buyer_match_threshold=buyer_match_threshold,
            negotiation_threshold=negotiation_threshold,
            use_init_snapshot=bool(args.use_init_snapshot),
            init_snapshot_root=init_snapshot_root,
            months_override=months_override,
            enable_raise_routing_cli=enable_raise_routing_cli,
            raise_routing_mode=raise_routing_mode,
            raise_routing_topk=raise_routing_topk,
            raise_routing_min_valid_bids=raise_routing_min_valid_bids,
            raise_routing_min_outbid=raise_routing_min_outbid,
            raise_routing_min_negotiations=raise_routing_min_negotiations,
            location_scarcity_weight=location_scarcity_weight,
            shortlist_location_bonus_weight=shortlist_location_bonus_weight,
            cross_zone_discount_threshold=cross_zone_discount_threshold,
        )
        results.append(run_result)
        print(
            f"=== 完成: 状态={run_result['status']} 耗时={run_result['elapsed_seconds']}秒 "
            f"run_dir={run_result['run_dir']} ===",
            flush=True,
        )

    write_batch_summary(out_dir, results)
    write_zone_chain_summary(out_dir)
    write_stage_trend_report(out_dir)
    print(f"\n批次汇总已写入: {out_dir}")
    all_success = all(item["status"] == "success" for item in results)
    if not all_success:
        return 1

    if args.fail_on_quality_gate:
        for item in results:
            if item.get("quality_gate_enabled", False) and (not item.get("quality_gate_passed", True)):
                print(
                    f"[gate-fail] {item.get('group_name')} / seed {item.get('seed')} "
                    f"未通过质量门禁: {item.get('quality_gate_reason')}"
                )
                return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
