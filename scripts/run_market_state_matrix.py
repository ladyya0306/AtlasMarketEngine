#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
市场状态变量最小矩阵脚本

目标：
1. 用固定主链 + 快照初始化，正式运行 V1 / V2 / V3：
   - V1: 平衡市场基线
   - V2: 买方市场
   - V3: 卖方市场
2. 不改交易核心，只通过外生市场状态参数控制供需环境。
3. 复用现有批次摘要与量价报告脚本，统一输出 A/B 区成交套数与均价证据。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.config_loader import SimulationConfig
from simulation_runner import SimulationRunner

NIGHT_RUN = ROOT / "scripts" / "night_run.py"
BASELINE = ROOT / "config" / "baseline.yaml"
STAGE_TREND = ROOT / "scripts" / "generate_stage_trend_report.py"
ZONE_CHAIN = ROOT / "scripts" / "analyze_experiment_batch.py"
ROLE_EXPLAIN = ROOT / "scripts" / "evaluate_role_activation_explainability.py"


def _extract_db_from_stdout(stdout_path: Path) -> Optional[str]:
    if not stdout_path.exists():
        return None
    lines = stdout_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    for ln in reversed(lines):
        if "DB path:" in ln:
            return ln.split("DB path:", 1)[1].strip()
    return None


def _write_yaml(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _validate_formal_info_market_plan(group_id: str, plan: Dict[str, Any]) -> None:
    if group_id not in {"V2B", "V2I", "V3B", "V3I"}:
        return
    smart_cfg = plan.get("smart_agent", {}) or {}
    role_opt = smart_cfg.get("role_decision_optimization", {}) or {}
    missing = []
    if int(smart_cfg.get("activation_batch_size", 0) or 0) != 10:
        missing.append("smart_agent.activation_batch_size=10")
    if role_opt.get("adaptive_batch_size_enabled", None) is not False:
        missing.append("smart_agent.role_decision_optimization.adaptive_batch_size_enabled=false")
    if role_opt.get("enable_model_dual_routing", None) is not False:
        missing.append("smart_agent.role_decision_optimization.enable_model_dual_routing=false")
    if role_opt.get("default_model_type") != "smart":
        missing.append("smart_agent.role_decision_optimization.default_model_type=smart")
    if role_opt.get("gray_model_type") != "smart":
        missing.append("smart_agent.role_decision_optimization.gray_model_type=smart")
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"{group_id} formal info-market plan missing stable activation settings: {joined}")


def _build_init_snapshot_signature(seed: int, agent_count: Optional[int]) -> str:
    payload = {
        "seed": int(seed),
        "agent_count": int(agent_count) if agent_count is not None else None,
        "baseline_path": str(BASELINE.resolve()),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()


def _snapshot_healthy(db_path: Path, expected_agents: Optional[int]) -> bool:
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
    if props <= 0:
        return False
    if expected_agents is None:
        return agents > 0
    return agents >= max(1, int(expected_agents * 0.9))


def ensure_init_snapshot(seed: int, agent_count: Optional[int], snapshot_root: Path) -> Dict[str, str]:
    expected_agents = int(agent_count) if agent_count is not None else int(
        SimulationConfig(str(BASELINE)).get("simulation.agent_count", 50) or 50
    )
    signature = _build_init_snapshot_signature(seed=seed, agent_count=agent_count)
    suffix = f"s{int(seed)}"
    if agent_count is not None:
        suffix += f"_a{int(agent_count)}"
    snapshot_dir = snapshot_root / f"market_state_init_{signature[:12]}_{suffix}"
    snapshot_db = snapshot_dir / "simulation.db"
    snapshot_cfg = snapshot_dir / "config.yaml"
    snapshot_meta = snapshot_dir / "snapshot_meta.json"

    if snapshot_db.exists() and snapshot_cfg.exists() and _snapshot_healthy(snapshot_db, expected_agents):
        return {
            "snapshot_dir": str(snapshot_dir.resolve()),
            "db_path": str(snapshot_db.resolve()),
            "config_path": str(snapshot_cfg.resolve()),
        }

    shutil.rmtree(snapshot_dir, ignore_errors=True)
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(BASELINE), str(snapshot_cfg))

    config = SimulationConfig(str(snapshot_cfg))
    config.update("simulation.random_seed", int(seed))
    config.update("simulation.enable_intervention_panel", False)
    config.save()

    runner = SimulationRunner(
        agent_count=int(agent_count) if agent_count is not None else int(config.get("simulation.agent_count", 50) or 50),
        months=1,
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
                "seed": int(seed),
                "agent_count": int(agent_count) if agent_count is not None else None,
                "snapshot_dir": str(snapshot_dir.resolve()),
                "db_path": str(snapshot_db.resolve()),
                "config_path": str(snapshot_cfg.resolve()),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return {
        "snapshot_dir": str(snapshot_dir.resolve()),
        "db_path": str(snapshot_db.resolve()),
        "config_path": str(snapshot_cfg.resolve()),
    }


def _market_state_plan(group_id: str, seed: int, months: int) -> Dict[str, Any]:
    if group_id == "V1":
        return {
            "simulation": {
                "random_seed": int(seed),
                "months": int(months),
                "enable_intervention_panel": False,
                "agent": {"income_adjustment_rate": 1.00},
            },
            "macro_environment": {"override_mode": "stable"},
            "mortgage": {
                "down_payment_ratio": 0.30,
                "max_dti_ratio": 0.50,
                "annual_interest_rate": 0.035,
            },
            "market": {
                "initial_listing_rate": 0.05,
            },
            "market_pulse": {
                "enabled": False,
                "seed_existing_mortgage_ratio": 0.55,
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
            },
        }
    if group_id == "V2":
        return {
            "simulation": {
                "random_seed": int(seed),
                "months": int(months),
                "enable_intervention_panel": False,
                "agent": {"income_adjustment_rate": 0.98},
            },
            "macro_environment": {"override_mode": "stable"},
            "mortgage": {
                "down_payment_ratio": 0.30,
                "max_dti_ratio": 0.50,
                "annual_interest_rate": 0.036,
            },
            "market": {
                "initial_listing_rate": 0.18,
                "zones": {
                    "B": {
                        "supply_band_ratio": {"low": 0.55, "mid": 0.35, "high": 0.10},
                    }
                },
            },
            "market_pulse": {
                "enabled": True,
                "seed_existing_mortgage_ratio": 0.72,
            },
            "smart_agent": {
                "info_delay_enabled": True,
                "info_delay_ratio": 0.03,
                "info_delay_ratio_multiplier": 1.0,
                "info_delay_min_months": 1,
                "info_delay_apply_to_normal": True,
                "info_delay_ratio_normal": 0.12,
                "info_delay_max_months": 1,
            },
        }
    if group_id == "V2R":
        return {
            "simulation": {
                "random_seed": int(seed),
                "months": int(months),
                "enable_intervention_panel": False,
                "agent": {"income_adjustment_rate": 1.00},
            },
            "macro_environment": {"override_mode": "stable"},
            "mortgage": {
                "down_payment_ratio": 0.30,
                "max_dti_ratio": 0.50,
                "annual_interest_rate": 0.036,
            },
            "market": {
                "initial_listing_rate": 0.12,
                "zones": {
                    "B": {
                        "supply_band_ratio": {"low": 0.58, "mid": 0.32, "high": 0.10},
                    }
                },
            },
            "market_pulse": {
                "enabled": True,
                "seed_existing_mortgage_ratio": 0.64,
            },
            "smart_agent": {
                "info_delay_enabled": True,
                "info_delay_ratio": 0.22,
                "info_delay_ratio_multiplier": 1.0,
                "info_delay_min_months": 1,
                "info_delay_apply_to_normal": True,
                "info_delay_ratio_normal": 0.08,
                "info_delay_max_months": 2,
                "candidate_top_k": 4,
                "candidate_exploration_slots": 0,
            },
        }
    if group_id == "V2S":
        return {
            "simulation": {
                "random_seed": int(seed),
                "months": int(months),
                "enable_intervention_panel": False,
                "agent": {"income_adjustment_rate": 1.00},
            },
            "macro_environment": {"override_mode": "stable"},
            "mortgage": {
                "down_payment_ratio": 0.30,
                "max_dti_ratio": 0.50,
                "annual_interest_rate": 0.036,
            },
            "market": {
                "initial_listing_rate": 0.14,
                "zones": {
                    "B": {
                        "supply_band_ratio": {"low": 0.55, "mid": 0.35, "high": 0.10},
                    }
                },
            },
            "market_pulse": {
                "enabled": True,
                "seed_existing_mortgage_ratio": 0.64,
            },
            "smart_agent": {
                "info_delay_enabled": True,
                "info_delay_ratio": 0.20,
                "info_delay_ratio_multiplier": 1.0,
                "info_delay_min_months": 1,
                "info_delay_apply_to_normal": True,
                "info_delay_ratio_normal": 0.08,
                "info_delay_max_months": 2,
                "candidate_top_k": 5,
                "candidate_exploration_slots": 1,
            },
        }
    if group_id == "V3":
        return {
            "simulation": {
                "random_seed": int(seed),
                "months": int(months),
                "enable_intervention_panel": False,
                "agent": {"income_adjustment_rate": 1.10},
            },
            "macro_environment": {"override_mode": "optimistic"},
            "mortgage": {
                "down_payment_ratio": 0.20,
                "max_dti_ratio": 0.64,
                "annual_interest_rate": 0.0315,
            },
            "market": {
                "initial_listing_rate": 0.038,
            },
            "market_pulse": {
                "enabled": False,
                "seed_existing_mortgage_ratio": 0.46,
            },
            "smart_agent": {
                "info_delay_enabled": False,
                "info_delay_ratio": 0.00,
                "info_delay_max_months": 0,
            },
        }
    if group_id == "V2B":
        return {
            "simulation": {
                "random_seed": int(seed),
                "months": int(months),
                "enable_intervention_panel": False,
                "agent": {"income_adjustment_rate": 1.00},
            },
            "macro_environment": {"override_mode": "stable"},
            "mortgage": {
                "down_payment_ratio": 0.30,
                "max_dti_ratio": 0.50,
                "annual_interest_rate": 0.036,
            },
            "market": {
                "initial_listing_rate": 0.14,
                "zones": {
                    "B": {
                        "supply_band_ratio": {"low": 0.55, "mid": 0.35, "high": 0.10},
                    }
                },
            },
            "market_pulse": {
                "enabled": True,
                "seed_existing_mortgage_ratio": 0.76,
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
            },
        }
    if group_id == "V2I":
        return {
            "simulation": {
                "random_seed": int(seed),
                "months": int(months),
                "enable_intervention_panel": False,
                "agent": {"income_adjustment_rate": 1.00},
            },
            "macro_environment": {"override_mode": "stable"},
            "mortgage": {
                "down_payment_ratio": 0.30,
                "max_dti_ratio": 0.50,
                "annual_interest_rate": 0.036,
            },
            "market": {
                "initial_listing_rate": 0.14,
                "zones": {
                    "B": {
                        "supply_band_ratio": {"low": 0.55, "mid": 0.35, "high": 0.10},
                    }
                },
            },
            "market_pulse": {
                "enabled": True,
                "seed_existing_mortgage_ratio": 0.66,
            },
            "smart_agent": {
                "activation_batch_size": 10,
                "role_decision_optimization": {
                    "adaptive_batch_size_enabled": False,
                    "enable_model_dual_routing": False,
                    "default_model_type": "smart",
                    "gray_model_type": "smart",
                },
                "info_delay_enabled": True,
                "info_delay_ratio": 0.68,
                "info_delay_ratio_multiplier": 1.60,
                "info_delay_min_months": 1,
                "info_delay_apply_to_normal": True,
                "info_delay_ratio_normal": 0.35,
                "info_delay_max_months": 3,
            },
        }
    if group_id == "V3B":
        return {
            "simulation": {
                "random_seed": int(seed),
                "months": int(months),
                "enable_intervention_panel": False,
                "agent": {"income_adjustment_rate": 1.10},
            },
            "macro_environment": {"override_mode": "optimistic"},
            "mortgage": {
                "down_payment_ratio": 0.20,
                "max_dti_ratio": 0.64,
                "annual_interest_rate": 0.0315,
            },
            "market": {
                "initial_listing_rate": 0.038,
            },
            "market_pulse": {
                "enabled": True,
                "seed_existing_mortgage_ratio": 0.62,
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
            },
        }
    if group_id == "V3I":
        return {
            "simulation": {
                "random_seed": int(seed),
                "months": int(months),
                "enable_intervention_panel": False,
                "agent": {"income_adjustment_rate": 1.10},
            },
            "macro_environment": {"override_mode": "optimistic"},
            "mortgage": {
                "down_payment_ratio": 0.20,
                "max_dti_ratio": 0.64,
                "annual_interest_rate": 0.0315,
            },
            "market": {
                "initial_listing_rate": 0.038,
            },
            "market_pulse": {
                "enabled": True,
                "seed_existing_mortgage_ratio": 0.46,
            },
            "smart_agent": {
                "activation_batch_size": 10,
                "role_decision_optimization": {
                    "adaptive_batch_size_enabled": False,
                    "enable_model_dual_routing": False,
                    "default_model_type": "smart",
                    "gray_model_type": "smart",
                },
                "info_delay_enabled": True,
                "info_delay_ratio": 0.68,
                "info_delay_ratio_multiplier": 1.60,
                "info_delay_min_months": 1,
                "info_delay_apply_to_normal": True,
                "info_delay_ratio_normal": 0.35,
                "info_delay_max_months": 3,
            },
        }
    if group_id == "V4":
        developer_supply = []
        for month_i in range(1, int(months) + 1):
            developer_supply.append(
                {
                    "action_type": "developer_supply",
                    "month": int(month_i),
                    "zone": "A",
                    "count": 8,
                    "price_per_sqm": 56000.0,
                    "school_units": 5,
                    "build_year": 2026,
                }
            )
        return {
            "simulation": {
                "random_seed": int(seed),
                "months": int(months),
                "enable_intervention_panel": False,
                "agent": {"income_adjustment_rate": 1.00},
                "preplanned_interventions": developer_supply,
            },
            "macro_environment": {"override_mode": "stable"},
            "mortgage": {
                "down_payment_ratio": 0.30,
                "max_dti_ratio": 0.50,
                "annual_interest_rate": 0.035,
            },
            "market": {
                "initial_listing_rate": 0.05,
                "zones": {
                    "A": {
                        "supply_band_ratio": {"low": 0.18, "mid": 0.42, "high": 0.40},
                    }
                },
            },
            "market_pulse": {
                "enabled": False,
                "seed_existing_mortgage_ratio": 0.55,
            },
            "smart_agent": {
                "info_delay_enabled": False,
                "info_delay_ratio": 0.00,
                "info_delay_max_months": 0,
            },
        }
    if group_id == "V5":
        developer_supply = []
        for month_i in range(1, int(months) + 1):
            developer_supply.append(
                {
                    "action_type": "developer_supply",
                    "month": int(month_i),
                    "zone": "B",
                    "count": 10,
                    "price_per_sqm": 10500.0,
                    "school_units": 2,
                    "build_year": 2026,
                }
            )
        return {
            "simulation": {
                "random_seed": int(seed),
                "months": int(months),
                "enable_intervention_panel": False,
                "agent": {"income_adjustment_rate": 1.00},
                "preplanned_interventions": developer_supply,
            },
            "macro_environment": {"override_mode": "stable"},
            "mortgage": {
                "down_payment_ratio": 0.30,
                "max_dti_ratio": 0.50,
                "annual_interest_rate": 0.035,
            },
            "market": {
                "initial_listing_rate": 0.05,
                "zones": {
                    "B": {
                        "supply_band_ratio": {"low": 0.70, "mid": 0.24, "high": 0.06},
                    }
                },
            },
            "market_pulse": {
                "enabled": False,
                "seed_existing_mortgage_ratio": 0.55,
            },
            "smart_agent": {
                "info_delay_enabled": False,
                "info_delay_ratio": 0.00,
                "info_delay_max_months": 0,
            },
        }
    if group_id == "V6":
        return {
            "simulation": {
                "random_seed": int(seed),
                "months": int(months),
                "enable_intervention_panel": False,
                "agent": {"income_adjustment_rate": 1.15},
            },
            "macro_environment": {"override_mode": "stable"},
            "mortgage": {
                "down_payment_ratio": 0.30,
                "max_dti_ratio": 0.50,
                "annual_interest_rate": 0.035,
            },
            "market": {
                "initial_listing_rate": 0.05,
            },
            "market_pulse": {
                "enabled": False,
                "seed_existing_mortgage_ratio": 0.55,
            },
            "smart_agent": {
                "info_delay_enabled": False,
                "info_delay_ratio": 0.00,
                "info_delay_max_months": 0,
            },
        }
    if group_id == "V7":
        return {
            "simulation": {
                "random_seed": int(seed),
                "months": int(months),
                "enable_intervention_panel": False,
                "agent": {"income_adjustment_rate": 0.85},
            },
            "macro_environment": {"override_mode": "stable"},
            "mortgage": {
                "down_payment_ratio": 0.30,
                "max_dti_ratio": 0.50,
                "annual_interest_rate": 0.035,
            },
            "market": {
                "initial_listing_rate": 0.05,
            },
            "market_pulse": {
                "enabled": False,
                "seed_existing_mortgage_ratio": 0.55,
            },
            "smart_agent": {
                "info_delay_enabled": False,
                "info_delay_ratio": 0.00,
                "info_delay_max_months": 0,
            },
        }
    if group_id == "V8":
        return {
            "simulation": {
                "random_seed": int(seed),
                "months": int(months),
                "enable_intervention_panel": False,
                "agent": {"income_adjustment_rate": 1.00},
            },
            "macro_environment": {"override_mode": "stable"},
            "mortgage": {
                "down_payment_ratio": 0.30,
                "max_dti_ratio": 0.50,
                "annual_interest_rate": 0.035,
            },
            "market": {
                "initial_listing_rate": 0.05,
            },
            "market_pulse": {
                "enabled": True,
                "seed_existing_mortgage_ratio": 0.72,
            },
            "smart_agent": {
                "info_delay_enabled": False,
                "info_delay_ratio": 0.00,
                "info_delay_max_months": 0,
            },
        }
    if group_id == "V9":
        return {
            "simulation": {
                "random_seed": int(seed),
                "months": int(months),
                "enable_intervention_panel": False,
                "agent": {"income_adjustment_rate": 1.00},
            },
            "macro_environment": {"override_mode": "stable"},
            "mortgage": {
                "down_payment_ratio": 0.30,
                "max_dti_ratio": 0.50,
                "annual_interest_rate": 0.035,
            },
            "market": {
                "initial_listing_rate": 0.05,
            },
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
        }
    if group_id in {"V10", "V10H"}:
        developer_supply = []
        for month_i in range(1, int(months) + 1):
            developer_supply.extend(
                [
                    {
                        "action_type": "developer_supply",
                        "month": int(month_i),
                        "zone": "A",
                        "count": 12,
                        "price_per_sqm": 52000.0,
                        "school_units": 6,
                        "build_year": 2026,
                    },
                    {
                        "action_type": "developer_supply",
                        "month": int(month_i),
                        "zone": "B",
                        "count": 14,
                        "price_per_sqm": 26000.0,
                        "school_units": 4,
                        "build_year": 2026,
                    },
                ]
            )
        simulation_block = {
            "random_seed": int(seed),
            "months": int(months),
            "enable_intervention_panel": False,
            "agent": {"income_adjustment_rate": 0.82},
            "preplanned_interventions": developer_supply,
        }
        if group_id == "V10H":
            simulation_block.update(
                {
                    "low_tx_auto_relax_enabled": False,
                    "min_transactions_gate": 3,
                }
            )

        result = {
            "simulation": simulation_block,
            "macro_environment": {"override_mode": "pessimistic"},
            "mortgage": {
                "down_payment_ratio": 0.42,
                "max_dti_ratio": 0.36,
                "annual_interest_rate": 0.052,
            },
            "market": {
                "initial_listing_rate": 0.18,
                "zones": {
                    "A": {
                        "price_per_sqm_range": {"min": 42000, "max": 52000},
                        "supply_band_ratio": {"low": 0.10, "mid": 0.25, "high": 0.65},
                    },
                    "B": {
                        "price_per_sqm_range": {"min": 18000, "max": 30000},
                        "supply_band_ratio": {"low": 0.08, "mid": 0.22, "high": 0.70},
                    },
                },
            },
            "market_pulse": {
                "enabled": True,
                "seed_existing_mortgage_ratio": 0.76,
            },
            "smart_agent": {
                "info_delay_enabled": True,
                "info_delay_ratio": 0.55,
                "info_delay_ratio_multiplier": 1.35,
                "info_delay_min_months": 1,
                "info_delay_apply_to_normal": True,
                "info_delay_ratio_normal": 0.25,
                "info_delay_max_months": 2,
                "candidate_top_k": 5,
                "candidate_exploration_slots": 1,
            },
        }
        if group_id == "V10H":
            result["market_state_note"] = "extreme_buyer_mismatch_without_auto_relax"
        return result
    raise ValueError(f"unknown market state group: {group_id}")


def _run_one(
    group_id: str,
    seed: int,
    months: int,
    out_dir: Path,
    py_exec: str,
    agent_count: Optional[int],
    use_init_snapshot: bool,
    init_snapshot_root: Optional[Path],
) -> Dict[str, Any]:
    case_name = f"{group_id}_s{seed}_m{months}"
    case_dir = out_dir / case_name
    case_dir.mkdir(parents=True, exist_ok=True)

    group_name_map = {
        "V1": "V1_平衡市场基线",
        "V2": "V2_买方市场",
        "V2B": "V2B_买方市场_广播强化",
        "V2I": "V2I_买方市场_信息迟滞放大",
        "V2R": "V2R_买方市场_节奏摩擦校准",
        "V2S": "V2S_买方市场_温和迟滞校准",
        "V3": "V3_卖方市场",
        "V3B": "V3B_卖方市场_广播强化",
        "V3I": "V3I_卖方市场_信息迟滞放大",
        "V4": "V4_高价新增供给",
        "V5": "V5_低价新增供给",
        "V6": "V6_工资普升",
        "V7": "V7_工资普降",
        "V8": "V8_广播强化",
        "V9": "V9_信息迟滞放大",
        "V10": "V10_极端买方市场错配压力组",
        "V10H": "V10H_极端买方市场错配压力组_关闭低成交自动松绑",
    }
    market_state_map = {
        "V1": "balanced",
        "V2": "buyer_market",
        "V2B": "buyer_market_broadcast",
        "V2I": "buyer_market_info_delay",
        "V2R": "buyer_market_rhythm_friction",
        "V2S": "buyer_market_soft_delay",
        "V3": "seller_market",
        "V3B": "seller_market_broadcast",
        "V3I": "seller_market_info_delay",
        "V4": "high_price_supply",
        "V5": "low_price_supply",
        "V6": "income_up",
        "V7": "income_down",
        "V8": "broadcast_amplified",
        "V9": "info_delay_amplified",
        "V10": "extreme_buyer_mismatch",
        "V10H": "extreme_buyer_mismatch_no_auto_relax",
    }

    plan = _market_state_plan(group_id, seed, months)
    _validate_formal_info_market_plan(group_id, plan)
    plan_path = case_dir / "plan.yaml"
    _write_yaml(plan_path, plan)

    case_db_path = case_dir / "simulation.db"
    base_config_path = BASELINE
    snapshot_info: Dict[str, str] | None = None
    resume = False
    if use_init_snapshot:
        snapshot_root = init_snapshot_root or (ROOT / "results" / "init_snapshots")
        snapshot_info = ensure_init_snapshot(
            seed=int(seed),
            agent_count=int(agent_count) if agent_count is not None else None,
            snapshot_root=snapshot_root,
        )
        shutil.copy2(snapshot_info["db_path"], str(case_db_path))
        copied_cfg = case_dir / "snapshot_base_config.yaml"
        shutil.copy2(snapshot_info["config_path"], str(copied_cfg))
        base_config_path = copied_cfg
        resume = True

    stdout = case_dir / "stdout.log"
    stderr = case_dir / "stderr.log"
    cmd = [
        py_exec,
        str(NIGHT_RUN),
        "--base-config",
        str(base_config_path),
        "--plan",
        str(plan_path),
        "--seed",
        str(seed),
        "--months",
        str(months),
    ]
    if resume:
        cmd += ["--resume", "--db-path", str(case_db_path)]
    if agent_count is not None:
        cmd += ["--agent-count", str(int(agent_count))]

    env = dict(os.environ)
    prev_py = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(ROOT) if not prev_py else (str(ROOT) + ";" + prev_py)

    with stdout.open("w", encoding="utf-8") as out, stderr.open("w", encoding="utf-8") as err:
        proc = subprocess.run(cmd, cwd=str(ROOT), stdout=out, stderr=err, env=env)

    db_path = _extract_db_from_stdout(stdout)
    return {
        "group_key": group_id,
        "group_name": group_name_map[group_id],
        "market_state": market_state_map[group_id],
        "stage": group_id,
        "developer_mode": "NONE",
        "seed": int(seed),
        "status": "success" if proc.returncode == 0 else "failed",
        "exit_code": int(proc.returncode),
        "run_dir": str(case_dir),
        "plan_path": str(plan_path),
        "stdout": str(stdout),
        "stderr": str(stderr),
        "db_path": db_path,
        "months": int(months),
        "agent_count": int(agent_count) if agent_count is not None else None,
        "init_snapshot_used": bool(use_init_snapshot),
        "init_snapshot_dir": snapshot_info["snapshot_dir"] if snapshot_info else None,
    }


def write_batch_summary(out_dir: Path, runs: List[Dict[str, Any]], months: int, seeds: List[int]) -> None:
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "batch_type": "market_state_matrix",
        "months": int(months),
        "seeds": [int(x) for x in seeds],
        "run_count": len(runs),
        "success_count": sum(1 for item in runs if item["status"] == "success"),
        "failed_count": sum(1 for item in runs if item["status"] != "success"),
        "runs": runs,
    }
    (out_dir / "batch_summary.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    lines = [
        "# 市场状态变量矩阵",
        "",
        f"- 生成时间: {payload['generated_at']}",
        f"- 月数: {months}",
        f"- seeds: {', '.join(str(x) for x in seeds)}",
        "",
        "| 组别 | 市场状态 | seed | 状态 | run_dir |",
        "| --- | --- | --- | --- | --- |",
    ]
    for item in runs:
        lines.append(
            f"| {item['group_name']} | {item['market_state']} | {item['seed']} | {item['status']} | {item['run_dir']} |"
        )
    (out_dir / "batch_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _run_post_report(script_path: Path, out_dir: Path, label: str) -> None:
    if not script_path.exists():
        print(f"[warn] {label} 脚本不存在，跳过: {script_path}")
        return
    try:
        subprocess.run(
            [sys.executable, str(script_path), str(out_dir)],
            check=True,
            cwd=str(ROOT),
        )
        print(f"{label}已写入: {out_dir}")
    except Exception as exc:
        print(f"[warn] {label}生成失败: {exc}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run V1/V2/V3 market state matrix.")
    parser.add_argument("--months", type=int, default=6, help="Simulation months for each case.")
    parser.add_argument("--agent-count", type=int, default=None, help="Optional agent count override.")
    parser.add_argument("--seeds", nargs="+", type=int, default=[606, 607, 608], help="Seeds to run.")
    parser.add_argument("--groups", nargs="+", default=["V1", "V2", "V3"], help="Market state groups.")
    parser.add_argument("--out-dir", default="", help="Optional output root directory.")
    parser.add_argument("--use-init-snapshot", action="store_true", help="Reuse a month-0 initialization snapshot for the same seed.")
    parser.add_argument(
        "--init-snapshot-root",
        default=str(ROOT / "results" / "init_snapshots"),
        help="Initialization snapshot directory.",
    )
    args = parser.parse_args()

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir).resolve() if args.out_dir else ROOT / "results" / "market_state_matrix" / f"market_state_matrix_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    py_exec = sys.executable
    runs: List[Dict[str, Any]] = []
    init_snapshot_root = Path(args.init_snapshot_root).resolve()
    for group_id in args.groups:
        for seed in args.seeds:
            result = _run_one(
                group_id=str(group_id),
                seed=int(seed),
                months=int(args.months),
                out_dir=out_dir,
                py_exec=py_exec,
                agent_count=int(args.agent_count) if args.agent_count is not None else None,
                use_init_snapshot=bool(args.use_init_snapshot),
                init_snapshot_root=init_snapshot_root,
            )
            runs.append(result)
            print(f"[{group_id}] seed={seed} exit={result['exit_code']}")

    write_batch_summary(out_dir=out_dir, runs=runs, months=int(args.months), seeds=[int(x) for x in args.seeds])
    _run_post_report(STAGE_TREND, out_dir, "阶段量价报告")
    _run_post_report(ZONE_CHAIN, out_dir, "区域链路摘要")
    _run_post_report(ROLE_EXPLAIN, out_dir, "角色激活可解释性报告")
    print(f"batch={out_dir}")
    return 0 if all(item["status"] == "success" for item in runs) else 1


if __name__ == "__main__":
    raise SystemExit(main())
