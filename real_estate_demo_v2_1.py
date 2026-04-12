#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Oasis Real Estate Simulation Runner (v2.2 Scholar Edition)
增强版：完整的交互式参数配置，包含收入档次、房产分配、市场健康检查
"""
import logging
import json
import os
import random
import sqlite3
import subprocess
import sys
from typing import Any, Dict, Optional

import numpy as np

from config.config_loader import SimulationConfig
from simulation_runner import SimulationRunner


# ✅ LoggerWriter for Tee Logging (Console + File)
# Uses the FileHandler from logging to avoid file locking issues on Windows
class LoggerWriter:
    def __init__(self, writer, file_stream=None):
        self.writer = writer
        self.file_stream = file_stream

    def write(self, message):
        self.writer.write(message)
        if self.file_stream:
            try:
                self.file_stream.write(message)
                self.file_stream.flush()  # Ensure it hits disk
            except BaseException:
                pass

    def flush(self):
        self.writer.flush()
        if self.file_stream:
            try:
                self.file_stream.flush()
            except BaseException:
                pass


# Configure logging first (via SimulationRunner import or explicit config check)
# Since SimulationRunner import configures logging, we can inspect handlers
log_file_stream = None
root_logger = logging.getLogger()
for h in root_logger.handlers:
    if isinstance(h, logging.FileHandler):
        log_file_stream = h.stream
        break

# If no file handler found (e.g. import didn't run it), configure it manually fallback
if not log_file_stream:
    handlers = [logging.StreamHandler()]
    try:
        handlers.insert(0, logging.FileHandler("simulation_run.log", encoding='utf-8', mode='a'))
    except OSError:
        pass
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        handlers=handlers
    )
    for h in logging.getLogger().handlers:
        if isinstance(h, logging.FileHandler):
            log_file_stream = h.stream
            break

# Redirect stdout/stderr
sys.stdout = LoggerWriter(sys.stdout, log_file_stream)
sys.stderr = LoggerWriter(sys.stderr, log_file_stream)


def input_default(prompt, default_value):
    """Helper for input with default value"""
    try:
        val = input(f"{prompt} [default: {default_value}]: ").strip()
        return val if val else str(default_value)
    except EOFError:
        print(f"\n⚠️ 输入流结束，自动使用默认值: {default_value}")
        return str(default_value)
    except KeyboardInterrupt:
        print(f"\n⚠️ 检测到输入中断，自动使用默认值: {default_value}")
        return str(default_value)


def _clamp_float(value, min_v, max_v, fallback):
    """Clamp numeric input into a safe range, fallback when invalid."""
    try:
        v = float(value)
    except Exception:
        return float(fallback)
    if v < min_v or v > max_v:
        return float(fallback)
    return float(v)


def _clamp_int(value, min_v, max_v, fallback):
    try:
        v = int(value)
    except Exception:
        return int(fallback)
    if v < min_v or v > max_v:
        return int(fallback)
    return int(v)


def _input_yes_no(prompt: str, default: bool = False) -> bool:
    default_text = "y" if default else "n"
    value = input_default(prompt, default_text).strip().lower()
    if value in ("y", "yes", "1", "true"):
        return True
    if value in ("n", "no", "0", "false"):
        return False
    return bool(default)


def _print_param_help(title: str, meaning: str, value_range: str, low_high_hint: str) -> None:
    print(f"\n[参数] {title}")
    print(f"  含义: {meaning}")
    print(f"  范围: {value_range}")
    print(f"  极端值提示: {low_high_hint}")


def _prompt_int_param(
    title: str,
    meaning: str,
    value_range: str,
    low_high_hint: str,
    default: int,
    min_v: int,
    max_v: int,
    prompt: Optional[str] = None,
) -> int:
    _print_param_help(title, meaning, value_range, low_high_hint)
    raw = input_default(prompt or title, str(default)).strip()
    return _clamp_int(raw if raw else default, min_v, max_v, default)


def _prompt_float_param(
    title: str,
    meaning: str,
    value_range: str,
    low_high_hint: str,
    default: float,
    min_v: float,
    max_v: float,
    prompt: Optional[str] = None,
) -> float:
    _print_param_help(title, meaning, value_range, low_high_hint)
    raw = input_default(prompt or title, f"{default:.2f}").strip()
    return _clamp_float(raw if raw else default, min_v, max_v, default)


def _read_last_completed_month(db_path: str) -> int:
    if not db_path or not os.path.exists(db_path):
        return 0
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        tables = {
            row[0]
            for row in cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        month_candidates = []
        if "transactions" in tables:
            cur.execute("SELECT MAX(month) FROM transactions")
            row = cur.fetchone()
            month_candidates.append(int(row[0] or 0))
        if "decision_logs" in tables:
            cur.execute("SELECT MAX(month) FROM decision_logs")
            row = cur.fetchone()
            month_candidates.append(int(row[0] or 0))
        if "properties_market" in tables:
            cur.execute("SELECT MAX(listing_month) FROM properties_market")
            row = cur.fetchone()
            month_candidates.append(int(row[0] or 0))
        conn.close()
        return max(month_candidates) if month_candidates else 0
    except Exception:
        return 0


def _safe_scalar_db(db_path: str, sql: str, default=0):
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        conn.close()
        if not row:
            return default
        value = row[0]
        return default if value is None else value
    except Exception:
        return default


def _detect_table_names(db_path: str) -> set[str]:
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        tables = {
            str(row[0])
            for row in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        conn.close()
        return tables
    except Exception:
        return set()


def _market_goal_explainer(market_goal: str) -> str:
    mapping = {
        "balanced": "平衡市场：供给和需求接近，结果更依赖结构分布和链路转化。",
        "buyer_market": "买方市场：供给相对更松，买家有更多替代房，竞争通常更弱。",
        "seller_market": "卖方市场：需求压力更集中，更容易出现热点竞争和被挤出。",
    }
    return mapping.get(market_goal, mapping["balanced"])


def _outcome_hint_from_target(target_r_order_hint: float) -> str:
    if target_r_order_hint < 0.9:
        return "研究目标偏买方环境"
    if target_r_order_hint > 1.1:
        return "研究目标偏卖方环境"
    return "研究目标偏平衡环境"


def _result_market_read(
    transactions_last_month: int,
    for_sale_inventory_now: int,
    active_participants_now: int,
    target_r_order_hint: float,
) -> str:
    if transactions_last_month <= 0:
        if for_sale_inventory_now >= max(12, active_participants_now // 2):
            return "本轮结果更像偏冷的买方环境：库存还在，但成交没有真正放量。"
        if active_participants_now > 0:
            return "本轮结果更像结构卡住的紧平衡环境：有人在场，但订单和成交没有有效转化。"
        return "本轮结果更像低活跃环境：当前没有形成明显的成交热度。"
    if target_r_order_hint > 1.1 and transactions_last_month >= 3:
        return "本轮结果更像卖方环境：需求压力开始转成真实成交。"
    if target_r_order_hint < 0.9 and for_sale_inventory_now > transactions_last_month * 3:
        return "本轮结果更像买方环境：库存相对宽松，成交并不拥挤。"
    return "本轮结果更像平衡附近：已经形成交易，但还没有极端偏热或偏冷。"


def _render_block_title(title: str) -> None:
    print("\n" + "=" * 70)
    print(title.center(70))
    print("=" * 70)


def _print_kv_rows(rows: list[tuple[str, Any]]) -> None:
    if not rows:
        return
    label_width = max(len(str(label)) for label, _ in rows)
    for label, value in rows:
        print(f"{label:<{label_width}} : {value}")


def _bool_tag(value: Any) -> str:
    return "开启" if bool(value) else "关闭"


def _market_goal_title(market_goal: str) -> str:
    mapping = {
        "balanced": "平衡市场",
        "buyer_market": "买方市场",
        "seller_market": "卖方市场",
    }
    return mapping.get(market_goal, "平衡市场")


def build_resume_status_card(db_path: str, config: SimulationConfig) -> Dict[str, Any]:
    completed_month = _read_last_completed_month(db_path)
    tables = _detect_table_names(db_path)
    tx_total = int(_safe_scalar_db(db_path, "SELECT COUNT(*) FROM transactions", 0)) if "transactions" in tables else 0
    tx_last_month = (
        int(_safe_scalar_db(db_path, f"SELECT COUNT(*) FROM transactions WHERE month={completed_month}", 0))
        if "transactions" in tables and completed_month > 0
        else 0
    )
    avg_price_last_month = (
        float(_safe_scalar_db(db_path, f"SELECT AVG(price) FROM transactions WHERE month={completed_month}", 0.0))
        if "transactions" in tables and completed_month > 0
        else 0.0
    )
    for_sale = int(_safe_scalar_db(db_path, "SELECT COUNT(*) FROM properties_market WHERE status='for_sale'", 0)) if "properties_market" in tables else 0
    active_participants = int(_safe_scalar_db(db_path, "SELECT COUNT(*) FROM active_participants", 0)) if "active_participants" in tables else 0
    buyer_side_active = (
        int(
            _safe_scalar_db(
                db_path,
                "SELECT COUNT(*) FROM active_participants WHERE role IN ('BUYER','BUYER_SELLER')",
                0,
            )
        )
        if "active_participants" in tables
        else 0
    )
    seller_side_active = (
        int(
            _safe_scalar_db(
                db_path,
                "SELECT COUNT(*) FROM active_participants WHERE role IN ('SELLER','BUYER_SELLER')",
                0,
            )
        )
        if "active_participants" in tables
        else 0
    )
    scholar_cli = config.get("simulation.scholar_cli", {}) or {}
    return {
        "completed_month": completed_month,
        "transactions_total": tx_total,
        "transactions_last_month": tx_last_month,
        "average_price_last_month": avg_price_last_month,
        "for_sale_inventory_now": for_sale,
        "active_participants_now": active_participants,
        "buyer_side_active_now": buyer_side_active,
        "seller_side_active_now": seller_side_active,
        "market_goal": str(scholar_cli.get("market_goal", "balanced")),
        "target_r_order_hint": float(scholar_cli.get("target_r_order_hint", 1.0) or 1.0),
        "estimated_l0": scholar_cli.get("estimated_l0"),
        "estimated_initial_listing_rate": scholar_cli.get("estimated_initial_listing_rate"),
    }


def print_resume_status_card(db_path: str, config: SimulationConfig) -> None:
    card = build_resume_status_card(db_path, config)
    _render_block_title("续跑前状态摘要卡")
    print("你现在不是从空项目重新开始，而是在上个月的真实状态上继续往后跑。")
    print("下面这张卡展示的是数据库里已经沉淀下来的历史状态。")
    _print_kv_rows(
        [
            ("当前已完成月份", card["completed_month"]),
            ("累计成交", card["transactions_total"]),
            ("上月成交", card["transactions_last_month"]),
            ("当前在售库存", card["for_sale_inventory_now"]),
            ("当前活跃参与者", card["active_participants_now"]),
            ("当前买方侧活跃人数", card["buyer_side_active_now"]),
            ("当前卖方侧活跃人数", card["seller_side_active_now"]),
            ("历史研究标签", f"{card['market_goal']} / {_market_goal_title(card['market_goal'])}"),
            ("历史目标 R_order 提示", f"{card['target_r_order_hint']:.2f}"),
            ("历史估算初始 L0", card["estimated_l0"]),
        ]
    )
    if card["average_price_last_month"] > 0:
        print(f"上月平均成交价         : {card['average_price_last_month']:.0f}")
    print(f"项目数据库             : {db_path}")


def build_scholar_result_card(db_path: str, config: SimulationConfig) -> Dict[str, Any]:
    run_dir = os.path.dirname(db_path)
    tables = _detect_table_names(db_path)
    completed_month = _read_last_completed_month(db_path)
    tx_total = int(_safe_scalar_db(db_path, "SELECT COUNT(*) FROM transactions", 0)) if "transactions" in tables else 0
    tx_buyers = int(_safe_scalar_db(db_path, "SELECT COUNT(DISTINCT buyer_id) FROM transactions", 0)) if "transactions" in tables else 0
    avg_price = float(_safe_scalar_db(db_path, "SELECT AVG(price) FROM transactions", 0.0)) if "transactions" in tables else 0.0
    tx_last_month = (
        int(_safe_scalar_db(db_path, f"SELECT COUNT(*) FROM transactions WHERE month={completed_month}", 0))
        if "transactions" in tables and completed_month > 0
        else 0
    )
    for_sale = int(_safe_scalar_db(db_path, "SELECT COUNT(*) FROM properties_market WHERE status='for_sale'", 0)) if "properties_market" in tables else 0
    active_participants = int(_safe_scalar_db(db_path, "SELECT COUNT(*) FROM active_participants", 0)) if "active_participants" in tables else 0
    scholar_cli = config.get("simulation.scholar_cli", {}) or {}
    market_goal = str(scholar_cli.get("market_goal", "balanced"))
    target_r_order_hint = float(scholar_cli.get("target_r_order_hint", 1.0) or 1.0)
    evidence = {
        "db_path": db_path,
        "config_path": os.path.join(run_dir, "config.yaml"),
        "metadata_path": os.path.join(run_dir, "metadata.json"),
        "log_path": os.path.join(run_dir, "simulation_run.log"),
        "parameter_assumption_report": os.path.join(run_dir, "parameter_assumption_report.md"),
    }
    return {
        "run_dir": run_dir,
        "completed_month": completed_month,
        "market_goal": market_goal,
        "market_goal_explainer": _market_goal_explainer(market_goal),
        "target_r_order_hint": target_r_order_hint,
        "target_outcome_hint": _outcome_hint_from_target(target_r_order_hint),
        "estimated_l0": scholar_cli.get("estimated_l0"),
        "estimated_initial_listing_rate": scholar_cli.get("estimated_initial_listing_rate"),
        "transactions_total": tx_total,
        "transactions_last_month": tx_last_month,
        "distinct_buyers_transacted": tx_buyers,
        "average_transaction_price": avg_price,
        "for_sale_inventory_now": for_sale,
        "active_participants_now": active_participants,
        "result_market_read": _result_market_read(
            transactions_last_month=tx_last_month,
            for_sale_inventory_now=for_sale,
            active_participants_now=active_participants,
            target_r_order_hint=target_r_order_hint,
        ),
        "evidence": evidence,
    }


def render_and_save_scholar_result_card(db_path: str, config: SimulationConfig) -> Dict[str, Any]:
    card = build_scholar_result_card(db_path, config)
    run_dir = card["run_dir"]
    markdown_path = os.path.join(run_dir, "scholar_result_card.md")
    json_path = os.path.join(run_dir, "scholar_result_card.json")
    lines = [
        "# Scholar Result Card",
        "",
        "## 一、这次实验是什么",
        f"- 运行目录: `{run_dir}`",
        f"- 已完成月份: `{card['completed_month']}`",
        f"- 研究目标: `{card['market_goal']}` / `{_market_goal_title(card['market_goal'])}`",
        f"- 目标解释: {card['market_goal_explainer']}",
        f"- 目标订单压力提示: `{card['target_r_order_hint']:.2f}` ({card['target_outcome_hint']})",
        f"- 估算初始可售 L0: `{card['estimated_l0']}`",
        f"- 估算 initial_listing_rate: `{card['estimated_initial_listing_rate']}`",
        "",
        "## 二、先看结论",
        f"- 结果解释: {card['result_market_read']}",
        "",
        "## 三、结果速览",
        f"- 累计成交: `{card['transactions_total']}`",
        f"- 最后一个已完成月份成交: `{card['transactions_last_month']}`",
        f"- 累计成交买家数: `{card['distinct_buyers_transacted']}`",
        f"- 平均成交价: `{card['average_transaction_price']:.0f}`",
        f"- 当前在售库存: `{card['for_sale_inventory_now']}`",
        f"- 当前活跃参与者: `{card['active_participants_now']}`",
        "",
        "## 四、怎么复查",
        "- 先看本文件，快速理解本轮目标和结果。",
        "- 再看 `config.yaml`，确认本轮参数是否按预期写入。",
        "- 再看 `simulation.db`，核验成交、库存、活跃参与者等硬证据。",
        "- 如需追踪过程，再看 `simulation_run.log` 和 `parameter_assumption_report.md`。",
        "",
        "## 五、证据路径",
        f"- 数据库: `{card['evidence']['db_path']}`",
        f"- 配置: `{card['evidence']['config_path']}`",
        f"- 元数据: `{card['evidence']['metadata_path']}`",
        f"- 运行日志: `{card['evidence']['log_path']}`",
        f"- 参数说明: `{card['evidence']['parameter_assumption_report']}`",
    ]
    with open(markdown_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(card, f, ensure_ascii=False, indent=2)

    _render_block_title("本次 Run 结果卡")
    print("先看一句话结论，再看数字，最后去证据目录复查。")
    _print_kv_rows(
        [
            ("研究目标", f"{card['market_goal']} / {_market_goal_title(card['market_goal'])}"),
            ("目标订单压力提示", f"{card['target_r_order_hint']:.2f}"),
            ("估算初始 L0", card["estimated_l0"]),
            ("估算 initial_listing_rate", card["estimated_initial_listing_rate"]),
            ("已完成月份", card["completed_month"]),
            ("累计成交", card["transactions_total"]),
            ("最后月成交", card["transactions_last_month"]),
            ("累计成交买家数", card["distinct_buyers_transacted"]),
            ("平均成交价", f"{card['average_transaction_price']:.0f}"),
            ("当前在售库存", card["for_sale_inventory_now"]),
            ("当前活跃参与者", card["active_participants_now"]),
        ]
    )
    print(f"结果解释               : {card['result_market_read']}")
    print(f"结果卡文件             : {markdown_path}")
    print(f"证据目录               : {run_dir}")
    return card


def _scale_role_defaults(agent_count: int, market_goal: str) -> Dict[str, int]:
    presets = {
        "balanced": {"BUYER": 8, "SELLER": 8, "BUYER_SELLER": 4},
        "buyer_market": {"BUYER": 4, "SELLER": 12, "BUYER_SELLER": 2},
        "seller_market": {"BUYER": 12, "SELLER": 4, "BUYER_SELLER": 6},
    }
    base = presets.get(market_goal, presets["balanced"])
    scale = max(0.2, float(agent_count) / 50.0)
    return {key: max(0, int(round(value * scale))) for key, value in base.items()}


def _choose_profile_pack(market_goal: str) -> str:
    mapping = {
        "balanced": "config/line_b_profiled_market_template_scarce_mid.yaml",
        "buyer_market": "config/line_b_profiled_market_template.yaml",
        "seller_market": "config/line_b_profiled_market_template_scarce_heavy.yaml",
    }
    return mapping.get(market_goal, mapping["balanced"])


def _choose_experiment_mode(market_goal: str) -> str:
    if market_goal == "buyer_market":
        return "abundant"
    return "scarce"


def _estimate_listing_rate(
    property_count: int,
    buyer_quota: int,
    buyer_seller_quota: int,
    target_r_order_hint: float,
) -> Dict[str, float]:
    effective_buyers = max(1, int(buyer_quota) + int(buyer_seller_quota))
    target_ratio = max(0.2, float(target_r_order_hint))
    desired_l0 = max(4, min(int(property_count), int(round(effective_buyers / target_ratio))))
    listing_rate = max(0.02, min(0.85, float(desired_l0) / max(1.0, float(property_count))))
    return {
        "desired_l0": int(desired_l0),
        "listing_rate": float(listing_rate),
    }


def render_scholar_banner() -> None:
    _render_block_title("🏠 Oasis Real Estate Sandbox (Scholar Edition Research Release)")
    print("这是课题线 B 的对外演示入口，重点展示“可控市场推演能力”。")
    print("你在这里输入的关键参数，会写进本次 run 的 config.yaml，并直接作用于本次模拟。")
    print("当前发布边界: 可控市场推演已开放；自然激活属于研究中功能，可切换但不作为本版主卖点。")
    print("\n你现在可以做三类事情:")
    print("1. 新建实验，快速搭一个平衡 / 买方 / 卖方环境。")
    print("2. 继续实验，继承上月 agent 和房源状态往后跑。")
    print("3. 打开研究员工具，对已有项目做体检或夜跑。")


def collect_scholar_new_run_inputs(config: SimulationConfig, seed_to_use: Optional[int]) -> Dict[str, Any]:
    print("\n" + "-" * 70)
    print("Scholar Guided Setup")
    print("-" * 70)
    print("推荐场景:")
    print("1. balanced      平衡市场")
    print("2. buyer_market  买方市场")
    print("3. seller_market 卖方市场")
    goal_raw = input_default("Choose market goal [balanced/buyer_market/seller_market]", "balanced").strip().lower()
    goal_map = {"1": "balanced", "2": "buyer_market", "3": "seller_market"}
    market_goal = goal_map.get(goal_raw, goal_raw if goal_raw in goal_map.values() else "balanced")

    months = _prompt_int_param(
        "模拟月数",
        "本次要跑多少个月。短测适合 1-3 个月，长测适合 6-12 个月。",
        "1-24",
        "越小越快，但可能只看到首月现象；越大越能看到跨月承接和趋势。",
        3,
        1,
        24,
        "Simulation months",
    )
    agent_count = _prompt_int_param(
        "Agent 数量",
        "参与本次市场的总人数。包含潜在买家、卖家和兼具买卖任务的人。",
        "20-500",
        "过低容易受随机波动影响；过高更稳定，但运行更慢。",
        50,
        20,
        500,
        "Total agents",
    )
    property_count = _prompt_int_param(
        "房源总量",
        "初始化阶段生成的总房产数量，不等于当月立刻挂牌的数量。",
        "20-1000",
        "过低会让供给很紧；过高会让市场更冷、更分散。",
        max(40, int(agent_count * 1.2)),
        20,
        1000,
        "Total properties",
    )

    role_defaults = _scale_role_defaults(agent_count, market_goal)
    buyer_quota = _prompt_int_param(
        "强制纯买家人数",
        "本月被直接推进 BUYER 角色的人数。只强制角色，不强制成交。",
        f"0-{agent_count}",
        "越低，需求更弱；越高，会把更多压力推进候选、竞价和支付链路。",
        role_defaults["BUYER"],
        0,
        agent_count,
        "Forced BUYER quota",
    )
    seller_quota = _prompt_int_param(
        "强制纯卖家人数",
        "本月被直接推进 SELLER 角色的人数。",
        f"0-{max(0, agent_count - buyer_quota)}",
        "越低，供给更紧；越高，供给更松，买方更容易有更多候选房。",
        min(role_defaults["SELLER"], max(0, agent_count - buyer_quota)),
        0,
        max(0, agent_count - buyer_quota),
        "Forced SELLER quota",
    )
    max_dual = max(0, agent_count - buyer_quota - seller_quota)
    buyer_seller_quota = _prompt_int_param(
        "强制买卖并行人数",
        "同时承担卖旧房和买新房任务的人数，对置换链和跨月承接很重要。",
        f"0-{max_dual}",
        "越低，链式置换更弱；越高，会增加先卖后买和跨月延续现象。",
        min(role_defaults["BUYER_SELLER"], max_dual),
        0,
        max_dual,
        "Forced BUYER_SELLER quota",
    )
    target_r_order_hint = _prompt_float_param(
        "目标 R_order 提示值",
        "这是研究员给系统的目标订单压力提示，系统会据此反推初始挂牌率和可售底线。",
        "0.20-2.50",
        "低于 1 倾向买方环境；接近 1 更像平衡；高于 1 倾向卖方环境。它是引导值，不是硬保证值。",
        1.00 if market_goal == "balanced" else (0.70 if market_goal == "buyer_market" else 1.30),
        0.20,
        2.50,
        "Target R_order hint",
    )
    income_multiplier = _prompt_float_param(
        "收入倍率",
        "对全体收入做统一乘法。1.00 表示不改，1.20 表示整体收入上浮 20%。",
        "0.50-2.00",
        "过低会压缩购买力；过高会让更多买家进入有效竞争区间。",
        1.00,
        0.50,
        2.00,
        "Income multiplier",
    )
    force_role_months = _prompt_int_param(
        "强制角色生效月数",
        "从第 1 个月开始，连续多少个月启用 forced_role_mode。",
        f"1-{months}",
        "月数越短，越接近首月注入；月数越长，对多月边界影响更持续。",
        min(months, 3),
        1,
        months,
        "Forced role active months",
    )

    profiled_market_mode = _input_yes_no(
        "启用画像供需模式 profiled_market_mode? (推荐 y)",
        True,
    )
    hard_bucket_matcher = _input_yes_no(
        "启用硬 bucket 匹配器 hard_bucket_matcher? (推荐 y)",
        True,
    )
    enable_intervention_panel = _input_yes_no(
        "启用月末人工干预面板 enable_intervention_panel?",
        False,
    )
    open_startup_intervention_menu = _input_yes_no(
        "启动前打开一次人工干预菜单?",
        False,
    )

    listing_plan = _estimate_listing_rate(
        property_count=property_count,
        buyer_quota=buyer_quota,
        buyer_seller_quota=buyer_seller_quota,
        target_r_order_hint=target_r_order_hint,
    )
    profile_pack_path = _choose_profile_pack(market_goal)
    experiment_mode = _choose_experiment_mode(market_goal)

    return {
        "market_goal": market_goal,
        "months": months,
        "agent_count": agent_count,
        "property_count": property_count,
        "buyer_quota": buyer_quota,
        "seller_quota": seller_quota,
        "buyer_seller_quota": buyer_seller_quota,
        "target_r_order_hint": target_r_order_hint,
        "income_multiplier": income_multiplier,
        "force_role_months": force_role_months,
        "profiled_market_mode": profiled_market_mode,
        "hard_bucket_matcher": hard_bucket_matcher,
        "enable_intervention_panel": enable_intervention_panel,
        "open_startup_intervention_menu": open_startup_intervention_menu,
        "profile_pack_path": profile_pack_path,
        "experiment_mode": experiment_mode,
        "listing_plan": listing_plan,
        "seed": seed_to_use,
    }


def apply_scholar_release_config(
    config: SimulationConfig,
    scholar_inputs: Dict[str, Any],
    start_month: int = 1,
) -> None:
    property_count = int(scholar_inputs["property_count"])
    listing_plan = scholar_inputs["listing_plan"]
    desired_l0 = int(listing_plan["desired_l0"])
    listing_rate = float(listing_plan["listing_rate"])
    months_window = int(scholar_inputs["force_role_months"])
    apply_months = list(range(int(start_month), int(start_month) + months_window))

    if scholar_inputs.get("seed") is not None:
        config.update("simulation.random_seed", int(scholar_inputs["seed"]))

    config.update("simulation.months", int(scholar_inputs["months"]))
    config.update("simulation.agent_count", int(scholar_inputs["agent_count"]))
    config._config["user_property_count"] = int(property_count)

    config.update("simulation.enable_intervention_panel", bool(scholar_inputs["enable_intervention_panel"]))
    config.update("simulation.agent.income_adjustment_rate", float(scholar_inputs["income_multiplier"]))
    config.update("market.initial_listing_rate", float(listing_rate))
    config.update("smart_agent.init_min_tradable_floor_total", int(desired_l0))
    config.update("smart_agent.init_min_tradable_ratio_total", round(float(desired_l0) / max(1, property_count), 4))

    config.update("smart_agent.forced_role_mode.enabled", True)
    config.update("smart_agent.forced_role_mode.apply_months", apply_months)
    config.update("smart_agent.forced_role_mode.selection_policy", "affordability_inventory_balanced")
    config.update("smart_agent.forced_role_mode.allow_force_locked_buyers", True)
    config.update("smart_agent.forced_role_mode.quota.buyer", int(scholar_inputs["buyer_quota"]))
    config.update("smart_agent.forced_role_mode.quota.seller", int(scholar_inputs["seller_quota"]))
    config.update("smart_agent.forced_role_mode.quota.buyer_seller", int(scholar_inputs["buyer_seller_quota"]))

    config.update("smart_agent.profiled_market_mode.enabled", bool(scholar_inputs["profiled_market_mode"]))
    config.update("smart_agent.profiled_market_mode.profile_pack_path", str(scholar_inputs["profile_pack_path"]))
    config.update("smart_agent.profiled_market_mode.background_library_path", "config/persona_background_library.json")
    config.update("smart_agent.profiled_market_mode.persona_generation_mode", "code_only")
    config.update("smart_agent.profiled_market_mode.experiment_mode", str(scholar_inputs["experiment_mode"]))
    config.update("smart_agent.profiled_market_mode.hard_bucket_matcher_enabled", bool(scholar_inputs["hard_bucket_matcher"]))
    config.update("smart_agent.profiled_market_mode.startup_bucket_precheck", True)
    config.update("smart_agent.profiled_market_mode.enforce_bucket_alignment", True)

    config.update(
        "simulation.scholar_cli",
        {
            "market_goal": scholar_inputs["market_goal"],
            "target_r_order_hint": float(scholar_inputs["target_r_order_hint"]),
            "estimated_l0": int(desired_l0),
            "estimated_initial_listing_rate": float(listing_rate),
            "open_startup_intervention_menu": bool(scholar_inputs["open_startup_intervention_menu"]),
            "profile_pack_path": str(scholar_inputs["profile_pack_path"]),
            "experiment_mode": str(scholar_inputs["experiment_mode"]),
            "resume_start_month": int(start_month),
        },
    )


def print_scholar_summary(
    scholar_inputs: Dict[str, Any],
    db_path: Optional[str] = None,
    resume_from_month: Optional[int] = None,
) -> None:
    listing_plan = scholar_inputs["listing_plan"]
    target_r_order_hint = float(scholar_inputs["target_r_order_hint"])
    if target_r_order_hint < 0.9:
        market_read = "偏买方环境：供给相对更宽松，买家更容易找到替代房。"
    elif target_r_order_hint > 1.1:
        market_read = "偏卖方环境：需求压力更集中，更容易出现竞争和挤出。"
    else:
        market_read = "偏平衡环境：供需压力接近，结果更依赖结构分布和链路转化。"
    _render_block_title("本次 Scholar 配置摘要 / 市场说明卡")
    print("这张卡回答三个问题：你准备跑什么、系统会怎么理解、结果大概率往哪边推。")
    if db_path:
        print(f"项目数据库: {db_path}")
    if resume_from_month is not None:
        print(f"续跑起点: 已完成到第 {resume_from_month} 月，接下来继续往后跑。")
    _print_kv_rows(
        [
            ("目标市场", f"{scholar_inputs['market_goal']} / {_market_goal_title(scholar_inputs['market_goal'])}"),
            ("模拟月数", scholar_inputs["months"]),
            ("Agent 总数", scholar_inputs["agent_count"]),
            ("房源总量", scholar_inputs["property_count"]),
            (
                "强制角色配额",
                (
                    f"BUYER={scholar_inputs['buyer_quota']}, "
                    f"SELLER={scholar_inputs['seller_quota']}, "
                    f"BUYER_SELLER={scholar_inputs['buyer_seller_quota']}"
                ),
            ),
            ("目标 R_order 提示值", f"{scholar_inputs['target_r_order_hint']:.2f}"),
            ("推导初始可售 L0", f"≈ {listing_plan['desired_l0']}"),
            ("推导 initial_listing_rate", f"≈ {listing_plan['listing_rate']:.3f}"),
            ("市场解读", market_read),
            ("画像供需模式", _bool_tag(scholar_inputs["profiled_market_mode"])),
            ("硬 bucket 匹配器", _bool_tag(scholar_inputs["hard_bucket_matcher"])),
            ("收入倍率", f"{scholar_inputs['income_multiplier']:.2f}"),
            ("强制角色作用月数", scholar_inputs["force_role_months"]),
            ("启动前人工干预菜单", _bool_tag(scholar_inputs["open_startup_intervention_menu"])),
        ]
    )
    print("说明: R_order 提示值只是研究员目标，不是系统承诺值。真实结果仍由候选、支付、竞争和交割链共同决定。")


def collect_scholar_resume_inputs(
    config: SimulationConfig,
    db_path: str,
    seed_to_use: Optional[int],
) -> Dict[str, Any]:
    last_month = _read_last_completed_month(db_path)
    print("\n" + "-" * 70)
    print("Scholar Resume Setup")
    print("-" * 70)
    print_resume_status_card(db_path, config)
    print(f"\n检测到该项目已完成到第 {last_month} 月。")
    print("Resume 模式会继承上月 agent、房源、active_participants 和数据库状态，再继续往后跑。")

    extra_months = _prompt_int_param(
        "追加模拟月数",
        "在现有项目状态基础上，再继续跑多少个月。",
        "1-24",
        "越小越像补跑验证；越大越适合继续做趋势观察。",
        3,
        1,
        24,
        "More months to simulate",
    )
    market_goal = input_default(
        "续跑市场目标标签 [balanced/buyer_market/seller_market]",
        str(config.get("simulation.scholar_cli.market_goal", "balanced")),
    ).strip().lower()
    if market_goal not in {"balanced", "buyer_market", "seller_market"}:
        market_goal = "balanced"

    agent_count_guess = int(config.get("simulation.agent_count", 50) or 50)
    property_count_guess = int(config.get("user_property_count", agent_count_guess) or agent_count_guess)
    role_defaults = {
        "BUYER": int(config.get("smart_agent.forced_role_mode.quota.buyer", 0) or 0),
        "SELLER": int(config.get("smart_agent.forced_role_mode.quota.seller", 0) or 0),
        "BUYER_SELLER": int(config.get("smart_agent.forced_role_mode.quota.buyer_seller", 0) or 0),
    }
    if sum(role_defaults.values()) <= 0:
        role_defaults = _scale_role_defaults(agent_count_guess, market_goal)

    buyer_quota = _prompt_int_param(
        "未来月份强制纯买家人数",
        "从续跑起点之后，对未来月份生效的 BUYER 配额。",
        f"0-{agent_count_guess}",
        "越高，未来月份的需求注入越强。",
        role_defaults["BUYER"],
        0,
        agent_count_guess,
        "Future BUYER quota",
    )
    seller_quota = _prompt_int_param(
        "未来月份强制纯卖家人数",
        "从续跑起点之后，对未来月份生效的 SELLER 配额。",
        f"0-{max(0, agent_count_guess - buyer_quota)}",
        "越高，未来月份供给更松。",
        min(role_defaults["SELLER"], max(0, agent_count_guess - buyer_quota)),
        0,
        max(0, agent_count_guess - buyer_quota),
        "Future SELLER quota",
    )
    max_dual = max(0, agent_count_guess - buyer_quota - seller_quota)
    buyer_seller_quota = _prompt_int_param(
        "未来月份强制买卖并行人数",
        "对未来月份生效的 BUYER_SELLER 配额。",
        f"0-{max_dual}",
        "越高，置换链和跨月承接更强。",
        min(role_defaults["BUYER_SELLER"], max_dual),
        0,
        max_dual,
        "Future BUYER_SELLER quota",
    )
    target_r_order_hint = _prompt_float_param(
        "未来月份目标 R_order 提示值",
        "用于重新估算续跑阶段的供需压力。",
        "0.20-2.50",
        "低于 1 偏买方，高于 1 偏卖方。",
        float(config.get("simulation.scholar_cli.target_r_order_hint", 1.0) or 1.0),
        0.20,
        2.50,
        "Future target R_order hint",
    )
    income_multiplier = _prompt_float_param(
        "未来月份收入倍率",
        "仅影响续跑阶段的收入缩放，不回写历史月份。",
        "0.50-2.00",
        "越低购买力越弱，越高购买力越强。",
        float(config.get("simulation.agent.income_adjustment_rate", 1.0) or 1.0),
        0.50,
        2.00,
        "Future income multiplier",
    )
    future_force_months = _prompt_int_param(
        "未来月份强制角色生效月数",
        "从下一个月开始，连续多少个月启用 forced_role_mode。",
        f"1-{extra_months}",
        "越短越像一次冲击，越长越像持续政策环境。",
        min(extra_months, 3),
        1,
        extra_months,
        "Future forced role active months",
    )
    profiled_market_mode = _input_yes_no(
        "续跑阶段继续启用画像供需模式?",
        bool(config.get("smart_agent.profiled_market_mode.enabled", True)),
    )
    hard_bucket_matcher = _input_yes_no(
        "续跑阶段继续启用硬 bucket 匹配器?",
        bool(config.get("smart_agent.profiled_market_mode.hard_bucket_matcher_enabled", True)),
    )
    enable_intervention_panel = _input_yes_no(
        "续跑阶段启用月末人工干预面板?",
        bool(config.get("simulation.enable_intervention_panel", False)),
    )
    open_startup_intervention_menu = _input_yes_no(
        "续跑前打开一次人工干预菜单?",
        False,
    )

    listing_plan = _estimate_listing_rate(
        property_count=property_count_guess,
        buyer_quota=buyer_quota,
        buyer_seller_quota=buyer_seller_quota,
        target_r_order_hint=target_r_order_hint,
    )

    return {
        "market_goal": market_goal,
        "months": last_month + extra_months,
        "agent_count": agent_count_guess,
        "property_count": property_count_guess,
        "buyer_quota": buyer_quota,
        "seller_quota": seller_quota,
        "buyer_seller_quota": buyer_seller_quota,
        "target_r_order_hint": target_r_order_hint,
        "income_multiplier": income_multiplier,
        "force_role_months": future_force_months,
        "profiled_market_mode": profiled_market_mode,
        "hard_bucket_matcher": hard_bucket_matcher,
        "enable_intervention_panel": enable_intervention_panel,
        "open_startup_intervention_menu": open_startup_intervention_menu,
        "profile_pack_path": _choose_profile_pack(market_goal),
        "experiment_mode": _choose_experiment_mode(market_goal),
        "listing_plan": listing_plan,
        "seed": seed_to_use,
        "resume_from_month": last_month,
    }


def collect_location_scarcity_params(config):
    """
    Optional interactive overrides for location-related shortlist logic.
    Returns dict with three keys:
      - location_scarcity_weight (0.0~0.4)
      - shortlist_location_bonus_weight (0.0~0.8)
      - cross_zone_discount_threshold (0.0~0.6)
    """
    current_location_weight = float(config.get('smart_agent.location_scarcity_weight', 0.12))
    current_shortlist_bonus = float(config.get('smart_agent.shortlist_location_bonus_weight', 0.10))
    current_cross_zone_threshold = float(config.get('smart_agent.cross_zone_discount_threshold', 0.20))

    print("\n--- 地段与跨区参数（可选） ---")
    print("说明：不修改请直接回车，沿用当前配置。")
    print("1) 地段权重：越高越强调核心区稀缺性。")
    print("2) 候选地段加分：越高越容易把核心区房源排进前列。")
    print("3) 跨区折价阈值：越高越不容易因便宜而跨区。")

    w_input = input_default(
        "地段权重 smart_agent.location_scarcity_weight (0.00-0.40)",
        f"{current_location_weight:.2f}",
    ).strip()
    b_input = input_default(
        "候选地段加分 smart_agent.shortlist_location_bonus_weight (0.00-0.80)",
        f"{current_shortlist_bonus:.2f}",
    ).strip()
    t_input = input_default(
        "跨区折价阈值 smart_agent.cross_zone_discount_threshold (0.00-0.60)",
        f"{current_cross_zone_threshold:.2f}",
    ).strip()

    location_weight = _clamp_float(w_input if w_input else current_location_weight, 0.0, 0.4, current_location_weight)
    shortlist_bonus = _clamp_float(b_input if b_input else current_shortlist_bonus, 0.0, 0.8, current_shortlist_bonus)
    cross_zone_threshold = _clamp_float(
        t_input if t_input else current_cross_zone_threshold,
        0.0,
        0.6,
        current_cross_zone_threshold,
    )

    return {
        "location_scarcity_weight": location_weight,
        "shortlist_location_bonus_weight": shortlist_bonus,
        "cross_zone_discount_threshold": cross_zone_threshold,
    }


def collect_preplanned_developer_supply(config):
    """
    Collect optional config-driven developer supply plan (auto-executed in target month).
    Returns list for simulation.preplanned_interventions.
    """
    plans = []
    use_plan = input_default("是否预排开发商投放计划(按月自动执行) [y/N]", "n").strip().lower()
    if use_plan not in ("y", "yes", "1", "true"):
        return plans

    month = int(input_default("投放月份", "6"))
    zone = input_default("投放区域 (A/B)", "A").strip().upper()
    if zone not in ("A", "B"):
        print("⚠️ 区域无效，自动回退 A。")
        zone = "A"
    count = int(input_default("投放数量", "20"))
    price_per_sqm = float(input_default("单价(元/㎡)", "10000"))
    school_units = int(input_default(f"学区房数量(0-{max(0, count)})", "10"))
    if school_units < 0:
        school_units = 0
    if school_units > count:
        school_units = count
    default_base_year = int(config.get("simulation.base_year", 2026))
    build_year = int(input_default("建成年份", str(default_base_year)))
    size_val = input_default("面积(㎡, 回车=随机80-140)", "").strip()
    size = float(size_val) if size_val else None

    plans.append({
        "action_type": "developer_supply",
        "month": int(month),
        "zone": zone,
        "count": int(count),
        "price_per_sqm": float(price_per_sqm),
        "school_units": int(school_units),
        "build_year": int(build_year),
        "size": float(size) if size is not None else None,
    })
    print(f"✅ 已预排: 第{month}月 {zone}区 投放{count}套, {price_per_sqm:.0f}元/㎡, 学区{school_units}套")
    return plans


def collect_benchmark_test_controls(config) -> Dict[str, Any]:
    """
    Collect optional benchmark controls for:
      1) ideal supply baseline
      2) diversified buyer profile baseline
    Returns a dict for both config patching and default CLI behavior.
    """
    controls: Dict[str, Any] = {
        "mode": "custom",
        "apply_ideal_supply": False,
        "apply_profile_diversity": False,
        "config_patches": {},
        "tier_ratio_override": {},
        "property_range_override": {},
    }

    print("\n" + "=" * 60)
    print("【研究基准快捷模式】")
    print("=" * 60)
    print("0) 关闭（保持当前自定义流程）")
    print("1) 理想供给基准（先验证机制能跑通）")
    print("2) 理想供给 + 画像差异化基准（推荐）")
    mode = input_default("请选择模式 [0/1/2]", "2").strip()
    if mode not in ("0", "1", "2"):
        mode = "0"
    if mode == "0":
        return controls

    controls["apply_ideal_supply"] = True
    controls["mode"] = "ideal_supply" if mode == "1" else "ideal_plus_diverse"

    # Ideal-supply recommended baseline (can be adjusted here later if needed).
    controls["config_patches"].update({
        "market.initial_listing_rate": 0.12,
        "smart_agent.init_min_for_sale_floor_by_zone": 12,
        "smart_agent.init_min_for_sale_ratio_by_zone": 0.06,
        "smart_agent.init_min_tradable_floor_total": 30,
        "smart_agent.init_min_tradable_ratio_total": 0.20,
        "market.zones.A.supply_band_ratio.low": 0.30,
        "market.zones.A.supply_band_ratio.mid": 0.45,
        "market.zones.A.supply_band_ratio.high": 0.25,
        "market.zones.B.supply_band_ratio.low": 0.55,
        "market.zones.B.supply_band_ratio.mid": 0.35,
        "market.zones.B.supply_band_ratio.high": 0.10,
        "market.zones.B.school_district_ratio_by_band.low": 0.18,
        "market.zones.B.school_district_ratio_by_band.mid": 0.28,
        "market.zones.B.school_district_ratio_by_band.high": 0.38,
    })

    if mode == "2":
        controls["apply_profile_diversity"] = True
        # More spread default ratio to avoid homogeneous B-demand cohorts.
        controls["tier_ratio_override"] = {
            "ultra_high": 0.08,
            "high": 0.14,
            "middle": 0.38,
            "low_mid": 0.25,
            "low": 0.15,
        }
        # Keep ownership spread realistic while reducing all-cash-rich clustering.
        controls["property_range_override"] = {
            "ultra_high": (1, 3),
            "high": (1, 2),
            "middle": (0, 1),
            "low_mid": (0, 1),
            "low": (0, 0),
        }
    return controls


def validate_config(agent_config, property_count):
    """
    市场健康检查：验证配置是否可能导致0交易

    Returns:
        (is_valid, warnings, errors)
    """
    warnings = []
    errors = []

    # 1. 检查房产总数是否足够
    total_properties_needed = sum(tier['property_count'][1] for tier in agent_config.values())
    if property_count < total_properties_needed:
        errors.append(f"🔴 严重: 房产总数({property_count}) < 各档次房产数之和({total_properties_needed})")
        errors.append(f"   最少需要 {total_properties_needed} 套房产")

    # 2. 检查收入分布（低收入人群不应过多）
    total_agents = sum(tier['count'] for tier in agent_config.values())
    low_income_count = agent_config['low']['count'] + agent_config['low_mid']['count']
    low_income_ratio = low_income_count / total_agents

    if low_income_ratio > 0.7:
        warnings.append(f"🟡 提示: 低收入人群占比 {low_income_ratio:.1%} 过高")
        warnings.append("   可能导致大部分Agent买不起房产，建议控制在60%以下")

    # 3. 检查房产分配的合理性
    avg_properties_per_person = property_count / total_agents
    if avg_properties_per_person < 0.5:
        warnings.append(f"🟡 提示: 人均房产数 {avg_properties_per_person:.2f} 偏低")
        warnings.append("   可能导致市场房源不足，建议至少0.8套/人")

    # 4. 估算可负担性（粗略）
    # 假设中高收入人群能买得起房
    potential_buyers = (agent_config['middle']['count'] +
                        agent_config['high']['count'] +
                        agent_config['ultra_high']['count'])
    buyer_ratio = potential_buyers / total_agents

    if buyer_ratio < 0.3:
        warnings.append(f"🟡 提示: 潜在买家占比 {buyer_ratio:.1%} 偏低")
        warnings.append("   建议中高收入群体至少占30%")

    return (len(errors) == 0, warnings, errors)


def show_intervention_menu(runner):
    """
    显示研究员干预面板
    """
    print("\n" + "=" * 50)
    print("🔬 研究员干预面板 (Researcher Intervention Panel)")
    print("=" * 50)
    print("通过调整以下参数，模拟不同的宏观经济环境。")
    print("📉 消极影响: 降薪、失业、加息 -> 抑制需求")
    print("📈 积极影响: 人口流入、降息、增供 -> 刺激交易")

    interventions = []

    while True:
        print("\n--- 干预选项 ---")
        print("1. [劳动力] 薪资调整 (Wage Shock)")
        print("2. [劳动力] 失业潮 (Unemployment Shock)")
        print("3. [人口] 新增人口 (Migration In)")
        print("4. [人口] 移除人口 (Migration Out)")
        print("5. [房产] 新增房源 (New Supply)")
        print("6. [房产] 下架房源 (Supply Cut)")
        print("0. ✅ 执行策略并继续 (Execute)")

        choice = input("Select option [0-6]: ").strip()

        try:
            if choice == '0':
                if interventions:
                    runner.set_interventions(interventions)
                    print(f"✅ 已提交 {len(interventions)} 项干预措施给公告栏。")
                break

            elif choice == '1':
                val = input("调整幅度 (e.g. -0.1 for -10%, 0.1 for +10%): ").strip()
                if not val:
                    continue
                pct = float(val)
                tier = input_default("覆盖阶层 (all/low/middle/high...)", "all")
                count = runner.intervention_service.apply_wage_shock(runner.agent_service, pct, tier)
                msg = f"Policy: Wage adjusted by {pct * 100:+.1f}% for {tier} tier."
                interventions.append(msg)
                print(f"✅ {msg}")

            elif choice == '2':
                val = input("失业率 (e.g. 0.2 for 20%): ").strip()
                if not val:
                    continue
                rate = float(val)
                tier = input_default("目标阶层 (low/middle...)", "low")
                count = runner.intervention_service.apply_unemployment_shock(runner.agent_service, rate, tier)
                msg = f"Policy: Unemployment shock of {rate * 100:.1f}% hit {tier} tier ({count} affected)."
                interventions.append(msg)
                print(f"✅ {msg}")

            elif choice == '3':
                val = input("新增数量: ").strip()
                if not val:
                    continue
                count = int(val)
                tier = input_default("阶层 (low/middle/high...)", "middle")
                added = runner.intervention_service.add_population(runner.agent_service, count, tier)
                msg = f"Demographics: {added} new {tier} income agents entered the city."
                interventions.append(msg)
                print(f"✅ {msg}")

            elif choice == '4':
                val = input("移除数量: ").strip()
                if not val:
                    continue
                count = int(val)
                tier = input_default("阶层 (low/middle/high...)", "low")
                removed = runner.intervention_service.remove_population(runner.agent_service, count, tier)
                msg = f"Demographics: {removed} {tier} income agents left the city."
                interventions.append(msg)
                print(f"✅ {msg}")

            elif choice == '5':
                val = input("新增房源数: ").strip()
                if not val:
                    continue
                count = int(val)
                zone = input_default("区域 (A/B)", "A")
                runner.intervention_service.adjust_housing_supply(
                    runner.market_service,
                    count,
                    zone,
                    config=runner.config,
                    current_month=0
                )
                msg = f"Supply: {count} new properties released in Zone {zone}."
                interventions.append(msg)
                print(f"✅ {msg}")

            elif choice == '6':
                val = input("下架房源数: ").strip()
                if not val:
                    continue
                count = int(val)
                zone = input_default("区域 (A/B)", "A")
                removed = runner.intervention_service.supply_cut(runner.market_service, count, zone)
                msg = f"Supply: {removed} listings removed from Zone {zone}."
                interventions.append(msg)
                print(f"✅ {msg}")

            else:
                print("❌ Invalid option.")

        except Exception as e:
            if isinstance(e, EOFError):
                print("\n⚠️ 输入流结束，自动退出干预菜单。")
                break
            print(f"❌ Error executing intervention: {e}")
            import traceback
            traceback.print_exc()


def run_forensic_analysis_menu():
    """
    运行逻辑体检 (Forensic Analysis) 菜单
    """
    print("\n" + "=" * 60)
    print("🕵️  逻辑体检与法医分析 (Forensic Analysis)".center(60))
    print("=" * 60)

    # Select Project
    import project_manager
    print("📂 请选择要分析的项目:")
    projects = project_manager.list_projects()

    if not projects:
        print("❌ 未找到任何项目。")
        return

    for i, p in enumerate(projects):
        print(f"  {i + 1}. {os.path.basename(p)}")

    idx_str = input_default("选择项目 (0返回)", "1")
    if not idx_str.isdigit():
        return
    idx = int(idx_str) - 1
    if idx < 0:
        return

    if 0 <= idx < len(projects):
        selected_proj = projects[idx]
        _, db_path = project_manager.load_project_paths(selected_proj)

        if not os.path.exists(db_path):
            print(f"❌ 数据库文件不存在: {db_path}")
            return

        print(f"✅ 已选中数据库: {db_path}")

        while True:
            print("\n--- 分析模式 ---")
            print("A. 批量全面扫描 (Batch Check) - 快速找出逻辑硬伤")
            print("B. 单人深度画像 (Single Profile) - 生成时序逻辑报告")
            print("0. 返回主菜单")

            mode = input("请选择模式 [A/B/0]: ").strip().upper()

            if mode == '0':
                break

            cmd = [sys.executable, "generate_enhanced_diaries.py", "--db", db_path]

            if mode == 'A':
                cmd.extend(["--mode", "batch"])
                subprocess.run(cmd)

            elif mode == 'B':
                cmd.extend(["--mode", "single"])
                aid = input("请输入 Agent ID: ").strip()
                if aid:
                    cmd.extend(["--agent_id", aid])
                    subprocess.run(cmd)
            else:
                print("❌ 无效选项")
    else:
        print("❌ 无效选择")


def run_night_ops_menu():
    """
    夜跑工具箱：启动夜跑、看门狗、后验收与诊断。
    """
    while True:
        print("\n" + "=" * 60)
        print("🌙 Night Run Toolkit".center(60))
        print("=" * 60)
        print("1. 启动夜跑 (run_night_stress_100x12.py)")
        print("2. 启动看门狗 (night_run_watchdog.py)")
        print("3. 夜跑后验收 (night_run_postcheck.py)")
        print("4. 0成交诊断 (zero_tx_diagnostics.py)")
        print("5. Gate告警检查 (gate_alerts.py)")
        print("6. 从最新项目恢复继续跑 (resume_from_latest.py)")
        print("0. 返回主菜单")

        choice = input_default("Choose option", "0")
        if choice == "0":
            return
        try:
            if choice == "1":
                subprocess.run([sys.executable, "run_night_stress_100x12.py"])
            elif choice == "2":
                idle = input_default("看门狗空闲阈值(分钟)", "15")
                restarts = input_default("最大自动重启次数", "2")
                subprocess.run([
                    sys.executable, "tools/night_run_watchdog.py",
                    "--cmd", f"{sys.executable} run_night_stress_100x12.py",
                    "--idle-minutes", str(int(idle)),
                    "--max-restarts", str(int(restarts)),
                ])
            elif choice == "3":
                run_dir = input_default("指定run目录(留空=自动找最新night_100x12)", "")
                cmd = [sys.executable, "tools/night_run_postcheck.py"]
                if run_dir:
                    cmd.extend(["--run-dir", run_dir])
                subprocess.run(cmd)
            elif choice == "4":
                run_dir = input_default("指定run目录(留空=自动找最新run_*)", "")
                cmd = [sys.executable, "tools/zero_tx_diagnostics.py"]
                if run_dir:
                    cmd.extend(["--run-dir", run_dir])
                subprocess.run(cmd)
            elif choice == "5":
                run_dir = input_default("指定run目录(留空=自动找最新run_*)", "")
                gate = input_default("低成交门槛(min_tx_gate)", "3")
                cmd = [sys.executable, "tools/gate_alerts.py", "--min-tx-gate", str(int(gate))]
                if run_dir:
                    cmd.extend(["--run-dir", run_dir])
                subprocess.run(cmd)
            elif choice == "6":
                run_dir = input_default("指定run目录(留空=自动找最新run_*)", "")
                extra = input_default("追加模拟月数(extra_months)", "2")
                cmd = [sys.executable, "tools/resume_from_latest.py", "--extra-months", str(int(extra))]
                if run_dir:
                    cmd.extend(["--run-dir", run_dir])
                subprocess.run(cmd)
            else:
                print("❌ 无效选项")
        except Exception as e:
            print(f"❌ 夜跑工具执行失败: {e}")


def main():
    # UTF-8
    try:
        if sys.stdout.encoding != 'utf-8':
            sys.stdout.reconfigure(encoding='utf-8')
    except BaseException:
        pass

    while True:
        render_scholar_banner()

        # --- 1. Seed Control ---
        seed_val = input_default("Enter Random Seed (for reproducibility)", "random")
        seed_to_use = None
        if seed_val != "random":
            try:
                seed_int = int(seed_val)
                seed_to_use = seed_int
                random.seed(seed_int)
                np.random.seed(seed_int)
                print(f"✅ Random Seed set to: {seed_int}")
            except ValueError:
                print("⚠️ Invalid seed, using random.")
                logging.info("使用随机种子 (结果不可复现)")
        else:
            logging.info("使用随机种子 (结果不可复现)")

        # --- 2. Mode Selection ---
        _render_block_title("主菜单")
        print("请选择你接下来要做的事：")
        print("1. 新建实验（推荐，真人友好引导版）")
        print("   适合对外演示、正式复现实验、快速搭建平衡 / 买方 / 卖方环境。")
        print("2. 继续实验（继承上月状态后续跑）")
        print("   适合验证跨月承接、趋势延续、冲击后的第二阶段反应。")
        print("3. 新建实验（高级研究员配置版）")
        print("   适合研究员做更细粒度参数试验，不建议外部演示时默认使用。")
        print("4. 项目体检（Forensic Analysis）")
        print("   适合跑完后检查逻辑、证据和异常信号。")
        print("5. 夜跑工具箱")
        print("   适合批量运行和运维场景。")
        print("0. Exit")

        mode = input_default("Choose option", "1")

        if mode == '0':
            print("Bye!")
            break

        if mode == '4':
            run_forensic_analysis_menu()
            continue
        if mode == '5':
            run_night_ops_menu()
            continue

        resume = False
        open_startup_intervention_menu = False

        if mode == "2":
            resume = True
            print("📂 Select a project to RESUME:")
            import project_manager
            projects = project_manager.list_projects()

            if not projects:
                print("❌ No projects found to resume.")
                continue  # Loop back

            for i, p in enumerate(projects):
                print(f"  {i + 1}. {os.path.basename(p)}")

            idx = int(input_default("Select project (0 to cancel)", "1")) - 1
            if idx < 0:
                continue

            if 0 <= idx < len(projects):
                selected_proj = projects[idx]
                config_path, db_path = project_manager.load_project_paths(selected_proj)
                print(f"✅ Loading project: {selected_proj}")

                # Load config from project
                config = SimulationConfig(config_path)
                scholar_inputs = collect_scholar_resume_inputs(config, db_path, seed_to_use)
                months = int(scholar_inputs["months"])
                agent_count = int(scholar_inputs["agent_count"])
                apply_scholar_release_config(
                    config,
                    scholar_inputs,
                    start_month=int(scholar_inputs["resume_from_month"]) + 1,
                )
                config.save()
                open_startup_intervention_menu = bool(scholar_inputs["open_startup_intervention_menu"])
                print_scholar_summary(
                    scholar_inputs,
                    db_path=db_path,
                    resume_from_month=int(scholar_inputs["resume_from_month"]),
                )
            else:
                print("❌ Invalid selection.")
                continue

        elif mode == "1":
            print("\n" + "=" * 60)
            print("--- Scholar Guided Configuration ---")
            print("=" * 60)
            import project_manager
            proj_dir, config_path, db_path = project_manager.create_new_project("config/baseline.yaml")
            print(f"✅ Created New Project at: {proj_dir}")

            config = SimulationConfig(config_path)
            scholar_inputs = collect_scholar_new_run_inputs(config, seed_to_use)
            months = int(scholar_inputs["months"])
            agent_count = int(scholar_inputs["agent_count"])
            apply_scholar_release_config(config, scholar_inputs, start_month=1)
            config.save()
            open_startup_intervention_menu = bool(scholar_inputs["open_startup_intervention_menu"])
            print_scholar_summary(scholar_inputs, db_path=db_path)

        else:
            # NEW Simulation
            pass
            # Remove old DB handled by project_manager logic (new folder)

            # Remove old DB handled by project_manager logic (new folder)
            # try-except block removed as it was orphaned

            print("\n" + "=" * 60)
            print("--- Legacy Advanced Configuration ---")
            print("=" * 60)
            use_custom = input_default("Use Custom Parameters? (y/N)", "n")

            if use_custom.lower() != 'y':
                # 使用默认配置
                print("✅ Using Default Parameters.")

                # [Fix] Also create project folder for default config
                import project_manager
                proj_dir, config_path, db_path = project_manager.create_new_project("config/baseline.yaml")
                print(f"✅ Created New Project at: {proj_dir}")

                config = SimulationConfig(config_path)

                agent_count = 100
                months = 12
                default_base_year = int(config.get('simulation.base_year', 2026))
                base_year_input = input_default("模拟基准年份(用于房龄计算)", str(default_base_year)).strip()
                base_year = int(base_year_input) if base_year_input else default_base_year
                if base_year < 1900 or base_year > 2100:
                    print("⚠️ 基准年份超出合理范围，已回退默认值。")
                    base_year = default_base_year

                default_min_cash = config.get('decision_factors.activation.min_cash_observer_no_property', 500000)
                threshold_input = input(
                    f"无房且现金低于该值者不参与交易（万元） [default: {default_min_cash / 10000:.0f}]: "
                ).strip()
                if threshold_input:
                    min_cash_observer_threshold = int(float(threshold_input) * 10000)
                else:
                    min_cash_observer_threshold = int(default_min_cash)
                if min_cash_observer_threshold <= 0:
                    print("⚠️ 阈值必须大于0，已回退默认值。")
                    min_cash_observer_threshold = int(default_min_cash)
                config.update(
                    'decision_factors.activation.min_cash_observer_no_property',
                    int(min_cash_observer_threshold)
                )
                mp_enabled_input = input("启用战时模式 Market Pulse (个贷压力测试)? [y/N]: ").strip().lower()
                market_pulse_enabled = mp_enabled_input in ('y', 'yes', '1', 'true')
                config.update('market_pulse.enabled', bool(market_pulse_enabled))

                default_panel = bool(config.get('simulation.enable_intervention_panel', True))
                panel_input = input_default(
                    f"启用月末人工干预面板(enable_intervention_panel) [default: {'y' if default_panel else 'n'}]",
                    'y' if default_panel else 'n'
                ).strip().lower()
                enable_intervention_panel = panel_input in ('y', 'yes', '1', 'true')
                config.update('simulation.enable_intervention_panel', bool(enable_intervention_panel))

                default_income_adj = float(config.get('simulation.agent.income_adjustment_rate', 1.0))
                income_adj_input = input_default("全体收入调整系数(income_adjustment_rate)", f"{default_income_adj:.2f}").strip()
                income_adjustment_rate = float(income_adj_input) if income_adj_input else default_income_adj
                config.update('simulation.agent.income_adjustment_rate', float(income_adjustment_rate))

                default_down = float(config.get('mortgage.down_payment_ratio', 0.3))
                default_dti = float(config.get('mortgage.max_dti_ratio', 0.5))
                default_rate = float(config.get('mortgage.annual_interest_rate', 0.035))
                down_input = input_default("首付比例(mortgage.down_payment_ratio)", f"{default_down:.2f}").strip()
                dti_input = input_default("DTI上限(mortgage.max_dti_ratio)", f"{default_dti:.2f}").strip()
                rate_input = input_default("年利率(mortgage.annual_interest_rate)", f"{default_rate:.3f}").strip()
                config.update('mortgage.down_payment_ratio', float(down_input) if down_input else default_down)
                config.update('mortgage.max_dti_ratio', float(dti_input) if dti_input else default_dti)
                config.update('mortgage.annual_interest_rate', float(rate_input) if rate_input else default_rate)

                default_bid_floor_ratio = float(config.get('smart_agent.effective_bid_floor_ratio', 0.98))
                bid_floor_input = input_default(
                    "有效出价下限系数(smart_agent.effective_bid_floor_ratio, 0.50-1.20)",
                    f"{default_bid_floor_ratio:.2f}"
                ).strip()
                effective_bid_floor_ratio = float(bid_floor_input) if bid_floor_input else default_bid_floor_ratio
                if effective_bid_floor_ratio < 0.50 or effective_bid_floor_ratio > 1.20:
                    print("⚠️ 超出范围，已回退默认值 0.98")
                    effective_bid_floor_ratio = 0.98
                config.update('smart_agent.effective_bid_floor_ratio', float(effective_bid_floor_ratio))

                location_params = collect_location_scarcity_params(config)
                config.update('smart_agent.location_scarcity_weight', float(location_params['location_scarcity_weight']))
                config.update(
                    'smart_agent.shortlist_location_bonus_weight',
                    float(location_params['shortlist_location_bonus_weight'])
                )
                config.update(
                    'smart_agent.cross_zone_discount_threshold',
                    float(location_params['cross_zone_discount_threshold'])
                )

                preplanned_interventions = collect_preplanned_developer_supply(config)
                config.update('simulation.preplanned_interventions', preplanned_interventions)

                if seed_to_use is not None:
                    config.update('simulation.random_seed', seed_to_use)
                config.update('simulation.base_year', int(base_year))
                config.save()
            else:
                print("\n⚠️  注意: 以下参数将直接影响市场流动性和交易活跃度")
                print("   不当配置可能导致0交易，请参考默认值谨慎设置\n")

                # === Agent 配置 ===
                print("=" * 60)
                print("【步骤 1/4】Agent 数量与收入档次配置")
                print("=" * 60)

                # Agent总数
                agent_count = int(input_default("\n总Agent数量", "100"))
                benchmark_controls = collect_benchmark_test_controls(config)

                # 收入档次配置
                print("\n📊 收入档次配置 (共5档):")
                print("   提示: 收入分界线单位为 元/月")
                print("   参考: 低收入<20k, 中低收入20-40k, 中等收入40-80k, 高收入80-150k, 超高收入>150k\n")

                # 默认收入分界线
                default_income_bounds = {
                    'ultra_high': (150000, 300000),
                    'high': (80000, 150000),
                    'middle': (40000, 80000),
                    'low_mid': (20000, 40000),
                    'low': (8000, 20000)
                }

                agent_config = {}
                total_assigned = 0

                for tier_key in ['ultra_high', 'high', 'middle', 'low_mid', 'low']:
                    tier_names = {
                        'ultra_high': '超高收入',
                        'high': '高收入',
                        'middle': '中等收入',
                        'low_mid': '中低收入',
                        'low': '低收入'
                    }

                    default_bounds = default_income_bounds[tier_key]
                    print(f"\n【{tier_names[tier_key]}档】")
                    print(f"  默认收入范围: {default_bounds[0]:,} - {default_bounds[1]:,} 元/月")

                    # 该档次人数
                    remaining = agent_count - total_assigned
                    if tier_key == 'low':
                        # 最后一档自动分配剩余
                        count = remaining
                        print(f"  该档Agent数量: {count} (剩余自动分配)")
                    else:
                        ratio_override = benchmark_controls.get("tier_ratio_override", {}) if "benchmark_controls" in locals() else {}
                        if ratio_override and tier_key in ratio_override:
                            default_count = max(1, int(agent_count * float(ratio_override[tier_key])))
                        else:
                            default_count = {
                                'ultra_high': max(1, agent_count // 20),  # 5%
                                'high': max(2, agent_count // 10),   # 10%
                                'middle': max(5, agent_count // 2),    # 50%
                                'low_mid': max(2, agent_count // 5)    # 20%
                            }.get(tier_key, 1)
                        count = int(input_default("  该档Agent数量", str(min(default_count, remaining))))

                    total_assigned += count

                    # 该档次房产数范围
                    default_props = {
                        'ultra_high': (2, 5),
                        'high': (1, 3),
                        'middle': (0, 1),
                        'low_mid': (0, 1),
                        'low': (0, 0)
                    }[tier_key]
                    prop_override = benchmark_controls.get("property_range_override", {}) if "benchmark_controls" in locals() else {}
                    if prop_override and tier_key in prop_override:
                        default_props = tuple(prop_override[tier_key])

                    props_min = int(input_default("  该档人均房产数(最小)", str(default_props[0])))
                    props_max = int(input_default("  该档人均房产数(最大)", str(default_props[1])))

                    agent_config[tier_key] = {
                        'count': count,
                        'income_range': default_bounds,
                        'property_count': (props_min, props_max)
                    }

                # === 房产配置 ===
                print("\n" + "=" * 60)
                print("【步骤 2/4】房产总量配置")
                print("=" * 60)

                min_properties = sum(tier['property_count'][0] * tier['count']
                                     for tier in agent_config.values())
                max_properties = sum(tier['property_count'][1] * tier['count']
                                     for tier in agent_config.values())

                print(f"\n根据配置，至少需要 {min_properties} 套房产")
                print(f"最多需要 {max_properties} 套房产")
                print(f"建议: {int(max_properties * 1.2)} 套 (留20%市场库存)\n")

                property_count = int(input_default("房产总数量", str(int(max_properties * 1.2))))

                print("\n📌 房源结构提示（新机制）:")
                print("   系统会把房源按低/中/高总价带分层生成，避免大量房子扎堆在同一价格段。")
                print("   如果你不熟悉，建议直接使用“自动平衡（推荐）”。")
                print("   若房产总量过少，会导致很多买家无候选房源，出现交易偏冷。\n")

                supply_mode = input_default(
                    "房源结构模式 [1=自动平衡(推荐), 2=手动设定A/B区低中高比例]",
                    "1"
                ).strip()
                if supply_mode not in ("1", "2"):
                    supply_mode = "1"

                supply_band_ratio_config = {}
                school_ratio_by_band_config = {}
                if supply_mode == "2":
                    print("\n🏘️ 手动设置供给价格带比例（每个区域总和建议=100）")
                    default_supply_ratio = {
                        "A": {"low": 25, "mid": 50, "high": 25},
                        "B": {"low": 45, "mid": 40, "high": 15},
                    }
                    default_school_ratio = {
                        "A": {"low": 18, "mid": 30, "high": 45},
                        "B": {"low": 5, "mid": 10, "high": 22},
                    }
                    for zone in ("A", "B"):
                        print(f"\n【{zone}区供给比例】")
                        d = default_supply_ratio[zone]
                        low_v = float(input_default("  低总价占比(%)", str(d["low"])))
                        mid_v = float(input_default("  中总价占比(%)", str(d["mid"])))
                        high_v = float(input_default("  高总价占比(%)", str(d["high"])))
                        total_v = low_v + mid_v + high_v
                        if total_v <= 0:
                            low_v, mid_v, high_v = d["low"], d["mid"], d["high"]
                            total_v = low_v + mid_v + high_v
                            print("  ⚠️ 输入无效，已回退默认比例。")
                        supply_band_ratio_config[zone] = {
                            "low": float(low_v / total_v),
                            "mid": float(mid_v / total_v),
                            "high": float(high_v / total_v),
                        }
                        print(
                            f"  ✅ {zone}区供给比例(归一后): "
                            f"低{round(supply_band_ratio_config[zone]['low'] * 100, 1)}% / "
                            f"中{round(supply_band_ratio_config[zone]['mid'] * 100, 1)}% / "
                            f"高{round(supply_band_ratio_config[zone]['high'] * 100, 1)}%"
                        )

                        use_custom_school = input(
                            f"  是否手动设置{zone}区按价格带的学区比例? [y/N]: "
                        ).strip().lower()
                        if use_custom_school == "y":
                            ds = default_school_ratio[zone]
                            s_low = float(input_default("    低总价学区比例(%)", str(ds["low"])))
                            s_mid = float(input_default("    中总价学区比例(%)", str(ds["mid"])))
                            s_high = float(input_default("    高总价学区比例(%)", str(ds["high"])))
                            school_ratio_by_band_config[zone] = {
                                "low": max(0.0, min(1.0, s_low / 100.0)),
                                "mid": max(0.0, min(1.0, s_mid / 100.0)),
                                "high": max(0.0, min(1.0, s_high / 100.0)),
                            }
                            print(f"  ✅ {zone}区按价格带学区比例已设置。")
                else:
                    print("✅ 已选择自动平衡房源结构（推荐）。")

                # 🆕 === 区域单价配置 ===
                print("\n" + "=" * 60)
                print("【步骤 2.5/4】区域房价配置 (单价)")
                print("=" * 60)

                print("\n💰 区域单价配置 (¥/㎡)")
                print("   说明: 配置后，房产价格 = 单价 × 建筑面积")
                print("   参考: 一线城市核心区3-5万/㎡，非核心区1-2万/㎡\n")

                zone_price_config = {}
                # [Fix] Create temp config to read defaults (since project config doesn't exist yet)
                temp_config = SimulationConfig("config/baseline.yaml")

                for zone, zone_name in [('A', '核心区'), ('B', '非核心区')]:
                    # 从配置文件获取默认值
                    default_range = temp_config.get_zone_price_range(zone)
                    default_min = default_range['min']
                    default_max = default_range['max']

                    print(f"【{zone}区 - {zone_name}】")
                    print(f"  当前默认单价: {default_min:,} - {default_max:,} ¥/㎡")

                    use_custom = input(f"  是否自定义{zone}区单价? [y/N]: ").strip().lower()

                    if use_custom == 'y':
                        min_price_input = input(f"    最低单价 (¥/㎡) [default: {default_min:,}]: ").strip()
                        max_price_input = input(f"    最高单价 (¥/㎡) [default: {default_max:,}]: ").strip()

                        min_price = int(min_price_input) if min_price_input else default_min
                        max_price = int(max_price_input) if max_price_input else default_max

                        if min_price >= max_price:
                            print("  ⚠️ 最低价不能大于等于最高价，使用默认值")
                            min_price, max_price = default_min, default_max

                        zone_price_config[zone] = {'min': min_price, 'max': max_price}
                        print(f"  ✅ {zone}区单价设置为: {min_price:,} - {max_price:,} ¥/㎡\n")
                    else:
                        print("  ✅ 使用默认单价\n")

                    # ==========================================
                    # 🆕 7.1 CLI: Rental Price Configuration
                    # ==========================================
                    default_rent = {
                        'A': temp_config.get('market.rental.zone_a_rent_per_sqm', 100),
                        'B': temp_config.get('market.rental.zone_b_rent_per_sqm', 60)
                    }

                    print(f"  🏘️ {zone}区 租金水平配置 (元/㎡/月)")
                    rent_input = input(f"    平均租金 [default: {default_rent[zone]}]: ").strip()
                    rent_val = float(rent_input) if rent_input else default_rent[zone]

                    # Store in config structure
                    # We need to structure this to push to config later
                    if 'rental_config' not in locals():
                        rental_config = {}
                    rental_config[zone] = rent_val
                    print(f"  ✅ {zone}区 租金设置为: {rent_val} 元/㎡/月\n")

                # 暂存配置，稍后应用到 config 对象
                if zone_price_config:
                    print("✅ 区域单价配置已暂存")
                if 'rental_config' in locals() and rental_config:
                    print("✅ 租金配置已暂存\n")

                # === 交易参与现金门槛配置 ===
                default_min_cash = temp_config.get('decision_factors.activation.min_cash_observer_no_property', 500000)
                threshold_input = input(
                    f"无房且现金低于该值者不参与交易（万元） [default: {default_min_cash / 10000:.0f}]: "
                ).strip()
                if threshold_input:
                    min_cash_observer_threshold = int(float(threshold_input) * 10000)
                else:
                    min_cash_observer_threshold = int(default_min_cash)
                if min_cash_observer_threshold <= 0:
                    print("⚠️ 阈值必须大于0，已回退默认值。")
                    min_cash_observer_threshold = int(default_min_cash)
                print(f"✅ 交易参与现金门槛设置为: {min_cash_observer_threshold / 10000:.1f} 万元\n")

                # === 房龄基准年份配置 ===
                default_base_year = int(temp_config.get('simulation.base_year', 2026))
                base_year_input = input_default("模拟基准年份(用于房龄计算)", str(default_base_year)).strip()
                base_year = int(base_year_input) if base_year_input else default_base_year
                if base_year < 1900 or base_year > 2100:
                    print("⚠️ 基准年份超出合理范围，已回退默认值。")
                    base_year = default_base_year
                print(f"✅ 房龄基准年份设置为: {base_year}\n")

                # === 订单与竞价稳态参数 ===
                default_bid_floor_ratio = float(temp_config.get('smart_agent.effective_bid_floor_ratio', 0.98))
                bid_floor_input = input(
                    f"有效出价下限系数(相对卖方底价, 0.50-1.20) [default: {default_bid_floor_ratio:.2f}]: "
                ).strip()
                effective_bid_floor_ratio = float(bid_floor_input) if bid_floor_input else default_bid_floor_ratio
                if effective_bid_floor_ratio < 0.50 or effective_bid_floor_ratio > 1.20:
                    print("⚠️ 超出范围，已回退默认值 0.98")
                    effective_bid_floor_ratio = 0.98

                default_precheck_buffer = int(temp_config.get('smart_agent.precheck_liquidity_buffer_months', 3))
                precheck_buf_input = input(
                    f"下单前现金缓冲(月) [default: {default_precheck_buffer}]: "
                ).strip()
                precheck_liquidity_buffer_months = int(precheck_buf_input) if precheck_buf_input else default_precheck_buffer
                if precheck_liquidity_buffer_months < 0:
                    print("⚠️ 不能为负数，已回退默认值 3")
                    precheck_liquidity_buffer_months = 3

                default_precheck_fees = temp_config.get('smart_agent.precheck_include_tax_and_fee', True)
                precheck_fees_input = input(
                    f"预检是否计入税费杂费? [Y/n] [default: {'Y' if default_precheck_fees else 'N'}]: "
                ).strip().lower()
                if precheck_fees_input == "":
                    precheck_include_tax_and_fee = bool(default_precheck_fees)
                else:
                    precheck_include_tax_and_fee = precheck_fees_input not in ("n", "no", "0", "false")

                location_params = collect_location_scarcity_params(temp_config)

                default_panel = bool(temp_config.get('simulation.enable_intervention_panel', True))
                panel_input = input_default(
                    f"启用月末人工干预面板(enable_intervention_panel) [default: {'y' if default_panel else 'n'}]",
                    'y' if default_panel else 'n'
                ).strip().lower()
                enable_intervention_panel = panel_input in ('y', 'yes', '1', 'true')

                default_income_adj = float(temp_config.get('simulation.agent.income_adjustment_rate', 1.0))
                income_adj_input = input_default("全体收入调整系数(income_adjustment_rate)", f"{default_income_adj:.2f}").strip()
                income_adjustment_rate = float(income_adj_input) if income_adj_input else default_income_adj

                default_down = float(temp_config.get('mortgage.down_payment_ratio', 0.3))
                default_dti = float(temp_config.get('mortgage.max_dti_ratio', 0.5))
                default_rate = float(temp_config.get('mortgage.annual_interest_rate', 0.035))
                mortgage_down_payment_ratio = float(input_default("首付比例(mortgage.down_payment_ratio)", f"{default_down:.2f}"))
                mortgage_max_dti_ratio = float(input_default("DTI上限(mortgage.max_dti_ratio)", f"{default_dti:.2f}"))
                mortgage_annual_interest_rate = float(input_default("年利率(mortgage.annual_interest_rate)", f"{default_rate:.3f}"))

                # === 战时模式（Market Pulse）===
                default_mp_enabled = bool(temp_config.get('market_pulse.enabled', False))
                mp_input = input(
                    f"启用战时模式 Market Pulse (个贷压力测试)? [y/N] [default: {'Y' if default_mp_enabled else 'N'}]: "
                ).strip().lower()
                if mp_input == "":
                    market_pulse_enabled = default_mp_enabled
                else:
                    market_pulse_enabled = mp_input in ('y', 'yes', '1', 'true')

                default_mp_seed_ratio = float(temp_config.get('market_pulse.seed_existing_mortgage_ratio', 0.55))
                mp_seed_input = input(
                    f"存量按揭注入覆盖率(0-1) [default: {default_mp_seed_ratio:.2f}]: "
                ).strip()
                market_pulse_seed_ratio = float(mp_seed_input) if mp_seed_input else default_mp_seed_ratio
                if market_pulse_seed_ratio < 0 or market_pulse_seed_ratio > 1:
                    print("⚠️ 覆盖率必须在0-1之间，已回退默认值 0.55")
                    market_pulse_seed_ratio = 0.55

                # 启动前风险提示（仅提示，不阻断）
                risk_score = 0
                if min_cash_observer_threshold >= 800000:
                    risk_score += 1
                if effective_bid_floor_ratio >= 1.05:
                    risk_score += 2
                if precheck_liquidity_buffer_months >= 8:
                    risk_score += 1
                if risk_score >= 3:
                    risk_level = "高"
                elif risk_score >= 1:
                    risk_level = "中"
                else:
                    risk_level = "低"
                print(f"⚠️ 交易冻结风险评估: {risk_level}")
                if risk_level != "低":
                    print("   提示: 现金门槛过高 + 出价下限过严 + 现金缓冲过大，可能导致0成交。")

                # === 市场健康检查 ===
                print("\n" + "=" * 60)
                print("【步骤 3/4】市场健康检查")
                print("=" * 60)

                is_valid, warnings, errors = validate_config(agent_config, property_count)

                if errors:
                    print("\n❌ 配置错误:")
                    for err in errors:
                        print(f"  {err}")
                    print("\n请修正后重新运行。")
                    continue  # Loop back

                if warnings:
                    print("\n⚠️  配置警告:")
                    for warn in warnings:
                        print(f"  {warn}")
                    print("\n这些配置可能导致交易不活跃，但可以继续运行。")
                    confirm = input("\n是否继续? [Y/n]: ").strip().lower()
                    if confirm == 'n':
                        print("已取消模拟。")
                        continue  # Loop back
                else:
                    print("\n✅ 配置检查通过！")

                # === 最终确认 ===
                print("\n" + "=" * 60)
                print("【步骤 4/4】配置总览与确认")
                print("=" * 60)

                months = int(input_default("\n模拟月数", "12"))

                print("\n配置总览:")
                print(f"  - Agent总数: {agent_count}")
                for tier_key, tier_data in agent_config.items():
                    tier_names = {'ultra_high': '超高', 'high': '高', 'middle': '中', 'low_mid': '中低', 'low': '低'}
                    print(f"      {tier_names[tier_key]}收入: {tier_data['count']}人, "
                          f"收入{tier_data['income_range'][0] // 1000}-{tier_data['income_range'][1] // 1000}k, "
                          f"拥房{tier_data['property_count'][0]}-{tier_data['property_count'][1]}套")
                print(f"  - 房产总数: {property_count}")
                print(f"  - 模拟月数: {months}")
                print(f"  - 随机种子: {seed_to_use or '随机'}")
                print(f"  - 无房参与门槛: 现金≥{min_cash_observer_threshold / 10000:.1f}万元")
                print(f"  - 房龄基准年份: {base_year}")
                print(f"  - 有效出价下限系数: {effective_bid_floor_ratio:.2f}")
                print(f"  - 下单前现金缓冲(月): {precheck_liquidity_buffer_months}")
                print(f"  - 预检计入税费: {'是' if precheck_include_tax_and_fee else '否'}")
                print(f"  - 地段权重: {location_params['location_scarcity_weight']:.2f}")
                print(f"  - 候选地段加分: {location_params['shortlist_location_bonus_weight']:.2f}")
                print(f"  - 跨区折价阈值: {location_params['cross_zone_discount_threshold']:.2f}")
                print(f"  - 月末人工干预面板: {'开启' if enable_intervention_panel else '关闭'}")
                print(f"  - 全体收入调整系数: {income_adjustment_rate:.2f}")
                print(f"  - 首付比例: {mortgage_down_payment_ratio:.2f}")
                print(f"  - DTI上限: {mortgage_max_dti_ratio:.2f}")
                print(f"  - 年利率: {mortgage_annual_interest_rate:.3f}")
                print(f"  - 战时模式(Market Pulse): {'开启' if market_pulse_enabled else '关闭'}")
                print(f"  - 存量按揭覆盖率: {market_pulse_seed_ratio:.2f}")
                if 'benchmark_controls' in locals():
                    bm_mode = str(benchmark_controls.get("mode", "custom"))
                    print(f"  - 研究基准快捷模式: {bm_mode}")
                    print(f"    理想供给: {'是' if benchmark_controls.get('apply_ideal_supply', False) else '否'}")
                    print(f"    画像差异化: {'是' if benchmark_controls.get('apply_profile_diversity', False) else '否'}")
                supply_mode_label = "自动平衡(推荐)" if str(supply_mode) == "1" else "手动比例"
                print(f"  - 房源结构模式: {supply_mode_label}")
                if 'supply_band_ratio_config' in locals() and supply_band_ratio_config:
                    for z in ("A", "B"):
                        if z in supply_band_ratio_config:
                            rz = supply_band_ratio_config[z]
                            print(
                                f"    {z}区低/中/高: "
                                f"{round(float(rz.get('low', 0.0)) * 100, 1)}% / "
                                f"{round(float(rz.get('mid', 0.0)) * 100, 1)}% / "
                                f"{round(float(rz.get('high', 0.0)) * 100, 1)}%"
                            )

                confirm = input("\n确认启动模拟? [Y/n]: ").strip().lower()
                if confirm == 'n':
                    print("已取消模拟。")
                    continue  # Loop back

                # === 创建项目文件夹 ===
                import project_manager
                proj_dir, config_path, db_path = project_manager.create_new_project("config/baseline.yaml")
                print(f"✅ Created New Project at: {proj_dir}")

                # 重新加载新位置的配置
                config = SimulationConfig(config_path)

                # 更新配置并保存到项目目录
                if seed_to_use is not None:
                    config.update('simulation.random_seed', seed_to_use)
                if 'base_year' in locals():
                    config.update('simulation.base_year', int(base_year))

                # 保存用户自定义参数
                if 'agent_config' in locals() and agent_config:
                    config._config['user_agent_config'] = agent_config
                if 'property_count' in locals():
                    config._config['user_property_count'] = property_count
                if 'benchmark_controls' in locals() and benchmark_controls:
                    config._config['research_benchmark_controls'] = benchmark_controls

                # [Fix] Apply deferred zone price configuration
                if 'zone_price_config' in locals() and zone_price_config:
                    for zone, prices in zone_price_config.items():
                        config.update(f'market.zones.{zone}.price_per_sqm_range.min', prices['min'])
                        config.update(f'market.zones.{zone}.price_per_sqm_range.max', prices['max'])

                # [New] Apply supply-band ratio configuration (if manually provided)
                if 'supply_band_ratio_config' in locals() and supply_band_ratio_config:
                    for zone, ratio_map in supply_band_ratio_config.items():
                        config.update(f'market.zones.{zone}.supply_band_ratio.low', float(ratio_map.get('low', 0.0)))
                        config.update(f'market.zones.{zone}.supply_band_ratio.mid', float(ratio_map.get('mid', 0.0)))
                        config.update(f'market.zones.{zone}.supply_band_ratio.high', float(ratio_map.get('high', 0.0)))

                # [New] Apply school ratio by price band (optional manual input)
                if 'school_ratio_by_band_config' in locals() and school_ratio_by_band_config:
                    for zone, ratio_map in school_ratio_by_band_config.items():
                        config.update(f'market.zones.{zone}.school_district_ratio_by_band.low', float(ratio_map.get('low', 0.0)))
                        config.update(f'market.zones.{zone}.school_district_ratio_by_band.mid', float(ratio_map.get('mid', 0.0)))
                        config.update(f'market.zones.{zone}.school_district_ratio_by_band.high', float(ratio_map.get('high', 0.0)))

                # 🆕 7.1 Apply deferred rental configuration
                if 'rental_config' in locals() and rental_config:
                    config.update('market.rental.zone_a_rent_per_sqm', rental_config.get('A', 100))
                    config.update('market.rental.zone_b_rent_per_sqm', rental_config.get('B', 60))

                if 'min_cash_observer_threshold' in locals():
                    config.update(
                        'decision_factors.activation.min_cash_observer_no_property',
                        int(min_cash_observer_threshold)
                    )
                if 'effective_bid_floor_ratio' in locals():
                    config.update('smart_agent.effective_bid_floor_ratio', float(effective_bid_floor_ratio))
                if 'precheck_liquidity_buffer_months' in locals():
                    config.update('smart_agent.precheck_liquidity_buffer_months', int(precheck_liquidity_buffer_months))
                if 'precheck_include_tax_and_fee' in locals():
                    config.update('smart_agent.precheck_include_tax_and_fee', bool(precheck_include_tax_and_fee))
                if 'location_params' in locals():
                    config.update('smart_agent.location_scarcity_weight', float(location_params['location_scarcity_weight']))
                    config.update(
                        'smart_agent.shortlist_location_bonus_weight',
                        float(location_params['shortlist_location_bonus_weight'])
                    )
                    config.update(
                        'smart_agent.cross_zone_discount_threshold',
                        float(location_params['cross_zone_discount_threshold'])
                    )
                if 'market_pulse_enabled' in locals():
                    config.update('market_pulse.enabled', bool(market_pulse_enabled))
                if 'market_pulse_seed_ratio' in locals():
                    config.update('market_pulse.seed_existing_mortgage_ratio', float(market_pulse_seed_ratio))
                if 'enable_intervention_panel' in locals():
                    config.update('simulation.enable_intervention_panel', bool(enable_intervention_panel))
                if 'income_adjustment_rate' in locals():
                    config.update('simulation.agent.income_adjustment_rate', float(income_adjustment_rate))
                if 'mortgage_down_payment_ratio' in locals():
                    config.update('mortgage.down_payment_ratio', float(mortgage_down_payment_ratio))
                if 'mortgage_max_dti_ratio' in locals():
                    config.update('mortgage.max_dti_ratio', float(mortgage_max_dti_ratio))
                if 'mortgage_annual_interest_rate' in locals():
                    config.update('mortgage.annual_interest_rate', float(mortgage_annual_interest_rate))
                if 'benchmark_controls' in locals() and benchmark_controls.get("config_patches"):
                    for cfg_key, cfg_val in benchmark_controls["config_patches"].items():
                        config.update(str(cfg_key), cfg_val)
                preplanned_interventions = collect_preplanned_developer_supply(config)
                if 'preplanned_interventions' in locals():
                    config.update('simulation.preplanned_interventions', preplanned_interventions)

                config.save()

        # --- 3. Execution ---
        print("\n🚀 Initializing Runner...")

        runner = SimulationRunner(
            agent_count=agent_count if not resume else 0,
            months=months,
            seed=seed_to_use,
            resume=resume,
            config=config,
            db_path=db_path
        )

        try:
            # NEW: Researcher Intervention Panel
            if open_startup_intervention_menu:
                show_intervention_menu(runner)

            runner.run()
            print("\n✅ Simulation Completed Successfully.")

            # --- 4. Auto Export ---
            print("\n📦 Exporting Results...")
            try:
                import scripts.export_results as exporter

                # Pass correct paths to exporter
                output_dir = os.path.dirname(db_path)
                exporter.export_data(db_path=db_path, output_dir=output_dir)
            except ImportError:
                import subprocess
                subprocess.run([sys.executable, "scripts/export_results.py"])

            try:
                render_and_save_scholar_result_card(db_path, config)
            except Exception as result_card_error:
                print(f"⚠️ 结果卡生成失败，但不影响主结果保存: {result_card_error}")

            # === 5. Auto Forensic Check ===
            print("\n" + "=" * 50)
            check_now = input("是否立即运行逻辑体检 (Forensic Analysis)? [y/N]: ").strip().lower()
            if check_now == 'y':
                import subprocess
                print("🚀 Launching Forensic Analysis...")
                subprocess.run([sys.executable, "generate_enhanced_diaries.py", "--db", db_path, "--mode", "batch"])

        except KeyboardInterrupt:
            print("\n🛑 Simulation Stopped by User.")
        except Exception as e:
            print(f"\n❌ FATAL ERROR: {e}")
            import traceback
            traceback.print_exc()

        print("\nPress Enter to return to main menu...")
        input()


if __name__ == "__main__":
    try:
        main()
    except EOFError:
        # Non-interactive stdin exhausted: exit gracefully for smoke/batch runs.
        print("\n⚠️ 输入流结束，程序已安全退出。")
