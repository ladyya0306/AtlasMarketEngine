import asyncio
import datetime
import json
import logging
import os
import random
import sqlite3
import sys
from typing import Any, Callable, Dict, List, Optional

from config.config_loader import SimulationConfig
from config.settings import MACRO_ENVIRONMENT, get_current_macro_sentiment
from database import init_db
from services.agent_service import AgentService
from services.intervention_service import InterventionService
from services.market_service import MarketService
from services.mortgage_risk_service import MortgageRiskService
from services.rental_service import RentalService
from services.reporting_service import ReportingService
from services.transaction_service import TransactionService

# from utils.behavior_logger import BehaviorLogger
from utils.exchange_display import ExchangeDisplay
from utils.workflow_logger import WorkflowLogger

# Configure Logging (robust on Windows when log file cannot be opened)
_handlers = [logging.StreamHandler()]
try:
    _handlers.insert(0, logging.FileHandler("simulation_run.log", encoding='utf-8', mode='a'))
except OSError:
    # Fall back to console logging only.
    pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=_handlers
)
# Force set stdio to utf-8 on Windows/PowerShell to reduce mojibake.
for _stream_name in ("stdin", "stdout", "stderr"):
    try:
        _s = getattr(sys, _stream_name, None)
        if _s and hasattr(_s, "reconfigure"):
            _s.reconfigure(encoding="utf-8")
    except Exception:
        pass

logger = logging.getLogger(__name__)


def _safe_read_json(path: str) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


class SimulationRunner:
    def __init__(self, agent_count=50, months=12, seed=42, resume=False, config=None, db_path=None):
        self.agent_count = agent_count
        self.months = months
        self.seed = seed
        self.resume = resume
        self.config = config if config else SimulationConfig()
        self.db_path = db_path
        self._run_dir = None
        self._run_log_handler = None
        self._apply_llm_runtime_env()

        # Initialize Database connection
        self.db_path = self._resolve_db_path(self.db_path)
        self._run_dir = os.path.dirname(self.db_path) or os.getcwd()
        os.makedirs(self._run_dir, exist_ok=True)
        self._configure_run_logging()

        # Initialize DB Schema if needed
        if not self.resume:
            init_db(self.db_path)

        self.conn = sqlite3.connect(self.db_path, timeout=60.0, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout = 30000")

        # Initialize Services
        self.market_service = MarketService(self.config, self.conn)
        self.agent_service = AgentService(self.config, self.conn)
        self.rental_service = RentalService(self.config, self.conn)
        self.mortgage_risk_service = MortgageRiskService(self.config, self.conn)

        # Tier 4.2: Intervention Service (CLI)
        self.intervention_service = InterventionService(self.conn)

        # V3: Developer Account Service
        from services.developer_account_service import DeveloperAccountService
        self.developer_account_service = DeveloperAccountService(self.conn)

        # Transaction Service
        self.transaction_service = TransactionService(
            self.config,
            self.conn,
            developer_service=self.developer_account_service,
            mortgage_risk_service=self.mortgage_risk_service,
        )

        # Tier 5: Reporting Service (Market Bulletins, Final Reports)
        self.reporting_service = ReportingService(self.config, self.conn)
        
        # Pending Interventions (Tier 5)
        self.pending_interventions = []
        self._applied_preplanned_interventions = set()
        self._initialized = False
        self.current_month = 0
        self.status = "idle"
        self.last_error = None
        self.started_at = None
        self.completed_at = None
        self.last_bulletin = None
        self.last_month_summary = None
        self.final_summary = None
        self.intervention_history = []
        self.progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None
        
        # V3: Generate experiment metadata card
        if not self.resume:
            self.generate_experiment_card()

    def _parameter_assumption_artifact_paths(self) -> Dict[str, str]:
        return {
            "markdown_path": os.path.join(self._run_dir, "parameter_assumption_report.md"),
            "json_path": os.path.join(self._run_dir, "parameter_assumption_report.json"),
        }

    def _motivation_report_artifact_paths(self) -> Dict[str, str]:
        return {
            "markdown_path": os.path.join(self._run_dir, "motivation_agent_report.md"),
            "json_path": os.path.join(self._run_dir, "motivation_agent_report.json"),
        }

    def _derive_tier_snapshot(self) -> List[Dict[str, object]]:
        user_tiers = self.config._config.get("user_agent_config")
        if isinstance(user_tiers, list) and user_tiers:
            rows = []
            for item in user_tiers:
                rows.append(
                    {
                        "tier": item.get("tier"),
                        "count": int(item.get("count", 0) or 0),
                        "income_min": int(item.get("income_min", 0) or 0),
                        "income_max": int(item.get("income_max", 0) or 0),
                        "property_min": int(item.get("property_min", 0) or 0),
                        "property_max": int(item.get("property_max", 0) or 0),
                    }
                )
            return rows

        ordered = ["ultra_high", "high", "middle", "lower_middle", "low"]
        dist = self.config.get("agent_tiers.distribution", {}) or {}
        boundaries = self.config.get("agent_tiers.boundaries", {}) or {}
        wealth = self.config.get("agent_tiers.initial_wealth", {}) or {}
        assigned = 0
        rows = []
        for tier in ordered:
            if tier == "low":
                count = max(0, int(self.agent_count) - assigned)
            else:
                pct = float(dist.get(tier, 0) or 0)
                count = int(round(int(self.agent_count) * pct / 100.0))
                assigned += count
            income_min = int(boundaries.get(tier, 0) or 0)
            rows.append(
                {
                    "tier": tier,
                    "count": count,
                    "income_min": income_min,
                    "income_max": None,
                    "property_min": int((wealth.get(tier, {}) or {}).get("property_count", [0, 0])[0] or 0),
                    "property_max": int((wealth.get(tier, {}) or {}).get("property_count", [0, 0])[1] or 0),
                }
            )
        return rows

    def _build_parameter_rows(self) -> List[Dict[str, object]]:
        pulse_plans = self.config.get("simulation.preplanned_interventions", []) or []
        rows = [
            {
                "parameter_key": "simulation.agent_count",
                "label": "Agent 数量",
                "current_value": int(self.agent_count),
                "parameter_category": "系统规模",
                "source_category": "D",
                "source_explanation": "实验规模控制参数，由研究者启动时直接设定。",
                "why_set": "当前阶段更适合 100-500 的研究样本，兼顾行为多样性与 token/时间成本。",
                "high_impact": "样本更多，结构更丰富，但运行成本更高。",
                "low_impact": "成本更低，但更容易受个体随机性影响。",
                "is_key": True,
                "confidence": "高",
                "needs_calibration": True,
            },
            {
                "parameter_key": "simulation.months",
                "label": "模拟月数",
                "current_value": int(self.months),
                "parameter_category": "时间范围",
                "source_category": "D",
                "source_explanation": "实验观察窗口，由研究者根据问题设定。",
                "why_set": "需要覆盖足够月数，才方便观察挂牌、谈判、成交和失败的连续链条。",
                "high_impact": "时间更长，现象更完整，但运行更慢。",
                "low_impact": "验证更快，但机制链条可能不完整。",
                "is_key": True,
                "confidence": "高",
                "needs_calibration": False,
            },
            {
                "parameter_key": "market.zones.A.price_per_sqm_range",
                "label": "A 区单价区间",
                "current_value": self.config.get_zone_price_range("A"),
                "parameter_category": "区域价格",
                "source_category": "A/B",
                "source_explanation": "优先参考住建局数据与区域市场经验。",
                "why_set": "A 区代表核心区，应该体现更高价格锚点。",
                "high_impact": "会抬高成交均价与购房门槛。",
                "low_impact": "会削弱 A 区与 B 区的层级差异。",
                "is_key": True,
                "confidence": "中",
                "needs_calibration": True,
            },
            {
                "parameter_key": "market.zones.B.price_per_sqm_range",
                "label": "B 区单价区间",
                "current_value": self.config.get_zone_price_range("B"),
                "parameter_category": "区域价格",
                "source_category": "A/B",
                "source_explanation": "优先参考住建局数据与区域市场经验。",
                "why_set": "B 区代表非核心区，应与 A 区形成可辨识梯度。",
                "high_impact": "会抬高外围市场门槛，减少刚需承接。",
                "low_impact": "会增强刚需成交和外围吸引力。",
                "is_key": True,
                "confidence": "中",
                "needs_calibration": True,
            },
            {
                "parameter_key": "decision_factors.activation.rental.zone_a_rent_per_sqm",
                "label": "A 区租金 / 平米",
                "current_value": float(self.config.get("market.rental.zone_a_rent_per_sqm", 0) or 0),
                "parameter_category": "租赁市场",
                "source_category": "A/B",
                "source_explanation": "参考区域租售对比的经验值。",
                "why_set": "租金会影响持有回报、买租比较和激活意愿。",
                "high_impact": "更容易支撑高价资产的持有逻辑。",
                "low_impact": "更容易削弱买入动机和投资型需求。",
                "is_key": True,
                "confidence": "中",
                "needs_calibration": True,
            },
            {
                "parameter_key": "decision_factors.activation.rental.zone_b_rent_per_sqm",
                "label": "B 区租金 / 平米",
                "current_value": float(self.config.get("market.rental.zone_b_rent_per_sqm", 0) or 0),
                "parameter_category": "租赁市场",
                "source_category": "A/B",
                "source_explanation": "参考区域租售对比的经验值。",
                "why_set": "用于支撑 B 区房产的投资和持有吸引力。",
                "high_impact": "可能提高外围资产留存和买入意愿。",
                "low_impact": "会削弱 B 区资产吸引力。",
                "is_key": True,
                "confidence": "中",
                "needs_calibration": True,
            },
            {
                "parameter_key": "mortgage.down_payment_ratio",
                "label": "首付比例",
                "current_value": float(self.config.get("mortgage.down_payment_ratio", 0.3) or 0.3),
                "parameter_category": "融资约束",
                "source_category": "B",
                "source_explanation": "银行按揭业务常识与现实政策约束。",
                "why_set": "是购房门槛的核心参数之一。",
                "high_impact": "会显著压缩可成交买家范围。",
                "low_impact": "会放宽成交门槛，但也会降低约束强度。",
                "is_key": True,
                "confidence": "高",
                "needs_calibration": True,
            },
            {
                "parameter_key": "mortgage.annual_interest_rate",
                "label": "年利率",
                "current_value": float(self.config.get("mortgage.annual_interest_rate", 0.035) or 0.035),
                "parameter_category": "融资约束",
                "source_category": "B",
                "source_explanation": "银行按揭业务常识与现实利率环境。",
                "why_set": "利率直接影响月供、可负担性和买家信心。",
                "high_impact": "会压缩购买力，抑制成交。",
                "low_impact": "会扩大购买力，增强成交。",
                "is_key": True,
                "confidence": "高",
                "needs_calibration": True,
            },
            {
                "parameter_key": "mortgage.max_dti_ratio",
                "label": "最高负债收入比",
                "current_value": float(self.config.get("mortgage.max_dti_ratio", 0.5) or 0.5),
                "parameter_category": "融资约束",
                "source_category": "B",
                "source_explanation": "银行授信与按揭审批常识。",
                "why_set": "用于限制过度加杠杆。",
                "high_impact": "会放大可借额度和成交概率。",
                "low_impact": "会提前淘汰边缘买家。",
                "is_key": True,
                "confidence": "高",
                "needs_calibration": True,
            },
            {
                "parameter_key": "decision_factors.activation.min_cash_observer_no_property",
                "label": "无房参与现金门槛",
                "current_value": int(self.config.get("decision_factors.activation.min_cash_observer_no_property", 0) or 0),
                "parameter_category": "参与门槛",
                "source_category": "A/B/C",
                "source_explanation": "兼具真实门槛与实验筛选属性。",
                "why_set": "用于避免现金严重不足的无房者进入交易环节。",
                "high_impact": "会减少潜在买家，降低活跃度。",
                "low_impact": "会引入更多边缘买家，增加尝试与失败。",
                "is_key": True,
                "confidence": "中",
                "needs_calibration": True,
            },
            {
                "parameter_key": "simulation.agent.income_adjustment_rate",
                "label": "收入调整倍率",
                "current_value": float(self.config.get("simulation.agent.income_adjustment_rate", 1.0) or 1.0),
                "parameter_category": "收入冲击",
                "source_category": "C",
                "source_explanation": "实验性控制参数。",
                "why_set": "用于模拟工资整体上升或下降的情景。",
                "high_impact": "会提高购买力与持有能力。",
                "low_impact": "会增加现金压力和被迫出售概率。",
                "is_key": False,
                "confidence": "中",
                "needs_calibration": True,
            },
            {
                "parameter_key": "market_pulse.enabled",
                "label": "启用市场脉冲",
                "current_value": bool(self.config.get("market_pulse.enabled", False)),
                "parameter_category": "信息传播",
                "source_category": "C",
                "source_explanation": "实验性机制开关。",
                "why_set": "用于模拟市场压力、存量按揭和违约传播环境。",
                "high_impact": "会增加市场情绪和风险传播效应。",
                "low_impact": "市场将更平滑、更少脉冲扰动。",
                "is_key": True,
                "confidence": "中",
                "needs_calibration": True,
            },
            {
                "parameter_key": "simulation.preplanned_interventions",
                "label": "夜跑预设干预",
                "current_value": len(pulse_plans),
                "parameter_category": "干预方案",
                "source_category": "C",
                "source_explanation": "研究者预设实验动作。",
                "why_set": "用于观察在特定月份注入人口、供给或收入冲击后的链式反应。",
                "high_impact": "会明显改变局部月份的市场路径。",
                "low_impact": "更接近自然演化。",
                "is_key": False,
                "confidence": "高",
                "needs_calibration": False,
            },
            {
                "parameter_key": "bulletin_visibility_ratio",
                "label": "公报接收比例",
                "current_value": self.config.get(
                    "smart_agent.info_delay_ratio",
                    self.config.get("info_delay_ratio", None),
                ),
                "parameter_category": "信息传播",
                "source_category": "C",
                "source_explanation": "当前实验通过 smart_agent 信息迟滞比例控制延迟覆盖面。",
                "why_set": "用于研究有多少 agent 会在角色决策时看到滞后的市场公报。",
                "high_impact": "会增强市场共识与同步反应。",
                "low_impact": "会增加信息不对称和局部差异。",
                "is_key": True,
                "confidence": "中",
                "needs_calibration": True,
            },
            {
                "parameter_key": "agent_information_delay",
                "label": "公报接收时滞",
                "current_value": {
                    "smart_enabled": bool(self.config.get("smart_agent.info_delay_enabled", self.config.get("info_delay_enabled", False))),
                    "smart_ratio": self.config.get("smart_agent.info_delay_ratio", self.config.get("info_delay_ratio", None)),
                    "normal_ratio": self.config.get("smart_agent.info_delay_ratio_normal", self.config.get("info_delay_ratio_normal", None)),
                    "apply_to_normal": bool(self.config.get("smart_agent.info_delay_apply_to_normal", self.config.get("info_delay_apply_to_normal", False))),
                    "min_months": self.config.get("smart_agent.info_delay_min_months", self.config.get("info_delay_min_months", None)),
                    "max_months": self.config.get("smart_agent.info_delay_max_months", self.config.get("info_delay_max_months", None)),
                },
                "parameter_category": "信息传播",
                "source_category": "C",
                "source_explanation": "当前实验已正式通过 smart_agent.info_delay_* 系列参数建模。",
                "why_set": "用于研究市场公报与趋势信号晚到若干个月后，对买卖决策链的影响。",
                "high_impact": "会放大延迟反应和错位行为。",
                "low_impact": "会增加同步性和即时反馈。",
                "is_key": True,
                "confidence": "中",
                "needs_calibration": True,
            },
        ]
        return rows

    def build_parameter_assumption_report(self) -> Dict[str, object]:
        metadata = _safe_read_json(os.path.join(self._run_dir, "metadata.json"))
        tier_rows = self._derive_tier_snapshot()
        export_report = self.get_export_report()
        return {
            "experiment_info": {
                "experiment_name": f"参数与假设说明表 - {self._run_id()}",
                "run_id": self._run_id(),
                "db_path": str(self.db_path),
                "metadata_path": os.path.join(self._run_dir, "metadata.json").replace("\\", "/"),
                "generated_at": datetime.datetime.now().isoformat(),
                "created_at": metadata.get("created_at"),
                "seed": self.seed,
                "months": self.months,
                "agent_count": self.agent_count,
                "night_run_plan_path": self.config._config.get("_applied_night_plan"),
                "has_preplanned_interventions": bool(self.config.get("simulation.preplanned_interventions", [])),
                "has_runtime_adjustments": bool(self.intervention_history),
            },
            "parameter_rows": self._build_parameter_rows(),
            "role_structure": {
                "income_tiers": tier_rows,
                "zone_a_price_range": self.config.get_zone_price_range("A"),
                "zone_b_price_range": self.config.get_zone_price_range("B"),
                "user_property_count": self.config.get("user_property_count", None),
            },
            "applied_overrides": self.config._config.get("_applied_startup_overrides", {}),
            "latest_results": {
                "last_month_summary": self.last_month_summary,
                "final_summary": self.final_summary,
                "export_report": export_report,
            },
        }

    def write_parameter_assumption_report(self):
        report = self.build_parameter_assumption_report()
        paths = self._parameter_assumption_artifact_paths()
        with open(paths["json_path"], "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        experiment = report["experiment_info"]
        rows = report["parameter_rows"]
        tier_rows = report["role_structure"]["income_tiers"]
        lines = [
            "# 参数与假设说明表",
            "",
            f"- run_id: `{experiment['run_id']}`",
            f"- 数据库路径: `{experiment['db_path']}`",
            f"- metadata.json: `{experiment['metadata_path']}`",
            f"- 生成时间: `{experiment['generated_at']}`",
            f"- Agent 数量: `{experiment['agent_count']}`",
            f"- 模拟月数: `{experiment['months']}`",
            f"- 随机种子: `{experiment['seed']}`",
            f"- 含夜跑预设干预: `{experiment['has_preplanned_interventions']}`",
            f"- 含运行期调整: `{experiment['has_runtime_adjustments']}`",
            "",
            "## 1. 参数总表（自动生成）",
            "",
            "| 参数键 | 中文名称 | 当前值 | 参数类别 | 来源类别 | 来源说明 | 为什么这样设 | 调高影响 | 调低影响 | 是否关键 | 当前信心 | 后续是否需要校准 |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for row in rows:
            lines.append(
                "| `{parameter_key}` | {label} | {current_value} | {parameter_category} | {source_category} | {source_explanation} | {why_set} | {high_impact} | {low_impact} | {is_key} | {confidence} | {needs_calibration} |".format(
                    parameter_key=row["parameter_key"],
                    label=row["label"],
                    current_value=str(row["current_value"]).replace("|", "/"),
                    parameter_category=row["parameter_category"],
                    source_category=row["source_category"],
                    source_explanation=str(row["source_explanation"]).replace("|", "/"),
                    why_set=str(row["why_set"]).replace("|", "/"),
                    high_impact=str(row["high_impact"]).replace("|", "/"),
                    low_impact=str(row["low_impact"]).replace("|", "/"),
                    is_key="是" if row["is_key"] else "否",
                    confidence=row["confidence"],
                    needs_calibration="是" if row["needs_calibration"] else "否",
                )
            )

        lines.extend(
            [
                "",
                "## 2. 角色结构说明（自动生成）",
                "",
                "| 收入档 | 人数 | 收入下限 | 收入上限 | 拥房最小值 | 拥房最大值 |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in tier_rows:
            lines.append(
                f"| {row.get('tier')} | {row.get('count')} | {row.get('income_min')} | {row.get('income_max')} | {row.get('property_min')} | {row.get('property_max')} |"
            )

        lines.extend(
            [
                "",
                "## 3. 本轮结果摘要（自动生成）",
                "",
                "```json",
                json.dumps(report["latest_results"], ensure_ascii=False, indent=2, default=str),
                "```",
            ]
        )

        with open(paths["markdown_path"], "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        logger.info(f"✅ 参数与假设说明表已更新: {paths['markdown_path']}")
        return {
            "markdown_path": paths["markdown_path"].replace("\\", "/"),
            "json_path": paths["json_path"].replace("\\", "/"),
        }

    def build_motivation_agent_report(self) -> Dict[str, object]:
        cursor = self.conn.cursor()
        tables = {
            row[0]
            for row in cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        has_agent_end_reports = "agent_end_reports" in tables

        total_tx = int(
            (cursor.execute("SELECT COUNT(*) FROM transactions").fetchone() or [0])[0] or 0
        )
        total_buyers = int(
            (cursor.execute("SELECT COUNT(DISTINCT buyer_id) FROM transactions").fetchone() or [0])[0] or 0
        )

        cursor.execute(
            """
            SELECT
              COALESCE(NULLIF(s.purchase_motive_primary, ''), 'unknown') AS motive,
              COUNT(*) AS tx_count,
              COUNT(DISTINCT t.buyer_id) AS buyer_count,
              ROUND(AVG(t.final_price), 2) AS avg_price
            FROM transactions t
            JOIN agents_static s ON s.agent_id = t.buyer_id
            GROUP BY COALESCE(NULLIF(s.purchase_motive_primary, ''), 'unknown')
            ORDER BY tx_count DESC, buyer_count DESC
            """
        )
        motive_rows = [
            {
                "motive": str(r[0]),
                "tx_count": int(r[1] or 0),
                "buyer_count": int(r[2] or 0),
                "avg_price": float(r[3] or 0),
            }
            for r in cursor.fetchall()
        ]

        # 按买家聚合，附带最近画像与激活触发词，便于研究者追踪个体。
        if has_agent_end_reports:
            cursor.execute(
                """
                SELECT
                  t.buyer_id AS agent_id,
                  s.name AS agent_name,
                  COALESCE(NULLIF(s.purchase_motive_primary, ''), 'unknown') AS motive,
                  COUNT(*) AS tx_count,
                  ROUND(SUM(t.final_price), 2) AS total_amount,
                  ROUND(AVG(t.final_price), 2) AS avg_price,
                  GROUP_CONCAT(DISTINCT t.property_id) AS property_ids,
                  GROUP_CONCAT(DISTINCT t.month) AS months,
                  COALESCE(MAX(ap.activation_trigger), '') AS latest_activation_trigger,
                  COALESCE(lp.llm_portrait, '') AS llm_portrait
                FROM transactions t
                JOIN agents_static s ON s.agent_id = t.buyer_id
                LEFT JOIN active_participants ap
                  ON ap.agent_id = t.buyer_id
                 AND ap.month = t.month
                 AND LOWER(ap.role) = 'buyer'
                LEFT JOIN (
                  SELECT aer1.agent_id, aer1.llm_portrait
                  FROM agent_end_reports aer1
                  WHERE aer1.report_id = (
                    SELECT MAX(aer2.report_id)
                    FROM agent_end_reports aer2
                    WHERE aer2.agent_id = aer1.agent_id
                  )
                ) lp ON lp.agent_id = t.buyer_id
                GROUP BY t.buyer_id, s.name, motive, lp.llm_portrait
                ORDER BY tx_count DESC, total_amount DESC
                """
            )
        else:
            cursor.execute(
                """
                SELECT
                  t.buyer_id AS agent_id,
                  s.name AS agent_name,
                  COALESCE(NULLIF(s.purchase_motive_primary, ''), 'unknown') AS motive,
                  COUNT(*) AS tx_count,
                  ROUND(SUM(t.final_price), 2) AS total_amount,
                  ROUND(AVG(t.final_price), 2) AS avg_price,
                  GROUP_CONCAT(DISTINCT t.property_id) AS property_ids,
                  GROUP_CONCAT(DISTINCT t.month) AS months,
                  COALESCE(MAX(ap.activation_trigger), '') AS latest_activation_trigger,
                  '' AS llm_portrait
                FROM transactions t
                JOIN agents_static s ON s.agent_id = t.buyer_id
                LEFT JOIN active_participants ap
                  ON ap.agent_id = t.buyer_id
                 AND ap.month = t.month
                 AND LOWER(ap.role) = 'buyer'
                GROUP BY t.buyer_id, s.name, motive
                ORDER BY tx_count DESC, total_amount DESC
                """
            )
        buyer_rows = []
        for r in cursor.fetchall():
            buyer_rows.append(
                {
                    "agent_id": int(r[0]),
                    "agent_name": str(r[1] or ""),
                    "motive": str(r[2] or "unknown"),
                    "tx_count": int(r[3] or 0),
                    "total_amount": float(r[4] or 0),
                    "avg_price": float(r[5] or 0),
                    "property_ids": str(r[6] or ""),
                    "months": str(r[7] or ""),
                    "latest_activation_trigger": str(r[8] or ""),
                    "llm_portrait": str(r[9] or ""),
                }
            )

        return {
            "run_id": self._run_id(),
            "generated_at": datetime.datetime.now().isoformat(),
            "db_path": str(self.db_path),
            "summary": {
                "transaction_count": total_tx,
                "buyer_count": total_buyers,
            },
            "motivation_summary": motive_rows,
            "buyer_details": buyer_rows,
        }

    def write_motivation_agent_report(self) -> Dict[str, str]:
        report = self.build_motivation_agent_report()
        paths = self._motivation_report_artifact_paths()

        with open(paths["json_path"], "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        lines = [
            "# 动机分层与Agent明细报告",
            "",
            f"- run_id: `{report['run_id']}`",
            f"- 数据库: `{report['db_path']}`",
            f"- 生成时间: `{report['generated_at']}`",
            f"- 成交笔数: `{report['summary']['transaction_count']}`",
            f"- 成交买家数: `{report['summary']['buyer_count']}`",
            "",
            "## 1. 动机分层统计",
            "",
            "| 主购房动机 | 成交笔数 | 成交买家数 | 成交均价 |",
            "| --- | --- | --- | --- |",
        ]

        for row in report["motivation_summary"]:
            lines.append(
                f"| {row['motive']} | {row['tx_count']} | {row['buyer_count']} | {row['avg_price']:.0f} |"
            )

        lines.extend(
            [
                "",
                "## 2. Agent 明细（按成交笔数排序）",
                "",
                "| Agent ID | 姓名 | 主动机 | 成交笔数 | 成交总额 | 成交均价 | 成交月份 | 成交房产ID | 最近激活触发词 | 画像摘要 |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )

        for row in report["buyer_details"]:
            portrait = (row["llm_portrait"] or "").replace("\n", " ").strip()
            if len(portrait) > 80:
                portrait = portrait[:80] + "..."
            lines.append(
                "| {agent_id} | {agent_name} | {motive} | {tx_count} | {total_amount:.0f} | {avg_price:.0f} | {months} | {property_ids} | {trigger} | {portrait} |".format(
                    agent_id=row["agent_id"],
                    agent_name=row["agent_name"],
                    motive=row["motive"],
                    tx_count=row["tx_count"],
                    total_amount=row["total_amount"],
                    avg_price=row["avg_price"],
                    months=(row["months"] or "-"),
                    property_ids=(row["property_ids"] or "-"),
                    trigger=(row["latest_activation_trigger"] or "-").replace("|", "/"),
                    portrait=(portrait or "-").replace("|", "/"),
                )
            )

        with open(paths["markdown_path"], "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        logger.info(f"✅ 动机分层与Agent明细报告已生成: {paths['markdown_path']}")
        return {
            "markdown_path": paths["markdown_path"].replace("\\", "/"),
            "json_path": paths["json_path"].replace("\\", "/"),
        }

    def _resolve_db_path(self, db_path):
        """Create an isolated run directory when no DB path is provided."""
        if db_path:
            return str(db_path)

        ts = datetime.datetime.now().strftime("run_%Y%m%d_%H%M%S")
        # Keep all auto-created run artifacts under a dedicated child folder.
        run_dir = os.path.join("results", "runs", ts)
        os.makedirs(run_dir, exist_ok=True)
        return os.path.join(run_dir, "simulation.db")

    def _configure_run_logging(self):
        """Attach a per-run file handler so each run has isolated artifacts."""
        log_path = os.path.join(self._run_dir, "simulation_run.log")
        root_logger = logging.getLogger()
        for handler in list(root_logger.handlers):
            if isinstance(handler, logging.FileHandler):
                base = ""
                try:
                    base = os.path.abspath(getattr(handler, "baseFilename", ""))
                except Exception:
                    base = ""
                if base == os.path.abspath(log_path):
                    self._run_log_handler = handler
                    return
        try:
            handler = logging.FileHandler(log_path, encoding="utf-8", mode="a")
            handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
            root_logger.addHandler(handler)
            self._run_log_handler = handler
        except OSError:
            self._run_log_handler = None

    def _apply_llm_runtime_env(self):
        """
        Bridge config -> env for LLM runtime controls.
        This keeps llm_client decoupled from SimulationConfig while allowing
        reproducible tuning from baseline.yaml / project config.
        """
        mapping = {
            "system.llm.max_concurrency_smart": "LLM_MAX_CONCURRENCY_SMART",
            "system.llm.max_concurrency_fast": "LLM_MAX_CONCURRENCY_FAST",
            "system.llm.qps_smart": "LLM_QPS_SMART",
            "system.llm.qps_fast": "LLM_QPS_FAST",
            "system.llm.timeout_seconds": "LLM_TIMEOUT_SECONDS",
            "system.llm.max_retries": "LLM_MAX_RETRIES",
            "system.llm.backoff_base_seconds": "LLM_BACKOFF_BASE_SECONDS",
            "system.llm.breaker_fail_threshold": "LLM_BREAKER_FAIL_THRESHOLD",
            "system.llm.breaker_cooldown_seconds": "LLM_BREAKER_COOLDOWN_SECONDS",
            "system.llm.enable_caching": "LLM_ENABLE_CACHE",
            "system.llm.cache_max_size": "LLM_CACHE_MAX_SIZE",
        }
        for key, env_key in mapping.items():
            try:
                value = self.config.get(key, None)
            except Exception:
                value = None
            if value is None:
                continue
            # External env overrides (for A/B perf experiments) take precedence.
            if env_key in os.environ and str(os.environ.get(env_key, "")).strip() != "":
                continue
            os.environ[env_key] = str(value)

    def generate_experiment_card(self):
        """Generate experiment metadata.json for reproducibility"""
        import json
        import datetime
        import subprocess
        
        metadata = {
            "experiment_id": os.path.basename(os.path.dirname(self.db_path)),
            "created_at": datetime.datetime.now().isoformat(),
            "seed": self.seed,
            "agent_count": self.agent_count,
            "months": self.months,
            "config_path": str(getattr(self.config, 'config_path', 'baseline.yaml')),
            "db_path": str(self.db_path)  # Convert Path to string for JSON serialization
        }
        forced_role_mode = self.config.get(
            "smart_agent.forced_role_mode",
            self.config.get("forced_role_mode", {}),
        )
        if isinstance(forced_role_mode, dict):
            metadata["forced_role_mode"] = forced_role_mode
        
        # Try to get git commit
        try:
            commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], 
                                            stderr=subprocess.DEVNULL).decode().strip()
            metadata["git_commit"] = commit
        except Exception:
            metadata["git_commit"] = "N/A"
        
        # Save to results directory
        results_dir = os.path.dirname(self.db_path)
        metadata_path = os.path.join(results_dir, "metadata.json")
        
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        logger.info(f"✅ 实验卡片已生成: {metadata_path}")
        self.write_parameter_assumption_report()

    def set_interventions(self, news_items: List[str]):
        """Set interventions for the upcoming month."""
        self.pending_interventions = news_items

    def _apply_preplanned_interventions(self, month: int):
        """
        Apply config-driven monthly interventions (non-interactive).
        Supported action_type:
        - developer_supply: inject developer listings in target month
        - population_add: inject new agents in target month
        - income_shock: apply wage shock in target month
        """
        plans = self.config.get("simulation.preplanned_interventions", [])
        if not isinstance(plans, list) or not plans:
            return

        for idx, plan in enumerate(plans):
            if not isinstance(plan, dict):
                continue
            try:
                target_month = int(plan.get("month", -1))
            except Exception:
                target_month = -1
            if target_month != int(month):
                continue

            key = f"{idx}:{target_month}:{plan.get('action_type','')}"
            if key in self._applied_preplanned_interventions:
                continue

            action_type = str(plan.get("action_type", "")).strip().lower()
            try:
                if action_type == "developer_supply":
                    zone = str(plan.get("zone", "A")).upper()
                    count = int(plan.get("count", 0))
                    template = str(plan.get("template", "") or "").strip().lower() or None
                    price_per_sqm = float(plan.get("price_per_sqm")) if plan.get("price_per_sqm") is not None else None
                    size = float(plan.get("size")) if plan.get("size") is not None else None
                    school_units = int(plan.get("school_units")) if plan.get("school_units") is not None else None
                    build_year = int(plan.get("build_year")) if plan.get("build_year") is not None else None

                    if zone not in ("A", "B") or count <= 0:
                        logger.warning(f"Skip invalid preplanned developer supply: {plan}")
                        continue

                    result = self.inject_developer_supply_intervention(
                        count=count,
                        zone=zone,
                        template=template,
                        price_per_sqm=price_per_sqm,
                        size=size,
                        school_units=school_units,
                        build_year=build_year,
                        target_month_override=month,
                    )
                    news = (
                        f"Preplanned intervention executed: developer supplied {int(result.get('count', count))} units "
                        f"in Zone {zone} at month {month}."
                    )
                elif action_type == "population_add":
                    count = int(plan.get("count", 0))
                    tier = str(plan.get("tier", "lower_middle")).strip().lower()
                    template = str(plan.get("template", "") or "").strip().lower() or None
                    income_multiplier = float(plan.get("income_multiplier")) if plan.get("income_multiplier") is not None else None
                    income_multiplier_min = (
                        float(plan.get("income_multiplier_min"))
                        if plan.get("income_multiplier_min") is not None
                        else None
                    )
                    income_multiplier_max = (
                        float(plan.get("income_multiplier_max"))
                        if plan.get("income_multiplier_max") is not None
                        else None
                    )
                    if count <= 0:
                        logger.warning(f"Skip invalid preplanned population_add: {plan}")
                        continue

                    result = self.add_population_intervention(
                        count=count,
                        tier=tier,
                        template=template,
                        income_multiplier=income_multiplier,
                        income_multiplier_min=income_multiplier_min,
                        income_multiplier_max=income_multiplier_max,
                        target_month_override=month,
                    )
                    news = (
                        f"Preplanned intervention executed: added {int(result.get('added_count', count))} "
                        f"{tier} agents at month {month}."
                    )
                elif action_type == "income_shock":
                    pct_change = float(plan.get("pct_change")) if plan.get("pct_change") is not None else None
                    target_tier = str(plan.get("target_tier", "all")).strip().lower()
                    tier_adjustments = plan.get("tier_adjustments")
                    if pct_change is None and not tier_adjustments:
                        logger.warning(f"Skip invalid preplanned income_shock: {plan}")
                        continue

                    result = self.apply_income_intervention(
                        pct_change=pct_change,
                        target_tier=target_tier,
                        tier_adjustments=tier_adjustments,
                        target_month_override=month,
                    )
                    news = (
                        "Preplanned intervention executed: income shock applied "
                        f"at month {month}."
                    )
                else:
                    logger.warning(f"Skip unsupported preplanned intervention action_type: {plan}")
                    continue

                self.pending_interventions.append(news)
                logger.info(news)
                self._applied_preplanned_interventions.add(key)
            except Exception as e:
                logger.error(f"Failed preplanned intervention {plan}: {e}")

    def initialize(self):
        """Initialize Simulation State"""
        if getattr(self, '_initialized', False):
            return

        self.status = "initializing"
        if self.started_at is None:
            self.started_at = datetime.datetime.now().isoformat()

        # Ensure deterministic random stream for both fresh and resume runs.
        # Resume mode skips market/agent initialization, but monthly stochastic
        # branches still rely on global random state.
        import random
        random.seed(self.seed)

        if self.resume:
            self.load_from_db()
            self.current_month = self.get_last_simulation_month()
            self._initialized = True
            self.status = "initialized"
            return

        logger.info(f"Initializing Simulation with Seed: {self.seed}")

        try:
            # 1. Initialize Market
            properties = self.market_service.initialize_market()

            # 2. Initialize Agents (and allocate properties)
            self.agent_service.initialize_agents(self.agent_count, properties)
            self.mortgage_risk_service.seed_existing_mortgages(self.agent_service.agents, month=0)

            # Show Summary
            wf_logger = WorkflowLogger(self.config)
            wf_logger.show_agent_generation_summary(self.agent_service.agents, sample_size=3)
            
            self._initialized = True
            self.current_month = 0
            self.status = "initialized"

        except Exception as e:
            self.status = "failed"
            self.last_error = str(e)
            logger.error(f"Initialization Failed: {e}")
            raise

    def load_from_db(self):
        """Load state from DB"""
        from database import migrate_db_v2_7

        # Ensure Schema is up to date (V2.7)
        migrate_db_v2_7(self.db_path)

        self.agent_service.load_agents_from_db()
        delay_stats = self.agent_service.refresh_info_delay_assignments()
        self.market_service.load_market_from_db(self.agent_service.agents)
        self.mortgage_risk_service.sync_agent_finance_from_mortgages(agent_ids=list(self.agent_service.agent_map.keys()))
        logger.info(
            "Resume info-delay assignments refreshed: delayed=%s/%s",
            int(delay_stats.get("delayed_total", 0)),
            int(delay_stats.get("total_agents", 0)),
        )

    def get_last_simulation_month(self) -> int:
        """Get the last simulated month from DB."""
        try:
            cursor = self.conn.cursor()
            # Check decision_logs or transactions
            cursor.execute("SELECT MAX(month) FROM decision_logs")
            result = cursor.fetchone()
            if result and result[0]:
                return int(result[0])
            return 0
        except Exception as e:
            logger.warning(f"Could not determine last month: {e}")
            return 0

    def _build_status_snapshot(self) -> Dict[str, Optional[object]]:
        remaining_months = max(0, int(self.months) - int(self.current_month))
        return {
            "status": self.status,
            "initialized": bool(self._initialized),
            "current_month": int(self.current_month),
            "total_months": int(self.months),
            "remaining_months": int(remaining_months),
            "db_path": str(self.db_path),
            "run_dir": str(self._run_dir),
            "last_error": self.last_error,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "last_month_summary": self.last_month_summary,
            "final_summary": self.final_summary,
            "intervention_history": list(self.intervention_history[-20:]),
            "runtime_controls": self.get_runtime_controls(),
            "stage_snapshot": self.get_stage_snapshot(),
            "stage_replay_events": self.get_stage_replay_events(),
        }

    def get_status(self) -> Dict[str, Optional[object]]:
        return self._build_status_snapshot()

    def set_progress_callback(self, callback: Optional[Callable[[Dict[str, Any]], None]]):
        self.progress_callback = callback

    def _emit_progress(self, stage: str, message: str, month: Optional[int] = None, detail: Optional[Dict[str, object]] = None):
        callback = self.progress_callback
        if callback is None:
            return
        snapshot = self._build_status_snapshot()
        payload = {
            "stage": str(stage),
            "message": str(message),
            "month": int(month if month is not None else snapshot.get("current_month", 0) or 0),
            "detail": detail or {},
            "status": snapshot,
        }
        try:
            callback(payload)
        except Exception as exc:
            logger.debug(f"Progress callback failed: {exc}")

    def get_runtime_controls(self) -> Dict[str, object]:
        return {
            "down_payment_ratio": float(self.config.get("mortgage.down_payment_ratio", 0.3) or 0.3),
            "annual_interest_rate": float(self.config.get("mortgage.annual_interest_rate", 0.035) or 0.035),
            "max_dti_ratio": float(self.config.get("mortgage.max_dti_ratio", 0.5) or 0.5),
            "market_pulse_enabled": bool(self.config.get("market_pulse.enabled", False)),
            "macro_override_mode": self.config.get("macro_environment.override_mode", None),
            "negotiation_quote_stream_enabled": bool(self.config.get("negotiation.quote_stream_enabled", False)),
            "negotiation_quote_filter_mode": str(self.config.get("negotiation.quote_filter_mode", "all") or "all"),
            "negotiation_quote_mode": str(self.config.get("negotiation.quote_mode", "limited_quotes") or "limited_quotes"),
            "negotiation_quote_turn_limit": int(self.config.get("negotiation.quote_turn_limit", 4) or 4),
            "negotiation_quote_char_limit": int(self.config.get("negotiation.quote_char_limit", 84) or 84),
        }

    def _run_id(self) -> str:
        return os.path.basename(str(self._run_dir or "")) or "run"

    def _build_agent_generated_events(self, agent_ids: List[int], month: int = 0, phase: str = "system") -> List[Dict[str, object]]:
        if not self.conn or not agent_ids:
            return []

        normalized_ids = sorted({int(agent_id) for agent_id in agent_ids})
        placeholders = ",".join("?" for _ in normalized_ids)
        cursor = self.conn.cursor()
        cursor.execute(
            f"""
            SELECT agent_id, name, occupation, agent_type
            FROM agents_static
            WHERE agent_id IN ({placeholders})
            ORDER BY agent_id
            """,
            tuple(normalized_ids),
        )

        run_id = self._run_id()
        ts = datetime.datetime.now().isoformat()
        events: List[Dict[str, object]] = []
        for row in cursor.fetchall() or []:
            agent_id = int(row[0])
            events.append(
                {
                    "event_id": f"{run_id}:m{int(month)}:agent_generated:{agent_id}",
                    "run_id": run_id,
                    "month": int(month),
                    "phase": phase,
                    "event_type": "AGENT_GENERATED",
                    "ts": ts,
                    "payload": {
                        "agent_id": agent_id,
                        "name": str(row[1] or ""),
                        "occupation": str(row[2] or ""),
                        "agent_type": str(row[3] or "normal"),
                    },
                    "source": "simulation_runner",
                    "schema_version": "v1",
                }
            )
        return events

    def _build_property_generated_events(self, property_ids: List[int], month: int = 0, phase: str = "system") -> List[Dict[str, object]]:
        if not self.conn or not property_ids:
            return []

        normalized_ids = sorted({int(property_id) for property_id in property_ids})
        placeholders = ",".join("?" for _ in normalized_ids)
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM properties_static")
        total_properties = int((cursor.fetchone() or [0])[0] or 0)
        cursor.execute(
            f"""
            SELECT property_id, zone, property_type, is_school_district
            FROM properties_static
            WHERE property_id IN ({placeholders})
            ORDER BY property_id
            """,
            tuple(normalized_ids),
        )

        run_id = self._run_id()
        ts = datetime.datetime.now().isoformat()
        events: List[Dict[str, object]] = []
        for row in cursor.fetchall() or []:
            property_id = int(row[0])
            events.append(
                {
                    "event_id": f"{run_id}:m{int(month)}:property_generated:{property_id}",
                    "run_id": run_id,
                    "month": int(month),
                    "phase": phase,
                    "event_type": "PROPERTY_GENERATED",
                    "ts": ts,
                    "payload": {
                        "property_id": property_id,
                        "zone": str(row[1] or ""),
                        "property_type": str(row[2] or ""),
                        "is_school_district": bool(row[3]),
                        "display_only": True,
                        "display_total_properties": total_properties,
                    },
                    "source": "simulation_runner",
                    "schema_version": "v1",
                }
            )
        return events

    def _build_property_listed_events(self, property_ids: List[int], month: int, phase: str = "listing") -> List[Dict[str, object]]:
        if not self.conn or not property_ids:
            return []

        normalized_ids = sorted({int(property_id) for property_id in property_ids})
        placeholders = ",".join("?" for _ in normalized_ids)
        cursor = self.conn.cursor()
        cursor.execute(
            f"""
            SELECT pm.property_id, pm.owner_id, pm.listed_price, pm.status, ps.zone, ps.property_type, ps.is_school_district
            FROM properties_market pm
            JOIN properties_static ps ON ps.property_id = pm.property_id
            WHERE pm.property_id IN ({placeholders})
            ORDER BY pm.property_id
            """,
            tuple(normalized_ids),
        )

        run_id = self._run_id()
        ts = datetime.datetime.now().isoformat()
        events: List[Dict[str, object]] = []
        for row in cursor.fetchall() or []:
            property_id = int(row[0])
            events.append(
                {
                    "event_id": f"{run_id}:m{int(month)}:property_listed:{property_id}",
                    "run_id": run_id,
                    "month": int(month),
                    "phase": phase,
                    "event_type": "PROPERTY_LISTED",
                    "ts": ts,
                    "payload": {
                        "property_id": property_id,
                        "owner_id": int(row[1] or -1),
                        "listed_price": float(row[2] or 0.0),
                        "status": str(row[3] or ""),
                        "zone": str(row[4] or ""),
                        "property_type": str(row[5] or ""),
                        "is_school_district": bool(row[6]),
                    },
                    "source": "simulation_runner",
                    "schema_version": "v1",
                }
            )
        return events

    def _record_intervention(
        self,
        event_type: str,
        summary: str,
        payload: Dict[str, object],
        target_month_override: Optional[int] = None,
    ) -> Dict[str, object]:
        target_month = int(target_month_override) if target_month_override is not None else int(self.current_month)
        if target_month_override is None and self.status in {"initialized", "paused"} and int(self.current_month) < int(self.months):
            target_month = int(self.current_month) + 1
        record = {
            "month": target_month,
            "event_type": str(event_type),
            "summary": str(summary),
            "payload": dict(payload),
            "ts": datetime.datetime.now().isoformat(),
        }
        self.intervention_history.append(record)
        self.intervention_history = self.intervention_history[-50:]
        return record

    def record_scenario_preset(self, preset: str) -> Dict[str, object]:
        normalized_preset = str(preset or "").strip().lower()
        return self._record_intervention(
            "SCENARIO_PRESET_APPLIED",
            f"Scenario preset applied: {normalized_preset}",
            {"preset": normalized_preset},
        )

    def apply_runtime_controls(
        self,
        *,
        down_payment_ratio: Optional[float] = None,
        annual_interest_rate: Optional[float] = None,
        max_dti_ratio: Optional[float] = None,
        market_pulse_enabled: Optional[bool] = None,
        macro_override_mode: Optional[str] = None,
        negotiation_quote_stream_enabled: Optional[bool] = None,
        negotiation_quote_filter_mode: Optional[str] = None,
        negotiation_quote_mode: Optional[str] = None,
        negotiation_quote_turn_limit: Optional[int] = None,
        negotiation_quote_char_limit: Optional[int] = None,
    ) -> Dict[str, object]:
        if down_payment_ratio is not None:
            value = float(down_payment_ratio)
            if value <= 0 or value >= 1:
                raise ValueError("down_payment_ratio must be between 0 and 1.")
            self.config.update("mortgage.down_payment_ratio", value)

        if annual_interest_rate is not None:
            value = float(annual_interest_rate)
            if value <= 0 or value >= 1:
                raise ValueError("annual_interest_rate must be between 0 and 1.")
            self.config.update("mortgage.annual_interest_rate", value)

        if max_dti_ratio is not None:
            value = float(max_dti_ratio)
            if value <= 0 or value >= 1:
                raise ValueError("max_dti_ratio must be between 0 and 1.")
            self.config.update("mortgage.max_dti_ratio", value)

        if market_pulse_enabled is not None:
            self.config.update("market_pulse.enabled", bool(market_pulse_enabled))

        if macro_override_mode is not None:
            normalized = None if str(macro_override_mode).strip() in {"", "null", "none"} else str(macro_override_mode).strip().lower()
            if normalized is not None and normalized not in MACRO_ENVIRONMENT:
                raise ValueError(f"macro_override_mode must be one of: {', '.join(sorted(MACRO_ENVIRONMENT.keys()))}")
            self.config.update("macro_environment.override_mode", normalized)

        if negotiation_quote_stream_enabled is not None:
            self.config.update("negotiation.quote_stream_enabled", bool(negotiation_quote_stream_enabled))

        if negotiation_quote_filter_mode is not None:
            normalized_mode = str(negotiation_quote_filter_mode or "").strip().lower() or "all"
            if normalized_mode not in {"all", "focused", "heated_only", "high_value_only"}:
                raise ValueError("negotiation_quote_filter_mode must be one of: all, focused, heated_only, high_value_only.")
            self.config.update("negotiation.quote_filter_mode", normalized_mode)

        if negotiation_quote_mode is not None:
            normalized_mode = str(negotiation_quote_mode or "").strip().lower() or "limited_quotes"
            if normalized_mode not in {"off", "summary", "limited_quotes", "full_quotes"}:
                raise ValueError("negotiation_quote_mode must be one of: off, summary, limited_quotes, full_quotes.")
            self.config.update("negotiation.quote_mode", normalized_mode)

        if negotiation_quote_turn_limit is not None:
            value = int(negotiation_quote_turn_limit)
            if value < 1 or value > 20:
                raise ValueError("negotiation_quote_turn_limit must be between 1 and 20.")
            self.config.update("negotiation.quote_turn_limit", value)

        if negotiation_quote_char_limit is not None:
            value = int(negotiation_quote_char_limit)
            if value < 20 or value > 500:
                raise ValueError("negotiation_quote_char_limit must be between 20 and 500.")
            self.config.update("negotiation.quote_char_limit", value)

        return self.get_runtime_controls()

    def _should_emit_negotiation_quotes(
        self,
        *,
        round_count: int,
        success: bool,
        final_price: float,
        listing_price: float,
    ) -> bool:
        mode = str(self.config.get("negotiation.quote_filter_mode", "all") or "all").strip().lower()
        if mode == "all":
            return True

        is_heated = int(round_count) >= 3 or not bool(success)
        has_price_premium = float(listing_price or 0.0) > 0 and float(final_price or 0.0) >= float(listing_price) * 1.05
        if mode == "heated_only":
            return is_heated
        if mode == "high_value_only":
            return has_price_premium
        return is_heated or has_price_premium

    def add_population_intervention(
        self,
        *,
        count: int,
        tier: str,
        template: Optional[str] = None,
        income_multiplier: Optional[float] = None,
        income_multiplier_min: Optional[float] = None,
        income_multiplier_max: Optional[float] = None,
        target_month_override: Optional[int] = None,
    ) -> Dict[str, object]:
        normalized_count = int(count)
        normalized_tier = str(tier or "").strip().lower()
        if normalized_count <= 0:
            raise ValueError("count must be greater than 0.")
        if normalized_tier not in {"low", "lower_middle", "middle", "upper_middle", "high", "ultra_high"}:
            raise ValueError("tier must be one of: low, lower_middle, middle, upper_middle, high, ultra_high.")

        normalized_template = str(template or "").strip().lower() or None
        template_defaults = {
            "young_first_home": {
                "tier": "lower_middle",
                "income_multiplier_min": 0.85,
                "income_multiplier_max": 1.05,
            },
            "middle_upgrade": {
                "tier": "middle",
                "income_multiplier_min": 0.95,
                "income_multiplier_max": 1.20,
            },
            "capital_investor": {
                "tier": "high",
                "income_multiplier_min": 1.10,
                "income_multiplier_max": 1.50,
            },
        }
        if normalized_template is not None:
            if normalized_template not in template_defaults:
                raise ValueError("template must be one of: young_first_home, middle_upgrade, capital_investor.")
            defaults = template_defaults[normalized_template]
            normalized_tier = str(defaults["tier"])
            if income_multiplier is None and income_multiplier_min is None and income_multiplier_max is None:
                income_multiplier_min = float(defaults["income_multiplier_min"])
                income_multiplier_max = float(defaults["income_multiplier_max"])

        before_ids = {int(agent.id) for agent in self.agent_service.agents}
        added_count = int(self.intervention_service.add_population(self.agent_service, count=normalized_count, tier=normalized_tier))
        new_ids = sorted(int(agent.id) for agent in self.agent_service.agents if int(agent.id) not in before_ids)

        applied_multiplier = None
        multiplier_range = None
        if income_multiplier is not None and (income_multiplier_min is not None or income_multiplier_max is not None):
            raise ValueError("Use either income_multiplier or income_multiplier_min/income_multiplier_max, not both.")
        if income_multiplier_min is not None or income_multiplier_max is not None:
            if income_multiplier_min is None or income_multiplier_max is None:
                raise ValueError("income_multiplier_min and income_multiplier_max must be provided together.")
            min_multiplier = float(income_multiplier_min)
            max_multiplier = float(income_multiplier_max)
            if min_multiplier <= 0 or max_multiplier <= 0 or min_multiplier > max_multiplier:
                raise ValueError("income multiplier range must be positive and min must be <= max.")
            multiplier_range = {"min": min_multiplier, "max": max_multiplier}
            if new_ids:
                cursor = self.conn.cursor()
                updates = []
                for agent_id in new_ids:
                    agent = self.agent_service.agent_map.get(agent_id)
                    if agent is None:
                        continue
                    sampled_multiplier = random.uniform(min_multiplier, max_multiplier)
                    agent.monthly_income = float(agent.monthly_income) * sampled_multiplier
                    updates.append((float(agent.monthly_income), agent_id))
                if updates:
                    cursor.executemany("UPDATE agents_finance SET monthly_income=? WHERE agent_id=?", updates)
                    self.conn.commit()
        elif income_multiplier is not None:
            applied_multiplier = float(income_multiplier)
            if applied_multiplier <= 0:
                raise ValueError("income_multiplier must be greater than 0.")
            if new_ids:
                cursor = self.conn.cursor()
                updates = []
                for agent_id in new_ids:
                    agent = self.agent_service.agent_map.get(agent_id)
                    if agent is None:
                        continue
                    agent.monthly_income = float(agent.monthly_income) * applied_multiplier
                    updates.append((float(agent.monthly_income), agent_id))
                if updates:
                    cursor.executemany("UPDATE agents_finance SET monthly_income=? WHERE agent_id=?", updates)
                    self.conn.commit()

        history_entry = self._record_intervention(
            "POPULATION_ADDED",
            f"Added {added_count} {normalized_tier} agents",
            {
                "added_count": added_count,
                "tier": normalized_tier,
                "template": normalized_template,
                "income_multiplier": applied_multiplier,
                "income_multiplier_range": multiplier_range,
            },
            target_month_override=target_month_override,
        )

        return {
            "added_count": added_count,
            "tier": normalized_tier,
            "template": normalized_template,
            "income_multiplier": applied_multiplier,
            "income_multiplier_range": multiplier_range,
            "history_entry": history_entry,
            "generated_events": self._build_agent_generated_events(new_ids, month=int(self.current_month), phase="system"),
        }

    def apply_income_intervention(
        self,
        *,
        pct_change: Optional[float] = None,
        target_tier: str = "all",
        tier_adjustments: Optional[List[Dict[str, object]]] = None,
        target_month_override: Optional[int] = None,
    ) -> Dict[str, object]:
        allowed_tiers = {"all", "low", "lower_middle", "middle", "upper_middle", "high", "ultra_high"}
        normalized_tier = str(target_tier or "all").strip().lower()
        if normalized_tier not in allowed_tiers:
            raise ValueError("target_tier must be one of: all, low, lower_middle, middle, upper_middle, high, ultra_high.")

        if tier_adjustments:
            results = []
            total_updated = 0
            normalized_adjustments = []
            for item in tier_adjustments:
                tier_name = str(getattr(item, "tier", None) or (item.get("tier") if isinstance(item, dict) else "")).strip().lower()
                if tier_name not in allowed_tiers or tier_name == "all":
                    raise ValueError("tier_adjustments tier must be one of: low, lower_middle, middle, upper_middle, high, ultra_high.")
                tier_pct = float(getattr(item, "pct_change", None) if not isinstance(item, dict) else item.get("pct_change"))
                updated_count = int(
                    self.intervention_service.apply_wage_shock(
                        self.agent_service,
                        pct_change=tier_pct,
                        target_tier=tier_name,
                    )
                )
                total_updated += updated_count
                normalized_adjustments.append({"tier": tier_name, "pct_change": tier_pct, "updated_count": updated_count})
                results.append(f"{tier_name} {tier_pct:+.2%}")
            history_entry = self._record_intervention(
                "INCOME_SHOCK_APPLIED",
                f"Tiered income shock: {', '.join(results)}",
                {
                    "updated_count": total_updated,
                    "tier_adjustments": normalized_adjustments,
                },
                target_month_override=target_month_override,
            )
            return {
                "updated_count": total_updated,
                "tier_adjustments": normalized_adjustments,
                "history_entry": history_entry,
            }

        if pct_change is None:
            raise ValueError("pct_change is required when tier_adjustments is not provided.")
        normalized_pct = float(pct_change)
        updated_count = int(
            self.intervention_service.apply_wage_shock(
                self.agent_service,
                pct_change=normalized_pct,
                target_tier=normalized_tier,
            )
        )
        history_entry = self._record_intervention(
            "INCOME_SHOCK_APPLIED",
            f"Income shock {normalized_pct:+.2%} for {normalized_tier}",
            {
                "updated_count": updated_count,
                "pct_change": normalized_pct,
                "target_tier": normalized_tier,
            },
            target_month_override=target_month_override,
        )
        return {
            "updated_count": updated_count,
            "pct_change": normalized_pct,
            "target_tier": normalized_tier,
            "history_entry": history_entry,
        }

    def inject_developer_supply_intervention(
        self,
        *,
        count: int,
        zone: str,
        template: Optional[str] = None,
        price_per_sqm: Optional[float] = None,
        size: Optional[float] = None,
        school_units: Optional[int] = None,
        build_year: Optional[int] = None,
        target_month_override: Optional[int] = None,
    ) -> Dict[str, object]:
        normalized_count = int(count)
        normalized_zone = str(zone or "").strip().upper()
        if normalized_count <= 0:
            raise ValueError("count must be greater than 0.")
        if normalized_zone not in {"A", "B"}:
            raise ValueError("zone must be either A or B.")

        normalized_template = str(template or "").strip().lower() or None
        template_defaults = {
            "a_district_premium": {
                "zone": "A",
                "price_per_sqm": 52000.0,
                "size": 118.0,
                "school_units": max(1, int(round(normalized_count * 0.8))),
            },
            "b_entry_level": {
                "zone": "B",
                "price_per_sqm": 18000.0,
                "size": 88.0,
                "school_units": 0,
            },
            "mixed_balanced": {
                "zone": normalized_zone,
                "price_per_sqm": 32000.0 if normalized_zone == "A" else 21000.0,
                "size": 102.0,
                "school_units": max(0, int(round(normalized_count * 0.3))),
            },
        }
        if normalized_template is not None:
            if normalized_template not in template_defaults:
                raise ValueError("template must be one of: a_district_premium, b_entry_level, mixed_balanced.")
            defaults = template_defaults[normalized_template]
            normalized_zone = str(defaults["zone"])
            if price_per_sqm is None:
                price_per_sqm = float(defaults["price_per_sqm"])
            if size is None:
                size = float(defaults["size"])
            if school_units is None:
                school_units = int(defaults["school_units"])

        cursor = self.conn.cursor()
        cursor.execute("SELECT MAX(property_id) FROM properties_static")
        before_max_id = int((cursor.fetchone() or [0])[0] or 0)
        injected_count = int(
            self.intervention_service.adjust_housing_supply(
                market_service=self.market_service,
                count=normalized_count,
                zone=normalized_zone,
                price_per_sqm=price_per_sqm,
                size=size,
                school_units=school_units,
                build_year=build_year,
                config=self.config,
                current_month=int(self.current_month),
            )
        )
        self.developer_account_service.record_investment(injected_count, int(self.current_month))
        property_ids = list(range(before_max_id + 1, before_max_id + injected_count + 1))
        history_entry = self._record_intervention(
            "DEVELOPER_SUPPLY_INJECTED",
            f"Injected {injected_count} units into zone {normalized_zone}" + (f" via {normalized_template}" if normalized_template else ""),
            {
                "count": injected_count,
                "zone": normalized_zone,
                "template": normalized_template,
                "price_per_sqm": float(price_per_sqm) if price_per_sqm is not None else None,
                "size": float(size) if size is not None else None,
                "school_units": int(school_units) if school_units is not None else None,
                "build_year": int(build_year) if build_year is not None else None,
            },
            target_month_override=target_month_override,
        )
        return {
            "count": injected_count,
            "zone": normalized_zone,
            "template": normalized_template,
            "price_per_sqm": float(price_per_sqm) if price_per_sqm is not None else None,
            "size": float(size) if size is not None else None,
            "school_units": int(school_units) if school_units is not None else None,
            "build_year": int(build_year) if build_year is not None else None,
            "history_entry": history_entry,
            "generated_events": self._build_property_generated_events(property_ids, month=int(self.current_month), phase="system"),
            "listed_events": self._build_property_listed_events(property_ids, month=int(self.current_month), phase="listing"),
        }

    def get_final_summary(self) -> Dict[str, object]:
        """
        Build a compact run-end summary for API/front-end review panels.
        This is a read-only aggregation layer over existing simulation facts.
        """
        if not self.conn:
            return {
                "completed_month": int(self.current_month),
                "top_agents": [],
                "key_properties": [],
                "failure_reasons": [],
                "interventions": list(self.intervention_history[-20:]),
            }

        cursor = self.conn.cursor()

        cursor.execute(
            """
            SELECT
                ast.agent_id,
                COALESCE(ast.name, 'Agent ' || ast.agent_id) AS name,
                COALESCE(ast.agent_type, 'normal') AS agent_type,
                SUM(CASE WHEN dl.event_type = 'ROLE_DECISION'
                           AND dl.decision IN ('BUYER', 'SELLER', 'BUYER_SELLER')
                         THEN 1 ELSE 0 END) AS activations,
                SUM(CASE WHEN tor.status = 'filled' THEN 1 ELSE 0 END) AS deals,
                SUM(CASE WHEN tor.status IN ('cancelled', 'expired', 'breached') THEN 1 ELSE 0 END) AS failures
            FROM agents_static ast
            LEFT JOIN decision_logs dl
              ON dl.agent_id = ast.agent_id
            LEFT JOIN transaction_orders tor
              ON tor.buyer_id = ast.agent_id
            GROUP BY ast.agent_id, ast.name, ast.agent_type
            HAVING activations > 0 OR deals > 0 OR failures > 0
            ORDER BY (deals * 3 + activations - failures) DESC, ast.agent_id ASC
            LIMIT 5
            """
        )
        top_agents = [
            {
                "agent_id": int(row[0]),
                "name": str(row[1] or f"Agent {row[0]}"),
                "agent_type": str(row[2] or "normal"),
                "activations": int(row[3] or 0),
                "deals": int(row[4] or 0),
                "failures": int(row[5] or 0),
            }
            for row in cursor.fetchall() or []
        ]

        cursor.execute(
            """
            SELECT
                ps.property_id,
                COALESCE(ps.zone, '?') AS zone,
                COALESCE(ps.property_type, 'Property') AS property_type,
                SUM(CASE WHEN pm.property_id IS NOT NULL THEN 1 ELSE 0 END) AS listings,
                SUM(CASE WHEN pbm.property_id IS NOT NULL THEN 1 ELSE 0 END) AS attempts,
                SUM(CASE WHEN tor.status = 'filled' THEN 1 ELSE 0 END) AS deals,
                SUM(CASE WHEN tor.status IN ('cancelled', 'expired', 'breached') THEN 1 ELSE 0 END) AS failures
            FROM properties_static ps
            LEFT JOIN properties_market pm
              ON pm.property_id = ps.property_id
            LEFT JOIN property_buyer_matches pbm
              ON pbm.property_id = ps.property_id
            LEFT JOIN transaction_orders tor
              ON tor.property_id = ps.property_id
            GROUP BY ps.property_id, ps.zone, ps.property_type
            HAVING listings > 0 OR attempts > 0 OR deals > 0 OR failures > 0
            ORDER BY (deals * 4 + attempts + listings) DESC, ps.property_id ASC
            LIMIT 5
            """
        )
        key_properties = [
            {
                "property_id": int(row[0]),
                "zone": str(row[1] or "?"),
                "property_type": str(row[2] or "Property"),
                "listings": int(row[3] or 0),
                "attempts": int(row[4] or 0),
                "deals": int(row[5] or 0),
                "failures": int(row[6] or 0),
            }
            for row in cursor.fetchall() or []
        ]

        cursor.execute(
            """
            SELECT
                COALESCE(NULLIF(close_reason, ''), status, 'unknown') AS reason,
                COUNT(*) AS cnt
            FROM transaction_orders
            WHERE status IN ('cancelled', 'expired', 'breached')
            GROUP BY COALESCE(NULLIF(close_reason, ''), status, 'unknown')
            ORDER BY cnt DESC, reason ASC
            LIMIT 5
            """
        )
        failure_reasons = [
            {
                "reason": str(row[0] or "unknown"),
                "count": int(row[1] or 0),
            }
            for row in cursor.fetchall() or []
        ]

        return {
            "completed_month": int(self.current_month),
            "top_agents": top_agents,
            "key_properties": key_properties,
            "failure_reasons": failure_reasons,
            "interventions": list(self.intervention_history[-20:]),
        }

    def get_export_report(self) -> Dict[str, object]:
        completed_month = int(self.current_month or 0)
        month_reviews = [
            {
                "month": month,
                "month_review": self.get_month_review(month),
            }
            for month in range(1, completed_month + 1)
        ]

        return {
            "run": {
                "run_id": self._run_id(),
                "status": self.status,
                "agent_count": int(self.agent_count),
                "total_months": int(self.months),
                "completed_month": completed_month,
                "db_path": str(self.db_path),
                "run_dir": str(self._run_dir),
                "started_at": self.started_at,
                "completed_at": self.completed_at,
                "last_error": self.last_error,
            },
            "artifacts": {
                "parameter_assumption_markdown": self._parameter_assumption_artifact_paths()["markdown_path"].replace("\\", "/"),
                "parameter_assumption_json": self._parameter_assumption_artifact_paths()["json_path"].replace("\\", "/"),
            },
            "runtime_controls": self.get_runtime_controls(),
            "last_month_summary": self.last_month_summary,
            "month_reviews": month_reviews,
            "final_summary": self.final_summary or self.get_final_summary(),
        }

    def get_month_review(self, month: int) -> Dict[str, object]:
        """
        Build a compact month-end review object from extracted month events.
        Keeps month-end payloads structurally aligned with final_summary.
        """
        target_month = int(month)
        events = self.get_month_events(target_month)

        agent_stats: Dict[str, Dict[str, object]] = {}
        property_stats: Dict[str, Dict[str, object]] = {}
        failure_stats: Dict[str, Dict[str, object]] = {}

        for event in events:
            payload = event.get("payload", {}) or {}
            event_type = str(event.get("event_type") or "")

            if event_type == "AGENT_ACTIVATED" and payload.get("agent_id") is not None:
                agent_id = int(payload["agent_id"])
                key = str(agent_id)
                current = agent_stats.get(key, {
                    "agent_id": agent_id,
                    "name": str(payload.get("name") or f"Agent {agent_id}"),
                    "agent_type": "normal",
                    "activations": 0,
                    "deals": 0,
                    "failures": 0,
                })
                current["activations"] = int(current["activations"]) + 1
                agent_stats[key] = current

            if event_type in {"DEAL_SUCCESS", "DEAL_FAIL"} and payload.get("buyer_id") is not None:
                buyer_id = int(payload["buyer_id"])
                key = str(buyer_id)
                current = agent_stats.get(key, {
                    "agent_id": buyer_id,
                    "name": f"Agent {buyer_id}",
                    "agent_type": "normal",
                    "activations": 0,
                    "deals": 0,
                    "failures": 0,
                })
                if event_type == "DEAL_SUCCESS":
                    current["deals"] = int(current["deals"]) + 1
                else:
                    current["failures"] = int(current["failures"]) + 1
                agent_stats[key] = current

            if payload.get("property_id") is not None and event_type in {"PROPERTY_LISTED", "MATCH_ATTEMPT", "DEAL_SUCCESS", "DEAL_FAIL"}:
                property_id = int(payload["property_id"])
                key = str(property_id)
                current = property_stats.get(key, {
                    "property_id": property_id,
                    "zone": str(payload.get("zone") or "?"),
                    "property_type": str(payload.get("property_type") or "Property"),
                    "listings": 0,
                    "attempts": 0,
                    "deals": 0,
                    "failures": 0,
                })
                if event_type == "PROPERTY_LISTED":
                    current["listings"] = int(current["listings"]) + 1
                    current["zone"] = str(payload.get("zone") or current["zone"])
                    current["property_type"] = str(payload.get("property_type") or current["property_type"])
                elif event_type == "MATCH_ATTEMPT":
                    current["attempts"] = int(current["attempts"]) + 1
                elif event_type == "DEAL_SUCCESS":
                    current["deals"] = int(current["deals"]) + 1
                elif event_type == "DEAL_FAIL":
                    current["failures"] = int(current["failures"]) + 1
                property_stats[key] = current

            if event_type == "DEAL_FAIL":
                reason = str(payload.get("reason") or payload.get("status") or "unknown").strip() or "unknown"
                current = failure_stats.get(reason, {"reason": reason, "count": 0})
                current["count"] = int(current["count"]) + 1
                failure_stats[reason] = current

        top_agents = sorted(
            agent_stats.values(),
            key=lambda item: (int(item["deals"]) * 3 + int(item["activations"]) - int(item["failures"]), -int(item["agent_id"])),
            reverse=True,
        )[:5]
        key_properties = sorted(
            property_stats.values(),
            key=lambda item: (int(item["deals"]) * 4 + int(item["attempts"]) + int(item["listings"]), -int(item["property_id"])),
            reverse=True,
        )[:5]
        failure_reasons = sorted(
            failure_stats.values(),
            key=lambda item: (int(item["count"]), item["reason"]),
            reverse=True,
        )[:5]

        return {
            "month": target_month,
            "top_agents": top_agents,
            "key_properties": key_properties,
            "failure_reasons": failure_reasons,
            "interventions": [item for item in self.intervention_history if int(item.get("month", -1)) == target_month],
        }

    def get_month_average_transaction_price(self, month: int) -> float:
        """Calculate the average completed transaction price for a month."""
        if not self.conn:
            return 0.0
        target_month = int(month)
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT AVG(final_price)
            FROM transactions
            WHERE month = ? AND COALESCE(final_price, 0) > 0
            """,
            (target_month,),
        )
        value = cursor.fetchone()
        avg_price = float(value[0] or 0.0) if value else 0.0
        if avg_price <= 0:
            return 0.0
        return avg_price

    def get_month_buyer_count(self, month: int) -> int:
        """
        Count distinct buyers that can be justified from persisted month facts.
        This intentionally uses DB-backed participation rather than transient
        in-memory buyer lists so the UI summary stays auditable.
        """
        if not self.conn:
            return 0

        target_month = int(month)
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(DISTINCT agent_id)
            FROM (
                SELECT agent_id
                FROM active_participants
                WHERE month = ?
                  AND role IN ('BUYER', 'BUYER_SELLER')
                UNION
                SELECT buyer_id AS agent_id
                FROM property_buyer_matches
                WHERE month = ?
                  AND buyer_id IS NOT NULL
                UNION
                SELECT buyer_id AS agent_id
                FROM transactions
                WHERE month = ?
                  AND buyer_id IS NOT NULL
            )
            """,
            (target_month, target_month, target_month),
        )
        row = cursor.fetchone()
        return int((row or [0])[0] or 0)

    def get_bulletin_event(self, month: int) -> Dict[str, object]:
        """
        Build a lightweight month bulletin event for WebSocket consumers.
        """
        target_month = int(month)
        run_id = os.path.basename(str(self._run_dir or "")) or "run"
        bulletin = str(self.last_bulletin or "").strip()
        excerpt = " ".join(bulletin.split())[:280]
        return {
            "event_id": f"{run_id}:m{target_month}:market_bulletin",
            "run_id": run_id,
            "month": target_month,
            "phase": "month_start",
            "event_type": "MARKET_BULLETIN_READY",
            "ts": datetime.datetime.now().isoformat(),
            "payload": {
                "month": target_month,
                "bulletin": bulletin,
                "bulletin_excerpt": excerpt,
            },
            "source": "simulation_runner",
            "schema_version": "v1",
        }

    def get_generation_events(self, property_display_limit: int = 24) -> List[Dict[str, object]]:
        """
        Build initialization-stage events for the frontend generation pool.
        Properties are intentionally sampled for display to avoid flooding the UI.
        """
        if not self.conn:
            return []

        cursor = self.conn.cursor()
        run_id = os.path.basename(str(self._run_dir or "")) or "run"
        ts = datetime.datetime.now().isoformat()
        events: List[Dict[str, object]] = []

        cursor.execute(
            """
            SELECT agent_id, name, occupation, agent_type
            FROM agents_static
            ORDER BY agent_id
            """
        )
        for row in cursor.fetchall() or []:
            agent_id = int(row[0])
            events.append(
                {
                    "event_id": f"{run_id}:m0:agent_generated:{agent_id}",
                    "run_id": run_id,
                    "month": 0,
                    "phase": "month_start",
                    "event_type": "AGENT_GENERATED",
                    "ts": ts,
                    "payload": {
                        "agent_id": agent_id,
                        "name": str(row[1] or ""),
                        "occupation": str(row[2] or ""),
                        "agent_type": str(row[3] or "normal"),
                    },
                    "source": "simulation_runner",
                    "schema_version": "v1",
                }
            )

        cursor.execute("SELECT COUNT(*) FROM properties_static")
        total_properties = int((cursor.fetchone() or [0])[0] or 0)
        cursor.execute(
            """
            SELECT property_id, zone, property_type, is_school_district
            FROM properties_static
            ORDER BY property_id
            LIMIT ?
            """,
            (max(1, int(property_display_limit)),),
        )
        for row in cursor.fetchall() or []:
            property_id = int(row[0])
            zone = str(row[1] or "")
            property_type = str(row[2] or "")
            school_flag = bool(row[3])
            display_name = f"{zone}区{'学区房' if school_flag else property_type or '房产'} #{property_id}"
            events.append(
                {
                    "event_id": f"{run_id}:m0:property_generated:{property_id}",
                    "run_id": run_id,
                    "month": 0,
                    "phase": "month_start",
                    "event_type": "PROPERTY_GENERATED",
                    "ts": ts,
                    "payload": {
                        "property_id": property_id,
                        "zone": zone,
                        "property_type": property_type,
                        "is_school_district": school_flag,
                        "display_name": display_name,
                        "display_only": True,
                        "display_total_properties": total_properties,
                    },
                    "source": "simulation_runner",
                    "schema_version": "v1",
                }
            )

        return events

    def get_stage_replay_events(self, property_display_limit: int = 48) -> List[Dict[str, object]]:
        if not self.conn:
            return []
        events = self.get_generation_events(property_display_limit=property_display_limit)
        for month in range(1, int(self.current_month) + 1):
            events.extend(self.get_month_events(month))
        return events

    def _stage_tone(self, entity_type: str, lane: str, agent_type: str = "normal", stage_status: str = "") -> str:
        stage_status = str(stage_status or lane or "").lower()
        if stage_status in ("settled", "sold") or lane == "success":
            return "#89f2a6"
        if stage_status == "cooldown":
            return "#f4d47d"
        if stage_status == "failed" or lane == "failure":
            return "#ff8d8d"
        if stage_status == "observer":
            return "#9cb6cc"
        if stage_status in ("active_participant", "active") or lane == "activation":
            return "#9cf0b5"
        if stage_status in ("listed", "inventory") and entity_type == "property":
            return "#8dc2ff"
        if stage_status == "negotiating" or lane == "negotiation":
            return "#7cc9ff"
        if lane == "listing":
            return "#8dc2ff"
        if entity_type == "property":
            return "#8dc2ff"
        return "#f4d47d" if str(agent_type or "normal") == "smart" else "#85d6c0"

    def _empty_stage_snapshot(self) -> Dict[str, object]:
        return {
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
        }

    def get_stage_snapshot(self) -> Dict[str, object]:
        if not self.conn:
            return self._empty_stage_snapshot()

        cursor = self.conn.cursor()
        current_month = int(self.current_month)
        snapshot = self._empty_stage_snapshot()
        nodes: List[Dict[str, object]] = []

        observed_agent_ids: set[int] = set()
        active_agent_ids: set[int] = set()
        negotiating_agent_ids: set[int] = set()
        success_agent_ids: set[int] = set()
        failure_agent_ids: set[int] = set()
        agent_stage_status: Dict[int, str] = {}

        listed_property_ids: set[int] = set()
        negotiating_property_ids: set[int] = set()
        success_property_ids: set[int] = set()
        failure_property_ids: set[int] = set()
        property_stage_status: Dict[int, str] = {}

        cursor.execute(
            """
            SELECT ap.agent_id,
                   COALESCE(ap.role, 'OBSERVER') AS role,
                   COALESCE(ap.buy_completed, 0) AS buy_completed,
                   COALESCE(ap.sell_completed, 0) AS sell_completed,
                   COALESCE(ap.consecutive_failures, 0) AS consecutive_failures,
                   COALESCE(ap.cooldown_months, 0) AS cooldown_months,
                   COALESCE(ap.agent_type, 'normal') AS agent_type
            FROM active_participants ap
            JOIN (
                SELECT agent_id, MAX(COALESCE(month, 0)) AS max_month
                FROM active_participants
                GROUP BY agent_id
            ) latest
              ON latest.agent_id = ap.agent_id
             AND COALESCE(ap.month, 0) = latest.max_month
            """,
        )
        for row in cursor.fetchall() or []:
            if row[0] is None:
                continue
            agent_id = int(row[0])
            role = str(row[1] or "OBSERVER").upper()
            buy_completed = int(row[2] or 0)
            sell_completed = int(row[3] or 0)
            consecutive_failures = int(row[4] or 0)
            cooldown_months = int(row[5] or 0)
            observed_agent_ids.add(agent_id)
            if role in ("BUYER", "SELLER", "BUYER_SELLER"):
                active_agent_ids.add(agent_id)
                agent_stage_status[agent_id] = "active_participant"
            else:
                agent_stage_status[agent_id] = "observer"
            if buy_completed or sell_completed:
                success_agent_ids.add(agent_id)
                agent_stage_status[agent_id] = "settled"
            elif consecutive_failures > 0 and cooldown_months > 0:
                failure_agent_ids.add(agent_id)
                agent_stage_status[agent_id] = "cooldown"

        cursor.execute(
            """
            SELECT DISTINCT buyer_id, property_id
            FROM property_buyer_matches
            WHERE month <= ?
              AND proceeded_to_negotiation = 1
            """,
            (current_month,),
        )
        for row in cursor.fetchall() or []:
            if row[0] is not None:
                buyer_id = int(row[0])
                negotiating_agent_ids.add(buyer_id)
                agent_stage_status[buyer_id] = "negotiating"
            if row[1] is not None:
                property_id = int(row[1])
                negotiating_property_ids.add(property_id)
                property_stage_status[property_id] = "negotiating"

        cursor.execute(
            """
            SELECT DISTINCT property_id, status
            FROM properties_market
            WHERE listing_month <= ?
               OR status = 'for_sale'
            """,
            (current_month,),
        )
        for row in cursor.fetchall() or []:
            if row[0] is None:
                continue
            property_id = int(row[0])
            listed_property_ids.add(property_id)
            property_stage_status[property_id] = "listed" if str(row[1] or "") == "for_sale" else property_stage_status.get(property_id, "listed")

        cursor.execute(
            """
            SELECT buyer_id, property_id, status
            FROM transaction_orders
            WHERE created_month <= ? OR close_month <= ?
            ORDER BY COALESCE(close_month, created_month, 0), order_id
            """,
            (current_month, current_month),
        )
        for row in cursor.fetchall() or []:
            buyer_id = int(row[0] or 0)
            property_id = int(row[1] or 0)
            status = str(row[2] or "")
            if status == "filled":
                success_agent_ids.add(buyer_id)
                success_property_ids.add(property_id)
                agent_stage_status[buyer_id] = "settled"
                property_stage_status[property_id] = "sold"
                failure_agent_ids.discard(buyer_id)
                failure_property_ids.discard(property_id)
                negotiating_agent_ids.discard(buyer_id)
                negotiating_property_ids.discard(property_id)
            elif status == "pending_settlement":
                negotiating_agent_ids.add(buyer_id)
                negotiating_property_ids.add(property_id)
                if buyer_id not in success_agent_ids:
                    agent_stage_status[buyer_id] = "negotiating"
                if property_id not in success_property_ids:
                    property_stage_status[property_id] = "negotiating"
                failure_agent_ids.discard(buyer_id)
                failure_property_ids.discard(property_id)
            elif status in ("cancelled", "expired", "breached"):
                if buyer_id not in success_agent_ids:
                    failure_agent_ids.add(buyer_id)
                    agent_stage_status[buyer_id] = "failed"
                if property_id not in success_property_ids:
                    failure_property_ids.add(property_id)
                    property_stage_status[property_id] = "failed"

        cursor.execute(
            """
            SELECT agent_id, name, occupation, agent_type
            FROM agents_static
            ORDER BY agent_id
            """
        )
        for row in cursor.fetchall() or []:
            agent_id = int(row[0])
            agent_type = str(row[3] or "normal")
            lane = "generated"
            stage_status = agent_stage_status.get(agent_id, "observer")
            if agent_id in observed_agent_ids or agent_id in active_agent_ids:
                lane = "activation"
            if agent_id in negotiating_agent_ids:
                lane = "negotiation"
            if agent_id in failure_agent_ids:
                lane = "failure"
            if agent_id in success_agent_ids:
                lane = "success"
            if lane == "generated":
                stage_status = "observer"
            nodes.append(
                {
                    "entity_key": f"agent:{agent_id}",
                    "entity_type": "agent",
                    "label": str(row[1] or f"Agent {agent_id}"),
                    "subtitle": str(row[2] or ""),
                    "lane": lane,
                    "tone": self._stage_tone("agent", lane, agent_type, stage_status),
                    "agent_type": agent_type,
                    "stage_status": stage_status,
                }
            )
            if lane == "generated":
                snapshot["counts"]["generatedAgents"] += 1
            elif lane == "activation":
                snapshot["counts"]["activations"] += 1
            elif lane == "negotiation":
                snapshot["counts"]["negotiations"] += 1
            elif lane == "success":
                snapshot["counts"]["successes"] += 1
            elif lane == "failure":
                snapshot["counts"]["failures"] += 1

        cursor.execute(
            """
            SELECT property_id, zone, property_type, is_school_district
            FROM properties_static
            ORDER BY property_id
            """
        )
        for row in cursor.fetchall() or []:
            property_id = int(row[0])
            zone = str(row[1] or "")
            property_type = str(row[2] or "")
            school_flag = bool(row[3])
            lane = "generated"
            stage_status = property_stage_status.get(property_id, "inventory")
            if property_id in listed_property_ids:
                lane = "listing"
            if property_id in negotiating_property_ids:
                lane = "negotiation"
            if property_id in failure_property_ids:
                lane = "failure"
            if property_id in success_property_ids:
                lane = "success"
            if lane == "listing":
                stage_status = property_stage_status.get(property_id, "listed")
            elif lane == "success":
                stage_status = "sold"
            elif lane == "failure":
                stage_status = property_stage_status.get(property_id, "failed")
            elif lane == "negotiation":
                stage_status = "negotiating"
            display_name = f"{zone}区{'学区房' if school_flag else property_type or '房产'} #{property_id}"
            nodes.append(
                {
                    "entity_key": f"property:{property_id}",
                    "entity_type": "property",
                    "label": f"房#{property_id}",
                    "subtitle": property_type,
                    "property_id": property_id,
                    "display_name": display_name,
                    "lane": lane,
                    "tone": self._stage_tone("property", lane, stage_status=stage_status),
                    "stage_status": stage_status,
                }
            )
            if lane == "generated":
                snapshot["counts"]["generatedProperties"] += 1
            elif lane == "listing":
                snapshot["counts"]["listings"] += 1
            elif lane == "negotiation":
                snapshot["counts"]["negotiations"] += 1
            elif lane == "success":
                snapshot["counts"]["successes"] += 1
            elif lane == "failure":
                snapshot["counts"]["failures"] += 1

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM property_buyer_matches
            WHERE month <= ?
            """,
            (current_month,),
        )
        snapshot["counts"]["matches"] = int((cursor.fetchone() or [0])[0] or 0)
        snapshot["nodes"] = nodes

        focus_lane = "generated"
        if snapshot["counts"]["negotiations"] > 0:
            focus_lane = "negotiation"
        elif snapshot["counts"]["listings"] > 0:
            focus_lane = "listing"
        elif snapshot["counts"]["activations"] > 0:
            focus_lane = "activation"
        elif snapshot["counts"]["successes"] > 0:
            focus_lane = "success"
        snapshot["focus_lane"] = focus_lane
        return snapshot

    def get_month_events(self, month: int) -> List[Dict[str, object]]:
        """
        Build lightweight replay events for WebSocket consumers from DB facts.
        This is a month-end snapshot/replay layer, not an intrusive in-loop emitter.
        """
        if not self.conn:
            return []

        cursor = self.conn.cursor()
        events: List[Dict[str, object]] = []
        run_id = os.path.basename(str(self._run_dir or "")) or "run"
        target_month = int(month)
        ts = datetime.datetime.now().isoformat()

        # 1. Agent activations for the month.
        cursor.execute(
            """
            SELECT dl.log_id, dl.agent_id, dl.decision, dl.reason, ast.name, ast.occupation
            FROM decision_logs dl
            LEFT JOIN agents_static ast ON ast.agent_id = dl.agent_id
            WHERE dl.month = ?
              AND dl.event_type = 'ROLE_DECISION'
              AND dl.decision IN ('BUYER', 'SELLER', 'BUYER_SELLER')
            ORDER BY dl.log_id
            """,
            (target_month,),
        )
        for row in cursor.fetchall() or []:
            events.append(
                {
                    "event_id": f"{run_id}:m{target_month}:agent:{int(row[0])}",
                    "run_id": run_id,
                    "month": target_month,
                    "phase": "activation",
                    "event_type": "AGENT_ACTIVATED",
                    "ts": ts,
                    "payload": {
                        "agent_id": int(row[1]),
                        "role": str(row[2]),
                        "reason": str(row[3] or ""),
                        "name": str(row[4] or ""),
                        "occupation": str(row[5] or ""),
                    },
                    "source": "simulation_runner",
                    "schema_version": "v1",
                }
            )

        # 2. Successful deals reached this month.
        cursor.execute(
            """
            SELECT pm.property_id, pm.owner_id, pm.listed_price, pm.status, ps.zone, ps.property_type, ps.is_school_district
            FROM properties_market pm
            JOIN properties_static ps ON ps.property_id = pm.property_id
            WHERE pm.listing_month = ?
            ORDER BY pm.property_id
            """,
            (target_month,),
        )
        for row in cursor.fetchall() or []:
            property_id = int(row[0])
            events.append(
                {
                    "event_id": f"{run_id}:m{target_month}:property_listed:{property_id}",
                    "run_id": run_id,
                    "month": target_month,
                    "phase": "listing",
                    "event_type": "PROPERTY_LISTED",
                    "ts": ts,
                    "payload": {
                        "property_id": property_id,
                        "owner_id": int(row[1] or -1),
                        "listed_price": float(row[2] or 0.0),
                        "status": str(row[3] or ""),
                        "zone": str(row[4] or ""),
                        "property_type": str(row[5] or ""),
                        "is_school_district": bool(row[6]),
                    },
                    "source": "simulation_runner",
                    "schema_version": "v1",
                }
            )

        cursor.execute(
            """
            SELECT match_id, buyer_id, property_id, listing_price, buyer_bid, proceeded_to_negotiation, final_outcome
            FROM property_buyer_matches
            WHERE month = ?
            ORDER BY match_id
            """,
            (target_month,),
        )
        for row in cursor.fetchall() or []:
            match_id = int(row[0])
            events.append(
                {
                    "event_id": f"{run_id}:m{target_month}:match_attempt:{match_id}",
                    "run_id": run_id,
                    "month": target_month,
                    "phase": "matching",
                    "event_type": "MATCH_ATTEMPT",
                    "ts": ts,
                    "payload": {
                        "match_id": match_id,
                        "buyer_id": int(row[1] or 0),
                        "property_id": int(row[2] or 0),
                        "listing_price": float(row[3] or 0.0),
                        "buyer_bid": float(row[4] or 0.0),
                        "proceeded_to_negotiation": bool(row[5]),
                        "final_outcome": str(row[6] or ""),
                    },
                    "source": "simulation_runner",
                    "schema_version": "v1",
                }
            )

        cursor.execute(
            """
            SELECT
                n.negotiation_id,
                n.buyer_id,
                n.seller_id,
                n.property_id,
                n.round_count,
                n.final_price,
                n.success,
                n.reason,
                n.log,
                pbm.listing_price
            FROM negotiations n
            JOIN property_buyer_matches pbm
              ON pbm.buyer_id = n.buyer_id
             AND pbm.property_id = n.property_id
             AND pbm.month = ?
             AND pbm.proceeded_to_negotiation = 1
            ORDER BY n.negotiation_id
            """,
            (target_month,),
        )
        for row in cursor.fetchall() or []:
            negotiation_id = int(row[0])
            buyer_id = int(row[1] or 0)
            seller_id = int(row[2] or -1)
            property_id = int(row[3] or 0)
            round_count = int(row[4] or 0)
            final_price = float(row[5] or 0.0)
            success = bool(row[6])
            reason = str(row[7] or "")
            log_text = str(row[8] or "")
            listing_price = float(row[9] or 0.0)
            excerpt = " ".join(log_text.replace("\n", " ").split())[:220] if log_text.strip() else ""
            quote_stream_enabled = bool(self.config.get("negotiation.quote_stream_enabled", False))
            quote_mode = str(self.config.get("negotiation.quote_mode", "limited_quotes") or "limited_quotes")
            quote_limit = int(self.config.get("negotiation.quote_turn_limit", 4) or 4)
            quote_char_limit = int(self.config.get("negotiation.quote_char_limit", 84) or 84)

            events.append(
                {
                    "event_id": f"{run_id}:m{target_month}:negotiation_started:{negotiation_id}",
                    "run_id": run_id,
                    "month": target_month,
                    "phase": "negotiation",
                    "event_type": "NEGOTIATION_STARTED",
                    "ts": ts,
                    "payload": {
                        "negotiation_id": negotiation_id,
                        "buyer_id": buyer_id,
                        "seller_id": seller_id,
                        "property_id": property_id,
                        "round_count": round_count,
                    },
                    "source": "simulation_runner",
                    "schema_version": "v1",
                }
            )
            events.append(
                {
                    "event_id": f"{run_id}:m{target_month}:negotiation_progress:{negotiation_id}",
                    "run_id": run_id,
                    "month": target_month,
                    "phase": "negotiation",
                    "event_type": "NEGOTIATION_PROGRESS",
                    "ts": ts,
                    "payload": {
                        "negotiation_id": negotiation_id,
                        "buyer_id": buyer_id,
                        "seller_id": seller_id,
                        "property_id": property_id,
                        "round_count": round_count,
                        "summary": excerpt or reason or f"Negotiation progressed through {max(round_count, 1)} rounds.",
                    },
                    "source": "simulation_runner",
                    "schema_version": "v1",
                }
            )
            if quote_stream_enabled and quote_mode != "off" and quote_mode != "summary" and log_text.strip() and self._should_emit_negotiation_quotes(
                round_count=round_count,
                success=success,
                final_price=final_price,
                listing_price=listing_price,
            ):
                try:
                    parsed_log = json.loads(log_text)
                except (json.JSONDecodeError, TypeError, ValueError):
                    parsed_log = []
                if isinstance(parsed_log, list):
                    emitted_turns = 0
                    for index, turn in enumerate(parsed_log[:quote_limit], start=1):
                        speaker = str((turn or {}).get("speaker") or "agent").strip().lower()[:16]
                        message = " ".join(str((turn or {}).get("message") or "").split())
                        if not message:
                            continue
                        clipped = message[:quote_char_limit]
                        if len(message) > quote_char_limit:
                            clipped = f"{clipped}..."
                        event_type = "NEGOTIATION_TURN" if quote_mode == "full_quotes" else "NEGOTIATION_QUOTE"
                        phase = "negotiation_turn" if quote_mode == "full_quotes" else "negotiation_quote"
                        payload = {
                            "negotiation_id": negotiation_id,
                            "buyer_id": buyer_id,
                            "seller_id": seller_id,
                            "property_id": property_id,
                            "round_count": round_count,
                            "turn_index": index,
                            "speaker": speaker,
                            "quote": clipped,
                        }
                        if quote_mode == "full_quotes":
                            payload["turn_text"] = clipped
                        events.append(
                            {
                                "event_id": f"{run_id}:m{target_month}:negotiation_turn:{negotiation_id}:{index}",
                                "run_id": run_id,
                                "month": target_month,
                                "phase": phase,
                                "event_type": event_type,
                                "ts": ts,
                                "payload": payload,
                                "source": "simulation_runner",
                                "schema_version": "v1",
                            }
                        )
                        emitted_turns += 1
                    if quote_mode == "full_quotes" and emitted_turns > 0:
                        events.append(
                            {
                                "event_id": f"{run_id}:m{target_month}:negotiation_turn_batch_end:{negotiation_id}",
                                "run_id": run_id,
                                "month": target_month,
                                "phase": "negotiation_turn",
                                "event_type": "NEGOTIATION_TURN_BATCH_END",
                                "ts": ts,
                                "payload": {
                                    "negotiation_id": negotiation_id,
                                    "buyer_id": buyer_id,
                                    "seller_id": seller_id,
                                    "property_id": property_id,
                                    "emitted_turns": emitted_turns,
                                    "quote_mode": quote_mode,
                                },
                                "source": "simulation_runner",
                                "schema_version": "v1",
                            }
                        )
            events.append(
                {
                    "event_id": f"{run_id}:m{target_month}:negotiation_closed:{negotiation_id}",
                    "run_id": run_id,
                    "month": target_month,
                    "phase": "negotiation",
                    "event_type": "NEGOTIATION_CLOSED",
                    "ts": ts,
                    "payload": {
                        "negotiation_id": negotiation_id,
                        "buyer_id": buyer_id,
                        "seller_id": seller_id,
                        "property_id": property_id,
                        "round_count": round_count,
                        "final_price": final_price,
                        "success": success,
                        "reason": reason or ("Deal pending settlement" if success else "Negotiation closed"),
                    },
                    "source": "simulation_runner",
                    "schema_version": "v1",
                }
            )

        # 3. Completed settlements closed this month.
        cursor.execute(
            """
            SELECT order_id, buyer_id, seller_id, property_id, agreed_price, status, close_month
            FROM transaction_orders
            WHERE close_month = ?
              AND status = 'filled'
            ORDER BY order_id
            """,
            (target_month,),
        )
        for row in cursor.fetchall() or []:
            order_id = int(row[0])
            events.append(
                {
                    "event_id": f"{run_id}:m{target_month}:deal_success:{order_id}",
                    "run_id": run_id,
                    "month": target_month,
                    "phase": "settlement",
                    "event_type": "DEAL_SUCCESS",
                    "ts": ts,
                    "payload": {
                        "order_id": order_id,
                        "buyer_id": int(row[1] or 0),
                        "seller_id": int(row[2] or -1),
                        "property_id": int(row[3] or 0),
                        "agreed_price": float(row[4] or 0.0),
                        "deal_stage": "settlement_completed",
                        "status": "filled",
                    },
                    "source": "simulation_runner",
                    "schema_version": "v1",
                }
            )

        # 3.5 Orders that reached agreement this month but have not settled yet.
        cursor.execute(
            """
            SELECT order_id, buyer_id, seller_id, property_id, agreed_price, status, settlement_due_month
            FROM transaction_orders
            WHERE created_month = ?
              AND status = 'pending_settlement'
            ORDER BY order_id
            """,
            (target_month,),
        )
        for row in cursor.fetchall() or []:
            order_id = int(row[0])
            events.append(
                {
                    "event_id": f"{run_id}:m{target_month}:settlement_pending:{order_id}",
                    "run_id": run_id,
                    "month": target_month,
                    "phase": "settlement",
                    "event_type": "SETTLEMENT_PENDING",
                    "ts": ts,
                    "payload": {
                        "order_id": order_id,
                        "buyer_id": int(row[1] or 0),
                        "seller_id": int(row[2] or -1),
                        "property_id": int(row[3] or 0),
                        "agreed_price": float(row[4] or 0.0),
                        "deal_stage": "pending_settlement",
                        "status": str(row[5] or "pending_settlement"),
                        "settlement_due_month": int(row[6] or 0),
                    },
                    "source": "simulation_runner",
                    "schema_version": "v1",
                }
            )

        # 4. Failed deals/orders closed this month.
        cursor.execute(
            """
            SELECT order_id, buyer_id, seller_id, property_id, status, close_reason
            FROM transaction_orders
            WHERE close_month = ?
              AND status IN ('cancelled', 'expired', 'breached')
            ORDER BY order_id
            """,
            (target_month,),
        )
        for row in cursor.fetchall() or []:
            order_id = int(row[0])
            events.append(
                {
                    "event_id": f"{run_id}:m{target_month}:deal_fail:{order_id}",
                    "run_id": run_id,
                    "month": target_month,
                    "phase": "order_lifecycle",
                    "event_type": "DEAL_FAIL",
                    "ts": ts,
                    "payload": {
                        "order_id": order_id,
                        "buyer_id": int(row[1] or 0),
                        "seller_id": int(row[2] or -1),
                        "property_id": int(row[3] or 0),
                        "status": str(row[4] or ""),
                        "reason": str(row[5] or ""),
                    },
                    "source": "simulation_runner",
                    "schema_version": "v1",
                }
            )

        return events

    def _run_month(self, month: int, allow_intervention_panel: bool = False) -> Dict[str, object]:
        logger.info(f"--- Month {month} ---")
        self._emit_progress("month_start", f"开始推进第 {month} 月", month, {"phase": "month_start"})

        # Initialize Loggers
        log_dir = os.path.dirname(self.db_path)
        if not log_dir:
            log_dir = "results"
        exchange_display = ExchangeDisplay(use_rich=True)
        wf_logger = WorkflowLogger(self.config)

        # 0. Preplanned interventions (config-driven, no manual input required)
        self._emit_progress("intervention", "正在检查预设干预", month, {"phase": "intervention"})
        self._apply_preplanned_interventions(month)

        # 1. Macro Environment
        self._emit_progress("macro", "正在更新宏观环境", month, {"phase": "macro"})
        macro_key = str(self.config.get("macro_environment.override_mode", None) or "").strip().lower()
        if macro_key not in MACRO_ENVIRONMENT:
            macro_key = get_current_macro_sentiment(month)
        macro_desc = f"{macro_key.upper()}: {MACRO_ENVIRONMENT[macro_key]['description']}"
        exchange_display.show_exchange_header(month, macro_desc)

        # 2. Market Bulletin (Service)
        self._emit_progress("bulletin", "正在生成市场公报", month, {"phase": "bulletin"})
        bulletin = asyncio.run(
            self.market_service.generate_market_bulletin(
                month,
                self.pending_interventions,
                use_llm_analysis=False,
            )
        )
        logger.info(bulletin)
        self.last_bulletin = bulletin

        # News is one-off; keep effect in state but clear the announcement queue.
        self.pending_interventions = []

        market_trend = self.market_service.get_market_trend(month)

        # 3. Agent Updates (Financials)
        self._emit_progress("finance", "正在更新 Agent 财务状态", month, {"phase": "finance"})
        self.agent_service.update_financials()
        pulse_metrics = self.mortgage_risk_service.process_monthly_cycle(
            month,
            self.agent_service.agent_map,
            getattr(self.market_service.market, "properties", None),
        )
        if pulse_metrics and any(v > 0 for v in pulse_metrics.values()):
            logger.info(f"Market Pulse M{month}: {pulse_metrics}")

        # 3.5 Rental Market (Phase 7.2)
        self._emit_progress("rental", "正在处理租赁市场", month, {"phase": "rental"})
        self.rental_service.process_rental_market(month)

        # 4. Agent Lifecycle: Manage Active Participants (Timeouts/Exits)
        self._emit_progress("active_participants", "正在更新存量参与者状态", month, {"phase": "active_participants"})
        batch_decision_logs = []
        active_buyers = self.agent_service.update_active_participants(month, self.market_service.market, batch_decision_logs)

        # 5. Tier 3: LLM Price Adjustments (Service)
        self._emit_progress("listing_adjustment", "正在调整挂牌价格", month, {"phase": "listing_adjustment"})
        asyncio.run(self.transaction_service.process_listing_price_adjustments(month, market_trend))

        # 6. Life Events (Stochastic)
        self._emit_progress("life_events", "正在处理生活事件", month, {"phase": "life_events"})
        self.agent_service.process_life_events(month, batch_decision_logs)

        # 6.5 Market Memory (Phase 7.2)
        recent_bulletins = self.market_service.get_recent_bulletins(month, n=3)

        # 7. Agent Activation (New Participants)
        self._emit_progress("activation", "正在激活潜在买家和卖家", month, {"phase": "activation"})
        new_buyers, decisions, batch_bulletin_exposure = asyncio.run(
            self.agent_service.activate_new_agents(
                month, self.market_service.market, macro_desc,
                batch_decision_logs, market_trend, bulletin,
                recent_bulletins=recent_bulletins
            )
        )

        all_buyers = active_buyers + new_buyers

        # Flush decision logs from activation/lifecycle
        if batch_decision_logs:
            self.conn.executemany(
                """INSERT INTO decision_logs
                        (agent_id, month, event_type, decision, reason, thought_process, context_metrics, llm_called)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                batch_decision_logs,
            )
            self.conn.commit()

        if batch_bulletin_exposure:
            self.conn.executemany(
                """
                INSERT INTO bulletin_exposure_log
                    (agent_id, decision_month, event_type, role_decision, info_delay_months,
                     visible_bulletins, seen_bulletin_month, applied_lag_months, market_trend_seen,
                     bulletin_channel, llm_called)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                batch_bulletin_exposure,
            )
            self.conn.commit()

        wf_logger.show_activation_summary(decisions)

        # 8. Transaction Processing (Service)
        self._emit_progress("matching", "正在处理挂牌、撮合与谈判", month, {"phase": "matching"})
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                    SELECT property_id, owner_id, owner_id as seller_id, listed_price, min_price, status,
                           listing_month as created_month, listing_month, last_price_update_reason,
                           sell_deadline_month, sell_deadline_total_months, sell_urgency_score, forced_sale_mode
                    FROM properties_market
                    WHERE status='for_sale'
                """
            )
        except Exception:
            cursor.execute(
                """
                    SELECT property_id, owner_id, owner_id as seller_id, listed_price, min_price, status,
                           listing_month as created_month, listing_month, last_price_update_reason
                    FROM properties_market
                    WHERE status='for_sale'
                """
            )
        cols = [description[0] for description in cursor.description]
        active_listings = [dict(zip(cols, row)) for row in cursor.fetchall()]

        props_map = {p['property_id']: p for p in self.market_service.market.properties}

        listings_by_zone = {}
        for listing in active_listings:
            pid = listing.get('property_id')
            lm = listing.get('listing_month')
            try:
                listing['listing_age_months'] = max(0, int(month) - int(lm)) if lm is not None else 0
            except Exception:
                listing['listing_age_months'] = 0
            if pid in props_map:
                z = props_map[pid].get('zone', 'A')
                listing['zone'] = z
                if z not in listings_by_zone:
                    listings_by_zone[z] = []
                listings_by_zone[z].append(listing)

        exchange_display.show_listings(active_listings, props_map)
        exchange_display.show_buyers(all_buyers)

        self._emit_progress("settlement", "正在结算成交结果", month, {"phase": "settlement"})
        tx_count, fail_count = asyncio.run(
            self.transaction_service.process_monthly_transactions(
                month, all_buyers, listings_by_zone, active_listings,
                props_map, self.agent_service.agent_map,
                self.market_service.market,
                wf_logger, exchange_display
            )
        )

        # Refresh the bulletin after settlement so persisted market_bulletin
        # matches the actual month-end transaction facts shown in the UI.
        month_end_bulletin_llm_enabled = bool(
            self.config.get("system.market_bulletin.post_settlement_llm_analysis_enabled", True)
        )
        self.last_bulletin = asyncio.run(
            self.market_service.generate_market_bulletin(
                month,
                self.pending_interventions,
                observed_month=month,
                use_llm_analysis=month_end_bulletin_llm_enabled,
            )
        )
        bulletin = self.last_bulletin

        logger.info(f"Month {month} Complete. Transactions: {tx_count}, Failed Negs: {fail_count}")

        if allow_intervention_panel and self._should_show_intervention_panel():
            self._intervention_panel(month)
        else:
            logger.info("Intervention panel skipped (non-interactive or disabled by config).")

        self._emit_progress("summary", "正在汇总月度结果", month, {"phase": "summary"})
        bulletin_excerpt = " ".join(str(bulletin).split())[:240]
        avg_transaction_price = self.get_month_average_transaction_price(month)
        summary = {
            "month": int(month),
            "transactions": int(tx_count),
            "failed_negotiations": int(fail_count),
            "active_listing_count": int(len(active_listings)),
            "buyer_count": int(self.get_month_buyer_count(month)),
            "avg_transaction_price": float(round(avg_transaction_price, 2)),
            "bulletin_excerpt": bulletin_excerpt,
            "event_count": int(len(self.get_month_events(month))),
            "controls_snapshot": self.get_runtime_controls(),
            "month_review": self.get_month_review(month),
        }
        self.last_month_summary = summary
        self.write_parameter_assumption_report()
        self._emit_progress("month_end", f"第 {month} 月结果已生成", month, {"phase": "month_end", "summary": summary})
        return summary

    def run_one_month(self) -> Dict[str, object]:
        if not self._initialized:
            self.initialize()

        if self.current_month >= self.months:
            self.status = "completed"
            if self.completed_at is None:
                self.completed_at = datetime.datetime.now().isoformat()
            raise RuntimeError("Simulation already completed.")

        next_month = self.current_month + 1
        self.status = "running"
        self.last_error = None

        try:
            summary = self._run_month(next_month, allow_intervention_panel=False)
            self.current_month = next_month

            if self.current_month >= self.months:
                self.status = "completed"
                if self.completed_at is None:
                    self.completed_at = datetime.datetime.now().isoformat()
                self.final_summary = self.get_final_summary()
                self.write_parameter_assumption_report()
            else:
                self.status = "paused"

            return summary
        except Exception as e:
            self.status = "failed"
            self.last_error = str(e)
            logger.error(f"Simulation Error at month {next_month}: {e}")
            raise

    def _intervention_panel(self, month: int):
        """V3 月末CLI干预面板：玩家投放房产、调控人口、查看报告"""
        print("\n" + "="*60)
        print(f"  🎮 第 {month} 月结束 - 大盘上帝干预面板")
        print("="*60)
        
        while True:
            print("\n请选择操作：")
            print("  1. 📦 强行投放新楼盘 (开发商)")
            print("  2. 👥 注入外来人口 (增加刚需买家)")
            print("  3. 🚪 人口流失 (移除部分底层/中产)")
            print("  4. 📊 查看开发商账户报告")
            print("  5. ⏩ 继续下一月")
            print("-" * 60)
            
            try:
                choice = input("输入选项 (1-5): ").strip()
                
                if choice == "1":
                    self._invest_properties(month)
                elif choice == "2":
                    self._adjust_population(is_add=True)
                elif choice == "3":
                    self._adjust_population(is_add=False)
                elif choice == "4":
                    self.developer_account_service.show_report(month)
                elif choice == "5":
                    print("\n✅ 继续模拟...\n")
                    break
                else:
                    print("❌ 无效选项，请输入 1-5")
            except KeyboardInterrupt:
                print("\n\n⚠️ 用户中断，退出干预面板")
                break
            except EOFError:
                logger.warning("干预面板输入流结束（EOF），自动退出面板。")
                print("\n⚠️ 输入流结束，自动退出干预面板。")
                break
            except Exception as e:
                logger.error(f"干预面板错误: {e}")
                print(f"❌ 操作失败: {e}")

    def _should_show_intervention_panel(self) -> bool:
        """
        Decide whether month-end intervention panel should be shown.
        Priority:
        1) Config switch simulation.enable_intervention_panel
        2) Non-interactive stdin auto-skip (for gates/night runs)
        """
        enabled = bool(self.config.get("simulation.enable_intervention_panel", True))
        if not enabled:
            return False
        try:
            return bool(sys.stdin and sys.stdin.isatty())
        except Exception:
            return False

    def _adjust_population(self, is_add=True):
        """交互式的增减人口逻辑"""
        action = "注入" if is_add else "移除"
        print("\n" + "-"*60)
        print(f"  👥 {action}人口")
        print("-"*60)
        try:
            tier_map = {"1": "low", "2": "lower_middle", "3": "middle", "4": "high", "5": "ultra_high", "6": "all"}
            print("目标阶层:")
            print(" 1. 底层 (low)\n 2. 工薪 (lower_middle)\n 3. 中产 (middle)\n 4. 高收入 (high)\n 5. 顶豪 (ultra_high)\n 6. 随机混合 (all)")
            t_choice = input("请选择阶层 (1-6) [默认3]: ").strip() or "3"
            tier = tier_map.get(t_choice, "middle")

            count_str = input(f"请输入要{action}的人数 [默认10]: ").strip() or "10"
            count = int(count_str)

            if is_add:
                result = self.intervention_service.add_population(self.agent_service, count=count, tier=tier)
            else:
                result = self.intervention_service.remove_population(self.agent_service, count=count, tier=tier)
            
            print(f"\n✅ 成功{action} {result} 名 {tier} 阶层的 Agent！")
        except Exception as e:
            print(f"❌ 操作失败: {e}")

    def _invest_properties(self, month: int):
        """V3 投放房产交互逻辑"""
        print("\n" + "-"*60)
        print("  📦 投放房产")
        print("-"*60)
        
        try:
            zone = input("请输入区域 (A/B): ").strip().upper()
            if zone not in ["A", "B"]:
                print("❌ 无效区域，必须是 A 或 B")
                return
            
            count_str = input("请输入投放数量: ").strip()
            count = int(count_str)
            if count <= 0 or count > 50:
                print("❌ 数量必须在 1-50 之间")
                return
            
            price_input = input(f"请输入单价（元/㎡，回车使用默认）: ").strip()
            price_per_sqm = float(price_input) if price_input else None
            
            size_input = input(f"请输入面积（㎡，回车使用随机80-140）: ").strip()
            size = float(size_input) if size_input else None

            school_units_input = input(f"请输入学区房数量（0-{count}，回车按区域默认比例）: ").strip()
            school_units = int(school_units_input) if school_units_input else None

            base_year = int(self.config.get("simulation.base_year", 2026))
            build_year_input = input(f"请输入建成年份（回车默认{base_year}）: ").strip()
            build_year = int(build_year_input) if build_year_input else None
            
            print(f"\n确认投放：{count}套 {zone}区房产")
            if price_per_sqm:
                print(f"  单价: ¥{price_per_sqm:,.0f}/㎡")
            else:
                print(f"  单价: 默认（从config读取）")
            if size:
                print(f"  面积: {size}㎡")
            else:
                print(f"  面积: 随机 80-140㎡")
            if school_units is None:
                print("  学区房数量: 按区域默认比例")
            else:
                print(f"  学区房数量: {school_units}套")
            print(f"  建成年份: {build_year if build_year else base_year}")
            
            confirm = input("\n确认投放？(y/n): ").strip().lower()
            if confirm != 'y':
                print("❌ 已取消投放")
                return
            
            result = self.intervention_service.adjust_housing_supply(
                market_service=self.market_service,
                count=count,
                zone=zone,
                price_per_sqm=price_per_sqm,
                size=size,
                school_units=school_units,
                build_year=build_year,
                config=self.config,
                current_month=month
            )
            
            self.developer_account_service.record_investment(count, month)
            
            print(f"\n✅ 成功投放 {result} 套房产！")
            
            stats = self.developer_account_service.get_stats()
            print(f"  当前待售: {stats['unsold_count']} 套")
            print(f"  累计投放: {stats['total_invested']} 套")
            
        except ValueError as e:
            print(f"❌ 输入格式错误: {e}")
        except Exception as e:
            logger.error(f"投放房产失败: {e}")
            print(f"❌ 投放失败: {e}")

    def run(self, allow_intervention_panel: bool = True):
        """Main Simulation Loop (Coordinator)"""
        self.initialize()
        start_month = self.current_month
        self.status = "running"
        logger.info(f"Starting Simulation: {self.months} Months (From {start_month + 1} to {self.months})")

        try:
            while self.current_month < self.months:
                next_month = self.current_month + 1
                self._run_month(next_month, allow_intervention_panel=allow_intervention_panel)
                self.current_month = next_month

            # --- Phase 10: End-of-Run Reporting ---
            enable_end_reports = bool(self.config.get("reporting.enable_end_reports", True))
            if enable_end_reports:
                logger.info("Generating Final Agent Reports (Automated Portrait)...")
                asyncio.run(self.reporting_service.generate_all_agent_reports(self.months))
            else:
                logger.info("Final Agent Reports skipped by config: reporting.enable_end_reports=false")
            self.status = "completed"
            self.completed_at = datetime.datetime.now().isoformat()
            self.final_summary = self.get_final_summary()
            self.write_parameter_assumption_report()
            self.write_motivation_agent_report()

        except KeyboardInterrupt:
            logger.info("Simulation Stopped by User.")
            self.status = "paused"
        except Exception as e:
            self.status = "failed"
            self.last_error = str(e)
            logger.error(f"Simulation Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.close()

    def run_night(self):
        """Non-interactive full run for scheduled overnight experiments."""
        self.run(allow_intervention_panel=False)

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
        if self._run_log_handler is not None:
            root_logger = logging.getLogger()
            try:
                root_logger.removeHandler(self._run_log_handler)
                self._run_log_handler.close()
            except Exception:
                pass
            self._run_log_handler = None


if __name__ == "__main__":
    # Allow running directly for testing

    # Clean up previous DB to avoid unique constraint errors
    db_file = "simulation.db"
    if os.path.exists(db_file):
        try:
            os.remove(db_file)
            print(f"Removed existing {db_file} for clean run.")
        except Exception as e:
            print(f"Warning: Could not remove {db_file}: {e}")

    runner = SimulationRunner(agent_count=50, months=1)
    runner.run()
    runner.close()
