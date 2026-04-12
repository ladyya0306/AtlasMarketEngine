"""
Agent行为日志记录器 - 按agent_id和时间排序完整记录LLM决策
用于研究分析Agent的思维过程和决策逻辑
"""
import csv
import os
from datetime import datetime
from typing import Any, Dict, List


class BehaviorLogger:
    """
    行为日志记录器

    记录内容：
    1. agent_decisions.csv - Agent月度决策（角色选择、策略描述、推理过程）
    2. negotiations_detail.csv - 谈判详情（每轮对话、内心想法）
    """

    def __init__(self, results_dir: str = "results"):
        self.results_dir = results_dir
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_dir = os.path.join(results_dir, f"result_{self.session_id}")
        os.makedirs(self.output_dir, exist_ok=True)

        # 初始化CSV文件路径
        self.decisions_file = os.path.join(self.output_dir, "agent_decisions.csv")
        self.negotiations_file = os.path.join(self.output_dir, "negotiations_detail.csv")

        # 计数器
        self.negotiation_counter = 0

        self._init_csv_files()

    def _init_csv_files(self):
        """初始化CSV文件头"""
        # 决策日志
        with open(self.decisions_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow([
                "月份", "Agent_ID", "姓名", "角色",
                "行动描述", "目标区域", "价格预期",
                "紧迫程度", "推理过程", "记录时间"
            ])

        # 谈判详情日志
        with open(self.negotiations_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow([
                "月份", "谈判ID", "轮次", "角色", "Agent_ID",
                "动作", "价格", "对外发言", "内心想法", "记录时间"
            ])

    def log_decision(self, month: int, agent: Any, decision: Dict):
        """
        记录Agent月度决策

        Args:
            month: 当前月份
            agent: Agent对象
            decision: LLM返回的决策字典
        """
        with open(self.decisions_file, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow([
                month,
                agent.id,
                getattr(agent, 'name', f'Agent_{agent.id}'),
                decision.get('role', 'OBSERVER'),
                decision.get('action_description', ''),
                decision.get('target_zone', ''),
                decision.get('price_expectation', ''),
                decision.get('urgency', 0.5),
                decision.get('reasoning', '').replace('\n', ' '),  # 去除换行
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ])

    def log_negotiation(self, month: int, buyer_id: int, seller_id: int,
                        property_id: int, history: List[Dict],
                        outcome: str, final_price: float = 0):
        """
        记录完整谈判过程

        Args:
            month: 当前月份
            buyer_id: 买家ID
            seller_id: 卖家ID
            property_id: 房产ID
            history: 谈判历史列表
            outcome: 谈判结果 (success/failed/max_rounds)
            final_price: 最终成交价
        """
        self.negotiation_counter += 1
        neg_id = f"{month}_{self.negotiation_counter}"

        with open(self.negotiations_file, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)

            # 写入每轮谈判记录
            for entry in history:
                party = entry.get('party', '')
                agent_id = buyer_id if party == 'buyer' else seller_id

                # Handle None values safely
                price_val = entry.get('price')
                message_val = entry.get('message') or ''
                thought_val = entry.get('thought') or ''

                writer.writerow([
                    month,
                    neg_id,
                    entry.get('round', 0),
                    "买方" if party == 'buyer' else "卖方",
                    agent_id,
                    entry.get('action', ''),
                    price_val if price_val is not None else '',
                    str(message_val).replace('\n', ' '),
                    str(thought_val).replace('\n', ' '),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ])

            # 写入谈判结果汇总行
            # Handle None values
            safe_price = float(final_price) if final_price is not None else 0

            if outcome == 'success':
                result_text = f'✅ 达成意向（待交割） ¥{safe_price:,.0f}'
            elif outcome == 'failed':
                result_text = '❌ 失败'
            elif outcome == 'max_rounds':
                result_text = '⏱️ 超时'
            else:
                result_text = str(outcome)

            writer.writerow([
                month, neg_id, "结果", "-", "-",
                result_text, safe_price,
                f"房产{property_id}: 买家{buyer_id} vs 卖家{seller_id}",
                "", datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ])

    def get_output_dir(self) -> str:
        """获取输出目录路径"""
        return self.output_dir

    def get_agent_history(self, agent_id: int, max_months: int = 3) -> str:
        """
        获取指定Agent最近N个月的决策历史和谈判历史（用于构建LLM上下文）

        Args:
            agent_id: Agent ID
            max_months: 最多返回几个月的记录

        Returns:
            格式化的历史字符串
        """
        history_lines = []

        # 1. 获取决策历史
        if os.path.exists(self.decisions_file):
            try:
                decisions = []
                with open(self.decisions_file, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if str(row.get('Agent_ID', '')) == str(agent_id):
                            decisions.append(row)

                # Filter recent
                recent_decisions = decisions[-max_months:] if len(decisions) > max_months else decisions
                for r in recent_decisions:
                    history_lines.append(f"- [月度决策] 第{r.get('月份', '?')}月: [{r.get('角色', '?')}] {r.get('行动描述', '')} (紧迫度:{r.get('紧迫程度', '?')})")
            except Exception:
                pass

        # 2. 获取谈判历史
        if os.path.exists(self.negotiations_file):
            try:
                negotiations = []
                with open(self.negotiations_file, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if str(row.get('Agent_ID', '')) == str(agent_id):
                            negotiations.append(row)

                # Filter recent negotiations (limit to last 5 entries to save tokens)
                recent_negs = negotiations[-5:] if len(negotiations) > 5 else negotiations
                for r in recent_negs:
                    action = r.get('动作', '')
                    thought = r.get('内心想法', '')
                    # Truncate thought
                    if len(thought) > 30:
                        thought = thought[:30] + "..."
                    history_lines.append(f"- [谈判记录] 第{r.get('月份', '?')}月: {action} (想法: {thought})")
            except Exception:
                pass

        if not history_lines:
            return ""

        return "\n".join(history_lines)
