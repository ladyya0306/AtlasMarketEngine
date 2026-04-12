import logging
import sys
from typing import Dict, List

from tqdm import tqdm


class WorkflowLogger:
    """
    工作流日志管理器
    负责将模拟过程以结构化、可视化的方式输出到控制台。
    替代原有的散乱 print() 语句。
    """

    def __init__(self, config=None):
        self.config = config
        self.logger = logging.getLogger('workflow')
        self._setup_logger()

        # 计数器
        self.negotiation_count = 0

    def _setup_logger(self):
        """统一日志格式"""
        # 防止重复添加 Handler
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        handler = logging.StreamHandler(sys.stdout)
        # 简化的格式，因为主要靠 structure output
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def section_header(self, title: str):
        """打印主章节标题"""
        print(f"\n{'=' * 60}")
        print(f"  {title}")
        print(f"{'=' * 60}")

    def subsection_header(self, title: str):
        """打印子章节标题"""
        print(f"\n--- {title} ---")

    # ====== 阶段 1: Agent 生成 ======
    def show_agent_generation_summary(self, agents: List, sample_size: int = 3):
        """显示生成的 Agent 样本"""
        self.section_header("📋 阶段1：Agent 数据生成")

        print(f"\n共生成 {len(agents)} 个 Agent。前 {sample_size} 个样本:")

        for i, agent in enumerate(agents[:sample_size], 1):
            print(f"\n【Agent {agent.id}】")
            print(f"  姓名: {agent.name}")
            print(f"  年龄: {agent.age} | 婚姻: {agent.marital_status}")
            print(f"  收入: {agent.monthly_income:,.0f} 元/月")
            print(f"  现金: {agent.cash:,.0f} 元")
            print(f"  职业: {agent.story.occupation}")
            print(f"  房产: {len(agent.owned_properties)} 套")

        if len(agents) > sample_size:
            print(f"\n... (省略剩余 {len(agents) - sample_size} 个 Agent)")

    # ====== 阶段 2: Agent 激活 ======
    def show_activation_summary(self, activation_decisions: List[Dict], sample_size: int = 3):
        """显示 LLM 激活决策样本"""
        self.section_header("🎯 阶段2：LLM 角色激活")

        active_roles = [d for d in activation_decisions if d['role'] in ['BUYER', 'SELLER']]
        print(f"\n本月共有 {len(active_roles)} 个 Agent 被激活为买家或卖家。")

        if not active_roles:
            print("  (本月市场平淡，无活跃角色)")
            return

        print(f"前 {sample_size} 个决策样本:")
        for decision in active_roles[:sample_size]:
            role_icon = "🛒" if decision['role'] == 'BUYER' else "🏷️"
            print(f"\n【Agent {decision['id']}】 {role_icon} {decision['role']}")
            print(f"  触发原因: {decision.get('trigger', 'N/A')}")
            print(f"  紧迫程度: {decision.get('urgency', 0.0):.1f}")
            # 如果有思考过程简略显示
            # thought = decision.get('reason', '')
            # if thought:
            #     print(f"  思考摘要: {thought[:50]}...")

    # ====== 阶段 3: 买卖双方名单 ======
    def show_role_lists(self, buyers: List, sellers: List, limit: int = 10):
        """显示买卖双方 ID 列表"""
        self.section_header("👥 阶段3：买卖双方入场")

        buyer_ids = [b.id for b in buyers]
        # sellers 可能是字典列表或对象列表，适配一下
        if sellers and isinstance(sellers[0], dict):
            seller_ids = [s.get('owner_id', s.get('seller_id', 'N/A')) for s in sellers]
        else:
            seller_ids = [s.id for s in sellers] if sellers else []

        print(f"\n🛒 买家 ({len(buyers)} 人): {buyer_ids[:limit]}")
        if len(buyers) > limit:
            print(f"   ... (共 {len(buyers)} 人)")

        print(f"\n🏷️  卖家 ({len(sellers)} 人): {seller_ids[:limit]}")
        if len(sellers) > limit:
            print(f"   ... (共 {len(sellers)} 人)")

    # ====== 阶段 4: 挂牌信息 (可选，合并到谈判或独立) ======
    def show_listings(self, listings: List[Dict], limit: int = 5):
        if not listings:
            return
        self.subsection_header("房源挂牌概览")
        for i, listing in enumerate(listings[:limit]):
            print(f"  [房产 {listing['property_id']}] 挂牌价: {listing['listed_price']:,.0f} 元 | 区域 {listing.get('zone', '?')}")
        if len(listings) > limit:
            print(f"  ... (共 {len(listings)} 套挂牌)")

    # ====== 阶段 5 & 6: 匹配与谈判 ======

    def log_negotiation(self, buyer_id: int, seller_id: int, property_id: int,
                        listed_price: float, history: List[Dict], success: bool, final_price: float):
        """
        记录一次完整的谈判过程
        设计为：前 N 个完整显示，后面的仅显示结果摘要
        """
        self.negotiation_count += 1

        # 阈值控制：前 2 个完整显示
        show_full = (self.negotiation_count <= 2)

        if show_full:
            if self.negotiation_count == 1:
                self.section_header("💬 阶段6：谈判实录 (展示前2例)")

            print(f"\n====== 谈判案例 #{self.negotiation_count} ======")
            print(f"买家 {buyer_id} vs 卖家 {seller_id} | 房产 {property_id}")
            print(f"挂牌价: {listed_price:,.0f} 元")

            print("\n--- 对话记录 ---")
            for round_data in history:
                party = "🛒 买家" if round_data['party'] == 'buyer' else "🏷️  卖家"
                action = round_data.get('action', 'UNKNOWN')

                # 安全获取价格
                price_val = round_data.get('price')
                if price_val is not None:
                    try:
                        price_str = f"{float(price_val):,.0f}"
                    except BaseException:
                        price_str = str(price_val)
                else:
                    price_str = "-"

                # 兼容新旧字段 (message/content)
                content = round_data.get('message', round_data.get('content', ''))

                # 截断过长内容
                display_content = (str(content)[:60] + '...') if len(str(content)) > 60 else content

                print(f"  [轮次 {round_data.get('round', '?')}] {party} ({action})")
                print(f"    出价: {price_str} 元")
                print(f"    理由: {display_content}")

            result_icon = "✅ 成交" if success else "❌ 谈崩"
            final_p_str = f"{final_price:,.0f}" if success else "N/A"
            print(f"--- 结果: {result_icon} (最终价: {final_p_str}) ---\n")

        else:
            # 简略模式 (可选)
            # print(f"  [谈判 #{self.negotiation_count}] {buyer_id} <-> {seller_id}: {'✅' if success else '❌'}")
            pass

    # ====== 阶段 7: 成交汇总 ======
    def show_monthly_summary(self, month: int, transactions: List, elapsed_time: float):
        """月度总结"""
        self.section_header(f"📅 第 {month} 月 模拟结束")
        print(f"成交数量: {len(transactions)} 笔")
        print(f"本月耗时: {elapsed_time:.2f} 秒")

        if transactions:
            avg_price = sum(t['price'] for t in transactions) / len(transactions)
            print(f"平均成交价: {avg_price:,.0f} 元")

    # ====== 进度条工具 ======
    def get_progress_bar(self, iterable, desc="", total=None):
        """获取 tqdm 进度条"""
        return tqdm(iterable, desc=desc, total=total,
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]")
