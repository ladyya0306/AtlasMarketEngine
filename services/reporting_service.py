import asyncio
import datetime
import json
import logging
import sqlite3
from typing import Dict

from utils.llm_client import call_llm_async

logger = logging.getLogger(__name__)


async def safe_call_llm_async(prompt, default_return, system_prompt="", model_type="fast"):
    """
    Backward-compatible text wrapper for tests and legacy callers.
    Unlike utils.safe_call_llm_async, this wrapper is for free-form text output.
    """
    response = await call_llm_async(prompt, system_prompt=system_prompt, model_type=model_type)
    text = str(response or "").strip()
    if not text or text.startswith("Error:"):
        return default_return
    return text


class ReportingService:
    def __init__(self, config, db_conn: sqlite3.Connection):
        self.config = config
        self.conn = db_conn

    async def generate_all_agent_reports(self, month: int, run_id: str = None) -> int:
        """
        Generate end-of-simulation reports for ALL agents.
        Returns count of reports generated.
        """
        logger.info(f"Generating Agent End Reports for Month {month}...")

        # 1. Get all Agent IDs
        cursor = self.conn.cursor()
        # Exclude system/developer virtual accounts (agent_id <= 0).
        cursor.execute("SELECT agent_id FROM agents_static WHERE agent_id > 0")
        agent_ids = [r[0] for r in cursor.fetchall()]

        # 2. Process in batches to control concurrency (if using LLM)
        batch_size = 5
        generated_count = 0

        # Check if LLM portrait is enabled.
        # Default to False for research runs to avoid non-decision token consumption.
        use_llm = getattr(self.config, 'enable_llm_portraits', None)
        if use_llm is None and hasattr(self.config, "get"):
            use_llm = self.config.get("enable_llm_portraits", False)
        use_llm = bool(use_llm)

        for i in range(0, len(agent_ids), batch_size):
            batch_ids = agent_ids[i: i + batch_size]
            tasks = [self._process_single_agent(aid, run_id, use_llm) for aid in batch_ids]

            # Run batch
            results = await asyncio.gather(*tasks)
            generated_count += sum(1 for ok in results if ok)

            logger.info(f"Processed batch {i // batch_size + 1}, total {generated_count}/{len(agent_ids)}")

        logger.info("Agent Reporting Complete.")
        return generated_count

    async def _process_single_agent(self, agent_id: int, run_id: str, use_llm: bool):
        """Aggregate data and generate report for one agent."""
        try:
            # 1. Collect Data
            data = self._collect_agent_data(agent_id)

            # 2. Generate LLM Portrait (Optional)
            if use_llm:
                portrait = await self._generate_llm_portrait(data)
                data['llm_portrait'] = portrait
            else:
                data['llm_portrait'] = "LLM Analysis Disabled"

            # 3. Persist
            self._persist_report(agent_id, run_id, data)
            return True
        except Exception as e:
            logger.error(f"Failed to report agent {agent_id}: {e}")
            return False

    def _collect_agent_data(self, agent_id: int) -> Dict:
        """Fetch all relevant DB data for the agent."""
        cursor = self.conn.cursor()

        # Identity
        cursor.execute("SELECT * FROM agents_static WHERE agent_id=?", (agent_id,))
        static = dict(cursor.fetchone())

        # Finance
        cursor.execute("SELECT * FROM agents_finance WHERE agent_id=?", (agent_id,))
        finance = dict(cursor.fetchone())

        # Transactions (Buy/Sell)
        cursor.execute("""
            SELECT month, property_id, final_price, 'BUY' as type
            FROM transactions WHERE buyer_id=?
            UNION ALL
            SELECT month, property_id, final_price, 'SELL' as type
            FROM transactions WHERE seller_id=?
            ORDER BY month
        """, (agent_id, agent_id))
        tx_rows = cursor.fetchall()
        transactions = [dict(r) for r in tx_rows]

        # Key Decisions (Sample important ones)
        cursor.execute("""
            SELECT month, event_type, decision, reason
            FROM decision_logs
            WHERE agent_id=? AND event_type IN ('BID', 'LIST_PROPERTY', 'ACCEPT_OFFER', 'REJECT_OFFER')
            ORDER BY month
        """, (agent_id,))
        dec_rows = cursor.fetchall()
        decisions = [dict(r) for r in dec_rows]

        return {
            "identity": static,
            "finance": finance,
            "transactions": transactions,
            "decisions": decisions
        }

    async def _generate_llm_portrait(self, data: Dict) -> str:
        """Call the LLM to generate a free-form portrait."""
        identity = data['identity']
        finance = data['finance']
        txs = data['transactions']
        decisions = data['decisions']

        # Simplify data for prompt
        tx_summary = ", ".join([f"Month {t['month']} {t['type']} 房产{t['property_id']} ({t['final_price'] / 10000:.0f}万)" for t in txs]) if txs else "无交易"

        prompt = f"""
        你是一位犀利的房地产观察家。请为以下 Agent 撰写一段【人物画像/投资风格辣评】（100字左右）。

        **档案**:
        - 姓名: {identity['name']} ({datetime.datetime.now().year - identity['birth_year']}岁)
        - 职业: {identity['occupation']}
        - 设定风格: {identity['investment_style']}
        - 财务状况: 现金 {finance['cash'] / 10000:.0f}万, 净资产 {finance['total_assets'] / 10000:.0f}万, 负债 {finance['total_debt'] / 10000:.0f}万

        **行为记录**:
        - 交易历史: {tx_summary}
        - 关键决策数: {len(decisions)} 次

        **要求**:
        1. 风格犀利、幽默或一针见血。
        2. 评价其行为是否符合其身份和投资风格。
        3. 如果是"韭菜"（高买低卖）请无情嘲讽；如果是"股神"（低买高卖）请给予赞赏；如果是"等等党"（一直不买）请评价其心态。
        4. 必须用中文。
        """

        portrait_model_type = "fast"
        if hasattr(self.config, "get"):
            portrait_model_type = str(
                self.config.get("system.reporting.portrait_model_type", "fast")
            ).strip().lower()
        if portrait_model_type not in {"fast", "smart"}:
            portrait_model_type = "fast"
        return await safe_call_llm_async(
            prompt,
            default_return="分析生成失败",
            model_type=portrait_model_type,
        )

    def _persist_report(self, agent_id: int, run_id: str, data: Dict):
        """Save structured data to DB."""
        cursor = self.conn.cursor()

        # Serialize JSON fields
        id_json = json.dumps(data['identity'], ensure_ascii=False)
        fin_json = json.dumps(data['finance'], ensure_ascii=False)
        tx_json = json.dumps(data['transactions'], ensure_ascii=False)
        dec_json = json.dumps(data['decisions'], ensure_ascii=False)
        llm_text = data.get('llm_portrait', "")

        if isinstance(llm_text, dict):  # Handle if safe_call returned dict error
            llm_text = str(llm_text)

        cursor.execute("""
            INSERT INTO agent_end_reports
            (agent_id, simulation_run_id, identity_summary, finance_summary, transaction_summary, imp_decision_log, llm_portrait)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (agent_id, run_id, id_json, fin_json, tx_json, dec_json, llm_text))

        self.conn.commit()
