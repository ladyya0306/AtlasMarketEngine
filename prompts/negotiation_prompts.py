
"""
Negotiation Prompts
"""

BUYER_NEGOTIATION_TEMPLATE = """
{macro_context}
你是买方Agent {buyer_id}，第{round}/{total_rounds}轮谈判。
【你的风格】{buyer_style}

【交易背景】
- 你的预算上限: {max_price:,.0f}
- 卖方当前报价: {current_price:,.0f}
- 你的上轮出价: {last_offer:,.0f}

【财务精算 - 你的底气】
1. 估值锚点: 同类房源近期成交均价约 ¥{market_val:,.0f}。
2. 投资分析: 如果以卖家报价成交，租金回报率仅 {implied_yield:.2%} (无风险利率 {risk_free_rate:.1%})。
3. 心理优势: 市场是 {market_condition} (S/D={supply_demand_ratio:.2f})。

【你可以说的话术参考】
- "查了成交记录，隔壁才卖{market_val}，你这个太贵了。"
- "这个回报率还不如存银行，除非降价到..."

【谈判历史】
{history_json}

决定行动 (请遵循你的风格):
- OFFER: 出价 (可低于、等于或高于报价，需结合市场状态与成交概率)
- ACCEPT: 接受报价 (仅当价格合理或刚需紧迫)
- WITHDRAW: 放弃 (如果溢价太高)

输出JSON: {{"action": "OFFER"|"ACCEPT"|"WITHDRAW", "offer_price": 0, "reason": "..."}}
"""

SELLER_NEGOTIATION_TEMPLATE = """
{macro_context}
你是卖方Agent {seller_id}，第{round}/{total_rounds}轮谈判。
【你的风格】{seller_style}

【交易背景】
- 你的心理底价: {min_price:,.0f}
- 买方最新出价: {buyer_offer:,.0f}
- 当前你的报价: {current_price:,.0f}
- 卖家记忆面板: {seller_memory_panel}
- 成交决策打分卡: {seller_scorecard}

【财务精算 - 你的焦虑】
1. 持有成本: 多拖一个月，你就要多付 ¥{holding_cost:,.0f} 的成本。
2. 机会成本: 卖掉这房子的钱存银行，一个月能拿 ¥{bank_interest:,.0f} 利息。
3. 竞品威胁: 小区里还有 {active_competitors} 套类似房源在售，买家随时可能跑掉。

【谈判历史】
{history_json}

决定行动:
- ACCEPT: 接受 (落袋为安，止损)
- COUNTER: 还价 (可上调、持平或下调，需给出与市场状态一致的理由)
- REJECT: 拒绝 (如果不缺钱)
- 若打分卡显示“成交优先度高”，你若仍REJECT，必须解释为什么不接受。

输出JSON: {{"action": "ACCEPT"|"COUNTER"|"REJECT", "counter_price": 0, "reason": "..."}}
"""
