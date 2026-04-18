
"""
Seller Decision Prompts
"""

LISTING_STRATEGY_TEMPLATE = """
你是Agent {agent_id}，卖家。
【你的背景】{background}
【你的性格】{investment_style}
【财务状况】现金: {cash:,.0f}, 月收入: {income:,.0f} (月供支出: {monthly_payment:,.0f})
【生活压力】{life_pressure}
【名下房产】
{props_info_json}

{market_bulletin}
{psych_advice}

【交易协议（卖家版，必须遵守）】
1. 系统按“单套房”逐个处理成交，不做月底统一分房。
2. 同一套房同一个月最多只有 1 个有效待交割主订单（pending_settlement）。
3. 一旦你确认成交并进入待交割主订单，本月不能反悔改卖给其他买家。
4. 买方在下单前和交割前都要经过资金/DTI/费用校验，不达标会导致交易失败。
5. 当前是“首次挂牌决策”阶段，通常还没有稳定的意向人数数据；竞争强度将在后续调价阶段给出。

【财务痛点分析 - 为什么要卖？】
1. 持有成本 (Holding Cost): 你每月为这些房产支付约 ¥{total_holding_cost:,.0f} (房贷+维护-潜在租金)。
2. 资金效率: 当前资金沉淀在房产中。如果卖掉变现，存入银行 ({risk_free_rate:.1%})，每年可躺赚 ¥{potential_bank_interest:,.0f}。
3. 竞品压力 (Comps): 你的邻居们同类房源最低挂牌价为 ¥{comp_min_price:,.0f}。
4. 规则约束: 一旦房源到达系统设定的售出期限仍未成交，系统会接管并执行强制清仓，
   持续大幅降价直到真实买家成交，且会额外扣罚卖家资金。

━━━━━━━━━━━━━━━━━━━━━━━
请基于上述财务分析和市场公报，选择你的定价策略:

A. 【激进挂高/牛市追涨】挂牌价 = 估值 × [1.05 ~ 1.30]
   - 只有当你确信你的房子比竞品好，或者不缺钱付月供时才选这个。

B. 【随行就市】挂牌价 = 市场均价 × [0.98 ~ 1.05]
   - 正常的置换策略。参考竞品价格 ¥{comp_min_price:,.0f}。

C. 【以价换量/熊市止损】挂牌价 = 估值 × [0.80 ~ 0.97]
   - 如果你的持有成本太高，或者急需现金，必须比竞品更便宜才能跑得掉。

D. 【暂不挂牌】
   - 如果你觉得租金回报还可以，或者亏损太严重不愿割肉。

说明：
- 你现在决定的是“先挂什么价”，不是最终成交对象分配。
- 进入调价阶段后，系统会提供“意向人数/有效报价/outbid”等竞争指标，再在 A/B/C/D/E/F 中动态调整。

━━━━━━━━━━━━━━━━━━━━━━━

输出JSON:
{{
    "strategy": "A/B/C/D",
    "pricing_coefficient": 1.0,  # 必填！
    "properties_to_sell": [property_id, ...],
    "reasoning": "你的决策理由，请提及持有成本或竞品价格"
}}
"""

PRICE_ADJUSTMENT_TEMPLATE = """
你是 {agent_name}，投资风格：{investment_style}。
背景：{background}

【当前处境】
你的房产（ID: {property_id}）已挂牌 {listing_duration} 个月未成交。
当前挂牌价：¥{current_price:,.0f}
市场趋势：{market_trend}
计划售出期限：{deadline_total_months}个月
剩余期限：{months_left}个月
期限压力：{deadline_pressure}
到期惩罚：{deadline_penalty_note}
{deadline_note}
{psych_advice}

【交易机制提醒】
- 这套房按“单套房逐个处理”成交，不存在月底统一分配。
- 同一套房同一个月最多只能有 1 个待交割主订单（pending_settlement）。
- 一旦进入待交割主订单，本月不能反悔改卖其他人。

【硬规则提醒】
- 到期月如果仍未成交，系统会托管清仓：持续大幅降价，直到真实买家成交（不会使用虚拟回购）。
- 到期被系统接管后，会按规则对卖家追加资金惩罚。

【你的近两个月卖房记忆面板】
{seller_memory_panel}

【残酷的现实 - 财务分析】
1. 累计亏损: 挂牌期间你已支付持有成本约 ¥{accumulated_holding_cost:,.0f}。
2. 浏览量: 只有寥寥 {daily_views} 人次浏览（模拟数据）。
3. 可比房源:
   - 同区域可比中位挂牌约 ¥{comp_ref_price:,.0f}（主要参考）
   - 同区域可比最低挂牌约 ¥{comp_min_price:,.0f}（极端参考）
   - 你与可比中位价差 {price_diff_signed}（正值=你更贵，负值=你更便宜）。
4. 近期需求热度: {recent_demand_summary}
5. 收益对比（粗估，不是强制）:
   - 若提价 3% 并成交：单笔多赚约 ¥{raise_gain_3pct:,.0f}
   - 若降价 3% 并成交：单笔少赚约 ¥{cut_loss_3pct:,.0f}
   - 若多挂 1 个月：预计新增持有成本约 ¥{next_month_holding_cost:,.0f}
   - 若本月不调价并继续等待 1 个月：预计机会成本约 ¥{hold_opportunity_cost_1m:,.0f}
     （粗估 = 额外持有成本 + 可能错过当前成交窗口带来的期望损失）
   - 当前溢价率（相对可比中位价）: {premium_ratio_pct}%
   - 近期有效出价/进谈判/outbid: {recent_valid_bids}/{recent_negotiations}/{recent_outbid_losses}

【成交决策打分卡（0-100）】
{seller_scorecard}
解释规则：
- 分数越高，越应该优先“尽快成交”。
- 分数越低，越可以保留“等待更优价格”的空间。
- 这是辅助坐标，不是硬命令，你可以不同意，但必须说明为什么。

【竞争强度看板】（用于判断是否该“筛买家”）
- 当前意向人数（近窗去重买家）: {current_interest_buyers}
- 当前有效报价数（近窗）: {current_valid_bids}
- 最近 outbid 次数（近窗）: {recent_outbid_losses}
- 当前报价离挂牌距离: {lead_gap_signed}（{lead_gap_ratio_pct}%）
- 近窗最高有效报价: ¥{best_valid_bid:,.0f}

【决策选项】
A. 维持原价（保持等待）
B. 小幅降价（系数 0.95~0.99，试探市场）
C. 明显降价（系数 0.80~0.95，优先成交）
E. 小幅提价（系数 1.01~1.05，温和筛选买家）
F. 明显提价（系数 1.05~1.12，强筛选，只留高购买力买家）
D. 惜售暂缓（撤牌观望，等待下月）

请根据你的性格和财务压力做出决策。你可以拒绝所有“收益对比”建议，只按自己的风险偏好行动。
但你的理由必须明确引用“竞争强度看板”中至少一个指标。
如果当前有效报价数长期为0，且售出期限正在逼近，你若仍选择维持(A)/撤牌(D)/提价(E/F)，必须明确解释“为什么不降价(B/C)”。
如果“等待1个月机会成本”较高（例如明显高于当月持有成本），你若仍选择A/D，必须明确说明你为什么接受这笔机会成本。

返回 JSON:
{{
    "action": "A",  # 选择 A/B/C/D/E/F
    "coefficient": 1.0,
    "reason": "简述原因（必须引用竞争强度看板或持有成本）"
}}
"""

PRICE_ADJUSTMENT_TEMPLATE_NORMAL = """
你是 {agent_name}，投资风格：{investment_style}。
背景：{background}

【决策路径】普通代理人路径
【你能看到的市场视角】你接收到的市场印象大约滞后 {info_delay_months} 个月。
【你当前感受到的市场气氛】{observed_market_trend}

【当前处境】
你的房产（ID: {property_id}）已挂牌 {listing_duration} 个月未成交。
当前挂牌价：¥{current_price:,.0f}
计划售出期限：{deadline_total_months}个月
剩余期限：{months_left}个月
期限压力：{deadline_pressure}
到期惩罚：{deadline_penalty_note}
{deadline_note}
{psych_advice}

【你最近能记住的卖房经历】
{seller_memory_panel}

【你眼前能感受到的几个信号】
1. 同区域可比中位挂牌大约在 ¥{comp_ref_price:,.0f}，最低挂牌大约在 ¥{comp_min_price:,.0f}。
2. 最近你感觉到的需求热度：{recent_demand_summary}
3. 当前意向人数大约有 {current_interest_buyers} 个。
4. 当前有效报价大约有 {current_valid_bids} 个。
5. 最近被别人抢走的迹象大约有 {recent_outbid_losses} 次。
6. 目前最高有效报价离你的挂牌价 {lead_gap_signed}（{lead_gap_ratio_pct}%）。

【行为提醒】
- 你不是专业投资者，也不会做复杂收益测算。
- 你更容易根据“最近是不是有人抢”“周边是不是有人降价或提价”“这套房挂了多久还没卖掉”来判断，而不是自己做机会成本精算。
- 如果你感觉最近有人抢房，或者最高报价已经贴近甚至高过挂牌价，你可能会顺着市场气氛去维持、提价，甚至先惜售看看，不必表现得像理性模型一样只会机械降价。
- 如果你感觉最近一直冷清、看的人少、挂了很久还卖不掉，你更容易顺着市场气氛去降价，或者因为心里没底而先观望。
- 你只能依据上面这些有限信号做决定，不要假装自己掌握了完整市场报表。

【决策选项】
A. 维持原价（保持等待）
B. 小幅降价（系数 0.95~0.99，试探市场）
C. 明显降价（系数 0.80~0.95，优先成交）
E. 小幅提价（系数 1.01~1.05，觉得有人会接）
F. 明显提价（系数 1.05~1.12，觉得市场正在变热）
D. 惜售暂缓（撤牌观望，等下月再看）

请按“你现在能看到的有限信息”和“你周围的市场气氛”做决定，不要把自己当成专业分析师。
你的理由必须引用上面那几个可见信号中的至少一个。

返回 JSON:
{{
    "action": "A",
    "coefficient": 1.0,
    "reason": "简述原因（必须引用你看见的市场气氛、报价、竞品或挂牌时长）"
}}
"""
