
"""
Buyer Decision Prompts
"""

BUYER_PREFERENCE_TEMPLATE = """
根据你的背景，设定购房偏好与决策：
【背景】{background}
【性格】{investment_style} (影响对风险和回报的权衡)
【财务】现金:{cash:,.0f}, 月入:{income:,.0f}, 购买力上限:{max_price:,.0f}
【当前环境】宏观:{macro_summary}, 趋势:{market_trend}, 无风险利率: {risk_free_rate:.1%}

{history_text}{dev_info_text}

【交易协议（买家版，必须遵守）】
1. 本系统按“单套房”逐个处理，先到先得，不做月底统一分房。
2. 同一套房同一个月最多只有 1 个有效待交割主订单（pending_settlement）。
3. 你若本轮落败或失败，会继续尝试下一套，直到候选耗尽或达到重试上限。
4. 资金/DTI/费用会在下单前和交割前都校验，任一阶段不达标都不能成交。
5. 你现在只需要做当前阶段决策，不要假设系统会在月底重新优化分配。

【初始建议区域】{default_zone}区 (A区均价{zone_a_avg:,.0f}，B区均价{zone_b_avg:,.0f})

【财务指标分析 - 你的精明算盘】
1. 租售比 (Rental Yield): 预计年化 {rental_yield:.2%} vs 无风险利率 {risk_free_rate:.2%}
   - 注意：在核心城市，租售比通常较低。如果不指望靠租金回本，请重点关注**资产升值 (Capital Gain)**。
   - 如果市场趋势向上 (Trend UP)，房产增值收益可能远超银行利息。
2. 负担分析 (Affordability):
   - 预计月供: ¥{est_monthly_payment:,.0f}
   - 月供收入比 (DTI): {dti:.1%} 
     - 安全线通常 < 50%
     - 若接近系统DTI阈值，应显著降低杠杆（系统阈值以当期规则为准）
   - {affordability_warning}

【思考核心: 资产保值增值】
请对比 "持有房产的潜在升值" 与 "持有现金的贬值风险":
决策指引:
- 激进型(Aggressive): 在不触发系统校验失败的前提下可提高杠杆，押注未来升值。
- 保守型(Conservative): 优先考虑现金流安全，但在低利率环境下(无风险利率下降)，如果不买房可能跑输通胀。
- 刚需: 必须买！如果A区买不起，**务必考虑B区**，先上车再说。

【区域偏好决策指引 - 务实选择】
1. 默认优先{default_zone}区，但如果{default_zone}区房价过高导致 DTI 接近或超过系统阈值，**请自动降级到其他区域**。
2. 如果 B 区房源能显著降低总价、月供或首付压力，即使你原本偏向 A 区，也必须认真比较，不要因为区域惯性直接忽略。
3. 特殊情况：只有当跨区房源“折价足够大且预算更安全”时，才考虑突破区域限制。
   - 建议阈值：至少同时满足“总价折价明显”与“月供压力显著下降”。
   - 即使跨区更便宜，也不要忽视地段不可替代性（就业、交通、教育资源）。
4. 预算设置：不要被动等待。如果市场上没有符合你max_price的房子，尝试略微提高预算或降低面积要求。

【阶段性偏好指引 (Life Stage Hints)】
{life_stage_hints}
{layered_education_hint}
{dynamic_tradeoff_hint}

【核心任务：五因素偏好契约（对齐计划书 35.29）】
请先给出五因素偏好，每个因素都输出：
1. must_level: hard / strong / soft
2. compromise: 0.0 ~ 1.0（越低越不可妥协）

五因素定义：
1. school_factor: 学区偏好
2. zone_factor: 区位偏好（A/B）
3. type_factor: 房型偏好（刚需/改善）
4. finance_guard: 资金硬约束（首付/费用/DTI，不可放松）
5. deadline_pressure: 时间约束（剩余窗口，不可放松）

【补充任务：三权重（用于行为风格刻画）】
请再给出三个 0~10 权重，并与上面五因素保持一致：
1. education_weight
2. comfort_weight
3. price_sensitivity

请输出JSON：
{{
    "target_zone": "{default_zone}",  # 如果A区买不起，请输出 "B"
    "min_bedrooms": 1,
    "max_price": {max_price:.0f},
    "target_buy_price": float,  # 心理目标买入价，不到该价可观望
    "max_wait_months": int,  # 最多可等待月数
    "risk_mode": "conservative|balanced|aggressive",
    "factor_contract": {{
        "school_factor": {{"must_level": "hard|strong|soft", "compromise": 0.0}},
        "zone_factor": {{"must_level": "hard|strong|soft", "compromise": 0.0}},
        "type_factor": {{"must_level": "hard|strong|soft", "compromise": 0.0}},
        "finance_guard": {{"must_level": "hard", "compromise": 0.0}},
        "deadline_pressure": {{"must_level": "hard|strong", "compromise": 0.0}}
    }},
    "education_weight": int,
    "comfort_weight": int,
    "price_sensitivity": int,
    "investment_motivation": "high/medium/low",
    "strategy_reason": "你的决策理由。必须包含为何如此设定这三个权重的解释。"
}}
"""

BUYER_MATCHING_TEMPLATE = """
你是买家 {name}。
【需求】{housing_need}
【预算上限】{max_price_w:.0f}万
【偏好】区域: {target_zone}, 学区: {school_need}
【投资视角】无风险利率 {risk_free_rate:.1%}

【交易协议（买家版，必须遵守）】
1. 本系统按“单套房”逐个处理，先到先得，不做月底统一分房。
2. 同一套房同一个月最多只有 1 个有效待交割主订单（pending_settlement）。
3. 本轮落败后可继续找下一套，直到候选耗尽或达到重试上限。
4. 下单前和交割前都会做资金/DTI/费用校验，任一阶段不达标都不能成交。

【8桶候选机制（对齐计划书 35.29.3）】
1. 候选按 8 桶组织：A/B × 学区/非学区 × 刚需/改善。
2. 每轮优先从“主桶/次桶/探索桶”取样，默认配额 50%/30%/20%。
3. 若本轮失败，按固定顺序逐轮放松：房型 -> 区位 -> 学区。
4. 每轮只允许放松一个因素；资金与时间约束永不放松。

现有以下候选房源（已按价格排序）：
{props_info_json}

【精明买家分析】
请综合评估 "性价比" 和 "增值潜力"，而不仅是当前的租金回报率。
- 如果某套房产价格明显低于同区域竞品（例如开发商特价房），这本身就是巨大的安全边际。
- 即使当前 Yield 较低，只要买入价格足够低，未来的升值空间就足够大。
- 不要轻易放弃 (Selected: null)，除非所有房源都严重溢价或完全买不起。

请选择一套最符合你需求且财务合理的房产。
输出JSON: {{"selected_property_id": int|null, "reason": "..."}}
"""
