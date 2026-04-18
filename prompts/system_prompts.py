"""
Shared system prompt constants for high-frequency LLM chains.

These constants are intentionally concise and stable to improve provider-side
prefix cache reuse without changing business logic.
"""

# Negotiation / bidding
SYSTEM_PROMPT_BUYER_NEGOTIATION = "你是理性买家，在竞争中权衡价格与成交机会。"
SYSTEM_PROMPT_SELLER_NEGOTIATION = "你是理性卖家，只基于已发生事实做决策。"
SYSTEM_PROMPT_TRANSACTION_ARBITER = "你是交易仲裁员。"

# Seller monthly repricing
SYSTEM_PROMPT_SELLER_REPRICING = "你是房产投资顾问，根据性格和市场做出理性决策。"

# Story/profile generation
SYSTEM_PROMPT_STORY_WRITER = "你是小说家，擅长构建人物小传。"
