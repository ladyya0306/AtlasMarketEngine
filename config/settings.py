# Oasis Simulation Settings (V2.6)

# ---------------------------------------------------------
# 1. Market Configuration
# ---------------------------------------------------------
INITIAL_MARKET_CONFIG = {
    "A": {
        "base_price_per_sqm": 35000,
        "price_range": (30000, 45000),
        "school_district_ratio": 0.3
    },
    "B": {
        "base_price_per_sqm": 15000,
        "price_range": (12000, 20000),
        "school_district_ratio": 0.1
    }
}

# Property Distribution (Count of properties per zone/quality)
PROPERTY_DISTRIBUTION = {
    "A": {"quality_1": 30, "quality_2": 50, "quality_3": 20},
    "B": {"quality_1": 80, "quality_2": 100, "quality_3": 20},
}

# ---------------------------------------------------------
# 2. Mortgage Configuration
# ---------------------------------------------------------
MORTGAGE_CONFIG = {
    "down_payment_ratio": 0.3,      # 30% Down payment
    "annual_interest_rate": 0.05,   # 5% Annual interest rate
    "loan_term_years": 30,          # 30 Years loan term
    "max_dti_ratio": 0.5            # Max Debt-to-Income ratio (50%)
}

# ---------------------------------------------------------
# 3. Life Event Pool
# ---------------------------------------------------------
LIFE_EVENT_POOL = [
    {"event": "升职加薪", "cash_change": 0.2, "buy_tendency": 0.3, "sell_tendency": 0.0},
    {"event": "年终奖", "cash_change": 0.15, "buy_tendency": 0.2, "sell_tendency": 0.0},
    {"event": "结婚", "cash_change": 0.0, "buy_tendency": 0.5, "sell_tendency": 0.0},
    {"event": "生子", "cash_change": -0.1, "buy_tendency": 0.4, "sell_tendency": 0.0},
    {"event": "降薪", "cash_change": -0.15, "buy_tendency": -0.3, "sell_tendency": 0.2},
    {"event": "失业", "cash_change": -0.3, "buy_tendency": -0.5, "sell_tendency": 0.4},
    {"event": "生病", "cash_change": -0.2, "buy_tendency": -0.3, "sell_tendency": 0.3},
    {"event": "离婚", "cash_change": 0.0, "buy_tendency": 0.0, "sell_tendency": 0.5},
]

# ---------------------------------------------------------
# 4. Macro Environment Configuration
# ---------------------------------------------------------

# Defines parameters for different macro sentiments
MACRO_ENVIRONMENT = {
    "optimistic": {
        "income_growth_rate": 0.05,      # 5% annual growth rate
        "buy_tendency_modifier": 0.2,    # Boost buying prob
        "sell_tendency_modifier": -0.1,  # Reduce selling
        "price_expectation": 0.03,       # Expect 3% price rise
        "description": "经济繁荣，股市上涨，大家对未来充满信心。"
    },
    "stable": {
        "income_growth_rate": 0.02,      # 2% annual (inflation)
        "buy_tendency_modifier": 0.0,
        "sell_tendency_modifier": 0.0,
        "price_expectation": 0.0,
        "description": "经济平稳，政策稳定，市场供需平衡。"
    },
    "pessimistic": {
        "income_growth_rate": -0.02,     # 2% contraction
        "buy_tendency_modifier": -0.2,   # Reduce buying
        "sell_tendency_modifier": 0.2,   # panic selling
        "price_expectation": -0.05,      # Expect 5% drop
        "description": "经济衰退，失业率上升，市场弥漫着恐慌情绪。"
    }
}

# Macro-Economic Environment (Tier 2)
# Removed fixed schedule in favor of Interactive Macro (v2.6)


def get_current_macro_sentiment(month: int) -> str:
    """
    Return current macro sentiment.
    Default: STABLE.
    Future: Could query DB if we implement 'Set Macro State' intervention.
    """
    return "stable"
