
"""
Agent Income Tier Configuration for Million Agent Simulation
"""

AGENT_TIER_CONFIG = {
    "tier_boundaries": {  # Annual Income (CNY) lower bounds
        "ultra_high": 1000000,   # >= 100w
        "high": 500000,          # >= 50w
        "middle": 200000,        # >= 20w
        "lower_middle": 100000,  # >= 10w
        "low": 0                 # < 10w
    },

    "tier_distribution": {  # Ratio out of 100 (Population Structure)
        "ultra_high": 1,    # 1% - Elite
        "high": 9,          # 9% - Upper Class
        "middle": 30,       # 30% - Middle Class
        "lower_middle": 40,  # 40% - Working Class
        "low": 20           # 20% - Low Income
    },

    # Financial Init Parameters (Random Range Multipliers based on Income)
    "init_params": {
        "ultra_high": {"cash_ratio": (5, 20), "property_count": (3, 6)},
        "high": {"cash_ratio": (3, 10), "property_count": (1, 3)},
        "middle": {"cash_ratio": (1, 5), "property_count": (0, 2)},
        "lower_middle": {"cash_ratio": (0.5, 3), "property_count": (0, 1)},
        "low": {"cash_ratio": (0, 1), "property_count": (0, 0)}
    }
}


def get_tier_by_income(income: float) -> str:
    """Return tier name for a given income"""
    bounds = AGENT_TIER_CONFIG["tier_boundaries"]
    if income >= bounds["ultra_high"]:
        return "ultra_high"
    if income >= bounds["high"]:
        return "high"
    if income >= bounds["middle"]:
        return "middle"
    if income >= bounds["lower_middle"]:
        return "lower_middle"
    return "low"
