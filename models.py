import json
from datetime import datetime
from typing import Dict, List, Optional


class AgentStory:
    def __init__(
        self,
        occupation="",
        career_outlook="",
        family_plan="",
        education_need="",
        housing_need="",
        selling_motivation="",
        background_story="",
        investment_style="balanced",
        purchase_motive_primary="",
        housing_stage="",
        family_stage="",
        education_path="",
        financial_profile="",
        seller_profile="",
    ):
        self.occupation = occupation
        self.career_outlook = career_outlook
        self.family_plan = family_plan
        self.education_need = education_need
        self.housing_need = housing_need
        self.selling_motivation = selling_motivation
        self.background_story = background_story
        self.investment_style = investment_style
        self.purchase_motive_primary = purchase_motive_primary
        self.housing_stage = housing_stage
        self.family_stage = family_stage
        self.education_path = education_path
        self.financial_profile = financial_profile
        self.seller_profile = seller_profile


class AgentPreference:
    def __init__(self, target_zone="", max_price=0.0, min_bedrooms=1, need_school_district=False,
                 max_affordable_price=0.0, psychological_price=0.0,
                 education_weight=5, comfort_weight=5, price_sensitivity=5):
        self.target_zone = target_zone
        self.max_price = max_price
        self.min_bedrooms = min_bedrooms
        self.need_school_district = need_school_district
        self.max_affordable_price = max_affordable_price
        self.psychological_price = psychological_price
        # V5 Deep Negotiation Attributes (0-10 scale)
        self.education_weight = education_weight
        self.comfort_weight = comfort_weight
        self.price_sensitivity = price_sensitivity


class BuyerPreference:
    def __init__(self, target_zone="", target_price_range=(0, 0), min_bedrooms=1, need_school_district=False, max_affordable_price=0.0, psychological_price=0.0, education_weight=5, comfort_weight=5, price_sensitivity=5):
        self.target_zone = target_zone
        self.target_price_range = target_price_range
        self.min_bedrooms = min_bedrooms
        self.max_price = target_price_range[1]
        self.need_school_district = need_school_district
        self.max_affordable_price = max_affordable_price
        self.psychological_price = psychological_price
        self.education_weight = education_weight
        self.comfort_weight = comfort_weight
        self.price_sensitivity = price_sensitivity


class Agent:
    def __init__(self, id: int, name: str = "", age: int = 30, marital_status: str = "single", cash: float = 0.0, monthly_income: float = 0.0):
        self.id = id
        self.name = name
        self.age = age
        self.marital_status = marital_status
        self.cash = cash
        self.last_month_cash = cash
        self.monthly_income = monthly_income
        self.owned_properties: List[Dict] = []
        self.life_events: Dict[int, str] = {}
        self.children_ages: List[int] = []

        self.children_ages: List[int] = []

        # 🆕 Extended Attributes via Composition
        self.story = AgentStory()
        self.preference = AgentPreference()
        # Runtime intelligence tier label: "normal" | "smart"
        self.agent_type = "normal"
        self.info_delay_months = 0
        self.monthly_event = None  # To store current month's event
        self.current_life_event = None
        self.timing_role = ""
        self.decision_urgency = ""
        self.lifecycle_labels: List[str] = []
        self.lifecycle_summary = ""
        self.substitute_spillover_context = {}
        self.substitute_spillover_month = -1
        self.mortgage_monthly_payment = 0.0  # Monthly mortgage payment commitment
        self.total_debt = 0.0  # Total mortgage debt
        # Runtime finance snapshots used by hard-constraint checks.
        self.total_assets = float(cash)
        self.net_cashflow = float(monthly_income)
        self.payment_tolerance_ratio = 0.45
        self.down_payment_tolerance_ratio = 0.30

        # Backward compatibility properties (property routing)
        @property
        def occupation(self): return self.story.occupation
        @occupation.setter
        def occupation(self, value): self.story.occupation = value

        @property
        def background_story(self): return self.story.background_story
        @background_story.setter
        def background_story(self, value): self.story.background_story = value

        @property
        def housing_need(self): return self.story.housing_need
        @housing_need.setter
        def housing_need(self, value): self.story.housing_need = value

        @property
        def education_need(self): return self.story.education_need
        @education_need.setter
        def education_need(self, value): self.story.education_need = value

    def get_life_event(self, month: int) -> Optional[str]:
        return self.life_events.get(month)

    def set_life_event(self, month: int, event: str):
        self.life_events[month] = event

    def has_children_near_school_age(self) -> bool:
        """Check if any child is near school age (5-6 years old)"""
        for age in self.children_ages:
            if 5 <= age <= 6:
                return True
        return False

    @property
    def net_worth(self) -> float:
        """Calculate total net worth (Cash + Property Value)"""
        # Note: Property value here simplifies to base_value or listed_price if available
        # Real calculation should query market current price, but for model simplicity we sum stored value
        prop_value = sum(p.get('base_value', 2000000) for p in self.owned_properties)
        return self.cash + prop_value

    def get_profile_summary(self) -> str:
        return (f"Agent {self.id} | Age: {self.age} | {self.marital_status} | "
                f"Cash: {self.cash / 10000:.0f}w | Props: {len(self.owned_properties)} | "
                f"Net Worth: {self.net_worth / 10000:.0f}w")

    def to_dict(self):
        # Legacy V1 dict
        return {
            "id": self.id,
            "age": self.age,
            "marital_status": self.marital_status,
            "cash": self.cash,
            "monthly_income": self.monthly_income,
            "children_ages": self.children_ages,
            "owned_properties_count": len(self.owned_properties),
            "net_worth": self.net_worth,
            "occupation": self.occupation,
            "education_need": self.education_need,
            "housing_need": self.housing_need
        }

    # --- V2 Schema Helpers ---
    @property
    def investment_style(self): return self.story.investment_style

    def to_v2_static_dict(self):
        current_year = datetime.now().year
        return {
            "agent_id": self.id,
            "name": self.name,
            "birth_year": current_year - self.age,  # Approx
            "marital_status": self.marital_status,
            "children_ages": json.dumps(self.children_ages),
            "occupation": self.story.occupation,
            "background_story": self.story.background_story,
            "investment_style": self.story.investment_style,
            "purchase_motive_primary": self.story.purchase_motive_primary,
            "housing_stage": self.story.housing_stage,
            "family_stage": self.story.family_stage,
            "education_path": self.story.education_path,
            "financial_profile": self.story.financial_profile,
            "seller_profile": self.story.seller_profile,
            "agent_type": getattr(self, "agent_type", "normal"),
            "info_delay_months": int(getattr(self, "info_delay_months", 0) or 0),
        }

    def to_v2_finance_dict(self):
        # Calculate total debt (sum of all properties?)
        # total_debt = 0  <-- Removed logic, utilize self.total_debt
        # ✅ Phase 3.2: Simplified - Net Cashflow = Income - Mortgage Payment
        net_cf = self.monthly_income - self.mortgage_monthly_payment
        current_assets = float(self.net_worth)
        self.total_assets = current_assets

        return {
            "agent_id": self.id,
            "monthly_income": round(self.monthly_income, 2),
            "cash": round(self.cash, 2),
            "total_assets": round(current_assets, 2),
            "total_debt": round(self.total_debt, 2),
            "mortgage_monthly_payment": round(self.mortgage_monthly_payment, 2),
            "net_cashflow": round(net_cf, 2),
            "max_affordable_price": round(getattr(self.preference, 'max_affordable_price', 0), 2),
            "psychological_price": round(getattr(self.preference, 'psychological_price', 0), 2),
            "payment_tolerance_ratio": round(float(getattr(self, "payment_tolerance_ratio", 0.45) or 0.45), 4),
            "down_payment_tolerance_ratio": round(float(getattr(self, "down_payment_tolerance_ratio", 0.30) or 0.30), 4),
            "last_price_update_month": 0,  # Default
            "last_price_update_reason": ""
        }

    def to_v2_active_dict(self, role, market=None):
        return {
            "agent_id": self.id,
            "role": role,
            "target_zone": self.preference.target_zone if role == "BUYER" else None,
            "max_price": self.preference.max_price if role == "BUYER" else None,
            "selling_property_id": None,  # Should be set by caller logic
            "min_price": None,  # Set by logic
            "listed_price": None,  # Set by logic
            "life_pressure": getattr(self, 'life_pressure', 'patient'),
            "activation_trigger": getattr(self, 'activation_trigger', ''),
            "school_urgency": int(getattr(self, 'school_urgency', 0) or 0),
            "llm_intent_summary": str(self.monthly_event) if self.monthly_event else ""
        }


class PropertyStatic:
    def __init__(self, property_id: int, zone: str, quality: int, building_area: float,
                 property_type: str, is_school_district: bool, school_tier: int,
                 base_value: float = 0.0, unit_price: float = 0.0, created_at: int = 0):
        self.property_id = property_id
        self.zone = zone
        self.quality = quality
        self.building_area = building_area
        self.property_type = property_type
        self.is_school_district = is_school_district
        self.school_tier = school_tier
        self.base_value = base_value  # Also acts as initial_value
        self.unit_price = unit_price
        self.created_at = created_at

    def to_dict(self):
        return {
            "property_id": self.property_id,
            "zone": self.zone,
            "quality": self.quality,
            "base_value": self.base_value,
            "building_area": self.building_area,
            "unit_price": self.unit_price,
            "property_type": self.property_type,
            "is_school_district": self.is_school_district,
            "school_tier": self.school_tier,
            "created_at": self.created_at
        }


class PropertyMarket:
    def __init__(self, property_id: int, owner_id: int = None, status: str = 'off_market',
                 listed_price: float = None, min_price: float = None,
                 current_valuation: float = None,
                 listing_month: int = None, last_transaction_month: int = None):
        self.property_id = property_id
        self.owner_id = owner_id
        self.status = status  # 'off_market', 'for_sale'
        self.listed_price = listed_price
        self.min_price = min_price
        self.current_valuation = current_valuation
        self.listing_month = listing_month
        self.last_transaction_month = last_transaction_month

    def to_dict(self):
        return {
            "property_id": self.property_id,
            "owner_id": self.owner_id,
            "status": self.status,
            "listed_price": self.listed_price,
            "min_price": self.min_price,
            "current_valuation": self.current_valuation,
            "listing_month": self.listing_month,
            "last_transaction_month": self.last_transaction_month
        }


class Market:
    def __init__(self, properties: List[Dict] = None):
        self.properties = properties or []
        self.price_history: Dict[str, Dict[int, float]] = {'A': {}, 'B': {}}  # zone -> {month: avg_price}

    def get_price_change_rate(self, zone: str, month: int) -> float:
        """Calculate price change rate for a zone in a given month compared to previous month"""
        # For simulation start or missing data, return 0
        if month <= 1:
            return 0.0

        current_price = self.get_avg_price(zone, month)
        last_month_price = self.get_avg_price(zone, month - 1)

        if last_month_price == 0:
            return 0.0

        return (current_price - last_month_price) / last_month_price

    def get_avg_price(self, zone: str, month: int = None) -> float:
        """Get average price for a zone. If month is provided, use historical data."""
        if month is not None:
            # Look for recorded history first
            if month in self.price_history[zone]:
                return self.price_history[zone][month]

        # Fallback to current properties list
        zone_props = [p for p in self.properties if p['zone'] == zone]
        if not zone_props:
            return 0.0

        # Calculate avg based on listed_price or base_value
        total_price = sum(p.get('listed_price', p.get('base_value', 0)) for p in zone_props)
        return total_price / len(zone_props)

    def set_price_change(self, zone: str, month: int, change_rate: float):
        """Mock method for testing price changes: sets price for current month based on prev * (1+rate)"""
        prev_price = self.get_avg_price(zone, month - 1)
        if prev_price == 0:
            prev_price = 5000000 if zone == 'A' else 2500000  # Default

        new_price = prev_price * (1 + change_rate)
        self.price_history[zone][month] = new_price

    def add_property(self, property: Dict):
        self.properties.append(property)


class DecisionLog:
    def __init__(self, agent_id: int, month: int, event_type: str, decision: str, reason: str, thought_process: str, context_metrics: Dict = None, llm_called: bool = False):
        self.agent_id = agent_id
        self.month = month
        self.event_type = event_type
        self.decision = decision
        self.reason = reason
        self.thought_process = thought_process
        self.context_metrics = context_metrics
        self.llm_called = llm_called

    def to_dict(self):
        return {
            "agent_id": self.agent_id,
            "month": self.month,
            "event_type": self.event_type,
            "decision": self.decision,
            "reason": self.reason,
            "thought_process": self.thought_process,
            "context_metrics": json.dumps(self.context_metrics) if self.context_metrics else None,
            "llm_called": self.llm_called
        }
