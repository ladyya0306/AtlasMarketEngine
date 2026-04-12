# Force CI Refresh
import random
from typing import Dict, List, Optional, Tuple

from config.settings import INITIAL_MARKET_CONFIG, PROPERTY_DISTRIBUTION


def classify_property_type(area: float, unit_price: float, zone: str) -> str:
    """
    Classify property type based on area and zone.
    """
    if zone == "A":
        if area < 70:
            return "刚需小户型"
        elif area < 120:
            return "普通住宅"
        elif area < 180:
            return "改善型大户型"
        else:
            return "豪宅"
    else:
        if area < 80:
            return "刚需小户型"
        elif area < 120:
            return "普通住宅"
        elif area < 180:
            return "改善型大户型"
        else:
            return "豪宅"


def assign_school_district(zone: str, config=None, price_band: Optional[str] = None) -> Tuple[bool, int]:
    """
    Assign school district status and tier based on zone ratio.
    Returns: (is_school_district, school_tier)
    """
    ratio = 0.0
    if config:
        zone_cfg = config.market.get('zones', {}).get(zone, {})
        ratio_by_band = zone_cfg.get('school_district_ratio_by_band', {}) or {}
        if price_band:
            ratio = float(ratio_by_band.get(str(price_band).lower(), 0.0) or 0.0)
        if ratio <= 0.0:
            ratio = float(zone_cfg.get('school_district_ratio', 0.0) or 0.0)
    else:
        ratio = float(INITIAL_MARKET_CONFIG[zone]["school_district_ratio"])

    is_district = random.random() < ratio

    if is_district:
        # 30% Tier 1 (Key School), 70% Tier 2 (Normal School)
        tier = random.choices([1, 2], weights=[0.3, 0.7])[0]
        return True, tier
    else:
        return False, 3  # Tier 3 means no school district


def assign_build_year(zone: str, quality: int, config=None) -> int:
    """
    Assign build year by zone/quality profile.
    Older stock is more likely in low quality buckets ("老破小"),
    while quality_3 tends to be newer.
    """
    current_year = 2026
    profile = {}
    if config:
        current_year = int(config.get("simulation.base_year", current_year))
        profile = config.market.get("property_age_profile", {})

    default_profile = {
        "A": {
            "quality_1": {"min_age": 22, "max_age": 38},
            "quality_2": {"min_age": 10, "max_age": 24},
            "quality_3": {"min_age": 1, "max_age": 12},
        },
        "B": {
            "quality_1": {"min_age": 18, "max_age": 32},
            "quality_2": {"min_age": 8, "max_age": 20},
            "quality_3": {"min_age": 1, "max_age": 10},
        },
    }

    z = str(zone or "A").upper()
    qk = f"quality_{int(quality)}"
    age_cfg = (
        profile.get(z, {}).get(qk)
        or default_profile.get(z, {}).get(qk)
        or {"min_age": 5, "max_age": 20}
    )
    min_age = max(0, int(age_cfg.get("min_age", 5)))
    max_age = max(min_age, int(age_cfg.get("max_age", 20)))
    age = random.randint(min_age, max_age)
    return current_year - age


def create_property(
    prop_id: int,
    zone: str,
    quality: int,
    config=None,
    price_band: Optional[str] = None,
) -> Dict:
    """Create a single property record with extended fields"""

    # 1. Randomize Area and Bedrooms
    if quality == 1:   # Small/Low quality
        area = random.uniform(50, 80)
        # bedrooms = random.choice([1, 2])
    elif quality == 2:  # Medium
        area = random.uniform(80, 130)
        # bedrooms = random.choice([2, 3])
    else:              # High quality
        area = random.uniform(130, 250)
        # bedrooms = random.choice([3, 4, 5])

    # 🆕 2. Calculate Unit Price (price_per_sqm) - PRIORITY LOGIC
    # Use new price_per_sqm_range from config if available
    if config and hasattr(config, 'get_zone_price_range'):
        zone_cfg = config.market.get('zones', {}).get(zone, {})
        price_tiers = zone_cfg.get('price_tiers', {}) or {}
        band_to_tier = {"LOW": "budget", "MID": "standard", "HIGH": "premium"}
        tier_key = band_to_tier.get(str(price_band or "").upper(), "")
        tier_range = price_tiers.get(tier_key, None)
        if isinstance(tier_range, (list, tuple)) and len(tier_range) >= 2:
            pmin = float(min(tier_range[0], tier_range[1]))
            pmax = float(max(tier_range[0], tier_range[1]))
            base_unit_price = random.uniform(pmin, pmax)
        else:
            price_range = config.get_zone_price_range(zone)
            base_unit_price = random.uniform(price_range['min'], price_range['max'])
    else:
        # Fallback to old logic
        base_price = 0
        if config:
            base_price = config.market.get('zones', {}).get(zone, {}).get('base_price_per_sqm', 50000)
        else:
            base_price = INITIAL_MARKET_CONFIG[zone]["base_price_per_sqm"]

        # Fluctuate based on quality factor (0.9, 1.0, 1.2)
        quality_factor = {1: 0.9, 2: 1.0, 3: 1.2}[quality]
        base_unit_price = base_price * quality_factor
        # Add random variation (+- 10%)
        base_unit_price = base_unit_price * random.uniform(0.9, 1.1)

    unit_price = base_unit_price  # Store original unit price

    # 3. Calculate Base Value (unit_price × area)
    base_value = area * unit_price

    # 4. Classify Type
    prop_type = classify_property_type(area, unit_price, zone)

    # 5. Assign School District
    is_district, school_tier = assign_school_district(zone, config, price_band=price_band)
    build_year = assign_build_year(zone, quality, config)
    if is_district:
        # School district adds premium (15%-30%)
        premium = random.uniform(1.15, 1.30)
        unit_price *= premium  # Update unit price with premium
        base_value *= premium

    # 6. Listed Price (Base value + 10% premium initially)
    listed_price = base_value * random.uniform(1.05, 1.15)

    # 🆕 7. Calculate Rental Price and Yield
    # Default rent per sqm
    rent_per_sqm_a = 100
    rent_per_sqm_b = 60

    if config:
        rent_per_sqm_a = config.market.get('rental', {}).get('zone_a_rent_per_sqm', 100)
        rent_per_sqm_b = config.market.get('rental', {}).get('zone_b_rent_per_sqm', 60)

    rent_unit_price = rent_per_sqm_a if zone == 'A' else rent_per_sqm_b
    rental_price = area * rent_unit_price

    # Random fluctuation for rent (+- 5%)
    rental_price *= random.uniform(0.95, 1.05)

    # Calculate Yield (Annual Rent / Listed Price)
    rental_yield = (rental_price * 12) / listed_price if listed_price > 0 else 0

    return {
        "property_id": prop_id,
        "zone": zone,
        "quality": quality,
        "base_value": base_value,
        "building_area": round(area, 2),
        "price_per_sqm": round(unit_price, 0),  # 🆕 Store unit price
        "zone_price_tier": str(price_band).upper() if price_band else None,
        "unit_price": round(unit_price, 0),  # Keep for backwards compatibility
        "listed_price": round(listed_price, 0),
        "rental_price": round(rental_price, 0),
        "rental_yield": round(rental_yield, 4),
        "build_year": build_year,
        "property_type": prop_type,
        "is_school_district": is_district,
        "school_tier": school_tier,
        "owner_id": None,  # System owned initially
        "status": "off_market",  # Fixed: was "for_sale", but unowned properties shouldn't be listed
        "min_price": round(base_value * 0.95, 0),  # Added for V2
        "current_valuation": base_value,  # Added for V2
        "listing_month": 0,  # Added for V2
        "last_transaction_month": None,
        "created_at": 0  # Added for V2
    }


def convert_to_v2_tuples(prop_dict: Dict) -> Tuple[Dict, Dict]:
    """Helper to split a property dict into Static and Market dicts for V2 DB insertion"""
    static_data = {
        "property_id": prop_dict["property_id"],
        "zone": prop_dict["zone"],
        "quality": prop_dict["quality"],
        "building_area": prop_dict["building_area"],
        "property_type": prop_dict["property_type"],
        "is_school_district": prop_dict["is_school_district"],
        "school_tier": prop_dict["school_tier"],
        "price_per_sqm": prop_dict.get("price_per_sqm", 0),  # 🆕
        "zone_price_tier": prop_dict.get("zone_price_tier", None),  # 🆕
        "initial_value": prop_dict["base_value"],  # Map base_value to initial_value
        "build_year": prop_dict.get("build_year"),
        "created_at": prop_dict.get("created_at", 0)
    }

    market_data = {
        "property_id": prop_dict["property_id"],
        "owner_id": prop_dict.get("owner_id"),
        "status": prop_dict.get("status", "off_market"),
        "current_valuation": prop_dict.get("current_valuation", prop_dict["base_value"]),
        "listed_price": prop_dict.get("listed_price"),
        "min_price": prop_dict.get("min_price"),
        "rental_price": prop_dict.get("rental_price", 0),  # Added rental_price
        "rental_yield": prop_dict.get("rental_yield", 0),  # Added rental_yield
        "listing_month": prop_dict.get("listing_month"),
        "last_transaction_month": prop_dict.get("last_transaction_month")
    }
    return static_data, market_data


def initialize_market_properties(target_total_count: int = None, config=None) -> List[Dict]:
    """
    Initialize market properties list
    Args:
        target_total_count: If provided, scales the default distribution to match this total
        config: SimulationConfig object
    """
    properties = []
    property_id = 1

    # Use config distribution or fallback
    distribution_map = {}
    if config:
        for zone, z_cfg in config.market.get('zones', {}).items():
            distribution_map[zone] = z_cfg.get('property_count', {})
    else:
        distribution_map = PROPERTY_DISTRIBUTION

    # Calculate scaling factor if target count provided
    scale_factor = 1.0
    if target_total_count:
        # Calculate current total in distribution config
        current_total = 0
        for zone_dist in distribution_map.values():
            current_total += sum(zone_dist.values())

        if current_total > 0:
            scale_factor = target_total_count / current_total

    def _resolve_supply_band_ratio(zone_key: str) -> Dict[str, float]:
        default_map = {
            "A": {"low": 0.25, "mid": 0.50, "high": 0.25},
            "B": {"low": 0.45, "mid": 0.40, "high": 0.15},
        }
        if not config:
            return dict(default_map.get(zone_key, {"low": 0.33, "mid": 0.34, "high": 0.33}))
        zone_cfg = config.market.get('zones', {}).get(zone_key, {}) or {}
        ratio_cfg = zone_cfg.get('supply_band_ratio', {}) or {}
        low = max(0.0, float(ratio_cfg.get("low", default_map.get(zone_key, {}).get("low", 0.33)) or 0.0))
        mid = max(0.0, float(ratio_cfg.get("mid", default_map.get(zone_key, {}).get("mid", 0.34)) or 0.0))
        high = max(0.0, float(ratio_cfg.get("high", default_map.get(zone_key, {}).get("high", 0.33)) or 0.0))
        total = low + mid + high
        if total <= 0:
            return dict(default_map.get(zone_key, {"low": 0.33, "mid": 0.34, "high": 0.33}))
        return {"low": low / total, "mid": mid / total, "high": high / total}

    def _build_band_pool(zone_key: str, total_count: int) -> List[str]:
        ratios = _resolve_supply_band_ratio(zone_key)
        low_count = int(round(total_count * ratios["low"]))
        mid_count = int(round(total_count * ratios["mid"]))
        high_count = max(0, total_count - low_count - mid_count)
        pool = ["LOW"] * max(0, low_count) + ["MID"] * max(0, mid_count) + ["HIGH"] * max(0, high_count)
        if len(pool) < total_count:
            pool.extend(["MID"] * (total_count - len(pool)))
        if len(pool) > total_count:
            pool = pool[:total_count]
        random.shuffle(pool)
        return pool

    for zone, distribution in distribution_map.items():
        quality_pool: List[int] = []
        for quality_level in [1, 2, 3]:
            base_count = distribution.get(f"quality_{quality_level}", 0)
            count = int(base_count * scale_factor)
            if base_count > 0 and count == 0:
                count = 1
            quality_pool.extend([quality_level] * max(0, count))

        if not quality_pool:
            continue

        random.shuffle(quality_pool)
        band_pool = _build_band_pool(zone, len(quality_pool))
        for idx, quality_level in enumerate(quality_pool):
            band = band_pool[idx] if idx < len(band_pool) else random.choice(["LOW", "MID", "HIGH"])
            prop = create_property(property_id, zone, quality_level, config, price_band=band)
            properties.append(prop)
            property_id += 1

    # If we are slightly off due to rounding, add/remove random properties to match exactly
    if target_total_count and len(properties) != target_total_count:
        diff = target_total_count - len(properties)
        if diff > 0:
            # Add more properties (clone random logic)
            for _ in range(diff):
                # Pick random zone/quality based on weights? Simplified: Random choice
                zone = random.choice(list(distribution_map.keys()))
                quality = random.choice([1, 2, 3])
                band = _build_band_pool(zone, 1)[0]
                prop = create_property(property_id, zone, quality, config, price_band=band)
                properties.append(prop)
                property_id += 1
        elif diff < 0:
            # Trim properties (from the end or random? End is fine as order is mixed by zone loop)
            properties = properties[:target_total_count]

    return properties
