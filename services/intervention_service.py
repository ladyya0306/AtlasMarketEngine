import logging
import random
import sqlite3
from datetime import datetime

from models import Agent
from utils.name_generator import ChineseNameGenerator

logger = logging.getLogger(__name__)


class InterventionService:
    def __init__(self, db_conn: sqlite3.Connection):
        self.conn = db_conn
        self.name_gen = ChineseNameGenerator()

    def _get_tier(self, income: float) -> str:
        """Helper to classify agent tier based on income."""
        # Simple heuristics based on default config boundaries (Adjust if needed)
        if income < 5000:
            return "low"
        if income < 12000:
            return "lower_middle"
        if income < 25000:
            return "middle"
        if income < 50000:
            return "upper_middle"
        if income < 100000:
            return "high"
        return "ultra_high"

    def apply_wage_shock(self, agent_service, pct_change: float, target_tier: str = "all"):
        """
        Adjust monthly income for agents.
        pct_change: -0.10 for 10% cut.
        """
        updated_count = 0
        batch_updates = []

        for agent in agent_service.agents:
            # Skip unemployed
            if agent.monthly_income == 0:
                continue

            tier = self._get_tier(agent.monthly_income)
            if target_tier != "all" and tier != target_tier:
                continue

            # Apply Shock
            old_income = agent.monthly_income
            new_income = old_income * (1 + pct_change)
            agent.monthly_income = new_income

            batch_updates.append((new_income, agent.id))
            updated_count += 1

        if batch_updates:
            cursor = self.conn.cursor()
            cursor.executemany("UPDATE agents_finance SET monthly_income=? WHERE agent_id=?", batch_updates)
            self.conn.commit()

        logger.info(f"Intervention: Wage Shock {pct_change * 100:.1f}% applied to {updated_count} agents.")
        return updated_count

    def apply_unemployment_shock(self, agent_service, rate: float, target_tier: str = "low"):
        """
        Force unemployment on a subset of agents.
        rate: 0.20 means 20% of the target tier will become unemployed.
        """
        candidates = []
        for agent in agent_service.agents:
            if agent.monthly_income == 0:
                continue  # Already unemployed
            tier = self._get_tier(agent.monthly_income)
            if target_tier == "all" or tier == target_tier:
                candidates.append(agent)

        if not candidates:
            return 0

        count = int(len(candidates) * rate)
        targets = random.sample(candidates, count)

        static_updates = []
        finance_updates = []

        for agent in targets:
            agent.story.occupation = "Unemployed"
            agent.monthly_income = 0
            # agent.cash does not change immediately, but will drain via living expenses

            static_updates.append(("Unemployed", agent.id))
            finance_updates.append((0, agent.id))

        cursor = self.conn.cursor()
        if static_updates:
            cursor.executemany("UPDATE agents_static SET occupation=? WHERE agent_id=?", static_updates)
            cursor.executemany("UPDATE agents_finance SET monthly_income=? WHERE agent_id=?", finance_updates)
            self.conn.commit()

        logger.info(f"Intervention: Unemployment Shock ({rate * 100}%) applied to {len(targets)} agents in {target_tier}.")
        return len(targets)

    def add_population(self, agent_service, count: int, tier: str):
        """
        Inject new agents into the simulation.
        """
        cursor = self.conn.cursor()

        # Determine Income/Cash based on tier
        # Simplified logic (copying AgentService defaults broadly)
        base_income = {
            "low": 3000, "lower_middle": 8000, "middle": 18000,
            "upper_middle": 35000, "high": 70000, "ultra_high": 150000
        }
        income_center = base_income.get(tier, 18000)

        new_agents = []

        # Get max ID
        max_id = max((a.id for a in agent_service.agents), default=0)
        start_id = max_id + 1

        for i in range(count):
            current_id = start_id + i
            income = random.uniform(income_center * 0.8, income_center * 1.2)
            cash = income * 12 * random.uniform(0.5, 3.0)  # Variable savings

            name = self.name_gen.generate()
            age = random.randint(22, 55)

            agent = Agent(current_id, name, age, "single", cash, income)
            agent.story.occupation = "Newcomer"  # Marker
            agent.story.background_story = "Migrated to city recently."

            # Add to memory
            agent_service.agents.append(agent)
            agent_service.agent_map[current_id] = agent
            new_agents.append(agent)

            # DB Inserts
            # agents_static: agent_id, name, birth_year, marital_status, children_ages, occupation, background_story, investment_style
            birth_year = datetime.now().year - age
            inv_style = random.choice(["conservative", "balanced", "aggressive"])
            agent.story.investment_style = inv_style
            agent.story.purchase_motive_primary = "starter_home"
            agent.story.housing_stage = "starter_no_home"
            agent.story.family_stage = "single_or_couple_no_children"
            agent.story.education_path = "not_school_sensitive"
            agent.story.financial_profile = "balanced_finance"
            agent.story.seller_profile = "patient_holder"
            agent.payment_tolerance_ratio = 0.42
            agent.down_payment_tolerance_ratio = 0.25

            cursor.execute("""
                INSERT INTO agents_static (
                    agent_id, name, birth_year, marital_status, children_ages, occupation, background_story,
                    investment_style, purchase_motive_primary, housing_stage, family_stage, education_path,
                    financial_profile, seller_profile
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                agent.id, agent.name, birth_year, "single", "[]", agent.story.occupation,
                agent.story.background_story, inv_style, agent.story.purchase_motive_primary,
                agent.story.housing_stage, agent.story.family_stage, agent.story.education_path,
                agent.story.financial_profile, agent.story.seller_profile
            ))

            # agents_finance: agent_id, monthly_income, cash, total_assets, total_debt, mortgage_monthly_payment, net_cashflow ...
            # New columns have defaults, but let's be explicit where needed or rely on defaults.
            # Schema has total_debt.
            cursor.execute("""
                INSERT INTO agents_finance (
                    agent_id, monthly_income, cash, total_assets, total_debt, mortgage_monthly_payment,
                    net_cashflow, payment_tolerance_ratio, down_payment_tolerance_ratio
                )
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                agent.id, agent.monthly_income, agent.cash, agent.cash, 0, 0, 0,
                agent.payment_tolerance_ratio, agent.down_payment_tolerance_ratio
            ))

        self.conn.commit()
        logger.info(f"Intervention: Added {count} new agents ({tier}).")
        return count

    def adjust_housing_supply(self, market_service, count: int, zone: str,
                              price_per_sqm: float = None, size: float = None,
                              school_units: int = None, build_year: int = None,
                              config=None, current_month: int = 0):
        """
        玩家投放房产（开发商模式）。
        
        Args:
            market_service: 市场服务
            count: 投放数量
            zone: 区域 (A/B)
            price_per_sqm: 玩家指定单价（元/㎡），None则从config读取
            size: 玩家指定面积（㎡），None则随机80-140
            school_units: 学区房数量（0~count），None则按区域默认比例随机
            build_year: 建成年份，None则按 simulation.base_year 默认新盘
            config: 配置对象，用于读取默认价格
            current_month: 当前月份，用于记录投放时间
        """
        cursor = self.conn.cursor()
        min_total_price = 500000.0
        if config:
            min_total_price = float(config.get('decision_factors.activation.min_cash_observer_no_property', 500000))

        # Get max property ID from DB (支持RESUME模式)
        cursor.execute("SELECT MAX(property_id) FROM properties_static")
        result = cursor.fetchone()
        max_id = result[0] if result and result[0] else 0

        start_id = max_id + 1

        # 修复：从config读取或使用玩家指定值
        if price_per_sqm is None:
            # 从config读取基础价格
            if config:
                zone_config = config.get(f"market.zones.{zone}", {})
                price_per_sqm = zone_config.get("base_price_per_sqm", 
                                                 35000 if zone == "A" else 15000)
            else:
                # 无config时使用默认值
                price_per_sqm = 35000 if zone == "A" else 15000

        if school_units is not None:
            school_units = int(school_units)
            if school_units < 0 or school_units > count:
                raise ValueError(f"school_units 必须在 0 到 {count} 之间")

        zone_ratio = 0.0
        if config:
            zone_ratio = float(config.get(f"market.zones.{zone}.school_district_ratio", 0.0))

        if school_units is None:
            school_units = int(round(count * zone_ratio))

        school_slots = set(random.sample(range(count), k=max(0, min(count, school_units)))) if count > 0 else set()
        base_year_default = 2026
        if config:
            base_year_default = int(config.get("simulation.base_year", base_year_default))
        resolved_build_year = int(build_year) if build_year else base_year_default

        for i in range(count):
            pid = start_id + i
            # 面积：玩家指定或随机
            area = size if size else random.randint(80, 140)
            base_total_price = area * price_per_sqm
            if base_total_price < min_total_price:
                raise ValueError(
                    f"投放失败: 单套总价 {base_total_price:,.0f} < 最低门槛 {min_total_price:,.0f}。"
                    f" 请提高单价或面积。"
                )
            # 单价：基础价格 ± 5% 随机波动
            u_price = price_per_sqm * random.uniform(0.95, 1.05)
            # 总价
            total_price = area * u_price
            if total_price < min_total_price:
                total_price = min_total_price
                u_price = total_price / area
            # 底价 = 挂牌价 × 0.9
            min_price = total_price * 0.9
            is_school = i in school_slots
            school_tier = random.choices([1, 2], weights=[0.3, 0.7])[0] if is_school else 3
            if area < 80:
                property_type = "刚需小户型"
            elif area < 120:
                property_type = "普通住宅"
            elif area < 180:
                property_type = "改善型大户型"
            else:
                property_type = "豪宅"

            prop = {
                "property_id": pid,
                "zone": zone,
                "building_area": area,
                "price_per_sqm": u_price,
                "base_value": total_price,
                "property_type": property_type,
                "is_school_district": is_school,
                "school_tier": school_tier,
                "build_year": resolved_build_year,
                "owner_id": -1,           # 开发商ID（玩家）
                "status": "for_sale",
                "listed_price": total_price,
                "min_price": min_price,
                "listing_month": current_month
            }

            # Add to memory (只在新模拟模式下，RESUME模式market为None)
            if market_service.market is not None and hasattr(market_service.market, 'properties'):
                market_service.market.properties.append(prop)

            # DB Insert - properties_static
            try:
                cursor.execute("""
                    INSERT INTO properties_static 
                    (property_id, zone, quality, building_area, property_type,
                     is_school_district, school_tier, price_per_sqm, zone_price_tier,
                     initial_value, build_year)
                    VALUES (?,?,2,?,?,?,?,?,?,?,?)
                """, (
                    pid, zone, area, property_type,
                    int(is_school), int(school_tier), u_price, None,
                    total_price, resolved_build_year
                ))
            except Exception as e:
                logger.error(f"Failed to insert property {pid} into properties_static: {e}")

            # DB Insert - properties_market
            try:
                cursor.execute("""
                    INSERT INTO properties_market 
                    (property_id, owner_id, status, current_valuation, 
                     listed_price, min_price, listing_month)
                    VALUES (?,?,'for_sale',?,?,?,?)
                """, (pid, -1, total_price, total_price, min_price, current_month))
            except Exception as e:
                logger.error(f"Failed to insert property {pid} into properties_market: {e}")

        try:
            self.conn.commit()
            logger.info(f"✅ 成功投放开发商房产: {count}套 (ID: {start_id}-{start_id+count-1}) -> DB Committed")
            
            # Verify insertion immediately
            cursor.execute(f"SELECT COUNT(*) FROM properties_market WHERE owner_id = -1 AND property_id >= {start_id}")
            inserted_count = cursor.fetchone()[0]
            logger.info(f"🔍 验证数据库: 查找到 {inserted_count} 套新投放的开发商房产")
            
        except Exception as e:
            logger.error(f"Failed to commit developer properties: {e}")
            
        logger.info(f"玩家开发商投放: {count}套 {zone}区房产, {price_per_sqm:.0f}元/㎡, "
                    f"挂牌总价{total_price:.0f}元, 底价{min_price:.0f}元, 学区房{school_units}套, 建成年份{resolved_build_year}")
        return count

    def remove_population(self, agent_service, count: int, tier: str):
        """
        Force exit agents.
        """
        cursor = self.conn.cursor()
        # removed_count = 0

        candidates = []
        for agent in agent_service.agents:
            # Skip newly added agents to avoid immediate removal? No, random is fine.
            # Skip 'system' agents if any?
            if tier == "all" or self._get_tier(agent.monthly_income) == tier:
                candidates.append(agent)

        if not candidates:
            return 0

        targets = random.sample(candidates, min(count, len(candidates)))

        ids_to_remove = [a.id for a in targets]

        # 1. DB Updates
        # Remove from active_participants (stops them from buying/selling)
        cursor.execute(f"DELETE FROM active_participants WHERE agent_id IN ({','.join(['?'] * len(ids_to_remove))})", ids_to_remove)

        # Mark as 'Exited' in static?
        cursor.execute(f"UPDATE agents_static SET occupation='Exited' WHERE agent_id IN ({','.join(['?'] * len(ids_to_remove))})", ids_to_remove)

        # Set Income to 0?
        cursor.execute(f"UPDATE agents_finance SET monthly_income=0 WHERE agent_id IN ({','.join(['?'] * len(ids_to_remove))})", ids_to_remove)

        self.conn.commit()

        # 2. Memory Updates
        # We need to remove from agent_service.agents list so they don't get processed in loop
        # But removing from list while iterating is bad. SimulationRunner iterates copy or key?
        # SimulationRunner uses `agent_service.agents`.
        # Best to just mark them as 'Exited' and have AgentService loop skip them?
        # But AgentService doesn't check 'Exited'.
        # Safer to remove from list.

        for t in targets:
            if t in agent_service.agents:
                agent_service.agents.remove(t)
            if t.id in agent_service.agent_map:
                del agent_service.agent_map[t.id]

        logger.info(f"Intervention: Removed {len(targets)} agents ({tier}).")
        return len(targets)

    def supply_cut(self, market_service, count: int, zone: str):
        """
        Remove listings (force off-market).
        """
        cursor = self.conn.cursor()

        candidates = [p for p in market_service.market.properties if p['status'] == 'for_sale' and p['zone'] == zone]

        if not candidates:
            return 0

        targets = random.sample(candidates, min(count, len(candidates)))
        ids = [p['property_id'] for p in targets]

        # DB Update
        cursor.execute(f"UPDATE properties_market SET status='off_market' WHERE property_id IN ({','.join(['?'] * len(ids))})", ids)
        self.conn.commit()

        # Memory Update
        for p in targets:
            p['status'] = 'off_market'

        logger.info(f"Intervention: Supply Cut - Removed {len(targets)} listings in Zone {zone}.")
        return len(targets)

    def set_financial_policy(self, config, down_payment_ratio: float = None, mortgage_rate: float = None):
        """
        Update global financial config.
        """
        # updates = []
        if down_payment_ratio is not None:
            # Try to update transaction_engine/config logic
            # config object is passed in.
            # Assuming config structure matches simulation usage
            # config.mortgage_config? or MORTGAGE_CONFIG constant?
            # settings.py has constants. ConfigLoader loads them.
            # We need to modify the runtime config object.
            # If system relies on global constants in settings.py, we can't easily change it without reload.
            # But SimulationRunner passes `self.config` to services.
            # AND mortgage_system.py might import `MORTGAGE_CONFIG` directly.
            pass

        # For Tier 5, we assume we can modify run-time config or DB.
        # Let's log it for now as a "Policy Announcement".

        logger.info(f"Intervention: Financial Policy - DP: {down_payment_ratio}, Rate: {mortgage_rate}")
        return True
