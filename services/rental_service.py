
import logging
import sqlite3

logger = logging.getLogger(__name__)


class RentalService:
    def __init__(self, config, db_conn: sqlite3.Connection):
        self.config = config
        self.conn = db_conn

    def process_rental_market(self, month: int):
        """
        Main entry point for monthly rental activities (Abstract Model).
        1. Update Rental Prices (Yield ~2%).
        2. Calculate Rent Income for Landlords.
        3. Deduct Rent Expense for Tenants.
        """
        logger.info(f"--- Processing Rental Market for Month {month} (Abstract Model) ---")

        self._update_rental_valuations()
        self._process_abstract_rent_flows(month)

    def _update_rental_valuations(self):
        """Update rental_price based on current_valuation * yield (approx 2% / 12)"""
        cursor = self.conn.cursor()
        # Annual Yield 2% -> Monthly = 0.02 / 12 = 0.001667
        cursor.execute("""
            UPDATE properties_market
            SET rental_price = current_valuation * 0.001667
            WHERE current_valuation IS NOT NULL
        """)
        self.conn.commit()

    def _process_abstract_rent_flows(self, month: int):
        """
        Abstract Cash Flow:
        - Landlords: Earn rent from non-primary properties.
        - Tenants: Pay average rent of their TARGET zone.
        """
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA table_info(active_participants)")
        active_cols = {r[1] for r in cursor.fetchall()}
        has_month = "month" in active_cols

        # 1. Landlord Income
        # Identify owners with > 1 property
        # Fetch all properties grouped by owner
        cursor.execute("""
            SELECT owner_id, rental_price, property_id
            FROM properties_market
            WHERE owner_id IS NOT NULL AND rental_price IS NOT NULL
            ORDER BY owner_id, current_valuation DESC
        """)
        rows = cursor.fetchall()

        from collections import defaultdict
        owner_props = defaultdict(list)
        for r in rows:
            owner_props[r[0]].append(r[1])  # List of rental_prices, sorted by value DESC

        total_income = 0
        landlord_count = 0
        batch_income = []

        for owner_id, rents in owner_props.items():
            if len(rents) > 1:
                # Primary (first one) is free/lived-in. Rest are rented out.
                income = sum(rents[1:])
                if income > 0:
                    batch_income.append((income, income, owner_id))
                    total_income += income
                    landlord_count += 1

        if batch_income:
            cursor.executemany("UPDATE agents_finance SET cash = cash + ?, net_cashflow = net_cashflow + ? WHERE agent_id = ?", batch_income)

        # 2. Tenant Expense
        # Pre-calc Zone Averages
        cursor.execute("SELECT AVG(rental_price) FROM properties_market JOIN properties_static ON properties_market.property_id = properties_static.property_id WHERE zone = 'A'")
        res_a = cursor.fetchone()
        rent_a = res_a[0] if res_a and res_a[0] else 5000

        cursor.execute("SELECT AVG(rental_price) FROM properties_market JOIN properties_static ON properties_market.property_id = properties_static.property_id WHERE zone = 'B'")
        res_b = cursor.fetchone()
        rent_b = res_b[0] if res_b and res_b[0] else 2500

        rents = {"A": rent_a, "B": rent_b}

        # Identify Tenants: Active Non-Owners
        if has_month:
            cursor.execute("""
                SELECT ap.agent_id, ap.target_zone
                FROM active_participants ap
                JOIN (
                    SELECT agent_id, MAX(month) AS max_month
                    FROM active_participants
                    GROUP BY agent_id
                ) latest
                ON ap.agent_id = latest.agent_id
                AND (
                    ap.month = latest.max_month
                    OR (ap.month IS NULL AND latest.max_month IS NULL)
                )
                WHERE ap.agent_id NOT IN (SELECT DISTINCT owner_id FROM properties_market WHERE owner_id IS NOT NULL)
                AND UPPER(ap.role) != 'OBSERVER'
            """)
        else:
            cursor.execute("""
                SELECT ap.agent_id, ap.target_zone
                FROM active_participants ap
                WHERE ap.agent_id NOT IN (SELECT DISTINCT owner_id FROM properties_market WHERE owner_id IS NOT NULL)
                AND UPPER(ap.role) != 'OBSERVER'
            """)
        tenants = cursor.fetchall()

        total_expense = 0
        tenant_count = 0
        batch_expense = []
        # batch_status_update = []

        for agent_id, target_zone in tenants:
            # If target_zone is None, default to B
            zone = target_zone if target_zone in ['A', 'B'] else 'B'
            rent_to_pay = rents[zone]

            batch_expense.append((rent_to_pay, rent_to_pay, agent_id))
            # Just mark them as renting, don't change role
            # But we might want to track rental status
            # 'rental_status' column in active_participants?
            # Or just assume.
            total_expense += rent_to_pay
            tenant_count += 1

        if batch_expense:
            cursor.executemany("UPDATE agents_finance SET cash = cash - ?, net_cashflow = net_cashflow - ? WHERE agent_id = ?", batch_expense)

        # Update Owners status
        if owner_props:
            # owner_ids = list(owner_props.keys())
            # Batch update rental_status='owned' is tricky without column, but let's assume valid
            pass

        self.conn.commit()
        logger.info(f"Rental Flow: {landlord_count} Landlords earned {total_income:,.0f}; {tenant_count} Tenants paid {total_expense:,.0f} (A:{rent_a:.0f}, B:{rent_b:.0f})")
