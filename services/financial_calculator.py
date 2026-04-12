
"""
Financial Calculator Service
Provides investment metrics for Agent decision making.
"""
from typing import Dict


class FinancialCalculator:

    @staticmethod
    def calculate_rental_yield(property_price: float, monthly_rental_income: float) -> float:
        """
        Calculate Annual Rental Yield.
        Formula: (Monthly Rent * 12) / Property Price
        """
        if property_price <= 0:
            return 0.0
        return (monthly_rental_income * 12) / property_price

    @staticmethod
    def calculate_holding_cost(agent, property_data: Dict, mortgage_payment: float = 0) -> float:
        """
        Calculate Monthly Holding Cost.
        Formula: Mortgage + Maintenance - Rent (if rented out)
        Note: If property is vacant, Rent is 0.
        """
        base_val = property_data.get('base_value', property_data.get('current_valuation', 0))
        maintenance_cost = base_val * 0.0003  # Approx 0.3% monthly maintenance/tax
        monthly_rent = property_data.get('rental_income', property_data.get('rental_price', 0))

        # Current project schema mainly uses for_sale/off_market; treat listed properties as vacant.
        if property_data.get('status') == 'for_sale':
            monthly_rent = 0

        return mortgage_payment + maintenance_cost - monthly_rent

    @staticmethod
    def calculate_potential_roi(
        down_payment: float,
        monthly_cash_flow: float,
        appreciation_rate: float,
        property_value: float,
        years: int = 1
    ) -> float:
        """
        Calculate Return on Investment (ROI) Projection.
        Includes Cash Flow + Appreciation.
        """
        if down_payment <= 0:
            return 0.0

        annual_cash_flow = monthly_cash_flow * 12
        appreciation_gain = property_value * ((1 + appreciation_rate) ** years - 1)

        total_gain = (annual_cash_flow * years) + appreciation_gain
        return total_gain / down_payment

    @staticmethod
    def compare_with_risk_free(yield_rate: float, risk_free_rate: float) -> str:
        """
        Return a textual comparison signal.
        """
        diff = yield_rate - risk_free_rate
        if diff > 0.02:
            return "EXCELLENT (远超存款)"
        elif diff > 0:
            return "GOOD (略高于存款)"
        elif diff > -0.01:
            return "FAIR (不如存款，但可博增值)"
        else:
            return "POOR (严重亏损)"

    @staticmethod
    def calculate_transaction_costs(
        price: float,
        config=None,
        side: str = "buyer",
    ) -> Dict[str, float]:
        """
        Calculate one-off transaction costs for buyer/seller.
        Returned keys:
          - brokerage_fee
          - taxes
          - misc_fees
          - total
        """
        if price <= 0:
            return {
                "brokerage_fee": 0.0,
                "taxes": 0.0,
                "misc_fees": 0.0,
                "total": 0.0,
            }

        side_key = "buyer" if str(side).lower() != "seller" else "seller"

        def _cfg(path: str, default: float) -> float:
            try:
                if config:
                    return float(config.get(path, default))
            except Exception:
                pass
            return float(default)

        # Conservative defaults if not configured.
        if side_key == "buyer":
            brokerage_ratio = _cfg("transaction_costs.buyer.brokerage_ratio", 0.010)
            tax_ratio = _cfg("transaction_costs.buyer.tax_ratio", 0.007)
            misc_ratio = _cfg("transaction_costs.buyer.misc_ratio", 0.003)
        else:
            brokerage_ratio = _cfg("transaction_costs.seller.brokerage_ratio", 0.010)
            tax_ratio = _cfg("transaction_costs.seller.tax_ratio", 0.005)
            misc_ratio = _cfg("transaction_costs.seller.misc_ratio", 0.002)

        brokerage_fee = max(0.0, float(price) * brokerage_ratio)
        taxes = max(0.0, float(price) * tax_ratio)
        misc_fees = max(0.0, float(price) * misc_ratio)
        total = brokerage_fee + taxes + misc_fees

        return {
            "brokerage_fee": brokerage_fee,
            "taxes": taxes,
            "misc_fees": misc_fees,
            "total": total,
        }
