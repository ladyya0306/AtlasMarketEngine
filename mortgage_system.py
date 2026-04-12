"""
Mortgage System: Loan Calculation and Affordability Check
"""
from typing import Tuple

from config.settings import MORTGAGE_CONFIG


def calculate_monthly_payment(loan_amount: float, annual_rate: float, years: int) -> float:
    """
    Calculate monthly mortgage payment using standard formula.
    M = P [ i(1 + i)^n ] / [ (1 + i)^n – 1 ]
    """
    if loan_amount <= 0:
        return 0.0

    monthly_rate = annual_rate / 12
    num_payments = years * 12

    if monthly_rate == 0:
        return loan_amount / num_payments

    payment = loan_amount * (monthly_rate * (1 + monthly_rate) ** num_payments) / ((1 + monthly_rate) ** num_payments - 1)
    return payment


def check_affordability(
    agent,
    price: float,
    config=None,
    extra_cash_needed: float = 0.0,
    liquidity_floor_months_override: int = None,
) -> Tuple[bool, float, float]:
    """
    Check if agent can afford the property with mortgage.
    Returns: (is_affordable, down_payment, loan_amount)
    """
    # 0. Config Setup
    mortgage_cfg = config.mortgage if config else MORTGAGE_CONFIG
    down_ratio = mortgage_cfg.get('down_payment_ratio', 0.3)
    annual_rate = mortgage_cfg.get('annual_interest_rate', 0.05)
    loan_term = mortgage_cfg.get('loan_term_years', 30)
    max_dti = mortgage_cfg.get('max_dti_ratio', 0.5)

    # 1. Down Payment Check
    min_down_payment = price * down_ratio
    required_cash_now = min_down_payment + max(0.0, float(extra_cash_needed or 0.0))
    if agent.cash < required_cash_now:
        return False, 0.0, 0.0

    # 2. Loan Amount Needed
    down_payment = min_down_payment
    loan_amount = price - down_payment

    # 3. DTI Check (Debt-to-Income)
    # Calculate new monthly payment
    new_monthly_payment = calculate_monthly_payment(
        loan_amount,
        annual_rate,
        loan_term
    )

    total_monthly_payment = agent.mortgage_monthly_payment + new_monthly_payment
    max_payment = agent.monthly_income * max_dti

    if total_monthly_payment > max_payment:
        # Cannot afford monthly payment
        return False, 0.0, 0.0

    # 4. Liquidity Floor Check (M08)
    # Keep a minimum cash buffer after down payment to avoid "buying while cash-starved".
    if liquidity_floor_months_override is not None:
        try:
            liquidity_floor_months = max(0, int(liquidity_floor_months_override))
        except Exception:
            liquidity_floor_months = 0
    else:
        liquidity_floor_months = 0
        try:
            if config:
                liquidity_floor_months = int(
                    config.get(
                        "smart_agent.liquidity_floor_months",
                        config.get("liquidity_floor_months", 0),
                    )
                )
        except Exception:
            liquidity_floor_months = 0

    if liquidity_floor_months > 0:
        expense_ratio = 0.5  # Simple default proxy: monthly expense ~= 50% income
        monthly_expense = max(1.0, float(agent.monthly_income) * expense_ratio)
        required_cash_floor = liquidity_floor_months * monthly_expense
        remaining_cash = float(agent.cash) - float(required_cash_now)
        if remaining_cash < required_cash_floor:
            return False, 0.0, 0.0

    # 5. Leverage Cap Check (M08)
    # Guardrail on debt/assets ratio after transaction.
    leverage_cap = None
    try:
        if config:
            raw_cap = config.get("smart_agent.leverage_cap", config.get("leverage_cap", None))
            if raw_cap is not None and str(raw_cap).strip() != "":
                leverage_cap = float(raw_cap)
    except Exception:
        leverage_cap = None

    if leverage_cap is not None and leverage_cap > 0:
        new_total_debt = float(getattr(agent, "total_debt", 0.0) or 0.0) + float(loan_amount)
        # Use runtime total_assets first; fall back to computed net worth to avoid zero-asset false negatives.
        current_assets = float(getattr(agent, "total_assets", 0.0) or 0.0)
        if current_assets <= 0:
            if hasattr(agent, "net_worth"):
                try:
                    current_assets = float(agent.net_worth)
                except Exception:
                    current_assets = 0.0
            if current_assets <= 0:
                current_assets = float(getattr(agent, "cash", 0.0) or 0.0)
        # Approximate post-transaction asset base: existing assets + acquired property value.
        asset_base = max(0.0, current_assets) + float(price)
        if asset_base <= 0:
            return False, 0.0, 0.0
        if (new_total_debt / asset_base) > leverage_cap:
            return False, 0.0, 0.0

    return True, down_payment, loan_amount


def get_max_loan(agent) -> float:
    """
    Calculate max loan amount agent can get based on income.
    """
    max_payment = agent.monthly_income * MORTGAGE_CONFIG["max_dti_ratio"]
    available_payment = max(0, max_payment - agent.mortgage_monthly_payment)

    # Inverse of monthly payment formula to get Principal
    # P = M * [ (1 + i)^n - 1 ] / [ i(1 + i)^n ]

    annual_rate = MORTGAGE_CONFIG["annual_interest_rate"]
    years = MORTGAGE_CONFIG["loan_term_years"]
    monthly_rate = annual_rate / 12
    num_payments = years * 12

    if monthly_rate == 0:
        return available_payment * num_payments

    max_loan = available_payment * ((1 + monthly_rate) ** num_payments - 1) / (monthly_rate * (1 + monthly_rate) ** num_payments)
    return max_loan


def calculate_max_affordable(cash: float, monthly_income: float, existing_payment: float = 0, config=None) -> float:
    """
    计算真实购买力 = min(首付能撬动的总价, 现金+贷款能力)
    """
    mortgage_cfg = config.mortgage if config else MORTGAGE_CONFIG
    down_ratio = mortgage_cfg.get('down_payment_ratio', 0.3)
    max_dti = mortgage_cfg.get('max_dti_ratio', 0.5)
    annual_rate = mortgage_cfg.get('annual_interest_rate', 0.05)
    years = mortgage_cfg.get('loan_term_years', 30)

    monthly_rate = annual_rate / 12
    num_payments = years * 12

    # 方法1: 首付能撬动的总价
    max_by_down = cash / down_ratio

    # 方法2: 贷款能力
    available_payment = max(0, monthly_income * max_dti - existing_payment)

    if monthly_rate > 0:
        loan_capacity = available_payment * ((1 + monthly_rate) ** num_payments - 1) / (monthly_rate * (1 + monthly_rate) ** num_payments)
    else:
        loan_capacity = available_payment * num_payments

    max_by_loan = cash + loan_capacity

    return min(max_by_down, max_by_loan)


def calculate_max_loan_from_payment(monthly_payment: float, annual_rate: float, years: int) -> float:
    """
    根据月供反推最大贷款额

    公式推导：
    月供 P = L * [r*(1+r)^n] / [(1+r)^n - 1]
    => L = P * [(1+r)^n - 1] / [r*(1+r)^n]

    Args:
        monthly_payment: 可用月供金额
        annual_rate: 年利率（例如 0.05 表示 5%）
        years: 贷款年限

    Returns:
        float: 最大贷款额
    """
    monthly_rate = annual_rate / 12
    n = years * 12

    if monthly_rate == 0:
        return monthly_payment * n

    factor = (1 + monthly_rate) ** n
    return monthly_payment * (factor - 1) / (monthly_rate * factor)


def calculate_max_affordable_price(agent, config=None) -> float:
    """
    计算agent能负担的最高房价（基于现金和DTI限制）
    这是 calculate_max_affordable 的Agent对象包装版本，方便在交易引擎中调用

    Args:
        agent: Agent对象，需要有 cash, monthly_income, monthly_payment 属性
        config: 配置对象（可选），包含 mortgage 配置

    Returns:
        float: 最大可负担房价
    """
    return calculate_max_affordable(
        cash=agent.cash,
        monthly_income=agent.monthly_income,
        existing_payment=agent.mortgage_monthly_payment,
        config=config
    )
