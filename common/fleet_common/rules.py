"""Business rules for vehicle profitability.

The batch layer applies these rules with Spark column expressions
(spark/jobs/fleet_transforms.py) and the tests check that both versions give
the same answer, so the two code paths cannot drift apart silently.

Profit model (fleet operator view):
    driver payout = fares x driver share
    net profit    = fares - driver payout - fuel cost - maintenance cost
    margin %      = net profit / fares x 100
"""
from __future__ import annotations

PROFITABLE = "PROFITABLE"
AT_RISK = "AT_RISK"
UNPROFITABLE = "UNPROFITABLE"
NO_COST_DATA = "NO_COST_DATA"

DECLINING = "DECLINING"
IMPROVING = "IMPROVING"
STABLE = "STABLE"
NEW = "NEW"


def net_profit(revenue: float, fuel_cost: float, maintenance_cost: float, driver_share: float) -> float:
    return revenue - revenue * driver_share - fuel_cost - maintenance_cost


def margin_pct(net: float, revenue: float) -> float | None:
    if revenue <= 0:
        return None
    return net / revenue * 100.0


def classify(net: float, revenue: float, has_cost_data: bool, at_risk_margin_pct: float) -> str:
    """Status of one vehicle for one day."""
    if not has_cost_data:
        return NO_COST_DATA
    if net < 0:
        return UNPROFITABLE
    margin = margin_pct(net, revenue)
    if margin is None or margin < at_risk_margin_pct:
        return AT_RISK
    return PROFITABLE


def trend(nets: list[float | None]) -> str:
    """Trend of daily net profit, oldest first and today last (3 values)."""
    values = [n for n in nets if n is not None]
    if len(values) < 3 or len(nets) < 3 or nets[-1] is None:
        return NEW
    a, b, c = nets[-3], nets[-2], nets[-1]
    if a > b > c:
        return DECLINING
    if a < b < c:
        return IMPROVING
    return STABLE


def becoming_unprofitable(status: str, trend_value: str, prev_status: str | None, service_day: bool) -> bool:
    """A vehicle is "becoming unprofitable" when it is losing money or at risk
    today and either its profit has fallen for two days in a row, or it was
    profitable yesterday and is losing money today. Days with a garage service
    bill are never counted: one large bill is not a trend."""
    if status not in (UNPROFITABLE, AT_RISK) or service_day:
        return False
    if trend_value == DECLINING:
        return True
    return status == UNPROFITABLE and prev_status == PROFITABLE
