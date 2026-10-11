"""
Graduated & Tiered Pricing Calculation Engine.
Uses exact Decimal arithmetic to eliminate floating-point rounding errors in billing.
"""
from decimal import Decimal
from typing import List, Dict, Any, Tuple
from app.models import PricingTier


# Standard rate cards for common SaaS meters
DEFAULT_RATE_CARDS: Dict[str, List[PricingTier]] = {
    "api_calls": [
        PricingTier(tier_name="Tier 1 (Free Allowance)", up_to=Decimal("10000"), unit_price=Decimal("0.0000")),
        PricingTier(tier_name="Tier 2 (Growth)", up_to=Decimal("50000"), unit_price=Decimal("0.0025")),
        PricingTier(tier_name="Tier 3 (Enterprise Overage)", up_to=None, unit_price=Decimal("0.0010")),
    ],
    "compute_seconds": [
        PricingTier(tier_name="Base Compute", up_to=Decimal("36000"), unit_price=Decimal("0.0005")),
        PricingTier(tier_name="Burst Compute", up_to=None, unit_price=Decimal("0.0008")),
    ],
    "storage_gb": [
        PricingTier(tier_name="Standard Storage", up_to=Decimal("100"), unit_price=Decimal("0.1500")),
        PricingTier(tier_name="Scale Storage", up_to=None, unit_price=Decimal("0.0800")),
    ],
}


class GraduatedPricingCalculator:
    """
    Computes graduated pricing:
    Example for 65,000 api_calls:
      - First 10,000 @ $0.0000 = $0.00
      - Next 40,000 (up to 50k) @ $0.0025 = $100.00
      - Remaining 15,000 @ $0.0010 = $15.00
      Total = $115.00
    """
    @staticmethod
    def calculate_meter_cost(meter_name: str, total_units: Decimal) -> Tuple[Decimal, List[Dict[str, Any]]]:
        tiers = DEFAULT_RATE_CARDS.get(
            meter_name,
            [PricingTier(tier_name="Standard Default", up_to=None, unit_price=Decimal("0.0100"))]
        )

        remaining_units = total_units
        current_baseline = Decimal("0")
        subtotal = Decimal("0.0000")
        breakdown: List[Dict[str, Any]] = []

        for tier in tiers:
            if remaining_units <= Decimal("0"):
                break

            if tier.up_to is not None:
                tier_capacity = tier.up_to - current_baseline
                units_in_tier = min(remaining_units, tier_capacity)
                current_baseline = tier.up_to
            else:
                units_in_tier = remaining_units

            cost_for_tier = units_in_tier * tier.unit_price
            subtotal += cost_for_tier
            remaining_units -= units_in_tier

            breakdown.append({
                "tier_name": tier.tier_name,
                "units_billed": float(units_in_tier),
                "unit_price": float(tier.unit_price),
                "tier_subtotal": float(cost_for_tier.quantize(Decimal("0.0001")))
            })

        return subtotal.quantize(Decimal("0.01")), breakdown
