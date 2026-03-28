"""
Position Sizer
===============
Percentage-based position sizing with Kelly criterion scaling.

Kelly determines where in the 1-3% bankroll range each trade lands:
- High conviction (95%+, cheap fill) → near 3%
- Moderate conviction (85-90%) → near 1-2%
- Low conviction → below 1% floor → skip

The floor ensures Polymarket minimum order requirements are met.
The ceiling prevents any single trade from risking more than 3%.

Bankroll scaling behavior:
  $200   → $2-6 per trade
  $1,000 → $10-30 per trade
  $10K   → $100-300 per trade
  $50K   → $500-1,500 per trade
"""

import logging
from dataclasses import dataclass

logger = logging.getLogger("oracle.sizer")


@dataclass
class SizeResult:
    """Complete audit trail for a sizing decision."""
    size_usd: float
    size_pct: float           # percentage of bankroll used
    kelly_raw: float          # raw Kelly fraction
    kelly_adjusted: float     # after fractional multiplier
    capped_by: str            # "kelly" | "max_pct" | "min_floor" | "daily_loss" | etc
    should_trade: bool
    reason: str
    fee_estimate: float       # estimated taker fee at this size


class PositionSizer:
    """
    Compute position size as a percentage of bankroll.

    Usage:
        sizer = PositionSizer(config)
        result = sizer.compute_size(confidence=0.95, fill_price=0.10, daily_pnl=-5.0)
        if result.should_trade:
            place_order(size=result.size_usd)
    """

    def __init__(self, config: dict):
        self.bankroll = float(config.get("bankroll", 1000.0))
        self.fractional_kelly = float(config.get("fractional_kelly", 0.20))
        self.min_size_pct = float(config.get("min_position_pct", 0.01))      # 1% floor
        self.max_size_pct = float(config.get("max_position_pct", 0.03))      # 3% ceiling
        self.max_daily_loss_pct = float(config.get("max_daily_loss_pct", 0.10))
        self.taker_fee_rate = float(config.get("taker_fee_rate", 0.0156))    # 1.56%
        self.min_order_usd = float(config.get("min_order_usd", 1.0))

    def update_bankroll(self, new_bankroll: float):
        """Update bankroll (called after each settlement)."""
        self.bankroll = new_bankroll

    def compute_size(
        self,
        confidence: float,
        fill_price: float,
        daily_pnl: float = 0.0,
    ) -> SizeResult:
        """
        Compute position size for a trade.

        Args:
            confidence: empirical win rate / fair value (0.50-0.99)
            fill_price: actual market price we'd pay as taker
            daily_pnl: running P&L for today (negative = losses)

        Returns:
            SizeResult with full audit trail
        """
        # 1. Daily loss check
        loss_limit = self.bankroll * self.max_daily_loss_pct
        if daily_pnl < -loss_limit:
            return SizeResult(
                0, 0, 0, 0, "daily_loss", False,
                f"Daily loss ${daily_pnl:.2f} exceeds "
                f"-${loss_limit:.2f} ({self.max_daily_loss_pct:.0%} limit)",
                0,
            )

        # 2. Validate fill price
        if fill_price <= 0 or fill_price >= 1.0:
            return SizeResult(
                0, 0, 0, 0, "invalid_price", False,
                f"Invalid fill price: {fill_price}", 0,
            )

        # 3. Kelly formula for binary bet
        #    b = payout ratio = (1 - fill_price) / fill_price
        #    kelly = (p * b - q) / b
        b = (1.0 - fill_price) / fill_price
        q = 1.0 - confidence
        kelly_raw = (confidence * b - q) / b if b > 0 else 0.0

        if kelly_raw <= 0:
            return SizeResult(
                0, 0, round(kelly_raw, 4), 0, "negative_kelly", False,
                f"Negative Kelly ({kelly_raw:.4f}) — no edge at "
                f"${fill_price:.3f} with {confidence:.1%} confidence",
                0,
            )

        # 4. Apply fractional Kelly
        kelly_adjusted = kelly_raw * self.fractional_kelly

        # 5. Convert to bankroll percentage
        size_pct = kelly_adjusted
        capped_by = "kelly"

        # 6. Apply floor and ceiling
        if size_pct < self.min_size_pct:
            size_pct = self.min_size_pct
            capped_by = "min_floor"

        if size_pct > self.max_size_pct:
            size_pct = self.max_size_pct
            capped_by = "max_pct"

        # 7. Convert to USD
        size_usd = self.bankroll * size_pct

        # 8. Polymarket minimum order check (5 shares minimum)
        min_usd_for_5_shares = 5.0 * fill_price
        pm_min = max(self.min_order_usd, min_usd_for_5_shares)

        if size_usd < pm_min:
            if self.bankroll * self.min_size_pct >= pm_min:
                size_usd = pm_min
                size_pct = size_usd / self.bankroll
                capped_by = "pm_minimum"
            else:
                return SizeResult(
                    0, 0, round(kelly_raw, 4), round(kelly_adjusted, 4),
                    "bankroll_too_small", False,
                    f"Bankroll ${self.bankroll:.2f} too small for "
                    f"PM minimum ${pm_min:.2f} "
                    f"(5 shares × ${fill_price:.3f})",
                    0,
                )

        # 9. Estimate taker fee
        fee_estimate = size_usd * self.taker_fee_rate

        return SizeResult(
            size_usd=round(size_usd, 2),
            size_pct=round(size_pct, 4),
            kelly_raw=round(kelly_raw, 4),
            kelly_adjusted=round(kelly_adjusted, 4),
            capped_by=capped_by,
            should_trade=True,
            reason=(
                f"Kelly {kelly_raw:.3f} × {self.fractional_kelly} = "
                f"{kelly_adjusted:.3f} → {size_pct:.1%} of "
                f"${self.bankroll:.0f} = ${size_usd:.2f}"
            ),
            fee_estimate=round(fee_estimate, 4),
        )
