"""
Fair Value Model
=================
Computes the probability that the oracle's direction call is correct,
based on empirical data from 236 Chainlink windows.

This is the pricing brain of the bot. Instead of fixed $0.52/$0.48,
every trade is priced according to how confident we ACTUALLY are
given the magnitude of the Chainlink move and the timing within
the 5-minute window.

The key insight: a 0.20% move at 60 seconds in is a MUCH stronger
signal than a 0.05% move at 240 seconds in. The fair value model
captures this relationship.
"""

import math
import logging
from dataclasses import dataclass
from typing import Optional, Dict

logger = logging.getLogger("oracle.fair_value")


# ═══════════════════════════════════════════════════════════════
# EMPIRICAL WIN RATE TABLE
# ═══════════════════════════════════════════════════════════════
# Source: 236 Chainlink windows from live bot data (March 2026)
# Format: (min_magnitude_pct, min_pct_through_window) -> win_rate
#
# These are CONSERVATIVE estimates. The local_learner will update
# them as more settlement data accumulates.

EMPIRICAL_WIN_RATES = {
    # (magnitude, pct_through) -> win_rate
    # Early window — only trade very large moves
    (0.20, 0.20): 0.88,
    (0.15, 0.20): 0.84,
    (0.12, 0.20): 0.80,
    # Mid window — moderate moves tradeable
    (0.20, 0.40): 0.92,
    (0.15, 0.40): 0.89,
    (0.12, 0.40): 0.86,
    (0.08, 0.40): 0.82,
    # Sweet spot (60-120s remaining)
    (0.20, 0.60): 0.96,
    (0.15, 0.60): 0.95,
    (0.12, 0.60): 0.93,
    (0.08, 0.60): 0.90,
    (0.05, 0.60): 0.85,
    # Primary zone (60-90s remaining = 70-80% through)
    (0.20, 0.75): 0.99,
    (0.15, 0.75): 0.98,
    (0.12, 0.75): 0.97,
    (0.08, 0.75): 0.95,
    (0.05, 0.75): 0.90,
    # Late window (30-60s remaining)
    (0.20, 0.85): 0.99,
    (0.15, 0.85): 0.99,
    (0.12, 0.85): 0.99,
    (0.08, 0.85): 0.97,
    (0.05, 0.85): 0.92,
    (0.03, 0.85): 0.85,
}

# Continuous monitoring thresholds: minimum magnitude to trade at each timing
TIMING_THRESHOLDS = [
    # (max_seconds_remaining, min_magnitude_pct, base_confidence)
    (300, 0.20, 0.88),   # 0-60s into window — only massive moves
    (240, 0.15, 0.84),   # 60-120s into window
    (180, 0.12, 0.90),   # 120-180s into window
    (120, 0.08, 0.93),   # 180-240s into window (sweet spot begins)
    (60,  0.05, 0.90),   # 240-270s into window (primary zone)
    (30,  0.05, 0.92),   # 270-300s into window (late, needs LTP confirm)
    (0,   999,  0.00),   # Too late — never trade
]


def get_required_magnitude(seconds_remaining: int) -> float:
    """
    Return the minimum Chainlink move % needed to trade at this timing.

    Args:
        seconds_remaining: seconds until the 5-minute window closes

    Returns:
        Minimum |chainlink_move_pct| required to generate a trade signal.
        Returns 999.0 if it's too late to trade (< 20s remaining).
    """
    if seconds_remaining < 20:
        return 999.0  # Too late for fills

    for max_secs, min_mag, _ in TIMING_THRESHOLDS:
        if seconds_remaining <= max_secs:
            continue
        return min_mag

    # Fallback: use the most conservative threshold
    return TIMING_THRESHOLDS[0][1]


def compute_fair_value(magnitude_pct: float, seconds_remaining: int) -> float:
    """
    Compute the fair value (probability of oracle being correct)
    based on the magnitude of the Chainlink move and timing.

    This is an interpolation over the empirical win rate table.

    Args:
        magnitude_pct: absolute value of chainlink_move_pct (e.g. 0.08 for 0.08%)
        seconds_remaining: seconds until window closes

    Returns:
        float between 0.50 and 0.99 representing estimated win probability.
        Returns 0.50 (coin flip) if conditions don't meet minimum thresholds.
    """
    pct_through = 1.0 - (seconds_remaining / 300.0)
    pct_through = max(0.0, min(1.0, pct_through))
    abs_mag = abs(magnitude_pct)

    if abs_mag < 0.03:
        return 0.50  # No edge below 0.03%

    # Find the two closest entries in the empirical table and interpolate
    best_match = 0.50
    best_distance = float("inf")

    for (mag, pct), wr in EMPIRICAL_WIN_RATES.items():
        # Only consider entries where our magnitude >= table magnitude
        if abs_mag < mag * 0.8:
            continue

        dist = abs(pct - pct_through) + abs(mag - abs_mag) * 5
        if dist < best_distance:
            best_distance = dist
            best_match = wr

    # Apply magnitude scaling: bigger moves = more confident
    if abs_mag >= 0.20:
        magnitude_boost = 0.02
    elif abs_mag >= 0.12:
        magnitude_boost = 0.01
    else:
        magnitude_boost = 0.0

    fair_value = min(0.99, best_match + magnitude_boost)
    return round(fair_value, 4)


def estimate_fill_price(book_state: dict, direction: str) -> Optional[float]:
    """
    Estimate what we'd actually pay as a taker based on current book state.

    Args:
        book_state: dict with keys best_bid_yes, best_ask_yes
        direction: "UP" or "DOWN"

    Returns:
        Estimated fill price, or None if book data is unavailable.
        For UP trades: we buy YES at best_ask_yes
        For DOWN trades: we buy NO at (1 - best_bid_yes)
    """
    bid_yes = book_state.get("best_bid_yes")
    ask_yes = book_state.get("best_ask_yes")

    if direction == "UP":
        # Buy YES — pay ask_yes
        if ask_yes is not None and 0.01 <= ask_yes < 0.99:
            return float(ask_yes)
        # Fallback: infer YES ask from NO bid (1 - bid_no ≈ ask_yes)
        bid_no = book_state.get("best_bid_no")
        if bid_no is not None and 0.01 < bid_no < 0.99:
            return round(1.0 - float(bid_no), 4)
        return None
    else:  # DOWN — buy NO
        # Primary: NO ask = 1 - bid_yes
        if bid_yes is not None and 0.01 < bid_yes < 0.99:
            return round(1.0 - float(bid_yes), 4)
        # Fallback: use ask_no directly if available
        ask_no = book_state.get("best_ask_no")
        if ask_no is not None and 0.01 <= ask_no < 0.99:
            return float(ask_no)
        # Last resort: infer from ask_yes (when YES is nearly worthless, NO is cheap)
        if ask_yes is not None and 0.0 < ask_yes < 0.50:
            no_price = round(1.0 - float(ask_yes), 4)
            if 0.50 < no_price < 0.99:
                return no_price
        return None


@dataclass
class EdgeResult:
    """Result of edge calculation at actual fill price."""
    ev_per_dollar: float
    edge_pct: float
    kelly_fraction: float
    should_trade: bool
    breakeven_wr: float
    reason: str


def compute_edge_at_fill(
    fair_value: float,
    fill_price: float,
    min_edge_pct: float = 3.0,
    taker_fee_rate: float = 0.0156,
) -> EdgeResult:
    """
    Given the fair value and the price we'd actually fill at,
    compute whether there's enough edge to justify the trade.

    Args:
        fair_value: probability the direction is correct (0.50-0.99)
        fill_price: what we'd actually pay as taker
        min_edge_pct: minimum edge percentage to trade (default 3%)
        taker_fee_rate: taker fee as decimal (0.0156 = 1.56%)

    Returns:
        EdgeResult with full analysis
    """
    if fill_price <= 0 or fill_price >= 1.0:
        return EdgeResult(0, 0, 0, False, 0, f"Invalid fill price: {fill_price}")

    # Binary bet math
    # Win: profit = (1 - fill_price) per share
    # Lose: loss = fill_price per share
    p = fair_value
    q = 1 - p
    win_payout = 1.0 - fill_price
    loss_amount = fill_price

    # EV per dollar risked (before fees)
    ev_gross = p * win_payout - q * loss_amount
    # Deduct taker fee
    ev_net = ev_gross - (fill_price * taker_fee_rate)
    edge_pct = (ev_net / fill_price) * 100 if fill_price > 0 else 0

    # Kelly fraction
    b = win_payout / loss_amount if loss_amount > 0 else 0
    kelly = (p * b - q) / b if b > 0 else 0

    # Breakeven win rate at this fill price (including fee)
    breakeven_wr = (fill_price + fill_price * taker_fee_rate) / 1.0

    should_trade = edge_pct >= min_edge_pct and kelly > 0

    reason = (
        f"FV={fair_value:.3f}, fill=${fill_price:.3f}, "
        f"edge={edge_pct:.1f}%, kelly={kelly:.3f}, "
        f"BE_WR={breakeven_wr:.1%}"
    )
    if not should_trade:
        if edge_pct < min_edge_pct:
            reason = f"Edge {edge_pct:.1f}% < {min_edge_pct}% minimum at ${fill_price:.3f}"
        elif kelly <= 0:
            reason = f"Negative Kelly at ${fill_price:.3f} (no edge after fees)"

    return EdgeResult(
        ev_per_dollar=round(ev_net, 4),
        edge_pct=round(edge_pct, 2),
        kelly_fraction=round(max(0, kelly), 4),
        should_trade=should_trade,
        breakeven_wr=round(breakeven_wr, 4),
        reason=reason,
    )
