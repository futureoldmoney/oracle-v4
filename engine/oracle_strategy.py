"""
Oracle Strategy
================
The single decision-maker for the oracle frontrunning bot.

Reads Chainlink price movement, checks timing + magnitude against
sliding thresholds, computes fair value at actual market price,
verifies sufficient edge, and returns a trade-or-skip decision.

ONE file, ONE job. All gating logic lives here — no duplicate
checks in main_v4.py or run.py.

Continuous monitoring: instead of a fixed 20-120s window, the bot
evaluates every cycle throughout the entire 5-minute window. Bigger
moves are tradeable earlier; smaller moves need later confirmation.

  seconds_remaining | min_magnitude
  300-180 (early)   | 0.20% (only massive moves)
  180-120 (mid)     | 0.12%
  120-60 (sweet)    | 0.08% (primary zone)
  60-30 (late)      | 0.05% (with LTP confirmation)
  <20 (too late)    | SKIP (fill uncertainty)
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any

from engine.fair_value import (
    get_required_magnitude,
    compute_fair_value,
    estimate_fill_price,
    compute_edge_at_fill,
)
from engine.position_sizer import PositionSizer, SizeResult

logger = logging.getLogger("oracle.strategy")


@dataclass
class TradeDecision:
    """Complete trade decision with full audit trail."""
    should_trade: bool
    direction: str = "NEUTRAL"       # UP / DOWN / NEUTRAL
    side: str = ""                   # YES / NO
    confidence: float = 0.0          # empirical win rate (fair value)
    fair_value: float = 0.0          # same as confidence (probability correct)
    fill_price: float = 0.0          # actual market price we'd pay
    edge_at_fill: float = 0.0        # fair_value - fill_price (as %)
    size_usd: float = 0.0           # position size in USD
    size_pct: float = 0.0           # position size as % of bankroll
    execution_mode: str = "TAKER"    # TAKER / AGGRESSIVE_MAKER
    magnitude_pct: float = 0.0       # absolute Chainlink move %
    seconds_remaining: int = 0
    reason: str = ""
    # Amplifier signals
    ltp_confirms: Optional[bool] = None
    pcr_adjustment: float = 0.0
    sentiment_adjustment: float = 0.0
    tick_velocity: float = 0.0
    coinbase_price: float = 0.0
    # Metadata for logging
    chainlink_price: float = 0.0
    window_open_price: float = 0.0
    timestamp: float = field(default_factory=time.time)


class OracleStrategy:
    """
    Pure oracle frontrunning strategy with continuous monitoring.

    Usage:
        strategy = OracleStrategy(config, position_sizer)
        decision = strategy.evaluate(
            chainlink_tracker=tracker,
            seconds_remaining=85,
            book_state={"best_bid_yes": 0.93, "best_ask_yes": 0.95},
            ltp=0.55,
            pcr_signal=None,
            sentiment_tracker=None,
        )
        if decision.should_trade:
            executor.execute(decision)
    """

    def __init__(self, config: dict, position_sizer: PositionSizer):
        self.config = config
        self.sizer = position_sizer
        self.min_edge_pct = float(config.get("min_edge_pct", 3.0))
        self.taker_fee_rate = float(config.get("taker_fee_rate", 0.0156))
        self._daily_pnl = 0.0

        # Track which windows we've already traded (one trade per window)
        self._traded_windows: set = set()
        # Limit set size to prevent memory growth
        self._max_tracked_windows = 500

    def reset_daily(self):
        """Call at midnight UTC to reset daily state."""
        self._daily_pnl = 0.0
        self._traded_windows.clear()

    def record_pnl(self, pnl: float):
        """Record a settled trade's P&L for daily tracking."""
        self._daily_pnl += pnl

    def evaluate(
        self,
        chainlink_tracker,
        seconds_remaining: int,
        book_state: Optional[Dict] = None,
        ltp: Optional[float] = None,
        pcr_signal: Optional[Dict] = None,
        sentiment_tracker=None,
        tick_velocity_30s: float = 0.0,
        coinbase_price: float = 0.0,
        window_ts: Optional[int] = None,
    ) -> TradeDecision:
        """
        Evaluate whether to trade this cycle.

        This is the SINGLE decision point. All gating logic is here.

        Args:
            chainlink_tracker: ChainlinkWindowTracker instance
            seconds_remaining: seconds until the 5-min window closes
            book_state: dict with best_bid_yes, best_ask_yes
            ltp: last trade price from CLOB (0-1)
            pcr_signal: Deribit put/call ratio signal dict
            sentiment_tracker: BTCSentimentTracker instance
            tick_velocity_30s: how fast the PM probability is moving
            coinbase_price: independent BTC spot price
            window_ts: current window timestamp (for one-trade-per-window)

        Returns:
            TradeDecision with full audit trail
        """
        # ── GATE 1: One trade per window ──────────────────────
        if window_ts is not None:
            if window_ts in self._traded_windows:
                return self._skip(
                    f"Already traded window {window_ts}",
                    seconds_remaining=seconds_remaining,
                )

        # ── GATE 2: Too late to fill ──────────────────────────
        if seconds_remaining < 20:
            return self._skip(
                f"Too late: {seconds_remaining}s remaining (< 20s)",
                seconds_remaining=seconds_remaining,
            )

        # ── GATE 3: Get Chainlink data ────────────────────────
        cl_data = self._get_chainlink_data(chainlink_tracker)
        if cl_data is None:
            return self._skip(
                "No Chainlink window data available",
                seconds_remaining=seconds_remaining,
            )

        magnitude_pct = abs(cl_data["move_pct"])
        direction = "UP" if cl_data["move_pct"] > 0 else "DOWN"

        # ── GATE 4: Magnitude vs timing threshold ─────────────
        required_mag = get_required_magnitude(seconds_remaining)
        if magnitude_pct < required_mag:
            return self._skip(
                f"Magnitude {magnitude_pct:.4f}% < required "
                f"{required_mag:.2f}% at {seconds_remaining}s remaining",
                seconds_remaining=seconds_remaining,
                magnitude_pct=magnitude_pct,
                chainlink_price=cl_data.get("current_price", 0),
                window_open_price=cl_data.get("open_price", 0),
            )

        # ── GATE 5: LTP confirmation for weak signals ────────
        ltp_confirms = None
        if ltp is not None:
            if direction == "UP" and ltp > 0.55:
                ltp_confirms = True
            elif direction == "DOWN" and ltp < 0.45:
                ltp_confirms = True
            elif direction == "UP" and ltp < 0.45:
                ltp_confirms = False
            elif direction == "DOWN" and ltp > 0.55:
                ltp_confirms = False

        # If magnitude is in the weak zone (0.05-0.08%) AND late in window,
        # require LTP confirmation
        if magnitude_pct < 0.08 and seconds_remaining < 90:
            if ltp_confirms is False:
                return self._skip(
                    f"LTP {ltp:.3f} contradicts oracle {direction} "
                    f"(weak signal {magnitude_pct:.4f}%)",
                    seconds_remaining=seconds_remaining,
                    magnitude_pct=magnitude_pct,
                )

        # ── COMPUTE: Fair value ───────────────────────────────
        base_confidence = compute_fair_value(magnitude_pct, seconds_remaining)

        # Apply amplifiers (each capped at ±0.03)
        confidence = base_confidence

        # LTP confirmation boost
        if ltp_confirms is True:
            confidence = min(0.99, confidence + 0.02)
        elif ltp_confirms is False:
            confidence = max(0.50, confidence - 0.03)

        # Deribit PCR adjustment
        pcr_adj = 0.0
        if pcr_signal and pcr_signal.get("direction"):
            pcr_dir = pcr_signal["direction"]
            if pcr_dir == direction:
                pcr_adj = min(0.03, pcr_signal.get("strength", 0) * 0.05)
            elif pcr_dir != "NEUTRAL":
                pcr_adj = max(-0.03, -pcr_signal.get("strength", 0) * 0.03)
            confidence = max(0.50, min(0.99, confidence + pcr_adj))

        # Sentiment adjustment
        sentiment_adj = 0.0
        if sentiment_tracker:
            sentiment_adj = sentiment_tracker.get_confidence_adjustment(direction)
            confidence = max(0.50, min(0.99, confidence + sentiment_adj))

        # Coinbase cross-validation: use more conservative magnitude
        if coinbase_price > 0 and cl_data.get("open_price", 0) > 0:
            coinbase_move = abs(
                (coinbase_price - cl_data["open_price"]) / cl_data["open_price"] * 100
            )
            if coinbase_move < magnitude_pct * 0.5:
                # Coinbase disagrees significantly — reduce confidence
                confidence = max(0.50, confidence - 0.03)
                logger.info(
                    f"Coinbase cross-check: CL={magnitude_pct:.4f}% vs "
                    f"CB={coinbase_move:.4f}% — confidence reduced"
                )

        # ── GATE 6: Book data required ────────────────────────
        if not book_state:
            return self._skip(
                "No book data available",
                seconds_remaining=seconds_remaining,
                magnitude_pct=magnitude_pct,
                direction=direction,
            )

        # ── COMPUTE: Fill price at actual market ──────────────
        fill_price = estimate_fill_price(book_state, direction)
        if fill_price is None:
            return self._skip(
                f"Cannot estimate fill price for {direction} "
                f"(book: bid={book_state.get('best_bid_yes')}, "
                f"ask={book_state.get('best_ask_yes')})",
                seconds_remaining=seconds_remaining,
                magnitude_pct=magnitude_pct,
                direction=direction,
            )

        # ── GATE 7: Edge check at actual fill price ───────────
        edge = compute_edge_at_fill(
            confidence, fill_price,
            min_edge_pct=self.min_edge_pct,
            taker_fee_rate=self.taker_fee_rate,
        )

        if not edge.should_trade:
            return self._skip(
                edge.reason,
                seconds_remaining=seconds_remaining,
                magnitude_pct=magnitude_pct,
                direction=direction,
                fair_value=confidence,
                fill_price=fill_price,
                edge_at_fill=edge.edge_pct,
            )

        # ── COMPUTE: Position size ────────────────────────────
        size_result = self.sizer.compute_size(
            confidence=confidence,
            fill_price=fill_price,
            daily_pnl=self._daily_pnl,
        )

        if not size_result.should_trade:
            return self._skip(
                f"Sizer rejected: {size_result.reason}",
                seconds_remaining=seconds_remaining,
                magnitude_pct=magnitude_pct,
                direction=direction,
                fair_value=confidence,
                fill_price=fill_price,
                edge_at_fill=edge.edge_pct,
            )

        # ── DECIDE: Execution mode ────────────────────────────
        # High tick velocity = market repricing fast = must taker
        # Low velocity = can try aggressive maker first
        if abs(tick_velocity_30s) > 0.02:
            execution_mode = "TAKER"
        else:
            execution_mode = "ADAPTIVE"  # Try maker, fallback taker

        # ── RESULT: TRADE ─────────────────────────────────────
        side = "YES" if direction == "UP" else "NO"

        # Mark this window as traded
        if window_ts is not None:
            self._traded_windows.add(window_ts)
            if len(self._traded_windows) > self._max_tracked_windows:
                # Remove oldest entries
                sorted_windows = sorted(self._traded_windows)
                self._traded_windows = set(sorted_windows[-200:])

        logger.info(
            f"TRADE: {side} {direction} | "
            f"FV={confidence:.3f} fill=${fill_price:.3f} "
            f"edge={edge.edge_pct:.1f}% | "
            f"${size_result.size_usd:.2f} ({size_result.size_pct:.1%}) | "
            f"CL={cl_data['move_pct']:+.4f}% {seconds_remaining}s rem | "
            f"mode={execution_mode}"
        )

        return TradeDecision(
            should_trade=True,
            direction=direction,
            side=side,
            confidence=confidence,
            fair_value=confidence,
            fill_price=fill_price,
            edge_at_fill=edge.edge_pct,
            size_usd=size_result.size_usd,
            size_pct=size_result.size_pct,
            execution_mode=execution_mode,
            magnitude_pct=magnitude_pct,
            seconds_remaining=seconds_remaining,
            reason=(
                f"TRADE: {direction} | CL {cl_data['move_pct']:+.4f}% | "
                f"FV={confidence:.3f} fill=${fill_price:.3f} "
                f"edge={edge.edge_pct:.1f}% | "
                f"${size_result.size_usd:.2f} ({size_result.size_pct:.1%})"
            ),
            ltp_confirms=ltp_confirms,
            pcr_adjustment=pcr_adj,
            sentiment_adjustment=sentiment_adj,
            tick_velocity=tick_velocity_30s,
            coinbase_price=coinbase_price,
            chainlink_price=cl_data.get("current_price", 0),
            window_open_price=cl_data.get("open_price", 0),
        )

    def _get_chainlink_data(self, tracker) -> Optional[Dict]:
        """
        Extract current Chainlink move data from the window tracker.

        Returns dict with: move_pct, current_price, open_price
        or None if unavailable.
        """
        try:
            if tracker is None:
                return None

            if hasattr(tracker, "get_current_move"):
                move = tracker.get_current_move()
                if move is not None:
                    return {
                        "move_pct": move.get("move_pct", 0),
                        "current_price": move.get("current_price", 0),
                        "open_price": move.get("open_price", 0),
                    }

            # Fallback: try direct attribute access
            if hasattr(tracker, "current_move_pct"):
                return {
                    "move_pct": tracker.current_move_pct,
                    "current_price": getattr(tracker, "current_price", 0),
                    "open_price": getattr(tracker, "window_open_price", 0),
                }

            # Fallback: try get_direction() method
            if hasattr(tracker, "get_direction"):
                direction_data = tracker.get_direction()
                if direction_data:
                    return {
                        "move_pct": direction_data.get("move_pct", 0),
                        "current_price": direction_data.get("price", 0),
                        "open_price": direction_data.get("open_price", 0),
                    }

            return None
        except Exception as e:
            logger.debug(f"Failed to get Chainlink data: {e}")
            return None

    def _skip(
        self,
        reason: str,
        seconds_remaining: int = 0,
        magnitude_pct: float = 0,
        direction: str = "NEUTRAL",
        fair_value: float = 0,
        fill_price: float = 0,
        edge_at_fill: float = 0,
        chainlink_price: float = 0,
        window_open_price: float = 0,
    ) -> TradeDecision:
        """Create a skip decision with context."""
        return TradeDecision(
            should_trade=False,
            direction=direction,
            magnitude_pct=magnitude_pct,
            seconds_remaining=seconds_remaining,
            reason=reason,
            fair_value=fair_value,
            fill_price=fill_price,
            edge_at_fill=edge_at_fill,
            chainlink_price=chainlink_price,
            window_open_price=window_open_price,
        )
