"""
Tier 1 Signals: Core Edge Amplifiers
=====================================
These signals enhance the primary oracle frontrunning edge.
They don't generate standalone trades — they make existing trades better.

Signal 1: Stale Quote Sniping
Signal 2: Spread Regime Gating
Signal 3: Oracle Confidence Scoring
"""

import time
import math
import logging
from dataclasses import dataclass, field
from typing import Optional, Deque, Dict, List, Tuple
from collections import deque

from ensemble_engine import SignalOutput, Direction, RegimeState

logger = logging.getLogger("oracle.tier1")


# ═══════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════

@dataclass
class PriceTick:
    """A single price observation from any source."""
    source: str       # "binance", "chainlink", "polymarket_yes", "polymarket_no"
    price: float
    timestamp: float  # Unix seconds with millisecond precision

@dataclass
class BookLevel:
    price: float
    size: float

@dataclass
class OrderBookSnapshot:
    """Full order book state at a point in time."""
    bids: List[BookLevel]  # Sorted descending by price
    asks: List[BookLevel]  # Sorted ascending by price
    timestamp: float
    book_hash: str = ""

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0].price if self.asks else None

    @property
    def spread(self) -> Optional[float]:
        if self.best_bid is not None and self.best_ask is not None:
            return self.best_ask - self.best_bid
        return None

    @property
    def midpoint(self) -> Optional[float]:
        if self.best_bid is not None and self.best_ask is not None:
            return (self.best_bid + self.best_ask) / 2.0
        return None

    def depth(self, side: str, levels: int = 5) -> float:
        """Total size across top N levels."""
        book = self.bids if side == "bid" else self.asks
        return sum(level.size for level in book[:levels])

    def weighted_depth(self, side: str, levels: int = 5) -> float:
        """Price-weighted depth: sum(price * size) for top N levels."""
        book = self.bids if side == "bid" else self.asks
        return sum(level.price * level.size for level in book[:levels])

    def executable_price(self, side: str, size: float) -> Optional[float]:
        """
        What price would we actually get filling `size` units?
        side="buy" → walk up the asks
        side="sell" → walk down the bids
        Returns volume-weighted average fill price, or None if insufficient liquidity.
        """
        book = self.asks if side == "buy" else self.bids
        remaining = size
        total_cost = 0.0
        for level in book:
            fill = min(remaining, level.size)
            total_cost += fill * level.price
            remaining -= fill
            if remaining <= 0:
                break
        if remaining > 0:
            return None  # Not enough liquidity
        return total_cost / size


# ═══════════════════════════════════════════════════════════════
# SIGNAL 1: STALE QUOTE SNIPING
# ═══════════════════════════════════════════════════════════════

class StaleQuoteDetector:
    """
    Detects when Polymarket order book quotes are stale relative to
    Binance price movements. This is latency arbitrage — the market
    maker hasn't updated yet, so their quotes are mispriced.
    
    How it works:
    1. Track Binance BTC price with sub-second granularity
    2. Track Polymarket order book update timestamps
    3. When Binance moves > threshold but book hasn't changed in > staleness_window,
       flag the quotes as stale
    4. Direction: if Binance moved UP, YES quotes are stale-cheap → buy YES
    
    Parameters to tune:
    - binance_move_threshold: Minimum Binance move to consider (0.1% = 10bps)
    - staleness_window_ms: How long book must be unchanged (500ms default)
    - lookback_seconds: Window to measure Binance move over
    """

    def __init__(
        self,
        binance_move_threshold_pct: float = 0.10,   # 0.10% = 10 bps
        staleness_window_ms: float = 500.0,          # 500ms book unchanged
        lookback_seconds: float = 5.0,               # Measure Binance move over 5s
        max_ticks: int = 200,                        # Rolling buffer size
        confidence_base: float = 0.5,                # Base confidence when stale
        confidence_per_10bps: float = 0.1,           # +0.1 confidence per 10bps move
    ):
        self.binance_move_threshold_pct = binance_move_threshold_pct
        self.staleness_window_ms = staleness_window_ms
        self.lookback_seconds = lookback_seconds
        self.confidence_base = confidence_base
        self.confidence_per_10bps = confidence_per_10bps

        self._binance_ticks: Deque[PriceTick] = deque(maxlen=max_ticks)
        self._last_book_update: float = 0.0
        self._last_book_hash: str = ""

    def ingest_binance(self, tick: PriceTick):
        """Feed Binance BTC price ticks."""
        self._binance_ticks.append(tick)

    def ingest_book(self, book: OrderBookSnapshot):
        """Feed Polymarket order book snapshots."""
        if book.book_hash != self._last_book_hash:
            self._last_book_update = book.timestamp
            self._last_book_hash = book.book_hash

    def evaluate(self, now: Optional[float] = None) -> SignalOutput:
        """
        Check if current quotes are stale.
        
        Returns a SignalOutput with:
        - Direction: UP if Binance went up (YES is stale-cheap), DOWN if Binance went down
        - Confidence: Higher for larger Binance moves and longer staleness
        - Edge estimate: Approximately equal to the Binance move magnitude
        """
        now = now or time.time()

        # Need sufficient ticks for reliable move detection
        if len(self._binance_ticks) < 10:
            return self._neutral("Insufficient Binance data")

        # Get current and lookback price
        current_tick = self._binance_ticks[-1]
        cutoff = now - self.lookback_seconds
        lookback_ticks = [t for t in self._binance_ticks if t.timestamp >= cutoff]

        if len(lookback_ticks) < 2:
            return self._neutral("Insufficient lookback data")

        earliest = lookback_ticks[0]
        latest = lookback_ticks[-1]

        # Compute Binance move
        if earliest.price == 0:
            return self._neutral("Zero base price")

        move_pct = ((latest.price - earliest.price) / earliest.price) * 100.0
        abs_move_pct = abs(move_pct)

        # Check if move exceeds threshold
        if abs_move_pct < self.binance_move_threshold_pct:
            return self._neutral(f"Binance move {abs_move_pct:.3f}% < threshold")

        # Check if book is stale
        staleness_ms = (now - self._last_book_update) * 1000.0
        if staleness_ms < self.staleness_window_ms:
            return self._neutral(
                f"Book updated {staleness_ms:.0f}ms ago (< {self.staleness_window_ms}ms)"
            )

        # Book IS stale and Binance HAS moved — signal!
        direction = Direction.UP if move_pct > 0 else Direction.DOWN

        # Confidence scales with move size and staleness duration
        move_bps = abs_move_pct * 100  # Convert % to bps
        conf = self.confidence_base + (move_bps / 10.0) * self.confidence_per_10bps
        # Staleness bonus: longer stale = higher confidence (maker is slower)
        staleness_bonus = min(staleness_ms / 2000.0, 0.2)  # Max +0.2 for 2s stale
        conf = min(conf + staleness_bonus, 0.95)

        # Edge estimate ≈ Binance move magnitude (in bps of probability)
        # If BTC moved +0.2%, YES price should be ~100-200bps higher
        edge_bps = move_bps * 1.0  # 1:1 mapping initially, calibrate with data

        return SignalOutput(
            signal_name="STALE_QUOTE",
            direction=direction,
            confidence=round(conf, 3),
            estimated_edge_bps=round(edge_bps, 1),
            magnitude=round(abs_move_pct, 4),
            metadata={
                "binance_move_pct": round(move_pct, 4),
                "staleness_ms": round(staleness_ms, 0),
                "lookback_seconds": self.lookback_seconds,
                "binance_ticks_in_window": len(lookback_ticks),
            },
        )

    def _neutral(self, reason: str) -> SignalOutput:
        return SignalOutput(
            signal_name="STALE_QUOTE",
            direction=Direction.NEUTRAL,
            confidence=0.0,
            estimated_edge_bps=0.0,
            magnitude=0.0,
            metadata={"skip_reason": reason},
        )


# ═══════════════════════════════════════════════════════════════
# SIGNAL 2: SPREAD REGIME GATING
# ═══════════════════════════════════════════════════════════════

class SpreadRegimeDetector:
    """
    Classifies the current market regime based on spread behavior.
    This is a META-SIGNAL: it doesn't generate trades, it modulates
    position sizing for other signals.
    
    Regimes:
    - TIGHT:   Spread ≤ P20 of recent history → consensus, low edge
    - NORMAL:  P20 < Spread ≤ P60 → standard trading
    - WIDE:    P60 < Spread ≤ P90 → high opportunity, oracle edge is amplified
    - EXTREME: Spread > P90 → unusual, be cautious
    
    Uses a rolling window of spread observations to compute percentiles.
    """

    def __init__(
        self,
        window_size: int = 360,  # ~30 min at 5-second polling
        tight_pct: float = 20.0,
        wide_pct: float = 60.0,
        extreme_pct: float = 90.0,
    ):
        self.window_size = window_size
        self.tight_pct = tight_pct
        self.wide_pct = wide_pct
        self.extreme_pct = extreme_pct
        self._spread_history: Deque[float] = deque(maxlen=window_size)
        self._depth_history: Deque[float] = deque(maxlen=window_size)
        self._vol_history: Deque[float] = deque(maxlen=window_size)

    def ingest_book(self, book: OrderBookSnapshot):
        """Feed order book snapshots to build spread distribution."""
        spread = book.spread
        if spread is not None and spread >= 0:
            self._spread_history.append(spread)

        # Track depth for vacuum detection
        total_depth = book.depth("bid", 5) + book.depth("ask", 5)
        self._depth_history.append(total_depth)

    def ingest_binance_vol(self, vol_1m: float):
        """Feed 1-minute realized volatility from Binance."""
        self._vol_history.append(vol_1m)

    def evaluate(self) -> RegimeState:
        """Classify current regime."""
        if len(self._spread_history) < 30:
            return RegimeState(
                spread_percentile=50.0,
                volatility_1m=0.0,
                book_depth_ratio=1.0,
                label="normal",
            )

        current_spread = self._spread_history[-1]
        sorted_spreads = sorted(self._spread_history)
        n = len(sorted_spreads)

        # Compute percentile of current spread
        rank = sum(1 for s in sorted_spreads if s <= current_spread)
        percentile = (rank / n) * 100.0

        # Classify
        if percentile <= self.tight_pct:
            label = "tight"
        elif percentile <= self.wide_pct:
            label = "normal"
        elif percentile <= self.extreme_pct:
            label = "wide"
        else:
            label = "extreme"

        # Depth ratio
        avg_depth = sum(self._depth_history) / len(self._depth_history) if self._depth_history else 1.0
        current_depth = self._depth_history[-1] if self._depth_history else avg_depth
        depth_ratio = current_depth / avg_depth if avg_depth > 0 else 1.0

        # Volatility
        vol = self._vol_history[-1] if self._vol_history else 0.0

        return RegimeState(
            spread_percentile=round(percentile, 1),
            volatility_1m=round(vol, 6),
            book_depth_ratio=round(depth_ratio, 3),
            label=label,
        )


# ═══════════════════════════════════════════════════════════════
# SIGNAL 3: ORACLE CONFIDENCE SCORING
# ═══════════════════════════════════════════════════════════════

class OracleConfidenceScorer:
    """
    Scores the reliability of the oracle frontrunning signal.
    
    The core oracle signal says "Chainlink is going to settle at X, 
    but Polymarket thinks it'll settle at Y." The CONFIDENCE of that
    signal depends on:
    
    1. MAGNITUDE: How big is the Binance move? Larger = more certain
    2. SPEED: How fast did it happen? Sharp spikes propagate more reliably
    3. VOLUME: Is Binance volume supporting the move? High volume = real
    4. SPREAD: Is Polymarket already adjusting? Wide spread = hasn't caught up
    5. BOOK STALENESS: Are makers still quoting old prices?
    
    The output is a modified oracle signal with calibrated confidence.
    """

    def __init__(
        self,
        # Sigmoid parameters for confidence function
        alpha: float = 5.0,       # Magnitude sensitivity
        beta: float = 2.0,        # Volume spike sensitivity
        gamma: float = 1.5,       # Spread penalty sensitivity
        base_confidence: float = 0.3,
        max_confidence: float = 0.95,
        # Thresholds
        min_magnitude_pct: float = 0.05,  # Minimum BTC move to consider
    ):
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.base_confidence = base_confidence
        self.max_confidence = max_confidence
        self.min_magnitude_pct = min_magnitude_pct

        self._binance_volume_1m: Deque[float] = deque(maxlen=60)

    def ingest_binance_volume(self, volume: float):
        """Feed 1-second Binance volume observations."""
        self._binance_volume_1m.append(volume)

    def score(
        self,
        oracle_direction: Direction,
        binance_move_pct: float,
        move_duration_seconds: float,
        current_spread: float,
        avg_spread_1h: float,
        book_staleness_ms: float,
        chainlink_price: float,
        polymarket_midpoint: float,
    ) -> SignalOutput:
        """
        Score an oracle frontrunning opportunity.
        
        Args:
            oracle_direction: UP or DOWN based on Chainlink vs Polymarket
            binance_move_pct: Absolute BTC price change (%)
            move_duration_seconds: How long the move took
            current_spread: Current Polymarket spread
            avg_spread_1h: Average spread over last hour
            book_staleness_ms: Time since last book update
            chainlink_price: Current Chainlink BTC price
            polymarket_midpoint: Current Polymarket YES midpoint
        """
        if oracle_direction == Direction.NEUTRAL:
            return self._neutral("No oracle signal")

        abs_move = abs(binance_move_pct)
        if abs_move < self.min_magnitude_pct:
            return self._neutral(f"Move {abs_move:.3f}% < minimum {self.min_magnitude_pct}%")

        # 1. Speed-adjusted magnitude
        duration = max(move_duration_seconds, 0.1)
        speed_score = abs_move / math.sqrt(duration)

        # 2. Volume spike
        avg_vol = (sum(self._binance_volume_1m) / len(self._binance_volume_1m)
                   if self._binance_volume_1m else 1.0)
        recent_vol = self._binance_volume_1m[-1] if self._binance_volume_1m else avg_vol
        vol_spike = recent_vol / avg_vol if avg_vol > 0 else 1.0

        # 3. Spread percentile (high = polymarket hasn't caught up = good for us)
        spread_ratio = current_spread / avg_spread_1h if avg_spread_1h > 0 else 1.0

        # 4. Staleness bonus
        staleness_factor = min(book_staleness_ms / 1000.0, 2.0)  # 0-2 range

        # Composite confidence via modified sigmoid
        raw_score = (
            self.alpha * speed_score
            + self.beta * math.log1p(vol_spike)
            + self.gamma * math.log1p(spread_ratio)
            + staleness_factor
        )

        # Sigmoid mapping to [base_confidence, max_confidence]
        sigmoid = 1.0 / (1.0 + math.exp(-raw_score + 3.0))  # Shifted so 50% ≈ moderate signal
        confidence = self.base_confidence + sigmoid * (self.max_confidence - self.base_confidence)
        confidence = min(confidence, self.max_confidence)

        # Edge estimate: the gap between where Chainlink will settle and
        # where Polymarket currently prices
        # This is in probability space, not price space
        edge_bps = abs_move * 100  # Rough: 0.1% BTC move ≈ 10bps edge on 50¢ market
        # Adjust for how much of the move is already reflected in spread
        if spread_ratio > 1.5:
            edge_bps *= 0.7  # Market is partially repricing
        if spread_ratio > 2.5:
            edge_bps *= 0.5  # Market has significantly repriced

        return SignalOutput(
            signal_name="ORACLE_CONFIDENCE",
            direction=oracle_direction,
            confidence=round(confidence, 3),
            estimated_edge_bps=round(edge_bps, 1),
            magnitude=round(abs_move, 4),
            metadata={
                "speed_score": round(speed_score, 4),
                "vol_spike": round(vol_spike, 2),
                "spread_ratio": round(spread_ratio, 3),
                "staleness_ms": round(book_staleness_ms, 0),
                "raw_score": round(raw_score, 3),
                "chainlink_price": chainlink_price,
                "polymarket_midpoint": polymarket_midpoint,
            },
        )

    def _neutral(self, reason: str) -> SignalOutput:
        return SignalOutput(
            signal_name="ORACLE_CONFIDENCE",
            direction=Direction.NEUTRAL,
            confidence=0.0,
            estimated_edge_bps=0.0,
            magnitude=0.0,
            metadata={"skip_reason": reason},
        )


# ═══════════════════════════════════════════════════════════════
# COMBINED TIER 1 RUNNER
# ═══════════════════════════════════════════════════════════════

class Tier1SignalSuite:
    """
    Orchestrates all Tier 1 signals together.
    
    Usage:
        suite = Tier1SignalSuite()
        
        # Feed data continuously
        suite.ingest_binance_tick(price_tick)
        suite.ingest_book_snapshot(book)
        
        # Get combined signals for ensemble
        signals = suite.evaluate(oracle_direction, oracle_data)
    """

    def __init__(self, config: Optional[Dict] = None):
        config = config or {}

        self.stale_detector = StaleQuoteDetector(
            binance_move_threshold_pct=config.get("stale_move_threshold_pct", 0.10),
            staleness_window_ms=config.get("staleness_window_ms", 500.0),
            lookback_seconds=config.get("stale_lookback_seconds", 5.0),
        )
        self.regime_detector = SpreadRegimeDetector(
            window_size=config.get("regime_window_size", 360),
        )
        self.confidence_scorer = OracleConfidenceScorer(
            alpha=config.get("confidence_alpha", 5.0),
            beta=config.get("confidence_beta", 2.0),
            gamma=config.get("confidence_gamma", 1.5),
        )

        self._last_binance_price: float = 0.0
        self._last_binance_time: float = 0.0
        self._avg_spread_1h: float = 0.02  # Default 2 cents
        self._spread_history_1h: Deque[float] = deque(maxlen=720)  # 1hr at 5s

    def ingest_binance_tick(self, tick: PriceTick):
        """Feed a Binance BTC price tick."""
        self.stale_detector.ingest_binance(tick)
        self._last_binance_price = tick.price
        self._last_binance_time = tick.timestamp

    def ingest_book_snapshot(self, book: OrderBookSnapshot):
        """Feed a Polymarket order book snapshot."""
        self.stale_detector.ingest_book(book)
        self.regime_detector.ingest_book(book)

        spread = book.spread
        if spread is not None:
            self._spread_history_1h.append(spread)
            if self._spread_history_1h:
                self._avg_spread_1h = sum(self._spread_history_1h) / len(self._spread_history_1h)

    def ingest_binance_volume(self, volume: float):
        """Feed Binance volume data."""
        self.confidence_scorer.ingest_binance_volume(volume)

    def ingest_binance_volatility(self, vol_1m: float):
        """Feed 1-minute realized volatility."""
        self.regime_detector.ingest_binance_vol(vol_1m)

    def evaluate(
        self,
        oracle_direction: Direction,
        binance_move_pct: float,
        move_duration_seconds: float,
        chainlink_price: float,
        polymarket_midpoint: float,
        book_staleness_ms: float,
    ) -> Tuple[List[SignalOutput], RegimeState]:
        """
        Evaluate all Tier 1 signals.
        
        Returns:
            Tuple of (list of signal outputs, current regime state)
        """
        signals = []

        # 1. Stale quote detection
        stale_signal = self.stale_detector.evaluate()
        if stale_signal.is_actionable:
            signals.append(stale_signal)

        # 2. Regime detection (always evaluate, used by ensemble)
        regime = self.regime_detector.evaluate()

        # 3. Oracle confidence scoring
        current_spread = self._spread_history_1h[-1] if self._spread_history_1h else 0.02
        oracle_signal = self.confidence_scorer.score(
            oracle_direction=oracle_direction,
            binance_move_pct=binance_move_pct,
            move_duration_seconds=move_duration_seconds,
            current_spread=current_spread,
            avg_spread_1h=self._avg_spread_1h,
            book_staleness_ms=book_staleness_ms,
            chainlink_price=chainlink_price,
            polymarket_midpoint=polymarket_midpoint,
        )
        if oracle_signal.is_actionable:
            signals.append(oracle_signal)

        return signals, regime

    def get_diagnostics(self) -> Dict:
        """Return diagnostic data for Brain review."""
        return {
            "spread_history_count": len(self._spread_history_1h),
            "avg_spread_1h": round(self._avg_spread_1h, 4),
            "binance_ticks": len(self.stale_detector._binance_ticks),
            "regime": self.regime_detector.evaluate().label,
            "regime_percentile": self.regime_detector.evaluate().spread_percentile,
        }
