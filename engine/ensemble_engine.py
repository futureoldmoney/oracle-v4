"""
Signal Ensemble Engine
======================
Aggregates multiple uncorrelated signals into a single trading decision.
Each signal produces a SignalOutput. The ensemble combines them using
weighted voting with confidence-adjusted Kelly sizing.

This is the core brain of the institutional-grade bot.
"""

import time
import math
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, List
from enum import Enum

logger = logging.getLogger("oracle.ensemble")


class Direction(Enum):
    UP = "UP"
    DOWN = "DOWN"
    NEUTRAL = "NEUTRAL"


@dataclass
class SignalOutput:
    """Standard output from any signal module."""
    signal_name: str
    direction: Direction
    confidence: float          # 0.0 to 1.0
    estimated_edge_bps: float  # Expected edge in basis points
    magnitude: float           # Raw signal strength (signal-specific units)
    metadata: Dict = field(default_factory=dict)  # Signal-specific debug data
    timestamp: float = field(default_factory=time.time)

    @property
    def is_actionable(self) -> bool:
        return self.direction != Direction.NEUTRAL and self.confidence > 0.0


@dataclass
class EnsembleDecision:
    """Aggregated decision from all signals."""
    direction: Direction
    aggregate_confidence: float
    weighted_edge_bps: float
    kelly_fraction: float
    recommended_size_usd: float
    contributing_signals: List[SignalOutput]
    regime: str  # "tight", "normal", "wide", "extreme"
    should_trade: bool
    reason: str
    timestamp: float = field(default_factory=time.time)


@dataclass
class RegimeState:
    """Market regime derived from spread and volatility."""
    spread_percentile: float  # 0-100, where 100 = widest
    volatility_1m: float      # 1-minute realized vol
    book_depth_ratio: float   # current depth / avg depth
    label: str                # "tight", "normal", "wide", "extreme"

    @property
    def regime_multiplier(self) -> float:
        """
        Regime-dependent Kelly multiplier.
        
        Tight spreads = consensus = low multiplier (skip or small)
        Normal spreads = standard trading
        Wide spreads = high conviction opportunities
        Extreme = be careful, could be structural
        """
        multipliers = {
            "tight": 1.2,    # Real liquidity = good trading conditions
            "normal": 1.0,
            "wide": 0.5,     # Broken/thin book = risky
            "extreme": 0.3,  # Something weird happening = very cautious
        }
        return multipliers.get(self.label, 1.0)


class SignalRegistry:
    """Registry of all active signals with their weights and performance stats."""

    def __init__(self):
        self._signals: Dict[str, dict] = {}

    def register(self, name: str, weight: float = 1.0, min_confidence: float = 0.3):
        """Register a signal with its voting weight."""
        self._signals[name] = {
            "weight": weight,
            "min_confidence": min_confidence,
            "total_trades": 0,
            "wins": 0,
            "total_pnl_bps": 0.0,
            "last_output": None,
        }
        logger.info(f"Registered signal: {name} (weight={weight})")

    def update_performance(self, name: str, won: bool, pnl_bps: float):
        """Update signal's historical performance for dynamic weight adjustment."""
        if name not in self._signals:
            return
        s = self._signals[name]
        s["total_trades"] += 1
        if won:
            s["wins"] += 1
        s["total_pnl_bps"] += pnl_bps

    def get_weight(self, name: str) -> float:
        """Get signal weight, potentially adjusted by recent performance."""
        if name not in self._signals:
            return 0.0
        s = self._signals[name]

        # Base weight
        base = s["weight"]

        # Performance adjustment (only after 20+ trades)
        if s["total_trades"] >= 20:
            win_rate = s["wins"] / s["total_trades"]
            # Scale weight by how far win rate is from 50%
            # Win rate of 70% → multiplier 1.4
            # Win rate of 40% → multiplier 0.8
            perf_mult = 0.4 + win_rate
            return base * min(perf_mult, 2.0)

        return base

    def get_min_confidence(self, name: str) -> float:
        if name not in self._signals:
            return 0.5
        return self._signals[name]["min_confidence"]

    def get_stats(self) -> Dict[str, dict]:
        """Return performance stats for Brain review."""
        result = {}
        for name, s in self._signals.items():
            wr = s["wins"] / s["total_trades"] if s["total_trades"] > 0 else 0.0
            result[name] = {
                "weight": self.get_weight(name),
                "trades": s["total_trades"],
                "win_rate": round(wr, 3),
                "total_pnl_bps": round(s["total_pnl_bps"], 1),
                "avg_pnl_bps": round(s["total_pnl_bps"] / max(s["total_trades"], 1), 1),
            }
        return result


class EnsembleEngine:
    """
    Combines signals into trading decisions.
    
    The ensemble uses weighted voting where:
    - Each signal votes UP, DOWN, or NEUTRAL
    - Votes are weighted by signal weight × confidence
    - Direction = majority weighted vote
    - Aggregate confidence = agreement ratio × average confidence
    - Kelly fraction = f(edge, confidence, regime)
    """

    def __init__(
        self,
        registry: SignalRegistry,
        bankroll: float = 15.0,
        fractional_kelly: float = 0.25,
        min_aggregate_confidence: float = 0.4,
        min_edge_bps: float = 50.0,
        max_position_pct: float = 0.15,
        min_bet_usd: float = 1.0,
    ):
        self.registry = registry
        self.bankroll = bankroll
        self.fractional_kelly = fractional_kelly
        self.min_aggregate_confidence = min_aggregate_confidence
        self.min_edge_bps = min_edge_bps
        self.max_position_pct = max_position_pct
        self.min_bet_usd = min_bet_usd

    def update_bankroll(self, new_bankroll: float):
        self.bankroll = new_bankroll

    def decide(
        self,
        signals: List[SignalOutput],
        regime: RegimeState,
        current_price_yes: float,
    ) -> EnsembleDecision:
        """
        Core decision function.
        
        Args:
            signals: List of signal outputs from all active signals
            regime: Current market regime
            current_price_yes: Current YES price on Polymarket (0.01 - 0.99)
        
        Returns:
            EnsembleDecision with direction, sizing, and reasoning
        """
        # Filter to actionable signals above their minimum confidence
        actionable = []
        for sig in signals:
            min_conf = self.registry.get_min_confidence(sig.signal_name)
            if sig.is_actionable and sig.confidence >= min_conf:
                actionable.append(sig)

        if not actionable:
            return self._no_trade("No actionable signals", signals, regime)

        # Weighted voting
        up_weight = 0.0
        down_weight = 0.0
        for sig in actionable:
            w = self.registry.get_weight(sig.signal_name) * sig.confidence
            if sig.direction == Direction.UP:
                up_weight += w
            elif sig.direction == Direction.DOWN:
                down_weight += w

        total_weight = up_weight + down_weight
        if total_weight == 0:
            return self._no_trade("Zero total weight", signals, regime)

        # Direction = majority weighted vote
        if up_weight > down_weight:
            direction = Direction.UP
            agreement = up_weight / total_weight
            agreeing = [s for s in actionable if s.direction == Direction.UP]
        elif down_weight > up_weight:
            direction = Direction.DOWN
            agreement = down_weight / total_weight
            agreeing = [s for s in actionable if s.direction == Direction.DOWN]
        else:
            return self._no_trade("Tied vote", signals, regime)

        # Aggregate confidence = agreement × mean confidence of agreeing signals
        mean_conf = sum(s.confidence for s in agreeing) / len(agreeing)
        aggregate_confidence = agreement * mean_conf

        if aggregate_confidence < self.min_aggregate_confidence:
            return self._no_trade(
                f"Aggregate confidence {aggregate_confidence:.2f} < {self.min_aggregate_confidence}",
                signals, regime,
            )

        # Weighted edge estimate (bps)
        total_w = sum(self.registry.get_weight(s.signal_name) for s in agreeing)
        if total_w == 0:
            return self._no_trade("Zero agreeing weight", signals, regime)

        weighted_edge = sum(
            s.estimated_edge_bps * self.registry.get_weight(s.signal_name)
            for s in agreeing
        ) / total_w

        if weighted_edge < self.min_edge_bps:
            return self._no_trade(
                f"Edge {weighted_edge:.0f}bps < {self.min_edge_bps}bps minimum",
                signals, regime,
            )

        # Kelly fraction for binary bet
        # Edge in probability terms: if we think YES at 0.50 should be 0.55,
        # our edge is 0.05 probability points = 500 bps
        edge_probability = weighted_edge / 10000.0

        if direction == Direction.UP:
            p_win = current_price_yes + edge_probability  # We think YES is underpriced
            cost = current_price_yes
        else:
            p_win = (1 - current_price_yes) + edge_probability  # NO is underpriced
            cost = 1 - current_price_yes

        p_win = min(max(p_win, 0.01), 0.99)

        # Binary Kelly: f* = (p*b - q) / b where b = (1/cost - 1), q = 1-p
        b = (1.0 / cost) - 1.0
        if b <= 0:
            return self._no_trade("Invalid odds (cost >= 1.0)", signals, regime)

        q = 1.0 - p_win
        kelly_raw = (p_win * b - q) / b

        if kelly_raw <= 0:
            return self._no_trade(
                f"Negative Kelly: p_win={p_win:.3f}, cost={cost:.3f}",
                signals, regime,
            )

        # Apply fractional Kelly + regime multiplier
        kelly_adj = kelly_raw * self.fractional_kelly * regime.regime_multiplier
        kelly_adj = min(kelly_adj, self.max_position_pct)

        # Convert to USD
        size_usd = kelly_adj * self.bankroll
        size_usd = max(size_usd, 0)

        if size_usd < self.min_bet_usd:
            return self._no_trade(
                f"Size ${size_usd:.2f} < minimum ${self.min_bet_usd}",
                signals, regime,
            )

        # Round to reasonable precision
        size_usd = round(size_usd, 2)

        return EnsembleDecision(
            direction=direction,
            aggregate_confidence=round(aggregate_confidence, 3),
            weighted_edge_bps=round(weighted_edge, 1),
            kelly_fraction=round(kelly_adj, 4),
            recommended_size_usd=size_usd,
            contributing_signals=agreeing,
            regime=regime.label,
            should_trade=True,
            reason=self._build_reason(agreeing, aggregate_confidence, weighted_edge, regime),
        )

    def _no_trade(self, reason: str, signals: List[SignalOutput], regime: RegimeState) -> EnsembleDecision:
        return EnsembleDecision(
            direction=Direction.NEUTRAL,
            aggregate_confidence=0.0,
            weighted_edge_bps=0.0,
            kelly_fraction=0.0,
            recommended_size_usd=0.0,
            contributing_signals=[],
            regime=regime.label,
            should_trade=False,
            reason=reason,
        )

    def _build_reason(
        self,
        agreeing: List[SignalOutput],
        confidence: float,
        edge: float,
        regime: RegimeState,
    ) -> str:
        signal_names = ", ".join(s.signal_name for s in agreeing)
        return (
            f"TRADE: {len(agreeing)} signals agree ({signal_names}), "
            f"confidence={confidence:.2f}, edge={edge:.0f}bps, "
            f"regime={regime.label}(x{regime.regime_multiplier})"
        )
