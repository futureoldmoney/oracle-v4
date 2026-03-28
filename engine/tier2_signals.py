"""
Tier 2 Signals: Independent Edge Sources
=========================================
These signals generate standalone trade ideas uncorrelated with oracle.
They diversify the portfolio's return stream.

Signal 4: Liquidity Vacuum Detection
Signal 5: Executable Complement Arbitrage
Signal 6: Whale Flow Mirroring
Signal 7: Enhanced Order Book Imbalance
"""

import time
import math
import logging
from dataclasses import dataclass, field
from typing import Optional, Deque, Dict, List, Tuple
from collections import deque, defaultdict

from ensemble_engine import SignalOutput, Direction
from tier1_signals import OrderBookSnapshot, BookLevel, PriceTick

logger = logging.getLogger("oracle.tier2")


# ═══════════════════════════════════════════════════════════════
# SIGNAL 4: LIQUIDITY VACUUM DETECTION
# ═══════════════════════════════════════════════════════════════

class LiquidityVacuumDetector:
    """
    Detects when one side of the order book thins out suddenly.
    
    When informed traders are about to push a market directionally,
    they first pull their resting orders from the side they're about
    to hit. This creates a "vacuum" — a sudden drop in depth on one
    side while the other remains stable.
    
    How it works:
    1. Track rolling average of bid depth and ask depth (top 5 levels)
    2. Compute vacuum_ratio = current_depth / rolling_avg_depth per side
    3. If one side's ratio drops below threshold while the other stays stable,
       predict direction toward the thinning side
    
    Signal: Ask vacuum (asks thin out) → price going UP (buy YES)
            Bid vacuum (bids thin out) → price going DOWN (buy NO)
    """

    def __init__(
        self,
        window_size: int = 60,        # ~5 min at 5-second polling
        vacuum_threshold: float = 0.5, # Side drops to 50% of average
        stability_threshold: float = 0.7,  # Other side must be >70% of avg
        levels: int = 5,               # Top N book levels to consider
        confidence_base: float = 0.4,
        edge_estimate_bps: float = 150.0,
    ):
        self.window_size = window_size
        self.vacuum_threshold = vacuum_threshold
        self.stability_threshold = stability_threshold
        self.levels = levels
        self.confidence_base = confidence_base
        self.edge_estimate_bps = edge_estimate_bps

        self._bid_depth_history: Deque[float] = deque(maxlen=window_size)
        self._ask_depth_history: Deque[float] = deque(maxlen=window_size)

    def ingest_book(self, book: OrderBookSnapshot):
        """Feed order book snapshot."""
        bid_depth = book.depth("bid", self.levels)
        ask_depth = book.depth("ask", self.levels)
        self._bid_depth_history.append(bid_depth)
        self._ask_depth_history.append(ask_depth)

    def evaluate(self) -> SignalOutput:
        """Detect vacuum conditions."""
        if len(self._bid_depth_history) < 10:
            return self._neutral("Insufficient depth history")

        avg_bid = sum(self._bid_depth_history) / len(self._bid_depth_history)
        avg_ask = sum(self._ask_depth_history) / len(self._ask_depth_history)

        if avg_bid == 0 or avg_ask == 0:
            return self._neutral("Zero average depth")

        # Minimum depth guard: don't fire on empty/thin books
        if avg_bid < 100 or avg_ask < 100:
            return self._neutral(f"Insufficient depth: avg_bid={avg_bid:.0f}, avg_ask={avg_ask:.0f}")

        current_bid = self._bid_depth_history[-1]
        current_ask = self._ask_depth_history[-1]

        # Guard: current depth of exactly 0 when history shows non-zero almost always
        # means the CLOB feed stopped delivering that side, not a real vacuum.
        # A genuine vacuum is a *drop* in depth, not a permanent zero.
        if current_ask == 0 and avg_ask > 0:
            return self._neutral(
                f"Ask depth zero (avg={avg_ask:.0f}) — dead feed, not a vacuum"
            )
        if current_bid == 0 and avg_bid > 0:
            return self._neutral(
                f"Bid depth zero (avg={avg_bid:.0f}) — dead feed, not a vacuum"
            )

        bid_ratio = current_bid / avg_bid
        ask_ratio = current_ask / avg_ask

        # Ask vacuum: asks thin, bids stable → UP signal
        if ask_ratio < self.vacuum_threshold and bid_ratio > self.stability_threshold:
            vacuum_severity = 1.0 - ask_ratio  # 0 to 1, higher = more severe
            confidence = self.confidence_base + vacuum_severity * 0.4
            confidence = min(confidence, 0.85)
            edge = self.edge_estimate_bps * (1 + vacuum_severity)

            return SignalOutput(
                signal_name="LIQUIDITY_VACUUM",
                direction=Direction.UP,
                confidence=round(confidence, 3),
                estimated_edge_bps=round(edge, 1),
                magnitude=round(vacuum_severity, 3),
                metadata={
                    "vacuum_side": "ask",
                    "ask_ratio": round(ask_ratio, 3),
                    "bid_ratio": round(bid_ratio, 3),
                    "avg_ask_depth": round(avg_ask, 1),
                    "current_ask_depth": round(current_ask, 1),
                },
            )

        # Bid vacuum: bids thin, asks stable → DOWN signal
        if bid_ratio < self.vacuum_threshold and ask_ratio > self.stability_threshold:
            vacuum_severity = 1.0 - bid_ratio
            confidence = self.confidence_base + vacuum_severity * 0.4
            confidence = min(confidence, 0.85)
            edge = self.edge_estimate_bps * (1 + vacuum_severity)

            return SignalOutput(
                signal_name="LIQUIDITY_VACUUM",
                direction=Direction.DOWN,
                confidence=round(confidence, 3),
                estimated_edge_bps=round(edge, 1),
                magnitude=round(vacuum_severity, 3),
                metadata={
                    "vacuum_side": "bid",
                    "bid_ratio": round(bid_ratio, 3),
                    "ask_ratio": round(ask_ratio, 3),
                    "avg_bid_depth": round(avg_bid, 1),
                    "current_bid_depth": round(current_bid, 1),
                },
            )

        return self._neutral(
            f"No vacuum: bid_ratio={bid_ratio:.2f}, ask_ratio={ask_ratio:.2f}"
        )

    def _neutral(self, reason: str) -> SignalOutput:
        return SignalOutput(
            signal_name="LIQUIDITY_VACUUM",
            direction=Direction.NEUTRAL,
            confidence=0.0,
            estimated_edge_bps=0.0,
            magnitude=0.0,
            metadata={"skip_reason": reason},
        )


# ═══════════════════════════════════════════════════════════════
# SIGNAL 5: EXECUTABLE COMPLEMENT ARBITRAGE
# ═══════════════════════════════════════════════════════════════

class ExecutableComplementArb:
    """
    Enhanced complement arbitrage using actual fill prices, not midpoints.
    
    Theory: In a binary market, YES + NO = $1.00 (minus fees).
    If we can BUY YES at 0.48 and BUY NO at 0.50, total cost = 0.98.
    We're guaranteed to win $1.00 on one side, locking in $0.02 profit
    minus fees on both sides.
    
    The key improvement over the basic version:
    - Uses full order book depth to compute ACTUAL fill prices at target size
    - Accounts for fees on both legs
    - Computes the maximum profitable size (where the arb closes)
    - Only fires when the executable arb exceeds the fee threshold
    
    This is a RISK-FREE signal: if it fires, the trade has zero directional risk.
    The only risk is execution risk (partial fills, race conditions).
    """

    def __init__(
        self,
        fee_rate_bps: float = 156.0,  # Max 1.56% effective fee at 50¢
        min_profit_bps: float = 20.0,  # Minimum arb profit in bps after fees
        target_size_usd: float = 5.0,  # Size to check arb at
    ):
        self.fee_rate_bps = fee_rate_bps
        self.min_profit_bps = min_profit_bps
        self.target_size_usd = target_size_usd

    def evaluate(
        self,
        yes_book: OrderBookSnapshot,
        no_book: OrderBookSnapshot,
    ) -> SignalOutput:
        """
        Check for executable complement arbitrage.
        
        Args:
            yes_book: Full order book for YES token
            no_book: Full order book for NO token
        """
        if not yes_book.asks or not no_book.asks:
            return self._neutral("Missing ask-side liquidity")

        # Compute executable buy prices for both sides at target size
        # "Buy YES" means walking up YES asks
        # "Buy NO" means walking up NO asks
        yes_fill = yes_book.executable_price("buy", self.target_size_usd)
        no_fill = no_book.executable_price("buy", self.target_size_usd)

        if yes_fill is None or no_fill is None:
            return self._neutral("Insufficient liquidity for target size")

        # Total cost to buy both sides
        total_cost = yes_fill + no_fill

        # Guaranteed payout = $1.00 per unit
        gross_profit_per_unit = 1.00 - total_cost

        # Fee calculation
        # Fees are paid on BOTH legs. Effective fee depends on fill price.
        # At 50¢, fee = 1.56%. At 10¢, fee ≈ 0.36%.
        # Fee formula: fee = price * (1 - price) * fee_rate_bps / 10000 * 2 (both directions)
        # Simplified: total fees ≈ max fee rate since we're buying both sides
        yes_fee = yes_fill * (1 - yes_fill) * self.fee_rate_bps / 10000.0 * 2
        no_fee = no_fill * (1 - no_fill) * self.fee_rate_bps / 10000.0 * 2
        total_fees = yes_fee + no_fee

        net_profit_per_unit = gross_profit_per_unit - total_fees

        if net_profit_per_unit <= 0:
            return self._neutral(
                f"No arb: cost={total_cost:.4f}, fees={total_fees:.4f}, "
                f"net={net_profit_per_unit:.4f}"
            )

        # Convert to bps relative to capital deployed
        profit_bps = (net_profit_per_unit / total_cost) * 10000.0

        if profit_bps < self.min_profit_bps:
            return self._neutral(f"Arb {profit_bps:.1f}bps < minimum {self.min_profit_bps}bps")

        # Find maximum profitable size
        max_size = self._find_max_arb_size(yes_book, no_book)

        # Direction doesn't matter for arb — we buy BOTH sides
        # But for the ensemble, report as UP (buying YES is the primary leg)
        return SignalOutput(
            signal_name="COMPLEMENT_ARB",
            direction=Direction.UP,  # Convention: primary leg is YES
            confidence=min(0.95, 0.5 + profit_bps / 100.0),  # High confidence for arb
            estimated_edge_bps=round(profit_bps, 1),
            magnitude=round(net_profit_per_unit, 4),
            metadata={
                "yes_fill_price": round(yes_fill, 4),
                "no_fill_price": round(no_fill, 4),
                "total_cost": round(total_cost, 4),
                "gross_profit": round(gross_profit_per_unit, 4),
                "total_fees": round(total_fees, 4),
                "net_profit": round(net_profit_per_unit, 4),
                "max_profitable_size": round(max_size, 2),
                "is_arb": True,  # Flag for executor to handle both legs
            },
        )

    def _find_max_arb_size(
        self, yes_book: OrderBookSnapshot, no_book: OrderBookSnapshot
    ) -> float:
        """
        Binary search for the maximum size where the arb is still profitable.
        """
        lo, hi = 0.1, 1000.0
        for _ in range(20):  # 20 iterations ≈ precision of $0.001
            mid = (lo + hi) / 2
            yes_fill = yes_book.executable_price("buy", mid)
            no_fill = no_book.executable_price("buy", mid)

            if yes_fill is None or no_fill is None:
                hi = mid
                continue

            total_cost = yes_fill + no_fill
            yes_fee = yes_fill * (1 - yes_fill) * self.fee_rate_bps / 10000.0 * 2
            no_fee = no_fill * (1 - no_fill) * self.fee_rate_bps / 10000.0 * 2
            net = 1.00 - total_cost - yes_fee - no_fee

            if net > 0:
                lo = mid
            else:
                hi = mid

        return lo

    def _neutral(self, reason: str) -> SignalOutput:
        return SignalOutput(
            signal_name="COMPLEMENT_ARB",
            direction=Direction.NEUTRAL,
            confidence=0.0,
            estimated_edge_bps=0.0,
            magnitude=0.0,
            metadata={"skip_reason": reason},
        )


# ═══════════════════════════════════════════════════════════════
# SIGNAL 6: WHALE FLOW MIRRORING
# ═══════════════════════════════════════════════════════════════

@dataclass
class WalletProfile:
    """Tracked wallet with performance history."""
    address: str
    total_trades: int = 0
    wins: int = 0
    total_volume: float = 0.0
    total_pnl: float = 0.0
    last_trade_timestamp: float = 0.0

    @property
    def win_rate(self) -> float:
        return self.wins / self.total_trades if self.total_trades > 0 else 0.0

    @property
    def avg_size(self) -> float:
        return self.total_volume / self.total_trades if self.total_trades > 0 else 0.0

    @property
    def is_profitable(self) -> bool:
        return self.total_pnl > 0 and self.total_trades >= 10

    @property
    def score(self) -> float:
        """
        Wallet quality score combining win rate, volume, and consistency.
        Higher = more worth following.
        """
        if self.total_trades < 10:
            return 0.0
        wr_component = max(self.win_rate - 0.5, 0) * 2  # 0-1, centered at 50%
        vol_component = min(self.total_volume / 10000.0, 1.0)  # Normalize to $10K
        consistency = min(self.total_trades / 50.0, 1.0)  # More trades = more reliable
        return wr_component * 0.5 + vol_component * 0.2 + consistency * 0.3


class WhaleFlowTracker:
    """
    Tracks large wallets on Polymarket and generates signals when
    historically profitable wallets make large trades on BTC 5-min markets.
    
    Data source: Polymarket Data API /trades?market=
    
    How it works:
    1. Periodically fetch recent trades from the Data API
    2. Build wallet profiles (volume, win rate, P&L)
    3. When a top-scored wallet places a trade >2x their average size,
       generate a mirror signal
    
    Latency: This signal has ~5-30 second latency (API polling delay),
    so it captures the DIRECTION but not the exact price. It's a
    medium-frequency signal, not a latency arb.
    """

    def __init__(
        self,
        min_wallet_score: float = 0.3,
        size_multiplier_threshold: float = 2.0,  # Trade must be >2x avg
        max_wallets_tracked: int = 100,
        confidence_base: float = 0.35,
        edge_estimate_bps: float = 80.0,
    ):
        self.min_wallet_score = min_wallet_score
        self.size_multiplier_threshold = size_multiplier_threshold
        self.max_wallets_tracked = max_wallets_tracked
        self.confidence_base = confidence_base
        self.edge_estimate_bps = edge_estimate_bps

        self._wallets: Dict[str, WalletProfile] = {}
        self._recent_whale_trades: Deque[dict] = deque(maxlen=50)
        self._processed_tx_hashes: set = set()

    def ingest_trade(self, trade: Dict):
        """
        Feed a trade from the Data API.
        
        Expected fields:
        - proxyWallet: str
        - side: "BUY" | "SELL"
        - size: float
        - price: float
        - timestamp: int
        - transactionHash: str
        - outcome: "YES" | "NO"
        """
        tx_hash = trade.get("transactionHash", "")
        if tx_hash in self._processed_tx_hashes:
            return
        self._processed_tx_hashes.add(tx_hash)

        # Trim processed set to prevent memory growth
        if len(self._processed_tx_hashes) > 10000:
            # Remove oldest half (order isn't guaranteed but good enough)
            to_remove = list(self._processed_tx_hashes)[:5000]
            for h in to_remove:
                self._processed_tx_hashes.discard(h)

        wallet = trade.get("proxyWallet", "")
        if not wallet:
            return

        size = float(trade.get("size", 0))
        price = float(trade.get("price", 0))
        usd_size = size * price

        # Update wallet profile
        if wallet not in self._wallets:
            self._wallets[wallet] = WalletProfile(address=wallet)

        profile = self._wallets[wallet]
        profile.total_trades += 1
        profile.total_volume += usd_size
        profile.last_trade_timestamp = float(trade.get("timestamp", time.time()))

        # Check if this is a whale trade
        if profile.score >= self.min_wallet_score:
            if profile.avg_size > 0 and usd_size > self.size_multiplier_threshold * profile.avg_size:
                self._recent_whale_trades.append(trade)

    def update_wallet_outcomes(self, wallet: str, won: bool, pnl: float):
        """Update wallet with trade outcome (call after market resolution)."""
        if wallet in self._wallets:
            self._wallets[wallet].total_pnl += pnl
            if won:
                self._wallets[wallet].wins += 1

    def evaluate(self, market_condition_id: str) -> SignalOutput:
        """
        Check for recent whale trades on the specified market.
        
        Returns a signal if a top-scored wallet made a large trade
        in the last 30 seconds.
        """
        now = time.time()
        recent_cutoff = now - 30  # Last 30 seconds

        # Filter whale trades for this market and time window
        relevant = [
            t for t in self._recent_whale_trades
            if (t.get("conditionId") == market_condition_id
                and float(t.get("timestamp", 0)) > recent_cutoff)
        ]

        if not relevant:
            return self._neutral("No recent whale trades on this market")

        # Aggregate: are whales net buying YES or NO?
        yes_volume = 0.0
        no_volume = 0.0
        max_score = 0.0

        for trade in relevant:
            wallet = trade.get("proxyWallet", "")
            profile = self._wallets.get(wallet)
            if not profile:
                continue

            size = float(trade.get("size", 0)) * float(trade.get("price", 0))
            side = trade.get("side", "")
            outcome = trade.get("outcome", "")

            # Determine if this is a YES or NO position
            if (side == "BUY" and outcome == "YES") or (side == "SELL" and outcome == "NO"):
                yes_volume += size * profile.score
            else:
                no_volume += size * profile.score

            max_score = max(max_score, profile.score)

        net_flow = yes_volume - no_volume
        total_flow = yes_volume + no_volume

        if total_flow == 0:
            return self._neutral("Zero total whale flow")

        # Direction = net flow direction
        if abs(net_flow) / total_flow < 0.3:
            return self._neutral("Whale flow is mixed (no clear direction)")

        direction = Direction.UP if net_flow > 0 else Direction.DOWN

        # Confidence scales with flow concentration and wallet quality
        concentration = abs(net_flow) / total_flow
        confidence = self.confidence_base + concentration * 0.3 + max_score * 0.2
        confidence = min(confidence, 0.80)

        edge = self.edge_estimate_bps * (1 + max_score)

        return SignalOutput(
            signal_name="WHALE_FLOW",
            direction=direction,
            confidence=round(confidence, 3),
            estimated_edge_bps=round(edge, 1),
            magnitude=round(abs(net_flow), 2),
            metadata={
                "yes_volume_weighted": round(yes_volume, 2),
                "no_volume_weighted": round(no_volume, 2),
                "whale_count": len(relevant),
                "max_wallet_score": round(max_score, 3),
                "concentration": round(concentration, 3),
            },
        )

    def get_top_wallets(self, n: int = 20) -> List[Dict]:
        """Return top N wallets by score for Brain review."""
        ranked = sorted(
            self._wallets.values(),
            key=lambda w: w.score,
            reverse=True,
        )[:n]
        return [
            {
                "address": w.address[:10] + "...",
                "score": round(w.score, 3),
                "win_rate": round(w.win_rate, 3),
                "trades": w.total_trades,
                "total_volume": round(w.total_volume, 2),
                "total_pnl": round(w.total_pnl, 2),
            }
            for w in ranked
        ]

    def _neutral(self, reason: str) -> SignalOutput:
        return SignalOutput(
            signal_name="WHALE_FLOW",
            direction=Direction.NEUTRAL,
            confidence=0.0,
            estimated_edge_bps=0.0,
            magnitude=0.0,
            metadata={"skip_reason": reason},
        )


# ═══════════════════════════════════════════════════════════════
# SIGNAL 7: ENHANCED ORDER BOOK IMBALANCE
# ═══════════════════════════════════════════════════════════════

class EnhancedOBImbalance:
    """
    Depth-weighted order book imbalance with momentum confirmation.
    
    Enhancement over basic OB_PRESSURE_DIVERGENCE:
    1. Price-weighted depth (closer levels count more)
    2. Rate-of-change detection (imbalance building vs static)
    3. Volume normalization across different market tick sizes
    
    Metric: imbalance = weighted_bid_depth / weighted_ask_depth
    - > 1.3 → bullish (buyers dominating) → UP
    - < 0.7 → bearish (sellers dominating) → DOWN
    
    Enhancement: track imbalance trajectory. A RISING imbalance from
    1.0 to 1.4 over 30 seconds is more predictive than a static 1.4.
    """

    def __init__(
        self,
        levels: int = 5,
        bullish_threshold: float = 1.3,
        bearish_threshold: float = 0.7,
        window_size: int = 60,  # ~5min at 5s polling
        momentum_lookback: int = 6,  # 30 seconds
        confidence_base: float = 0.35,
        edge_estimate_bps: float = 80.0,
    ):
        self.levels = levels
        self.bullish_threshold = bullish_threshold
        self.bearish_threshold = bearish_threshold
        self.momentum_lookback = momentum_lookback
        self.confidence_base = confidence_base
        self.edge_estimate_bps = edge_estimate_bps

        self._imbalance_history: Deque[float] = deque(maxlen=window_size)
        self._last_bid_wd: float = 0.0
        self._last_ask_wd: float = 0.0

    def ingest_book(self, book: OrderBookSnapshot):
        """Feed order book snapshots."""
        bid_wd = book.weighted_depth("bid", self.levels)
        ask_wd = book.weighted_depth("ask", self.levels)
        self._last_bid_wd = bid_wd
        self._last_ask_wd = ask_wd

        if ask_wd > 0:
            imbalance = bid_wd / ask_wd
        else:
            imbalance = 10.0  # Extremely bullish if no asks

        self._imbalance_history.append(imbalance)

    def evaluate(self) -> SignalOutput:
        """Check for significant order book imbalance."""
        if len(self._imbalance_history) < self.momentum_lookback + 1:
            return self._neutral("Insufficient imbalance history")

        # Minimum depth guard: don't fire on empty/thin books
        if min(self._last_bid_wd, self._last_ask_wd) < 50:
            return self._neutral(f"Insufficient depth: bid_wd={self._last_bid_wd:.0f}, ask_wd={self._last_ask_wd:.0f}")

        current = self._imbalance_history[-1]

        # Momentum: how is imbalance trending?
        lookback = list(self._imbalance_history)[-self.momentum_lookback:]
        momentum = (lookback[-1] - lookback[0]) / max(lookback[0], 0.01)

        # Bullish: imbalance > threshold AND momentum is positive (building)
        if current > self.bullish_threshold:
            severity = (current - 1.0) / 1.0  # How far above 1.0
            momentum_bonus = max(momentum, 0) * 0.3
            confidence = self.confidence_base + severity * 0.3 + momentum_bonus
            confidence = min(confidence, 0.80)

            edge = self.edge_estimate_bps * (1 + severity * 0.5)

            return SignalOutput(
                signal_name="OB_IMBALANCE",
                direction=Direction.UP,
                confidence=round(confidence, 3),
                estimated_edge_bps=round(edge, 1),
                magnitude=round(current, 3),
                metadata={
                    "imbalance": round(current, 3),
                    "momentum": round(momentum, 4),
                    "lookback_start": round(lookback[0], 3),
                    "lookback_end": round(lookback[-1], 3),
                },
            )

        # Bearish: imbalance < threshold AND momentum is negative (selling)
        if current < self.bearish_threshold:
            severity = (1.0 - current) / 1.0
            momentum_bonus = max(-momentum, 0) * 0.3
            confidence = self.confidence_base + severity * 0.3 + momentum_bonus
            confidence = min(confidence, 0.80)

            edge = self.edge_estimate_bps * (1 + severity * 0.5)

            return SignalOutput(
                signal_name="OB_IMBALANCE",
                direction=Direction.DOWN,
                confidence=round(confidence, 3),
                estimated_edge_bps=round(edge, 1),
                magnitude=round(current, 3),
                metadata={
                    "imbalance": round(current, 3),
                    "momentum": round(momentum, 4),
                    "lookback_start": round(lookback[0], 3),
                    "lookback_end": round(lookback[-1], 3),
                },
            )

        return self._neutral(
            f"Imbalance {current:.2f} within neutral range "
            f"[{self.bearish_threshold}, {self.bullish_threshold}]"
        )

    def _neutral(self, reason: str) -> SignalOutput:
        return SignalOutput(
            signal_name="OB_IMBALANCE",
            direction=Direction.NEUTRAL,
            confidence=0.0,
            estimated_edge_bps=0.0,
            magnitude=0.0,
            metadata={"skip_reason": reason},
        )
