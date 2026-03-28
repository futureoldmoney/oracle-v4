"""
Multi-Asset Multi-Timeframe Market Scanner
============================================
Scans ALL crypto prediction markets on Polymarket across:
- 7 assets: BTC, ETH, SOL, XRP, DOGE, HYPE, BNB
- 4 timeframes: 5-min, 15-min, 1-hour, 4-hour
- Range markets: "Bitcoin above ___" style

This replaces the single-market scanner from ws_connections.py.

Key insight: When BTC moves on Binance, ALL correlated assets across 
ALL timeframes are simultaneously mispriced. One oracle signal can 
generate 10-28 simultaneous trade opportunities.

Cross-Asset Correlation:
  BTC moves → ETH, SOL, XRP, DOGE, BNB all follow with lag
  The lag on Polymarket is even longer than on CEXes
  We track the Binance correlation matrix in real-time and exploit
  the propagation delay on Polymarket
"""

import time
import math
import logging
from typing import Optional, Dict, List, Tuple, Set
from dataclasses import dataclass, field
from collections import deque, defaultdict
from enum import Enum

logger = logging.getLogger("oracle.multi_asset")


# ═══════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════

class Asset(Enum):
    BTC = "btc"
    ETH = "eth"
    SOL = "sol"
    XRP = "xrp"
    DOGE = "doge"
    HYPE = "hype"
    BNB = "bnb"

class Timeframe(Enum):
    MIN_5 = "5min"
    MIN_15 = "15min"
    HOUR_1 = "1hour"
    HOUR_4 = "4hour"

# Binance symbol mapping
BINANCE_SYMBOLS = {
    Asset.BTC: "btcusdt",
    Asset.ETH: "ethusdt",
    Asset.SOL: "solusdt",
    Asset.XRP: "xrpusdt",
    Asset.DOGE: "dogeusdt",
    Asset.HYPE: "hypeusdt",  # May not be on Binance — fallback to other CEX
    Asset.BNB: "bnbusdt",
}

# Chainlink symbol mapping
CHAINLINK_SYMBOLS = {
    Asset.BTC: "btc/usd",
    Asset.ETH: "eth/usd",
    Asset.SOL: "sol/usd",
    Asset.XRP: "xrp/usd",
    Asset.DOGE: "doge/usd",
    # HYPE may not have Chainlink feed
    Asset.BNB: "bnb/usd",
}

# Keywords for matching Gamma API market names to assets
ASSET_KEYWORDS = {
    Asset.BTC: ["bitcoin", "btc"],
    Asset.ETH: ["ethereum", "eth"],
    Asset.SOL: ["solana", "sol"],
    Asset.XRP: ["xrp"],
    Asset.DOGE: ["dogecoin", "doge"],
    Asset.HYPE: ["hype", "hyperliquid"],
    Asset.BNB: ["bnb"],
}

TIMEFRAME_KEYWORDS = {
    Timeframe.MIN_5: ["5 min", "5-min", "5-minute"],
    Timeframe.MIN_15: ["15 min", "15-min", "15-minute"],
    Timeframe.HOUR_1: ["1 hour", "1-hour"],
    Timeframe.HOUR_4: ["4 hour", "4-hour", "4 hours"],
}

GAMMA_API_BASE = "https://gamma-api.polymarket.com"


# ═══════════════════════════════════════════════════════════════
# MARKET ENTRY
# ═══════════════════════════════════════════════════════════════

@dataclass
class MarketEntry:
    """A single tradeable market on Polymarket."""
    condition_id: str
    question: str
    asset: Asset
    timeframe: Timeframe
    yes_token_id: str
    no_token_id: str
    yes_price: float
    no_price: float
    end_date_iso: str
    volume24hr: float
    liquidity: float
    spread: float
    tick_size: str
    neg_risk: bool
    is_range_market: bool = False  # "above ___" style
    range_value: Optional[float] = None  # The price threshold for range markets
    seconds_until_close: Optional[int] = None

    @property
    def market_key(self) -> str:
        """Unique key for this market: asset_timeframe"""
        return f"{self.asset.value}_{self.timeframe.value}"

    @property
    def is_near_expiry(self) -> bool:
        """Market is within 2 minutes of settlement."""
        if self.seconds_until_close is not None:
            return self.seconds_until_close < 120
        return False

    @property
    def is_tradeable(self) -> bool:
        """Basic tradeability check."""
        if self.is_near_expiry:
            return False
        if self.yes_price <= 0.02 or self.yes_price >= 0.98:
            return False  # Already settled in practice
        return True


# ═══════════════════════════════════════════════════════════════
# MULTI-ASSET SCANNER
# ═══════════════════════════════════════════════════════════════

class MultiAssetScanner:
    """
    Scans Polymarket for ALL active crypto Up/Down markets.
    
    Returns a structured map:
      {(Asset.BTC, Timeframe.MIN_5): MarketEntry, ...}
    
    Also detects range markets ("Bitcoin above ___") for arb opportunities.
    
    Usage:
        scanner = MultiAssetScanner()
        markets = await scanner.scan()
        
        # Get all BTC markets
        btc_markets = scanner.get_markets_for_asset(Asset.BTC)
        
        # Get all 5-min markets
        five_min = scanner.get_markets_for_timeframe(Timeframe.MIN_5)
        
        # Get everything
        all_markets = scanner.all_tradeable
    """

    def __init__(self, scan_interval: float = 15.0):
        self._scan_interval = scan_interval
        self._markets: Dict[Tuple[Asset, Timeframe], MarketEntry] = {}
        self._range_markets: List[MarketEntry] = []
        self._all_markets: List[MarketEntry] = []
        self._last_scan_time = 0.0
        self._scan_count = 0

    async def scan(self) -> Dict[Tuple[Asset, Timeframe], MarketEntry]:
        """Scan Gamma API for all active crypto markets."""
        try:
            import httpx
        except ImportError:
            return self._markets

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                # Fetch all active crypto markets
                all_markets = []
                for tag in ["crypto"]:
                    resp = await client.get(
                        f"{GAMMA_API_BASE}/markets",
                        params={
                            "active": "true",
                            "closed": "false",
                            "tag": tag,
                            "limit": 100,
                        }
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    markets = data if isinstance(data, list) else data.get("data", [])
                    all_markets.extend(markets)

                # Parse and classify
                self._markets.clear()
                self._range_markets.clear()
                self._all_markets.clear()

                for m in all_markets:
                    entry = self._parse_market(m)
                    if entry is None:
                        continue

                    self._all_markets.append(entry)

                    if entry.is_range_market:
                        self._range_markets.append(entry)
                    else:
                        key = (entry.asset, entry.timeframe)
                        # Keep the one with higher liquidity if duplicate
                        if key not in self._markets or entry.liquidity > self._markets[key].liquidity:
                            self._markets[key] = entry

                self._last_scan_time = time.time()
                self._scan_count += 1

                logger.info(
                    f"MultiAsset Scanner: {len(self._markets)} up/down markets, "
                    f"{len(self._range_markets)} range markets across "
                    f"{len(set(e.asset for e in self._all_markets))} assets"
                )

                return self._markets

        except Exception as e:
            logger.error(f"MultiAsset Scanner: Scan failed: {e}")
            return self._markets

    def _parse_market(self, raw: dict) -> Optional[MarketEntry]:
        """Parse a Gamma API market response into a MarketEntry."""
        question = (raw.get("question", "") or "").lower()
        if not question:
            return None

        # Detect asset
        asset = None
        for a, keywords in ASSET_KEYWORDS.items():
            if any(kw in question for kw in keywords):
                asset = a
                break
        if asset is None:
            return None

        # Detect if this is a range market ("above ___")
        is_range = "above" in question
        range_value = None
        if is_range:
            # Extract the price threshold (e.g., "Bitcoin above 69,800")
            import re
            match = re.search(r'above\s+[\$]?([\d,]+)', question)
            if match:
                range_value = float(match.group(1).replace(",", ""))

        # Detect timeframe
        timeframe = None
        for tf, keywords in TIMEFRAME_KEYWORDS.items():
            if any(kw in question for kw in keywords):
                timeframe = tf
                break

        if timeframe is None and not is_range:
            # Try to infer from "Up or Down" format with date
            if "up or down" in question:
                # Check for date-based markets (1hr, 4hr have dates like "March 25, 11AM")
                if "am et" in question or "pm et" in question:
                    # Could be 1hr or 4hr — need more context
                    # Default to 1hr if we can't tell
                    timeframe = Timeframe.HOUR_1
                else:
                    return None  # Can't determine timeframe
            else:
                if not is_range:
                    return None

        if timeframe is None and not is_range:
            return None

        # For range markets, default to 1-hour timeframe
        if is_range and timeframe is None:
            timeframe = Timeframe.HOUR_1

        # Extract tokens
        tokens = raw.get("tokens", [])
        yes_token = next((t for t in tokens if t.get("outcome") == "Yes"), None)
        no_token = next((t for t in tokens if t.get("outcome") == "No"), None)
        if not yes_token or not no_token:
            return None

        return MarketEntry(
            condition_id=raw.get("conditionId", ""),
            question=raw.get("question", ""),
            asset=asset,
            timeframe=timeframe,
            yes_token_id=yes_token.get("token_id", ""),
            no_token_id=no_token.get("token_id", ""),
            yes_price=float(yes_token.get("price", 0.5)),
            no_price=float(no_token.get("price", 0.5)),
            end_date_iso=raw.get("endDateIso", ""),
            volume24hr=float(raw.get("volume24hr", 0)),
            liquidity=float(raw.get("liquidityNum", 0)),
            spread=float(raw.get("spread", 0)),
            tick_size=str(raw.get("orderPriceMinTickSize", "0.01")),
            neg_risk=raw.get("negRisk", False),
            is_range_market=is_range,
            range_value=range_value,
        )

    # ── Query Methods ─────────────────────────────────────

    def get_markets_for_asset(self, asset: Asset) -> List[MarketEntry]:
        """Get all markets for a specific asset across timeframes."""
        return [m for key, m in self._markets.items() if key[0] == asset]

    def get_markets_for_timeframe(self, tf: Timeframe) -> List[MarketEntry]:
        """Get all markets for a specific timeframe across assets."""
        return [m for key, m in self._markets.items() if key[1] == tf]

    def get_market(self, asset: Asset, tf: Timeframe) -> Optional[MarketEntry]:
        """Get a specific asset+timeframe market."""
        return self._markets.get((asset, tf))

    @property
    def all_tradeable(self) -> List[MarketEntry]:
        """All tradeable (non-expired, non-extreme-price) markets."""
        return [m for m in self._all_markets if m.is_tradeable]

    @property
    def range_markets(self) -> List[MarketEntry]:
        return self._range_markets

    def get_all_token_ids(self) -> List[str]:
        """Get all YES + NO token IDs for WebSocket subscription."""
        ids = []
        for m in self._all_markets:
            ids.append(m.yes_token_id)
            ids.append(m.no_token_id)
        return ids

    def get_subscription_symbols(self) -> Tuple[List[str], List[str]]:
        """
        Get Binance and Chainlink symbols to subscribe to.
        Returns: (binance_symbols, chainlink_symbols)
        """
        active_assets = set(m.asset for m in self._all_markets)
        binance = [BINANCE_SYMBOLS[a] for a in active_assets if a in BINANCE_SYMBOLS]
        chainlink = [CHAINLINK_SYMBOLS[a] for a in active_assets if a in CHAINLINK_SYMBOLS]
        return binance, chainlink

    @property
    def health(self) -> Dict:
        staleness = time.time() - self._last_scan_time if self._last_scan_time else float('inf')
        assets = set(m.asset.value for m in self._all_markets)
        timeframes = set(m.timeframe.value for m in self._all_markets if not m.is_range_market)
        return {
            "total_markets": len(self._all_markets),
            "updown_markets": len(self._markets),
            "range_markets": len(self._range_markets),
            "tradeable": len(self.all_tradeable),
            "assets": sorted(assets),
            "timeframes": sorted(timeframes),
            "scan_count": self._scan_count,
            "staleness_s": round(staleness, 1),
        }


# ═══════════════════════════════════════════════════════════════
# CROSS-ASSET CORRELATION TRACKER
# ═══════════════════════════════════════════════════════════════

class CrossAssetCorrelation:
    """
    Tracks real-time correlation between crypto assets on Binance.
    
    When BTC spikes +0.3% in 2 seconds:
    - ETH follows with ~0.85 correlation and ~1-3 second lag
    - SOL follows with ~0.75 correlation and ~2-5 second lag
    - XRP, DOGE, BNB follow with ~0.6-0.7 correlation
    
    This means a single BTC oracle signal can generate trades on
    ALL correlated assets where Polymarket hasn't repriced yet.
    
    Signal: CROSS_ASSET_ORACLE
    
    When BTC moves on Binance but ETH's Polymarket price hasn't
    adjusted, buy ETH in the direction of BTC's move (weighted by
    correlation strength and ETH's own recent behavior).
    """

    def __init__(self, window_size: int = 300):  # 5 min of 1-sec ticks
        self._window = window_size
        # Price histories per asset: deque of (timestamp, price)
        self._prices: Dict[Asset, deque] = {
            a: deque(maxlen=window_size) for a in Asset
        }
        # Cached correlations (recomputed periodically)
        self._correlations: Dict[Tuple[Asset, Asset], float] = {}
        self._last_correlation_update = 0.0
        self._correlation_update_interval = 60.0  # Recompute every 60s

    def ingest_price(self, asset: Asset, price: float, timestamp: float):
        """Feed a Binance price tick for any asset."""
        self._prices[asset].append((timestamp, price))

    def get_correlation(self, asset_a: Asset, asset_b: Asset) -> float:
        """Get the current correlation between two assets."""
        now = time.time()
        if now - self._last_correlation_update > self._correlation_update_interval:
            self._update_correlations()

        key = (asset_a, asset_b) if asset_a.value < asset_b.value else (asset_b, asset_a)
        return self._correlations.get(key, 0.0)

    def get_propagation_targets(
        self,
        source_asset: Asset,
        move_pct: float,
        min_correlation: float = 0.5,
    ) -> List[Tuple[Asset, float, float]]:
        """
        Given a price move in source_asset, find correlated assets
        that should follow but haven't yet on Polymarket.
        
        Returns: [(target_asset, correlation, expected_move_pct), ...]
        
        Expected move = source_move * correlation
        """
        targets = []
        for asset in Asset:
            if asset == source_asset:
                continue

            corr = self.get_correlation(source_asset, asset)
            if abs(corr) < min_correlation:
                continue

            expected_move = move_pct * corr
            targets.append((asset, corr, expected_move))

        # Sort by expected move magnitude (most impacted first)
        targets.sort(key=lambda x: abs(x[2]), reverse=True)
        return targets

    def _update_correlations(self):
        """Recompute pairwise correlations from recent returns."""
        self._last_correlation_update = time.time()
        assets = list(Asset)

        # Compute 1-second returns for each asset
        returns = {}
        for asset in assets:
            prices = list(self._prices[asset])
            if len(prices) < 30:
                continue
            rets = []
            for i in range(1, len(prices)):
                if prices[i - 1][1] > 0:
                    r = (prices[i][1] - prices[i - 1][1]) / prices[i - 1][1]
                    rets.append(r)
            if len(rets) >= 20:
                returns[asset] = rets

        # Pairwise Pearson correlation
        for i, a in enumerate(assets):
            for j, b in enumerate(assets):
                if j <= i:
                    continue
                if a not in returns or b not in returns:
                    continue

                # Align to same length
                min_len = min(len(returns[a]), len(returns[b]))
                ra = returns[a][-min_len:]
                rb = returns[b][-min_len:]

                corr = self._pearson(ra, rb)
                key = (a, b) if a.value < b.value else (b, a)
                self._correlations[key] = round(corr, 3)

    @staticmethod
    def _pearson(x: List[float], y: List[float]) -> float:
        n = len(x)
        if n < 5:
            return 0.0
        mean_x = sum(x) / n
        mean_y = sum(y) / n
        cov = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y)) / (n - 1)
        std_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x) / (n - 1))
        std_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y) / (n - 1))
        if std_x == 0 or std_y == 0:
            return 0.0
        return cov / (std_x * std_y)

    def get_correlation_matrix(self) -> Dict[str, Dict[str, float]]:
        """Return full correlation matrix for diagnostics."""
        matrix = {}
        for (a, b), corr in self._correlations.items():
            if a.value not in matrix:
                matrix[a.value] = {}
            if b.value not in matrix:
                matrix[b.value] = {}
            matrix[a.value][b.value] = corr
            matrix[b.value][a.value] = corr
        return matrix


# ═══════════════════════════════════════════════════════════════
# CROSS-ASSET SIGNAL
# ═══════════════════════════════════════════════════════════════

from ensemble_engine import SignalOutput, Direction

class CrossAssetOracleSignal:
    """
    Signal: When asset A moves on Binance, trade correlated asset B
    on Polymarket before B's market reprices.
    
    This is the multi-asset extension of the oracle frontrunning strategy.
    
    How it works:
    1. BTC spikes +0.3% on Binance in 2 seconds
    2. ETH correlation with BTC = 0.85
    3. Expected ETH move = +0.255%
    4. ETH 5-min market on Polymarket still shows 52% Up (hasn't moved)
    5. Signal: BUY ETH UP with confidence proportional to:
       - BTC move magnitude
       - BTC-ETH correlation strength  
       - ETH's Polymarket staleness
    
    6. Repeat for SOL, XRP, DOGE, BNB across ALL timeframes
    
    One BTC spike → potentially 6 assets × 4 timeframes = 24 signals
    """

    def __init__(
        self,
        correlation_tracker: CrossAssetCorrelation,
        min_source_move_pct: float = 0.10,
        min_correlation: float = 0.50,
        confidence_base: float = 0.35,
    ):
        self._correlations = correlation_tracker
        self._min_move = min_source_move_pct
        self._min_corr = min_correlation
        self._confidence_base = confidence_base

    def evaluate(
        self,
        source_asset: Asset,
        source_move_pct: float,
        move_duration_seconds: float,
        target_asset: Asset,
        target_polymarket_midpoint: float,
        target_book_staleness_ms: float,
    ) -> SignalOutput:
        """
        Evaluate cross-asset oracle signal for a specific target.
        
        Args:
            source_asset: The asset that moved (e.g., BTC)
            source_move_pct: How much it moved (%)
            move_duration_seconds: How fast
            target_asset: The asset to trade (e.g., ETH)
            target_polymarket_midpoint: Current Polymarket YES price for target
            target_book_staleness_ms: How stale the target's book is
        """
        abs_move = abs(source_move_pct)
        if abs_move < self._min_move:
            return self._neutral(f"Source move {abs_move:.3f}% < threshold")

        # Get correlation
        corr = self._correlations.get_correlation(source_asset, target_asset)
        if abs(corr) < self._min_corr:
            return self._neutral(
                f"{source_asset.value}-{target_asset.value} correlation {corr:.2f} < minimum"
            )

        # Expected target move
        expected_move = source_move_pct * corr

        # Direction
        direction = Direction.UP if expected_move > 0 else Direction.DOWN

        # Check if Polymarket has already repriced
        # If target midpoint already moved in the expected direction, edge is reduced
        midpoint_offset = target_polymarket_midpoint - 0.50
        already_repriced = (midpoint_offset > 0 and expected_move > 0) or \
                          (midpoint_offset < 0 and expected_move < 0)

        repricing_factor = 1.0
        if already_repriced:
            repricing_factor = max(0.0, 1.0 - abs(midpoint_offset) / 0.05)

        # Confidence
        speed_factor = min(abs_move / math.sqrt(max(move_duration_seconds, 0.1)), 2.0)
        staleness_bonus = min(target_book_staleness_ms / 2000.0, 0.3)

        confidence = (
            self._confidence_base
            + abs(corr) * 0.3        # Higher correlation = more confident
            + speed_factor * 0.1      # Faster moves propagate more reliably
            + staleness_bonus          # Staler book = more opportunity
        ) * repricing_factor

        confidence = min(max(confidence, 0.0), 0.90)

        # Edge estimate
        edge_bps = abs(expected_move) * 100 * repricing_factor

        if confidence < 0.20 or edge_bps < 10:
            return self._neutral(
                f"Low signal: conf={confidence:.2f}, edge={edge_bps:.0f}bps"
            )

        return SignalOutput(
            signal_name="CROSS_ASSET_ORACLE",
            direction=direction,
            confidence=round(confidence, 3),
            estimated_edge_bps=round(edge_bps, 1),
            magnitude=round(abs(expected_move), 4),
            metadata={
                "source_asset": source_asset.value,
                "target_asset": target_asset.value,
                "source_move_pct": round(source_move_pct, 4),
                "correlation": round(corr, 3),
                "expected_move_pct": round(expected_move, 4),
                "repricing_factor": round(repricing_factor, 3),
                "target_midpoint": round(target_polymarket_midpoint, 4),
                "book_staleness_ms": round(target_book_staleness_ms, 0),
            },
        )

    def evaluate_all_targets(
        self,
        source_asset: Asset,
        source_move_pct: float,
        move_duration_seconds: float,
        market_data: Dict[Asset, Dict],  # {asset: {"midpoint": 0.52, "staleness_ms": 500}}
    ) -> List[SignalOutput]:
        """
        Evaluate cross-asset signal for ALL correlated targets at once.
        Called when any asset makes a significant move.
        
        Returns list of actionable signals sorted by edge.
        """
        signals = []

        targets = self._correlations.get_propagation_targets(
            source_asset, source_move_pct, self._min_corr,
        )

        for target_asset, corr, expected_move in targets:
            data = market_data.get(target_asset, {})
            midpoint = data.get("midpoint", 0.50)
            staleness = data.get("staleness_ms", 0.0)

            sig = self.evaluate(
                source_asset, source_move_pct, move_duration_seconds,
                target_asset, midpoint, staleness,
            )

            if sig.is_actionable:
                signals.append(sig)

        # Sort by edge (highest first)
        signals.sort(key=lambda s: s.estimated_edge_bps, reverse=True)
        return signals

    def _neutral(self, reason: str) -> SignalOutput:
        return SignalOutput(
            signal_name="CROSS_ASSET_ORACLE",
            direction=Direction.NEUTRAL,
            confidence=0.0,
            estimated_edge_bps=0.0,
            magnitude=0.0,
            metadata={"skip_reason": reason},
        )


# ═══════════════════════════════════════════════════════════════
# MULTI-TIMEFRAME SIGNAL
# ═══════════════════════════════════════════════════════════════

class MultiTimeframeSignal:
    """
    When a 5-min market shows a strong signal for BTC UP,
    the 15-min and 1-hour markets for BTC should also be UP
    (with lower confidence due to longer timeframe noise).
    
    This signal propagates a strong single-timeframe oracle signal
    across other timeframes for the SAME asset.
    
    Key: 5-min is the fastest feedback loop, so 5-min signals
    propagate to 15-min and 1-hour, not the reverse.
    """

    # Confidence decay per timeframe step
    TIMEFRAME_DECAY = {
        Timeframe.MIN_5: 1.0,     # Source — full confidence
        Timeframe.MIN_15: 0.75,   # Slightly lower
        Timeframe.HOUR_1: 0.50,   # Much lower (too many other factors)
        Timeframe.HOUR_4: 0.30,   # Very low (mostly noise at this lag)
    }

    def propagate(
        self,
        source_signal: SignalOutput,
        source_timeframe: Timeframe,
        target_timeframe: Timeframe,
        target_asset: Asset,
    ) -> SignalOutput:
        """
        Propagate a signal from one timeframe to another.
        
        Only propagate from shorter to longer timeframes.
        """
        # Don't propagate backwards (1hr → 5min)
        source_order = list(Timeframe).index(source_timeframe)
        target_order = list(Timeframe).index(target_timeframe)
        if target_order <= source_order:
            return SignalOutput(
                signal_name="MULTI_TIMEFRAME",
                direction=Direction.NEUTRAL,
                confidence=0.0, estimated_edge_bps=0.0, magnitude=0.0,
                metadata={"skip_reason": "Wrong propagation direction"},
            )

        decay = self.TIMEFRAME_DECAY.get(target_timeframe, 0.3)

        return SignalOutput(
            signal_name="MULTI_TIMEFRAME",
            direction=source_signal.direction,
            confidence=round(source_signal.confidence * decay, 3),
            estimated_edge_bps=round(source_signal.estimated_edge_bps * decay, 1),
            magnitude=source_signal.magnitude,
            metadata={
                "source_signal": source_signal.signal_name,
                "source_timeframe": source_timeframe.value,
                "target_timeframe": target_timeframe.value,
                "target_asset": target_asset.value,
                "decay_factor": decay,
            },
        )
