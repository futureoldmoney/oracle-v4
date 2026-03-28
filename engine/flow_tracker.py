"""
Trade Flow Tracker
===================
Polls Polymarket Data API for trade flow direction on BTC 5-min markets.

Phase 2 enhancement — aggregates buy/sell flow to detect whether informed
traders are net long or short. Confirms or contradicts the oracle signal.

Flow direction logic:
  BUY YES (Up) or SELL NO (Down) = UP flow
  BUY NO (Down) or SELL YES (Up) = DOWN flow

Usage:
    tracker = FlowTracker()
    snapshot = await tracker.poll_trades(condition_id)
    direction = tracker.get_flow_signal(snapshot, min_ratio=1.5)
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Set
from collections import deque

logger = logging.getLogger("oracle.flow")


@dataclass
class FlowSnapshot:
    """Aggregated trade flow for a single poll."""
    up_trades: int = 0
    down_trades: int = 0
    up_volume_usd: float = 0.0
    down_volume_usd: float = 0.0
    net_direction: str = "NEUTRAL"    # "UP", "DOWN", or "NEUTRAL"
    flow_ratio: float = 1.0           # up_volume / down_volume (>1 = bullish)
    unique_makers: int = 0
    timestamp: float = field(default_factory=time.time)

    @property
    def total_trades(self) -> int:
        return self.up_trades + self.down_trades

    @property
    def total_volume(self) -> float:
        return self.up_volume_usd + self.down_volume_usd


class FlowTracker:
    """
    Polls Polymarket Data API for trade flow direction.

    Tracks:
    - Buy/sell flow aggregated by direction (UP vs DOWN)
    - Unique maker count per window (competition monitoring)
    - Deduplication by transactionHash

    Args:
        data_api_url: Polymarket Data API base URL
        poll_interval: Minimum seconds between polls per market
        max_trades_per_poll: Max trades to fetch per API call
    """

    def __init__(
        self,
        data_api_url: str = "https://data-api.polymarket.com",
        poll_interval: float = 10.0,
        max_trades_per_poll: int = 100,
    ):
        self._api_url = data_api_url
        self._poll_interval = poll_interval
        self._max_trades = max_trades_per_poll

        # Deduplication
        self._seen_hashes: Set[str] = set()

        # Per-market state
        self._last_poll: Dict[str, float] = {}  # condition_id → last poll timestamp
        self._snapshots: Dict[str, deque] = {}   # condition_id → deque of FlowSnapshot
        self._makers_per_window: Dict[str, Set[str]] = {}  # condition_id → set of maker addresses

    async def poll_trades(self, condition_id: str) -> Optional[FlowSnapshot]:
        """
        Poll Data API for recent trades on a market.

        Rate limited to one poll per poll_interval per market.

        Args:
            condition_id: Market condition ID

        Returns:
            FlowSnapshot with aggregated flow, or None if rate limited / error
        """
        if not condition_id:
            return None

        # Rate limit
        now = time.time()
        last = self._last_poll.get(condition_id, 0.0)
        if now - last < self._poll_interval:
            return self._get_latest_snapshot(condition_id)

        try:
            import httpx
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(
                    f"{self._api_url}/trades",
                    params={
                        "market": condition_id,
                        "limit": self._max_trades,
                    },
                )
                if resp.status_code != 200:
                    logger.debug(f"Flow: HTTP {resp.status_code} for {condition_id[:12]}...")
                    return None

                data = resp.json()
                trades = data if isinstance(data, list) else data.get("data", [])

                self._last_poll[condition_id] = now
                return self._aggregate(condition_id, trades)

        except Exception as e:
            logger.debug(f"Flow: Poll error for {condition_id[:12]}...: {e}")
            return None

    def _aggregate(self, condition_id: str, trades: List[dict]) -> FlowSnapshot:
        """Aggregate trade list into a FlowSnapshot, deduplicating by hash."""
        up_trades = 0
        down_trades = 0
        up_volume = 0.0
        down_volume = 0.0
        makers: Set[str] = set()

        for trade in trades:
            tx_hash = trade.get("transactionHash", "")
            if not tx_hash or tx_hash in self._seen_hashes:
                continue
            self._seen_hashes.add(tx_hash)

            side = trade.get("side", "").upper()       # "BUY" or "SELL"
            outcome = trade.get("outcome", "").upper()  # "YES"/"UP" or "NO"/"DOWN"
            size = float(trade.get("size", 0) or 0)
            price = float(trade.get("price", 0) or 0)
            volume_usd = size * price

            maker = trade.get("maker_address") or trade.get("proxyWallet") or ""
            if maker:
                makers.add(maker)

            # Classify flow direction:
            # BUY YES/UP or SELL NO/DOWN = UP flow
            # BUY NO/DOWN or SELL YES/UP = DOWN flow
            is_up_flow = (
                (side == "BUY" and outcome in ("YES", "UP")) or
                (side == "SELL" and outcome in ("NO", "DOWN"))
            )
            is_down_flow = (
                (side == "BUY" and outcome in ("NO", "DOWN")) or
                (side == "SELL" and outcome in ("YES", "UP"))
            )

            if is_up_flow:
                up_trades += 1
                up_volume += volume_usd
            elif is_down_flow:
                down_trades += 1
                down_volume += volume_usd

        # Compute flow ratio
        if down_volume > 0:
            flow_ratio = up_volume / down_volume
        elif up_volume > 0:
            flow_ratio = 10.0  # All UP, no DOWN
        else:
            flow_ratio = 1.0

        # Net direction
        if flow_ratio > 1.5:
            net_direction = "UP"
        elif flow_ratio < 0.67:  # 1/1.5
            net_direction = "DOWN"
        else:
            net_direction = "NEUTRAL"

        # Track makers for competition monitoring
        if condition_id not in self._makers_per_window:
            self._makers_per_window[condition_id] = set()
        self._makers_per_window[condition_id].update(makers)

        snapshot = FlowSnapshot(
            up_trades=up_trades,
            down_trades=down_trades,
            up_volume_usd=round(up_volume, 2),
            down_volume_usd=round(down_volume, 2),
            net_direction=net_direction,
            flow_ratio=round(flow_ratio, 3),
            unique_makers=len(self._makers_per_window.get(condition_id, set())),
            timestamp=time.time(),
        )

        # Store snapshot
        if condition_id not in self._snapshots:
            self._snapshots[condition_id] = deque(maxlen=30)
        self._snapshots[condition_id].append(snapshot)

        # Trim seen hashes to prevent memory growth
        if len(self._seen_hashes) > 50000:
            self._seen_hashes = set(list(self._seen_hashes)[-25000:])

        return snapshot

    def get_flow_signal(
        self,
        snapshot: Optional[FlowSnapshot],
        min_ratio: float = 1.5,
    ) -> str:
        """
        Classify flow snapshot into directional signal.

        Args:
            snapshot: FlowSnapshot from poll_trades
            min_ratio: Minimum flow ratio to signal direction (default 1.5x)

        Returns:
            "UP", "DOWN", or "NEUTRAL"
        """
        if snapshot is None or snapshot.total_trades == 0:
            return "NEUTRAL"
        if snapshot.flow_ratio >= min_ratio:
            return "UP"
        if snapshot.flow_ratio <= 1.0 / min_ratio:
            return "DOWN"
        return "NEUTRAL"

    def confirms_oracle(
        self,
        oracle_direction: str,
        snapshot: Optional[FlowSnapshot],
        min_ratio: float = 1.5,
    ) -> bool:
        """Check if trade flow confirms the oracle direction."""
        flow_dir = self.get_flow_signal(snapshot, min_ratio)
        return flow_dir == oracle_direction and oracle_direction != "NEUTRAL"

    def contradicts_oracle(
        self,
        oracle_direction: str,
        snapshot: Optional[FlowSnapshot],
        min_ratio: float = 1.5,
    ) -> bool:
        """Check if trade flow contradicts the oracle direction."""
        flow_dir = self.get_flow_signal(snapshot, min_ratio)
        if flow_dir == "NEUTRAL" or oracle_direction == "NEUTRAL":
            return False
        return flow_dir != oracle_direction

    def clear_window(self, condition_id: str):
        """Clear maker tracking for a market (call on window rotation)."""
        self._makers_per_window.pop(condition_id, None)

    def _get_latest_snapshot(self, condition_id: str) -> Optional[FlowSnapshot]:
        """Get the most recent cached snapshot."""
        hist = self._snapshots.get(condition_id)
        if hist and len(hist) > 0:
            snap = hist[-1]
            if time.time() - snap.timestamp < 60:
                return snap
        return None

    def get_diagnostics(self) -> Dict:
        """Return diagnostic data."""
        return {
            "tracked_markets": len(self._snapshots),
            "seen_hashes": len(self._seen_hashes),
            "makers_per_market": {
                cid[:12]: len(makers)
                for cid, makers in self._makers_per_window.items()
            },
        }
