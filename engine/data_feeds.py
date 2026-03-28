"""
Real-Time Data Feed Layer
=========================
Connects to Polymarket's WebSocket feeds and distributes data to all signals.

Feeds:
1. RTDS crypto_prices (Binance) → Binance BTC price ticks
2. RTDS crypto_prices_chainlink → Chainlink oracle price
3. Market WebSocket → Order book updates, trades, price changes

Architecture:
  WebSocket feeds → DataRouter → Signal callbacks

This is the plumbing layer. It doesn't make decisions.
"""

import json
import time
import logging
import threading
from dataclasses import dataclass, field
from typing import Optional, Callable, List, Dict
from collections import deque

logger = logging.getLogger("oracle.datafeed")

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

BINANCE_SUBSCRIBE = {
    "action": "subscribe",
    "subscriptions": [
        {
            "topic": "crypto_prices",
            "type": "update",
            "filters": "btcusdt"
        }
    ]
}

CHAINLINK_SUBSCRIBE = {
    "action": "subscribe",
    "subscriptions": [
        {
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": '{"symbol":"btc/usd"}'
        }
    ]
}


# ═══════════════════════════════════════════════════════════════
# DATA ROUTER
# ═══════════════════════════════════════════════════════════════

@dataclass
class FeedState:
    """Tracks the current state of all data feeds."""
    binance_btc: float = 0.0
    binance_btc_timestamp: float = 0.0
    chainlink_btc: float = 0.0
    chainlink_btc_timestamp: float = 0.0
    last_book_update: float = 0.0
    last_book_hash: str = ""
    feed_latencies: Dict[str, float] = field(default_factory=dict)

    @property
    def oracle_gap_pct(self) -> float:
        """Current gap between Binance and Chainlink prices."""
        if self.binance_btc == 0 or self.chainlink_btc == 0:
            return 0.0
        return ((self.binance_btc - self.chainlink_btc) / self.chainlink_btc) * 100.0

    @property
    def binance_staleness_ms(self) -> float:
        return (time.time() - self.binance_btc_timestamp) * 1000 if self.binance_btc_timestamp else float('inf')

    @property
    def chainlink_staleness_ms(self) -> float:
        return (time.time() - self.chainlink_btc_timestamp) * 1000 if self.chainlink_btc_timestamp else float('inf')


class DataRouter:
    """
    Central data distribution hub.
    
    Receives raw WebSocket messages and routes parsed data
    to registered signal callbacks.
    
    Usage:
        router = DataRouter()
        router.on_binance_tick(my_signal.ingest_binance)
        router.on_book_update(my_signal.ingest_book)
        
        # In WebSocket handler:
        router.handle_rtds_message(raw_json)
        router.handle_market_message(raw_json)
    """

    def __init__(self):
        self.state = FeedState()

        self._binance_callbacks: List[Callable] = []
        self._chainlink_callbacks: List[Callable] = []
        self._book_callbacks: List[Callable] = []
        self._trade_callbacks: List[Callable] = []

        # Price history for move detection
        self._binance_prices: deque = deque(maxlen=600)  # ~10 min at 1/sec
        self._chainlink_prices: deque = deque(maxlen=120)

        # Latency tracking
        self._msg_count = 0
        self._last_stats_time = time.time()

    # ── Callback Registration ─────────────────────────────

    def on_binance_tick(self, callback: Callable):
        """Register callback for Binance BTC price ticks."""
        self._binance_callbacks.append(callback)

    def on_chainlink_tick(self, callback: Callable):
        """Register callback for Chainlink BTC price ticks."""
        self._chainlink_callbacks.append(callback)

    def on_book_update(self, callback: Callable):
        """Register callback for order book updates."""
        self._book_callbacks.append(callback)

    def on_trade(self, callback: Callable):
        """Register callback for market trades."""
        self._trade_callbacks.append(callback)

    def on_multi_asset_price(self, callback: Callable):
        """
        Register callback for ALL crypto price updates (any asset, any source).
        Callback signature: (source: str, symbol: str, price: float, timestamp: float)
        Used by CrossAssetCorrelation tracker.
        """
        if not hasattr(self, '_multi_asset_callbacks'):
            self._multi_asset_callbacks = []
        self._multi_asset_callbacks.append(callback)

    def get_binance_price(self, symbol: str) -> Optional[float]:
        """Get latest Binance price for any symbol."""
        if hasattr(self, '_binance_multi') and symbol in self._binance_multi:
            return self._binance_multi[symbol][1]
        return None

    def get_chainlink_price(self, symbol: str) -> Optional[float]:
        """Get latest Chainlink price for any symbol."""
        if hasattr(self, '_chainlink_multi') and symbol in self._chainlink_multi:
            return self._chainlink_multi[symbol][1]
        return None

    # ── Message Handlers ──────────────────────────────────

    def handle_rtds_message(self, raw: str):
        """
        Handle messages from RTDS WebSocket (Binance + Chainlink prices).
        
        Expected format:
        {
            "topic": "crypto_prices" | "crypto_prices_chainlink",
            "type": "update",
            "timestamp": 1753314088421,
            "payload": {
                "symbol": "btcusdt" | "btc/usd",
                "timestamp": 1753314088395,
                "value": 67234.50
            }
        }
        """
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        topic = msg.get("topic", "")
        payload = msg.get("payload", {})
        value = payload.get("value")
        ts = payload.get("timestamp", 0) / 1000.0  # Convert ms to seconds

        if value is None:
            return

        self._msg_count += 1

        if topic == "crypto_prices":
            symbol = payload.get("symbol", "")
            price = float(value)
            
            # Store BTC specifically for backward compat
            if symbol == "btcusdt":
                self.state.binance_btc = price
                self.state.binance_btc_timestamp = ts
                self._binance_prices.append((ts, price))

            # Store all symbols in multi-asset dict
            if not hasattr(self, '_binance_multi'):
                self._binance_multi = {}
            self._binance_multi[symbol] = (ts, price)

            # Notify all Binance callbacks (with symbol info in the tick)
            from tier1_signals import PriceTick
            tick = PriceTick(source=f"binance:{symbol}", price=price, timestamp=ts)
            for cb in self._binance_callbacks:
                try:
                    cb(tick)
                except Exception as e:
                    logger.error(f"Binance callback error: {e}")

            # Notify multi-asset callbacks
            if hasattr(self, '_multi_asset_callbacks'):
                for cb in self._multi_asset_callbacks:
                    try:
                        cb("binance", symbol, price, ts)
                    except Exception as e:
                        logger.error(f"Multi-asset callback error: {e}")

        elif topic == "crypto_prices_chainlink":
            symbol = payload.get("symbol", "")
            price = float(value)
            
            # Store BTC specifically for backward compat
            if symbol == "btc/usd":
                self.state.chainlink_btc = price
                self.state.chainlink_btc_timestamp = ts
                self._chainlink_prices.append((ts, price))

            # Store all symbols
            if not hasattr(self, '_chainlink_multi'):
                self._chainlink_multi = {}
            self._chainlink_multi[symbol] = (ts, price)

            from tier1_signals import PriceTick
            tick = PriceTick(source=f"chainlink:{symbol}", price=price, timestamp=ts)
            for cb in self._chainlink_callbacks:
                try:
                    cb(tick)
                except Exception as e:
                    logger.error(f"Chainlink callback error: {e}")

    def handle_market_message(self, raw: str):
        """
        Handle messages from Market WebSocket.
        
        Event types:
        - "book": Full orderbook snapshot
        - "price_change": Individual price level update
        - "last_trade_price": New trade executed
        - "best_bid_ask": Top-of-book update
        - "market_resolved": Market resolved
        """
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return

        # Market WS can send batched events as a JSON array
        messages = data if isinstance(data, list) else [data]
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            event_type = msg.get("event_type", "")

            if event_type == "book":
                self._handle_book_snapshot(msg)
            elif event_type == "price_change":
                self._handle_price_change(msg)
            elif event_type == "last_trade_price":
                self._handle_trade(msg)

    def _handle_book_snapshot(self, msg: dict):
        """Parse full order book snapshot."""
        from tier1_signals import OrderBookSnapshot, BookLevel

        bids_raw = msg.get("bids", [])
        asks_raw = msg.get("asks", [])

        bids = [BookLevel(price=float(b["price"]), size=float(b["size"])) for b in bids_raw]
        asks = [BookLevel(price=float(a["price"]), size=float(a["size"])) for a in asks_raw]

        # Sort: bids descending, asks ascending
        bids.sort(key=lambda x: x.price, reverse=True)
        asks.sort(key=lambda x: x.price)

        book = OrderBookSnapshot(
            bids=bids,
            asks=asks,
            timestamp=time.time(),
            book_hash=msg.get("hash", ""),
        )

        self.state.last_book_update = book.timestamp
        self.state.last_book_hash = book.book_hash

        for cb in self._book_callbacks:
            try:
                cb(book)
            except Exception as e:
                logger.error(f"Book callback error: {e}")

    def _handle_price_change(self, msg: dict):
        """Handle incremental price level change."""
        # For now, we rely on full book snapshots
        # In production, maintain a local book from deltas
        pass

    def _handle_trade(self, msg: dict):
        """Handle trade notification."""
        for cb in self._trade_callbacks:
            try:
                cb(msg)
            except Exception as e:
                logger.error(f"Trade callback error: {e}")

    # ── Analytics ────────────────────────────────────────

    def get_binance_move(self, lookback_seconds: float = 5.0) -> tuple:
        """
        Compute Binance BTC price move over the last N seconds.
        
        Returns: (move_pct, duration_seconds)
        """
        now = time.time()
        cutoff = now - lookback_seconds

        recent = [(ts, p) for ts, p in self._binance_prices if ts >= cutoff]
        if len(recent) < 2:
            return 0.0, 0.0

        earliest = recent[0]
        latest = recent[-1]

        if earliest[1] == 0:
            return 0.0, 0.0

        move_pct = ((latest[1] - earliest[1]) / earliest[1]) * 100.0
        duration = latest[0] - earliest[0]

        return move_pct, max(duration, 0.1)

    def get_feed_stats(self) -> Dict:
        """Return feed health statistics for monitoring."""
        now = time.time()
        elapsed = now - self._last_stats_time

        stats = {
            "messages_per_second": round(self._msg_count / max(elapsed, 1), 1),
            "binance_price": self.state.binance_btc,
            "chainlink_price": self.state.chainlink_btc,
            "oracle_gap_pct": round(self.state.oracle_gap_pct, 4),
            "binance_staleness_ms": round(self.state.binance_staleness_ms, 0),
            "chainlink_staleness_ms": round(self.state.chainlink_staleness_ms, 0),
            "book_staleness_ms": round((now - self.state.last_book_update) * 1000, 0),
            "binance_history_depth": len(self._binance_prices),
            "chainlink_history_depth": len(self._chainlink_prices),
        }

        # Reset counter
        self._msg_count = 0
        self._last_stats_time = now

        return stats


# ═══════════════════════════════════════════════════════════════
# WEBSOCKET CONNECTION MANAGER
# ═══════════════════════════════════════════════════════════════

class WebSocketManager:
    """
    Manages WebSocket connections with auto-reconnect.
    
    This is the connection layer. In production, use the `websockets`
    library. This class defines the interface and reconnection logic.
    
    Usage:
        manager = WebSocketManager(router)
        manager.connect_rtds()    # Binance + Chainlink
        manager.connect_market(yes_token_id, no_token_id)
    """

    def __init__(self, router: DataRouter):
        self.router = router
        self._reconnect_delay = 1.0
        self._max_reconnect_delay = 30.0
        self._connections: Dict[str, object] = {}

    async def connect_rtds(self):
        """
        Connect to RTDS WebSocket for Binance + Chainlink prices.
        
        Production implementation:
        ```
        import websockets
        
        async with websockets.connect(RTDS_WS_URL) as ws:
            # Subscribe to Binance
            await ws.send(json.dumps(BINANCE_SUBSCRIBE))
            # Subscribe to Chainlink
            await ws.send(json.dumps(CHAINLINK_SUBSCRIBE))
            
            async for msg in ws:
                self.router.handle_rtds_message(msg)
        ```
        """
        logger.info(f"Connecting to RTDS: {RTDS_WS_URL}")
        # NOTE: Actual WebSocket connection implemented in the engine.
        # This class defines the subscription messages and reconnection logic.
        pass

    async def connect_market(self, yes_token_id: str, no_token_id: str):
        """
        Connect to Market WebSocket for order book updates.
        
        Production implementation:
        ```
        import websockets
        
        async with websockets.connect(MARKET_WS_URL) as ws:
            await ws.send(json.dumps({
                "type": "market",
                "assets_ids": [yes_token_id, no_token_id],
                "custom_feature_enabled": True,
            }))
            
            async for msg in ws:
                self.router.handle_market_message(msg)
        ```
        """
        logger.info(f"Connecting to Market WS: {MARKET_WS_URL}")
        pass

    def _handle_disconnect(self, feed_name: str):
        """Exponential backoff reconnection."""
        self._reconnect_delay = min(
            self._reconnect_delay * 2,
            self._max_reconnect_delay,
        )
        logger.warning(
            f"{feed_name} disconnected. Reconnecting in {self._reconnect_delay}s"
        )

    def _handle_connect(self, feed_name: str):
        """Reset reconnection delay on successful connection."""
        self._reconnect_delay = 1.0
        logger.info(f"{feed_name} connected successfully")

    def get_subscription_messages(self) -> Dict[str, dict]:
        """Return the subscription messages for documentation."""
        return {
            "rtds_binance": BINANCE_SUBSCRIBE,
            "rtds_chainlink": CHAINLINK_SUBSCRIBE,
            "market": {
                "type": "market",
                "assets_ids": ["<YES_TOKEN_ID>", "<NO_TOKEN_ID>"],
                "custom_feature_enabled": True,
            },
        }
