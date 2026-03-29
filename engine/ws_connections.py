"""
Production WebSocket Feed Connections
======================================
Real WebSocket connections to Polymarket's three data streams:
1. RTDS - Binance + Chainlink real-time prices
2. Market - Order book updates for YES/NO tokens
3. Data API - Polling endpoint for recent trades (whale tracking)

Uses the `websockets` library with:
- Exponential backoff reconnection
- PING/PONG heartbeat handling
- Connection health monitoring
- Thread-safe message dispatch to DataRouter

This replaces the stub in data_feeds.py WebSocketManager.
"""

import json
import os
import time
import asyncio
import logging
import threading
from typing import Optional, Callable, Dict, List, Set
from collections import deque

logger = logging.getLogger("oracle.ws")

# ═══════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════

RTDS_URL = "wss://ws-live-data.polymarket.com"
MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
DATA_API_BASE = "https://data-api.polymarket.com"
GAMMA_API_BASE = "https://gamma-api.polymarket.com"

RECONNECT_BASE_DELAY = 1.0
RECONNECT_MAX_DELAY = 30.0
PING_INTERVAL = 5.0        # Server pings every 5s
PONG_TIMEOUT = 10.0         # Must respond within 10s
HEALTH_CHECK_INTERVAL = 30.0


# ═══════════════════════════════════════════════════════════════
# RTDS FEED (Binance + Chainlink)
# ═══════════════════════════════════════════════════════════════

class RTDSFeed:
    """
    Connects to Polymarket's Real-Time Data Socket for:
    - Binance BTC/USDT prices (crypto_prices topic)
    - Chainlink BTC/USD oracle prices (crypto_prices_chainlink topic)
    
    The RTDS WebSocket is at wss://ws-live-data.polymarket.com
    No authentication required.
    
    Message format:
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

    def __init__(self, on_message: Callable[[str], None]):
        self._on_message = on_message
        self._ws = None
        self._running = False
        self._reconnect_delay = RECONNECT_BASE_DELAY
        self._connected = False
        self._last_msg_time = 0.0
        self._msg_count = 0
        self._error_count = 0
        # Default: all 7 crypto assets
        self._binance_symbols = "btcusdt,ethusdt,solusdt,xrpusdt,dogeusdt,hypeusdt,bnbusdt"
        self._chainlink_symbols = ["btc/usd", "eth/usd", "sol/usd", "xrp/usd", "doge/usd", "bnb/usd"]

    def set_symbols(self, binance: List[str], chainlink: List[str]):
        """Update which symbols to subscribe to (call before connect)."""
        self._binance_symbols = ",".join(binance)
        self._chainlink_symbols = chainlink

    async def connect(self):
        """Main connection loop with auto-reconnect."""
        try:
            import websockets
        except ImportError:
            logger.error("websockets not installed: pip install websockets --break-system-packages")
            return

        self._running = True
        while self._running:
            try:
                logger.info(f"RTDS: Connecting to {RTDS_URL}")
                async with websockets.connect(
                    RTDS_URL,
                    ping_interval=20,    # Send WebSocket pings every 20s to keep connection alive
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=2**20,      # 1MB max message
                ) as ws:
                    self._ws = ws
                    self._connected = True
                    self._reconnect_delay = RECONNECT_BASE_DELAY
                    logger.info("RTDS: Connected successfully")

                    # Subscribe to ALL Binance crypto prices
                    await ws.send(json.dumps({
                        "action": "subscribe",
                        "subscriptions": [{
                            "topic": "crypto_prices",
                            "type": "update",
                            "filters": self._binance_symbols
                        }]
                    }))

                    # Subscribe to ALL Chainlink oracle prices
                    for symbol in self._chainlink_symbols:
                        await ws.send(json.dumps({
                            "action": "subscribe",
                            "subscriptions": [{
                                "topic": "crypto_prices_chainlink",
                                "type": "*",
                                "filters": json.dumps({"symbol": symbol})
                            }]
                        }))

                    logger.info(
                        f"RTDS: Subscribed to Binance [{self._binance_symbols}] + "
                        f"Chainlink {self._chainlink_symbols}"
                    )

                    # Message loop
                    async for raw in ws:
                        self._last_msg_time = time.time()
                        self._msg_count += 1

                        # Handle PING
                        if isinstance(raw, str) and raw.strip().upper() == "PING":
                            await ws.send("PONG")
                            continue

                        try:
                            self._on_message(raw)
                        except Exception as e:
                            logger.error(f"RTDS: Message handler error: {e}")

            except Exception as e:
                self._connected = False
                self._error_count += 1
                logger.warning(f"RTDS: Connection lost ({e}). "
                               f"Reconnecting in {self._reconnect_delay:.1f}s")
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(
                    self._reconnect_delay * 2,
                    RECONNECT_MAX_DELAY,
                )

    def stop(self):
        self._running = False
        if self._ws:
            asyncio.ensure_future(self._ws.close())

    @property
    def health(self) -> Dict:
        staleness = time.time() - self._last_msg_time if self._last_msg_time else float('inf')
        return {
            "connected": self._connected,
            "msg_count": self._msg_count,
            "error_count": self._error_count,
            "staleness_s": round(staleness, 1),
            "healthy": self._connected and staleness < 10.0,
        }


# ═══════════════════════════════════════════════════════════════
# MARKET FEED (Order Book)
# ═══════════════════════════════════════════════════════════════

class MarketFeed:
    """
    Connects to Polymarket's Market WebSocket for order book data.
    
    Endpoint: wss://ws-subscriptions-clob.polymarket.com/ws/market
    No authentication required.
    
    Subscribes to both YES and NO token IDs to get full book data.
    
    Event types:
    - "book": Full orderbook snapshot (initial + periodic)
    - "price_change": Individual price level update
    - "last_trade_price": New trade executed
    - "best_bid_ask": Top-of-book update (requires custom_feature_enabled)
    - "market_resolved": Market settled
    """

    def __init__(self, on_message: Callable[[str], None]):
        self._on_message = on_message
        self._ws = None
        self._running = False
        self._reconnect_delay = RECONNECT_BASE_DELAY
        self._connected = False
        self._last_msg_time = 0.0
        self._msg_count = 0
        self._error_count = 0
        self._subscribed_assets: Set[str] = set()

    async def connect(self, asset_ids: List[str]):
        """
        Connect and subscribe to order book updates for given assets.
        
        Args:
            asset_ids: List of token IDs (YES and NO) to subscribe to
        """
        try:
            import websockets
        except ImportError:
            logger.error("websockets not installed")
            return

        self._running = True
        while self._running:
            try:
                logger.info(f"Market WS: Connecting to {MARKET_URL}")
                async with websockets.connect(
                    MARKET_URL,
                    ping_interval=20,    # Send WebSocket pings every 20s to keep connection alive
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=2**20,
                ) as ws:
                    self._ws = ws
                    self._connected = True
                    self._reconnect_delay = RECONNECT_BASE_DELAY
                    logger.info("Market WS: Connected")

                    # Subscribe
                    sub_msg = {
                        "type": "market",
                        "assets_ids": asset_ids,
                        "custom_feature_enabled": True,  # Enable best_bid_ask + market_resolved
                    }
                    await ws.send(json.dumps(sub_msg))
                    self._subscribed_assets = set(asset_ids)
                    logger.info(f"Market WS: Subscribed to {len(asset_ids)} assets")

                    # Message loop
                    async for raw in ws:
                        self._last_msg_time = time.time()
                        self._msg_count += 1

                        if isinstance(raw, str) and raw.strip().upper() == "PING":
                            await ws.send("PONG")
                            continue

                        try:
                            self._on_message(raw)
                        except Exception as e:
                            logger.error(f"Market WS: Handler error: {e}")

            except Exception as e:
                self._connected = False
                self._error_count += 1
                logger.warning(f"Market WS: Connection lost ({e}). "
                               f"Reconnecting in {self._reconnect_delay:.1f}s")
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(
                    self._reconnect_delay * 2,
                    RECONNECT_MAX_DELAY,
                )

    async def update_subscription(self, add_ids: List[str] = None, remove_ids: List[str] = None):
        """
        Dynamically update subscribed assets without reconnecting.
        
        This is critical for market rotation — when one 5-min window
        closes and a new market opens, we need to switch subscriptions.
        """
        if not self._ws or not self._connected:
            return

        if add_ids:
            msg = {
                "type": "market",
                "assets_ids": add_ids,
                "action": "subscribe",
            }
            await self._ws.send(json.dumps(msg))
            self._subscribed_assets.update(add_ids)
            logger.info(f"Market WS: Added {len(add_ids)} assets")

        if remove_ids:
            msg = {
                "type": "market",
                "assets_ids": remove_ids,
                "action": "unsubscribe",
            }
            await self._ws.send(json.dumps(msg))
            self._subscribed_assets -= set(remove_ids)
            logger.info(f"Market WS: Removed {len(remove_ids)} assets")

    def stop(self):
        self._running = False
        if self._ws:
            asyncio.ensure_future(self._ws.close())

    @property
    def health(self) -> Dict:
        staleness = time.time() - self._last_msg_time if self._last_msg_time else float('inf')
        return {
            "connected": self._connected,
            "msg_count": self._msg_count,
            "error_count": self._error_count,
            "subscribed_assets": len(self._subscribed_assets),
            "staleness_s": round(staleness, 1),
            "healthy": self._connected and staleness < 5.0,
        }


# ═══════════════════════════════════════════════════════════════
# DATA API POLLER (Whale Tracking)
# ═══════════════════════════════════════════════════════════════

class DataAPIPoller:
    """
    Polls Polymarket's Data API for recent trades on target markets.
    
    Used for:
    - Whale flow tracking (who's trading, how much, which direction)
    - Market-level trade flow analysis
    - Position tracking
    
    Endpoints:
    - GET /trades?market={conditionId}&limit=100
    - GET /positions?user={address}
    - GET /activity?user={address}&limit=100
    
    Polling frequency: every 5-10 seconds (rate limited)
    """

    def __init__(
        self,
        on_trades: Callable[[List[dict]], None],
        poll_interval: float = 5.0,
        max_trades_per_poll: int = 100,
    ):
        self._on_trades = on_trades
        self._poll_interval = poll_interval
        self._max_trades = max_trades_per_poll
        self._running = False
        self._seen_hashes: Set[str] = set()
        self._target_markets: Set[str] = set()
        self._poll_count = 0
        self._error_count = 0
        self._last_poll_time = 0.0

    def set_target_markets(self, condition_ids: List[str]):
        """Set which markets to poll for trade data."""
        self._target_markets = set(condition_ids)
        logger.info(f"DataAPI: Tracking {len(condition_ids)} markets")

    async def poll_loop(self):
        """Main polling loop."""
        try:
            import httpx
        except ImportError:
            logger.error("httpx not installed: pip install httpx --break-system-packages")
            return

        self._running = True
        async with httpx.AsyncClient(timeout=10.0) as client:
            while self._running:
                for market_id in list(self._target_markets):
                    if not self._running:
                        break
                    await self._poll_market(client, market_id)

                await asyncio.sleep(self._poll_interval)

    async def _poll_market(self, client, condition_id: str):
        """Poll a single market for recent trades."""
        try:
            url = f"{DATA_API_BASE}/trades"
            params = {
                "market": condition_id,
                "limit": self._max_trades,
            }
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            self._poll_count += 1
            self._last_poll_time = time.time()

            # Filter to unseen trades
            trades = data if isinstance(data, list) else data.get("data", [])
            new_trades = []
            for trade in trades:
                tx_hash = trade.get("transactionHash", "")
                if tx_hash and tx_hash not in self._seen_hashes:
                    self._seen_hashes.add(tx_hash)
                    new_trades.append(trade)

            # Trim seen set to prevent memory growth
            if len(self._seen_hashes) > 50000:
                # Keep most recent half
                self._seen_hashes = set(list(self._seen_hashes)[-25000:])

            if new_trades:
                try:
                    self._on_trades(new_trades)
                except Exception as e:
                    logger.error(f"DataAPI: Trade handler error: {e}")

        except Exception as e:
            self._error_count += 1
            logger.warning(f"DataAPI: Poll error for {condition_id[:16]}...: {e}")

    def stop(self):
        self._running = False

    @property
    def health(self) -> Dict:
        staleness = time.time() - self._last_poll_time if self._last_poll_time else float('inf')
        return {
            "poll_count": self._poll_count,
            "error_count": self._error_count,
            "tracked_markets": len(self._target_markets),
            "seen_trades": len(self._seen_hashes),
            "staleness_s": round(staleness, 1),
            "healthy": staleness < self._poll_interval * 3,
        }


# ═══════════════════════════════════════════════════════════════
# MARKET SCANNER (Find Active BTC 5-Min Markets)
# ═══════════════════════════════════════════════════════════════

class MarketScanner:
    """
    Finds active BTC 5-minute binary markets via deterministic slug.

    BTC 5-min markets use slug pattern: btc-updown-5m-{unix_timestamp}
    where the timestamp is aligned to 5-minute boundaries (divisible by 300).

    Uses /events?slug= endpoint for exact match (not generic search),
    since these short-lived markets don't appear in tag-based listings.
    """

    WINDOW_SECONDS = 300  # 5 minutes

    def __init__(self, scan_interval: float = 15.0):
        self._scan_interval = scan_interval
        self._active_markets: List[Dict] = []
        self._last_scan_time = 0.0
        self._scan_count = 0

    def _current_window_ts(self) -> int:
        """Get current 5-min window start timestamp."""
        now = int(time.time())
        return (now // self.WINDOW_SECONDS) * self.WINDOW_SECONDS

    def _parse_event_market(self, market: Dict) -> Optional[Dict]:
        """Parse a market from the events API response into our standard format."""
        import json as _json

        condition_id = market.get("conditionId", "")
        question = market.get("question", "")

        # clobTokenIds is a JSON STRING, not a list
        token_ids_raw = market.get("clobTokenIds", "[]")
        if isinstance(token_ids_raw, str):
            try:
                token_ids = _json.loads(token_ids_raw)
            except Exception:
                token_ids = []
        else:
            token_ids = token_ids_raw

        # outcomes is also a JSON string
        outcomes_raw = market.get("outcomes", "[]")
        if isinstance(outcomes_raw, str):
            try:
                outcomes = _json.loads(outcomes_raw)
            except Exception:
                outcomes = []
        else:
            outcomes = outcomes_raw

        # Map token IDs to Yes(Up)/No(Down)
        yes_token_id = no_token_id = None
        if len(token_ids) >= 2 and len(outcomes) >= 2:
            for i, outcome in enumerate(outcomes):
                o = str(outcome).strip().lower()
                if o in ("up", "yes"):
                    yes_token_id = token_ids[i]
                elif o in ("down", "no"):
                    no_token_id = token_ids[i]

        if not yes_token_id or not no_token_id:
            return None

        # Sanity check token IDs
        if len(str(yes_token_id)) < 10 or len(str(no_token_id)) < 10:
            return None

        # Parse outcomePrices
        prices_raw = market.get("outcomePrices", "[]")
        if isinstance(prices_raw, str):
            try:
                prices = _json.loads(prices_raw)
            except Exception:
                prices = []
        else:
            prices = prices_raw

        yes_price = float(prices[0]) if len(prices) >= 1 else 0.5
        no_price = float(prices[1]) if len(prices) >= 2 else 0.5

        return {
            "condition_id": condition_id,
            "question": question,
            "yes_token_id": yes_token_id,
            "no_token_id": no_token_id,
            "yes_price": yes_price,
            "no_price": no_price,
            "end_date_iso": market.get("endDateIso", ""),
            "volume24hr": float(market.get("volume24hr", 0) or 0),
            "liquidity": float(market.get("liquidityNum", 0) or 0),
            "best_bid": max(yes_price - 0.01, 0.01),
            "best_ask": min(yes_price + 0.01, 0.99),
            "spread": 0.02,
            "neg_risk": market.get("negRisk", False),
            "tick_size": float(market.get("orderPriceMinTickSize", 0.01) or 0.01),
        }

    async def scan(self) -> List[Dict]:
        """
        Find the current BTC 5-minute market via deterministic slug.

        Returns list of market dicts with:
        - condition_id, question, yes_token_id, no_token_id
        - yes_price, no_price, end_date_iso
        - best_bid, best_ask, volume24hr, liquidity
        """
        try:
            import httpx
        except ImportError:
            return []

        window_ts = self._current_window_ts()
        slug = f"btc-updown-5m-{window_ts}"

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(
                    f"{GAMMA_API_BASE}/events",
                    params={"slug": slug}
                )
                resp.raise_for_status()
                events = resp.json()

                btc_5min = []

                if events and isinstance(events, list) and len(events) > 0:
                    event = events[0]
                    markets = event.get("markets", [])
                    for m in markets:
                        is_active = m.get("active", False)
                        if not is_active:
                            continue
                        parsed = self._parse_event_market(m)
                        if parsed:
                            # Add timing and slug info
                            window_end = window_ts + self.WINDOW_SECONDS
                            parsed["seconds_before_close"] = max(0, window_end - int(time.time()))
                            parsed["slug"] = slug
                            parsed["window_ts"] = window_ts
                            btc_5min.append(parsed)

                self._active_markets = btc_5min
                self._last_scan_time = time.time()
                self._scan_count += 1

                logger.info(f"Scanner: Found {len(btc_5min)} active BTC 5-min markets (slug={slug})")
                return btc_5min

        except Exception as e:
            logger.error(f"Scanner: Scan failed for {slug}: {e}")
            return self._active_markets  # Return cached

    @property
    def current_markets(self) -> List[Dict]:
        return self._active_markets

    @property
    def health(self) -> Dict:
        staleness = time.time() - self._last_scan_time if self._last_scan_time else float('inf')
        return {
            "active_markets": len(self._active_markets),
            "scan_count": self._scan_count,
            "staleness_s": round(staleness, 1),
        }


# ═══════════════════════════════════════════════════════════════
# FEED COORDINATOR
# ═══════════════════════════════════════════════════════════════

class FeedCoordinator:
    """
    Top-level coordinator that manages all data feeds.
    
    Lifecycle:
    1. Start market scanner to find active markets
    2. Connect RTDS feed for price data
    3. Connect market feed for order books
    4. Start data API poller for trade flow
    5. When markets rotate (5-min windows), update subscriptions
    
    Usage:
        from data_feeds import DataRouter
        
        router = DataRouter()
        coordinator = FeedCoordinator(router)
        await coordinator.start()
    """

    def __init__(self, router):
        """
        Args:
            router: DataRouter instance from data_feeds.py
        """
        self.router = router
        self.scanner = MarketScanner()
        self.rtds = RTDSFeed(on_message=router.handle_rtds_message)
        self.market_feed = MarketFeed(on_message=router.handle_market_message)
        self.data_poller = DataAPIPoller(
            on_trades=self._handle_new_trades,
            poll_interval=5.0,
        )

        self._current_yes_token: Optional[str] = None
        self._current_no_token: Optional[str] = None
        self._current_condition_id: Optional[str] = None

    def _handle_new_trades(self, trades: List[dict]):
        """Route new trades from Data API to whale tracker via router."""
        for trade in trades:
            for cb in self.router._trade_callbacks:
                try:
                    cb(trade)
                except Exception as e:
                    logger.error(f"Trade callback error: {e}")

    async def start(self):
        """Start all feeds concurrently."""
        logger.info("FeedCoordinator: Starting all feeds")

        # Initial scan
        markets = await self.scanner.scan()
        if markets:
            best = markets[0]  # Pick highest volume/liquidity
            self._current_condition_id = best["condition_id"]
            self._current_yes_token = best["yes_token_id"]
            self._current_no_token = best["no_token_id"]
            # Feed initial target to DataAPIPoller for whale flow tracking
            self.data_poller.set_target_markets([best["condition_id"]])
            logger.info(f"FeedCoordinator: Target market: {best['question']}")

        # Start all feeds concurrently
        tasks = [
            asyncio.create_task(self.rtds.connect()),
            asyncio.create_task(self._binance_poller()),
            asyncio.create_task(self._chainlink_poller()),
            asyncio.create_task(self._market_loop()),
            asyncio.create_task(self.data_poller.poll_loop()),
            asyncio.create_task(self._scanner_loop()),
            asyncio.create_task(self._health_monitor()),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("FeedCoordinator: Shutting down")

    async def _binance_poller(self):
        """
        Poll Binance REST API for BTC price as reliable fallback for RTDS.
        RTDS WebSocket often fails to deliver continuous price data.
        Injects prices into the router the same way RTDS would.
        Routes through CLOB_PROXY_URL if set (Binance blocks US IPs with HTTP 451).
        """
        try:
            import httpx
        except ImportError:
            logger.error("httpx not installed — Binance poller disabled")
            return

        BINANCE_URL = "https://api.binance.com/api/v3/ticker/price"
        poll_interval = 2.0  # Poll every 2 seconds
        _poll_count = 0

        proxy_url = os.environ.get("CLOB_PROXY_URL")
        if proxy_url:
            logger.info(f"Binance poller: using proxy")

        while True:
            try:
                async with httpx.AsyncClient(timeout=5.0, proxy=proxy_url) as client:
                    resp = await client.get(BINANCE_URL, params={"symbol": "BTCUSDT"})
                    if resp.status_code == 200:
                        data = resp.json()
                        price = float(data["price"])
                        now_ms = int(time.time() * 1000)
                        # Inject as RTDS-format message so the router processes it
                        synthetic_msg = json.dumps({
                            "topic": "crypto_prices",
                            "type": "update",
                            "timestamp": now_ms,
                            "payload": {
                                "symbol": "btcusdt",
                                "timestamp": now_ms,
                                "value": price,
                            }
                        })
                        self.router.handle_rtds_message(synthetic_msg)
                        _poll_count += 1
                        if _poll_count == 1 or _poll_count % 300 == 0:
                            logger.info(f"Binance poller: ${price:.2f} (poll #{_poll_count})")
                    else:
                        logger.warning(f"Binance poller: HTTP {resp.status_code}")
            except Exception as e:
                logger.warning(f"Binance poller error: {e}")

            await asyncio.sleep(poll_interval)

    async def _chainlink_poller(self):
        """
        Poll Chainlink BTC/USD price feed on Polygon via smart contract.

        This is the SOURCE OF TRUTH for Polymarket BTC 5-min market settlements.
        Polymarket settles using Chainlink's on-chain oracle price, NOT Binance.

        Contract: 0xc907E116054Ad103354f2D350FD2514433D57F6f (Chainlink BTC/USD on Polygon)
        Method: latestRoundData() → (roundId, answer, startedAt, updatedAt, answeredInRound)
        answer is in 8-decimal format (e.g., 8723450000000 = $87,234.50)
        """
        import os
        rpc_url = os.environ.get("POLYGON_RPC_URL")
        if not rpc_url:
            logger.warning("POLYGON_RPC_URL not set — Chainlink poller disabled")
            return

        try:
            from web3 import Web3
        except ImportError:
            logger.error("web3 not installed — Chainlink poller disabled")
            return

        CHAINLINK_BTC_USD = "0xc907E116054Ad103354f2D350FD2514433D57F6f"
        ABI = [
            {
                "inputs": [],
                "name": "latestRoundData",
                "outputs": [
                    {"name": "roundId", "type": "uint80"},
                    {"name": "answer", "type": "int256"},
                    {"name": "startedAt", "type": "uint256"},
                    {"name": "updatedAt", "type": "uint256"},
                    {"name": "answeredInRound", "type": "uint80"},
                ],
                "stateMutability": "view",
                "type": "function",
            }
        ]
        POLL_INTERVAL = 3.0  # Every 3 seconds

        w3 = Web3(Web3.HTTPProvider(rpc_url))
        contract = w3.eth.contract(address=Web3.to_checksum_address(CHAINLINK_BTC_USD), abi=ABI)

        logger.info(f"Chainlink poller started (contract={CHAINLINK_BTC_USD[:10]}..., rpc={rpc_url[:40]}...)")

        while True:
            try:
                round_data = contract.functions.latestRoundData().call()
                answer = round_data[1]  # int256, 8 decimals
                price = answer / 1e8
                now_ms = int(time.time() * 1000)

                # Inject as RTDS-format Chainlink message
                synthetic_msg = json.dumps({
                    "topic": "crypto_prices_chainlink",
                    "type": "update",
                    "timestamp": now_ms,
                    "payload": {
                        "symbol": "btc/usd",
                        "timestamp": now_ms,
                        "value": price,
                    }
                })
                self.router.handle_rtds_message(synthetic_msg)
            except Exception as e:
                logger.debug(f"Chainlink poller error: {e}")

            await asyncio.sleep(POLL_INTERVAL)

    async def _market_loop(self):
        """Connect market feed with current tokens."""
        while True:
            if self._current_yes_token and self._current_no_token:
                await self.market_feed.connect([
                    self._current_yes_token,
                    self._current_no_token,
                ])
            else:
                await asyncio.sleep(5)

    async def _scanner_loop(self):
        """Periodically scan for new markets and rotate subscriptions."""
        while True:
            await asyncio.sleep(self.scanner._scan_interval)

            markets = await self.scanner.scan()
            if not markets:
                continue

            best = markets[0]
            new_cid = best["condition_id"]

            # Check if market changed
            if new_cid != self._current_condition_id:
                logger.info(f"FeedCoordinator: Market rotation → {best['question']}")

                old_yes = self._current_yes_token
                old_no = self._current_no_token

                self._current_condition_id = new_cid
                self._current_yes_token = best["yes_token_id"]
                self._current_no_token = best["no_token_id"]

                # Update market WS subscription
                add_ids = [self._current_yes_token, self._current_no_token]
                remove_ids = [old_yes, old_no] if old_yes else []
                await self.market_feed.update_subscription(
                    add_ids=add_ids,
                    remove_ids=remove_ids,
                )

                # Update data poller target
                self.data_poller.set_target_markets([new_cid])

    async def _health_monitor(self):
        """Log feed health status periodically."""
        while True:
            await asyncio.sleep(HEALTH_CHECK_INTERVAL)

            health = self.get_health()
            all_healthy = all(
                v.get("healthy", False) for k, v in health.items()
                if isinstance(v, dict) and "healthy" in v
            )

            level = "INFO" if all_healthy else "WARNING"
            logger.log(
                getattr(logging, level),
                f"Feed Health: {json.dumps(health, default=str)}"
            )

    def get_health(self) -> Dict:
        """Aggregate health from all feeds."""
        return {
            "rtds": self.rtds.health,
            "market": self.market_feed.health,
            "data_api": self.data_poller.health,
            "scanner": self.scanner.health,
            "current_market": self._current_condition_id,
        }

    async def stop(self):
        """Gracefully stop all feeds."""
        self.rtds.stop()
        self.market_feed.stop()
        self.data_poller.stop()
        logger.info("FeedCoordinator: All feeds stopped")
