"""
Multi-Asset Data Collector
============================
Background task that collects Chainlink prices, LTP, and trade flow
for all 7 Polymarket crypto assets every 10 seconds. Data collection
only — does NOT trade.

Assets: BTC, ETH, SOL, XRP, DOGE, BNB, HYPE

Data written to:
  multi_asset_ticks — one row per asset per poll cycle
  multi_asset_settlements — one row per asset per settled window

This data powers:
  - Cross-asset correlation research
  - Multi-asset oracle strategy development
  - Competition monitoring (unique makers per window)
"""

import time
import json
import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Dict, List, Set
from collections import deque

logger = logging.getLogger("oracle.multi_collector")

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
DATA_API_BASE = "https://data-api.polymarket.com"

WINDOW_SECONDS = 300  # 5-minute windows

ASSETS = ["btc", "eth", "sol", "xrp", "doge", "bnb", "hype"]

# Chainlink symbol mapping (router state uses these keys)
CHAINLINK_KEYS = {
    "btc": "btc/usd",
    "eth": "eth/usd",
    "sol": "sol/usd",
    "xrp": "xrp/usd",
    "doge": "doge/usd",
    "bnb": "bnb/usd",
    # HYPE has no Chainlink feed
}


@dataclass
class AssetMarket:
    """Cached market info for one asset's current 5-min window."""
    asset: str
    condition_id: str
    yes_token_id: str
    no_token_id: str
    question: str
    window_ts: int


class MultiAssetCollector:
    """
    Background data collector for all 7 crypto assets.

    Runs two loops:
      1. collect_cycle() every 10 seconds — polls LTP, flow, Chainlink
      2. collect_settlement() 90 seconds after each window closes

    Args:
        supabase_client: Initialized Supabase client
        data_router: DataRouter instance for Chainlink prices (optional)
    """

    def __init__(self, supabase_client, data_router=None):
        self._supabase = supabase_client
        self._router = data_router
        self._markets: Dict[str, AssetMarket] = {}  # asset → current market
        self._seen_trade_hashes: Set[str] = set()
        self._last_window_ts: int = 0
        self._settled_windows: Set[int] = set()  # window_ts values already settled
        self._cycle_count = 0

    def _current_window_ts(self) -> int:
        now = int(time.time())
        return (now // WINDOW_SECONDS) * WINDOW_SECONDS

    async def start(self):
        """Start both collection loops."""
        logger.info("MultiAssetCollector: Starting data collection for 7 assets")
        tasks = [
            asyncio.create_task(self._tick_loop()),
            asyncio.create_task(self._settlement_loop()),
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("MultiAssetCollector: Stopped")

    async def _tick_loop(self):
        """Poll all assets every 10 seconds."""
        await asyncio.sleep(15)  # Let feeds warm up
        while True:
            try:
                await self.collect_cycle()
            except Exception as e:
                logger.error(f"MultiAssetCollector tick error: {e}")
            await asyncio.sleep(10)

    async def _settlement_loop(self):
        """Check settlements 90 seconds after each window closes."""
        await asyncio.sleep(30)
        while True:
            try:
                window_ts = self._current_window_ts()
                # Check the PREVIOUS window (which just closed)
                prev_window = window_ts - WINDOW_SECONDS
                now = time.time()
                seconds_since_close = now - (prev_window + WINDOW_SECONDS)

                if 90 <= seconds_since_close <= 600 and prev_window not in self._settled_windows:
                    await self.collect_settlement(prev_window)
                    self._settled_windows.add(prev_window)

                    # Trim settled set
                    if len(self._settled_windows) > 100:
                        cutoff = window_ts - (100 * WINDOW_SECONDS)
                        self._settled_windows = {w for w in self._settled_windows if w > cutoff}

            except Exception as e:
                logger.error(f"MultiAssetCollector settlement error: {e}")
            await asyncio.sleep(30)

    async def collect_cycle(self):
        """
        One collection cycle across all assets.

        For each asset:
          1. Find current market via Gamma API slug
          2. Get LTP from CLOB /book
          3. Get Chainlink price from router state
          4. Get trade flow from Data API
          5. Write to multi_asset_ticks
        """
        import httpx

        window_ts = self._current_window_ts()
        window_end = window_ts + WINDOW_SECONDS
        seconds_remaining = max(0, window_end - int(time.time()))

        # Refresh markets if window changed
        if window_ts != self._last_window_ts:
            await self._refresh_markets(window_ts)
            self._last_window_ts = window_ts

        self._cycle_count += 1
        rows = []

        async with httpx.AsyncClient(timeout=8.0) as client:
            for asset in ASSETS:
                market = self._markets.get(asset)
                if not market:
                    continue

                # 1. LTP from CLOB
                ltp = None
                try:
                    resp = await client.get(
                        f"{CLOB_BASE}/book",
                        params={"token_id": market.yes_token_id},
                    )
                    if resp.status_code == 200:
                        book_data = resp.json()
                        ltp = book_data.get("last_trade_price")
                        if ltp is not None:
                            ltp = float(ltp)
                except Exception:
                    pass

                # 2. Midpoint from book
                midpoint = None
                try:
                    if resp.status_code == 200:
                        bids = book_data.get("bids", [])
                        asks = book_data.get("asks", [])
                        if bids and asks:
                            best_bid = float(bids[0].get("price", 0))
                            best_ask = float(asks[0].get("price", 0))
                            if best_bid > 0 and best_ask > 0:
                                midpoint = round((best_bid + best_ask) / 2, 4)
                except Exception:
                    pass

                # 3. Chainlink price from router state
                chainlink_price = None
                if self._router:
                    cl_key = CHAINLINK_KEYS.get(asset)
                    if cl_key:
                        chainlink_price = self._router.get_chainlink_price(cl_key)

                # 4. Trade flow from Data API
                flow_up = 0.0
                flow_down = 0.0
                flow_direction = "NEUTRAL"
                unique_makers = 0
                try:
                    resp2 = await client.get(
                        f"{DATA_API_BASE}/trades",
                        params={"market": market.condition_id, "limit": 50},
                    )
                    if resp2.status_code == 200:
                        trades = resp2.json()
                        if not isinstance(trades, list):
                            trades = trades.get("data", [])
                        makers = set()
                        for t in trades:
                            tx = t.get("transactionHash", "")
                            if tx in self._seen_trade_hashes:
                                continue
                            self._seen_trade_hashes.add(tx)
                            side = (t.get("side", "") or "").upper()
                            outcome = (t.get("outcome", "") or "").upper()
                            size = float(t.get("size", 0) or 0)
                            price = float(t.get("price", 0) or 0)
                            vol = size * price
                            maker = t.get("maker_address") or t.get("proxyWallet") or ""
                            if maker:
                                makers.add(maker)
                            if (side == "BUY" and outcome in ("YES", "UP")) or \
                               (side == "SELL" and outcome in ("NO", "DOWN")):
                                flow_up += vol
                            elif (side == "BUY" and outcome in ("NO", "DOWN")) or \
                                 (side == "SELL" and outcome in ("YES", "UP")):
                                flow_down += vol
                        unique_makers = len(makers)
                        if flow_up > 0 or flow_down > 0:
                            ratio = flow_up / flow_down if flow_down > 0 else 10.0
                            flow_direction = "UP" if ratio > 1.5 else "DOWN" if ratio < 0.67 else "NEUTRAL"
                except Exception:
                    pass

                rows.append({
                    "window_ts": window_ts,
                    "asset": asset,
                    "chainlink_price": chainlink_price,
                    "ltp": ltp,
                    "midpoint": midpoint,
                    "flow_up_volume": round(flow_up, 2),
                    "flow_down_volume": round(flow_down, 2),
                    "flow_direction": flow_direction,
                    "unique_makers": unique_makers,
                    "seconds_remaining": seconds_remaining,
                })

        # Write all rows to Supabase
        if rows:
            try:
                self._supabase.table("multi_asset_ticks").insert(rows).execute()
            except Exception as e:
                logger.error(f"MultiAssetCollector: Failed to write ticks: {e}")

        # Trim seen hashes
        if len(self._seen_trade_hashes) > 50000:
            self._seen_trade_hashes = set(list(self._seen_trade_hashes)[-25000:])

        if self._cycle_count == 1 or self._cycle_count % 30 == 0:
            logger.info(
                f"MultiAssetCollector: Cycle #{self._cycle_count}, "
                f"{len(rows)} assets, {seconds_remaining}s remaining"
            )

    async def collect_settlement(self, window_ts: int):
        """
        Check market resolution for all assets after a window closes.

        Writes to multi_asset_settlements table.
        """
        import httpx

        logger.info(f"MultiAssetCollector: Checking settlements for window {window_ts}")
        rows = []

        async with httpx.AsyncClient(timeout=10.0) as client:
            for asset in ASSETS:
                slug = f"{asset}-updown-5m-{window_ts}"
                try:
                    resp = await client.get(
                        f"{GAMMA_API_BASE}/events",
                        params={"slug": slug},
                    )
                    if resp.status_code != 200:
                        continue

                    events = resp.json()
                    if not events or not isinstance(events, list) or len(events) == 0:
                        continue

                    event = events[0]
                    markets = event.get("markets", [])
                    if not markets:
                        continue

                    market = markets[0]
                    # Check if resolved
                    outcome_prices = market.get("outcomePrices", "[]")
                    if isinstance(outcome_prices, str):
                        try:
                            outcome_prices = json.loads(outcome_prices)
                        except Exception:
                            outcome_prices = []

                    # Determine actual direction from outcome prices
                    # If YES price = 1.0, outcome is UP; if NO price = 1.0, outcome is DOWN
                    actual_direction = None
                    if len(outcome_prices) >= 2:
                        yes_final = float(outcome_prices[0])
                        no_final = float(outcome_prices[1])
                        if yes_final > 0.9:
                            actual_direction = "UP"
                        elif no_final > 0.9:
                            actual_direction = "DOWN"

                    if not actual_direction:
                        # Market may not be resolved yet
                        continue

                    # Get LTP and flow at 80% through window from ticks table
                    ltp_80 = None
                    flow_80 = None
                    try:
                        tick_result = self._supabase.table("multi_asset_ticks").select(
                            "ltp, flow_direction, seconds_remaining"
                        ).eq("window_ts", window_ts).eq("asset", asset).order(
                            "seconds_remaining"
                        ).limit(20).execute()

                        if tick_result.data:
                            for tick in tick_result.data:
                                sr = tick.get("seconds_remaining", 999)
                                if 40 <= sr <= 80:  # Around 80% through
                                    ltp_80 = tick.get("ltp")
                                    flow_80 = tick.get("flow_direction")
                                    break
                    except Exception:
                        pass

                    # Get BTC direction for correlation tracking
                    btc_direction = None
                    btc_move_pct = None
                    try:
                        btc_result = self._supabase.table("multi_asset_settlements").select(
                            "actual_direction, chainlink_move_pct"
                        ).eq("window_ts", window_ts).eq("asset", "btc").limit(1).execute()
                        if btc_result.data:
                            btc_direction = btc_result.data[0].get("actual_direction")
                            btc_move_pct = btc_result.data[0].get("chainlink_move_pct")
                    except Exception:
                        pass

                    # Get chainlink open/close from chainlink_windows (BTC only) or ticks
                    cl_open = None
                    cl_close = None
                    cl_move = None
                    try:
                        ticks = self._supabase.table("multi_asset_ticks").select(
                            "chainlink_price, seconds_remaining"
                        ).eq("window_ts", window_ts).eq("asset", asset).order(
                            "created_at"
                        ).limit(50).execute()
                        if ticks.data:
                            prices = [t["chainlink_price"] for t in ticks.data if t.get("chainlink_price")]
                            if prices:
                                cl_open = prices[0]
                                cl_close = prices[-1]
                                if cl_open and cl_open > 0:
                                    cl_move = round(((cl_close - cl_open) / cl_open) * 100, 6)
                    except Exception:
                        pass

                    rows.append({
                        "window_ts": window_ts,
                        "asset": asset,
                        "chainlink_open": cl_open,
                        "chainlink_close": cl_close,
                        "chainlink_move_pct": cl_move,
                        "actual_direction": actual_direction,
                        "ltp_at_80pct": ltp_80,
                        "flow_direction_at_80pct": flow_80,
                        "btc_direction": btc_direction,
                        "btc_move_pct": btc_move_pct,
                    })

                except Exception as e:
                    logger.debug(f"MultiAssetCollector: Settlement check failed for {asset}: {e}")

        if rows:
            try:
                self._supabase.table("multi_asset_settlements").upsert(
                    rows, on_conflict="window_ts,asset"
                ).execute()
                logger.info(f"MultiAssetCollector: Settled {len(rows)} assets for window {window_ts}")
            except Exception as e:
                logger.error(f"MultiAssetCollector: Failed to write settlements: {e}")

    async def _refresh_markets(self, window_ts: int):
        """Find current 5-min market for each asset via Gamma API slug."""
        import httpx

        self._markets.clear()

        async with httpx.AsyncClient(timeout=10.0) as client:
            for asset in ASSETS:
                slug = f"{asset}-updown-5m-{window_ts}"
                try:
                    resp = await client.get(
                        f"{GAMMA_API_BASE}/events",
                        params={"slug": slug},
                    )
                    if resp.status_code != 200:
                        continue

                    events = resp.json()
                    if not events or not isinstance(events, list) or len(events) == 0:
                        continue

                    event = events[0]
                    markets = event.get("markets", [])
                    for m in markets:
                        if not m.get("active", False):
                            continue

                        cid = m.get("conditionId", "")
                        question = m.get("question", "")

                        # Parse token IDs
                        token_ids_raw = m.get("clobTokenIds", "[]")
                        outcomes_raw = m.get("outcomes", "[]")
                        if isinstance(token_ids_raw, str):
                            try:
                                token_ids = json.loads(token_ids_raw)
                            except Exception:
                                token_ids = []
                        else:
                            token_ids = token_ids_raw
                        if isinstance(outcomes_raw, str):
                            try:
                                outcomes = json.loads(outcomes_raw)
                            except Exception:
                                outcomes = []
                        else:
                            outcomes = outcomes_raw

                        yes_tid = no_tid = None
                        if len(token_ids) >= 2 and len(outcomes) >= 2:
                            for i, o in enumerate(outcomes):
                                o_lower = str(o).strip().lower()
                                if o_lower in ("up", "yes"):
                                    yes_tid = token_ids[i]
                                elif o_lower in ("down", "no"):
                                    no_tid = token_ids[i]

                        if yes_tid and no_tid:
                            self._markets[asset] = AssetMarket(
                                asset=asset,
                                condition_id=cid,
                                yes_token_id=yes_tid,
                                no_token_id=no_tid,
                                question=question,
                                window_ts=window_ts,
                            )
                            break

                except Exception as e:
                    logger.debug(f"MultiAssetCollector: Market scan failed for {asset}: {e}")

        logger.info(
            f"MultiAssetCollector: Found markets for "
            f"{list(self._markets.keys())} (window {window_ts})"
        )

    def get_diagnostics(self) -> Dict:
        return {
            "tracked_assets": list(self._markets.keys()),
            "cycle_count": self._cycle_count,
            "settled_windows": len(self._settled_windows),
            "seen_hashes": len(self._seen_trade_hashes),
        }
