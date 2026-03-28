"""
BTC Sentiment Tracker
======================
Multi-timeframe BTC directional sentiment from free public APIs.

Provides a background bias that adjusts oracle confidence by ±0.03:
- If oracle says UP and daily/weekly trend is UP → small confidence boost
- If oracle says UP but all timeframes say DOWN → small confidence penalty

Data sources (all free, no auth):
1. CoinGecko hourly prices → weekly, daily, 4H, 1H trends
2. Alternative.me Fear & Greed Index → daily sentiment
3. CryptoCompare news → headline sentiment (basic)

This is deliberately capped at ±3% influence — it's a background
bias, not a primary signal. It should never override the oracle.
"""

import time
import math
import logging
from dataclasses import dataclass
from typing import Optional, Dict, List

import httpx

logger = logging.getLogger("oracle.sentiment")

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
FNG_URL = "https://api.alternative.me/fng/"
NEWS_URL = "https://min-api.cryptocompare.com/data/v2/news/"


@dataclass
class TimeframeBias:
    timeframe: str          # "1W", "1D", "4H", "1H"
    direction: str          # "UP", "DOWN", "NEUTRAL"
    strength: float         # 0.0 to 1.0
    price_change_pct: float


@dataclass
class SentimentSnapshot:
    timestamp: float
    biases: Dict[str, TimeframeBias]
    fear_greed_index: int           # 0-100
    fear_greed_trend: str           # "rising", "falling", "stable"
    composite_bias: str             # "BULLISH", "BEARISH", "NEUTRAL"
    composite_strength: float       # 0.0 to 1.0


class BTCSentimentTracker:
    """
    Background task that maintains multi-timeframe BTC sentiment.

    Update schedule:
      - Price biases: every 15 minutes (CoinGecko)
      - Fear & Greed: every 6 hours
      - News: every 30 minutes

    Usage:
        tracker = BTCSentimentTracker()
        await tracker.update()
        adjustment = tracker.get_confidence_adjustment("UP")
        # Returns -0.03 to +0.03
    """

    def __init__(self):
        self._snapshot: Optional[SentimentSnapshot] = None
        self._hourly_prices: List[float] = []
        self._fng_values: List[int] = []
        self._last_price_update = 0
        self._last_fng_update = 0
        self._client: Optional[httpx.AsyncClient] = None

    async def update(self):
        """Periodic update — call from a background task loop."""
        now = time.time()

        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=15.0,
                headers={"User-Agent": "OracleBot/4.0"},
            )

        # Price data (CoinGecko) — every 15 min
        if now - self._last_price_update > 900:
            await self._update_prices()
            self._last_price_update = now

        # Fear & Greed — every 6 hours
        if now - self._last_fng_update > 21600:
            await self._update_fear_greed()
            self._last_fng_update = now

        # Build snapshot
        self._build_snapshot()

    def get_snapshot(self) -> Optional[SentimentSnapshot]:
        return self._snapshot

    def get_confidence_adjustment(self, oracle_direction: str) -> float:
        """
        Returns a confidence adjustment (-0.03 to +0.03) based on
        alignment of oracle direction with multi-timeframe trend.
        """
        if not self._snapshot or not self._snapshot.biases:
            return 0.0

        weights = {"1W": 0.15, "1D": 0.35, "4H": 0.30, "1H": 0.20}
        weighted_alignment = 0.0

        for tf, bias in self._snapshot.biases.items():
            w = weights.get(tf, 0.1)
            if bias.direction == oracle_direction:
                weighted_alignment += w * bias.strength
            elif bias.direction != "NEUTRAL":
                weighted_alignment -= w * bias.strength

        # Scale to [-0.03, +0.03]
        adjustment = max(-0.03, min(0.03, weighted_alignment * 0.06))
        return round(adjustment, 4)

    async def _update_prices(self):
        """Fetch hourly BTC prices from CoinGecko (7 days)."""
        try:
            resp = await self._client.get(
                COINGECKO_URL,
                params={"vs_currency": "usd", "days": "7"},
            )
            resp.raise_for_status()
            data = resp.json()

            prices = data.get("prices", [])
            if prices:
                self._hourly_prices = [p[1] for p in prices]
                logger.debug(
                    f"CoinGecko: {len(self._hourly_prices)} hourly prices, "
                    f"latest=${self._hourly_prices[-1]:,.0f}"
                )
        except Exception as e:
            logger.debug(f"CoinGecko fetch failed: {e}")

    async def _update_fear_greed(self):
        """Fetch Fear & Greed Index history."""
        try:
            resp = await self._client.get(
                FNG_URL, params={"limit": "7"},
            )
            resp.raise_for_status()
            data = resp.json()

            fng_data = data.get("data", [])
            if fng_data:
                self._fng_values = [int(d.get("value", 50)) for d in fng_data]
                logger.debug(
                    f"Fear & Greed: current={self._fng_values[0]}, "
                    f"7d avg={sum(self._fng_values)//len(self._fng_values)}"
                )
        except Exception as e:
            logger.debug(f"Fear & Greed fetch failed: {e}")

    def _build_snapshot(self):
        """Build sentiment snapshot from collected data."""
        biases = {}

        if self._hourly_prices and len(self._hourly_prices) > 4:
            current = self._hourly_prices[-1]

            for tf, hours_back in [("1W", 168), ("1D", 24), ("4H", 4), ("1H", 1)]:
                idx = -(hours_back + 1)
                if abs(idx) <= len(self._hourly_prices):
                    past = self._hourly_prices[idx]
                    change_pct = (current - past) / past * 100

                    if change_pct > 0.5:
                        direction = "UP"
                    elif change_pct < -0.5:
                        direction = "DOWN"
                    else:
                        direction = "NEUTRAL"

                    strength = min(1.0, abs(change_pct) / 5.0)
                    biases[tf] = TimeframeBias(tf, direction, round(strength, 3), round(change_pct, 3))

        # Fear & Greed
        fng = self._fng_values[0] if self._fng_values else 50
        fng_trend = "stable"
        if len(self._fng_values) >= 3:
            recent_avg = sum(self._fng_values[:3]) / 3
            older_avg = sum(self._fng_values[3:]) / max(1, len(self._fng_values[3:]))
            if recent_avg > older_avg + 5:
                fng_trend = "rising"
            elif recent_avg < older_avg - 5:
                fng_trend = "falling"

        # Composite
        if biases:
            up_score = sum(b.strength for b in biases.values() if b.direction == "UP")
            down_score = sum(b.strength for b in biases.values() if b.direction == "DOWN")
            if up_score > down_score + 0.2:
                composite = "BULLISH"
                comp_strength = min(1.0, up_score - down_score)
            elif down_score > up_score + 0.2:
                composite = "BEARISH"
                comp_strength = min(1.0, down_score - up_score)
            else:
                composite = "NEUTRAL"
                comp_strength = 0.0
        else:
            composite = "NEUTRAL"
            comp_strength = 0.0

        self._snapshot = SentimentSnapshot(
            timestamp=time.time(),
            biases=biases,
            fear_greed_index=fng,
            fear_greed_trend=fng_trend,
            composite_bias=composite,
            composite_strength=round(comp_strength, 3),
        )

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None
