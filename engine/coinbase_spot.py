"""
Coinbase Spot Price
====================
Simple REST GET for BTC-USD spot price from Coinbase.
Used to cross-validate Chainlink magnitude — take the more
conservative of the two when they disagree.

Free API, no auth required, rate limited to 1 call per 10 seconds.
"""

import time
import logging
from typing import Optional

import httpx

logger = logging.getLogger("oracle.coinbase")

COINBASE_URL = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"
MIN_POLL_INTERVAL = 10.0


class CoinbaseSpot:
    """Fetches BTC-USD spot price from Coinbase."""

    def __init__(self):
        self._last_price: Optional[float] = None
        self._last_fetch: float = 0
        self._client: Optional[httpx.AsyncClient] = None
        self._consecutive_errors = 0

    async def get_price(self) -> Optional[float]:
        """
        Get current BTC-USD spot price.

        Rate limited to 1 call per 10 seconds. Returns cached price
        if called more frequently.

        Returns:
            BTC price in USD, or None if unavailable.
        """
        now = time.time()
        if now - self._last_fetch < MIN_POLL_INTERVAL:
            return self._last_price

        try:
            if self._client is None:
                self._client = httpx.AsyncClient(
                    timeout=5.0,
                    headers={"User-Agent": "OracleBot/4.0"},
                )

            resp = await self._client.get(COINBASE_URL)
            resp.raise_for_status()
            data = resp.json()

            price = float(data.get("price", 0))
            if price > 0:
                self._last_price = price
                self._last_fetch = now
                self._consecutive_errors = 0
                return price

        except Exception as e:
            self._consecutive_errors += 1
            if self._consecutive_errors <= 3:
                logger.debug(f"Coinbase fetch failed ({self._consecutive_errors}): {e}")
            elif self._consecutive_errors == 10:
                logger.warning(f"Coinbase down: 10 consecutive failures")
            self._last_fetch = now  # Don't retry immediately

        return self._last_price

    async def close(self):
        """Clean up HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
