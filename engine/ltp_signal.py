"""
LTP Signal + Tick Velocity
===========================
Polls Polymarket CLOB API for last-trade-price data and computes
tick velocity (how fast the probability is moving).

Two outputs:
1. LTP confirmation: does the market agree with the oracle?
2. Tick velocity: how fast is the market repricing?
   - High velocity → market repricing fast → must use taker
   - Low velocity → market hasn't noticed → can try maker

Ported velocity math from: aulekator/tick_velocity_processor.py
"""

import time
import logging
from collections import deque
from typing import Optional, Dict, Tuple

import httpx

logger = logging.getLogger("oracle.ltp")

CLOB_BASE = "https://clob.polymarket.com"
MIN_POLL_INTERVAL = 1.0  # Max 1 call per second per token


class LTPSignal:
    """
    CLOB last-trade-price signal with velocity tracking.

    Usage:
        ltp = LTPSignal()
        price = await ltp.get_ltp(token_id)
        confirms = ltp.confirms_oracle("UP", price)
        velocity = ltp.get_velocity_30s(token_id)
        exec_mode = ltp.get_execution_recommendation(token_id)
    """

    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None
        # Rolling LTP history per token: deque of (timestamp, price)
        self._history: Dict[str, deque] = {}
        self._last_fetch: Dict[str, float] = {}
        self._max_history = 120  # ~2 minutes at 1/sec

    async def get_ltp(self, token_id: str) -> Optional[float]:
        """
        Fetch last trade price from CLOB book endpoint.

        Rate limited to 1 call per second per token.

        Args:
            token_id: YES token ID

        Returns:
            Last trade price (0-1), or None if unavailable.
        """
        if not token_id:
            return None

        now = time.time()
        last = self._last_fetch.get(token_id, 0)
        if now - last < MIN_POLL_INTERVAL:
            # Return most recent cached value
            hist = self._history.get(token_id)
            if hist:
                return hist[-1][1]
            return None

        try:
            if self._client is None:
                self._client = httpx.AsyncClient(timeout=5.0)

            resp = await self._client.get(
                f"{CLOB_BASE}/book",
                params={"token_id": token_id},
            )
            resp.raise_for_status()
            data = resp.json()

            ltp = data.get("last_trade_price")
            if ltp is not None:
                ltp = float(ltp)
                self._record(token_id, now, ltp)
                self._last_fetch[token_id] = now
                return ltp

        except Exception as e:
            logger.debug(f"LTP fetch failed for {token_id[:16]}…: {e}")
            self._last_fetch[token_id] = now

        # Return last known
        hist = self._history.get(token_id)
        return hist[-1][1] if hist else None

    def _record(self, token_id: str, ts: float, price: float):
        """Record a price observation for velocity tracking."""
        if token_id not in self._history:
            self._history[token_id] = deque(maxlen=self._max_history)
        self._history[token_id].append((ts, price))

    def confirms_oracle(self, oracle_direction: str, ltp: Optional[float]) -> Optional[bool]:
        """
        Check if LTP confirms the oracle direction.

        Returns:
            True if confirms, False if contradicts, None if neutral/unknown.
        """
        if ltp is None:
            return None
        if oracle_direction == "UP" and ltp > 0.55:
            return True
        if oracle_direction == "DOWN" and ltp < 0.45:
            return True
        if oracle_direction == "UP" and ltp < 0.42:
            return False
        if oracle_direction == "DOWN" and ltp > 0.58:
            return False
        return None  # Neutral zone

    def get_velocity_30s(self, token_id: str) -> float:
        """
        Compute 30-second price velocity.

        Returns:
            Rate of change as decimal (e.g. 0.02 = 2% move in 30s).
            Positive = moving up, negative = moving down.
        """
        return self._compute_velocity(token_id, 30.0)

    def get_velocity_60s(self, token_id: str) -> float:
        """Compute 60-second price velocity."""
        return self._compute_velocity(token_id, 60.0)

    def get_acceleration(self, token_id: str) -> float:
        """
        Compute acceleration: is the move speeding up or slowing down?

        Returns:
            Positive = accelerating in current direction.
            Negative = decelerating / reversing.
        """
        v30 = self.get_velocity_30s(token_id)
        v60 = self.get_velocity_60s(token_id)
        # Acceleration = recent velocity minus older velocity
        # v60 covers 60s. v30 covers the latest 30s.
        # The "older 30s" velocity = v60 - v30 (approximately)
        older_v30 = v60 - v30
        return v30 - older_v30

    def get_execution_recommendation(self, token_id: str) -> str:
        """
        Recommend execution mode based on tick velocity.

        Returns:
            "TAKER" — market moving fast, need instant fill
            "ADAPTIVE" — market slow, can try maker first
            "SKIP" — no data
        """
        v30 = abs(self.get_velocity_30s(token_id))
        if v30 == 0:
            hist = self._history.get(token_id)
            if not hist or len(hist) < 5:
                return "SKIP"  # Not enough data
            return "ADAPTIVE"  # Market stable, can try maker

        if v30 > 0.02:  # >2% move in 30s — fast market
            return "TAKER"
        return "ADAPTIVE"

    def _compute_velocity(self, token_id: str, window_seconds: float) -> float:
        """Compute price velocity over the given window."""
        hist = self._history.get(token_id)
        if not hist or len(hist) < 3:
            return 0.0

        now = time.time()
        current_price = hist[-1][1]

        # Find the price closest to window_seconds ago
        target_time = now - window_seconds
        past_price = None
        for ts, price in hist:
            if ts <= target_time:
                past_price = price
            else:
                break

        if past_price is None or past_price == 0:
            return 0.0

        return (current_price - past_price) / past_price

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None
