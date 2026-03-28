"""
Deribit Put/Call Ratio Signal
==============================
Fetches real-time BTC options data from Deribit (free API, no auth).

Short-dated put/call ratio is a genuine institutional sentiment indicator:
  PCR > 1.2 → More puts than calls = FEAR → contrarian BULLISH
  PCR < 0.7 → More calls than puts = GREED → contrarian BEARISH

We look at options expiring within 2 days — most sensitive to
near-term price movements, ideal for 5-minute trading.

Ported from: aulekator/signal_processors/deribit_pcr_processor.py
Adapted to return our oracle amplifier format.
"""

import time
import logging
from typing import Optional, Dict
from datetime import datetime, timezone

import httpx

logger = logging.getLogger("oracle.deribit")

DERIBIT_URL = "https://www.deribit.com/api/v2/public/get_book_summary_by_currency"
CACHE_SECONDS = 300  # 5 minutes — options don't change tick-by-tick
MAX_DAYS_TO_EXPIRY = 2


class DeribitPCR:
    """
    Deribit BTC options put/call ratio signal.

    Usage:
        pcr = DeribitPCR()
        signal = await pcr.get_signal()
        # signal = {"direction": "UP", "strength": 0.8, "pcr": 1.45, "skew": 0.05}
    """

    def __init__(
        self,
        bullish_pcr: float = 1.20,
        bearish_pcr: float = 0.70,
    ):
        self.bullish_pcr = bullish_pcr
        self.bearish_pcr = bearish_pcr
        self._cache: Optional[Dict] = None
        self._cache_time: float = 0
        self._client: Optional[httpx.AsyncClient] = None

    async def get_signal(self) -> Optional[Dict]:
        """
        Get the current PCR signal.

        Returns:
            Dict with keys: direction, strength, pcr, put_oi, call_oi
            or None if data unavailable.
        """
        now = time.time()
        if self._cache and (now - self._cache_time) < CACHE_SECONDS:
            return self._cache

        try:
            if self._client is None:
                self._client = httpx.AsyncClient(timeout=10.0)

            resp = await self._client.get(
                DERIBIT_URL,
                params={"currency": "BTC", "kind": "option"},
            )
            resp.raise_for_status()
            data = resp.json()

            instruments = data.get("result", [])
            if not instruments:
                return self._cache

            # Filter to short-dated options only
            put_oi = 0.0
            call_oi = 0.0
            now_dt = datetime.now(timezone.utc)

            for inst in instruments:
                name = inst.get("instrument_name", "")
                oi = float(inst.get("open_interest", 0))
                if oi <= 0:
                    continue

                # Parse expiry from instrument name: BTC-28MAR26-95000-P
                parts = name.split("-")
                if len(parts) < 4:
                    continue

                option_type = parts[-1]  # P or C
                try:
                    expiry_str = parts[1]
                    expiry = datetime.strptime(expiry_str, "%d%b%y")
                    expiry = expiry.replace(tzinfo=timezone.utc)
                    days_to_expiry = (expiry - now_dt).days
                except (ValueError, IndexError):
                    continue

                if days_to_expiry > MAX_DAYS_TO_EXPIRY:
                    continue

                if option_type == "P":
                    put_oi += oi
                elif option_type == "C":
                    call_oi += oi

            if call_oi == 0:
                return self._cache

            pcr = put_oi / call_oi

            # Determine direction (contrarian)
            if pcr > self.bullish_pcr:
                direction = "UP"
                strength = min(1.0, (pcr - self.bullish_pcr) / 0.5)
            elif pcr < self.bearish_pcr:
                direction = "DOWN"
                strength = min(1.0, (self.bearish_pcr - pcr) / 0.3)
            else:
                direction = "NEUTRAL"
                strength = 0.0

            signal = {
                "direction": direction,
                "strength": round(strength, 3),
                "pcr": round(pcr, 3),
                "put_oi": round(put_oi, 1),
                "call_oi": round(call_oi, 1),
            }

            self._cache = signal
            self._cache_time = now
            logger.debug(
                f"Deribit PCR: {pcr:.3f} ({direction}, "
                f"strength={strength:.2f}, puts={put_oi:.0f}, calls={call_oi:.0f})"
            )
            return signal

        except Exception as e:
            logger.debug(f"Deribit PCR fetch failed: {e}")
            return self._cache

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None
