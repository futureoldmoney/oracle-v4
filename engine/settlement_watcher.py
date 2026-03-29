"""
Settlement Watcher
===================
Monitors BTC 5-min market resolutions and closes the feedback loop:
  Order placed → Market resolves → Outcome determined → 
  Signal correctness updated → Brain can analyze

Two resolution methods:
1. Chainlink final price read (compare to window open price)
2. Polymarket API market status check (tokens[].winner)

Method 2 is preferred because it's authoritative — Polymarket 
decides the winner, not our interpretation of Chainlink data.

Runs as a background task that checks recently-closed markets
every 10-30 seconds.
"""

import time
import logging
from typing import Optional, Dict, List, Tuple
from collections import deque

from settlement_logic import compute_settlement, compute_pnl

logger = logging.getLogger("oracle.settlement")


class SettlementWatcher:
    """
    Watches for market settlements and updates trade outcomes.
    
    Lifecycle:
    1. Executor places order → watcher.track(market_id, ensemble_id, direction)
    2. Market settles → watcher detects via CLOB API or WebSocket market_resolved
    3. Watcher determines outcome (UP or DOWN won)
    4. Calls back to orchestrator + supabase_logger with result
    
    Usage:
        watcher = SettlementWatcher(clob_client)
        watcher.track("0xcondition...", "ensemble-uuid", "UP", order_id="0xorder...")
        
        # In main loop:
        settlements = watcher.check_settlements()
        for s in settlements:
            orchestrator.log_settlement(s["actual_outcome"], s["won"], s["pnl"])
    """

    def __init__(
        self,
        clob_client,
        check_interval_seconds: float = 30.0,
        max_tracked: int = 100,
    ):
        self._client = clob_client
        self._check_interval = check_interval_seconds
        self._last_check = 0.0

        # Tracked pending markets: condition_id → tracking info
        self._pending: Dict[str, Dict] = {}

        # Recently settled (for deduplication)
        self._settled_ids: deque = deque(maxlen=max_tracked)

    def track(
        self,
        condition_id: str,
        ensemble_id: str,
        direction: str,
        order_id: Optional[str] = None,
        entry_price: float = 0.50,
        size_usd: float = 1.0,
        token_id: Optional[str] = None,
        window_end_ts: Optional[int] = None,
        contributing_signals: Optional[List] = None,
    ):
        """
        Start tracking a market for settlement.

        Args:
            condition_id: Market condition ID
            ensemble_id: UUID from ensemble_log
            direction: "UP" or "DOWN" — our bet
            order_id: Polymarket order ID (if filled)
            entry_price: Price we paid
            size_usd: Amount risked
            token_id: Token ID we hold
            window_end_ts: Unix timestamp when the 5-min window closes
            contributing_signals: Signal snapshots for feedback loop at settlement
        """
        if condition_id in self._pending:
            logger.debug(f"Settlement: Already tracking {condition_id[:16]}")
            return

        self._pending[condition_id] = {
            "condition_id": condition_id,
            "ensemble_id": ensemble_id,
            "direction": direction,
            "order_id": order_id,
            "entry_price": entry_price,
            "size_usd": size_usd,
            "token_id": token_id,
            "tracked_at": time.time(),
            "window_end_ts": window_end_ts,
            "contributing_signals": contributing_signals or [],  # For feedback loop at settlement
        }
        logger.info(f"Settlement: Tracking {condition_id[:16]}... (direction={direction})")

    def check_settlements(self) -> List[Dict]:
        """
        Check all tracked markets for resolution.

        Checks every 30 seconds after window close.
        Never times out — keeps retrying until Polymarket resolves.

        Returns list of settlement results, each containing:
        - condition_id, ensemble_id, actual_outcome, won, pnl, direction
        """
        now = time.time()
        if now - self._last_check < self._check_interval:
            return []

        self._last_check = now
        results = []

        for cid in list(self._pending.keys()):
            if cid in self._settled_ids:
                continue

            info = self._pending[cid]
            window_end = info.get("window_end_ts")
            window_closed = window_end and now > window_end

            # Skip markets whose window hasn't closed yet
            if not window_closed:
                continue

            outcome = self._check_market_resolution(cid)

            if outcome is None:
                # Log a warning after 5 min, but keep retrying
                wait_time = now - window_end if window_end else 0
                if wait_time > 300 and not info.get("_warned_long_wait"):
                    logger.warning(
                        f"Settlement: {cid[:16]}... still unresolved "
                        f"after {int(wait_time)}s — will keep checking"
                    )
                    info["_warned_long_wait"] = True
                continue

            # Determine if we won using corrected side+outcome logic
            actual_outcome, winning_token = outcome
            our_direction = info["direction"]
            side = "YES" if our_direction == "UP" else "NO"

            # Primary: token_id match (most reliable)
            our_token = info.get("token_id")
            if our_token and winning_token:
                won = (our_token == winning_token)
            else:
                # Fallback: use corrected side+outcome formula
                result_check = compute_settlement(side, actual_outcome)
                won = result_check["won"]

            # Calculate P&L using actual fill price
            entry = info["entry_price"]
            size = info["size_usd"]
            pnl_data = compute_pnl(won=won, fill_price=entry, size_usd=size, taker_fee_rate=0.0156)

            result = {
                "condition_id": cid,
                "ensemble_id": info["ensemble_id"],
                "actual_outcome": actual_outcome,
                "direction": our_direction,
                "won": won,
                "pnl": pnl_data["net_pnl"],
                "entry_price": entry,
                "size_usd": size,
                "order_id": info.get("order_id"),
                "timed_out": False,
                "contributing_signals": info.get("contributing_signals", []),
            }
            results.append(result)

            self._settled_ids.append(cid)
            del self._pending[cid]

            outcome_emoji = "✅" if won else "❌"
            logger.info(
                f"Settlement: {outcome_emoji} {cid[:16]}... "
                f"bet={our_direction}, actual={actual_outcome}, "
                f"pnl=${pnl_data['net_pnl']:+.4f}"
            )

        return results

    def _check_market_resolution(self, condition_id: str) -> Optional[Tuple[str, str]]:
        """
        Check if a market has resolved via CLOB API.
        
        Returns:
            ("UP"/"DOWN", winning_token_id) if resolved, None if still open
        """
        try:
            market = self._client.get_market(condition_id)

            if not isinstance(market, dict):
                return None

            if not market.get("closed", False):
                return None

            # Find winning token
            tokens = market.get("tokens", [])
            winner = None
            for token in tokens:
                if token.get("winner", False):
                    winner = token
                    break

            if winner is None:
                return None

            # Determine direction from outcome
            # Polymarket uses "Yes"/"No" for standard markets, "Up"/"Down" for BTC 5-min
            outcome_str = winner.get("outcome", "").upper()
            if outcome_str in ("YES", "UP"):
                return ("UP", winner.get("token_id", ""))
            elif outcome_str in ("NO", "DOWN"):
                return ("DOWN", winner.get("token_id", ""))
            else:
                logger.warning(f"Settlement: Unknown outcome '{outcome_str}' for {condition_id[:16]}")
                return None

        except Exception as e:
            logger.debug(f"Settlement: Check failed for {condition_id[:16]}: {e}")
            return None

    def handle_market_resolved_ws(self, msg: Dict):
        """
        Handle market_resolved WebSocket event for instant settlement detection.
        
        Expected msg format (from Polymarket WS custom_feature_enabled):
        {
            "event_type": "market_resolved",
            "market": "0xcondition...",
            "winning_asset_id": "7604307...",
            "winning_outcome": "Yes",
            "timestamp": "1766790415550"
        }
        """
        condition_id = msg.get("market", "")
        winning_outcome = msg.get("winning_outcome", "").upper()

        if condition_id not in self._pending:
            return  # Not tracking this market

        if winning_outcome == "YES":
            actual = "UP"
        elif winning_outcome == "NO":
            actual = "DOWN"
        else:
            return

        info = self._pending[condition_id]

        # Use corrected settlement logic
        side = "YES" if info["direction"] == "UP" else "NO"
        our_token = info.get("token_id")
        winning_asset = msg.get("winning_asset_id")
        if our_token and winning_asset:
            won = (our_token == winning_asset)
        else:
            result_check = compute_settlement(side, actual)
            won = result_check["won"]

        entry = info["entry_price"]
        size = info["size_usd"]
        pnl_data = compute_pnl(won=won, fill_price=entry, size_usd=size, taker_fee_rate=0.0156)

        result = {
            "condition_id": condition_id,
            "ensemble_id": info["ensemble_id"],
            "actual_outcome": actual,
            "direction": info["direction"],
            "won": won,
            "pnl": pnl_data["net_pnl"],
            "entry_price": entry,
            "size_usd": size,
            "order_id": info.get("order_id"),
        }

        self._settled_ids.append(condition_id)
        del self._pending[condition_id]

        emoji = "✅" if won else "❌"
        logger.info(f"Settlement (WS): {emoji} {condition_id[:16]}... pnl=${pnl_data['net_pnl']:+.4f}")

        return result

    # ── Diagnostics ───────────────────────────────────────

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def get_stats(self) -> Dict:
        return {
            "pending_markets": self.pending_count,
            "settled_total": len(self._settled_ids),
            "tracked_markets": list(self._pending.keys()),
        }

    def get_pending_ensemble_ids(self) -> List[str]:
        """Return ensemble IDs that are still pending settlement."""
        return [info["ensemble_id"] for info in self._pending.values()]
