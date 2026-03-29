"""
V4 Executor
============
Translates ensemble/oracle decisions into Polymarket orders.

v4 changes:
- Taker-first execution (postOnly=False) for guaranteed fills
- Oracle strategy controls all trade decisions
- Dynamic pricing from fair_value model (not ensemble confidence)
- Logs execution_mode (TAKER / MAKER_FILLED / TAKER_FALLBACK)

Uses py_clob_client v0.34.6 with signature_type=0 (EOA direct).
"""

import os
import time
import logging
from typing import Optional, Dict, List, Set
from collections import deque
from dataclasses import dataclass, field

logger = logging.getLogger("oracle.executor")

# ═══════════════════════════════════════════════════════════════
# SAFETY GATE CONSTANTS
# ═══════════════════════════════════════════════════════════════

MAX_ENTRY_PRICE = 0.60    # Reject any trade where entry price exceeds this cap
MAX_ENTRY_SECONDS = 120   # Reject any trade with more than this many seconds remaining


# ═══════════════════════════════════════════════════════════════
# DATA
# ═══════════════════════════════════════════════════════════════

@dataclass
class OpenOrder:
    """Tracked open order on Polymarket."""
    order_id: str
    market_id: str
    token_id: str
    side: str            # "YES" or "NO"
    direction: str       # "UP" or "DOWN"
    price: float
    size_usd: float
    placed_at: float
    ensemble_id: str
    fill_status: str = "OPEN"     # OPEN, FILLED, PARTIAL, CANCELLED, EXPIRED, ERROR
    fill_price: Optional[float] = None
    fill_shares: Optional[float] = None


@dataclass
class ExecutionResult:
    """Result of an order placement attempt."""
    success: bool
    order_id: Optional[str]
    error_msg: str
    fill_status: str
    placed_at: float = 0.0


# ═══════════════════════════════════════════════════════════════
# RATE LIMITER
# ═══════════════════════════════════════════════════════════════

class RateLimiter:
    """Sliding-window rate limiter. Polymarket allows 60 req/min."""

    def __init__(self, max_requests: int = 50, window_seconds: float = 60.0):
        self._max = max_requests
        self._window = window_seconds
        self._timestamps: deque = deque()

    def can_request(self) -> bool:
        self._prune()
        return len(self._timestamps) < self._max

    def record(self):
        self._timestamps.append(time.time())

    def _prune(self):
        cutoff = time.time() - self._window
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()

    @property
    def remaining(self) -> int:
        self._prune()
        return max(0, self._max - len(self._timestamps))


# ═══════════════════════════════════════════════════════════════
# EXECUTOR
# ═══════════════════════════════════════════════════════════════

class V4Executor:
    """
    Places orders on Polymarket from ensemble decisions.
    
    Usage:
        executor = V4Executor(clob_client)
        result = executor.execute(decision, market_info, ensemble_id)
    """

    def __init__(
        self,
        clob_client,
        signature_type: int = 0,  # v4: EOA direct (sig_type=2 causes auth failures)
        max_open_orders: int = 3,
        cancel_before_close_seconds: int = 15,
        use_post_only: bool = False,  # v4: taker-first for guaranteed fills
    ):
        """
        Args:
            clob_client: Initialized py_clob_client ClobClient
            signature_type: Polymarket signature type (2 for funded wallets)
            max_open_orders: Maximum concurrent open orders
            cancel_before_close_seconds: Cancel orders this many seconds before market close
            use_post_only: Use postOnly for maker rebates (True except for stale snipes)
        """
        self._client = clob_client
        self._signature_type = signature_type
        self._max_open = max_open_orders
        self._cancel_threshold = cancel_before_close_seconds
        self._use_post_only = use_post_only

        self._open_orders: Dict[str, OpenOrder] = {}  # order_id → OpenOrder
        self._order_on_market: Set[str] = set()  # market_ids with open orders
        self._rate_limiter = RateLimiter(max_requests=50, window_seconds=60.0)

        # Fail streak tracking for backoff
        self._fail_streak = 0
        self._backoff_until = 0.0

    def execute(
        self,
        decision,  # EnsembleDecision
        market_info: Dict,
        ensemble_id: Optional[str] = None,
        oracle_confirmed: bool = False,
    ) -> ExecutionResult:
        """
        Execute an ensemble decision as a Polymarket order.
        
        Args:
            decision: EnsembleDecision from the ensemble engine
            market_info: Dict with keys:
                - condition_id: str
                - yes_token_id: str
                - no_token_id: str
                - tick_size: str (e.g. "0.01")
                - neg_risk: bool
                - seconds_before_close: int
            ensemble_id: UUID linking to ensemble_log
        
        Returns:
            ExecutionResult
        """
        if not decision.should_trade:
            return ExecutionResult(
                success=False, order_id=None,
                error_msg="Decision is no-trade", fill_status="SKIPPED",
            )

        # Oracle confirmation gate: ALL trade paths require explicit oracle sign-off.
        # This prevents ensemble-only trades from slipping through if the oracle check
        # in the evaluation loop is ever bypassed.
        if not oracle_confirmed:
            logger.warning(
                "ORACLE GATE: Trade rejected — oracle_confirmed=False. "
                "All trade paths require explicit oracle confirmation before execution."
            )
            return ExecutionResult(
                success=False, order_id=None,
                error_msg="Oracle confirmation required (oracle_confirmed=False)",
                fill_status="SKIPPED",
            )

        # Pre-flight checks
        check = self._preflight_checks(market_info)
        if check:
            return check

        # Determine token and side
        direction = decision.direction.value if hasattr(decision.direction, 'value') else decision.direction
        if direction == "UP":
            token_id = market_info["yes_token_id"]
            side_label = "YES"
        elif direction == "DOWN":
            token_id = market_info["no_token_id"]
            side_label = "NO"
        else:
            return ExecutionResult(
                success=False, order_id=None,
                error_msg=f"Invalid direction: {direction}", fill_status="ERROR",
            )

        # Calculate order price
        # Oracle path: use validated fill_price from estimate_fill_price() directly
        # Ensemble path: compute from confidence skew (legacy)
        if hasattr(decision, 'fill_price') and decision.fill_price and decision.fill_price > 0:
            price = round(float(decision.fill_price), 2)
        else:
            price = self._compute_limit_price(decision, side_label)

        # Entry price cap gate: reject trades with unrealistically high entry prices.
        # A price above $0.60 leaves almost no margin and risks negative EV.
        if price > MAX_ENTRY_PRICE:
            logger.warning(
                f"PRICE CAP: Trade rejected — computed entry price ${price:.2f} "
                f"> MAX_ENTRY_PRICE ${MAX_ENTRY_PRICE:.2f}"
            )
            return ExecutionResult(
                success=False, order_id=None,
                error_msg=f"Entry price ${price:.2f} exceeds cap ${MAX_ENTRY_PRICE:.2f}",
                fill_status="SKIPPED",
            )

        # Oracle path uses size_usd; ensemble path uses recommended_size_usd
        if hasattr(decision, 'size_usd') and decision.size_usd and decision.size_usd > 0:
            size = decision.size_usd
        else:
            size = decision.recommended_size_usd

        # Decide postOnly — oracle trades always taker (guaranteed fill)
        is_stale_snipe = any(
            s.signal_name == "STALE_QUOTE"
            for s in (getattr(decision, 'contributing_signals', None) or [])
        )
        post_only = self._use_post_only and not is_stale_snipe

        logger.info(
            f"Executor: Placing {side_label} order @ {price:.2f}, "
            f"size=${size:.2f}, postOnly={post_only}"
        )

        # Place the order
        try:
            result = self._place_order(
                token_id=token_id,
                price=price,
                size=size,
                side_label=side_label,
                tick_size=str(market_info.get("tick_size", "0.01")),
                neg_risk=market_info.get("neg_risk", False),
                post_only=post_only,
            )

            if result.success and result.order_id:
                # Track open order
                order = OpenOrder(
                    order_id=result.order_id,
                    market_id=market_info["condition_id"],
                    token_id=token_id,
                    side=side_label,
                    direction=direction,
                    price=price,
                    size_usd=size,
                    placed_at=time.time(),
                    ensemble_id=ensemble_id or "",
                )
                self._open_orders[result.order_id] = order
                self._order_on_market.add(market_info["condition_id"])
                self._fail_streak = 0
                logger.info(f"Executor: Order placed → {result.order_id[:16]}...")

            else:
                self._fail_streak += 1
                if self._fail_streak >= 3:
                    self._backoff_until = time.time() + min(30 * self._fail_streak, 300)
                    logger.warning(f"Executor: {self._fail_streak} consecutive failures, "
                                   f"backing off until {self._backoff_until}")

            return result

        except Exception as e:
            self._fail_streak += 1
            logger.error(f"Executor: Order placement failed: {e}")
            return ExecutionResult(
                success=False, order_id=None,
                error_msg=str(e), fill_status="ERROR",
            )

    def _preflight_checks(self, market_info: Dict) -> Optional[ExecutionResult]:
        """Run pre-flight checks before placing an order."""
        now = time.time()

        # Backoff check
        if now < self._backoff_until:
            wait = self._backoff_until - now
            return ExecutionResult(
                success=False, order_id=None,
                error_msg=f"In backoff ({wait:.0f}s remaining)", fill_status="SKIPPED",
            )

        # Rate limit
        if not self._rate_limiter.can_request():
            return ExecutionResult(
                success=False, order_id=None,
                error_msg=f"Rate limited ({self._rate_limiter.remaining} remaining)",
                fill_status="SKIPPED",
            )

        # Max open orders
        if len(self._open_orders) >= self._max_open:
            return ExecutionResult(
                success=False, order_id=None,
                error_msg=f"Max open orders ({self._max_open}) reached",
                fill_status="SKIPPED",
            )

        # Duplicate on same market
        market_id = market_info.get("condition_id")
        if market_id in self._order_on_market:
            return ExecutionResult(
                success=False, order_id=None,
                error_msg=f"Already have order on market {market_id[:16]}...",
                fill_status="SKIPPED",
            )

        # Timing gates: only trade in the valid entry window
        seconds_left = market_info.get("seconds_before_close", 999)

        # Lower bound: too close to settlement
        if seconds_left < self._cancel_threshold:
            return ExecutionResult(
                success=False, order_id=None,
                error_msg=f"Too close to settlement ({seconds_left}s < {self._cancel_threshold}s)",
                fill_status="SKIPPED",
            )

        # Note: v4 — timing gates are handled by oracle_strategy.evaluate()
        # The executor trusts the oracle's decision on timing.

        return None  # All checks passed

    def _compute_limit_price(self, decision, side_label: str) -> float:
        """
        Compute limit price from ensemble confidence.
        
        Our confidence IS our estimate of the true probability.
        We want to buy below our estimate (skew for profit margin).
        
        Example: confidence=0.65 for UP → buy YES at 0.60 (5¢ skew)
                 confidence=0.65 for DOWN → buy NO at 0.60
        """
        conf = decision.aggregate_confidence
        
        # Skew: place limit below our confidence estimate to ensure profit
        # Larger kelly fraction = more confident = tighter skew
        kelly = decision.kelly_fraction
        skew = max(0.02, 0.08 - kelly * 2)  # 2-8 cents skew

        price = conf - skew
        price = max(0.01, min(0.99, round(price, 2)))

        return price

    def _place_order(
        self,
        token_id: str,
        price: float,
        size: float,
        side_label: str,
        tick_size: str,
        neg_risk: bool,
        post_only: bool,
    ) -> ExecutionResult:
        """
        Place order via py-clob-client.
        
        Uses create_order() + post_order() (two-step) per audit recommendations.
        """
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY

            # Polymarket minimum is 5 shares
            shares = round(size / price, 2) if price > 0 else 0
            if shares < 5:
                return ExecutionResult(
                    success=False, order_id=None,
                    error_msg=f"Below min size (shares={shares:.1f}, min=5)",
                    fill_status="SKIPPED",
                )

            self._rate_limiter.record()

            # Create signed order
            signed_order = self._client.create_order(
                OrderArgs(
                    token_id=token_id,
                    price=price,
                    size=size,
                    side=BUY,  # Always BUY the side we want
                ),
                options={
                    "tick_size": tick_size,
                    "neg_risk": neg_risk,
                },
            )

            # Post order
            self._rate_limiter.record()
            resp = self._client.post_order(
                signed_order,
                OrderType.GTC,
                post_only,
            )

            # Parse response
            if isinstance(resp, dict):
                success = resp.get("success", False)
                error_msg = resp.get("errorMsg", "")
                order_id = resp.get("orderID") or resp.get("orderId")
                status = resp.get("status", "UNKNOWN")

                # Handle known error codes
                if error_msg == "INVALID_POST_ONLY_ORDER":
                    # Would have crossed the book — retry without postOnly
                    if post_only:
                        logger.info("Executor: PostOnly rejected (would cross), retrying as taker")
                        return self._place_order(
                            token_id, price, size, side_label,
                            tick_size, neg_risk, post_only=False,
                        )

                if error_msg == "INVALID_ORDER_NOT_ENOUGH_BALANCE":
                    logger.warning("Executor: Insufficient balance")

                return ExecutionResult(
                    success=bool(success and order_id),
                    order_id=order_id,
                    error_msg=error_msg or "",
                    fill_status="OPEN" if success else "ERROR",
                    placed_at=time.time(),
                )
            else:
                return ExecutionResult(
                    success=False, order_id=None,
                    error_msg=f"Unexpected response type: {type(resp)}",
                    fill_status="ERROR",
                )

        except Exception as e:
            return ExecutionResult(
                success=False, order_id=None,
                error_msg=str(e), fill_status="ERROR",
            )

    # ── Order Management ──────────────────────────────────

    def cancel_expiring_orders(self, seconds_threshold: int = 15):
        """Cancel any orders on markets about to settle."""
        to_cancel = []
        for oid, order in self._open_orders.items():
            if order.fill_status == "OPEN":
                # In production, check market end time
                # For now, cancel orders older than 4 minutes (5min market - 1min buffer)
                age = time.time() - order.placed_at
                if age > 240:  # 4 minutes
                    to_cancel.append(oid)

        for oid in to_cancel:
            self._cancel_order(oid)

    def _cancel_order(self, order_id: str):
        """Cancel a single order."""
        try:
            if self._rate_limiter.can_request():
                self._rate_limiter.record()
                self._client.cancel(order_id)
                logger.info(f"Executor: Cancelled order {order_id[:16]}...")

            order = self._open_orders.get(order_id)
            if order:
                order.fill_status = "CANCELLED"
                self._order_on_market.discard(order.market_id)

        except Exception as e:
            logger.error(f"Executor: Cancel failed for {order_id[:16]}: {e}")

    def cancel_all(self):
        """Cancel all open orders (emergency)."""
        try:
            if self._rate_limiter.can_request():
                self._rate_limiter.record()
                self._client.cancel_all()
                logger.info("Executor: Cancelled ALL orders")

            for order in self._open_orders.values():
                order.fill_status = "CANCELLED"
            self._order_on_market.clear()

        except Exception as e:
            logger.error(f"Executor: Cancel all failed: {e}")

    def check_fills(self) -> List[OpenOrder]:
        """
        Check fill status of open orders.
        Returns list of orders whose status changed.
        """
        changed = []
        for oid, order in list(self._open_orders.items()):
            if order.fill_status != "OPEN":
                continue

            try:
                if not self._rate_limiter.can_request():
                    break

                self._rate_limiter.record()
                resp = self._client.get_order(oid)

                if isinstance(resp, dict):
                    status = resp.get("status", "").upper()
                    size_matched = float(resp.get("size_matched", 0))
                    original_size = float(resp.get("original_size", 0))

                    if status == "MATCHED" or (size_matched > 0 and size_matched >= original_size):
                        order.fill_status = "FILLED"
                        order.fill_price = float(resp.get("price", order.price))
                        order.fill_shares = size_matched
                        self._order_on_market.discard(order.market_id)
                        changed.append(order)
                        logger.info(f"Executor: Order {oid[:16]} FILLED @ {order.fill_price}")

                    elif size_matched > 0:
                        order.fill_status = "PARTIAL"
                        order.fill_shares = size_matched
                        changed.append(order)

                    elif status in ("CANCELLED", "EXPIRED"):
                        order.fill_status = status
                        self._order_on_market.discard(order.market_id)
                        changed.append(order)

            except Exception as e:
                logger.error(f"Executor: Fill check failed for {oid[:16]}: {e}")

        return changed

    def mark_settled(self, market_id: str, won: bool, pnl: float):
        """Mark orders on a settled market."""
        for order in self._open_orders.values():
            if order.market_id == market_id:
                if order.fill_status == "OPEN":
                    order.fill_status = "EXPIRED"
                self._order_on_market.discard(market_id)

    def get_open_orders(self) -> List[OpenOrder]:
        return [o for o in self._open_orders.values() if o.fill_status == "OPEN"]

    def get_balance(self) -> float:
        """Fetch actual USDC balance directly from the Polygon blockchain."""
        if self._client is None:
            return 0.0
        eoa = os.environ.get("POLYMARKET_FUNDER_ADDRESS", "")
        if not eoa:
            return 0.0
        from web3 import Web3
        polygon_rpc = os.environ.get("POLYGON_RPC_URL", "https://polygon-rpc.com")
        w3 = Web3(Web3.HTTPProvider(polygon_rpc))
        USDC_ADDR = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        USDC_ABI = [{"constant": True, "inputs": [{"name": "owner", "type": "address"}],
                     "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}], "type": "function"}]
        usdc = w3.eth.contract(address=Web3.to_checksum_address(USDC_ADDR), abi=USDC_ABI)
        raw = usdc.functions.balanceOf(Web3.to_checksum_address(eoa)).call()
        return raw / 1e6

    def get_stats(self) -> Dict:
        """Executor stats for diagnostics."""
        statuses = {}
        for o in self._open_orders.values():
            statuses[o.fill_status] = statuses.get(o.fill_status, 0) + 1

        return {
            "open_orders": len([o for o in self._open_orders.values() if o.fill_status == "OPEN"]),
            "total_tracked": len(self._open_orders),
            "status_breakdown": statuses,
            "rate_limit_remaining": self._rate_limiter.remaining,
            "fail_streak": self._fail_streak,
            "in_backoff": time.time() < self._backoff_until,
        }

    def build_trade_row(
        self,
        result: ExecutionResult,
        decision,  # EnsembleDecision
        market_info: Dict,
        ensemble_id: str,
        trade_enhancement: Dict,
    ) -> Dict:
        """
        Build a complete trade row for Supabase (live_trades or paper_trades).
        
        Merges v2 fields with v3 ensemble fields.
        """
        direction = decision.direction.value if hasattr(decision.direction, 'value') else decision.direction
        side_label = "YES" if direction == "UP" else "NO"

        # Price: oracle path uses fill_price directly; ensemble path recomputes
        if hasattr(decision, 'fill_price') and decision.fill_price and decision.fill_price > 0:
            limit_price = round(float(decision.fill_price), 4)
        else:
            limit_price = self._compute_limit_price(decision, side_label)

        # Size: oracle path uses size_usd; ensemble path uses recommended_size_usd
        if hasattr(decision, 'size_usd') and decision.size_usd and decision.size_usd > 0:
            size = decision.size_usd
        else:
            size = decision.recommended_size_usd

        # Confidence/edge: oracle path uses confidence + magnitude; ensemble uses aggregate_confidence
        oracle_confidence = getattr(decision, 'confidence', None) or getattr(decision, 'aggregate_confidence', None)
        edge_bps = int(getattr(decision, 'magnitude_pct', 0) * 100) or int(getattr(decision, 'weighted_edge_bps', 0))
        trade_reason = getattr(decision, 'reason', None)

        row = {
            "market_id": market_info.get("condition_id"),
            "market_question": market_info.get("question"),
            "token_id": market_info.get("yes_token_id") if direction == "UP" else market_info.get("no_token_id"),
            "decision": "TRADE" if result.success else "SKIP",
            "skip_reason": result.error_msg if not result.success else None,
            "implied_direction": direction,
            "side": side_label,
            "limit_price": limit_price,
            "size_usdc": size,
            "order_id": result.order_id,
            "fill_status": result.fill_status,
            "oracle_confidence": oracle_confidence,
            "edge_bps": edge_bps,
            "trade_reason": trade_reason,
            **trade_enhancement,
        }

        return row
