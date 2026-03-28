"""
Oracle Bot v4: Main Event Loop
================================
The top-level entry point that runs everything.

v4 changes:
- oracle_strategy.evaluate_v4() is the SINGLE decision path
- Background tasks: LTP polling, Deribit PCR, BTC sentiment, Coinbase spot
- Prometheus metrics server on port 8000
- Taker-first execution (pay 1.56% fee, guaranteed fills)
- Paper mode uses real book data for fill simulation

Components:
  FeedCoordinator   → WebSocket connections (RTDS + Market + DataAPI)
  Orchestrator      → Signal evaluation + Oracle decisions
  Executor          → Order placement on Polymarket
  SettlementWatcher → Outcome tracking + feedback loop
  SupabaseLogger    → Database writes
  MetricsServer     → Prometheus endpoint for Grafana
  Brain           → Self-optimization (runs on cron, not in main loop)

Run: caffeinate -i python3 run.py 2>&1 | tee bot.log
Or deploy on Railway with Dockerfile.
"""

import os
import sys
import time
import json
import asyncio
import signal
import logging
from typing import Optional
from datetime import datetime, timezone

# Add engine/ to path so bare imports (from main_v4, executor_v4, etc.) resolve
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "engine"))

logger = logging.getLogger("oracle")


# ═══════════════════════════════════════════════════════════════
# ENVIRONMENT
# ═══════════════════════════════════════════════════════════════

def load_env():
    """Load required environment variables."""
    required = {
        "PRIVATE_KEY": "Polygon wallet private key",
        "SUPABASE_URL": "Supabase project URL",
        "SUPABASE_KEY": "Supabase anon/service key",
    }
    optional = {
        "POLYMARKET_FUNDER_ADDRESS": None,
        "ANTHROPIC_API_KEY": None,
        "DISCORD_WEBHOOK_URL": None,
        "POLYGON_RPC_URL": None,        # Alchemy/Infura Polygon RPC for Chainlink reads
        "BOT_MODE": "paper",           # "paper" or "live"
        "CYCLE_INTERVAL": "2.0",
        "LOG_LEVEL": "INFO",
    }

    env = {}
    missing = []
    for key, description in required.items():
        val = os.getenv(key)
        if not val:
            missing.append(f"  {key}: {description}")
        env[key] = val

    if missing:
        print("Missing required environment variables:")
        print("\n".join(missing))
        sys.exit(1)

    for key, default in optional.items():
        env[key] = os.getenv(key, default)

    return env


def setup_logging(level: str = "INFO"):
    """Configure structured logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Quiet noisy libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


# ═══════════════════════════════════════════════════════════════
# CLIENT INITIALIZATION
# ═══════════════════════════════════════════════════════════════

def init_clob_client(env: dict):
    """Initialize py-clob-client."""
    from py_clob_client.client import ClobClient

    host = "https://clob.polymarket.com"
    chain_id = 137
    private_key = env["PRIVATE_KEY"]
    funder = env.get("POLYMARKET_FUNDER_ADDRESS")
    signature_type = int(os.environ.get("POLYMARKET_SIG_TYPE", "0"))

    client = ClobClient(
        host=host,
        chain_id=chain_id,
        key=private_key,
        signature_type=signature_type,
        funder=funder,
    )

    # Derive API credentials
    try:
        creds = client.create_or_derive_api_creds()
        client.set_api_creds(creds)
        logger.info("CLOB client initialized with API credentials")
    except Exception as e:
        logger.error(f"Failed to derive API creds: {e}")
        raise

    return client


def init_supabase(env: dict):
    """Initialize Supabase client."""
    from supabase import create_client
    url = env["SUPABASE_URL"]
    key = env["SUPABASE_KEY"]
    client = create_client(url, key)
    logger.info("Supabase client initialized")
    return client


# ═══════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════

class BotRunner:
    """
    Top-level coordinator that runs the complete bot lifecycle.
    
    Architecture:
    
      ┌─────────────────────────────────────────────┐
      │              Async Event Loop                │
      │                                              │
      │  ┌──────────────┐  ┌───────────────────┐    │
      │  │ FeedCoordinator│ │  Evaluation Loop  │   │
      │  │  (WebSockets)  │ │  (every 2 sec)    │   │
      │  └───────┬────────┘ └────────┬──────────┘   │
      │          │                    │               │
      │          ▼                    ▼               │
      │  ┌──────────────────────────────────────┐    │
      │  │         Orchestrator                  │    │
      │  │  Signals → Ensemble → Decision        │    │
      │  └─────────────┬────────────────────────┘    │
      │                │                              │
      │      ┌─────────┴──────────┐                  │
      │      ▼                    ▼                   │
      │  ┌──────────┐     ┌──────────────┐           │
      │  │ Executor │     │ Supabase     │           │
      │  │ (orders) │     │ Logger       │           │
      │  └────┬─────┘     └──────────────┘           │
      │       │                                       │
      │       ▼                                       │
      │  ┌────────────────┐                           │
      │  │ Settlement     │                           │
      │  │ Watcher        │                           │
      │  └────────────────┘                           │
      └─────────────────────────────────────────────┘
    """

    def __init__(self, env: dict, config: Optional[dict] = None):
        self.env = env
        self.mode = env.get("BOT_MODE", "paper")
        self.cycle_interval = float(env.get("CYCLE_INTERVAL", "2.0"))
        self._running = False
        # Retry-aware guard: tracks fill state per market window
        # condition_id -> {"status": "filled"|"pending", "attempts": int, "last_ts": float}
        self._market_attempts: dict = {}
        # Consecutive cycles with no order book data (for health monitoring)
        self._no_book_data_count: int = 0

        # Import here to avoid circular imports at module level
        from main_v4 import StrategyOrchestrator, DEFAULT_CONFIG
        from executor_v4 import V4Executor
        from settlement_watcher import SettlementWatcher
        from ws_connections import FeedCoordinator
        from ltp_signal import LTPSignal
        from flow_tracker import FlowTracker
        from multi_asset_collector import MultiAssetCollector
        from metrics import start_metrics_server, get_metrics

        # Merge config
        full_config = {**DEFAULT_CONFIG, **(config or {})}

        # Initialize components
        logger.info(f"Initializing bot in {self.mode} mode...")

        self.clob_client = init_clob_client(env)
        self.supabase = init_supabase(env)

        self.orchestrator = StrategyOrchestrator(full_config)
        self.orchestrator.set_supabase(self.supabase, mode=self.mode)

        # v4: oracle_strategy lives inside orchestrator now (initialized in main_v4.py)
        # No separate OracleStrategy instance needed here

        # Phase 2 confirmation signals (LTP polling moved to background task)
        self.flow_tracker = FlowTracker()

        # Multi-asset data collector (background, data-only)
        self.multi_asset_collector = MultiAssetCollector(
            self.supabase, data_router=self.orchestrator.router,
        )

        self.executor = V4Executor(
            self.clob_client,
            max_open_orders=full_config.get("max_concurrent_positions", 3),
        )

        self.settlement = SettlementWatcher(self.clob_client)

        self.feed_coordinator = FeedCoordinator(self.orchestrator.router)

        # Wire market_resolved events to settlement watcher
        self.orchestrator.router.on_trade(self._handle_market_event)

        logger.info("All components initialized")

        # v4: Start metrics server
        metrics_port = int(os.environ.get("METRICS_PORT", "8000"))
        self.metrics = get_metrics()
        start_metrics_server(metrics_port)

    def _handle_market_event(self, msg: dict):
        """Route market_resolved events to settlement watcher."""
        if msg.get("event_type") == "market_resolved":
            result = self.settlement.handle_market_resolved_ws(msg)
            if result:
                self._process_settlement(result)

    def _process_settlement(self, result: dict):
        """Process a single settlement result."""
        timed_out = result.get("timed_out", False)

        if not timed_out:
            # Update orchestrator
            self.orchestrator.log_settlement(
                result["actual_outcome"],
                result["won"],
                result["pnl"],
            )

            # Update bankroll
            current = self.orchestrator.config["bankroll"]
            new_bankroll = current + result["pnl"]
            self.orchestrator.update_bankroll(new_bankroll)

            # Capture bankroll after settlement for equity curve tracking
            result["bankroll_after"] = round(new_bankroll, 4)

            # Update signal registry using the STORED signals from trade time
            # (not _last_decision which gets overwritten every 2 seconds)
            stored_signals = result.get("contributing_signals", [])
            if stored_signals:
                pnl_bps = (result["pnl"] / max(result["size_usd"], 0.01)) * 10000
                for sig in stored_signals:
                    sig_name = sig.get("signal_name") if isinstance(sig, dict) else getattr(sig, "signal_name", None)
                    if sig_name:
                        self.orchestrator.record_trade_outcome(sig_name, result["won"], pnl_bps)

        # Write settlement back to paper_trades / live_trades
        self._update_trade_settlement(result)

        # Discord alert
        self._send_settlement_alert(result)

    def _update_trade_settlement(self, result: dict):
        """Write settlement outcome back to paper_trades or live_trades, and sync to paper_settled."""
        table = "paper_trades" if self.mode == "paper" else "live_trades"
        condition_id = result["condition_id"]

        # Fetch the price_path from chainlink_windows for this trade's window
        price_path = None
        try:
            trade_check = self.supabase.table(table).select("window_start_ts").eq(
                "market_id", condition_id
            ).limit(1).execute()
            if trade_check.data and trade_check.data[0].get("window_start_ts"):
                wts = trade_check.data[0]["window_start_ts"]
                cw = self.supabase.table("chainlink_windows").select("price_path").eq(
                    "window_ts", wts
                ).limit(1).execute()
                if cw.data and cw.data[0].get("price_path"):
                    price_path = cw.data[0]["price_path"]
        except Exception as e:
            logger.debug(f"Settlement: Could not fetch price_path: {e}")

        update_data = {
            "resolved_outcome": result["actual_outcome"],
            "won": result["won"],
            "pnl_usdc": result["pnl"],
            "settled_at": datetime.now(timezone.utc).isoformat(),
            "bankroll_after": result.get("bankroll_after"),
        }
        if price_path:
            update_data["price_path"] = price_path

        try:
            self.supabase.table(table).update(update_data).eq(
                "market_id", condition_id
            ).is_("won", "null").execute()
            logger.info(f"Settlement: Updated {table} for {condition_id[:16]}{'  +price_path' if price_path else ''}")

            # Sync to paper_settled / live_settled
            settled_table = "paper_settled" if self.mode == "paper" else "live_settled"
            try:
                trade_row = self.supabase.table(table).select("*").eq(
                    "market_id", condition_id
                ).limit(1).execute()
                if trade_row.data:
                    if self.mode == "paper":
                        self._sync_to_paper_settled(trade_row.data[0], update_data)
                    else:
                        self._sync_to_live_settled(trade_row.data[0], update_data)
            except Exception:
                pass

            # Backfill ensemble_log and signal_log with settlement outcome
            actual = result["actual_outcome"]
            try:
                self.supabase.table("ensemble_log").update({
                    "actual_outcome": actual,
                    "was_correct": result["won"],
                    "trade_pnl": result["pnl"],
                }).eq("market_id", condition_id).is_("was_correct", "null").execute()

                # Update signal_log: was_correct = did this signal's direction match outcome
                if actual in ("UP", "DOWN"):
                    self.supabase.rpc("backfill_signal_log_outcome", {
                        "p_market_id": condition_id,
                        "p_outcome": actual,
                        "p_pnl": result["pnl"],
                    }).execute()
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Settlement: Failed to update {table}: {e}")

        # Clear from market attempts guard so we can trade the next window
        self._market_attempts.pop(condition_id, None)

    def _send_settlement_alert(self, result: dict):
        """Send settlement notification to Discord."""
        webhook_url = self.env.get("DISCORD_WEBHOOK_URL")
        if not webhook_url:
            return

        timed_out = result.get("timed_out", False)
        cid = result["condition_id"][:8]

        if timed_out:
            emoji = "⏰"
            msg = (
                f"{emoji} **Settlement Timeout** ({self.mode}) `{cid}`\n"
                f"Bet: {result['direction']} @ ${result['entry_price']:.2f}\n"
                f"Market did not resolve within 5 min of window close\n"
                f"Market: `{result['condition_id']}`"
            )
        else:
            emoji = "💰" if result["won"] else "❌"
            msg = (
                f"{emoji} **{'WIN' if result['won'] else 'LOSS'}** ({self.mode}) `{cid}`\n"
                f"Bet: {result['direction']} @ ${result['entry_price']:.2f}\n"
                f"Outcome: {result['actual_outcome']}\n"
                f"P&L: ${result['pnl']:+.4f}\n"
                f"Bankroll: ${self.orchestrator.config['bankroll']:.2f}\n"
                f"Market: `{result['condition_id']}`"
            )

        try:
            import httpx
            httpx.post(webhook_url, json={"content": msg}, timeout=5.0)
        except Exception:
            pass

    async def run(self):
        """Main entry point. Starts all async tasks."""
        self._running = True
        logger.info(f"Starting Oracle Bot v4 ({self.mode} mode)")

        tasks = [
            asyncio.create_task(self.feed_coordinator.start()),
            asyncio.create_task(self._evaluation_loop()),
            asyncio.create_task(self._settlement_loop()),
            asyncio.create_task(self._settlement_backfill()),
            asyncio.create_task(self._fill_check_loop()),
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._edge_measurement_loop()),
            asyncio.create_task(self._signal_snapshot_loop()),
            asyncio.create_task(self.multi_asset_collector.start()),
            # v4 background data tasks
            asyncio.create_task(self._ltp_poll_loop()),
            asyncio.create_task(self._sentiment_update_loop()),
            asyncio.create_task(self._pcr_update_loop()),
            asyncio.create_task(self._coinbase_poll_loop()),
        ]

        # Handle graceful shutdown
        loop = asyncio.get_event_loop()
        for sig_name in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig_name, lambda: self._shutdown(tasks))

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Bot shutting down gracefully")
        finally:
            self.orchestrator.flush_logs()
            self.executor.cancel_all()
            logger.info("Bot stopped")

    def _shutdown(self, tasks):
        """Handle graceful shutdown."""
        logger.info("Shutdown signal received")
        self._running = False
        for task in tasks:
            task.cancel()

    async def _evaluation_loop(self):
        """Main evaluation cycle — runs every N seconds."""
        # Wait for data feeds to warm up
        await asyncio.sleep(10)
        logger.info("Evaluation loop started")

        while self._running:
            try:
                # Push current market state to orchestrator
                mi = self._get_current_market_info()
                self.orchestrator.set_current_market_id(
                    mi.get("condition_id") if mi else None
                )
                self.orchestrator.set_seconds_remaining(
                    mi.get("seconds_before_close", 999) if mi else 999
                )

                # 1. Evaluate all signals (ensemble runs for logging/comparison)
                decision = self.orchestrator.evaluate_cycle()

                # 1b. v4: Oracle strategy evaluation via orchestrator
                # All gating (timing, magnitude, edge, LTP, PCR, sentiment)
                # happens inside oracle_strategy.evaluate() via the orchestrator.
                # Background tasks feed LTP/PCR/sentiment/Coinbase data continuously.
                _secs_left = mi.get("seconds_before_close", 999) if mi else 999

                if mi:
                    # Inject book state into market_info for oracle_strategy
                    book = self.orchestrator._last_book_yes
                    if book:
                        mi["best_bid_yes"] = book.best_bid
                        mi["best_ask_yes"] = book.best_ask

                    oracle_decision = self.orchestrator.evaluate_v4(mi)
                else:
                    from oracle_strategy import TradeDecision
                    oracle_decision = TradeDecision(should_trade=False, reason="No market info")

                # Log every 10th cycle or when oracle says trade
                cycle = self.orchestrator._cycle_count
                if oracle_decision.should_trade or cycle % 10 == 0:
                    market_info = self._get_current_market_info()
                    cl_price = self.orchestrator.router.state.chainlink_btc
                    cl_open = self.orchestrator.chainlink_tracker.window_open_price
                    cl_move = self.orchestrator.chainlink_tracker.get_move_pct(cl_price) if cl_price else 0.0
                    cl_str = f"${cl_price:,.2f}" if cl_price else "none"
                    cl_open_str = f"${cl_open:,.2f}" if cl_open else "none"
                    logger.info(
                        f"Eval #{cycle} | oracle={oracle_decision.should_trade} | "
                        f"ensemble={decision.should_trade} | "
                        f"dir={oracle_decision.direction} | "
                        f"conf={oracle_decision.confidence:.2f} | "
                        f"CL_move={cl_move:+.4f}% | "
                        f"secs_left={_secs_left} | "
                        f"reason={oracle_decision.reason} | "
                        f"CL={cl_str} | CL_open={cl_open_str} | "
                        f"market={'found' if market_info else 'NONE'}"
                    )

                # 2. Execute if we should trade
                # v4: oracle_decision.should_trade already includes ALL gating
                # (timing, magnitude, edge at fill, daily loss, one-trade-per-window)
                if oracle_decision.should_trade and self.mode == "live":
                    market_info = self._get_current_market_info()
                    if market_info:
                        result = self.executor.execute(
                            decision,
                            market_info,
                            self.orchestrator._last_ensemble_id,
                            oracle_confirmed=True,  # Oracle gate passed above
                        )

                        if result.success:
                            # Write trade row to Supabase
                            enhancement = self.orchestrator.get_trade_enhancement()
                            trade_row = self.executor.build_trade_row(
                                result, decision, market_info,
                                self.orchestrator._last_ensemble_id or "",
                                enhancement,
                            )
                            row_id = self._write_trade_row(trade_row)

                            # Send Discord alert for live trade
                            self._send_trade_alert(
                                decision, market_info,
                                trade_row.get("limit_price", 0.50),
                                decision.recommended_size_usd,
                                row_id, paper=False,
                            )

                            # Track for settlement
                            live_wts = market_info.get("window_ts")
                            live_wend = (live_wts + 300) if live_wts else None
                            live_sig_snapshot = [
                                {"signal_name": s.signal_name, "direction": s.direction.value if hasattr(s.direction, 'value') else str(s.direction)}
                                for s in (decision.contributing_signals or [])
                            ]
                            self.settlement.track(
                                condition_id=market_info["condition_id"],
                                ensemble_id=self.orchestrator._last_ensemble_id or "",
                                direction=decision.direction.value,
                                order_id=result.order_id,
                                entry_price=trade_row.get("limit_price", 0.50),
                                size_usd=decision.recommended_size_usd,
                                window_end_ts=live_wend,
                                contributing_signals=live_sig_snapshot,
                            )

                elif oracle_decision.should_trade and self.mode == "paper":
                    # Paper mode: oracle strategy drives trade decisions
                    market_info = self._get_current_market_info()
                    market_cid = market_info.get("condition_id", "") if market_info else ""
                    # Check retry-aware guard
                    import time as _time
                    _attempt_info = self._market_attempts.get(market_cid)
                    _should_proceed = False
                    _is_retry = False
                    _secs_left = market_info.get("seconds_before_close", 0) if market_info else 0

                    if market_info and _attempt_info is None:
                        _should_proceed = True  # First attempt
                    elif market_info and _attempt_info and _attempt_info["status"] == "pending":
                        # Retry if: <5 attempts, >=30s since last, >60s remaining
                        if (_attempt_info["attempts"] < 5
                                and _time.time() - _attempt_info["last_ts"] >= 30
                                and _secs_left > 60):
                            _should_proceed = True
                            _is_retry = True
                            logger.info(f"Retry attempt #{_attempt_info['attempts']+1} for {market_cid[:12]}... ({_secs_left}s left)")

                    # SAFETY GATE: v4 oracle_strategy already validated edge — trust it.
                    # No separate price cap needed; edge_at_fill gate does this better.

                    if _should_proceed:
                        enhancement = self.orchestrator.get_trade_enhancement()

                        # v4: Use fields directly from TradeDecision
                        side = oracle_decision.side or ("YES" if oracle_decision.direction == "UP" else "NO")
                        token_id = market_info.get("yes_token_id") if side == "YES" else market_info.get("no_token_id")

                        # v4: fill_price and size come from oracle_strategy (computed from real book)
                        hypo_price = oracle_decision.fill_price
                        hypo_size = oracle_decision.size_usd
                        hypo_shares = hypo_size / hypo_price if hypo_price > 0 else 0

                        # Get book and price data.
                        # We always subscribe to the YES book.  For NO-side trades derive NO
                        # prices via: NO_ask ≈ 1 − YES_bid,  NO_bid ≈ 1 − YES_ask.
                        book = self.orchestrator._last_book_yes
                        router_state = self.orchestrator.router.state
                        window_ts = market_info.get("window_ts")

                        if book:
                            self._no_book_data_count = 0
                            yes_best_bid = book.best_bid
                            yes_best_ask = book.best_ask
                            spread = book.spread
                            spread_bps_val = int(spread * 10000) if spread else None
                            if side == "YES":
                                best_bid = yes_best_bid
                                best_ask = yes_best_ask
                            else:
                                # Derive NO prices from the YES order book
                                best_bid = round(1.0 - yes_best_ask, 2) if yes_best_ask is not None else None
                                best_ask = round(1.0 - yes_best_bid, 2) if yes_best_bid is not None else None
                        else:
                            yes_best_bid = None
                            yes_best_ask = None
                            best_bid = None
                            best_ask = None
                            spread = None
                            spread_bps_val = None
                            self._no_book_data_count += 1
                            if self._no_book_data_count == 10:
                                logger.warning(
                                    f"ORDER BOOK FEED: No data for {self._no_book_data_count} "
                                    f"consecutive cycles — attempting reconnect"
                                )
                                try:
                                    self.feed_coordinator.market_feed.stop()
                                except Exception as _e:
                                    logger.warning(f"Reconnect attempt failed: {_e}")
                            elif self._no_book_data_count == 50:
                                logger.warning(
                                    f"ORDER BOOK FEED DOWN: No data for {self._no_book_data_count} cycles"
                                )
                                _wh = os.environ.get("DISCORD_WEBHOOK_URL")
                                if _wh:
                                    try:
                                        import httpx as _httpx
                                        _httpx.post(
                                            _wh,
                                            json={"content": (
                                                f"\u26a0\ufe0f **ORDER BOOK FEED DOWN** \u2014 no data for "
                                                f"{self._no_book_data_count} cycles "
                                                f"(~{self._no_book_data_count * int(self.cycle_interval)}s)"
                                            )},
                                            timeout=5.0,
                                        )
                                    except Exception:
                                        pass

                        # v4: oracle_strategy already computed fill_price from real book data
                        # and validated edge at that price. If we get here, the trade is fillable.
                        # Taker-first execution = guaranteed fill.
                        would_fill = True
                        fill_decision = "INSTANT FILL"
                        if hypo_price <= 0:
                            would_fill = False
                            fill_decision = "UNFILLED"

                        ev = (1.0 - hypo_price) * oracle_decision.confidence - hypo_price * (1 - oracle_decision.confidence) if hypo_price > 0 else None

                        # Snapshot config and Chainlink window state at trade time
                        cl_tracker = self.orchestrator.chainlink_tracker
                        cl_window_open = cl_tracker.window_open_price
                        cl_current = router_state.chainlink_btc
                        cl_move = cl_tracker.get_move_pct(cl_current) if cl_current else None

                        # Config snapshot
                        config_snap = {
                            k: v for k, v in self.orchestrator.config.items()
                            if not k.startswith("_") and isinstance(v, (int, float, str, bool))
                        }

                        paper_row = {
                            "market_id": market_info.get("condition_id"),
                            "market_slug": market_info.get("slug", ""),
                            "market_question": market_info.get("question", ""),
                            "token_id": token_id,
                            "window_start_ts": window_ts,
                            "window_end_ts": (window_ts + 300) if window_ts else None,
                            "seconds_remaining": market_info.get("seconds_before_close"),
                            "decision": fill_decision,
                            "skip_reason": (
                                None if would_fill
                                else f"[{side}] Limit ${hypo_price:.2f} < ask ${best_ask:.2f}" if best_ask
                                else f"[{side}] No book data (YES bid={yes_best_bid}, ask={yes_best_ask})"
                            ),
                            "implied_direction": oracle_decision.direction,
                            "side": side,
                            "chainlink_price": cl_current or None,
                            "btc_open_price": float(router_state.binance_btc) if router_state.binance_btc != 0.0 else None,
                            "best_bid_yes": yes_best_bid,
                            "best_ask_yes": yes_best_ask,
                            "spread_bps": spread_bps_val,
                            "hypothetical_price": hypo_price,
                            "hypothetical_size_usdc": hypo_size,
                            "hypothetical_shares": round(hypo_shares, 4),
                            "ev_per_share": round(ev, 4) if ev else None,
                            "oracle_confidence": oracle_decision.confidence,
                            "edge_bps": int(oracle_decision.magnitude_pct * 100),
                            "trade_reason": oracle_decision.reason,
                            "config_version": self.orchestrator._config_version,
                            "engine_version": self.env.get("ENGINE_VERSION", "4.0.0"),
                            "regime_multiplier": {"tight": 1.2, "normal": 1.0, "wide": 0.5, "extreme": 0.3}.get(decision.regime, 1.0),
                            "chainlink_window_open": round(cl_window_open, 2) if cl_window_open else None,
                            "chainlink_move_pct": round(cl_move, 6) if cl_move is not None else None,
                            "bankroll_at_trade": round(self.orchestrator.config["bankroll"], 2),
                            "config_snapshot": config_snap,
                            # v4 columns
                            "simulated_fill_price": oracle_decision.fill_price,
                            "fair_value_at_trade": oracle_decision.fair_value,
                            "edge_at_fill": oracle_decision.edge_at_fill,
                            "taker_fee_estimate": round(hypo_size * 0.0156, 4),
                            "execution_mode": oracle_decision.execution_mode,
                            "coinbase_price": oracle_decision.coinbase_price,
                            "deribit_pcr": oracle_decision.pcr_adjustment,
                            "ltp_velocity_30s": oracle_decision.tick_velocity,
                            "sentiment_bias": oracle_decision.sentiment_adjustment,
                            "size_pct": oracle_decision.size_pct,
                            **enhancement,
                        }
                        # Only write to DB on first attempt or if filled on retry
                        if not _is_retry or would_fill:
                            row_id = self._write_trade_row(paper_row, paper=True)
                        else:
                            row_id = None

                        # Update market attempts tracker
                        if would_fill:
                            self._market_attempts[market_cid] = {"status": "filled", "attempts": (_attempt_info["attempts"] + 1) if _attempt_info else 1, "last_ts": _time.time()}
                        else:
                            prev = _attempt_info["attempts"] if _attempt_info else 0
                            self._market_attempts[market_cid] = {"status": "pending", "attempts": prev + 1, "last_ts": _time.time()}

                        # Send Discord alert: first attempt always, retries only if filled
                        if not _is_retry or would_fill:
                            self._send_trade_alert(
                                oracle_decision, market_info, hypo_price, hypo_size,
                                row_id, paper=True, fill_status=fill_decision,
                                best_ask=best_ask,
                            )

                        # Only track for settlement if the order would have filled
                        if would_fill:
                            w_end = (window_ts + 300) if window_ts else None
                            sig_snapshot = [{"signal_name": "ORACLE_STRATEGY", "direction": oracle_decision.direction}]
                            self.settlement.track(
                                condition_id=market_info.get("condition_id", ""),
                                ensemble_id=self.orchestrator._last_ensemble_id or "",
                                direction=oracle_decision.direction,
                                entry_price=hypo_price,
                                size_usd=hypo_size,
                                window_end_ts=w_end,
                                contributing_signals=sig_snapshot,
                            )

            except Exception as e:
                logger.error(f"Evaluation loop error: {e}", exc_info=True)

            await asyncio.sleep(self.cycle_interval)

    async def _settlement_loop(self):
        """Check for market settlements every 5 seconds after window closes."""
        await asyncio.sleep(30)  # Wait for initial data
        logger.info("Settlement loop started")

        while self._running:
            try:
                results = self.settlement.check_settlements()
                for result in results:
                    self._process_settlement(result)
            except Exception as e:
                logger.error(f"Settlement loop error: {e}")

            await asyncio.sleep(5)  # Check every 5s for fast post-window updates

    async def _fill_check_loop(self):
        """Check order fill status periodically."""
        await asyncio.sleep(20)

        while self._running:
            try:
                if self.mode == "live":
                    changed = self.executor.check_fills()
                    for order in changed:
                        logger.info(f"Fill update: {order.order_id[:16]} → {order.fill_status}")

                    # Cancel expiring orders
                    self.executor.cancel_expiring_orders()
            except Exception as e:
                logger.error(f"Fill check error: {e}")

            await asyncio.sleep(10)

    async def _heartbeat_loop(self):
        """Log health status, hot-reload config, and write heartbeat to Supabase."""
        while self._running:
            try:
                # Hot-reload config from bot_config table
                self.orchestrator.reload_config()

                # Live mode: sync bankroll from actual wallet USDC balance
                if self.mode == "live":
                    try:
                        wallet_balance = self.executor.get_balance()
                        if wallet_balance > 0:
                            self.orchestrator.update_bankroll(wallet_balance, persist=False)
                    except Exception as e:
                        logger.debug(f"Wallet balance sync error: {e}")

                diag = self.orchestrator.get_full_diagnostics()
                feed_health = self.feed_coordinator.get_health()
                exec_stats = self.executor.get_stats()
                settle_stats = self.settlement.get_stats()

                logger.info(
                    f"Heartbeat | cycle={diag['cycle_count']} | "
                    f"bankroll=${diag['current_bankroll']:.2f} | "
                    f"pending={settle_stats['pending_markets']} | "
                    f"open_orders={exec_stats['open_orders']} | "
                    f"feeds={'OK' if all(v.get('healthy', False) for v in feed_health.values() if isinstance(v, dict) and 'healthy' in v) else 'DEGRADED'}"
                )

                # Write heartbeat to both tables for Discord bot compatibility
                hb_row = {
                    "status": "alive",
                    "bot_mode": self.mode,
                    "balance_usdc": round(diag["current_bankroll"], 4),
                    "active_orders": exec_stats.get("open_orders", 0),
                    "pending_settlements": settle_stats.get("pending_markets", 0),
                    "trades_today": diag.get("trades_today", 0),
                    "pnl_today": round(diag.get("daily_pnl", 0), 4),
                }
                try:
                    self.supabase.table("heartbeats_v2").insert(hb_row).execute()
                    # Cleanup: delete heartbeats older than 48 hours (every 10th cycle)
                    if diag["cycle_count"] % 10 == 0:
                        cutoff = (datetime.now(timezone.utc) - __import__('datetime').timedelta(hours=48)).isoformat()
                        self.supabase.table("heartbeats_v2").delete().lt("created_at", cutoff).execute()
                except Exception:
                    pass  # Non-critical

            except Exception as e:
                logger.error(f"Heartbeat error: {e}")

            await asyncio.sleep(60)

    async def _edge_measurement_loop(self):
        """Run edge measurement periodically on settled trades."""
        await asyncio.sleep(300)  # Wait 5 min for some data
        logger.info("Edge measurement loop started")

        while self._running:
            try:
                from edge_measurement import EdgeCalculator, TradeRecord

                # Query settled oracle trades only — hypothetical_price < 0.60 excludes
                # the pre-fix ensemble garbage ($0.74-$0.77 entries). Oracle YES = $0.52,
                # oracle NO = $0.48; both fall well below this threshold.
                table = "paper_trades" if self.mode == "paper" else "live_trades"
                result = self.supabase.table(table).select(
                    "implied_direction, hypothetical_price, hypothetical_size_usdc, "
                    "won, pnl_usdc, created_at, trade_reason"
                ).not_.is_("won", "null").lt(
                    "hypothetical_price", 0.60
                ).order(
                    "created_at", desc=True
                ).limit(500).execute()

                if not result.data or len(result.data) < 5:
                    await asyncio.sleep(900)  # Check every 15 min
                    continue

                # Convert to TradeRecord format
                records = []
                for row in result.data:
                    try:
                        records.append(TradeRecord(
                            signal_name=row.get("trade_reason", "UNKNOWN") or "UNKNOWN",
                            direction=row.get("implied_direction", "UP") or "UP",
                            entry_price=float(row.get("hypothetical_price", 0.5) or 0.5),
                            size_usd=float(row.get("hypothetical_size_usdc", 1.0) or 1.0),
                            won=bool(row.get("won")),
                            pnl_usd=float(row.get("pnl_usdc", 0) or 0),
                            timestamp=0,
                        ))
                    except (ValueError, TypeError):
                        continue

                if len(records) < 5:
                    await asyncio.sleep(900)
                    continue

                # Compute overall edge
                calc = EdgeCalculator()
                overall = calc.compute_edge(records)

                # Per-direction edge
                by_direction = calc.segment_analysis(records, "direction")

                logger.info(
                    f"Edge Report (oracle, price<$0.60) | n={overall.sample_size} | "
                    f"win_rate={overall.win_rate:.1%} | "
                    f"implied={overall.avg_implied_prob:.1%} | "
                    f"edge={overall.realized_edge:+.2%} | "
                    f"z={overall.z_score:.2f} | "
                    f"p={overall.p_value:.4f} | "
                    f"pnl=${overall.total_pnl_usd:+.2f} | "
                    f"sig={'YES' if overall.is_significant_2sigma else 'no'}"
                )

                for dir_name, est in by_direction.items():
                    logger.info(
                        f"  {dir_name}: n={est.sample_size} | "
                        f"win={est.win_rate:.1%} | edge={est.realized_edge:+.2%} | "
                        f"z={est.z_score:.2f}"
                    )

                # Send to Discord once we have at least 5 settled oracle trades
                webhook_url = self.env.get("DISCORD_WEBHOOK_URL")
                if webhook_url and overall.sample_size >= 5:
                    sig_emoji = "✅" if overall.is_significant_2sigma else "📊"
                    # Build per-direction summary line
                    dir_lines = []
                    for dir_name, est in by_direction.items():
                        dir_lines.append(
                            f"  {dir_name}: {est.sample_size}t | "
                            f"WR {est.win_rate:.0%} | edge {est.realized_edge:+.1%}"
                        )
                    dir_summary = "\n".join(dir_lines) if dir_lines else ""
                    msg = (
                        f"{sig_emoji} **Edge Report** ({self.mode}) | "
                        f"{overall.sample_size} oracle trades (price < $0.60)\n"
                        f"Win Rate: {overall.win_rate:.1%} vs Implied: {overall.avg_implied_prob:.1%}\n"
                        f"Edge: {overall.realized_edge:+.2%} | Z: {overall.z_score:.2f} | "
                        f"p={overall.p_value:.4f}\n"
                        f"Total P&L: ${overall.total_pnl_usd:+.2f} ({overall.total_pnl_bps:+.0f} bps)\n"
                        f"Need {overall.trades_needed_for_significance} trades for 2σ\n"
                        + (dir_summary if dir_summary else "")
                    )
                    try:
                        import httpx
                        httpx.post(webhook_url, json={"content": msg}, timeout=5.0)
                    except Exception:
                        pass

            except Exception as e:
                logger.error(f"Edge measurement error: {e}")

            await asyncio.sleep(900)  # Run every 15 minutes

    async def _signal_snapshot_loop(self):
        """Write daily signal performance snapshots to Supabase every hour."""
        await asyncio.sleep(600)  # Wait 10 min for some trades
        logger.info("Signal snapshot loop started")

        while self._running:
            try:
                today = datetime.now(timezone.utc).date().isoformat()
                stats = self.orchestrator.registry.get_stats()

                for signal_name, signal_stats in stats.items():
                    total = signal_stats.get("trades", 0)
                    win_rate = signal_stats.get("win_rate")
                    total_pnl = signal_stats.get("total_pnl_bps", 0)
                    wins = int(total * win_rate) if win_rate and total else 0

                    row = {
                        "snapshot_date": today,
                        "signal_name": signal_name,
                        "total_trades": total,
                        "wins": wins,
                        "win_rate": win_rate,
                        "total_pnl_bps": round(total_pnl, 2),
                        "avg_edge_bps": signal_stats.get("avg_pnl_bps", 0),
                        "current_weight": signal_stats.get("weight", 0),
                        "adjusted_weight": signal_stats.get("weight", 0),
                    }
                    try:
                        self.supabase.table("signal_snapshots").upsert(
                            row, on_conflict="snapshot_date,signal_name"
                        ).execute()
                    except Exception as e:
                        logger.debug(f"Signal snapshot write error: {e}")

                logger.info(f"Signal snapshots written for {len(stats)} signals")

            except Exception as e:
                logger.error(f"Signal snapshot loop error: {e}")

            await asyncio.sleep(3600)  # Every hour

    # ── v4 Background Tasks ──────────────────────────────

    async def _ltp_poll_loop(self):
        """Poll CLOB LTP every 2 seconds — feeds oracle_strategy with market price and velocity."""
        await asyncio.sleep(15)
        logger.info("v4: LTP poll loop started")
        while self._running:
            try:
                mi = self._get_current_market_info()
                token_id = mi.get("yes_token_id") if mi else None
                if token_id:
                    ltp = await self.orchestrator.ltp_signal.get_ltp(token_id)
                    self.orchestrator._last_ltp = ltp
                    self.orchestrator._last_tick_velocity = (
                        self.orchestrator.ltp_signal.get_velocity_30s(token_id)
                    )
            except Exception:
                pass
            await asyncio.sleep(2)

    async def _sentiment_update_loop(self):
        """Update multi-timeframe BTC sentiment every 15 minutes."""
        await asyncio.sleep(30)
        logger.info("v4: Sentiment update loop started")
        while self._running:
            try:
                await self.orchestrator.sentiment_tracker.update()
                snapshot = self.orchestrator.sentiment_tracker.get_snapshot()
                if snapshot:
                    logger.info(
                        f"Sentiment: {snapshot.composite_bias} "
                        f"(F&G:{snapshot.fear_greed_index})"
                    )
            except Exception as e:
                logger.debug(f"Sentiment update error: {e}")
            await asyncio.sleep(900)

    async def _pcr_update_loop(self):
        """Update Deribit PCR every 5 minutes."""
        await asyncio.sleep(20)
        logger.info("v4: Deribit PCR loop started")
        while self._running:
            try:
                signal = await self.orchestrator.deribit_pcr.get_signal()
                self.orchestrator._last_pcr = signal
                if signal:
                    logger.debug(f"Deribit PCR: {signal.get('pcr', 0):.3f} ({signal.get('direction', '?')})")
            except Exception:
                pass
            await asyncio.sleep(300)

    async def _coinbase_poll_loop(self):
        """Poll Coinbase spot every 10 seconds for cross-validation."""
        await asyncio.sleep(10)
        logger.info("v4: Coinbase spot loop started")
        while self._running:
            try:
                price = await self.orchestrator.coinbase_spot.get_price()
                if price:
                    self.orchestrator._last_coinbase_price = price
            except Exception:
                pass
            await asyncio.sleep(10)

    async def _settlement_backfill(self):
        """
        On startup, check for unsettled PAPER trades and resolve them via CLOB API.
        This handles trades that were lost when Railway redeployed (pending state is in-memory).
        Runs once on startup, then every 10 minutes to catch stragglers.
        """
        await asyncio.sleep(30)  # Wait for CLOB client to be ready
        logger.info("Settlement backfill started")

        while self._running:
            try:
                table = "paper_trades" if self.mode == "paper" else "live_trades"

                # Find unsettled filled trades with windows that have closed
                result = self.supabase.table(table).select(
                    "id, market_id, implied_direction, side, token_id, "
                    "hypothetical_price, hypothetical_size_usdc, window_end_ts, won"
                ).is_("won", "null").in_(
                    "decision", ["INSTANT FILL", "MAKER FILL"]
                ).order("created_at", desc=True).limit(50).execute()

                unsettled = result.data or []
                if not unsettled:
                    await asyncio.sleep(600)
                    continue

                now = time.time()
                resolved_count = 0

                for trade in unsettled:
                    condition_id = trade.get("market_id")
                    window_end = trade.get("window_end_ts")

                    # Skip if window hasn't closed yet
                    if window_end and now < window_end:
                        continue

                    # Skip if no market_id
                    if not condition_id:
                        continue

                    try:
                        market = self.clob_client.get_market(condition_id)
                        if not isinstance(market, dict) or not market.get("closed", False):
                            continue

                        # Find winner
                        tokens = market.get("tokens", [])
                        winner = None
                        for token in tokens:
                            if token.get("winner", False):
                                winner = token
                                break

                        if winner is None:
                            continue

                        outcome_str = winner.get("outcome", "").upper()
                        if outcome_str in ("YES", "UP"):
                            actual = "UP"
                        elif outcome_str in ("NO", "DOWN"):
                            actual = "DOWN"
                        else:
                            continue

                        # Guard: don't overwrite already-settled trades
                        if trade.get("won") is not None:
                            continue

                        # Determine won using side + outcome (not direction, which can mismatch)
                        our_side = trade.get("side", "")
                        our_token = trade.get("token_id", "")
                        winning_token = winner.get("token_id", "")

                        if our_token and winning_token:
                            won = (our_token == winning_token)
                        elif our_side:
                            won = (
                                (our_side == "YES" and actual in ("UP", "YES")) or
                                (our_side == "NO" and actual in ("DOWN", "NO"))
                            )
                        else:
                            # Last resort fallback
                            direction = trade.get("implied_direction", "")
                            won = (direction == actual)

                        entry = float(trade.get("hypothetical_price", 0.5) or 0.5)
                        size = float(trade.get("hypothetical_size_usdc", 1.0) or 1.0)
                        pnl = (1.0 - entry) * (size / entry) if won else -size

                        # Update the trade row
                        update_data = {
                            "resolved_outcome": actual,
                            "won": won,
                            "pnl_usdc": round(pnl, 4),
                            "settled_at": datetime.now(timezone.utc).isoformat(),
                        }
                        self.supabase.table(table).update(update_data).eq(
                            "id", trade["id"]
                        ).is_("won", "null").execute()  # Extra safety: only update unsettled

                        # Also write to paper_settled for v3 data
                        self._sync_to_paper_settled(trade, update_data)

                        # Backfill ensemble_log with settlement outcome
                        try:
                            self.supabase.table("ensemble_log").update({
                                "actual_outcome": actual,
                                "was_correct": won,
                                "trade_pnl": round(pnl, 4),
                            }).eq("market_id", condition_id).is_("was_correct", "null").execute()

                            self.supabase.rpc("backfill_signal_log_outcome", {
                                "p_market_id": condition_id,
                                "p_outcome": actual,
                                "p_pnl": round(pnl, 4),
                            }).execute()
                        except Exception:
                            pass

                        resolved_count += 1
                        emoji = "💰" if won else "❌"
                        logger.info(
                            f"Backfill: {emoji} {condition_id[:16]}... "
                            f"bet={direction}, actual={actual}, pnl=${pnl:+.4f}"
                        )

                    except Exception as e:
                        logger.debug(f"Backfill: Check failed for {condition_id[:16]}...: {e}")

                    # Rate limit: don't hammer the CLOB API
                    await asyncio.sleep(1)

                if resolved_count > 0:
                    logger.info(f"Backfill: Resolved {resolved_count} unsettled trades")

            except Exception as e:
                logger.error(f"Settlement backfill error: {e}")

            await asyncio.sleep(600)  # Re-check every 10 minutes

    def _sync_to_paper_settled(self, trade: dict, settlement: dict):
        """Write a settled trade to paper_settled table with full config linkage."""
        if self.mode != "paper":
            return
        try:
            row = {
                "market_id": trade.get("market_id"),
                "market_slug": trade.get("market_slug", ""),
                "market_question": trade.get("market_question", ""),
                "window_ts": trade.get("window_start_ts"),
                "side": trade.get("side"),
                "implied_direction": trade.get("implied_direction"),
                "oracle_confidence": trade.get("oracle_confidence"),
                "edge_bps": trade.get("edge_bps"),
                "ev_per_share": trade.get("ev_per_share"),
                "seconds_remaining": trade.get("seconds_remaining"),
                "hypothetical_price": trade.get("hypothetical_price"),
                "hypothetical_size_usdc": trade.get("hypothetical_size_usdc"),
                "hypothetical_shares": trade.get("hypothetical_shares"),
                "chainlink_price": trade.get("chainlink_price"),
                "btc_open_price": trade.get("btc_open_price"),
                "best_bid_yes": trade.get("best_bid_yes"),
                "best_ask_yes": trade.get("best_ask_yes"),
                "spread_bps": trade.get("spread_bps"),
                "resolved_outcome": settlement.get("resolved_outcome"),
                "won": settlement.get("won"),
                "pnl_usdc": settlement.get("pnl_usdc"),
                "settled_at": settlement.get("settled_at"),
                "config_version": trade.get("config_version"),
                "engine_version": trade.get("engine_version", "3.0.0"),
                "trade_reason": trade.get("trade_reason"),
                "chainlink_window_open": trade.get("chainlink_window_open"),
                "chainlink_move_pct": trade.get("chainlink_move_pct"),
                "bankroll_at_trade": trade.get("bankroll_at_trade"),
                "config_snapshot": trade.get("config_snapshot"),
                "regime": trade.get("regime"),
                "regime_multiplier": trade.get("regime_multiplier"),
                "kelly_fraction": trade.get("kelly_fraction"),
                "contributing_signals": trade.get("contributing_signals"),
                "signal_count": trade.get("signal_count"),
                "ensemble_id": trade.get("ensemble_id"),
                "aggregate_confidence": trade.get("aggregate_confidence"),
                "weighted_edge_bps": trade.get("weighted_edge_bps"),
                "price_path": trade.get("price_path"),
                "bankroll_after": settlement.get("bankroll_after"),
            }
            self.supabase.table("paper_settled").upsert(
                row, on_conflict="market_id"
            ).execute()
        except Exception as e:
            logger.debug(f"paper_settled sync error: {e}")

    def _sync_to_live_settled(self, trade: dict, settlement: dict):
        """Write a settled live trade to live_settled table."""
        try:
            row = {
                "market_id": trade.get("market_id"),
                "market_slug": trade.get("market_slug", ""),
                "market_question": trade.get("market_question", ""),
                "window_ts": trade.get("window_start_ts"),
                "side": trade.get("side"),
                "implied_direction": trade.get("implied_direction"),
                "oracle_confidence": trade.get("oracle_confidence"),
                "edge_bps": trade.get("edge_bps"),
                "ev_per_share": trade.get("ev_per_share"),
                "seconds_remaining": trade.get("seconds_remaining"),
                "order_id": trade.get("order_id"),
                "limit_price": trade.get("limit_price"),
                "fill_price": trade.get("fill_price"),
                "fill_shares": trade.get("fill_shares"),
                "size_usdc": trade.get("size_usdc"),
                "chainlink_price": trade.get("chainlink_price"),
                "btc_open_price": trade.get("btc_open_price"),
                "resolved_outcome": settlement.get("resolved_outcome"),
                "won": settlement.get("won"),
                "pnl_usdc": settlement.get("pnl_usdc"),
                "settled_at": settlement.get("settled_at"),
                "rebate_usdc": trade.get("rebate_usdc"),
                "config_version": trade.get("config_version"),
                "engine_version": trade.get("engine_version", "3.0.0"),
                "trade_reason": trade.get("trade_reason"),
                "config_snapshot": trade.get("config_snapshot"),
                "chainlink_window_open": trade.get("chainlink_window_open"),
                "chainlink_move_pct": trade.get("chainlink_move_pct"),
                "bankroll_at_trade": trade.get("bankroll_at_trade"),
                "regime": trade.get("regime"),
                "regime_multiplier": trade.get("regime_multiplier"),
                "kelly_fraction": trade.get("kelly_fraction"),
                "contributing_signals": trade.get("contributing_signals"),
                "signal_count": trade.get("signal_count"),
                "ensemble_id": trade.get("ensemble_id"),
                "aggregate_confidence": trade.get("aggregate_confidence"),
                "weighted_edge_bps": trade.get("weighted_edge_bps"),
                "price_path": trade.get("price_path"),
            }
            self.supabase.table("live_settled").upsert(
                row, on_conflict="market_id"
            ).execute()
        except Exception as e:
            logger.debug(f"live_settled sync error: {e}")

    def _get_current_market_info(self) -> Optional[dict]:
        """Get current market info from the feed coordinator's scanner."""
        markets = self.feed_coordinator.scanner.current_markets
        if not markets:
            return None
        return markets[0]  # Best market by volume/liquidity

    def _send_trade_alert(self, decision, market_info, price, size, row_id,
                          paper=True, fill_status="PAPER", best_ask=None):
        """Send Discord webhook alert for trades with Supabase row ID."""
        webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
        if not webhook_url:
            return

        # Support both EnsembleDecision and TradeDecision (oracle strategy)
        direction_str = getattr(decision, 'direction', 'NEUTRAL')
        if hasattr(direction_str, 'value'):
            direction_str = direction_str.value  # EnsembleDecision has Direction enum
        side = "YES" if direction_str == "UP" else "NO"
        id_short = row_id[:8] if row_id else "n/a"

        # Get edge/confidence — works for both decision types
        edge = getattr(decision, 'weighted_edge_bps', 0) or (getattr(decision, 'magnitude_pct', 0) * 100)
        conf = getattr(decision, 'aggregate_confidence', 0) or getattr(decision, 'confidence', 0)
        reason = getattr(decision, 'reason', '')

        mode_label = "Paper Trade" if paper else "Live Trade"
        if fill_status == "INSTANT FILL":
            emoji = "\U0001f7e2"   # green circle
            label = f"{mode_label} (INSTANT FILL)"
        elif fill_status == "MAKER FILL":
            emoji = "\U0001f7e1"   # yellow circle
            label = f"{mode_label} (MAKER FILL)"
        else:
            emoji = "\U0001f534"   # red circle
            label = f"{mode_label} (UNFILLED)"

        ask_info = f" | Ask: ${best_ask:.2f}" if best_ask else ""
        msg = (
            f"{emoji} **{label}** `{id_short}`\n"
            f"Market: {market_info.get('question', 'BTC 5-min')}\n"
            f"Side: {side} ({direction_str}) @ ${price:.4f}{ask_info}\n"
            f"Size: ${size:.2f} USDC\n"
            f"Edge: {edge:.0f}bps | Conf: {conf:.1%}\n"
            f"Reason: {reason}\n"
            f"ID: `{row_id}`"
        )
        try:
            import httpx
            httpx.post(webhook_url, json={"content": msg}, timeout=5.0)
        except Exception:
            pass

    def _write_trade_row(self, row: dict, paper: bool = False) -> Optional[str]:
        """Write trade row to Supabase. Returns the inserted row's ID."""
        table = "paper_trades" if (paper or self.mode == "paper") else "live_trades"
        try:
            result = self.supabase.table(table).insert(row).execute()
            if result.data and len(result.data) > 0:
                return str(result.data[0].get("id", ""))
        except Exception as e:
            logger.error(f"Failed to write trade row: {e}")
        return None


# ═══════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def main():
    env = load_env()
    setup_logging(env.get("LOG_LEVEL", "INFO"))

    logger.info("=" * 60)
    logger.info("Oracle Bot v4 — Institutional Strategy Engine")
    logger.info("=" * 60)

    runner = BotRunner(env)
    asyncio.run(runner.run())


if __name__ == "__main__":
    service = os.environ.get("SERVICE_TYPE", "engine").lower()

    if service == "engine":
        main()
    elif service == "backtest":
        try:
            from engine.backtest import main as bt_main
            bt_main()
        except ImportError:
            print("Backtest module not available. Run backtest from local machine.")
            sys.exit(1)
    elif service == "brain-t1":
        from brain.run import main as brain_main
        sys.argv = ["brain.run", "--tier", "1"]
        brain_main()
    elif service == "brain-t2":
        from brain.run import main as brain_main
        sys.argv = ["brain.run", "--tier", "2"]
        brain_main()
    elif service == "brain-t3":
        from brain.run import main as brain_main
        sys.argv = ["brain.run", "--tier", "3"]
        brain_main()
    elif service == "discord":
        from engine.discord_bot import main as discord_main
        discord_main()
    elif service == "redeem":
        from engine.redeem import main as redeem_main
        redeem_main()
    else:
        logger.error(f"Unknown SERVICE_TYPE: {service}")
        sys.exit(1)
