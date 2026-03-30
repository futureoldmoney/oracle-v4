"""
Oracle Bot v4: Main Strategy Orchestrator
==========================================
Ties together data feeds, all signal tiers, the oracle strategy engine,
and the execution layer into a single event loop.

v4 changes:
- Oracle strategy is the SINGLE decision maker (not ensemble)
- Continuous monitoring replaces fixed timing window
- Dynamic fair value pricing at actual market fill prices
- New signals: Deribit PCR, tick velocity, BTC sentiment, Coinbase spot
- Ensemble runs for logging/analysis only, not trade decisions

This is the integration point. Each cycle:
1. Data feeds push new ticks/books into signal modules
2. oracle_strategy.evaluate() handles ALL gating internally
3. If trade: executor places order. If skip: log reason.
4. Settlement watcher closes the feedback loop.
"""

import time
import json
import logging
import os
from typing import Optional, Dict, List
from collections import deque

# Internal modules
from ensemble_engine import (
    EnsembleEngine, SignalRegistry, EnsembleDecision, Direction, RegimeState,
)
from tier1_signals import (
    Tier1SignalSuite, PriceTick, OrderBookSnapshot, BookLevel,
)
from tier2_signals import (
    LiquidityVacuumDetector,
    ExecutableComplementArb,
    WhaleFlowTracker,
    EnhancedOBImbalance,
)
from data_feeds import DataRouter, FeedState

# v4 modules
from oracle_strategy import OracleStrategy, TradeDecision
from fair_value import compute_fair_value, estimate_fill_price
from position_sizer import PositionSizer
from settlement_logic import compute_settlement, compute_pnl
from ltp_signal import LTPSignal
from deribit_pcr import DeribitPCR
from btc_sentiment import BTCSentimentTracker
from coinbase_spot import CoinbaseSpot

logger = logging.getLogger("oracle.main")


# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

DEFAULT_CONFIG = {
    # ── Ensemble ──────────────────────────────────────
    "bankroll": 1000.0,
    "fractional_kelly": 0.25,
    "min_aggregate_confidence": 0.40,
    "min_edge_bps": 50.0,
    "max_position_pct": 0.03,
    "min_bet_usd": 1.0,

    # ── Signal Weights ────────────────────────────────
    # Higher weight = more influence on direction vote
    "weight_oracle_confidence": 2.0,   # Primary signal, highest weight
    "weight_stale_quote": 1.5,         # Amplifier for oracle
    "weight_liquidity_vacuum": 1.0,    # Independent
    "weight_complement_arb": 1.0,      # Risk-free (when it fires)
    "weight_whale_flow": 0.8,          # Lagged, less reliable
    "weight_ob_imbalance": 0.8,        # Moderate

    # ── Signal Min Confidence ─────────────────────────
    "min_conf_oracle_confidence": 0.30,
    "min_conf_stale_quote": 0.40,
    "min_conf_liquidity_vacuum": 0.45,
    "min_conf_complement_arb": 0.50,
    "min_conf_whale_flow": 0.40,
    "min_conf_ob_imbalance": 0.35,

    # ── Tier 1 Parameters ─────────────────────────────
    "stale_move_threshold_pct": 0.10,
    "staleness_window_ms": 500.0,
    "stale_lookback_seconds": 5.0,
    "regime_window_size": 360,
    "confidence_alpha": 5.0,
    "confidence_beta": 2.0,
    "confidence_gamma": 1.5,

    # ── Tier 2 Parameters ─────────────────────────────
    "vacuum_threshold": 0.5,
    "vacuum_stability_threshold": 0.7,
    "arb_min_profit_bps": 20.0,
    "arb_target_size": 5.0,
    "whale_min_score": 0.3,
    "whale_size_multiplier": 2.0,
    "ob_bullish_threshold": 1.3,
    "ob_bearish_threshold": 0.7,

    # ── Timing ────────────────────────────────────────
    "cycle_interval_seconds": 2.0,  # Main loop frequency
    "market_scan_interval": 15.0,   # How often to scan for new markets
    "data_api_poll_interval": 5.0,  # How often to poll Data API for trades

    # ── Risk Limits ───────────────────────────────────
    "max_concurrent_positions": 3,
    "max_daily_loss_pct": 0.20,      # 20% of bankroll
    "max_drawdown_pct": 0.30,        # 30% from peak
    "cooldown_after_loss_seconds": 30,

    # ── Oracle Strategy ──────────────────────────────
    "max_trade_size_usd": 15.0,          # Hard cap per trade
    "oracle_entry_price": 0.52,           # Fixed maker limit price
    "oracle_timing_max_seconds": 120,     # Only trade in last 120s of window
    "oracle_timing_min_seconds": 20,      # Don't trade in last 20s
    "oracle_magnitude_high_pct": 0.08,    # HIGH confidence threshold (100% accuracy)
    "oracle_magnitude_medium_pct": 0.05,  # MEDIUM confidence threshold (97% accuracy)
}


# ═══════════════════════════════════════════════════════════════
# CHAINLINK WINDOW TRACKER
# ═══════════════════════════════════════════════════════════════

class ChainlinkWindowTracker:
    """
    Tracks Chainlink BTC/USD price at 5-minute window boundaries.

    Polymarket settles BTC 5-min binary markets using Chainlink's on-chain
    oracle price, NOT Binance. The oracle arbitrage strategy compares:
      - Chainlink price at window open (captured once per window)
      - Current Chainlink price (live from poller)

    If current Chainlink > window open → price is trending UP → bet UP
    If current Chainlink < window open → price is trending DOWN → bet DOWN

    Writes completed windows to `chainlink_windows` table in Supabase
    for backtesting the oracle strategy across all historical windows.
    """

    WINDOW_SECONDS = 300  # 5-minute windows

    SAMPLE_INTERVAL = 30  # Record price every 30 seconds

    def __init__(self, supabase_client=None):
        self._supabase = supabase_client
        self._window_open_price: Optional[float] = None
        self._window_open_ts: int = 0
        self._current_window_start: int = 0
        self._window_high: float = 0.0
        self._window_low: float = float('inf')
        self._window_tick_count: int = 0
        self._price_path: List[Dict] = []  # [{t: unix_ts, p: price}, ...]
        self._last_sample_ts: float = 0.0
        self._history: deque = deque(maxlen=50)  # Last 50 windows
        self._last_price: float = 0.0

    def _get_window_start(self, ts: Optional[float] = None) -> int:
        """Get the 5-min window start timestamp."""
        now = int(ts or time.time())
        return (now // self.WINDOW_SECONDS) * self.WINDOW_SECONDS

    def update(self, chainlink_price: float):
        """
        Call this every cycle with the current Chainlink price.
        Captures the window open price when a new window starts.
        Writes the completed window to Supabase on window close.
        """
        if chainlink_price <= 0:
            return

        self._last_price = chainlink_price
        window_start = self._get_window_start()

        if window_start != self._current_window_start:
            # New window — save the old one and capture new open price
            if self._window_open_price and self._current_window_start > 0:
                close_price = chainlink_price
                move_pct = ((close_price - self._window_open_price) / self._window_open_price) * 100.0
                direction = "UP" if move_pct > 0.01 else "DOWN" if move_pct < -0.01 else "NEUTRAL"

                # Add final close price to path
                self._price_path.append({"t": int(time.time()), "p": round(close_price, 2)})

                window_record = {
                    "window_ts": self._current_window_start,
                    "open_price": self._window_open_price,
                    "close_price": close_price,
                    "high_price": self._window_high,
                    "low_price": self._window_low if self._window_low != float('inf') else self._window_open_price,
                    "price_move_pct": round(move_pct, 6),
                    "direction": direction,
                    "tick_count": self._window_tick_count,
                    "price_path": self._price_path,
                }
                self._history.append(window_record)
                self._write_window_to_supabase(window_record)

            self._current_window_start = window_start
            self._window_open_price = chainlink_price
            self._window_open_ts = int(time.time())
            self._window_high = chainlink_price
            self._window_low = chainlink_price
            self._window_tick_count = 0
            self._price_path = [{"t": int(time.time()), "p": round(chainlink_price, 2)}]
            self._last_sample_ts = time.time()
            logger.info(
                f"ChainlinkWindow: New window {window_start} | "
                f"open_price=${chainlink_price:,.2f}"
            )
        else:
            # Same window — update high/low/tick count
            self._window_high = max(self._window_high, chainlink_price)
            self._window_low = min(self._window_low, chainlink_price)
            self._window_tick_count += 1

            # Sample price every 30 seconds for the price path
            now = time.time()
            if now - self._last_sample_ts >= self.SAMPLE_INTERVAL:
                self._price_path.append({"t": int(now), "p": round(chainlink_price, 2)})
                self._last_sample_ts = now

    def _write_window_to_supabase(self, record: Dict):
        """Write a completed window to the chainlink_windows table."""
        if not self._supabase:
            return
        try:
            from datetime import datetime, timezone
            window_dt = datetime.fromtimestamp(record["window_ts"], tz=timezone.utc).isoformat()
            import json as _json
            row = {
                "window_ts": record["window_ts"],
                "window_start": window_dt,
                "open_price": round(record["open_price"], 2),
                "close_price": round(record["close_price"], 2),
                "high_price": round(record["high_price"], 2),
                "low_price": round(record["low_price"], 2),
                "price_move_pct": record["price_move_pct"],
                "direction": record["direction"],
                "tick_count": record["tick_count"],
                "price_path": record.get("price_path", []),
            }
            self._supabase.table("chainlink_windows").upsert(
                row, on_conflict="window_ts"
            ).execute()
        except Exception as e:
            logger.error(f"ChainlinkWindow: Failed to write to Supabase: {e}")

    @property
    def window_open_price(self) -> Optional[float]:
        return self._window_open_price

    @property
    def current_window_start(self) -> int:
        return self._current_window_start

    def get_current_move(self) -> Optional[Dict]:
        """
        Get current window move data (no arguments needed).
        Used by oracle_strategy._get_chainlink_data().
        """
        if not self._window_open_price or self._last_price <= 0:
            return None
        move_pct = ((self._last_price - self._window_open_price) / self._window_open_price) * 100.0
        return {
            "move_pct": move_pct,
            "current_price": self._last_price,
            "open_price": self._window_open_price,
        }

    def get_direction(self, current_chainlink: float, min_move_pct: float = 0.01) -> str:
        """
        Compare current Chainlink price to window open price.

        Args:
            current_chainlink: Latest Chainlink BTC/USD price
            min_move_pct: Minimum % move to signal a direction (default 0.01%)

        Returns:
            "UP", "DOWN", or "NEUTRAL"
        """
        if not self._window_open_price or current_chainlink <= 0:
            return "NEUTRAL"

        move_pct = ((current_chainlink - self._window_open_price) / self._window_open_price) * 100.0

        if move_pct > min_move_pct:
            return "UP"
        elif move_pct < -min_move_pct:
            return "DOWN"
        return "NEUTRAL"

    def get_move_pct(self, current_chainlink: float) -> float:
        """Get the % move from window open to current price."""
        if not self._window_open_price or current_chainlink <= 0:
            return 0.0
        return ((current_chainlink - self._window_open_price) / self._window_open_price) * 100.0

    def get_diagnostics(self) -> Dict:
        return {
            "window_start": self._current_window_start,
            "window_open_price": self._window_open_price,
            "window_high": self._window_high,
            "window_low": self._window_low if self._window_low != float('inf') else None,
            "tick_count": self._window_tick_count,
            "history_length": len(self._history),
        }


# ═══════════════════════════════════════════════════════════════
# MAIN ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════

class StrategyOrchestrator:
    """
    Main loop that coordinates all components.
    
    Lifecycle:
    1. __init__: Set up all signals, registry, ensemble, data router
    2. run(): Start data feeds and enter the main evaluation loop
    3. Each cycle: evaluate all signals → ensemble decision → execute
    4. Brain reviews happen asynchronously via cron
    """

    def __init__(self, config: Optional[Dict] = None):
        self.config = {**DEFAULT_CONFIG, **(config or {})}

        # ── Signal Registry ───────────────────────────
        self.registry = SignalRegistry()
        self._register_signals()

        # ── Ensemble Engine ───────────────────────────
        self.ensemble = EnsembleEngine(
            registry=self.registry,
            bankroll=self.config["bankroll"],
            fractional_kelly=self.config["fractional_kelly"],
            min_aggregate_confidence=self.config["min_aggregate_confidence"],
            min_edge_bps=self.config["min_edge_bps"],
            max_position_pct=self.config["max_position_pct"],
            min_bet_usd=self.config["min_bet_usd"],
        )

        # ── Tier 1 Signals ────────────────────────────
        self.tier1 = Tier1SignalSuite(self.config)

        # ── Tier 2 Signals ────────────────────────────
        self.vacuum_detector = LiquidityVacuumDetector(
            vacuum_threshold=self.config["vacuum_threshold"],
            stability_threshold=self.config["vacuum_stability_threshold"],
        )
        self.complement_arb = ExecutableComplementArb(
            min_profit_bps=self.config["arb_min_profit_bps"],
            target_size_usd=self.config["arb_target_size"],
        )
        self.whale_tracker = WhaleFlowTracker(
            min_wallet_score=self.config["whale_min_score"],
            size_multiplier_threshold=self.config["whale_size_multiplier"],
        )
        self.ob_imbalance = EnhancedOBImbalance(
            bullish_threshold=self.config["ob_bullish_threshold"],
            bearish_threshold=self.config["ob_bearish_threshold"],
        )

        # ── Data Router ───────────────────────────────
        self.router = DataRouter()
        self._wire_callbacks()

        # ── Chainlink Window Tracker ─────────────────
        self.chainlink_tracker = ChainlinkWindowTracker()

        # ── Oracle Strategy + Position Sizer (v4) ─────
        self.position_sizer = PositionSizer(self.config)
        self.oracle_strategy = OracleStrategy(self.config, self.position_sizer)
        self.ltp_signal = LTPSignal()
        self.deribit_pcr = DeribitPCR()
        self.sentiment_tracker = BTCSentimentTracker()
        self.coinbase_spot = CoinbaseSpot()
        # v4 state (populated by background tasks in run.py)
        self._last_ltp = None
        self._last_pcr = None
        self._last_tick_velocity = 0.0
        self._last_coinbase_price = 0.0

        # ── Supabase Logger ───────────────────────────
        # Initialized as None — call set_supabase() to enable logging
        self._sb_logger = None
        self._config_version = 0

        # ── State ─────────────────────────────────────
        self._cycle_count = 0
        self._last_decision: Optional[EnsembleDecision] = None
        self._last_ensemble_id: Optional[str] = None
        self._current_market_id: Optional[str] = None
        self._seconds_remaining: int = 999  # Set by BotRunner each cycle
        self._last_book_yes: Optional[OrderBookSnapshot] = None
        self._last_book_no: Optional[OrderBookSnapshot] = None
        self._peak_bankroll = self.config["bankroll"]
        self._daily_pnl = 0.0

    def set_supabase(self, supabase_client, mode: str = "paper"):
        """
        Enable Supabase logging for all signal/ensemble data.

        Call this after initialization to wire up the database:
            orchestrator = StrategyOrchestrator(config)
            orchestrator.set_supabase(supabase_client, mode="paper")
        """
        from supabase_logger import SupabaseSignalLogger
        self._sb_logger = SupabaseSignalLogger(supabase_client, mode=mode)
        self._supabase = supabase_client
        # Wire Supabase to Chainlink window tracker for persistence
        self.chainlink_tracker._supabase = supabase_client
        # Load config from bot_config table (overrides DEFAULT_CONFIG)
        self.reload_config()
        # Propagate loaded config into PositionSizer (created before reload_config ran)
        self._sync_position_sizer()
        logger.info(f"Supabase logging enabled (mode={mode})")

    def reload_config(self):
        """
        Hot-reload config from bot_config table in Supabase.
        Called on startup and periodically (every 60s via heartbeat).
        Only updates keys that exist in both bot_config and self.config.
        """
        if not self._supabase:
            return
        try:
            result = self._supabase.table("bot_config").select("*").eq("id", 1).execute()
            if not result.data:
                return
            db_config = result.data[0]

            changed = []
            for key, value in db_config.items():
                if key in ("id", "updated_at", "updated_by"):
                    continue
                if key in self.config and value is not None:
                    old = self.config[key]
                    new = type(old)(value) if old is not None else value
                    if old != new:
                        self.config[key] = new
                        changed.append(f"{key}: {old} → {new}")

            if changed:
                # Re-apply config to ensemble engine and oracle position sizer
                self.ensemble.update_bankroll(self.config["bankroll"])
                self._sync_position_sizer()
                self._config_version += 1
                logger.info(f"Config reloaded from bot_config (v{self._config_version}): {', '.join(changed)}")
        except Exception as e:
            logger.debug(f"Config reload error: {e}")

    def _register_signals(self):
        """Register all signals with the ensemble."""
        c = self.config
        signals = [
            ("ORACLE_CONFIDENCE", c["weight_oracle_confidence"],  c["min_conf_oracle_confidence"]),
            ("STALE_QUOTE",       c["weight_stale_quote"],        c["min_conf_stale_quote"]),
            ("LIQUIDITY_VACUUM",  c["weight_liquidity_vacuum"],   c["min_conf_liquidity_vacuum"]),
            ("COMPLEMENT_ARB",    c["weight_complement_arb"],     c["min_conf_complement_arb"]),
            ("WHALE_FLOW",        c["weight_whale_flow"],         c["min_conf_whale_flow"]),
            ("OB_IMBALANCE",      c["weight_ob_imbalance"],       c["min_conf_ob_imbalance"]),
        ]
        for name, weight, min_conf in signals:
            try:
                self.registry.register(name, weight=weight, min_confidence=min_conf)
            except Exception as e:
                logger.error(f"Failed to register signal {name}: {e}")

        registered = list(self.registry._signals.keys())
        logger.info(f"Signal registration complete: {registered} ({len(registered)}/6)")

    def _wire_callbacks(self):
        """Connect data router to signal ingestion methods."""
        # Binance ticks → Tier 1
        self.router.on_binance_tick(self.tier1.ingest_binance_tick)

        # Book updates → Tier 1 + Tier 2 signals
        def on_book(book):
            self.tier1.ingest_book_snapshot(book)
            self.vacuum_detector.ingest_book(book)
            self.ob_imbalance.ingest_book(book)
            # Store YES book
            self._last_book_yes = book
            # Derive NO book from YES (binary market: YES + NO ≈ $1)
            # This ensures complement arb and other signals have both sides
            try:
                from engine.tier1_signals import OrderBookSnapshot, BookLevel
                no_bids = [BookLevel(price=round(1.0 - a.price, 4), size=a.size)
                           for a in (book.asks or []) if 0.01 < a.price < 0.99]
                no_asks = [BookLevel(price=round(1.0 - b.price, 4), size=b.size)
                           for b in (book.bids or []) if 0.01 < b.price < 0.99]
                no_bids.sort(key=lambda x: x.price, reverse=True)
                no_asks.sort(key=lambda x: x.price)
                self._last_book_no = OrderBookSnapshot(
                    bids=no_bids, asks=no_asks,
                    timestamp=book.timestamp, book_hash="",
                )
            except Exception:
                pass  # Non-critical — evaluate_v4 has its own fallback

        self.router.on_book_update(on_book)

        # Trade notifications → Whale tracker
        self.router.on_trade(lambda t: self.whale_tracker.ingest_trade(t))

    def evaluate_cycle(self) -> EnsembleDecision:
        """
        Run one evaluation cycle across all signals.

        Returns the ensemble's decision.
        """
        self._cycle_count += 1
        now = time.time()

        # ═══════════════════════════════════════════════════
        # ALWAYS update ChainlinkWindowTracker — even outside
        # the trading window. Must capture the window OPEN price
        # at the start of each 5-min window, not just in the
        # last 120 seconds.
        # ═══════════════════════════════════════════════════
        _cl_price = self.router.state.chainlink_btc
        if _cl_price and _cl_price > 0:
            self.chainlink_tracker.update(_cl_price)

        # Default regime for early-exit decisions
        _default_regime = RegimeState(
            spread_percentile=50.0, volatility_1m=0.0,
            book_depth_ratio=1.0, label="normal",
        )

        # ═══════════════════════════════════════════════════
        # TIMING GATE: only trade in last 120-20 seconds
        # At 288s remaining (start of window) accuracy is ~50%
        # At 60-90s remaining (80% through) accuracy is 97-100%
        # ═══════════════════════════════════════════════════
        secs = self._seconds_remaining
        timing_max = int(self.config.get("oracle_timing_max_seconds", 120))
        timing_min = int(self.config.get("oracle_timing_min_seconds", 20))
        if secs > timing_max or secs < timing_min:
            self._last_decision = EnsembleDecision(
                direction=Direction.NEUTRAL,
                aggregate_confidence=0.0, weighted_edge_bps=0.0,
                kelly_fraction=0.0, recommended_size_usd=0.0,
                contributing_signals=[], regime=_default_regime.label,
                should_trade=False,
                reason=f"Timing gate: {secs}s remaining (window: {timing_min}-{timing_max}s)",
            )
            return self._last_decision

        # ═══════════════════════════════════════════════════
        # v4: Oracle gate removed from evaluate_cycle().
        # Trade decisions go through evaluate_v4() in run.py.
        # evaluate_cycle() only runs ensemble signals for logging.
        # ═══════════════════════════════════════════════════

        all_signals = []

        # Use oracle direction as the BASE direction
        oracle_direction = self._compute_oracle_direction()
        binance_move_pct, move_duration = self.router.get_binance_move(5.0)
        book_staleness_ms = (now - self.router.state.last_book_update) * 1000

        # Use Chainlink move (from window open) as the oracle signal magnitude
        # This is the REAL edge: Chainlink is what Polymarket settles on
        chainlink_price = self.router.state.chainlink_btc
        chainlink_move_pct = abs(self.chainlink_tracker.get_move_pct(chainlink_price)) if chainlink_price else 0.0
        # Use the larger of Chainlink or Binance move — Chainlink is truth,
        # Binance confirms. Either one moving is a signal.
        oracle_move_pct = max(chainlink_move_pct, abs(binance_move_pct))

        # ── Tier 1: Evaluate ──────────────────────────────
        tier1_signals, regime = self.tier1.evaluate(
            oracle_direction=oracle_direction,
            binance_move_pct=oracle_move_pct,
            move_duration_seconds=move_duration,
            chainlink_price=chainlink_price,
            polymarket_midpoint=self._get_midpoint(),
            book_staleness_ms=book_staleness_ms,
        )
        all_signals.extend(tier1_signals)

        # ── Tier 2: Evaluate ──────────────────────────────
        # Vacuum
        vacuum_signal = self.vacuum_detector.evaluate()
        if vacuum_signal.is_actionable:
            all_signals.append(vacuum_signal)

        # Complement arb (needs both YES and NO books)
        if self._last_book_yes and self._last_book_no:
            arb_signal = self.complement_arb.evaluate(
                self._last_book_yes, self._last_book_no
            )
            if arb_signal.is_actionable:
                all_signals.append(arb_signal)

        # Whale flow
        market_id = self._get_current_market_id()
        if market_id:
            whale_signal = self.whale_tracker.evaluate(market_id)
            if whale_signal.is_actionable:
                all_signals.append(whale_signal)

        # OB imbalance
        ob_signal = self.ob_imbalance.evaluate()
        if ob_signal.is_actionable:
            all_signals.append(ob_signal)

        # ── Ensemble Decision ─────────────────────────────
        current_price_yes = self._get_midpoint()
        if current_price_yes <= 0 or current_price_yes >= 1:
            current_price_yes = 0.50  # Fallback

        decision = self.ensemble.decide(
            signals=all_signals,
            regime=regime,
            current_price_yes=current_price_yes,
        )

        # ── Risk Checks ──────────────────────────────────
        decision = self._apply_risk_limits(decision)

        # ── Log to Supabase ───────────────────────────────
        ensemble_id = None
        if self._sb_logger:
            market_ctx = {
                "market_id": self._get_current_market_id(),
                "market_question": None,  # Populated by scanner
                "seconds_before_close": None,
                "midpoint": current_price_yes,
                "spread": self._last_book_yes.spread if self._last_book_yes else None,
                "bankroll": self.config["bankroll"],
                "peak_bankroll": self._peak_bankroll,
                "daily_pnl": self._daily_pnl,
            }
            ensemble_id = self._sb_logger.log_cycle(
                signals=all_signals,
                decision=decision,
                regime=regime,
                market_context=market_ctx,
                feed_state=self.router.state,
                config_version=self._config_version,
            )

        self._last_decision = decision
        self._last_ensemble_id = ensemble_id
        return decision

    # ── v4 Primary Evaluation Path ────────────────────

    def evaluate_v4(self, market_info: dict) -> TradeDecision:
        """
        v4 evaluation: single path through oracle_strategy.
        All gating logic lives in oracle_strategy.evaluate().
        Ensemble still runs in evaluate_cycle() for logging/comparison only.
        """
        seconds_remaining = market_info.get("seconds_before_close", 999)
        window_ts = market_info.get("window_ts")

        # Book data: prefer WS book snapshot keys (best_bid_yes/best_ask_yes)
        # injected by run.py, fall back to scanner's keys (best_bid/best_ask)
        bid_yes = (
            market_info.get("best_bid_yes")
            or market_info.get("best_bid")
            or (self._last_book_yes.best_bid if self._last_book_yes else None)
        )
        ask_yes = (
            market_info.get("best_ask_yes")
            or market_info.get("best_ask")
            or (self._last_book_yes.best_ask if self._last_book_yes else None)
        )

        book_state = {
            "best_bid_yes": bid_yes,
            "best_ask_yes": ask_yes,
            # Derive NO book from YES for binary markets (YES + NO ≈ $1)
            "best_bid_no": round(1.0 - ask_yes, 4) if ask_yes else None,
            "best_ask_no": round(1.0 - bid_yes, 4) if bid_yes else None,
        }

        decision = self.oracle_strategy.evaluate(
            chainlink_tracker=self.chainlink_tracker,
            seconds_remaining=seconds_remaining,
            book_state=book_state,
            ltp=self._last_ltp,
            pcr_signal=self._last_pcr,
            sentiment_tracker=self.sentiment_tracker,
            tick_velocity_30s=self._last_tick_velocity,
            coinbase_price=self._last_coinbase_price,
            window_ts=window_ts,
        )
        return decision

    def _compute_oracle_direction(self) -> Direction:
        """
        Determine if oracle suggests UP or DOWN using Chainlink price ONLY.

        Polymarket settles BTC 5-min markets using Chainlink's on-chain oracle.
        Compare current Chainlink price to window open price — that's the signal.
        No Binance fallback. Chainlink is the source of truth.
        """
        chainlink_price = self.router.state.chainlink_btc

        # Update the window tracker with current Chainlink price
        if chainlink_price and chainlink_price > 0:
            self.chainlink_tracker.update(chainlink_price)

            direction_str = self.chainlink_tracker.get_direction(
                chainlink_price, min_move_pct=0.003
            )
            if direction_str == "UP":
                return Direction.UP
            elif direction_str == "DOWN":
                return Direction.DOWN

        return Direction.NEUTRAL

    def _get_midpoint(self) -> float:
        """Get current Polymarket YES midpoint."""
        if self._last_book_yes and self._last_book_yes.midpoint:
            return self._last_book_yes.midpoint
        return 0.50  # Fallback

    def _get_current_market_id(self) -> Optional[str]:
        """Get the current BTC 5-min market condition ID from the feed coordinator."""
        return self._current_market_id

    def set_current_market_id(self, market_id: Optional[str]):
        """Called by BotRunner to keep orchestrator aware of current market."""
        self._current_market_id = market_id

    def set_seconds_remaining(self, seconds: int):
        """Called by BotRunner each cycle with time left in current window."""
        self._seconds_remaining = seconds

    def _apply_risk_limits(self, decision: EnsembleDecision) -> EnsembleDecision:
        """Apply risk management overlays."""
        if not decision.should_trade:
            return decision

        # Max drawdown check
        current_bankroll = self.config["bankroll"]
        drawdown = (self._peak_bankroll - current_bankroll) / self._peak_bankroll
        if drawdown > self.config["max_drawdown_pct"]:
            decision.should_trade = False
            decision.reason = f"RISK: Drawdown {drawdown:.1%} > max {self.config['max_drawdown_pct']:.1%}"
            return decision

        # Daily loss limit
        if self._daily_pnl < -(self.config["bankroll"] * self.config["max_daily_loss_pct"]):
            decision.should_trade = False
            decision.reason = f"RISK: Daily loss ${abs(self._daily_pnl):.2f} > limit"
            return decision

        return decision

    def _sync_position_sizer(self):
        """Propagate current config values into the PositionSizer."""
        c = self.config
        self.position_sizer.bankroll = float(c.get("bankroll", self.position_sizer.bankroll))
        self.position_sizer.max_size_pct = float(c.get("max_position_pct", self.position_sizer.max_size_pct))
        self.position_sizer.min_size_pct = float(c.get("min_position_pct", self.position_sizer.min_size_pct))
        self.position_sizer.fractional_kelly = float(c.get("fractional_kelly", self.position_sizer.fractional_kelly))
        self.position_sizer.max_daily_loss_pct = float(c.get("max_daily_loss_pct", self.position_sizer.max_daily_loss_pct))

    def update_bankroll(self, new_bankroll: float, persist: bool = True):
        """Update bankroll after trade settlement.

        Args:
            new_bankroll: New bankroll value
            persist: Write to bot_config (True for paper, False for live since wallet is source of truth)
        """
        self.config["bankroll"] = new_bankroll
        self.ensemble.update_bankroll(new_bankroll)
        self.position_sizer.update_bankroll(new_bankroll)
        self._peak_bankroll = max(self._peak_bankroll, new_bankroll)
        # Persist to bot_config so paper bankroll survives restarts
        if persist and hasattr(self, '_supabase') and self._supabase:
            try:
                self._supabase.table("bot_config").update(
                    {"bankroll": str(round(new_bankroll, 4))}
                ).eq("id", 1).execute()
            except Exception as e:
                logger.debug(f"Failed to persist bankroll: {e}")

    def record_trade_outcome(self, signal_name: str, won: bool, pnl_bps: float):
        """Record a trade outcome for signal performance tracking."""
        self.registry.update_performance(signal_name, won, pnl_bps)

    def get_trade_enhancement(self) -> Dict:
        """
        Get v3 columns to merge into a live_trades or paper_trades row.
        
        Call this in the executor when building the trade row:
        
            trade_row = {
                # ... existing v2 fields ...
                **orchestrator.get_trade_enhancement(),
            }
            supabase.table("live_trades").insert(trade_row).execute()
        """
        if self._sb_logger and self._last_decision and self._last_ensemble_id:
            return self._sb_logger.enhance_trade_row(
                self._last_decision,
                self._last_ensemble_id,
            )
        return {}

    def log_settlement(self, actual_outcome: str, won: bool, pnl: float, ensemble_id: str = None):
        """
        Log market settlement to update signal correctness.

        Call this after a market resolves:

            orchestrator.log_settlement("UP", won=True, pnl=0.48)
        """
        if self._sb_logger:
            eid = ensemble_id or self._last_ensemble_id
            if eid:
                self._sb_logger.log_settlement(eid, actual_outcome, won, pnl)

    def flush_logs(self):
        """Force flush any buffered Supabase writes."""
        if self._sb_logger:
            self._sb_logger.flush()

    # ── Diagnostics for Brain ─────────────────────────

    def get_full_diagnostics(self) -> Dict:
        """
        Return comprehensive diagnostics for Brain review.
        This is what gets sent to Claude API for analysis.
        """
        return {
            "cycle_count": self._cycle_count,
            "config": self.config,
            "signal_stats": self.registry.get_stats(),
            "feed_stats": self.router.get_feed_stats(),
            "tier1_diagnostics": self.tier1.get_diagnostics(),
            "top_wallets": self.whale_tracker.get_top_wallets(10),
            "current_bankroll": self.config["bankroll"],
            "peak_bankroll": self._peak_bankroll,
            "daily_pnl": self._daily_pnl,
            "chainlink_tracker": self.chainlink_tracker.get_diagnostics(),
            "chainlink_price": self.router.state.chainlink_btc,
            "logger_stats": self._sb_logger.get_stats() if self._sb_logger else None,
            "last_decision": {
                "should_trade": self._last_decision.should_trade if self._last_decision else None,
                "direction": self._last_decision.direction.value if self._last_decision else None,
                "confidence": self._last_decision.aggregate_confidence if self._last_decision else None,
                "reason": self._last_decision.reason if self._last_decision else None,
                "ensemble_id": self._last_ensemble_id,
            },
        }


# ═══════════════════════════════════════════════════════════════
# BRAIN INTEGRATION
# ═══════════════════════════════════════════════════════════════

BRAIN_REVIEW_PROMPT = """You are the Brain of an institutional-grade Polymarket trading bot.

## Your Role
Analyze the bot's performance data and recommend specific config parameter changes.
You think like a portfolio manager at a quantitative hedge fund — not a hobbyist.

## Current Config
{config_json}

## Signal Performance Stats
{signal_stats_json}

## Feed Health
{feed_stats_json}

## Recent Trade History
{trade_history_json}

## Diagnostics
{diagnostics_json}

## Instructions
1. Assess each signal's realized edge vs theoretical edge
2. Identify any signal with negative realized edge after 20+ trades → recommend reducing weight or disabling
3. Identify any signal with edge significantly above theoretical → verify it's not data artifact
4. Recommend specific config changes as JSON:
   {{"parameter_name": new_value, "parameter_name": new_value, ...}}
5. Flag any risk conditions requiring immediate attention
6. Estimate the minimum additional trades needed before conclusions are robust

Respond with JSON only. Structure:
{{
  "assessment": "string summarizing overall health",
  "config_changes": {{}},
  "risk_flags": [],
  "signal_recommendations": {{}},
  "next_review_after_trades": int
}}
"""


def build_brain_prompt(orchestrator: StrategyOrchestrator, trade_history: list) -> str:
    """Build the Brain review prompt with current data."""
    diag = orchestrator.get_full_diagnostics()
    return BRAIN_REVIEW_PROMPT.format(
        config_json=json.dumps(diag["config"], indent=2),
        signal_stats_json=json.dumps(diag["signal_stats"], indent=2),
        feed_stats_json=json.dumps(diag["feed_stats"], indent=2),
        trade_history_json=json.dumps(trade_history[:100], indent=2),  # Last 100 trades
        diagnostics_json=json.dumps({
            k: v for k, v in diag.items()
            if k not in ("config", "signal_stats", "feed_stats")
        }, indent=2),
    )
