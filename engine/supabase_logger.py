"""
Supabase Signal Logger
=======================
Writes every signal evaluation, ensemble decision, and regime snapshot
to Supabase for Brain analysis and historical tracking.

Tables written to:
  - signal_log:    Every signal output from every cycle
  - ensemble_log:  Every ensemble decision (trade or skip)
  - regime_log_v2: Regime snapshots on change
  - live_trades:     Enhanced with ensemble_id + signal metadata
  - paper_trades:  Same enhancements for paper mode

Architecture:
  Signals → EnsembleEngine → SupabaseLogger.log_cycle() → Supabase
  
  The logger buffers writes and flushes in batches to avoid
  hammering the API. Critical writes (trades) flush immediately.

Usage:
    from supabase_logger import SupabaseSignalLogger
    
    logger = SupabaseSignalLogger(supabase_client, mode="paper")
    
    # After each evaluation cycle:
    logger.log_cycle(
        signals=all_signal_outputs,
        decision=ensemble_decision,
        regime=regime_state,
        market_context={...},
        feed_state=router.state,
    )
    
    # After trade settlement:
    logger.log_settlement(ensemble_id, won, pnl, actual_outcome)
"""

import time
import json
import uuid
import logging
from typing import Optional, List, Dict, Deque
from collections import deque
from dataclasses import asdict

from ensemble_engine import EnsembleDecision, Direction, SignalOutput, RegimeState
from data_feeds import FeedState

logger = logging.getLogger("oracle.supabase_logger")

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

SIGNAL_LOG_TABLE = "signal_log"
ENSEMBLE_LOG_TABLE = "ensemble_log"
REGIME_LOG_TABLE = "regime_log_v2"
TRADE_TABLE_LIVE = "live_trades"
TRADE_TABLE_PAPER = "paper_trades"

# Batch settings
SIGNAL_BATCH_SIZE = 20       # Flush signals every N rows
SIGNAL_FLUSH_INTERVAL = 30.0  # Or every N seconds
MAX_QUEUE_SIZE = 500          # Drop oldest if queue exceeds this


# ═══════════════════════════════════════════════════════════════
# MAIN LOGGER
# ═══════════════════════════════════════════════════════════════

class SupabaseSignalLogger:
    """
    Writes signal/ensemble data to Supabase with batched writes.
    
    Every evaluation cycle produces:
    - 0-6 signal_log rows (one per evaluated signal)
    - 1 ensemble_log row
    - 0-1 regime_log_v2 rows (only on regime change)
    
    At ~2 second cycle intervals, that's ~180-540 signal rows/minute.
    We batch to keep Supabase API calls manageable.
    """

    def __init__(
        self,
        supabase_client,
        mode: str = "paper",  # "paper" or "live"
        batch_size: int = SIGNAL_BATCH_SIZE,
        flush_interval: float = SIGNAL_FLUSH_INTERVAL,
    ):
        self._sb = supabase_client
        self._mode = mode
        self._trade_table = TRADE_TABLE_PAPER if mode == "paper" else TRADE_TABLE_LIVE
        self._batch_size = batch_size
        self._flush_interval = flush_interval

        # Write queues
        self._signal_queue: Deque[Dict] = deque(maxlen=MAX_QUEUE_SIZE)
        self._last_flush_time = time.time()

        # Regime tracking (only log on change)
        self._last_regime_label: Optional[str] = None

        # Stats
        self._signals_written = 0
        self._ensembles_written = 0
        self._regimes_written = 0
        self._errors = 0

    # ── Main Logging Method ───────────────────────────────

    def log_cycle(
        self,
        signals: List[SignalOutput],
        decision: EnsembleDecision,
        regime: RegimeState,
        market_context: Dict,
        feed_state: FeedState,
        config_version: int = 0,
    ) -> Optional[str]:
        """
        Log a complete evaluation cycle to Supabase.
        
        Args:
            signals: All signal outputs (actionable and not)
            decision: Ensemble decision
            regime: Current regime state
            market_context: Dict with market_id, market_question, seconds_before_close
            feed_state: Current data feed state
            config_version: Current config version number
        
        Returns:
            ensemble_id (str) if successfully queued, None on error
        """
        ensemble_id = str(uuid.uuid4())
        now = time.time()

        try:
            # 1. Log all signal evaluations
            self._queue_signals(signals, ensemble_id, decision, market_context, feed_state)

            # 2. Log ensemble decision (immediate write - this is critical)
            self._write_ensemble(ensemble_id, decision, regime, market_context, feed_state, config_version)

            # 3. Log regime if changed
            self._maybe_log_regime(regime, feed_state)

            # 4. Flush signal queue if needed
            if (len(self._signal_queue) >= self._batch_size or
                    now - self._last_flush_time > self._flush_interval):
                self._flush_signal_queue()

            return ensemble_id

        except Exception as e:
            self._errors += 1
            logger.error(f"SupabaseLogger: log_cycle failed: {e}")
            return None

    # ── Signal Logging ────────────────────────────────────

    def _queue_signals(
        self,
        signals: List[SignalOutput],
        ensemble_id: str,
        decision: EnsembleDecision,
        market_context: Dict,
        feed_state: FeedState,
    ):
        """Queue signal rows for batch writing."""
        for sig in signals:
            # Did this signal agree with the ensemble's final direction?
            agreed = (sig.direction == decision.direction) if decision.should_trade else None

            row = {
                "ensemble_id": ensemble_id,
                "signal_name": sig.signal_name,
                "direction": sig.direction.value if isinstance(sig.direction, Direction) else sig.direction,
                "confidence": sig.confidence,
                "estimated_edge_bps": sig.estimated_edge_bps,
                "magnitude": sig.magnitude,
                "is_actionable": sig.is_actionable,
                "agreed_with_ensemble": agreed,
                "metadata": json.dumps(sig.metadata) if sig.metadata else "{}",
                "market_id": market_context.get("market_id"),
                "binance_price": feed_state.binance_btc,
                "chainlink_price": feed_state.chainlink_btc,
                "polymarket_midpoint": market_context.get("midpoint"),
                "spread": market_context.get("spread"),
            }
            self._signal_queue.append(row)

    def _flush_signal_queue(self):
        """Batch write queued signals to Supabase."""
        if not self._signal_queue:
            return

        rows = list(self._signal_queue)
        self._signal_queue.clear()
        self._last_flush_time = time.time()

        try:
            # Supabase supports batch inserts
            self._sb.table(SIGNAL_LOG_TABLE).insert(rows).execute()
            self._signals_written += len(rows)
            logger.debug(f"SupabaseLogger: Flushed {len(rows)} signals")
        except Exception as e:
            self._errors += 1
            logger.error(f"SupabaseLogger: Signal flush failed ({len(rows)} rows): {e}")
            # Re-queue failed rows (up to limit)
            for row in rows[:MAX_QUEUE_SIZE - len(self._signal_queue)]:
                self._signal_queue.append(row)

    # ── Ensemble Logging ──────────────────────────────────

    def _write_ensemble(
        self,
        ensemble_id: str,
        decision: EnsembleDecision,
        regime: RegimeState,
        market_context: Dict,
        feed_state: FeedState,
        config_version: int,
    ):
        """Write ensemble decision immediately (critical path)."""
        contributing_names = ", ".join(
            s.signal_name for s in decision.contributing_signals
        ) if decision.contributing_signals else ""

        drawdown = 0.0
        peak = market_context.get("peak_bankroll", 0)
        current = market_context.get("bankroll", 0)
        if peak > 0:
            drawdown = (peak - current) / peak

        row = {
            "id": ensemble_id,
            "market_id": market_context.get("market_id"),
            "market_question": market_context.get("market_question"),
            "seconds_before_close": market_context.get("seconds_before_close"),
            "direction": decision.direction.value if isinstance(decision.direction, Direction) else decision.direction,
            "should_trade": decision.should_trade,
            "reason": decision.reason,
            "aggregate_confidence": decision.aggregate_confidence,
            "weighted_edge_bps": decision.weighted_edge_bps,
            "kelly_fraction": decision.kelly_fraction,
            "recommended_size_usd": decision.recommended_size_usd,
            "total_signals_evaluated": len(decision.contributing_signals) if decision.contributing_signals else 0,
            "actionable_signals": len([s for s in (decision.contributing_signals or []) if s.is_actionable]),
            "agreeing_signals": len(decision.contributing_signals or []),
            "contributing_signal_names": contributing_names,
            "regime": regime.label,
            "regime_multiplier": regime.regime_multiplier,
            "spread_percentile": regime.spread_percentile,
            "binance_price": feed_state.binance_btc,
            "chainlink_price": feed_state.chainlink_btc,
            "polymarket_yes_mid": market_context.get("midpoint"),
            "polymarket_spread": market_context.get("spread"),
            "oracle_gap_pct": feed_state.oracle_gap_pct,
            "current_bankroll": current,
            "peak_bankroll": peak,
            "daily_pnl": market_context.get("daily_pnl", 0),
            "drawdown_pct": round(drawdown, 4),
            "config_version": config_version,
        }

        try:
            self._sb.table(ENSEMBLE_LOG_TABLE).insert(row).execute()
            self._ensembles_written += 1
        except Exception as e:
            self._errors += 1
            logger.error(f"SupabaseLogger: Ensemble write failed: {e}")

    # ── Regime Logging ────────────────────────────────────

    def _maybe_log_regime(self, regime: RegimeState, feed_state: FeedState):
        """Log regime snapshot only when regime label changes."""
        if regime.label == self._last_regime_label:
            return

        self._last_regime_label = regime.label

        row = {
            "regime": regime.label,
            "spread_percentile": regime.spread_percentile,
            "volatility_1m": regime.volatility_1m,
            "book_depth_ratio": regime.book_depth_ratio,
            "regime_multiplier": regime.regime_multiplier,
            "binance_price": feed_state.binance_btc,
            "chainlink_price": feed_state.chainlink_btc,
            "oracle_gap_pct": feed_state.oracle_gap_pct,
        }

        try:
            self._sb.table(REGIME_LOG_TABLE).insert(row).execute()
            self._regimes_written += 1
        except Exception as e:
            self._errors += 1
            logger.error(f"SupabaseLogger: Regime write failed: {e}")

    # ── Trade Enhancement ─────────────────────────────────

    def enhance_trade_row(self, decision: EnsembleDecision, ensemble_id: str) -> Dict:
        """
        Generate the v3 columns to add to a live_trades or paper_trades row.
        
        Call this when building the trade row in the executor:
        
            trade_row = {
                # ... existing v2 fields ...
                **logger.enhance_trade_row(decision, ensemble_id),
            }
            supabase.table("live_trades").insert(trade_row).execute()
        """
        contributing_names = ", ".join(
            s.signal_name for s in decision.contributing_signals
        ) if decision.contributing_signals else ""

        return {
            "ensemble_id": ensemble_id,
            "aggregate_confidence": decision.aggregate_confidence,
            "weighted_edge_bps": decision.weighted_edge_bps,
            "kelly_fraction": decision.kelly_fraction,
            "regime": decision.regime,
            "contributing_signals": contributing_names,
            "signal_count": len(decision.contributing_signals or []),
            # v4 columns
            "fair_value_at_trade": getattr(decision, 'fair_value', None),
            "edge_at_fill": getattr(decision, 'edge_at_fill', None),
            "simulated_fill_price": getattr(decision, 'fill_price', None),
            "taker_fee_estimate": getattr(decision, 'size_usd', 0) * 0.0156 if hasattr(decision, 'size_usd') else None,
            "execution_mode": getattr(decision, 'execution_mode', None),
            "coinbase_price": getattr(decision, 'coinbase_price', None),
            "deribit_pcr": getattr(decision, 'pcr_adjustment', None),
            "ltp_velocity_30s": getattr(decision, 'tick_velocity', None),
            "sentiment_bias": getattr(decision, 'sentiment_adjustment', None),
            "size_pct": getattr(decision, 'size_pct', None),
        }

    # ── Settlement Updates ────────────────────────────────

    def log_settlement(
        self,
        ensemble_id: str,
        actual_outcome: str,  # "UP" or "DOWN"
        won: bool,
        pnl: float,
    ):
        """
        Update ensemble_log and signal_log with settlement results.
        
        Called after a market resolves to record whether the
        ensemble and individual signals were correct.
        """
        try:
            # Update ensemble_log
            self._sb.table(ENSEMBLE_LOG_TABLE).update({
                "actual_outcome": actual_outcome,
                "was_correct": won,
                "trade_pnl": pnl,
            }).eq("id", ensemble_id).execute()

            # Update signal_log entries for this ensemble
            # Get all signals from this ensemble
            resp = (self._sb.table(SIGNAL_LOG_TABLE)
                    .select("id, direction")
                    .eq("ensemble_id", ensemble_id)
                    .execute())

            signal_rows = resp.data or []
            for sig_row in signal_rows:
                sig_direction = sig_row.get("direction")
                sig_correct = (sig_direction == actual_outcome) if sig_direction != "NEUTRAL" else None

                # Compute counterfactual P&L (if we traded this signal alone at 50¢)
                counterfactual = None
                if sig_direction != "NEUTRAL":
                    if sig_direction == actual_outcome:
                        counterfactual = 0.50  # Won 50¢ on a $0.50 bet
                    else:
                        counterfactual = -0.50  # Lost 50¢

                self._sb.table(SIGNAL_LOG_TABLE).update({
                    "was_correct": sig_correct,
                    "counterfactual_pnl": counterfactual,
                }).eq("id", sig_row["id"]).execute()

            logger.info(f"SupabaseLogger: Settlement logged for ensemble {ensemble_id[:8]}... "
                        f"({len(signal_rows)} signals updated)")

        except Exception as e:
            self._errors += 1
            logger.error(f"SupabaseLogger: Settlement update failed: {e}")

    # ── Batch Settlement (for backfill) ───────────────────

    def batch_settle_signals(
        self,
        ensemble_ids: List[str],
        actual_outcomes: Dict[str, str],  # {ensemble_id: "UP"/"DOWN"}
    ):
        """
        Batch update signal correctness for multiple ensembles.
        Used by the settlement watcher to backfill results efficiently.
        """
        for eid in ensemble_ids:
            outcome = actual_outcomes.get(eid)
            if not outcome:
                continue

            try:
                resp = (self._sb.table(SIGNAL_LOG_TABLE)
                        .select("id, direction")
                        .eq("ensemble_id", eid)
                        .execute())

                updates = []
                for sig in (resp.data or []):
                    sig_dir = sig.get("direction")
                    if sig_dir == "NEUTRAL":
                        correct = None
                        cf_pnl = None
                    else:
                        correct = sig_dir == outcome
                        cf_pnl = 0.50 if correct else -0.50

                    updates.append({
                        "id": sig["id"],
                        "was_correct": correct,
                        "counterfactual_pnl": cf_pnl,
                    })

                # Supabase upsert for batch
                if updates:
                    self._sb.table(SIGNAL_LOG_TABLE).upsert(updates).execute()

            except Exception as e:
                logger.error(f"Batch settle error for {eid[:8]}: {e}")

    # ── Flush & Cleanup ───────────────────────────────────

    def flush(self):
        """Force flush all queued writes."""
        self._flush_signal_queue()

    def get_stats(self) -> Dict:
        """Return logging statistics for monitoring."""
        return {
            "signals_written": self._signals_written,
            "ensembles_written": self._ensembles_written,
            "regimes_written": self._regimes_written,
            "errors": self._errors,
            "queue_size": len(self._signal_queue),
            "mode": self._mode,
        }

    # ── Brain Query Helpers ───────────────────────────────

    def get_settled_signals_for_brain(
        self,
        signal_name: Optional[str] = None,
        limit: int = 500,
        lookback_hours: float = 24.0,
    ) -> List[Dict]:
        """
        Fetch settled signal data for Brain analysis.
        
        Returns only signals that have been settled (was_correct IS NOT NULL).
        """
        try:
            from datetime import datetime, timezone, timedelta
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()

            query = (self._sb.table(SIGNAL_LOG_TABLE)
                     .select("signal_name, direction, confidence, estimated_edge_bps, "
                             "is_actionable, agreed_with_ensemble, was_correct, "
                             "counterfactual_pnl, metadata, created_at")
                     .not_.is_("was_correct", "null")
                     .gte("created_at", cutoff)
                     .order("created_at", desc=True)
                     .limit(limit))

            if signal_name:
                query = query.eq("signal_name", signal_name)

            resp = query.execute()
            return resp.data or []

        except Exception as e:
            logger.error(f"Brain query failed: {e}")
            return []

    def get_ensemble_performance_for_brain(
        self,
        limit: int = 200,
        lookback_hours: float = 24.0,
    ) -> List[Dict]:
        """Fetch settled ensemble decisions for Brain analysis."""
        try:
            from datetime import datetime, timezone, timedelta
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()

            resp = (self._sb.table(ENSEMBLE_LOG_TABLE)
                    .select("direction, should_trade, reason, aggregate_confidence, "
                            "weighted_edge_bps, kelly_fraction, regime, "
                            "contributing_signal_names, was_correct, trade_pnl, "
                            "created_at")
                    .not_.is_("was_correct", "null")
                    .gte("created_at", cutoff)
                    .order("created_at", desc=True)
                    .limit(limit)
                    .execute())
            return resp.data or []

        except Exception as e:
            logger.error(f"Ensemble query failed: {e}")
            return []

    def get_signal_combo_stats(self) -> List[Dict]:
        """
        Query the v_signal_combos view for Brain analysis.
        Shows which signal combinations have the best win rates.
        """
        try:
            resp = (self._sb.table("v_signal_combos")
                    .select("*")
                    .execute())
            return resp.data or []
        except Exception:
            return []
