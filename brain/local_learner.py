"""
Local Learning Engine
======================
Automatically adjusts signal weights based on which signals
historically predict correctly. Runs locally without API calls.

Every N settled trades (default 50), computes per-signal win rate
and adjusts weights toward signals that work, away from those that don't.

All weight changes bounded ±20% from base config values.
Brain tiers become weekly strategic reviews rather than continuous optimization.

Ported from: aulekator/feedback/learning_engine.py
Adapted for our signal architecture.
"""

import time
import logging
from typing import Dict, List, Optional

logger = logging.getLogger("oracle.learner")

# Maximum weight adjustment: ±20% from base
MAX_ADJUSTMENT_PCT = 0.20


class LocalLearner:
    """
    Automatic signal weight optimization.

    Usage:
        learner = LocalLearner(supabase, min_trades=50)
        # Call periodically (e.g. after each settlement):
        changes = await learner.maybe_adjust(current_config)
        if changes:
            apply_config_changes(changes)
    """

    def __init__(self, supabase_client, min_trades: int = 50, learning_rate: float = 0.10):
        self._sb = supabase_client
        self.min_trades = min_trades
        self.learning_rate = learning_rate
        self._last_adjustment_at = 0  # trade count at last adjustment
        self._adjustment_count = 0

    async def maybe_adjust(self, current_config: dict) -> Optional[Dict]:
        """
        Check if enough trades have settled to warrant a weight adjustment.

        Returns:
            Dict of config changes to apply, or None if not enough data.
        """
        try:
            # Count settled trades since last adjustment
            result = self._sb.table("paper_settled").select(
                "id", count="exact"
            ).not_.is_("won", "null").execute()

            total_settled = result.count or 0
            trades_since_last = total_settled - self._last_adjustment_at

            if trades_since_last < self.min_trades:
                return None

            logger.info(
                f"LocalLearner: {trades_since_last} new settlements "
                f"(total {total_settled}), running adjustment..."
            )

            # Fetch per-signal performance from v_signal_performance view
            perf_result = self._sb.table("v_signal_performance").select("*").execute()
            signal_perf = {
                row["signal_name"]: {
                    "win_rate": float(row.get("win_rate", 0)),
                    "total": int(row.get("settled_markets", 0)),
                    "correct": int(row.get("correct_markets", 0)),
                }
                for row in (perf_result.data or [])
                if row.get("settled_markets", 0) >= 10  # Minimum sample
            }

            if not signal_perf:
                logger.info("LocalLearner: No signals with enough data")
                self._last_adjustment_at = total_settled
                return None

            # Compute ensemble average win rate
            total_correct = sum(s["correct"] for s in signal_perf.values())
            total_markets = sum(s["total"] for s in signal_perf.values())
            avg_wr = total_correct / total_markets if total_markets > 0 else 0.5

            # Compute weight adjustments
            changes = {}
            weight_keys = {
                "ORACLE_CONFIDENCE": "weight_oracle_confidence",
                "STALE_QUOTE": "weight_stale_quote",
                "LIQUIDITY_VACUUM": "weight_liquidity_vacuum",
                "OB_IMBALANCE": "weight_ob_imbalance",
                "COMPLEMENT_ARB": "weight_complement_arb",
                "WHALE_FLOW": "weight_whale_flow",
            }

            for signal_name, config_key in weight_keys.items():
                perf = signal_perf.get(signal_name)
                if not perf:
                    continue

                current_weight = float(current_config.get(config_key, 1.0))
                base_weight = current_weight  # Assume current is base

                if perf["win_rate"] > avg_wr:
                    # Signal outperforms → increase weight
                    adjustment = self.learning_rate * (perf["win_rate"] - avg_wr)
                else:
                    # Signal underperforms → decrease weight
                    adjustment = self.learning_rate * (perf["win_rate"] - avg_wr)

                new_weight = current_weight + adjustment

                # Bound within ±20% of base
                min_weight = base_weight * (1 - MAX_ADJUSTMENT_PCT)
                max_weight = base_weight * (1 + MAX_ADJUSTMENT_PCT)
                new_weight = max(min_weight, min(max_weight, new_weight))
                new_weight = round(new_weight, 3)

                if abs(new_weight - current_weight) > 0.01:
                    changes[config_key] = new_weight
                    logger.info(
                        f"  {signal_name}: WR={perf['win_rate']:.1%} "
                        f"(avg={avg_wr:.1%}) → weight {current_weight:.3f} → {new_weight:.3f}"
                    )

            self._last_adjustment_at = total_settled
            self._adjustment_count += 1

            if changes:
                logger.info(f"LocalLearner: {len(changes)} weight adjustments (round #{self._adjustment_count})")
                return changes

            logger.info("LocalLearner: No adjustments needed")
            return None

        except Exception as e:
            logger.warning(f"LocalLearner failed: {e}")
            return None
