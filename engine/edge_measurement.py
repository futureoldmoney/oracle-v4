"""
Edge Measurement Framework
============================
Statistical tools for measuring and proving signal edge.

This is what separates institutional trading from gambling:
rigorous measurement of whether observed wins are skill or luck.

Key concepts:
1. Win rate alone is meaningless without the entry price (implied probability)
2. Edge = (actual win rate) - (average implied probability at entry)
3. Need sufficient sample size for statistical significance
4. Signals should be evaluated both independently and in combination
"""

import math
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from collections import defaultdict

logger = logging.getLogger("oracle.edge")


@dataclass
class TradeRecord:
    """A settled trade with outcome information."""
    signal_name: str
    direction: str            # "UP" or "DOWN"
    entry_price: float        # Price paid (implied probability)
    size_usd: float
    won: bool
    pnl_usd: float
    timestamp: float
    metadata: Dict = field(default_factory=dict)

    @property
    def implied_probability(self) -> float:
        """The market's implied probability at entry."""
        return self.entry_price

    @property
    def pnl_bps(self) -> float:
        """P&L in basis points relative to size."""
        if self.size_usd == 0:
            return 0.0
        return (self.pnl_usd / self.size_usd) * 10000.0


@dataclass
class EdgeEstimate:
    """Statistical estimate of a signal's edge."""
    signal_name: str
    sample_size: int
    win_rate: float
    avg_implied_prob: float
    realized_edge: float       # win_rate - avg_implied_prob
    edge_std_error: float      # Standard error of edge estimate
    z_score: float             # How many SDs from zero
    p_value: float             # Probability edge is just luck
    confidence_interval_95: Tuple[float, float]
    total_pnl_usd: float
    total_pnl_bps: float
    avg_pnl_per_trade_bps: float
    sharpe_equivalent: float   # Approximate Sharpe if annualized
    is_significant_2sigma: bool
    is_significant_3sigma: bool
    trades_needed_for_significance: int


class EdgeCalculator:
    """
    Computes rigorous edge statistics from trade history.
    
    Usage:
        calc = EdgeCalculator()
        estimate = calc.compute_edge(trade_records, signal_name="ORACLE_CONFIDENCE")
        print(estimate.realized_edge, estimate.z_score)
    """

    @staticmethod
    def compute_edge(
        trades: List[TradeRecord],
        signal_name: Optional[str] = None,
    ) -> EdgeEstimate:
        """
        Compute edge statistics for a set of trades.
        
        Args:
            trades: List of settled trades
            signal_name: Optional filter to a specific signal
        
        Returns:
            EdgeEstimate with full statistical breakdown
        """
        if signal_name:
            trades = [t for t in trades if t.signal_name == signal_name]

        n = len(trades)
        name = signal_name or "ALL"

        if n == 0:
            return EdgeEstimate(
                signal_name=name, sample_size=0, win_rate=0, avg_implied_prob=0,
                realized_edge=0, edge_std_error=0, z_score=0, p_value=1.0,
                confidence_interval_95=(0, 0), total_pnl_usd=0, total_pnl_bps=0,
                avg_pnl_per_trade_bps=0, sharpe_equivalent=0,
                is_significant_2sigma=False, is_significant_3sigma=False,
                trades_needed_for_significance=100,
            )

        # Basic metrics
        wins = sum(1 for t in trades if t.won)
        win_rate = wins / n
        avg_implied = sum(t.implied_probability for t in trades) / n
        realized_edge = win_rate - avg_implied

        # Total P&L
        total_pnl_usd = sum(t.pnl_usd for t in trades)
        total_size = sum(t.size_usd for t in trades)
        total_pnl_bps = (total_pnl_usd / total_size * 10000) if total_size > 0 else 0
        avg_pnl_bps = total_pnl_bps / n if n > 0 else 0

        # Standard error of win rate
        # For Bernoulli trials: SE = sqrt(p(1-p)/n)
        # But we use the IMPLIED probability as the null hypothesis
        se_null = math.sqrt(avg_implied * (1 - avg_implied) / n) if n > 0 and 0 < avg_implied < 1 else 0.5

        # Z-score: how many standard errors is the observed win rate from implied
        z = realized_edge / se_null if se_null > 0 else 0

        # P-value (one-tailed: probability of seeing this edge or higher by chance)
        # Using normal approximation
        p_value = 1.0 - _norm_cdf(z)

        # 95% confidence interval for the edge
        ci_lower = realized_edge - 1.96 * se_null
        ci_upper = realized_edge + 1.96 * se_null

        # Sharpe-equivalent
        # Treat each trade as a return observation
        returns = [t.pnl_bps for t in trades]
        if len(returns) > 1:
            mean_ret = sum(returns) / len(returns)
            var_ret = sum((r - mean_ret) ** 2 for r in returns) / (len(returns) - 1)
            std_ret = math.sqrt(var_ret) if var_ret > 0 else 1.0
            # Annualize assuming ~100 trades/day, 365 days
            # This is approximate but gives a Sharpe-like number
            trades_per_year = 100 * 365
            sharpe = (mean_ret / std_ret) * math.sqrt(trades_per_year) if std_ret > 0 else 0
        else:
            sharpe = 0

        # Trades needed for 2σ significance
        # n = (z_target * SE_per_trade / edge)^2
        se_per_trade = math.sqrt(avg_implied * (1 - avg_implied)) if 0 < avg_implied < 1 else 0.5
        if abs(realized_edge) > 0.001:
            trades_for_2sigma = int((2.0 * se_per_trade / abs(realized_edge)) ** 2) + 1
        else:
            trades_for_2sigma = 10000  # Edge too small to detect

        return EdgeEstimate(
            signal_name=name,
            sample_size=n,
            win_rate=round(win_rate, 4),
            avg_implied_prob=round(avg_implied, 4),
            realized_edge=round(realized_edge, 4),
            edge_std_error=round(se_null, 4),
            z_score=round(z, 3),
            p_value=round(p_value, 4),
            confidence_interval_95=(round(ci_lower, 4), round(ci_upper, 4)),
            total_pnl_usd=round(total_pnl_usd, 4),
            total_pnl_bps=round(total_pnl_bps, 1),
            avg_pnl_per_trade_bps=round(avg_pnl_bps, 1),
            sharpe_equivalent=round(sharpe, 2),
            is_significant_2sigma=abs(z) >= 2.0,
            is_significant_3sigma=abs(z) >= 3.0,
            trades_needed_for_significance=min(trades_for_2sigma, 10000),
        )

    @staticmethod
    def segment_analysis(
        trades: List[TradeRecord],
        segment_key: str = "signal_name",
    ) -> Dict[str, EdgeEstimate]:
        """
        Compute edge for each segment (e.g., by signal, direction, time).
        
        Args:
            trades: All trades
            segment_key: Key to segment by. Options:
                - "signal_name": per signal
                - "direction": UP vs DOWN
                - Custom: any key in trade.metadata
        """
        segments = defaultdict(list)

        for trade in trades:
            if segment_key == "signal_name":
                key = trade.signal_name
            elif segment_key == "direction":
                key = trade.direction
            elif segment_key in trade.metadata:
                key = str(trade.metadata[segment_key])
            else:
                key = "UNKNOWN"
            segments[key].append(trade)

        results = {}
        calc = EdgeCalculator()
        for key, segment_trades in segments.items():
            results[key] = calc.compute_edge(segment_trades)

        return results

    @staticmethod
    def correlation_matrix(
        trades: List[TradeRecord],
        window_seconds: float = 300.0,  # 5 minutes
    ) -> Dict[str, Dict[str, float]]:
        """
        Estimate signal correlation from co-occurring trade outcomes.
        
        Two signals are correlated if they tend to win/lose on the same trades.
        This matters for portfolio Kelly sizing.
        
        Returns: {signal_a: {signal_b: correlation, ...}, ...}
        """
        # Group trades by time window
        signals = set(t.signal_name for t in trades)
        if len(signals) < 2:
            return {}

        # Create time-bucketed outcome vectors
        buckets = defaultdict(lambda: defaultdict(list))
        for trade in trades:
            bucket = int(trade.timestamp / window_seconds)
            buckets[bucket][trade.signal_name].append(1 if trade.won else 0)

        # Compute pairwise correlations
        signal_list = sorted(signals)
        correlations = {s: {} for s in signal_list}

        for i, sig_a in enumerate(signal_list):
            for j, sig_b in enumerate(signal_list):
                if i >= j:
                    continue

                # Find buckets where both signals traded
                co_buckets = []
                for bucket, outcomes in buckets.items():
                    if sig_a in outcomes and sig_b in outcomes:
                        a_avg = sum(outcomes[sig_a]) / len(outcomes[sig_a])
                        b_avg = sum(outcomes[sig_b]) / len(outcomes[sig_b])
                        co_buckets.append((a_avg, b_avg))

                if len(co_buckets) < 5:
                    corr = 0.0  # Not enough data
                else:
                    corr = _pearson_correlation(co_buckets)

                correlations[sig_a][sig_b] = round(corr, 3)
                correlations[sig_b][sig_a] = round(corr, 3)

        return correlations

    @staticmethod
    def format_report(estimates: Dict[str, EdgeEstimate]) -> str:
        """Format edge estimates into a readable report."""
        lines = []
        lines.append("=" * 80)
        lines.append("EDGE ANALYSIS REPORT")
        lines.append("=" * 80)
        lines.append("")

        for name, est in sorted(estimates.items()):
            sig_icon = "✅" if est.is_significant_2sigma else "⚠️" if est.sample_size >= 20 else "📊"
            lines.append(f"{sig_icon} {name}")
            lines.append(f"   Trades: {est.sample_size}  |  Win Rate: {est.win_rate:.1%}  |  Implied: {est.avg_implied_prob:.1%}")
            lines.append(f"   Edge: {est.realized_edge:+.2%}  |  Z-score: {est.z_score:.2f}  |  p-value: {est.p_value:.4f}")
            lines.append(f"   95% CI: [{est.confidence_interval_95[0]:+.2%}, {est.confidence_interval_95[1]:+.2%}]")
            lines.append(f"   P&L: ${est.total_pnl_usd:+.4f} ({est.total_pnl_bps:+.1f} bps total)")
            lines.append(f"   Trades for 2σ: {est.trades_needed_for_significance}")
            if est.is_significant_2sigma:
                lines.append(f"   *** STATISTICALLY SIGNIFICANT at 2σ ***")
            if est.is_significant_3sigma:
                lines.append(f"   *** HIGHLY SIGNIFICANT at 3σ ***")
            lines.append("")

        return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def _norm_cdf(x: float) -> float:
    """
    Cumulative distribution function of standard normal.
    Approximation accurate to 1e-7.
    """
    # Abramowitz and Stegun approximation
    if x < -8:
        return 0.0
    if x > 8:
        return 1.0

    a1 = 0.254829592
    a2 = -0.284496736
    a3 = 1.421413741
    a4 = -1.453152027
    a5 = 1.061405429
    p = 0.3275911

    sign = 1 if x >= 0 else -1
    x = abs(x) / math.sqrt(2)

    t = 1.0 / (1.0 + p * x)
    y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-x * x)

    return 0.5 * (1.0 + sign * y)


def _pearson_correlation(pairs: List[Tuple[float, float]]) -> float:
    """Compute Pearson correlation coefficient from paired observations."""
    n = len(pairs)
    if n < 2:
        return 0.0

    xs = [p[0] for p in pairs]
    ys = [p[1] for p in pairs]

    mean_x = sum(xs) / n
    mean_y = sum(ys) / n

    cov = sum((x - mean_x) * (y - mean_y) for x, y in pairs) / (n - 1)
    std_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs) / (n - 1))
    std_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys) / (n - 1))

    if std_x == 0 or std_y == 0:
        return 0.0

    return cov / (std_x * std_y)


# ═══════════════════════════════════════════════════════════════
# QUICK TEST
# ═══════════════════════════════════════════════════════════════

def demo():
    """Demonstrate edge calculation with synthetic data."""
    import random
    random.seed(42)

    # Simulate 200 trades: true edge of 8% (58% win rate at 50¢ markets)
    trades = []
    for i in range(200):
        entry_price = 0.50 + random.uniform(-0.05, 0.05)  # Around 50¢
        won = random.random() < 0.58  # True 58% win rate
        pnl = (1.0 - entry_price) if won else -entry_price
        trades.append(TradeRecord(
            signal_name="ORACLE_CONFIDENCE" if i % 3 != 0 else "STALE_QUOTE",
            direction="UP" if random.random() > 0.5 else "DOWN",
            entry_price=entry_price,
            size_usd=random.uniform(1, 5),
            won=won,
            pnl_usd=pnl * random.uniform(1, 5),
            timestamp=1700000000 + i * 300,
        ))

    calc = EdgeCalculator()

    # Overall edge
    overall = calc.compute_edge(trades)
    print(f"\nOverall: {overall.sample_size} trades, "
          f"edge={overall.realized_edge:+.2%}, z={overall.z_score:.2f}")

    # Per-signal edge
    per_signal = calc.segment_analysis(trades, "signal_name")
    print(calc.format_report(per_signal))

    # Correlation
    corr = calc.correlation_matrix(trades)
    if corr:
        print("Signal Correlations:", json.dumps(corr, indent=2))


if __name__ == "__main__":
    import json
    demo()
