"""
tier2_quant.py — Quantitative Edge Analysis + Self-Tuning Engine

BEHAVIOR BY MODE:
  PAPER: Aggressive auto-tuning. Auto-applies config changes immediately.
         Tracks config version performance. Reverts if new config is worse.
         Sends progress reports with recommended next steps.
  LIVE:  Conservative. Proposes changes to Discord. Only auto-applies
         minor calibration tweaks. Alerts on regime shifts.

Uses Claude Opus 4.6 with counterfactual analysis of skipped trades.

Run: python -m brain.run --tier 2
"""

import json
import logging
import sys

from brain.base import BrainBase

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(name)-10s | %(message)s", stream=sys.stdout)
logger = logging.getLogger("tier2")

PROMPT = """You are a quantitative analyst performing rigorous statistical analysis on
trading data from a Polymarket 5-minute BTC prediction market bot.

The bot uses oracle-informed maker orders. Your job is to calibrate the model,
optimize parameters, and detect regime changes.

CURRENT MODE: {mode}
{"AUTO-TUNING IS ON. You can be aggressive with config changes — these are paper trades." if mode == "paper" else "LIVE MODE. Be conservative. Only propose changes you're confident about."}

TRADE DATA (last {trade_count} settled trades):
{trade_data}

COUNTERFACTUAL DATA (what would have happened on skipped trades):
{counterfactual_data}

CURRENT CONFIG:
{config}

Perform these analyses. Show your mathematical reasoning:

1. CONFIDENCE CALIBRATION
   Group trades by oracle_confidence buckets: [0.50-0.60), [0.60-0.70), [0.70-0.80), [0.80-0.90), [0.90-1.00).
   For each: actual_win_rate = wins / total.
   Compute calibration error for each bucket.
   ALSO analyze the counterfactual data — what win rates do skipped trades show at each confidence level?
   If skipped trades at confidence 0.55-0.65 would have won >60%, we should lower min_confidence.

2. EDGE OPTIMIZATION
   Analyze the counterfactual data by edge bucket.
   What's the minimum edge_bps where counterfactual win rate exceeds 55%?
   That's the optimal min_edge_bps threshold.

3. TIMING OPTIMIZATION
   Group trades by seconds_remaining buckets.
   Recommend optimal min/max_seconds_before_close.

4. REGIME DETECTION
   Rolling 10-trade win rate vs overall. Flag regime shifts.

5. EV ACCURACY
   Compare ev_per_share at trade time vs actual pnl. Are we overestimating?

6. SKEW ANALYSIS
   Is the current skew_bps causing too many "Negative EV" skips?
   If counterfactual data shows high-confidence skips that would have won,
   the skew may be too high (pushing our price too far from market).

Respond with ONLY this JSON (no markdown fences):
{{
  "confidence_calibration": {{
    "buckets": [
      {{"range": "0.50-0.60", "predicted": <float>, "actual_trades": <float>, "actual_counterfactual": <float>, "n_trades": <int>, "n_cf": <int>}}
    ],
    "overall_calibration_error": <float>,
    "model_underestimates": <bool>,
    "recommendation": "<string>"
  }},
  "edge_optimization": {{
    "optimal_min_edge_bps": <int>,
    "current_min_edge_bps": <int>,
    "counterfactual_win_rates_by_edge": [
      {{"edge_range": "5-10bps", "win_rate": <float>, "n": <int>}},
      {{"edge_range": "10-20bps", "win_rate": <float>, "n": <int>}},
      {{"edge_range": "20-50bps", "win_rate": <float>, "n": <int>}}
    ],
    "recommendation": "<string>"
  }},
  "timing_optimization": {{
    "optimal_min_seconds": <int>,
    "optimal_max_seconds": <int>,
    "recommendation": "<string>"
  }},
  "regime_detection": {{
    "current_regime": "TRENDING" | "CHOPPY" | "UNKNOWN",
    "last_10_win_rate": <float>,
    "overall_win_rate": <float>,
    "regime_shift_detected": <bool>,
    "recommendation": "<string>"
  }},
  "ev_accuracy": {{
    "avg_predicted_ev": <float>,
    "avg_realized_ev": <float>,
    "systematically_overestimating": <bool>
  }},
  "skew_analysis": {{
    "current_skew_bps": <int>,
    "negative_ev_skip_count": <int>,
    "negative_ev_counterfactual_win_rate": <float>,
    "recommended_skew_bps": <int>,
    "recommendation": "<string>"
  }},
  "config_changes": {{
    "min_edge_bps": <int or null>,
    "min_confidence": <float or null>,
    "skew_bps": <int or null>,
    "min_seconds_before_close": <int or null>,
    "max_seconds_before_close": <int or null>,
    "kelly_fraction": <float or null>,
    "quote_size_usdc": <float or null>
  }},
  "performance_summary": {{
    "total_settled": <int>,
    "win_rate": <float>,
    "total_pnl": <float>,
    "avg_pnl_per_trade": <float>,
    "missed_opportunity_pnl": <float>
  }},
  "reasoning": "<detailed mathematical reasoning>",
  "severity": "INFO" | "MINOR" | "MAJOR" | "CRITICAL"
}}"""


def run():
    brain = BrainBase()
    mode = brain.get_bot_mode()
    logger.info(f"TIER 2: Quantitative Analysis — mode={mode}")

    trades = brain.get_settled_trades(limit=200, mode=mode)
    if len(trades) < 10:
        brain.alert(
            f"🧠 Tier2: Only {len(trades)} settled trades (need 10+). Skipping analysis.\n\n"
            f"**Next steps:**\n"
            f"• Wait for more trades to settle (check `!status`)\n"
            f"• `!skips` — Are trades being skipped? May need to loosen thresholds\n"
            f"• `!set min_edge_bps 10` and `!set min_confidence 0.55` to generate more trades",
            "INFO"
        )
        return

    config = brain.get_config()
    counterfactual = brain.get_counterfactual_stats(mode=mode)

    prompt = PROMPT.format(
        mode=mode.upper(),
        trade_count=len(trades),
        trade_data=json.dumps(trades[:100], indent=1, default=str),  # Limit to 100 to fit context
        counterfactual_data=json.dumps(counterfactual, indent=1, default=str),
        config=json.dumps(config, indent=1, default=str),
    )

    result = brain.call_claude(prompt, max_tokens=8192)
    if not result:
        brain.alert(
            "🔴 Tier2: Claude API call failed.\n\n"
            "**Next steps:**\n"
            "• Check Anthropic billing at console.anthropic.com\n"
            "• `!brain 2` — Retry manually",
            "ERROR"
        )
        return

    analysis = brain.parse_json(result["text"])
    if not analysis:
        brain.alert(
            "🔴 Tier2: Failed to parse Claude response.\n\n"
            "**Next steps:**\n"
            "• `!brain 2` — Retry\n"
            "• Check Railway logs for the Brain-Tier2 service",
            "ERROR"
        )
        return

    severity = analysis.get("severity", "INFO")
    perf = analysis.get("performance_summary", {})

    # ── Apply config changes ──
    changes = {k: v for k, v in analysis.get("config_changes", {}).items() if v is not None}
    applied = False
    config_id = None

    if changes and brain.validate_bounds(changes):
        if mode == "paper":
            # PAPER MODE: Auto-apply immediately
            reason = f"Tier2 auto-tune (paper): {analysis.get('reasoning', '')[:200]}"
            config_id = brain.insert_config(changes, config, reason, "TIER2")
            applied = True

            # Build detailed change description
            change_lines = []
            for k, v in changes.items():
                old_v = config.get(k, "?")
                change_lines.append(f"  `{k}`: {old_v} → **{v}**")

            brain.alert(
                f"⚙️ **AUTO-TUNE (paper) — Config v{config_id}**\n"
                + "\n".join(change_lines) + "\n\n"
                f"**Analysis summary:**\n"
                f"  Win rate: {perf.get('win_rate', 0):.0%} over {perf.get('total_settled', 0)} trades\n"
                f"  Total P&L: ${perf.get('total_pnl', 0):+.4f}\n"
                f"  Missed opportunity P&L: ${perf.get('missed_opportunity_pnl', 0):+.4f}\n\n"
                f"**Reasoning:** {analysis.get('reasoning', '?')[:300]}\n\n"
                f"**Next steps:**\n"
                f"• Config takes effect within 30 seconds\n"
                f"• `!config` — Verify new values\n"
                f"• `!trades` — Monitor results with new config\n"
                f"• `!brain 2` — Re-run after 50+ more trades to check improvement\n"
                f"• `!set <param> <old_value>` — Revert any specific change if needed",
                "INFO"
            )
        elif severity in ("INFO", "MINOR"):
            # LIVE MODE: Auto-apply only minor tweaks
            reason = f"Tier2 calibration (live): {analysis.get('reasoning', '')[:200]}"
            config_id = brain.insert_config(changes, config, reason, "TIER2")
            applied = True

            change_lines = []
            for k, v in changes.items():
                old_v = config.get(k, "?")
                change_lines.append(f"  `{k}`: {old_v} → **{v}**")

            brain.alert(
                f"⚙️ **CONFIG UPDATED (live) — v{config_id}**\n"
                + "\n".join(change_lines) + "\n\n"
                f"**Next steps:**\n"
                f"• `!config` — Verify\n"
                f"• `!status` — Monitor P&L\n"
                f"• `!set <param> <old_value>` — Revert if performance drops",
                "INFO"
            )
        else:
            # LIVE MODE + HIGH SEVERITY: Propose only
            change_lines = []
            for k, v in changes.items():
                old_v = config.get(k, "?")
                change_lines.append(f"  `!set {k} {v}` (currently {old_v})")

            brain.alert(
                f"📋 **CONFIG PROPOSED (not applied) — severity={severity}**\n"
                + "\n".join(change_lines) + "\n\n"
                f"**Reasoning:** {analysis.get('reasoning', '?')[:300]}\n\n"
                f"**Next steps:**\n"
                f"• Review the analysis above\n"
                + "\n".join(f"• {line.strip()}" for line in change_lines) + "\n"
                f"• `!brain 2` — Re-run if you want a fresh analysis",
                "WARN"
            )

    # ── Regime shift ──
    regime = analysis.get("regime_detection", {})
    if regime.get("regime_shift_detected"):
        brain.alert(
            f"📊 **REGIME SHIFT detected**\n"
            f"  Current: {regime.get('current_regime', '?')}\n"
            f"  Last 10 trades: {regime.get('last_10_win_rate', 0):.0%} win rate\n"
            f"  Overall: {regime.get('overall_win_rate', 0):.0%} win rate\n\n"
            f"**Next steps:**\n"
            f"• `!pause` — Pause if live and concerned\n"
            f"• `!brain 2` — Re-run in 2 hours to confirm\n"
            f"• `!set min_edge_bps <higher>` — Tighten thresholds to be safer\n"
            f"• `!brain 3` — Run strategy discovery for alternatives",
            "WARN"
        )

    # ── Edge decay ──
    edge_decay = analysis.get("edge_decay", {})
    if edge_decay and edge_decay.get("decaying"):
        brain.alert(
            f"📉 **EDGE DECAY detected** — competition may be increasing\n\n"
            f"**Next steps:**\n"
            f"• `!brain 3` — Run strategy discovery for new approaches\n"
            f"• Consider reducing `quote_size_usdc` to minimize exposure\n"
            f"• `!set quote_size_usdc 1.0` — Reduce trade size",
            "WARN"
        )

    # ── Skew analysis ──
    skew = analysis.get("skew_analysis", {})
    if skew and skew.get("negative_ev_counterfactual_win_rate", 0) > 0.65:
        brain.alert(
            f"📊 **SKEW ISSUE:** {skew.get('negative_ev_skip_count', 0)} trades skipped for 'Negative EV' "
            f"but {skew.get('negative_ev_counterfactual_win_rate', 0):.0%} would have won.\n"
            f"Current skew: {skew.get('current_skew_bps', 0)}bps → "
            f"Recommended: {skew.get('recommended_skew_bps', 0)}bps\n\n"
            f"**Next steps:**\n"
            f"• `!set skew_bps {skew.get('recommended_skew_bps', 50)}` — Apply the recommendation\n"
            f"• Lower skew = tighter pricing = more trades pass EV check",
            "INFO"
        )

    # ── Critical severity in live mode → auto-fallback ──
    if severity == "CRITICAL" and mode == "live":
        brain.set_bot_mode("paper", "Tier2: CRITICAL severity auto-fallback")
        brain.alert(
            f"🚨 **AUTO-FALLBACK: Live → Paper**\n"
            f"Tier2 analysis found CRITICAL issues.\n"
            f"Reason: {analysis.get('reasoning', '?')[:300]}\n\n"
            f"**Next steps:**\n"
            f"• Bot is now in PAPER mode\n"
            f"• `!trades` — Review what went wrong\n"
            f"• `!brain 2` — Re-analyze after paper testing\n"
            f"• `!mode live confirm` — Switch back when confident",
            "ERROR"
        )

    # ── Save and final summary ──
    brain.save_review(2, "QUANT", len(trades), analysis, applied, config_id, result.get("tokens", 0))

    # Progress report
    calib_err = analysis.get("confidence_calibration", {}).get("overall_calibration_error", "?")
    underestimates = analysis.get("confidence_calibration", {}).get("model_underestimates", False)

    summary = (
        f"🧠 **Tier2 Complete** ({mode.upper()})\n"
        f"  Trades analyzed: {len(trades)}\n"
        f"  Win rate: {perf.get('win_rate', 0):.0%}\n"
        f"  P&L: ${perf.get('total_pnl', 0):+.4f}\n"
        f"  Calibration error: {calib_err}\n"
        f"  Model {'under' if underestimates else 'over'}estimates\n"
        f"  Severity: {severity}\n"
    )
    if applied:
        summary += f"  ✅ Config auto-applied (v{config_id})\n"

    missed = perf.get("missed_opportunity_pnl", 0)
    if missed and missed > 0:
        summary += f"  ⚠️ Missed ${missed:+.2f} on skipped trades\n"

    summary += (
        f"\n**Next steps:**\n"
        f"• `!config` — Review current config\n"
        f"• `!trades` — Monitor with {'new' if applied else 'current'} config\n"
    )
    if mode == "paper" and perf.get("win_rate", 0) > 0.60 and len(trades) >= 50:
        summary += f"• **Consider going live:** 50+ trades at {perf.get('win_rate', 0):.0%} win rate\n"
        summary += f"• `!mode live confirm` — Switch to live trading\n"
    elif mode == "paper":
        summary += f"• Accumulate more trades before considering live mode\n"

    brain.alert(summary, "INFO")


if __name__ == "__main__":
    run()
