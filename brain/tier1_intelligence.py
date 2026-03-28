"""
tier1_intelligence.py — Market Intelligence + Structural Integrity Audit (every 6 hours)

TWO PHASES:
  Phase 1: INTERNAL AUDIT — checks own data for corruption, anomalies, bugs
  Phase 2: EXTERNAL INTELLIGENCE — web search for market changes, fees, competition

If Phase 1 finds critical issues, it pauses the bot BEFORE Phase 2 even runs.
This prevents the bot from trading on corrupt data while researching fee changes.

Run: python -m brain.run --tier 1
"""

import json
import logging
import sys
import statistics
from collections import Counter
from datetime import datetime, timezone, timedelta

from brain.base import BrainBase

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(name)-10s | %(message)s", stream=sys.stdout)
logger = logging.getLogger("tier1")


# ── Phase 1: Structural Integrity Audit (no API call needed) ──────

def run_structural_audit(brain: BrainBase, mode: str) -> dict:
    """
    Analyze the bot's own data for signs of corruption, bugs, or anomalies.
    Returns an audit report dict. Does NOT call Claude — pure statistical checks.
    """
    trades = brain.get_trades_for_audit(limit=200, mode=mode)
    skips = brain.get_skips_for_audit(limit=200, mode=mode)
    heartbeats = brain.get_recent_heartbeats(limit=20)

    findings = []
    severity = "PASS"  # PASS, WARN, CRITICAL

    # ── CHECK 1: Win rate anomaly ──
    # A real binary market strategy should NOT have >85% win rate over 30+ trades
    settled = [t for t in trades if t.get("won") is not None]
    unsettled = [t for t in trades if t.get("won") is None]
    if len(settled) >= 30:
        wins = sum(1 for t in settled if t.get("won"))
        win_rate = wins / len(settled)
        if win_rate >= 0.95:
            findings.append({
                "check": "WIN_RATE_IMPOSSIBLE",
                "severity": "CRITICAL",
                "detail": f"Win rate {win_rate:.0%} over {len(settled)} trades — statistically impossible for binary markets. Data is likely corrupt.",
                "value": round(win_rate, 3),
            })
            severity = "CRITICAL"
        elif win_rate >= 0.85:
            findings.append({
                "check": "WIN_RATE_SUSPICIOUS",
                "severity": "WARN",
                "detail": f"Win rate {win_rate:.0%} over {len(settled)} trades — unusually high, may indicate data issues.",
                "value": round(win_rate, 3),
            })
            if severity != "CRITICAL":
                severity = "WARN"

    # ── CHECK 2: Identical P&L values ──
    # Every trade having the exact same P&L = systematic bug (like the $98 issue)
    pnls = [round(t.get("pnl_usdc", 0), 4) for t in settled if t.get("pnl_usdc") is not None]
    if len(pnls) >= 10:
        pnl_counter = Counter(pnls)
        most_common_pnl, most_common_count = pnl_counter.most_common(1)[0]
        if most_common_count >= len(pnls) * 0.80:  # 80%+ trades have same P&L
            findings.append({
                "check": "IDENTICAL_PNL",
                "severity": "CRITICAL",
                "detail": f"{most_common_count}/{len(pnls)} trades have identical P&L of ${most_common_pnl:+.4f} — systematic pricing bug.",
                "value": most_common_pnl,
            })
            severity = "CRITICAL"

    # ── CHECK 3: Unrealistic prices ──
    # Binary market prices should be between $0.10-$0.90
    price_field = "hypothetical_price" if mode == "paper" else "fill_price"
    prices = [float(t.get(price_field, 0) or 0) for t in trades if t.get(price_field)]
    bad_prices = [p for p in prices if p > 0 and (p < 0.10 or p > 0.90)]
    if bad_prices and len(prices) > 0:
        bad_pct = len(bad_prices) / len(prices)
        if bad_pct >= 0.20:
            findings.append({
                "check": "UNREALISTIC_PRICES",
                "severity": "CRITICAL",
                "detail": f"{len(bad_prices)}/{len(prices)} trades ({bad_pct:.0%}) have prices outside [0.10, 0.90]. "
                          f"Examples: {bad_prices[:5]}. Market data parsing is broken.",
                "value": round(bad_pct, 3),
            })
            severity = "CRITICAL"
        elif bad_pct > 0:
            findings.append({
                "check": "SOME_BAD_PRICES",
                "severity": "WARN",
                "detail": f"{len(bad_prices)} trades have prices outside [0.10, 0.90]: {bad_prices[:5]}",
                "value": round(bad_pct, 3),
            })
            if severity != "CRITICAL":
                severity = "WARN"

    # ── CHECK 4: P&L magnitude anomaly ──
    # With $2 trade size, max realistic P&L is ~$4
    # P&L > $10 per trade = almost certainly a bug
    extreme_pnl = [p for p in pnls if abs(p) > 10.0]
    if extreme_pnl:
        findings.append({
            "check": "EXTREME_PNL",
            "severity": "CRITICAL",
            "detail": f"{len(extreme_pnl)} trades have P&L > $10: {extreme_pnl[:5]}. "
                      f"At $2 trade size, max realistic P&L is ~$4. Pricing data is corrupt.",
            "value": max(abs(p) for p in extreme_pnl),
        })
        severity = "CRITICAL"

    # ── CHECK 5: Zero settlements ──
    # If we have 20+ trades but zero settlements, the settlement logic is broken
    if len(trades) >= 20 and len(settled) == 0:
        findings.append({
            "check": "ZERO_SETTLEMENTS",
            "severity": "CRITICAL",
            "detail": f"{len(trades)} trades placed, 0 settled. Settlement logic is broken — "
                      f"likely a Gamma API parsing issue.",
            "value": len(trades),
        })
        severity = "CRITICAL"
    elif len(trades) >= 20 and len(unsettled) > len(settled) * 3:
        findings.append({
            "check": "SETTLEMENT_BACKLOG",
            "severity": "WARN",
            "detail": f"Settlement backlog: {len(unsettled)} unsettled vs {len(settled)} settled.",
            "value": len(unsettled),
        })
        if severity != "CRITICAL":
            severity = "WARN"

    # ── CHECK 6: P&L standard deviation ──
    # Real trading P&L should have variance. stdev near zero = all trades identical.
    if len(pnls) >= 10:
        pnl_stdev = statistics.stdev(pnls)
        if pnl_stdev < 0.01:
            findings.append({
                "check": "ZERO_VARIANCE_PNL",
                "severity": "CRITICAL",
                "detail": f"P&L stdev is ${pnl_stdev:.6f} — near zero across {len(pnls)} trades. Systematic bug.",
                "value": round(pnl_stdev, 6),
            })
            severity = "CRITICAL"

    # ── CHECK 7: Confidence-price alignment ──
    # Confidence should roughly correlate with price
    if settled and prices:
        confs = [float(t.get("oracle_confidence", 0) or 0) for t in settled if t.get("oracle_confidence")]
        valid_prices = [p for p in prices if p > 0]
        if confs and valid_prices:
            avg_conf = statistics.mean(confs)
            avg_price = statistics.mean(valid_prices)
            if avg_conf > 0.50 and avg_price > 0 and abs(avg_conf - avg_price) > 0.35:
                findings.append({
                    "check": "CONFIDENCE_PRICE_DIVERGENCE",
                    "severity": "CRITICAL",
                    "detail": f"Avg confidence={avg_conf:.2f} but avg price=${avg_price:.2f}. "
                              f"Gap of {abs(avg_conf - avg_price):.2f} — market data not reflecting reality.",
                    "value": round(abs(avg_conf - avg_price), 3),
                })
                severity = "CRITICAL"

    # ── CHECK 8: Heartbeat gaps ──
    # Gaps > 10 minutes = Engine downtime
    if len(heartbeats) >= 2:
        for i in range(len(heartbeats) - 1):
            try:
                t1 = datetime.fromisoformat(str(heartbeats[i]["created_at"]).replace("Z", "+00:00"))
                t2 = datetime.fromisoformat(str(heartbeats[i + 1]["created_at"]).replace("Z", "+00:00"))
                gap = (t1 - t2).total_seconds()
                if gap > 600:
                    findings.append({
                        "check": "HEARTBEAT_GAP",
                        "severity": "WARN",
                        "detail": f"Engine was offline for {int(gap / 60)} minutes between heartbeats.",
                        "value": int(gap),
                    })
                    if severity != "CRITICAL":
                        severity = "WARN"
                    break
            except Exception:
                pass

    # ── CHECK 9: Zero activity ──
    if len(trades) == 0 and len(skips) == 0:
        findings.append({
            "check": "NO_ACTIVITY",
            "severity": "WARN",
            "detail": "Zero trades AND zero skips. Engine may not be running or market scanning is broken.",
            "value": 0,
        })
        if severity != "CRITICAL":
            severity = "WARN"
    elif len(trades) == 0 and len(skips) > 50:
        findings.append({
            "check": "ALL_SKIPS_NO_TRADES",
            "severity": "WARN",
            "detail": f"{len(skips)} skips, 0 trades. Config thresholds may be too restrictive.",
            "value": len(skips),
        })
        if severity != "CRITICAL":
            severity = "WARN"

    # ── CHECK 10: Counterfactual missed profit ──
    settled_skips = [s for s in skips if s.get("would_have_won") is not None]
    if len(settled_skips) >= 30:
        skip_wins = sum(1 for s in settled_skips if s.get("would_have_won"))
        skip_wr = skip_wins / len(settled_skips)
        skip_pnl = sum(s.get("counterfactual_pnl", 0) for s in settled_skips)
        if skip_wr > 0.65 and skip_pnl > 0:
            findings.append({
                "check": "MISSED_PROFIT",
                "severity": "WARN",
                "detail": f"Skipped trades have {skip_wr:.0%} win rate and ${skip_pnl:+.2f} hypothetical P&L. "
                          f"Consider loosening min_confidence or min_edge_bps.",
                "value": round(skip_pnl, 2),
            })
            if severity != "CRITICAL":
                severity = "WARN"

    # Build audit summary
    audit = {
        "severity": severity,
        "total_trades": len(trades),
        "settled_trades": len(settled),
        "unsettled_trades": len(unsettled),
        "total_skips": len(skips),
        "findings_count": len(findings),
        "findings": findings,
    }

    if not findings:
        audit["summary"] = "All structural checks passed. Data integrity is clean."
    else:
        critical = [f for f in findings if f["severity"] == "CRITICAL"]
        warns = [f for f in findings if f["severity"] == "WARN"]
        audit["summary"] = (
            f"{len(critical)} CRITICAL, {len(warns)} WARN issues found. "
            + (" ".join(f["check"] for f in critical) if critical else "")
        )

    return audit


# ── Phase 2: External Market Intelligence (Claude API call) ──────

INTELLIGENCE_PROMPT = """You are a market intelligence analyst for a Polymarket trading bot.
The bot trades 5-minute BTC up/down markets using oracle-informed maker orders.

CURRENT PERFORMANCE:
{performance}

CURRENT CONFIG:
{config}

STRUCTURAL AUDIT RESULTS:
{audit_summary}

RESEARCH using web search:
1. Polymarket fee structure — any changes to taker fees or maker rebates since Feb 2026?
2. Are 5-minute BTC markets still active and liquid? Daily volume?
3. Any Polymarket API changes, outages, or rule updates?
4. Competition level — are more bots entering 5-min markets?
5. Any new market types (1-min, ETH, SOL) worth evaluating?

Consider both the external market conditions AND the internal audit results.
If the audit found CRITICAL issues, your recommendations should prioritize fixing those.

Respond with ONLY this JSON (no markdown fences):
{{
  "market_conditions": {{
    "fee_structure_changed": <bool>,
    "fee_details": "<string if changed>",
    "five_min_btc_active": <bool>,
    "daily_volume_estimate": "<string>",
    "competition_level": "LOW" | "MEDIUM" | "HIGH" | "EXTREME"
  }},
  "strategy_assessment": {{
    "current_strategy_viable": <bool>,
    "reasoning": "<string>"
  }},
  "data_integrity_assessment": {{
    "data_clean": <bool>,
    "issues_found": ["<string>"],
    "recommended_actions": ["<string>"]
  }},
  "opportunities": [
    {{"name": "<string>", "description": "<string>", "priority": "LOW"|"MEDIUM"|"HIGH"}}
  ],
  "threats": [
    {{"threat": "<string>", "likelihood": "LOW"|"MEDIUM"|"HIGH", "mitigation": "<string>"}}
  ],
  "config_changes": {{
    "min_edge_bps": <int or null>,
    "min_confidence": <float or null>,
    "skew_bps": <int or null>,
    "quote_size_usdc": <float or null>,
    "kelly_fraction": <float or null>
  }},
  "recommendations": ["<string>"],
  "reasoning": "<detailed analysis>",
  "severity": "INFO" | "MINOR" | "MAJOR" | "CRITICAL"
}}"""


# ── Main Run Function ────────────────────────────────────────

def run():
    brain = BrainBase()
    mode = brain.get_bot_mode()
    logger.info(f"TIER 1: Market Intelligence + Structural Audit — mode={mode}")

    # ── PHASE 1: Structural integrity audit (no API call) ──
    logger.info("Phase 1: Running structural integrity audit...")
    audit = run_structural_audit(brain, mode)
    audit_severity = audit["severity"]

    # Alert on findings
    if audit["findings"]:
        findings_summary = "\n".join(
            f"  {'🔴' if f['severity'] == 'CRITICAL' else '⚠️'} {f['check']}: {f['detail'][:150]}"
            for f in audit["findings"]
        )
        brain.alert(
            f"Tier1 AUDIT: {audit_severity} | {audit['findings_count']} issues\n{findings_summary}",
            "ERROR" if audit_severity == "CRITICAL" else "WARN"
        )
    else:
        brain.alert("Tier1 AUDIT: PASS — all structural checks clean", "INFO")

    # If CRITICAL, pause the bot immediately — don't wait for Phase 2
    if audit_severity == "CRITICAL":
        critical_checks = [f["check"] for f in audit["findings"] if f["severity"] == "CRITICAL"]

        if mode == "live":
            # Auto-fallback to paper
            brain.set_bot_mode("paper", f"Tier1 AUDIT auto-fallback")
            brain.alert(
                f"🚨 **AUTO-FALLBACK: Live → Paper**\n"
                f"Tier1 audit found CRITICAL issues: {', '.join(critical_checks)}\n\n"
                f"**What happened:** Bot switched to PAPER mode automatically.\n"
                f"**Next steps:**\n"
                f"• `!brain 2` — Run quant analysis on the data\n"
                f"• `!trades` — Review recent trades for anomalies\n"
                f"• `!skips100` — Check counterfactual data\n"
                f"• Fix the underlying issue, wipe corrupt data if needed\n"
                f"• `!mode live confirm` — Switch back when resolved",
                "ERROR"
            )
        else:
            brain.pause_bot(f"Tier1 AUDIT: CRITICAL — {', '.join(critical_checks)}")
            brain.alert(
                f"🚨 **BOT PAUSED** by Tier1 audit\n"
                f"Critical issues: {', '.join(critical_checks)}\n\n"
                f"**Next steps:**\n"
                f"• Review the audit findings above\n"
                f"• Fix the underlying bug in the code\n"
                f"• Wipe corrupt data: `DELETE FROM paper_trades;` in Supabase SQL Editor\n"
                f"• `!resume` — Resume after fixing\n"
                f"• `!brain 1` — Re-run audit to verify the fix",
                "ERROR"
            )

    # ── PHASE 2: External market intelligence (Claude API call) ──
    logger.info("Phase 2: Running external market intelligence...")

    trades = brain.get_settled_trades(limit=50, mode=mode)
    config = brain.get_config()

    perf = {}
    if trades:
        wins = sum(1 for t in trades if t.get("won"))
        total = sum(1 for t in trades if t.get("won") is not None)
        total_pnl = sum(t.get("pnl_usdc", 0) for t in trades if t.get("pnl_usdc"))
        perf = {
            "settled": total, "wins": wins,
            "win_rate": round(wins / total, 3) if total else 0,
            "total_pnl": round(total_pnl, 4),
        }

    audit_for_prompt = {
        "severity": audit["severity"],
        "total_trades": audit["total_trades"],
        "settled": audit["settled_trades"],
        "unsettled": audit["unsettled_trades"],
        "findings": [
            {"check": f["check"], "severity": f["severity"], "detail": f["detail"][:200]}
            for f in audit["findings"]
        ] if audit["findings"] else "No issues found — all checks passed.",
    }

    prompt = INTELLIGENCE_PROMPT.format(
        performance=json.dumps(perf, default=str),
        config=json.dumps(config, indent=1, default=str),
        audit_summary=json.dumps(audit_for_prompt, indent=1, default=str),
    )

    result = brain.call_claude(prompt, use_web_search=True)
    if not result:
        brain.save_review(1, "AUDIT_ONLY", len(trades), audit, False, None, 0)
        return

    analysis = brain.parse_json(result["text"])
    if not analysis:
        brain.save_review(1, "AUDIT_ONLY", len(trades), audit, False, None, result.get("tokens", 0))
        return

    analysis["structural_audit"] = audit
    severity = analysis.get("severity", "INFO")
    viable = analysis.get("strategy_assessment", {}).get("current_strategy_viable", True)

    # Auto-apply config changes
    changes = {k: v for k, v in analysis.get("config_changes", {}).items() if v is not None}
    applied = False
    config_id = None

    if changes and brain.validate_bounds(changes):
        if mode == "paper":
            # PAPER MODE: Auto-apply all valid changes — worst case is bad paper data
            config_id = brain.insert_config(changes, config, f"Tier1 auto-apply (paper): {analysis.get('reasoning', '')[:200]}")
            applied = True
            brain.alert(
                f"⚙️ **AUTO-CONFIG (paper):** {changes}\n"
                f"Reason: {analysis.get('reasoning', '?')[:200]}\n\n"
                f"**Next steps:**\n"
                f"• Config takes effect within 30 seconds\n"
                f"• `!config` — Verify the new values\n"
                f"• `!trades` — Monitor paper trade results\n"
                f"• `!skips100` — Check if more trades are qualifying\n"
                f"• `!brain 2` — Run quant analysis after 50+ trades with new config",
                "INFO"
            )
        elif severity in ("INFO", "MINOR") and audit_severity == "PASS":
            # LIVE MODE: Only apply if audit clean and severity low
            config_id = brain.insert_config(changes, config, f"Tier1: {analysis.get('reasoning', '')[:200]}")
            applied = True
            brain.alert(
                f"⚙️ **CONFIG UPDATED (live):** {changes}\n"
                f"Reason: {analysis.get('reasoning', '?')[:200]}\n\n"
                f"**Next steps:**\n"
                f"• `!config` — Verify the new values\n"
                f"• `!status` — Monitor performance\n"
                f"• `!set <param> <old_value>` — Revert if needed",
                "INFO"
            )
        else:
            brain.alert(
                f"📋 **CONFIG PROPOSED (not applied):** {changes}\n"
                f"Reason: Not applied because severity={severity}, audit={audit_severity}\n\n"
                f"**Next steps:**\n"
                f"• Review the proposed changes above\n"
                f"• `!set <param> <value>` — Apply manually if you agree\n"
                f"• `!brain 1` — Re-run after fixing audit issues",
                "WARN"
            )

    if not viable:
        brain.pause_bot(f"Tier1: Strategy no longer viable")
        brain.alert(
            "🚨 **STRATEGY NOT VIABLE — bot paused**\n"
            f"Reason: {analysis.get('strategy_assessment', {}).get('reasoning', '?')[:200]}\n\n"
            f"**Next steps:**\n"
            f"• `!brain 3` — Run strategy discovery for alternatives\n"
            f"• `!resume` — Resume if you disagree with the assessment\n"
            f"• Review market conditions manually on polymarket.com",
            "ERROR"
        )

    if analysis.get("market_conditions", {}).get("fee_structure_changed"):
        brain.alert(f"FEE CHANGE: {analysis['market_conditions'].get('fee_details', '?')}", "ERROR")

    data_assessment = analysis.get("data_integrity_assessment", {})
    if not data_assessment.get("data_clean", True):
        issues = data_assessment.get("issues_found", [])
        brain.alert(f"Tier1 DATA INTEGRITY: Claude found issues — {'; '.join(issues[:3])}", "WARN")

    brain.save_review(1, "INTELLIGENCE+AUDIT", len(trades), analysis, applied, config_id, result.get("tokens", 0))

    audit_tag = f"audit={audit_severity}"
    brain.alert(
        f"Tier1: viable={viable} | {audit_tag} | severity={severity}"
        + (f" | Config: {changes}" if applied else ""),
        "INFO"
    )


if __name__ == "__main__":
    run()
