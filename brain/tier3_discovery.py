"""
tier3_discovery.py — Strategy Discovery (weekly or on-demand)

PhD-level strategy research from first principles:
- Full universe of possible edges given our infrastructure
- Cross-asset correlation analysis
- New market type evaluation
- Mathematical edge modeling for proposed strategies
- Formal investment memo for each candidate

Uses Claude Opus 4.6 with extended reasoning.

Run: python -m brain.tier3_discovery
"""

import json
import logging
import sys

from brain.base import BrainBase

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(name)-10s | %(message)s", stream=sys.stdout)
logger = logging.getLogger("tier3")

PROMPT = """You are a head of quantitative strategy at a prediction market trading firm.
Your job is NOT to tune existing parameters — that's done by lower tiers.
Your job is to identify fundamentally new strategies worth testing.

CURRENT STRATEGY: Oracle-informed directional maker orders on Polymarket
5-minute BTC up/down markets. We read Chainlink BTC/USD to predict
settlement direction, place postOnly limit orders on the winning side.

CURRENT PERFORMANCE ({trade_count} settled trades):
{performance}

TIER 2 LATEST ANALYSIS (if available):
{tier2_summary}

OUR INFRASTRUCTURE CAPABILITIES:
- Read Chainlink BTC/USD (and potentially ETH, SOL) price feeds on Polygon
- Read Binance spot prices (BTC, ETH, SOL) for cross-reference
- Place maker orders on Polymarket CLOB with postOnly enforcement
- Cancel/replace orders programmatically
- Monitor orderbook depth and spread
- Polymarket supports: 5-min BTC, 5-min ETH, 5-min SOL, 15-min markets
- Maker orders: zero fees + daily USDC rebates
- We run on Railway (always-on cloud container)

YOUR TASK: Propose 2-4 new strategy candidates. For each, provide:

1. STRATEGY NAME and one-sentence description
2. MATHEMATICAL EDGE MODEL: Define the edge formally.
   - What information asymmetry or structural inefficiency does it exploit?
   - Write the expected value formula: EV = f(inputs)
   - What assumptions must hold for the edge to exist?
   - What would invalidate the edge?

3. IMPLEMENTATION REQUIREMENTS:
   - What new data sources are needed?
   - What code changes are required?
   - Can it run alongside the current strategy or is it a replacement?

4. RISK ANALYSIS:
   - What's the worst-case scenario?
   - What's the expected Sharpe ratio?
   - How correlated is it with our current strategy?

5. TEST PLAN:
   - How would we paper-test this?
   - What success metric defines "worth going live"?
   - How many trades/days needed for statistical significance?

CANDIDATE STRATEGIES TO EVALUATE (minimum — add your own):

A) PURE MARKET MAKING: Quote both YES and NO at the midpoint ± spread.
   Earn spread + rebates with zero directional risk. Does the math work
   given current spread levels and rebate rates?

B) CROSS-ASSET MOMENTUM: If BTC 5-min market resolves UP, does that predict
   ETH 5-min market direction? If BTC-ETH correlation > 0.7 in 5-min windows,
   we can trade ETH based on BTC's partially-revealed direction.

C) FLOW-INFORMED TRADING: Monitor orderbook buy/sell pressure imbalance.
   If large buy orders are hitting YES, the market is about to reprice UP.
   Can we detect this and front-run the flow?

D) MULTI-WINDOW MOMENTUM: If BTC has gone UP for N consecutive windows,
   is N+1 more likely UP (momentum) or DOWN (mean reversion)?
   Analyze from historical data.

E) TIME-DECAY SCALPING: In the final 30 seconds, uncertainty resolves rapidly.
   Prices converge to 0 or 1. Is there systematic mispricing in this zone
   that a maker can exploit?

F) COMPLEMENT ARBITRAGE: When YES_ask + NO_ask < $1.00, buy both for
   guaranteed profit. How often does this occur? At what size?

Respond with ONLY this JSON (no markdown fences):
{{
  "current_strategy_assessment": {{
    "long_term_viability": "STRONG" | "MODERATE" | "WEAK",
    "primary_risk": "<string>",
    "months_of_remaining_edge": "<estimate>"
  }},
  "proposed_strategies": [
    {{
      "name": "<string>",
      "description": "<string>",
      "edge_model": {{
        "formula": "<mathematical EV formula>",
        "information_asymmetry": "<what we know that the market doesn't>",
        "assumptions": ["<string>"],
        "invalidation_conditions": ["<string>"]
      }},
      "expected_performance": {{
        "win_rate": "<range>",
        "monthly_return_pct": "<range>",
        "sharpe_estimate": <float>,
        "correlation_with_current": <float>
      }},
      "implementation": {{
        "new_data_sources": ["<string>"],
        "code_changes": ["<string>"],
        "runs_alongside_current": <bool>,
        "complexity": "LOW" | "MEDIUM" | "HIGH"
      }},
      "risk": {{
        "worst_case": "<string>",
        "max_drawdown_pct": "<estimate>"
      }},
      "test_plan": {{
        "paper_test_duration_days": <int>,
        "min_trades_for_significance": <int>,
        "success_threshold": "<string>"
      }},
      "priority": "LOW" | "MEDIUM" | "HIGH",
      "reasoning": "<detailed mathematical justification>"
    }}
  ],
  "portfolio_recommendation": {{
    "optimal_strategy_mix": "<description of how strategies should combine>",
    "capital_allocation": "<how to split capital across strategies>",
    "sequencing": "<what to test first, second, third>"
  }},
  "reasoning": "<overall strategic analysis>",
  "severity": "INFO" | "MINOR" | "MAJOR" | "CRITICAL"
}}"""


def run():
    brain = BrainBase()
    mode = brain.get_bot_mode()
    logger.info(f"TIER 3: Strategy Discovery — mode={mode}")

    # Gather context
    trades = brain.get_settled_trades(limit=100, mode=mode)
    config = brain.get_config()

    perf = {}
    if trades:
        wins = sum(1 for t in trades if t.get("won"))
        total = sum(1 for t in trades if t.get("won") is not None)
        perf = {
            "settled": total, "wins": wins,
            "win_rate": round(wins / total, 3) if total else 0,
            "total_pnl": round(sum(t.get("pnl_usdc", 0) for t in trades if t.get("pnl_usdc")), 4),
            "avg_edge_bps": round(sum(t.get("edge_bps", 0) for t in trades) / len(trades)) if trades else 0,
            "avg_confidence": round(sum(t.get("oracle_confidence", 0) for t in trades) / len(trades), 3) if trades else 0,
        }

    # Get latest Tier 2 analysis if available
    tier2_summary = "No Tier 2 analysis available yet."
    try:
        resp = brain.sb.table("brain_reviews").select("analysis_json, created_at").eq("tier", 2).order("created_at", desc=True).limit(1).execute()
        if resp.data:
            tier2_summary = json.dumps(resp.data[0].get("analysis_json", {}), indent=1, default=str)
    except Exception:
        pass

    prompt = PROMPT.format(
        trade_count=len(trades),
        performance=json.dumps(perf, indent=1, default=str),
        tier2_summary=tier2_summary,
    )

    # Extended token limit for deep reasoning
    result = brain.call_claude(prompt, use_web_search=True, max_tokens=8192)
    if not result:
        return

    analysis = brain.parse_json(result["text"])
    if not analysis:
        return

    severity = analysis.get("severity", "INFO")

    # Tier 3 NEVER auto-applies — only proposes
    brain.save_review(3, "DISCOVERY", len(trades), analysis, tokens_used=result.get("tokens", 0))

    # Alert with high-priority strategies
    proposed = analysis.get("proposed_strategies", [])
    high = [s for s in proposed if s.get("priority") == "HIGH"]

    if high:
        names = ", ".join(s["name"] for s in high)
        brain.alert(f"Tier3: {len(high)} HIGH priority strategies proposed: {names}", "INFO")
    else:
        brain.alert(f"Tier3: {len(proposed)} strategies proposed | severity={severity}", "INFO")

    # Log viability assessment
    viability = analysis.get("current_strategy_assessment", {}).get("long_term_viability", "?")
    logger.info(f"Current strategy viability: {viability}")
    if viability == "WEAK":
        brain.alert("Tier3: Current strategy viability rated WEAK — review urgently", "WARN")


if __name__ == "__main__":
    run()
