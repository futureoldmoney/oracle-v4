"""
base.py — Shared Brain infrastructure: Claude API client, config management, alerting.

ALL tiers use claude-opus-4-6. No exceptions.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import httpx
from supabase import create_client, Client

logger = logging.getLogger("brain")

CLAUDE_API_URL = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL = "claude-opus-4-6"  # ALWAYS Opus. Never Sonnet.


class BrainBase:
    """Shared infrastructure for all Brain tiers."""

    def __init__(self):
        required = ["SUPABASE_URL", "SUPABASE_KEY", "ANTHROPIC_API_KEY"]
        missing = [k for k in required if k not in os.environ]
        if missing:
            raise RuntimeError(f"Brain requires env vars: {', '.join(missing)}")
        self.sb: Client = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
        self.api_key = os.environ["ANTHROPIC_API_KEY"]
        self._discord_url = os.environ.get("DISCORD_WEBHOOK_URL")
        self._http = httpx.Client(timeout=180.0)  # Opus can take longer

    def call_claude(self, prompt: str, use_web_search: bool = False, max_tokens: int = 4096) -> Optional[dict]:
        """Call Claude Opus 4.6. Returns {text, tokens} or None."""
        logger.info(f"Calling {CLAUDE_MODEL} (search={use_web_search})...")

        body = {
            "model": CLAUDE_MODEL,
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        }
        if use_web_search:
            body["tools"] = [{"type": "web_search_20250305", "name": "web_search"}]

        try:
            resp = self._http.post(CLAUDE_API_URL, headers={
                "Content-Type": "application/json",
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
            }, json=body)
            resp.raise_for_status()
            data = resp.json()
            text = "\n".join(b["text"] for b in data.get("content", []) if b.get("type") == "text")
            usage = data.get("usage", {})
            tokens = usage.get("input_tokens", 0) + usage.get("output_tokens", 0)
            logger.info(f"Opus: {usage.get('input_tokens', 0)} in / {usage.get('output_tokens', 0)} out")
            return {"text": text, "tokens": tokens}
        except Exception as e:
            logger.error(f"Claude API failed: {e}")
            self.alert(f"Brain API failed: {str(e)[:100]}", "ERROR")
            return None

    def parse_json(self, text: str) -> Optional[dict]:
        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[-1]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse failed: {e}\nText: {cleaned[:300]}")
            return None

    # ── Data Access ─────────────────────────────────────────

    def get_settled_trades(self, limit=50, mode="live"):
        table = "paper_trades" if mode == "paper" else "live_trades"
        cols = "created_at,side,oracle_confidence,edge_bps,implied_direction,won,pnl_usdc,seconds_remaining,ev_per_share"
        if mode == "live":
            cols += ",limit_price,fill_price,rebate_usdc"
        else:
            cols += ",hypothetical_price"
        try:
            resp = self.sb.table(table).select(cols).not_.is_("won", "null").order("created_at", desc=True).limit(limit).execute()
            return resp.data or []
        except Exception:
            return []

    def get_config(self) -> dict:
        try:
            resp = self.sb.table("config_v2").select("*").order("id", desc=True).limit(1).execute()
            return resp.data[0] if resp.data else {}
        except Exception:
            return {}

    def get_balance(self) -> float:
        try:
            resp = self.sb.table("heartbeats_v2").select("balance_usdc").order("created_at", desc=True).limit(1).execute()
            return float(resp.data[0]["balance_usdc"]) if resp.data else 0.0
        except Exception:
            return 0.0

    def get_bot_mode(self) -> str:
        try:
            resp = self.sb.table("bot_control").select("bot_mode").eq("id", 1).execute()
            return resp.data[0].get("bot_mode", "paper") if resp.data else "paper"
        except Exception:
            return "paper"

    def set_bot_mode(self, mode: str, reason: str = ""):
        """Switch bot mode between paper and live."""
        from datetime import datetime, timezone
        try:
            self.sb.table("bot_control").upsert({
                "id": 1, "bot_mode": mode,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }).execute()
        except Exception as e:
            logger.warning(f"Mode switch failed: {e}")

    def get_counterfactual_stats(self, mode: str = "paper") -> dict:
        """Get counterfactual analysis of skipped trades for Brain calibration."""
        table = "paper_trades" if mode == "paper" else "live_trades"
        try:
            resp = (self.sb.table(table)
                    .select("skip_reason, oracle_confidence, edge_bps, would_have_won, counterfactual_pnl")
                    .eq("decision", "SKIP")
                    .not_.is_("would_have_won", "null")
                    .order("created_at", desc=True)
                    .limit(200)
                    .execute())
            skips = resp.data or []
        except Exception:
            return {}

        if not skips:
            return {}

        total = len(skips)
        wins = sum(1 for s in skips if s.get("would_have_won"))
        total_pnl = sum(s.get("counterfactual_pnl", 0) for s in skips)

        # Break down by confidence bucket
        buckets = {}
        for s in skips:
            conf = float(s.get("oracle_confidence", 0) or 0)
            if conf < 0.50:
                bucket = "0.00-0.50"
            elif conf < 0.60:
                bucket = "0.50-0.60"
            elif conf < 0.70:
                bucket = "0.60-0.70"
            elif conf < 0.80:
                bucket = "0.70-0.80"
            else:
                bucket = "0.80-1.00"
            if bucket not in buckets:
                buckets[bucket] = {"total": 0, "wins": 0, "pnl": 0.0}
            buckets[bucket]["total"] += 1
            if s.get("would_have_won"):
                buckets[bucket]["wins"] += 1
            buckets[bucket]["pnl"] += s.get("counterfactual_pnl", 0)

        for b in buckets.values():
            b["win_rate"] = round(b["wins"] / b["total"], 3) if b["total"] > 0 else 0

        # Break down by skip reason
        by_reason = {}
        for s in skips:
            reason = s.get("skip_reason", "Unknown")
            # Normalize reason (remove variable values)
            if reason.startswith("Low confidence"):
                reason = "Low confidence"
            elif reason.startswith("Low edge"):
                reason = "Low edge"
            if reason not in by_reason:
                by_reason[reason] = {"total": 0, "wins": 0, "pnl": 0.0}
            by_reason[reason]["total"] += 1
            if s.get("would_have_won"):
                by_reason[reason]["wins"] += 1
            by_reason[reason]["pnl"] += s.get("counterfactual_pnl", 0)

        for r in by_reason.values():
            r["win_rate"] = round(r["wins"] / r["total"], 3) if r["total"] > 0 else 0

        return {
            "total_skips_settled": total,
            "would_have_won": wins,
            "would_have_lost": total - wins,
            "counterfactual_win_rate": round(wins / total, 3) if total > 0 else 0,
            "counterfactual_pnl": round(total_pnl, 4),
            "by_confidence_bucket": buckets,
            "by_skip_reason": by_reason,
        }

    # ── Config Management ───────────────────────────────────

    def validate_bounds(self, changes: dict) -> bool:
        bounds = {
            "min_edge_bps": (3, 500), "min_confidence": (0.40, 0.95),
            "quote_size_usdc": (0.50, 20.0), "skew_bps": (25, 3000),
            "kelly_fraction": (0.05, 1.0), "max_position_usdc": (2.0, 50.0),
            "min_seconds_before_close": (10, 120), "max_seconds_before_close": (60, 290),
        }
        for k, v in changes.items():
            if k in bounds and not (bounds[k][0] <= v <= bounds[k][1]):
                logger.warning(f"{k}={v} out of bounds {bounds[k]}")
                return False
        return True

    def insert_config(self, changes: dict, current: dict, reason: str, updated_by: str = "BRAIN") -> int:
        new = {k: changes.get(k, current.get(k, v)) for k, v in [
            ("strategy_type", "ORACLE_MAKER"), ("min_edge_bps", 150),
            ("min_confidence", 0.60), ("skew_bps", 100), ("quote_size_usdc", 2.0),
            ("max_position_usdc", 10.0), ("min_seconds_before_close", 30),
            ("max_seconds_before_close", 240), ("max_daily_trades", 100),
            ("max_daily_loss_usdc", 10.0), ("max_concurrent_orders", 1),
            ("consecutive_fail_limit", 5), ("pause_minutes", 15), ("kelly_fraction", 0.25),
        ]}
        new["updated_by"] = updated_by
        new["reason"] = reason[:500]
        if "confidence_calibration" in changes:
            new["confidence_calibration"] = json.dumps(changes["confidence_calibration"])
        try:
            resp = self.sb.table("config_v2").insert(new).execute()
            nid = resp.data[0]["id"] if resp.data else 0
            logger.info(f"New config v{nid} by {updated_by}")
            return nid
        except Exception as e:
            logger.error(f"Config insert failed: {e}")
            return 0

    def pause_bot(self, reason: str):
        try:
            self.sb.table("bot_control").upsert({
                "id": 1, "paused": True, "pause_reason": reason[:200],
                "paused_at": datetime.now(timezone.utc).isoformat(), "paused_by": "BRAIN",
            }).execute()
            logger.warning(f"BOT PAUSED: {reason}")
        except Exception as e:
            logger.error(f"Pause failed: {e}")

    def save_review(self, tier: int, rtype: str, count: int, analysis: dict,
                    applied=False, config_id=None, tokens=0):
        try:
            summary = analysis.get("summary", {})
            self.sb.table("brain_reviews").insert({
                "review_type": rtype, "tier": tier, "trade_count": count,
                "win_rate": summary.get("win_rate"),
                "total_pnl": summary.get("total_pnl_usdc"),
                "analysis_json": analysis,
                "config_changes_json": analysis.get("config_changes"),
                "severity": analysis.get("severity", "INFO"),
                "reasoning": analysis.get("reasoning", "")[:2000],
                "config_applied": applied, "new_config_id": config_id,
                "api_model": CLAUDE_MODEL, "api_tokens_used": tokens,
            }).execute()
        except Exception as e:
            logger.error(f"Review save failed: {e}")

    def alert(self, msg: str, level: str = "INFO"):
        emoji = {"INFO": "🧠", "WARN": "⚠️", "ERROR": "🔴"}
        full = f"{emoji.get(level, '📌')} Brain | {msg}"

        if self._discord_url:
            try:
                self._http.post(self._discord_url, json={"content": full})
            except Exception:
                pass

        logger.info(f"[{level}] {msg}")

    # ── Self-Audit Queries ──────────────────────────────────

    def get_trades_for_audit(self, limit: int = 200, mode: str = "paper") -> list:
        """Get recent trades with ALL fields for structural integrity audit."""
        table = "paper_trades" if mode == "paper" else "live_trades"
        decision = "PAPER" if mode == "paper" else "TRADE"
        try:
            resp = (self.sb.table(table)
                    .select("*")
                    .eq("decision", decision)
                    .order("created_at", desc=True)
                    .limit(limit)
                    .execute())
            return resp.data or []
        except Exception:
            return []

    def get_skips_for_audit(self, limit: int = 200, mode: str = "paper") -> list:
        """Get recent skips with counterfactual outcomes for audit."""
        table = "paper_trades" if mode == "paper" else "live_trades"
        try:
            resp = (self.sb.table(table)
                    .select("*")
                    .eq("decision", "SKIP")
                    .order("created_at", desc=True)
                    .limit(limit)
                    .execute())
            return resp.data or []
        except Exception:
            return []

    def get_recent_heartbeats(self, limit: int = 20) -> list:
        """Get recent heartbeats for health monitoring."""
        try:
            resp = (self.sb.table("heartbeats_v2")
                    .select("*")
                    .order("created_at", desc=True)
                    .limit(limit)
                    .execute())
            return resp.data or []
        except Exception:
            return []
