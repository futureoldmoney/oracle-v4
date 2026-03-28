"""
discord_bot.py — Two-way Discord command interface (v4).

Runs as a separate Railway service. Connects to Discord via bot token
and listens for commands in a designated channel.

v4 CHANGES:
  - Edge Reports filter to oracle trades only (price < $0.60)
  - !status shows sentiment bias, Deribit PCR, Coinbase spot, feed health
  - !trades shows fair_value, fill_price, edge%, execution_mode
  - !pnl breaks down by direction (UP/YES vs DOWN/NO)
  - !config includes v4 params (position %, taker fees, min edge)
  - !set supports new v4 parameters
  - !signals — new command showing live signal status
  - !edge — new command showing edge statistics
  - Startup message says v4

Commands (use ! prefix):
  !status     — Balance, win rate, sentiment, feed health
  !pause      — Pause the bot (with optional reason)
  !resume     — Resume the bot
  !mode       — Show or switch mode (paper/live)
  !config     — Show current config
  !set        — Change a config parameter
  !trades     — Recent trade summary (v4 fields)
  !pnl        — Today's P&L breakdown by direction
  !signals    — Live signal status (oracle, PCR, sentiment, LTP)
  !edge       — Edge statistics from recent trades
  !skips      — Recent skip reasons
  !skips100   — Last 100 skips with summary
  !brain      — Trigger a Brain tier on demand
  !redeem     — Redeem resolved positions
  !kill       — Emergency: pause + cancel all orders
  !help       — Show all commands

Setup:
  1. Create a bot at discord.com/developers/applications
  2. Get the bot token → DISCORD_BOT_TOKEN env var
  3. Invite to your server with Messages permission
  4. Get channel ID → DISCORD_CHANNEL_ID env var

Run: python -m engine.discord_bot
"""

import json
import logging
import os
import sys
import time
import threading
from datetime import datetime, timezone

import httpx
from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-10s | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("discord")

DISCORD_API = "https://discord.com/api/v10"
GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"


class DiscordController:
    """
    Lightweight Discord bot using raw HTTP + Gateway websocket.
    No discord.py dependency needed — keeps requirements minimal.
    """

    def __init__(self):
        self._token = os.environ["DISCORD_BOT_TOKEN"]
        self._channel_id = os.environ["DISCORD_CHANNEL_ID"]
        self._http = httpx.Client(timeout=30.0, headers={
            "Authorization": f"Bot {self._token}",
            "Content-Type": "application/json",
        })
        self.sb: Client = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
        self._webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")

    def run(self):
        """Poll for messages using REST API (simpler than websocket gateway)."""
        logger.info("Discord controller started")
        self._send("🤖 **Oracle Bot v4** — Discord controller online. Type `!help` for commands.")

        last_message_id = self._get_latest_message_id()

        while True:
            try:
                messages = self._get_new_messages(last_message_id)
                for msg in reversed(messages):  # Process oldest first
                    last_message_id = msg["id"]
                    self._handle_message(msg)
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Poll error: {e}")
                time.sleep(5)

            time.sleep(2)

    def _get_latest_message_id(self) -> str:
        try:
            resp = self._http.get(f"{DISCORD_API}/channels/{self._channel_id}/messages?limit=1")
            if resp.status_code == 200:
                msgs = resp.json()
                if msgs:
                    return msgs[0]["id"]
        except Exception:
            pass
        return "0"

    def _get_new_messages(self, after_id: str) -> list:
        try:
            resp = self._http.get(
                f"{DISCORD_API}/channels/{self._channel_id}/messages",
                params={"after": after_id, "limit": 10},
            )
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        return []

    def _handle_message(self, msg: dict):
        if msg.get("author", {}).get("bot", False):
            return

        content = (msg.get("content") or "").strip()
        if not content.startswith("!"):
            return

        parts = content.split(maxsplit=1)
        cmd = parts[0].lower()
        args = parts[1] if len(parts) > 1 else ""

        handlers = {
            "!status": self._cmd_status,
            "!pause": self._cmd_pause,
            "!resume": self._cmd_resume,
            "!mode": self._cmd_mode,
            "!config": self._cmd_config,
            "!set": self._cmd_set,
            "!trades": self._cmd_trades,
            "!skips": self._cmd_skips,
            "!skips100": self._cmd_skips100,
            "!pnl": self._cmd_pnl,
            "!signals": self._cmd_signals,
            "!edge": self._cmd_edge,
            "!brain": self._cmd_brain,
            "!redeem": self._cmd_redeem,
            "!kill": self._cmd_kill,
            "!help": self._cmd_help,
        }

        handler = handlers.get(cmd)
        if handler:
            try:
                handler(args)
            except Exception as e:
                self._send(f"❌ Command failed: {str(e)[:200]}")
        else:
            self._send(f"Unknown command: `{cmd}`\nType `!help` for available commands.")

    # ── Commands ────────────────────────────────────────────

    def _cmd_status(self, args: str):
        ctrl = self._query_one("bot_control", "id, paused, bot_mode, pause_reason")
        mode = ctrl.get("bot_mode", "?") if ctrl else "?"
        paused = ctrl.get("paused", False) if ctrl else False
        pause_reason = ctrl.get("pause_reason", "") if ctrl else ""

        hb = self._query_latest("heartbeats_v2",
            "balance_usdc, active_orders, pending_settlements, trades_today, pnl_today, status, created_at")

        # Win rate — oracle trades only (price < 0.60)
        mode_table = "paper_trades" if mode == "paper" else "live_trades"
        try:
            resp = (self.sb.table(mode_table)
                    .select("won, pnl_usdc, implied_direction, simulated_fill_price, edge_at_fill")
                    .not_.is_("won", "null")
                    .lt("hypothetical_price", 0.60)
                    .order("created_at", desc=True)
                    .limit(50)
                    .execute())
            trades = resp.data or []
            wins = sum(1 for t in trades if t.get("won"))
            total = len(trades)
            win_rate = f"{wins}/{total} ({wins/total*100:.0f}%)" if total > 0 else "No oracle trades"
            # Direction breakdown
            up_trades = [t for t in trades if t.get("implied_direction") == "UP"]
            down_trades = [t for t in trades if t.get("implied_direction") == "DOWN"]
            up_wins = sum(1 for t in up_trades if t.get("won"))
            down_wins = sum(1 for t in down_trades if t.get("won"))
            # Average edge
            edges = [float(t.get("edge_at_fill", 0) or 0) for t in trades if t.get("edge_at_fill")]
            avg_edge = sum(edges) / len(edges) if edges else 0
        except Exception:
            win_rate = "Error"
            up_trades = down_trades = []
            up_wins = down_wins = 0
            avg_edge = 0

        status_emoji = "🟢" if not paused else "🔴"
        mode_emoji = "📝" if mode == "paper" else "💰"

        lines = [
            f"{status_emoji} **Status:** {'PAUSED — ' + pause_reason if paused else 'RUNNING'}",
            f"{mode_emoji} **Mode:** {mode.upper()}",
        ]

        if hb:
            lines.extend([
                f"💵 **Balance:** ${hb.get('balance_usdc', 0):.2f} USDC",
                f"📊 **Win Rate (oracle):** {win_rate}",
            ])
            if up_trades:
                lines.append(f"   ↑ UP/YES: {up_wins}/{len(up_trades)}")
            if down_trades:
                lines.append(f"   ↓ DOWN/NO: {down_wins}/{len(down_trades)}")
            if avg_edge > 0:
                lines.append(f"📐 **Avg Edge:** {avg_edge:.1f}%")
            lines.extend([
                f"📈 **Today:** {hb.get('trades_today', 0)} trades | ${hb.get('pnl_today', 0):+.4f} P&L",
                f"📋 **Active Orders:** {hb.get('active_orders', 0)}",
                f"⏳ **Pending Settlement:** {hb.get('pending_settlements', 0)}",
            ])

            # Sentiment snapshot from latest trade
            try:
                latest = (self.sb.table(mode_table)
                         .select("sentiment_bias, fear_greed_index, deribit_pcr, coinbase_price, btc_1d_change_pct")
                         .not_.is_("sentiment_bias", "null")
                         .order("created_at", desc=True)
                         .limit(1)
                         .execute())
                if latest.data:
                    lt = latest.data[0]
                    sentiment_parts = []
                    if lt.get("sentiment_bias"):
                        sentiment_parts.append(f"Bias: {lt['sentiment_bias']}")
                    if lt.get("fear_greed_index"):
                        sentiment_parts.append(f"F&G: {lt['fear_greed_index']}")
                    if lt.get("btc_1d_change_pct"):
                        sentiment_parts.append(f"BTC 24h: {lt['btc_1d_change_pct']:+.1f}%")
                    if lt.get("deribit_pcr"):
                        sentiment_parts.append(f"PCR adj: {lt['deribit_pcr']:+.3f}")
                    if sentiment_parts:
                        lines.append(f"🌡️ **Sentiment:** {' | '.join(sentiment_parts)}")
            except Exception:
                pass

            # Redeemable positions
            try:
                funder = os.environ.get("POLYMARKET_FUNDER_ADDRESS", "")
                if funder:
                    resp = self._http.get(f"https://data-api.polymarket.com/positions?user={funder}", timeout=10)
                    if resp.status_code == 200:
                        positions = resp.json() or []
                        redeemable = [p for p in positions if p.get("redeemable")]
                        if redeemable:
                            total_value = sum(float(p.get("currentValue", 0) or 0) for p in redeemable)
                            lines.append(f"🎁 **Redeemable:** {len(redeemable)} positions (~${total_value:.2f}) — `!redeem`")
            except Exception:
                pass

            lines.append(f"🕐 **Last Heartbeat:** {self._fmt_time(hb.get('created_at', ''))}")
        else:
            lines.append("⚠️ No heartbeat data — engine may not be running")

        self._send("\n".join(lines))

    def _cmd_trades(self, args: str):
        mode = self._get_mode()
        trade_table = "paper_trades" if mode == "paper" else "live_trades"

        try:
            resp = (self.sb.table(trade_table)
                    .select("created_at, side, implied_direction, won, pnl_usdc, "
                            "simulated_fill_price, fair_value_at_trade, edge_at_fill, "
                            "execution_mode, seconds_remaining, chainlink_move_pct, "
                            "size_pct, taker_fee_estimate, decision")
                    .not_.is_("won", "null")
                    .lt("hypothetical_price", 0.60)
                    .order("created_at", desc=True)
                    .limit(10)
                    .execute())
            trades = resp.data or []
        except Exception:
            self._send("❌ Could not fetch trades")
            return

        if not trades:
            self._send(f"No settled oracle {mode} trades yet.")
            return

        lines = [f"📋 **Last 10 oracle {mode} trades:**\n"]
        for t in trades:
            won = t.get("won")
            icon = "✅" if won is True else "❌"
            pnl = t.get("pnl_usdc")
            pnl_str = f"${pnl:+.2f}" if pnl is not None else "?"

            fill = t.get("simulated_fill_price")
            fv = t.get("fair_value_at_trade")
            edge = t.get("edge_at_fill")
            exec_mode = t.get("execution_mode", "?")
            cl_move = t.get("chainlink_move_pct")
            secs = t.get("seconds_remaining")
            size_pct = t.get("size_pct")

            detail = f"{icon} **{t.get('side', '?')} {t.get('implied_direction', '')}**"
            if fill:
                detail += f" fill=${fill:.3f}"
            if fv:
                detail += f" FV={fv:.3f}"
            if edge:
                detail += f" edge={edge:.1f}%"
            detail += f" | {pnl_str}"
            if size_pct:
                detail += f" ({size_pct:.1%})"
            if exec_mode and exec_mode != "?":
                detail += f" [{exec_mode}]"
            if cl_move:
                detail += f" | CL={cl_move:+.3f}%"
            if secs:
                detail += f" {secs}s"
            detail += f" | {self._fmt_time(t.get('created_at', ''))}"

            lines.append(detail)

        self._send("\n".join(lines))

    def _cmd_pnl(self, args: str):
        mode = self._get_mode()
        trade_table = "paper_trades" if mode == "paper" else "live_trades"
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()

        try:
            resp = (self.sb.table(trade_table)
                    .select("won, pnl_usdc, implied_direction, side, "
                            "simulated_fill_price, taker_fee_estimate")
                    .not_.is_("won", "null")
                    .lt("hypothetical_price", 0.60)
                    .gte("created_at", today)
                    .execute())
            trades = resp.data or []
        except Exception:
            self._send("❌ Could not fetch P&L data")
            return

        if not trades:
            self._send(f"No settled oracle {mode} trades today.")
            return

        wins = sum(1 for t in trades if t.get("won"))
        losses = len(trades) - wins
        total_pnl = sum(t.get("pnl_usdc", 0) or 0 for t in trades)
        total_fees = sum(t.get("taker_fee_estimate", 0) or 0 for t in trades)

        # Direction breakdown
        up_trades = [t for t in trades if t.get("implied_direction") == "UP"]
        down_trades = [t for t in trades if t.get("implied_direction") == "DOWN"]
        up_pnl = sum(t.get("pnl_usdc", 0) or 0 for t in up_trades)
        down_pnl = sum(t.get("pnl_usdc", 0) or 0 for t in down_trades)
        up_wins = sum(1 for t in up_trades if t.get("won"))
        down_wins = sum(1 for t in down_trades if t.get("won"))

        lines = [
            f"📈 **Today's P&L ({mode.upper()}) — oracle trades only:**\n",
            f"**Total: ${total_pnl:+.2f}** (fees: -${total_fees:.2f})",
            f"Trades: {len(trades)} ({wins}W / {losses}L) | WR: {wins/len(trades)*100:.0f}%",
        ]
        if up_trades:
            up_wr = f"{up_wins/len(up_trades)*100:.0f}%" if up_trades else "N/A"
            lines.append(f"  ↑ YES/UP: {len(up_trades)}t | {up_wins}W | ${up_pnl:+.2f} | WR {up_wr}")
        if down_trades:
            dn_wr = f"{down_wins/len(down_trades)*100:.0f}%" if down_trades else "N/A"
            lines.append(f"  ↓ NO/DOWN: {len(down_trades)}t | {down_wins}W | ${down_pnl:+.2f} | WR {dn_wr}")

        # Average fill prices
        up_fills = [float(t.get("simulated_fill_price", 0) or 0) for t in up_trades if t.get("simulated_fill_price")]
        down_fills = [float(t.get("simulated_fill_price", 0) or 0) for t in down_trades if t.get("simulated_fill_price")]
        if up_fills:
            lines.append(f"  ↑ Avg YES fill: ${sum(up_fills)/len(up_fills):.3f}")
        if down_fills:
            lines.append(f"  ↓ Avg NO fill: ${sum(down_fills)/len(down_fills):.3f}")

        self._send("\n".join(lines))

    def _cmd_signals(self, args: str):
        """Show live signal status — what each data source is reporting right now."""
        mode = self._get_mode()
        table = "paper_trades" if mode == "paper" else "live_trades"

        # Get latest trade with v4 fields
        try:
            resp = (self.sb.table(table)
                    .select("created_at, chainlink_move_pct, simulated_fill_price, "
                            "fair_value_at_trade, edge_at_fill, coinbase_price, "
                            "deribit_pcr, ltp_velocity_30s, sentiment_bias, "
                            "sentiment_adjustment, fear_greed_index, btc_1d_change_pct, btc_1h_change_pct, "
                            "seconds_remaining")
                    .order("created_at", desc=True)
                    .limit(1)
                    .execute())
            latest = resp.data[0] if resp.data else None
        except Exception:
            latest = None

        if not latest:
            self._send("No recent signal data. The bot may not have evaluated a trade yet.")
            return

        lines = [f"📡 **Signal Status** (from latest evaluation)\n"]

        # Oracle
        cl = latest.get("chainlink_move_pct")
        if cl is not None:
            dir_str = "UP ↑" if cl > 0 else "DOWN ↓" if cl < 0 else "FLAT"
            lines.append(f"🔮 **Oracle:** CL move {cl:+.4f}% → {dir_str}")
        else:
            lines.append("🔮 **Oracle:** No Chainlink data")

        # Fair value + edge
        fv = latest.get("fair_value_at_trade")
        edge = latest.get("edge_at_fill")
        fill = latest.get("simulated_fill_price")
        if fv:
            lines.append(f"📐 **Fair Value:** {fv:.3f} | Fill: ${fill:.3f} | Edge: {edge:.1f}%" if fill and edge else f"📐 **Fair Value:** {fv:.3f}")

        # Coinbase
        cb = latest.get("coinbase_price")
        if cb:
            lines.append(f"💱 **Coinbase:** ${cb:,.2f}")

        # Deribit PCR
        pcr = latest.get("deribit_pcr")
        if pcr is not None:
            lines.append(f"📊 **Deribit PCR adj:** {pcr:+.3f}")

        # LTP velocity
        vel = latest.get("ltp_velocity_30s")
        if vel is not None:
            speed = "FAST ⚡" if abs(vel) > 0.02 else "SLOW 🐌" if abs(vel) < 0.005 else "MODERATE"
            lines.append(f"⚡ **LTP Velocity 30s:** {vel:+.3f} ({speed})")

        # Sentiment
        bias = latest.get("sentiment_bias")
        adj = latest.get("sentiment_adjustment")
        fng = latest.get("fear_greed_index")
        d1 = latest.get("btc_1d_change_pct")
        h1 = latest.get("btc_1h_change_pct")

        if bias is not None or fng is not None:
            sent_parts = []
            if bias is not None:
                sent_parts.append(f"Bias: {bias:+.3f}")
            if fng is not None:
                sent_parts.append(f"F&G: {fng}")
            if d1 is not None:
                sent_parts.append(f"BTC 24h: {d1:+.1f}%")
            if h1 is not None:
                sent_parts.append(f"1h: {h1:+.1f}%")
            lines.append(f"🌡️ **Sentiment:** {' | '.join(sent_parts)}")

        # Timing
        secs = latest.get("seconds_remaining")
        if secs is not None:
            lines.append(f"⏱ **Timing:** {secs}s remaining ({(1-secs/300)*100:.0f}% through window)")

        lines.append(f"\n📅 Data from: {self._fmt_time(latest.get('created_at', ''))}")
        self._send("\n".join(lines))

    def _cmd_edge(self, args: str):
        """Show edge statistics from recent oracle trades."""
        mode = self._get_mode()
        table = "paper_settled" if mode == "paper" else "live_settled"

        try:
            resp = (self.sb.table(table)
                    .select("won, pnl_usdc, edge_at_fill, fair_value_at_trade, "
                            "simulated_fill_price, implied_direction, chainlink_move_pct, "
                            "seconds_remaining")
                    .not_.is_("won", "null")
                    .not_.is_("edge_at_fill", "null")
                    .order("created_at", desc=True)
                    .limit(100)
                    .execute())
            trades = resp.data or []
        except Exception:
            self._send("❌ Could not fetch edge data")
            return

        if len(trades) < 3:
            self._send(f"Need at least 3 settled oracle trades for edge stats. Have {len(trades)}.")
            return

        wins = sum(1 for t in trades if t.get("won"))
        total = len(trades)
        total_pnl = sum(t.get("pnl_usdc", 0) or 0 for t in trades)
        edges = [float(t.get("edge_at_fill", 0) or 0) for t in trades]
        fvs = [float(t.get("fair_value_at_trade", 0) or 0) for t in trades if t.get("fair_value_at_trade")]
        fills = [float(t.get("simulated_fill_price", 0) or 0) for t in trades if t.get("simulated_fill_price")]
        timings = [int(t.get("seconds_remaining", 0) or 0) for t in trades if t.get("seconds_remaining")]
        magnitudes = [abs(float(t.get("chainlink_move_pct", 0) or 0)) for t in trades if t.get("chainlink_move_pct")]

        avg_edge = sum(edges) / len(edges) if edges else 0
        avg_fv = sum(fvs) / len(fvs) if fvs else 0
        avg_fill = sum(fills) / len(fills) if fills else 0
        avg_timing = sum(timings) / len(timings) if timings else 0
        avg_mag = sum(magnitudes) / len(magnitudes) if magnitudes else 0

        lines = [
            f"📊 **Edge Statistics** ({total} oracle trades, {mode})\n",
            f"**Win Rate:** {wins}/{total} ({wins/total*100:.1f}%)",
            f"**Total P&L:** ${total_pnl:+.2f}",
            f"**Avg Edge at Fill:** {avg_edge:.1f}%",
            f"**Avg Fair Value:** {avg_fv:.3f}",
            f"**Avg Fill Price:** ${avg_fill:.3f}",
            f"**Avg CL Magnitude:** {avg_mag:.4f}%",
            f"**Avg Timing:** {avg_timing:.0f}s remaining",
        ]

        # Edge distribution
        high_edge = [e for e in edges if e >= 20]
        mid_edge = [e for e in edges if 5 <= e < 20]
        low_edge = [e for e in edges if 3 <= e < 5]
        lines.append(f"\n**Edge Distribution:**")
        lines.append(f"  >20% edge: {len(high_edge)} trades")
        lines.append(f"  5-20% edge: {len(mid_edge)} trades")
        lines.append(f"  3-5% edge: {len(low_edge)} trades")

        self._send("\n".join(lines))

    def _cmd_pause(self, args: str):
        reason = args.strip() or "Paused via Discord"
        try:
            self.sb.table("bot_control").upsert({
                "id": 1, "paused": True, "pause_reason": reason,
                "paused_at": datetime.now(timezone.utc).isoformat(),
                "paused_by": "HUMAN",
            }).execute()
            self._send(f"⏸ Bot **PAUSED**\nReason: {reason}")
        except Exception as e:
            self._send(f"❌ Failed to pause: {e}")

    def _cmd_resume(self, args: str):
        try:
            self.sb.table("bot_control").upsert({
                "id": 1, "paused": False, "pause_reason": None,
                "paused_at": None, "paused_by": None,
            }).execute()
            self._send("▶️ Bot **RESUMED**")
        except Exception as e:
            self._send(f"❌ Failed to resume: {e}")

    def _cmd_mode(self, args: str):
        new_mode = args.strip().lower()
        if not new_mode:
            ctrl = self._query_one("bot_control", "bot_mode")
            current = ctrl.get("bot_mode", "?") if ctrl else "?"
            self._send(f"Current mode: **{current.upper()}**\n\nTo switch: `!mode paper` or `!mode live confirm`")
            return
        if new_mode == "paper":
            self._set_mode("paper")
        elif new_mode == "live confirm":
            self._set_mode("live")
        elif new_mode == "live":
            self._send(
                "⚠️ **Switching to LIVE mode.**\n"
                "This will place REAL orders with REAL money.\n\n"
                "Type `!mode live confirm` to proceed."
            )
        else:
            self._send("❌ Invalid mode. Use: `!mode paper` or `!mode live confirm`")

    def _cmd_set(self, args: str):
        parts = args.strip().split()
        if len(parts) < 2:
            self._send(
                "**Usage:** `!set <parameter> <value>`\n\n"
                "**Sizing (v4):**\n"
                "`!set min_position_pct 0.01` — 1% floor\n"
                "`!set max_position_pct 0.03` — 3% ceiling\n"
                "`!set fractional_kelly 0.20`\n"
                "`!set min_edge_pct 3.0` — min edge % to trade\n"
                "`!set taker_fee_rate 0.0156`\n\n"
                "**Signal Weights:**\n"
                "`!set weight_oracle_confidence 3.0`\n"
                "`!set weight_liquidity_vacuum 0.3`\n"
                "`!set weight_ob_imbalance 0.3`\n\n"
                "**Risk:**\n"
                "`!set max_daily_loss_pct 0.10`\n\n"
                "Type `!config` to see all current values."
            )
            return

        key = parts[0]
        try:
            value = float(parts[1])
            if key in ("max_concurrent_positions", "cooldown_after_loss_seconds", "regime_window_size"):
                value = int(value)
        except ValueError:
            self._send(f"❌ Invalid value: `{parts[1]}`")
            return

        bounds = {
            "bankroll": (1, 100000),
            "fractional_kelly": (0.05, 1.0),
            "min_aggregate_confidence": (0.10, 0.95),
            "min_edge_bps": (5, 1000),
            "min_position_pct": (0.005, 0.10),
            "max_position_pct": (0.01, 0.10),
            "min_bet_usd": (0.25, 100),
            "min_edge_pct": (1.0, 20.0),
            "taker_fee_rate": (0.0, 0.05),
            "min_order_usd": (0.5, 50),
            "weight_oracle_confidence": (0, 10),
            "weight_stale_quote": (0, 10),
            "weight_liquidity_vacuum": (0, 10),
            "weight_complement_arb": (0, 10),
            "weight_whale_flow": (0, 10),
            "weight_ob_imbalance": (0, 10),
            "min_conf_oracle_confidence": (0.10, 0.95),
            "min_conf_stale_quote": (0.10, 0.95),
            "min_conf_liquidity_vacuum": (0.10, 0.95),
            "min_conf_complement_arb": (0.10, 0.95),
            "min_conf_whale_flow": (0.10, 0.95),
            "min_conf_ob_imbalance": (0.10, 0.95),
            "stale_move_threshold_pct": (0.01, 1.0),
            "staleness_window_ms": (100, 5000),
            "confidence_alpha": (0.1, 20),
            "confidence_beta": (0.1, 20),
            "confidence_gamma": (0.1, 20),
            "vacuum_threshold": (0.1, 1.0),
            "vacuum_stability_threshold": (0.1, 1.0),
            "arb_min_profit_bps": (5, 500),
            "whale_min_score": (0.1, 1.0),
            "ob_bullish_threshold": (1.0, 3.0),
            "ob_bearish_threshold": (0.1, 1.0),
            "max_concurrent_positions": (1, 10),
            "max_daily_loss_pct": (0.05, 0.50),
            "max_drawdown_pct": (0.05, 0.50),
            "cooldown_after_loss_seconds": (0, 300),
            "regime_window_size": (60, 1800),
            "stale_lookback_seconds": (1, 30),
            "arb_target_size": (1, 50),
            "whale_size_multiplier": (1, 10),
            "cycle_interval_seconds": (0.5, 10),
            "market_scan_interval": (5, 60),
            "data_api_poll_interval": (2, 30),
        }

        if key not in bounds:
            self._send(f"❌ Unknown parameter: `{key}`\nType `!set` for available parameters.")
            return

        lo, hi = bounds[key]
        if not (lo <= value <= hi):
            self._send(f"❌ `{key}={value}` out of bounds [{lo}, {hi}]")
            return

        try:
            self.sb.table("bot_config").update({
                key: value,
                "updated_by": "HUMAN",
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }).eq("id", 1).execute()
            self._send(f"✅ Config updated (hot-reloads in ~60s):\n**{key}** = {value}")
        except Exception as e:
            self._send(f"❌ Config update failed: {e}")

    def _cmd_config(self, args: str):
        try:
            resp = self.sb.table("bot_config").select("*").eq("id", 1).execute()
            cfg = resp.data[0] if resp.data else {}
        except Exception:
            self._send("❌ Could not fetch config")
            return

        sections = {
            "Sizing (v4)": [
                "bankroll", "fractional_kelly", "min_position_pct",
                "max_position_pct", "min_edge_pct", "taker_fee_rate",
                "min_order_usd", "min_aggregate_confidence", "min_edge_bps",
            ],
            "Signal Weights": [
                "weight_oracle_confidence", "weight_stale_quote",
                "weight_liquidity_vacuum", "weight_complement_arb",
                "weight_whale_flow", "weight_ob_imbalance",
            ],
            "Risk Limits": [
                "max_concurrent_positions", "max_daily_loss_pct",
                "max_drawdown_pct", "cooldown_after_loss_seconds",
            ],
            "Timing": [
                "cycle_interval_seconds", "market_scan_interval",
                "data_api_poll_interval",
            ],
        }

        lines = [f"⚙️ **v4 Config** (by {cfg.get('updated_by', 'SYSTEM')})"]
        for section, keys in sections.items():
            lines.append(f"\n**{section}:**")
            for k in keys:
                val = cfg.get(k, "?")
                lines.append(f"  `{k}`: **{val}**")

        self._send("\n".join(lines))

    def _cmd_skips(self, args: str):
        mode = self._get_mode()
        table = "paper_trades" if mode == "paper" else "live_trades"

        try:
            resp = (self.sb.table(table)
                    .select("created_at, skip_reason, oracle_confidence, edge_bps, would_have_won, counterfactual_pnl")
                    .eq("decision", "SKIP")
                    .order("created_at", desc=True)
                    .limit(10)
                    .execute())
            skips = resp.data or []
        except Exception:
            self._send("❌ Could not fetch skip data")
            return

        if not skips:
            self._send("No skips recorded yet.")
            return

        lines = [f"⏭ **Last 10 skips ({mode}):**\n"]
        for s in skips:
            reason = s.get("skip_reason", "Unknown")
            whw = s.get("would_have_won")
            icon = "🟢" if whw is True else ("🔴" if whw is False else "⏭")
            detail = ""
            conf = s.get("oracle_confidence")
            if conf is not None:
                detail += f" | conf={conf:.2f}"
            cfpnl = s.get("counterfactual_pnl")
            if cfpnl is not None:
                detail += f" | cf=${cfpnl:+.2f}"
            lines.append(f"{icon} {reason}{detail} | {self._fmt_time(s.get('created_at', ''))}")

        self._send("\n".join(lines))

    def _cmd_skips100(self, args: str):
        mode = self._get_mode()
        table = "paper_trades" if mode == "paper" else "live_trades"

        try:
            resp = (self.sb.table(table)
                    .select("created_at, skip_reason, would_have_won, counterfactual_pnl")
                    .eq("decision", "SKIP")
                    .order("created_at", desc=True)
                    .limit(100)
                    .execute())
            skips = resp.data or []
        except Exception:
            self._send("❌ Could not fetch skip data")
            return

        if not skips:
            self._send("No skips recorded yet.")
            return

        reason_counts = {}
        for s in skips:
            r = s.get("skip_reason", "Unknown")
            reason_counts[r] = reason_counts.get(r, 0) + 1

        settled = [s for s in skips if s.get("would_have_won") is not None]
        whw_count = sum(1 for s in settled if s.get("would_have_won"))
        total_cf = sum(s.get("counterfactual_pnl", 0) or 0 for s in settled)

        lines = [f"⏭ **Last {len(skips)} skips ({mode}):**\n"]
        lines.append("**Skip Reasons:**")
        for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
            lines.append(f"  {reason}: **{count}x**")

        if settled:
            cf_wr = whw_count / len(settled) * 100
            lines.append(f"\n**Counterfactual:**")
            lines.append(f"  🟢 Would have won: {whw_count} ({cf_wr:.0f}%)")
            lines.append(f"  🔴 Would have lost: {len(settled)-whw_count} ({100-cf_wr:.0f}%)")
            lines.append(f"  💰 Hypothetical P&L: ${total_cf:+.2f}")
            if total_cf > 0:
                lines.append("  ⚠️ Leaving money on the table")
            else:
                lines.append("  ✅ Good skips — thresholds protecting you")

        self._send("\n".join(lines))

    def _cmd_brain(self, args: str):
        tier = args.strip()
        if tier not in ("1", "2", "3"):
            self._send(
                "**Usage:** `!brain <tier>`\n\n"
                "`!brain 1` — Market intelligence (web search)\n"
                "`!brain 2` — Quant analysis (needs 20+ trades)\n"
                "`!brain 3` — Strategy discovery"
            )
            return

        self._send(f"🧠 Triggering Brain Tier {tier}... (this may take 1-2 minutes)")
        try:
            if tier == "1":
                from brain.tier1_intelligence import run as run_t1
                run_t1()
            elif tier == "2":
                from brain.tier2_quant import run as run_t2
                run_t2()
            elif tier == "3":
                from brain.tier3_discovery import run as run_t3
                run_t3()
            self._send(f"✅ Brain Tier {tier} completed. Check `!status` for any config changes.")
        except Exception as e:
            self._send(f"❌ Brain Tier {tier} failed: {str(e)[:200]}")

    def _cmd_kill(self, args: str):
        try:
            self.sb.table("bot_control").upsert({
                "id": 1, "paused": True,
                "pause_reason": "EMERGENCY KILL via Discord",
                "paused_at": datetime.now(timezone.utc).isoformat(),
                "paused_by": "HUMAN",
            }).execute()
            self._send(
                "🚨 **EMERGENCY KILL ACTIVATED**\n\n"
                "Bot is paused. Active orders will be cancelled on next Engine cycle.\n"
                "Type `!resume` when ready to restart."
            )
        except Exception as e:
            self._send(f"❌ Kill failed: {e}")

    def _cmd_redeem(self, args: str):
        self._send("🔄 Redeeming resolved positions...")
        try:
            from engine.redeem import main as redeem_main
            import io
            from contextlib import redirect_stdout
            f = io.StringIO()
            with redirect_stdout(f):
                redeem_main()
            output = f.getvalue()
            lines = output.strip().split("\n")
            summary = [l for l in lines if any(k in l for k in
                       ["USDC before", "USDC after", "Gained", "Redeemed", "Redeeming", "SKIP", "[OK]", "[FAILED]", "Found"])]
            if summary:
                self._send("💰 **Redeem Results:**\n```\n" + "\n".join(summary[-15:]) + "\n```")
            else:
                self._send(f"💰 Redeem complete:\n```\n{output[-800:]}\n```")
        except Exception as e:
            self._send(f"❌ Redeem error: {str(e)[:300]}")

    def _cmd_help(self, args: str):
        self._send(
            "🤖 **Oracle Bot v4 — Commands**\n\n"
            "📊 **Monitoring**\n"
            "`!status` — Balance, win rate, sentiment, feed health\n"
            "`!trades` — Last 10 oracle trades (fill price, edge, P&L)\n"
            "`!pnl` — Today's P&L by direction (UP vs DOWN)\n"
            "`!signals` — Live signal status (oracle, PCR, sentiment, LTP)\n"
            "`!edge` — Edge statistics from recent trades\n"
            "`!skips` — Last 10 skipped trades\n"
            "`!skips100` — Last 100 skips with breakdown\n"
            "`!config` — Current config (v4 params)\n\n"
            "🎮 **Control**\n"
            "`!pause [reason]` — Pause trading\n"
            "`!resume` — Resume trading\n"
            "`!mode` — Show/switch paper or live\n"
            "`!set <param> <value>` — Change config value\n"
            "`!redeem` — Redeem resolved positions\n"
            "`!kill` — Emergency stop\n\n"
            "🧠 **Brain**\n"
            "`!brain 1` — Market intelligence\n"
            "`!brain 2` — Quant analysis\n"
            "`!brain 3` — Strategy discovery"
        )

    # ── Helpers ──────────────────────────────────────────────

    def _send(self, text: str):
        if len(text) > 1950:
            text = text[:1950] + "\n... (truncated)"
        try:
            if self._webhook_url:
                self._http.post(self._webhook_url, json={"content": text}, headers={"Content-Type": "application/json"})
            else:
                self._http.post(
                    f"{DISCORD_API}/channels/{self._channel_id}/messages",
                    json={"content": text},
                )
        except Exception as e:
            logger.error(f"Discord send failed: {e}")

    def _query_one(self, table: str, columns: str) -> dict:
        try:
            resp = self.sb.table(table).select(columns).limit(1).execute()
            return resp.data[0] if resp.data else {}
        except Exception:
            return {}

    def _query_latest(self, table: str, columns: str) -> dict:
        try:
            resp = self.sb.table(table).select(columns).order("created_at", desc=True).limit(1).execute()
            return resp.data[0] if resp.data else {}
        except Exception:
            return {}

    def _get_mode(self) -> str:
        ctrl = self._query_one("bot_control", "bot_mode")
        return ctrl.get("bot_mode", "paper") if ctrl else "paper"

    def _set_mode(self, mode: str):
        try:
            self.sb.table("bot_control").upsert({
                "id": 1, "bot_mode": mode, "updated_at": datetime.now(timezone.utc).isoformat(),
            }).execute()
            emoji = "📝" if mode == "paper" else "💰"
            self._send(f"{emoji} Mode switched to **{mode.upper()}**")
        except Exception as e:
            self._send(f"❌ Mode switch failed: {e}")

    @staticmethod
    def _fmt_time(iso_str: str) -> str:
        if not iso_str:
            return "?"
        try:
            dt = datetime.fromisoformat(str(iso_str).replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            diff = now - dt
            if diff.total_seconds() < 60:
                return f"{int(diff.total_seconds())}s ago"
            elif diff.total_seconds() < 3600:
                return f"{int(diff.total_seconds() / 60)}m ago"
            elif diff.total_seconds() < 86400:
                return f"{int(diff.total_seconds() / 3600)}h ago"
            else:
                return dt.strftime("%b %d %H:%M")
        except Exception:
            return str(iso_str)[:16]


def main():
    required = ["DISCORD_BOT_TOKEN", "DISCORD_CHANNEL_ID", "SUPABASE_URL", "SUPABASE_KEY"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        print(f"Missing env vars: {', '.join(missing)}")
        sys.exit(1)

    controller = DiscordController()
    controller.run()


if __name__ == "__main__":
    main()
