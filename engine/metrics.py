"""
Prometheus Metrics Exporter
============================
Exports trading metrics for Grafana dashboards.
Runs an HTTP server on a configurable port (default 8000).

Metrics:
  oracle_trades_total        — counter (direction, outcome)
  oracle_win_rate_rolling    — gauge (50-trade rolling window)
  oracle_pnl_total           — gauge
  oracle_fill_rate           — gauge
  oracle_edge_per_trade      — histogram
  oracle_latency_seconds     — histogram
  oracle_book_reprice_speed  — histogram
  feed_healthy               — gauge (per feed: chainlink, clob, binance)
  bot_bankroll               — gauge
  bot_daily_pnl              — gauge

Ported from: aulekator/monitoring/grafana_exporter.py
Simplified for our use case (no Redis, no Nautilus).
"""

import time
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional
from collections import deque

logger = logging.getLogger("oracle.metrics")


class MetricsCollector:
    """Collects and serves Prometheus-format metrics."""

    def __init__(self):
        # Counters
        self.trades_total = 0
        self.trades_won = 0
        self.trades_lost = 0
        self.trades_up = 0
        self.trades_down = 0
        self.fills_attempted = 0
        self.fills_success = 0

        # Rolling window for win rate
        self._recent_outcomes: deque = deque(maxlen=50)

        # Gauges
        self.bankroll = 0.0
        self.daily_pnl = 0.0
        self.pnl_total = 0.0
        self.feed_chainlink = 1
        self.feed_clob = 1
        self.feed_binance = 0

        # Histograms (we'll just track recent values)
        self._edge_values: deque = deque(maxlen=100)
        self._latency_values: deque = deque(maxlen=100)
        self._reprice_values: deque = deque(maxlen=100)

    def record_trade(self, direction: str, won: bool, edge_pct: float):
        self.trades_total += 1
        if direction == "UP":
            self.trades_up += 1
        else:
            self.trades_down += 1
        if won:
            self.trades_won += 1
        else:
            self.trades_lost += 1
        self._recent_outcomes.append(1 if won else 0)
        self._edge_values.append(edge_pct)

    def record_fill_attempt(self, success: bool):
        self.fills_attempted += 1
        if success:
            self.fills_success += 1

    def record_latency(self, seconds: float):
        self._latency_values.append(seconds)

    def record_reprice_speed(self, seconds: float):
        self._reprice_values.append(seconds)

    @property
    def win_rate_rolling(self) -> float:
        if not self._recent_outcomes:
            return 0.0
        return sum(self._recent_outcomes) / len(self._recent_outcomes)

    @property
    def fill_rate(self) -> float:
        if self.fills_attempted == 0:
            return 0.0
        return self.fills_success / self.fills_attempted

    def to_prometheus(self) -> str:
        """Generate Prometheus text format metrics."""
        lines = []
        lines.append(f"# HELP oracle_trades_total Total trades placed")
        lines.append(f"# TYPE oracle_trades_total counter")
        lines.append(f'oracle_trades_total{{outcome="won"}} {self.trades_won}')
        lines.append(f'oracle_trades_total{{outcome="lost"}} {self.trades_lost}')
        lines.append(f'oracle_trades_total{{direction="up"}} {self.trades_up}')
        lines.append(f'oracle_trades_total{{direction="down"}} {self.trades_down}')

        lines.append(f"# HELP oracle_win_rate_rolling 50-trade rolling win rate")
        lines.append(f"# TYPE oracle_win_rate_rolling gauge")
        lines.append(f"oracle_win_rate_rolling {self.win_rate_rolling:.4f}")

        lines.append(f"# HELP oracle_fill_rate Fill rate")
        lines.append(f"# TYPE oracle_fill_rate gauge")
        lines.append(f"oracle_fill_rate {self.fill_rate:.4f}")

        lines.append(f"# HELP oracle_pnl_total Cumulative P&L")
        lines.append(f"# TYPE oracle_pnl_total gauge")
        lines.append(f"oracle_pnl_total {self.pnl_total:.4f}")

        lines.append(f"# HELP bot_bankroll Current bankroll")
        lines.append(f"# TYPE bot_bankroll gauge")
        lines.append(f"bot_bankroll {self.bankroll:.2f}")

        lines.append(f"# HELP bot_daily_pnl Today P&L")
        lines.append(f"# TYPE bot_daily_pnl gauge")
        lines.append(f"bot_daily_pnl {self.daily_pnl:.4f}")

        lines.append(f"# HELP feed_healthy Feed health (1=up, 0=down)")
        lines.append(f"# TYPE feed_healthy gauge")
        lines.append(f'feed_healthy{{feed="chainlink"}} {self.feed_chainlink}')
        lines.append(f'feed_healthy{{feed="clob"}} {self.feed_clob}')
        lines.append(f'feed_healthy{{feed="binance"}} {self.feed_binance}')

        # Edge histogram summary
        if self._edge_values:
            avg_edge = sum(self._edge_values) / len(self._edge_values)
            lines.append(f"# HELP oracle_avg_edge_pct Average edge per trade")
            lines.append(f"# TYPE oracle_avg_edge_pct gauge")
            lines.append(f"oracle_avg_edge_pct {avg_edge:.2f}")

        return "\n".join(lines) + "\n"


# Global singleton
_collector = MetricsCollector()


def get_metrics() -> MetricsCollector:
    return _collector


class _MetricsHandler(BaseHTTPRequestHandler):
    """HTTP handler for /metrics endpoint."""

    def do_GET(self):
        if self.path == "/metrics":
            body = _collector.to_prometheus().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(
                b"<html><body>"
                b"<h3>Oracle Bot Metrics</h3>"
                b"<a href='/metrics'>Prometheus metrics</a><br>"
                b"<a href='/health'>Health check</a>"
                b"</body></html>"
            )

    def log_message(self, format, *args):
        pass  # Suppress access logs


def start_metrics_server(port: int = 8000):
    """Start the metrics HTTP server in a background thread."""
    def _serve():
        try:
            server = HTTPServer(("0.0.0.0", port), _MetricsHandler)
            logger.info(f"Metrics server started on port {port}")
            server.serve_forever()
        except Exception as e:
            logger.error(f"Metrics server failed: {e}")

    thread = threading.Thread(target=_serve, daemon=True)
    thread.start()
    return thread
