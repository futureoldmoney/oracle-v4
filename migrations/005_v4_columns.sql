-- Migration 005: v4 columns for realistic paper trading + sentiment tracking
-- Run in Supabase SQL Editor BEFORE deploying v4 code

-- ═══════════════════════════════════════════════════════════════
-- Paper trades: add realistic fill tracking
-- ═══════════════════════════════════════════════════════════════
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS simulated_fill_price double precision;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS fair_value_at_trade double precision;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS edge_at_fill double precision;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS taker_fee_estimate double precision;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS execution_mode text;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS book_reprice_speed double precision;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS coinbase_price double precision;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS deribit_pcr double precision;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS ltp_velocity_30s double precision;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS sentiment_bias text;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS sentiment_adjustment double precision;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS fear_greed_index integer;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS btc_1d_change_pct double precision;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS btc_1h_change_pct double precision;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS size_pct double precision;

-- Same for live trades
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS simulated_fill_price double precision;
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS fair_value_at_trade double precision;
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS edge_at_fill double precision;
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS taker_fee_estimate double precision;
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS execution_mode text;
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS book_reprice_speed double precision;
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS coinbase_price double precision;
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS deribit_pcr double precision;
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS ltp_velocity_30s double precision;
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS sentiment_bias text;
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS sentiment_adjustment double precision;
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS fear_greed_index integer;
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS btc_1d_change_pct double precision;
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS btc_1h_change_pct double precision;
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS size_pct double precision;

-- Same for paper_settled
ALTER TABLE paper_settled ADD COLUMN IF NOT EXISTS simulated_fill_price double precision;
ALTER TABLE paper_settled ADD COLUMN IF NOT EXISTS fair_value_at_trade double precision;
ALTER TABLE paper_settled ADD COLUMN IF NOT EXISTS edge_at_fill double precision;
ALTER TABLE paper_settled ADD COLUMN IF NOT EXISTS taker_fee_estimate double precision;
ALTER TABLE paper_settled ADD COLUMN IF NOT EXISTS execution_mode text;
ALTER TABLE paper_settled ADD COLUMN IF NOT EXISTS sentiment_bias text;
ALTER TABLE paper_settled ADD COLUMN IF NOT EXISTS sentiment_adjustment double precision;

-- Same for live_settled
ALTER TABLE live_settled ADD COLUMN IF NOT EXISTS simulated_fill_price double precision;
ALTER TABLE live_settled ADD COLUMN IF NOT EXISTS fair_value_at_trade double precision;
ALTER TABLE live_settled ADD COLUMN IF NOT EXISTS edge_at_fill double precision;
ALTER TABLE live_settled ADD COLUMN IF NOT EXISTS taker_fee_estimate double precision;
ALTER TABLE live_settled ADD COLUMN IF NOT EXISTS execution_mode text;
ALTER TABLE live_settled ADD COLUMN IF NOT EXISTS sentiment_bias text;
ALTER TABLE live_settled ADD COLUMN IF NOT EXISTS sentiment_adjustment double precision;

-- ═══════════════════════════════════════════════════════════════
-- Position sizing config columns
-- ═══════════════════════════════════════════════════════════════
ALTER TABLE bot_config ADD COLUMN IF NOT EXISTS max_trade_size_usd double precision DEFAULT 15.0;
ALTER TABLE bot_config ADD COLUMN IF NOT EXISTS min_position_pct double precision DEFAULT 0.01;
ALTER TABLE bot_config ADD COLUMN IF NOT EXISTS max_position_pct double precision DEFAULT 0.03;
ALTER TABLE bot_config ADD COLUMN IF NOT EXISTS taker_fee_rate double precision DEFAULT 0.0156;
ALTER TABLE bot_config ADD COLUMN IF NOT EXISTS min_edge_pct double precision DEFAULT 3.0;
ALTER TABLE bot_config ADD COLUMN IF NOT EXISTS min_order_usd double precision DEFAULT 1.0;

-- ═══════════════════════════════════════════════════════════════
-- Apply corrected config values
-- ═══════════════════════════════════════════════════════════════
UPDATE bot_config SET
  weight_liquidity_vacuum = 0.3,
  weight_ob_imbalance = 0.3,
  weight_oracle_confidence = 3.0,
  fractional_kelly = 0.20,
  min_position_pct = 0.01,
  max_position_pct = 0.03,
  max_trade_size_usd = 15.0,
  taker_fee_rate = 0.0156,
  min_edge_pct = 3.0,
  min_aggregate_confidence = 0.50,
  min_edge_bps = 30,
  min_order_usd = 1.0,
  updated_by = 'V4_REBUILD',
  updated_at = now()
WHERE id = 1;

-- ═══════════════════════════════════════════════════════════════
-- Data retention cleanup (optional but recommended)
-- signal_log accumulates ~43K rows/day at 2-second cycles
-- ═══════════════════════════════════════════════════════════════
CREATE OR REPLACE FUNCTION cleanup_old_logs()
RETURNS void AS $$
BEGIN
  DELETE FROM signal_log WHERE created_at < now() - interval '7 days';
  DELETE FROM ensemble_log WHERE created_at < now() - interval '30 days';
  DELETE FROM regime_log_v2 WHERE created_at < now() - interval '30 days';
  DELETE FROM heartbeats_v2 WHERE created_at < now() - interval '7 days';
END;
$$ LANGUAGE plpgsql;
