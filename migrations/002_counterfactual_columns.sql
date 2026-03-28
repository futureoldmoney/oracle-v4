-- Migration 002: Counterfactual columns for skip analysis
-- Adds columns to track "what would have happened" on skipped trades
-- Already applied to your Supabase — included here for completeness

-- Paper trades: add counterfactual + additional trade columns
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS would_have_won boolean;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS counterfactual_pnl double precision;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS trade_reason text;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS config_snapshot jsonb;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS bankroll_at_trade double precision;

-- Expand decision check to include INSTANT FILL / MAKER FILL / UNFILLED
ALTER TABLE paper_trades DROP CONSTRAINT IF EXISTS paper_trades_decision_check;
ALTER TABLE paper_trades ADD CONSTRAINT paper_trades_decision_check 
  CHECK (decision IN ('PAPER', 'SKIP', 'INSTANT FILL', 'MAKER FILL', 'UNFILLED', 'TRADE'));

-- Live trades: add v2 enhancement columns
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS would_have_won boolean;
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS counterfactual_pnl double precision;
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS trade_reason text;
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS config_snapshot jsonb;
ALTER TABLE live_trades ADD COLUMN IF NOT EXISTS bankroll_at_trade double precision;

-- Rename trades_v2 to live_trades if not already done
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'trades_v2') 
     AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'live_trades') THEN
    ALTER TABLE trades_v2 RENAME TO live_trades;
  END IF;
END $$;

-- Create settled tables for fast queries
CREATE TABLE IF NOT EXISTS paper_settled (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at timestamptz DEFAULT now(),
  market_id text,
  market_slug text,
  market_question text,
  window_ts bigint,
  side text,
  implied_direction text,
  oracle_confidence double precision,
  edge_bps integer,
  ev_per_share double precision,
  seconds_remaining integer,
  hypothetical_price double precision,
  hypothetical_size_usdc double precision,
  hypothetical_shares double precision,
  chainlink_price double precision,
  btc_open_price double precision,
  best_bid_yes double precision,
  best_ask_yes double precision,
  spread_bps integer,
  resolved_outcome text,
  won boolean,
  pnl_usdc double precision,
  settled_at timestamptz,
  config_version integer,
  engine_version text,
  trade_reason text,
  chainlink_window_open double precision,
  chainlink_move_pct double precision,
  bankroll_at_trade double precision,
  bankroll_after double precision,
  config_snapshot jsonb,
  price_path jsonb,
  regime text,
  regime_multiplier double precision,
  kelly_fraction double precision,
  contributing_signals text,
  signal_count integer,
  ensemble_id uuid,
  aggregate_confidence double precision,
  weighted_edge_bps double precision,
  UNIQUE(market_id)
);

CREATE TABLE IF NOT EXISTS live_settled (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at timestamptz DEFAULT now(),
  market_id text,
  market_slug text,
  market_question text,
  window_ts bigint,
  side text,
  implied_direction text,
  oracle_confidence double precision,
  edge_bps integer,
  ev_per_share double precision,
  seconds_remaining integer,
  order_id text,
  limit_price double precision,
  fill_price double precision,
  fill_shares double precision,
  size_usdc double precision,
  chainlink_price double precision,
  btc_open_price double precision,
  resolved_outcome text,
  won boolean,
  pnl_usdc double precision,
  rebate_usdc double precision,
  settled_at timestamptz,
  config_version integer,
  engine_version text,
  trade_reason text,
  config_snapshot jsonb,
  chainlink_window_open double precision,
  chainlink_move_pct double precision,
  bankroll_at_trade double precision,
  regime text,
  regime_multiplier double precision,
  kelly_fraction double precision,
  contributing_signals text,
  signal_count integer,
  ensemble_id uuid,
  aggregate_confidence double precision,
  weighted_edge_bps double precision,
  price_path jsonb,
  UNIQUE(market_id)
);

-- Chainlink windows table for oracle backtesting
CREATE TABLE IF NOT EXISTS chainlink_windows (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at timestamptz DEFAULT now(),
  window_ts bigint UNIQUE NOT NULL,
  window_start timestamptz,
  open_price double precision,
  close_price double precision,
  high_price double precision,
  low_price double precision,
  price_move_pct double precision,
  direction text,
  tick_count integer,
  price_path jsonb
);

CREATE INDEX IF NOT EXISTS idx_chainlink_windows_ts ON chainlink_windows (window_ts DESC);

-- Bot config table (v3 ensemble parameters)
CREATE TABLE IF NOT EXISTS bot_config (
  id integer PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  bankroll double precision DEFAULT 1000.0,
  fractional_kelly double precision DEFAULT 0.25,
  min_aggregate_confidence double precision DEFAULT 0.40,
  min_edge_bps double precision DEFAULT 50.0,
  max_position_pct double precision DEFAULT 0.15,
  min_bet_usd double precision DEFAULT 1.0,
  weight_oracle_confidence double precision DEFAULT 2.0,
  weight_stale_quote double precision DEFAULT 1.5,
  weight_liquidity_vacuum double precision DEFAULT 1.0,
  weight_complement_arb double precision DEFAULT 1.0,
  weight_whale_flow double precision DEFAULT 0.8,
  weight_ob_imbalance double precision DEFAULT 0.8,
  min_conf_oracle_confidence double precision DEFAULT 0.30,
  min_conf_stale_quote double precision DEFAULT 0.40,
  min_conf_liquidity_vacuum double precision DEFAULT 0.45,
  min_conf_complement_arb double precision DEFAULT 0.50,
  min_conf_whale_flow double precision DEFAULT 0.40,
  min_conf_ob_imbalance double precision DEFAULT 0.35,
  stale_move_threshold_pct double precision DEFAULT 0.10,
  staleness_window_ms double precision DEFAULT 500.0,
  stale_lookback_seconds double precision DEFAULT 5.0,
  regime_window_size integer DEFAULT 360,
  confidence_alpha double precision DEFAULT 5.0,
  confidence_beta double precision DEFAULT 2.0,
  confidence_gamma double precision DEFAULT 1.5,
  vacuum_threshold double precision DEFAULT 0.5,
  vacuum_stability_threshold double precision DEFAULT 0.7,
  arb_min_profit_bps double precision DEFAULT 20.0,
  arb_target_size double precision DEFAULT 5.0,
  whale_min_score double precision DEFAULT 0.3,
  whale_size_multiplier double precision DEFAULT 2.0,
  ob_bullish_threshold double precision DEFAULT 1.3,
  ob_bearish_threshold double precision DEFAULT 0.7,
  cycle_interval_seconds double precision DEFAULT 2.0,
  market_scan_interval double precision DEFAULT 15.0,
  data_api_poll_interval double precision DEFAULT 5.0,
  max_concurrent_positions integer DEFAULT 3,
  max_daily_loss_pct double precision DEFAULT 0.20,
  max_drawdown_pct double precision DEFAULT 0.30,
  cooldown_after_loss_seconds integer DEFAULT 30,
  max_trade_size_usd double precision DEFAULT 15.0,
  updated_by text,
  updated_at timestamptz DEFAULT now()
);

INSERT INTO bot_config (id) VALUES (1) ON CONFLICT DO NOTHING;

-- Signal snapshots table
CREATE TABLE IF NOT EXISTS signal_snapshots (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at timestamptz DEFAULT now(),
  snapshot_date date NOT NULL,
  signal_name text NOT NULL,
  total_trades integer DEFAULT 0,
  wins integer DEFAULT 0,
  win_rate double precision,
  total_pnl_bps double precision DEFAULT 0,
  avg_edge_bps double precision DEFAULT 0,
  current_weight double precision,
  adjusted_weight double precision,
  UNIQUE(snapshot_date, signal_name)
);

-- Backfill function for signal_log settlement
CREATE OR REPLACE FUNCTION backfill_signal_log_outcome(
  p_market_id text,
  p_outcome text,
  p_pnl double precision
) RETURNS void AS $$
BEGIN
  UPDATE signal_log sl
  SET was_correct = (sl.direction = p_outcome),
      counterfactual_pnl = CASE
        WHEN sl.direction = 'NEUTRAL' THEN NULL
        WHEN sl.direction = p_outcome THEN 0.50
        ELSE -0.50
      END
  FROM ensemble_log el
  WHERE sl.ensemble_id = el.id
    AND el.market_id = p_market_id
    AND sl.was_correct IS NULL;
END;
$$ LANGUAGE plpgsql;
