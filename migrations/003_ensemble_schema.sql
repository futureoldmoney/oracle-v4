-- ============================================================
-- Oracle Bot v3: Ensemble Signal Schema
-- Run in Supabase SQL Editor
-- Project: bzqvqbabkxperhrrkcht
-- ============================================================
-- 
-- This migration adds 3 new tables for the multi-signal ensemble:
--   1. signal_log       — Every signal evaluation, every cycle
--   2. ensemble_log     — Every ensemble decision (trade or skip)
--   3. regime_log_v2    — Market regime snapshots
--
-- It also adds v3 columns to the existing trades_v2 table
-- to link trades back to their ensemble decisions.
-- ============================================================


-- ============================================================
-- TABLE: signal_log
-- Every signal evaluation from every cycle gets a row.
-- This is the raw data the Brain uses to measure per-signal edge.
-- ============================================================

CREATE TABLE IF NOT EXISTS signal_log (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at timestamptz DEFAULT now(),

  -- Link to the ensemble decision that this signal contributed to
  ensemble_id uuid,

  -- Signal identity
  signal_name text NOT NULL,
  -- e.g. ORACLE_CONFIDENCE, STALE_QUOTE, LIQUIDITY_VACUUM,
  --      COMPLEMENT_ARB, WHALE_FLOW, OB_IMBALANCE

  -- Signal output
  direction text CHECK (direction IN ('UP', 'DOWN', 'NEUTRAL')),
  confidence double precision,
  estimated_edge_bps double precision,
  magnitude double precision,

  -- Was this signal actionable? (passed min_confidence threshold)
  is_actionable boolean DEFAULT false,

  -- Did this signal agree with the ensemble's final direction?
  agreed_with_ensemble boolean,

  -- Signal-specific metadata (JSON blob for debugging)
  metadata jsonb DEFAULT '{}',

  -- Market context at signal evaluation time
  market_id text,
  binance_price double precision,
  chainlink_price double precision,
  polymarket_midpoint double precision,
  spread double precision,

  -- Outcome tracking (filled after settlement)
  -- Was this signal's direction correct?
  was_correct boolean,
  -- Counterfactual: if we traded ONLY this signal, what would P&L be?
  counterfactual_pnl double precision
);

-- Indexes for Brain queries
CREATE INDEX IF NOT EXISTS idx_signal_log_name 
  ON signal_log (signal_name);
CREATE INDEX IF NOT EXISTS idx_signal_log_created 
  ON signal_log (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_signal_log_ensemble 
  ON signal_log (ensemble_id);
CREATE INDEX IF NOT EXISTS idx_signal_log_actionable 
  ON signal_log (signal_name, is_actionable) 
  WHERE is_actionable = true;
CREATE INDEX IF NOT EXISTS idx_signal_log_correct 
  ON signal_log (signal_name, was_correct) 
  WHERE was_correct IS NOT NULL;


-- ============================================================
-- TABLE: ensemble_log
-- One row per ensemble evaluation cycle.
-- Records the aggregated decision: trade or skip, with full reasoning.
-- ============================================================

CREATE TABLE IF NOT EXISTS ensemble_log (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at timestamptz DEFAULT now(),

  -- Market context
  market_id text,
  market_question text,
  seconds_before_close integer,

  -- Ensemble decision
  direction text CHECK (direction IN ('UP', 'DOWN', 'NEUTRAL')),
  should_trade boolean NOT NULL DEFAULT false,
  reason text,

  -- Aggregate metrics
  aggregate_confidence double precision,
  weighted_edge_bps double precision,
  kelly_fraction double precision,
  recommended_size_usd double precision,

  -- How many signals contributed
  total_signals_evaluated integer DEFAULT 0,
  actionable_signals integer DEFAULT 0,
  agreeing_signals integer DEFAULT 0,
  
  -- Which signals fired (comma-separated names for quick filtering)
  contributing_signal_names text,

  -- Regime at decision time
  regime text CHECK (regime IN ('tight', 'normal', 'wide', 'extreme')),
  regime_multiplier double precision,
  spread_percentile double precision,

  -- Prices at decision time
  binance_price double precision,
  chainlink_price double precision,
  polymarket_yes_mid double precision,
  polymarket_spread double precision,
  oracle_gap_pct double precision,

  -- Risk state
  current_bankroll double precision,
  peak_bankroll double precision,
  daily_pnl double precision,
  drawdown_pct double precision,

  -- Outcome (filled after settlement)
  actual_outcome text CHECK (actual_outcome IN ('UP', 'DOWN')),
  was_correct boolean,
  trade_pnl double precision,

  -- Link to trade if one was placed
  trade_id uuid,

  -- Config version used
  config_version integer
);

CREATE INDEX IF NOT EXISTS idx_ensemble_log_created 
  ON ensemble_log (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ensemble_log_trade 
  ON ensemble_log (should_trade);
CREATE INDEX IF NOT EXISTS idx_ensemble_log_regime 
  ON ensemble_log (regime);
CREATE INDEX IF NOT EXISTS idx_ensemble_log_market 
  ON ensemble_log (market_id);


-- ============================================================
-- TABLE: regime_log_v2
-- Periodic snapshots of market regime (every 5 minutes or on change)
-- ============================================================

CREATE TABLE IF NOT EXISTS regime_log_v2 (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at timestamptz DEFAULT now(),
  
  regime text NOT NULL,
  spread_percentile double precision,
  volatility_1m double precision,
  book_depth_ratio double precision,
  regime_multiplier double precision,
  
  -- Book state
  best_bid double precision,
  best_ask double precision,
  spread double precision,
  bid_depth_5 double precision,
  ask_depth_5 double precision,
  
  -- Price state
  binance_price double precision,
  chainlink_price double precision,
  oracle_gap_pct double precision
);

CREATE INDEX IF NOT EXISTS idx_regime_log_v2_created 
  ON regime_log_v2 (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_regime_log_v2_regime 
  ON regime_log_v2 (regime);


-- ============================================================
-- ALTER: Add v3 columns to existing trades_v2 table
-- Links each trade to its ensemble decision + signal breakdown
-- ============================================================

ALTER TABLE trades_v2 ADD COLUMN IF NOT EXISTS ensemble_id uuid;
ALTER TABLE trades_v2 ADD COLUMN IF NOT EXISTS aggregate_confidence double precision;
ALTER TABLE trades_v2 ADD COLUMN IF NOT EXISTS weighted_edge_bps double precision;
ALTER TABLE trades_v2 ADD COLUMN IF NOT EXISTS kelly_fraction double precision;
ALTER TABLE trades_v2 ADD COLUMN IF NOT EXISTS regime text;
ALTER TABLE trades_v2 ADD COLUMN IF NOT EXISTS regime_multiplier double precision;
ALTER TABLE trades_v2 ADD COLUMN IF NOT EXISTS contributing_signals text;
ALTER TABLE trades_v2 ADD COLUMN IF NOT EXISTS signal_count integer;

-- Same for paper_trades
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS ensemble_id uuid;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS aggregate_confidence double precision;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS weighted_edge_bps double precision;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS kelly_fraction double precision;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS regime text;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS regime_multiplier double precision;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS contributing_signals text;
ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS signal_count integer;


-- ============================================================
-- ALTER: Add v3 columns to brain_reviews
-- ============================================================

ALTER TABLE brain_reviews ADD COLUMN IF NOT EXISTS signal_recommendations jsonb;
ALTER TABLE brain_reviews ADD COLUMN IF NOT EXISTS regime_assessment jsonb;
ALTER TABLE brain_reviews ADD COLUMN IF NOT EXISTS edge_estimates jsonb;


-- ============================================================
-- VIEWS: Convenient queries for the Brain
-- ============================================================

-- Per-signal win rate (only settled, actionable signals)
CREATE OR REPLACE VIEW v_signal_performance AS
SELECT 
  signal_name,
  COUNT(*) as total_evaluations,
  COUNT(*) FILTER (WHERE is_actionable) as actionable_count,
  COUNT(*) FILTER (WHERE was_correct IS NOT NULL) as settled_count,
  COUNT(*) FILTER (WHERE was_correct = true) as correct_count,
  ROUND(
    COUNT(*) FILTER (WHERE was_correct = true)::numeric / 
    NULLIF(COUNT(*) FILTER (WHERE was_correct IS NOT NULL), 0),
    4
  ) as win_rate,
  ROUND((AVG(confidence) FILTER (WHERE is_actionable))::numeric, 4) as avg_confidence,
  ROUND((AVG(estimated_edge_bps) FILTER (WHERE is_actionable))::numeric, 1) as avg_edge_bps,
  ROUND((SUM(counterfactual_pnl) FILTER (WHERE counterfactual_pnl IS NOT NULL))::numeric, 4) as total_counterfactual_pnl
FROM signal_log
GROUP BY signal_name
ORDER BY settled_count DESC;


-- Ensemble decision quality
CREATE OR REPLACE VIEW v_ensemble_performance AS
SELECT 
  regime,
  COUNT(*) as total_decisions,
  COUNT(*) FILTER (WHERE should_trade) as trades,
  COUNT(*) FILTER (WHERE NOT should_trade) as skips,
  COUNT(*) FILTER (WHERE was_correct = true) as correct,
  COUNT(*) FILTER (WHERE was_correct = false) as incorrect,
  ROUND(
    COUNT(*) FILTER (WHERE was_correct = true)::numeric / 
    NULLIF(COUNT(*) FILTER (WHERE was_correct IS NOT NULL), 0),
    4
  ) as win_rate,
  ROUND((AVG(aggregate_confidence) FILTER (WHERE should_trade))::numeric, 4) as avg_confidence,
  ROUND((AVG(weighted_edge_bps) FILTER (WHERE should_trade))::numeric, 1) as avg_edge_bps,
  ROUND((SUM(trade_pnl) FILTER (WHERE trade_pnl IS NOT NULL))::numeric, 4) as total_pnl
FROM ensemble_log
GROUP BY regime
ORDER BY total_decisions DESC;


-- Signal agreement analysis (which signal combos win most?)
CREATE OR REPLACE VIEW v_signal_combos AS
SELECT 
  e.contributing_signal_names as signal_combo,
  COUNT(*) as trades,
  COUNT(*) FILTER (WHERE e.was_correct = true) as wins,
  ROUND(
    COUNT(*) FILTER (WHERE e.was_correct = true)::numeric / 
    NULLIF(COUNT(*), 0),
    4
  ) as win_rate,
  ROUND(AVG(e.aggregate_confidence)::numeric, 4) as avg_confidence,
  ROUND(SUM(e.trade_pnl)::numeric, 4) as total_pnl
FROM ensemble_log e
WHERE e.should_trade = true AND e.was_correct IS NOT NULL
GROUP BY e.contributing_signal_names
HAVING COUNT(*) >= 3
ORDER BY win_rate DESC;
