-- Oracle Bot v2 — Final Schema
-- Run in: supabase.com/dashboard/project/bzqvqbabkxperhrrkcht/editor

-- ============================================================
-- BOT CONTROL: Engine ↔ Brain shared state
-- ============================================================
create table if not exists bot_control (
  id integer primary key default 1 check (id = 1),
  paused boolean default false,
  pause_reason text,
  paused_at timestamptz,
  paused_by text check (paused_by in ('BRAIN', 'HUMAN')),
  bot_mode text default 'paper' check (bot_mode in ('paper', 'live')),
  updated_at timestamptz default now()
);
insert into bot_control (id, paused, bot_mode) values (1, false, 'paper') on conflict do nothing;

-- ============================================================
-- CONFIG: versioned, latest by max(id) wins
-- ============================================================
create table if not exists config_v2 (
  id serial primary key,
  created_at timestamptz default now(),
  strategy_type text default 'ORACLE_MAKER',
  min_edge_bps integer default 150,
  min_confidence numeric default 0.60,
  skew_bps integer default 100,
  quote_size_usdc numeric default 2.0,
  max_position_usdc numeric default 10.0,
  min_seconds_before_close integer default 30,
  max_seconds_before_close integer default 240,
  max_daily_trades integer default 100,
  max_daily_loss_usdc numeric default 10.0,
  max_concurrent_orders integer default 1,
  consecutive_fail_limit integer default 5,
  pause_minutes integer default 15,
  kelly_fraction numeric default 0.25,
  -- Confidence calibration (Tier 2 writes these)
  confidence_calibration jsonb default '{}',
  updated_by text check (updated_by in ('BRAIN', 'HUMAN', 'TIER2', 'TIER3')),
  reason text
);
insert into config_v2 (updated_by, reason) values ('HUMAN', 'Initial default config');

-- ============================================================
-- LIVE TRADES
-- ============================================================
create table if not exists trades_v2 (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz default now(),
  market_id text not null,
  market_slug text,
  market_question text,
  token_id text,
  window_start_ts bigint,
  window_end_ts bigint,
  seconds_remaining integer,
  decision text not null check (decision in ('TRADE', 'SKIP')),
  skip_reason text,
  chainlink_price numeric,
  btc_open_price numeric,
  implied_direction text check (implied_direction in ('UP', 'DOWN')),
  oracle_confidence numeric,
  edge_bps integer,
  ev_per_share numeric,
  best_bid_yes numeric,
  best_ask_yes numeric,
  spread_bps integer,
  side text check (side in ('YES', 'NO')),
  limit_price numeric,
  size_usdc numeric,
  order_id text unique,
  placed_at timestamptz,
  fill_price numeric,
  filled_at timestamptz,
  fill_status text check (fill_status in ('OPEN', 'FILLED', 'PARTIAL', 'EXPIRED', 'CANCELLED', 'REJECTED', 'ERROR')),
  fill_shares numeric,
  resolved_outcome text,
  won boolean,
  pnl_usdc numeric,
  rebate_usdc numeric,
  settled_at timestamptz,
  config_version integer,
  engine_version text
);

-- ============================================================
-- PAPER TRADES: identical structure, completely isolated
-- ============================================================
create table if not exists paper_trades (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz default now(),
  market_id text not null,
  market_slug text,
  market_question text,
  token_id text,
  window_start_ts bigint,
  window_end_ts bigint,
  seconds_remaining integer,
  decision text not null check (decision in ('PAPER', 'SKIP')),
  skip_reason text,
  chainlink_price numeric,
  btc_open_price numeric,
  implied_direction text check (implied_direction in ('UP', 'DOWN')),
  oracle_confidence numeric,
  edge_bps integer,
  ev_per_share numeric,
  best_bid_yes numeric,
  best_ask_yes numeric,
  spread_bps integer,
  side text check (side in ('YES', 'NO')),
  hypothetical_price numeric,
  hypothetical_size_usdc numeric,
  hypothetical_shares numeric,
  resolved_outcome text,
  won boolean,
  pnl_usdc numeric,
  settled_at timestamptz,
  config_version integer,
  engine_version text
);

-- ============================================================
-- HEARTBEATS
-- ============================================================
create table if not exists heartbeats_v2 (
  id bigserial primary key,
  created_at timestamptz default now(),
  bot_mode text,
  balance_usdc numeric,
  active_orders integer default 0,
  pending_settlements integer default 0,
  trades_today integer default 0,
  pnl_today numeric default 0,
  status text default 'ALIVE' check (status in ('ALIVE', 'PAUSED', 'ERROR')),
  error_message text
);

-- ============================================================
-- BRAIN REVIEWS (all three tiers)
-- ============================================================
create table if not exists brain_reviews (
  id serial primary key,
  created_at timestamptz default now(),
  review_type text not null check (review_type in (
    'INTELLIGENCE', 'QUANT', 'DISCOVERY', 'EMERGENCY'
  )),
  tier integer check (tier in (1, 2, 3)),
  trade_count integer,
  win_rate numeric,
  total_pnl numeric,
  analysis_json jsonb,
  config_changes_json jsonb,
  severity text check (severity in ('INFO', 'MINOR', 'MAJOR', 'CRITICAL')),
  reasoning text,
  config_applied boolean default false,
  new_config_id integer,
  api_model text,
  api_tokens_used integer,
  alert_sent boolean default false
);

-- ============================================================
-- WINDOW PRICE CACHE (for btc_open_price tracking)
-- ============================================================
create table if not exists window_prices (
  window_ts bigint primary key,
  chainlink_price numeric not null,
  binance_price numeric,
  captured_at timestamptz default now()
);

-- ============================================================
-- INDEXES
-- ============================================================
create index if not exists idx_trades_v2_created on trades_v2 (created_at desc);
create index if not exists idx_trades_v2_order_id on trades_v2 (order_id) where order_id is not null;
create index if not exists idx_trades_v2_unsettled on trades_v2 (fill_status) where fill_status = 'FILLED' and won is null;
create index if not exists idx_trades_v2_market_open on trades_v2 (market_id, fill_status) where fill_status = 'OPEN';
create index if not exists idx_paper_unsettled on paper_trades (decision) where decision = 'PAPER' and won is null;
create index if not exists idx_heartbeats_created on heartbeats_v2 (created_at desc);
create index if not exists idx_window_prices on window_prices (window_ts desc);

-- ============================================================
-- RLS
-- ============================================================
alter table bot_control enable row level security;
alter table config_v2 enable row level security;
alter table trades_v2 enable row level security;
alter table paper_trades enable row level security;
alter table heartbeats_v2 enable row level security;
alter table brain_reviews enable row level security;
alter table window_prices enable row level security;

create policy "svc" on bot_control for all using (true) with check (true);
create policy "svc" on config_v2 for all using (true) with check (true);
create policy "svc" on trades_v2 for all using (true) with check (true);
create policy "svc" on paper_trades for all using (true) with check (true);
create policy "svc" on heartbeats_v2 for all using (true) with check (true);
create policy "svc" on brain_reviews for all using (true) with check (true);
create policy "svc" on window_prices for all using (true) with check (true);
