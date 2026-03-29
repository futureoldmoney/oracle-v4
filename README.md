# Oracle Bot v4

Automated quantitative trading system for Polymarket crypto prediction markets.
Primary strategy: oracle frontrunning via Chainlink price feeds.

## Quick Start

### Option A: Merge with existing repo (recommended)

```bash
# 1. Download and unzip this package
# 2. Run the merge script
cd oracle-bot-v4-master
bash _setup/merge.sh
# 3. Follow IMPLEMENTATION_CHECKLIST.md for 8 manual edits
# 4. Push to new GitHub repo
```

### Option B: Manual setup

1. Copy all files from your current `oracle-bot` repo into this folder
2. The v4 files already here will take priority (oracle_strategy, discord_bot, etc.)
3. Follow IMPLEMENTATION_CHECKLIST.md for the 8 manual edits
4. Push to GitHub

## Architecture

```
Every 2 seconds:
  Read feeds → oracle_strategy.evaluate() → executor.execute() → settlement
                    ↑                              ↑
              7 internal gates              taker-first fill
              (timing, magnitude,           (pay 1.56% fee,
               edge at fill price,           guaranteed fill)
               daily loss cap, etc.)
```

## Deployment

- **Engine:** Railway (auto-deploy from GitHub push)
- **Database:** Supabase (`bzqvqbabkxperhrrkcht`)
- **Alerts:** Discord bot (separate Railway service)
- **Monitoring:** Prometheus metrics on port 8000

## Files

| Category | Files | Status |
|----------|-------|--------|
| Core strategy | oracle_strategy, fair_value, position_sizer | NEW in v4 |
| Data sources | coinbase_spot, deribit_pcr, btc_sentiment, ltp_signal | NEW in v4 |
| Infrastructure | settlement_logic, metrics, discord_bot | NEW in v4 |
| Brain | local_learner | NEW in v4 |
| Existing engine | ensemble_engine, tier1/2_signals, data_feeds, ws_connections, executor, settlement_watcher, supabase_logger, edge_measurement | From current repo (with edits) |
| Existing brain | base, run, tier1-3, brain_tier2 | Unchanged |

## Commands (Discord)

```
!status   — Balance, win rate, sentiment, feed health
!trades   — Recent trades with fill price, edge, P&L
!pnl      — Today's P&L by direction
!signals  — Live signal status
!edge     — Edge statistics
!config   — Current config
!set      — Change config
!pause    — Pause trading
!resume   — Resume
!kill     — Emergency stop
!brain N  — Run Brain tier N
```


