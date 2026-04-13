# Cadence Options

A self-hosted Python options-selling trading bot targeting Tradier's brokerage API. Scans SPY and QQQ for 45 DTE iron condor opportunities, gates every trade through a risk manager, and serves an interactive web dashboard.

**No external dependencies.** Python 3.8+ stdlib only.

## Quick Start

```bash
# 1. Clone and configure
cp .env.example .env
# Edit .env with your Tradier sandbox credentials

# 2. Run (defaults to sandbox + dry run)
python3 dashboard.py

# 3. Open dashboard
# http://localhost:8050
```

## Strategy

**45 DTE Iron Condors** on SPY and QQQ:

- **Entry**: Short strikes at 16 delta (~1 SD), $10 wings
- **IV filter**: Only enter when IV rank >= 30
- **Min credit**: 20% of spread width
- **Profit target**: Close at 50% of max profit
- **Time stop**: Close at 21 DTE
- **Loss stop**: Close at 2x credit collected
- **No rolling**: Never roll losers

## Architecture

```
cadence_options/
  dashboard.py              Main entry, .env loader, HTTP server, glue
  dashboard.html            Single-file interactive frontend
  cadence/                  Core package
    tradier_client.py       Tradier REST API client (urllib only)
    iv_rank.py              IV rank computation with 1-hour cache
    strategy.py             Iron condor candidate scanner
    risk_manager.py         Trade gating, kill switch, drawdown limits
    position_manager.py     Exit detection (profit/time/loss stops)
    executor.py             Order placement with safety validation
    process_controller.py   Scanner + executor background threads
    market_calendar.py      NYSE holidays, early closes, DST handling
    kelly.py                Kelly-criterion position sizing
    notifier.py             Telegram notifications + commands
  tests/                    Test suite (287 tests)
    test_tradier_client.py
    test_iv_rank.py
    test_strategy.py
    test_risk_manager.py    Includes all 7 regression tests
    test_position_manager.py
    test_executor.py
    test_process_controller.py
    test_market_calendar.py
    test_kelly.py
    test_notifier.py
    test_dashboard.py
```

## Safety

- **Sandbox by default.** Production requires `TRADIER_ENV=production`.
- **Dry run by default.** Live trading requires explicit activation.
- **No naked options.** Only 4-leg iron condors or 2-leg credit spreads.
- **Kill switch** auto-activates on drawdown, requires manual resume.
- **Pre-trade balance sync** from Tradier on every trade (not cached).
- **Market hours only.** 9:30-16:00 ET Mon-Fri.
- **Max 1 trade per executor cycle** with 5-minute dedup.
- **Max 5 concurrent positions**, 2% equity risk per position.

## Configuration

All settings via `.env` file. See `.env.example` for full list.

Key settings:
| Variable | Default | Description |
|----------|---------|-------------|
| `TRADIER_ENV` | `sandbox` | `sandbox` or `production` -- selects which credentials below to use |
| `TRADIER_SANDBOX_ACCESS_TOKEN` | | Token for Tradier sandbox |
| `TRADIER_SANDBOX_ACCOUNT_ID` | | Account ID for Tradier sandbox |
| `TRADIER_PRODUCTION_ACCESS_TOKEN` | | Token for Tradier production (live money) |
| `TRADIER_PRODUCTION_ACCOUNT_ID` | | Account ID for Tradier production |
| `CADENCE_SYMBOLS` | `SPY,QQQ` | Comma-separated symbols |
| `CADENCE_MAX_DRAWDOWN_PCT` | `10` | Kill switch threshold |
| `CADENCE_DRAWDOWN_REFERENCE` | `session_start` | `session_start` or `peak` |
| `CADENCE_MAX_PER_POSITION_PCT` | `2` | Hard ceiling on per-position risk |
| `CADENCE_USE_KELLY` | `false` | Opt-in Kelly-based position sizing |
| `CADENCE_KELLY_FRACTION` | `0.25` | Fractional-Kelly safety factor (quarter) |
| `CADENCE_PORT` | `8050` | Dashboard HTTP port |

## Dashboard API

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/risk` | Risk manager status |
| GET | `/api/scan` | Scanner candidates |
| GET | `/api/positions` | Open positions |
| GET | `/api/processes` | Scanner/executor status |
| GET | `/api/diagnostics` | System diagnostics |
| POST | `/api/scanner/start` | Start scanner |
| POST | `/api/scanner/stop` | Stop scanner |
| POST | `/api/executor/start` | Start executor |
| POST | `/api/executor/stop` | Stop executor |
| POST | `/api/risk/kill-switch/activate` | Activate kill switch |
| POST | `/api/risk/kill-switch/deactivate` | Deactivate kill switch |
| POST | `/api/risk/reset-daily` | Reset daily metrics |

## Telegram Commands

Optional. Set `CADENCE_TELEGRAM_BOT_TOKEN` and `CADENCE_TELEGRAM_CHAT_ID`.

Read-only: `/help`, `/status`, `/positions`, `/decisions`, `/config`
Actions: `/kill`, `/resume`, `/reset`, `/scanner_start`, `/scanner_stop`, `/exec_start`, `/exec_stop`
Dangerous: `/exec_live` (requires CONFIRM reply within 30s)

## Tests

```bash
python3 -m pytest
```

296 unit tests covering all modules. Live sandbox tests run when `TRADIER_SANDBOX_ACCESS_TOKEN` (or legacy `TRADIER_ACCESS_TOKEN`) and matching account ID are set.

## IV Rank

IV rank is computed from the underlying's matching volatility index:

| Underlying | Volatility Index |
|------------|------------------|
| SPY | VIX (S&P 500 Volatility Index) |
| QQQ | VXN (Nasdaq-100 Volatility Index) |
| IWM | RVX (Russell 2000 Volatility Index) |
| DIA | VXD (Dow Jones Volatility Index) |

These indices ARE the 30-day implied volatility of the underlying's options, so `(current - 52w_low) / (52w_high - 52w_low) * 100` gives a true IV rank.

For symbols without a matching volatility index, `IVHistoryStore` can snapshot ATM IV from the live option chain daily and build history locally over time. Requires 20+ data points before producing rankings.

## Position Sizing

Two layers, both always active:

**1. Hardcoded ceiling** (`CADENCE_MAX_PER_POSITION_PCT`, default 2%).
No single position's max loss can exceed this fraction of equity. Acts
as a one-way safety ratchet: Kelly can recommend smaller, never larger.

**2. Kelly-criterion cap** (opt-in via `CADENCE_USE_KELLY=true`).
Derives a per-position risk cap from the operator's actual trade
history:

```
f* = (win_rate * avg_win - loss_rate * avg_loss) / avg_win
recommended_pct = min(f* * kelly_fraction, max_per_position_pct)
```

Defaults to quarter Kelly (`CADENCE_KELLY_FRACTION=0.25`). Full Kelly is
provably growth-optimal in the long run but unforgiving of sampling
noise and regime change -- fractional Kelly is how professionals
actually trade.

Before 20 completed trades, Kelly uses a conservative prior for 45-DTE
SPY iron condors (win_rate=0.75, avg_win=30% of max profit, avg_loss=70%
of max loss -> full Kelly ~17%, quarter ~4%, clipped by the 2% manual
ceiling to 2%). Once 20+ trades are in the book, the cap adapts to the
operator's empirical stats. If the edge degrades -- cap gets tighter.
If the edge improves past the manual ceiling -- cap stays at the manual
ceiling (safer than Kelly suggests).

`get_status()` exposes `kelly.full_kelly_fraction`, `fractional_kelly_pct`,
`effective_cap_pct`, `sample_size`, and `win_rate` so operators can see
what the data is saying without Kelly being enforced.

## Market Calendar

The bot respects NYSE holidays and early-close days computed at runtime
(no external data required):

- **Full closures (10/yr)**: New Year's Day, MLK Day, Presidents' Day,
  Good Friday, Memorial Day, Juneteenth (since 2022), Independence Day,
  Labor Day, Thanksgiving, Christmas. Sunday holidays observed Monday.
- **1pm ET early closes**: Day after Thanksgiving, July 3 (when July 4
  is a weekday), December 24 (when it is a weekday).

Good Friday is computed via the Anonymous Gregorian algorithm for Easter.
`is_market_open()` returns False on all holidays and after the early
close time on partial-day sessions.

## Timezone and DST

All market-hours logic runs in US Eastern with automatic DST handling.
`market_calendar.et_offset_hours(date)` returns -4 during EDT (2nd Sun
of March through 1st Sun of November) and -5 during EST, following
the US rules in effect since 2007.

`_now_et()` resolves to the correct local time at all times during
market hours. The 1-2am ambiguity on transition day itself does not
affect the bot since markets are closed then.

## Known Limitations

- Only symbols in `VOLATILITY_INDEX_SYMBOLS` get IV rank out of the box. Others need `IVHistoryStore` wired up and several weeks of data before IV rank filtering is useful.
- Ad-hoc NYSE closures (presidential funerals, weather, Sept 11-style events) are not covered. Rare, not deterministic, and require manual override via kill switch.
- Assumes US DST rules remain as enacted in 2007. If Congress permanently abolishes DST, the calendar will need a rule update.

## Disclaimer

This software is for educational purposes. Options trading involves substantial risk of loss. Past performance does not guarantee future results. Use at your own risk. Always start with paper trading (sandbox mode).
