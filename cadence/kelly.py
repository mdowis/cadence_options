"""Kelly-criterion position sizing for iron condors.

The Kelly fraction is the portion of bankroll to risk per trade that
maximizes long-run geometric growth:

    f* = (p * W - (1-p) * L) / W

where p is the win rate, W is the average win amount, and L is the
average loss amount. f* comes out as a fraction of bankroll to commit
as capital-at-risk per trade.

Full Kelly is provably growth-optimal in the long run but extremely
volatile and unforgiving of parameter error. Professional traders
almost always use fractional Kelly (half, quarter, or eighth) to
survive drawdowns caused by:
  - Sampling noise in the win-rate estimate (especially early on)
  - Non-stationary edges (regime changes break the stationarity Kelly assumes)
  - Path-dependent risk (iron condors can lose max_loss, not just avg_loss)

Defaults here use quarter Kelly (fraction_of_full=0.25) and the
typical 45-DTE SPY iron condor baseline:
  win_rate = 0.75, avg_win_pct = 0.30, avg_loss_pct = 0.70

which gives full Kelly ~17% and quarter Kelly ~4%. A conservative
operator might cap at eighth Kelly (~2%), which is why the hardcoded
max_risk_per_position_pct=2.0 is also a sensible choice.
"""

import logging

logger = logging.getLogger(__name__)


# ---- Pure Kelly math --------------------------------------------------------

def compute_kelly_fraction(win_rate, avg_win, avg_loss, loss_rate=None):
    """Compute the full Kelly fraction.

    f* = (p * W - (1-p) * L) / W

    Args:
      win_rate:   probability of winning (0..1)
      avg_win:    average win size (same units as avg_loss, > 0)
      avg_loss:   average loss size (positive value, > 0)
      loss_rate:  optional; defaults to 1 - win_rate

    Returns f* as a float. Negative means the bet has negative
    expectation and should not be taken. Returns 0.0 if avg_win <= 0
    (undefined). f* is not clamped to [0, 1] -- callers should do that.
    """
    if avg_win <= 0:
        return 0.0
    if loss_rate is None:
        loss_rate = 1.0 - win_rate
    edge = win_rate * avg_win - loss_rate * avg_loss
    return edge / avg_win


def fractional_kelly(full_kelly_fraction, fraction_of_full=0.25):
    """Apply a fractional-Kelly safety factor.

    fraction_of_full is typically 0.5 (half), 0.25 (quarter), or 0.125
    (eighth). Negative Kelly fractions are clamped to 0 -- you never
    bet a negative fraction of your bankroll.
    """
    if full_kelly_fraction <= 0:
        return 0.0
    return full_kelly_fraction * fraction_of_full


# ---- Derive Kelly inputs from completed trades ------------------------------

DEFAULT_WIN_RATE = 0.75
DEFAULT_AVG_WIN_PCT = 0.30    # Average win = 30% of max profit (profit-target exits)
DEFAULT_AVG_LOSS_PCT = 0.70   # Average loss = 70% of max loss (loss-stop exits)
MIN_TRADES_FOR_EMPIRICAL = 20 # Below this, use defaults


def compute_kelly_from_history(
    trade_history,
    default_win_rate=DEFAULT_WIN_RATE,
    default_avg_win_pct=DEFAULT_AVG_WIN_PCT,
    default_avg_loss_pct=DEFAULT_AVG_LOSS_PCT,
    min_trades=MIN_TRADES_FOR_EMPIRICAL,
):
    """Derive Kelly inputs from completed trades (trade_history).

    trade_history: list of dicts with at least `pnl_cents` key.

    Returns:
      {
        "win_rate": float,
        "avg_win_cents": int,
        "avg_loss_cents": int,
        "kelly_fraction": float,    # full Kelly (f*)
        "sample_size": int,         # number of trades used
        "using_defaults": bool,     # True if fell back to assumed values
      }

    With fewer than min_trades completed, falls back to defaults so the
    bot has a sensible cap from day one. The defaults are based on
    typical 45-DTE SPY iron condor statistics and should be treated as
    conservative priors, not certainties.
    """
    completed = [t for t in trade_history
                 if isinstance(t, dict) and "pnl_cents" in t]
    n = len(completed)

    if n < min_trades:
        # Not enough data -- use the default prior.
        # Defaults are in percentage-of-max units, so we treat them as
        # directly-comparable ratios for the Kelly formula.
        kelly = compute_kelly_fraction(
            default_win_rate, default_avg_win_pct, default_avg_loss_pct)
        return {
            "win_rate": default_win_rate,
            "avg_win_cents": 0,
            "avg_loss_cents": 0,
            "kelly_fraction": kelly,
            "sample_size": n,
            "using_defaults": True,
        }

    wins = [t["pnl_cents"] for t in completed if t["pnl_cents"] > 0]
    losses = [abs(t["pnl_cents"]) for t in completed if t["pnl_cents"] < 0]

    win_rate = len(wins) / n
    avg_win = sum(wins) / len(wins) if wins else 0
    avg_loss = sum(losses) / len(losses) if losses else 0

    kelly = compute_kelly_fraction(win_rate, avg_win, avg_loss)
    return {
        "win_rate": win_rate,
        "avg_win_cents": int(avg_win),
        "avg_loss_cents": int(avg_loss),
        "kelly_fraction": kelly,
        "sample_size": n,
        "using_defaults": False,
    }


def recommended_position_risk_pct(trade_history, fraction_of_full=0.25,
                                  absolute_cap_pct=None, **kwargs):
    """Return the recommended per-position risk cap as a percentage.

    Combines compute_kelly_from_history + fractional_kelly and
    optionally clips at an absolute cap (e.g., a conservative manual
    limit like 2%).

    Returns a dict:
      {
        "kelly_fraction": f*,
        "fractional_kelly_pct": f* * fraction_of_full * 100,
        "recommended_pct": final cap after absolute cap clip,
        "fraction_of_full": fraction_of_full,
        "sample_size": int,
        "using_defaults": bool,
        "win_rate": float,
      }
    """
    info = compute_kelly_from_history(trade_history, **kwargs)
    frac = fractional_kelly(info["kelly_fraction"], fraction_of_full)
    pct = frac * 100.0
    if absolute_cap_pct is not None:
        pct = min(pct, absolute_cap_pct)
    return {
        "kelly_fraction": info["kelly_fraction"],
        "fractional_kelly_pct": frac * 100.0,
        "recommended_pct": pct,
        "fraction_of_full": fraction_of_full,
        "sample_size": info["sample_size"],
        "using_defaults": info["using_defaults"],
        "win_rate": info["win_rate"],
    }
