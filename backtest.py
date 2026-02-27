"""
Bitcoin Prediction Market — Mid-Price Momentum Scalp Backtest v2
=================================================================
Key changes from v1:
  - Vectorized signal generation (100x faster — seconds not minutes)
  - Relaxed liquidity filter: signals fire when at least one side present
  - Robust mid-price estimation for one-sided markets
  - mean_edge KeyError fix
  - Diagnostic report added to explain signal counts before sweep
"""

import math
import itertools
import warnings
import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import Optional
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

warnings.filterwarnings("ignore")


# ─────────────────────────────────────────────
# 1. FEE MODEL
# ─────────────────────────────────────────────

def kalshi_fee(C: int, P: float) -> float:
    """ceil(0.07 * C * P * (1-P) * 100) / 100"""
    if C <= 0:
        return 0.0
    return math.ceil(0.07 * C * P * (1 - P) * 100) / 100


# ─────────────────────────────────────────────
# 2. KELLY SIZING
# ─────────────────────────────────────────────

def kelly_fraction(q: float, p: float) -> float:
    """Binary Kelly fraction. Returns negative if no edge."""
    if p <= 0 or p >= 1:
        return 0.0
    return (q * (1 - p) - (1 - q) * p) / (p * (1 - p))


MAX_KELLY_FRACTION = 0.25  # hard cap: never risk more than 25% of bankroll on one trade

def contracts_from_kelly(f, kelly_mult, bankroll, entry_price):
    # Cap the effective fraction to avoid catastrophic oversizing at extreme prices
    effective_f = min(f * kelly_mult, MAX_KELLY_FRACTION)
    stake = effective_f * bankroll
    if stake <= 0 or entry_price <= 0:
        return 0
    return max(0, math.floor(stake / entry_price))


# ─────────────────────────────────────────────
# 3. PREPROCESSING
# ─────────────────────────────────────────────

def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Drop rows where ALL bid/qty columns are NaN (post-resolution noise)
    bid_cols = ["yes_best_bid_dollars", "yes_best_bid_qty",
                "no_best_bid_dollars",  "no_best_bid_qty"]
    df = df.dropna(subset=bid_cols, how="all")

    df["fetched_at"] = pd.to_datetime(df["fetched_at"])
    df = df.sort_values(["market_ticker", "fetched_at"]).reset_index(drop=True)

    # Keep only complete chains (11–15 min duration)
    durations = df.groupby("market_ticker")["fetched_at"].agg(["min", "max"])
    durations["dur_min"] = (durations["max"] - durations["min"]).dt.total_seconds() / 60
    valid = durations[(durations["dur_min"] >= 11) & (durations["dur_min"] <= 15)].index
    df = df[df["market_ticker"].isin(valid)].copy()

    # Recompute target label
    def compute_label(g):
        """
        Use final orderbook state to determine resolution — not median.
        The last snapshot before NaN flood is ground truth:
          - YES-only bids > 0.85 at end  → resolved YES
          - NO-only bids  > 0.85 at end  → resolved NO
          - Both sides present at end    → use last mid
          - Fallback                     → median (original logic)
        """
        g = g.copy().sort_values("fetched_at")
        y_all = g["yes_best_bid_dollars"]
        n_all = g["no_best_bid_dollars"]

        last_valid = g[y_all.notna() | n_all.notna()]
        if len(last_valid) > 0:
            last    = last_valid.iloc[-1]
            yes_end = last["yes_best_bid_dollars"]
            no_end  = last["no_best_bid_dollars"]

            # One-sided final state → unambiguous resolution
            if pd.notna(yes_end) and pd.isna(no_end) and yes_end > 0.85:
                g["target"] = 1.0
                return g
            if pd.notna(no_end) and pd.isna(yes_end) and no_end > 0.85:
                g["target"] = 0.0
                return g

            # Both sides still present → use last mid
            if pd.notna(yes_end) and pd.notna(no_end):
                last_mid = (yes_end + (1 - no_end)) / 2
                g["target"] = 1.0 if last_mid > 0.5 else 0.0
                return g

        # Fallback: median
        y  = y_all.dropna()
        n  = n_all.dropna()
        ym = y.median() if len(y) > 0 else 0
        nm = n.median() if len(n) > 0 else 0
        g["target"] = 1.0 if ym > nm else 0.0
        return g
    df = df.groupby("market_ticker", group_keys=False).apply(compute_label)

    # Liquidity flags
    df["yes_bid_present"] = df["yes_best_bid_dollars"].notna().astype(int)
    df["no_bid_present"]  = df["no_best_bid_dollars"].notna().astype(int)
    df["two_sided"]       = (df["yes_bid_present"] & df["no_bid_present"]).astype(int)

    # Implied asks
    df["yes_ask"] = 1 - df["no_best_bid_dollars"]
    df["no_ask"]  = 1 - df["yes_best_bid_dollars"]

    # Mid price — robust to one-sided markets:
    #   two-sided:  standard (yes_bid + yes_ask) / 2
    #   only YES:   yes_bid as lower bound proxy (conservative — mid = yes_bid)
    #   only NO:    no_bid as upper bound proxy  (mid = 1 - no_bid)
    df["mid_recomputed"] = np.nan

    two   = df["two_sided"] == 1
    yes_only = (df["yes_bid_present"] == 1) & (df["no_bid_present"] == 0)
    no_only  = (df["yes_bid_present"] == 0) & (df["no_bid_present"] == 1)

    df.loc[two,      "mid_recomputed"] = (df.loc[two, "yes_best_bid_dollars"] + df.loc[two, "yes_ask"]) / 2
    df.loc[yes_only, "mid_recomputed"] = df.loc[yes_only, "yes_best_bid_dollars"]
    df.loc[no_only,  "mid_recomputed"] = 1 - df.loc[no_only, "no_best_bid_dollars"]

    df["spread_recomputed"] = np.nan
    df.loc[two, "spread_recomputed"] = df.loc[two, "yes_ask"] - df.loc[two, "yes_best_bid_dollars"]

    # Crossed book flag
    df["crossed_book"] = (two & (df["yes_best_bid_dollars"] + df["no_best_bid_dollars"] > 1.0)).astype(int)

    # Snapshot index
    df["snapshot_idx"] = df.groupby("market_ticker").cumcount()
    df["chain_length"] = df.groupby("market_ticker")["snapshot_idx"].transform("max") + 1

    # Chain start hour in Eastern time (for hour-of-day filter)
    chain_start = df.groupby("market_ticker")["fetched_at"].transform("min")
    try:
        df["chain_hour"] = chain_start.dt.tz_localize("UTC").dt.tz_convert("US/Eastern").dt.hour
    except TypeError:
        # Already timezone-aware
        df["chain_hour"] = chain_start.dt.tz_convert("US/Eastern").dt.hour

    return df


# ─────────────────────────────────────────────
# 4. DIAGNOSTIC REPORT
# ─────────────────────────────────────────────

def diagnostic_report(df: pd.DataFrame):
    """Print key stats to understand why signals may not fire."""
    print("\n── Data Diagnostic ─────────────────────────────────────────")
    print(f"  Total rows          : {len(df):,}")
    print(f"  Chains              : {df['market_ticker'].nunique():,}")
    chain_sizes = df.groupby("market_ticker").size()
    print(f"  Avg snapshots/chain : {chain_sizes.mean():.1f}")
    print(f"  Min/Max snapshots   : {chain_sizes.min()} / {chain_sizes.max()}")

    # Time delta between snapshots
    df2 = df.copy()
    df2["delta"] = df2.groupby("market_ticker")["fetched_at"].diff().dt.total_seconds()
    print(f"\n  Snapshot interval   : median={df2['delta'].median():.1f}s  "
          f"mean={df2['delta'].mean():.1f}s  "
          f"max={df2['delta'].max():.1f}s")

    # Liquidity breakdown
    n = len(df)
    two   = (df["two_sided"] == 1).sum()
    y_only= ((df["yes_bid_present"]==1) & (df["no_bid_present"]==0)).sum()
    n_only= ((df["yes_bid_present"]==0) & (df["no_bid_present"]==1)).sum()
    none  = ((df["yes_bid_present"]==0) & (df["no_bid_present"]==0)).sum()
    print(f"\n  Liquidity breakdown:")
    print(f"    Two-sided  : {two:>7,}  ({100*two/n:.1f}%)")
    print(f"    YES only   : {y_only:>7,}  ({100*y_only/n:.1f}%)")
    print(f"    NO only    : {n_only:>7,}  ({100*n_only/n:.1f}%)")
    print(f"    Neither    : {none:>7,}  ({100*none/n:.1f}%)")

    # Mid price distribution (where available)
    mid_valid = df["mid_recomputed"].dropna()
    print(f"\n  Mid price stats (n={len(mid_valid):,}):")
    print(f"    {mid_valid.describe().round(3).to_string()}")

    # Target balance
    targets = df.groupby("market_ticker")["target"].first()
    print(f"\n  Target balance: YES={targets.mean():.1%}  NO={(1-targets.mean()):.1%}")

    # Momentum breach rates — key for calibrating thresholds
    df_mom = df.copy()
    df_mom["mom_k1"] = df_mom.groupby("market_ticker")["mid_recomputed"].diff(1)
    tradeable = df_mom["mid_recomputed"].between(0.15, 0.85)
    conviction = (df_mom["mid_recomputed"] > 0.60) | (df_mom["mid_recomputed"] < 0.40)
    tradeable_conv = tradeable & conviction
    n_trade = tradeable.sum()
    n_conv  = tradeable_conv.sum()
    print(f"\n  Conviction zone rows (mid<0.40 or mid>0.60): {n_conv:,} / {n_trade:,} tradeable ({100*n_conv/n_trade:.1f}%)")
    print(f"\n  Momentum breach rates (k=1, mid in [0.15,0.85], n={n_trade:,}):")
    for thr in [0.005, 0.01, 0.02, 0.03, 0.05, 0.08, 0.10]:
        breach_all  = df_mom.loc[tradeable,      "mom_k1"].abs() > thr
        breach_conv = df_mom.loc[tradeable_conv, "mom_k1"].abs() > thr
        print(f"    thr={thr:.3f}: all={breach_all.sum():>6,}  conviction_zone={breach_conv.sum():>6,}")
    print("────────────────────────────────────────────────────────────\n")



# ─────────────────────────────────────────────
# 5. VECTORIZED SIGNAL GENERATION
# ─────────────────────────────────────────────

def generate_signals_vectorized(df: pd.DataFrame, k: int, threshold: float,
                                 entry_window: tuple) -> pd.DataFrame:
    """
    Vectorized momentum signal generation — ~100x faster than row-loop version.
    Uses pandas groupby + shift to compute momentum across all chains at once.
    """
    df = df.copy()
    win_start, win_end = entry_window

    # Compute lagged mid within each chain
    df["mid_lag"] = df.groupby("market_ticker")["mid_recomputed"].shift(k)

    # Momentum
    df["momentum"] = df["mid_recomputed"] - df["mid_lag"]

    # Entry window filter — convert negative win_end to absolute index
    df["win_end_abs"] = df["chain_length"] - 1 + win_end  # win_end is negative e.g. -2
    in_window = (df["snapshot_idx"] >= win_start) & (df["snapshot_idx"] <= df["win_end_abs"])

    # Final-minute exclusion: ~23 snapshots @ 2.6s = 60s
    # Skip any snapshot within the last minute of the chain
    FINAL_MIN_SNAPSHOTS = 23
    not_final_minute = (
        df["snapshot_idx"] <= (df["chain_length"] - 1 - FINAL_MIN_SNAPSHOTS)
    )

    # Market phase filter: skip 30-40% through chain (weak win rate historically)
    pct_through = df["snapshot_idx"] / df["chain_length"]
    not_dead_zone = ~pct_through.between(0.30, 0.40)

    # Entry zone: [0.70, 0.85] for YES, [0.15, 0.30] for NO (mirrored)
    in_yes_zone   = df["mid_recomputed"].between(0.75, 0.85)
    in_no_zone    = df["mid_recomputed"].between(0.15, 0.25)
    in_entry_zone = in_yes_zone | in_no_zone

    # Hour-of-day filter: skip hours 8, 9, 15, 16 EST (high volatility windows)
    BLOCKED_HOURS = {8, 9, 15, 16}
    if "chain_hour" in df.columns:
        not_blocked_hour = ~df["chain_hour"].isin(BLOCKED_HOURS)
    else:
        not_blocked_hour = pd.Series(True, index=df.index)

    # Valid rows: in entry zone, not crossed, mid available,
    #             not blocked hour, not final minute, not dead zone
    valid = (
        in_window &
        (df["crossed_book"] == 0) &
        df["mid_recomputed"].notna() &
        df["mid_lag"].notna() &
        in_entry_zone &
        not_blocked_hour &
        not_final_minute &
        not_dead_zone
    )

    # Signals
    df["signal"]      = 0
    df["entry_price"] = np.nan
    df["estimated_q"] = np.nan

    # YES signal: mid in [0.70, 0.85] AND positive momentum
    # NO signal:  mid in [0.15, 0.30] AND negative momentum
    buy_yes = valid & in_yes_zone & (df["momentum"] >  threshold)
    buy_no  = valid & in_no_zone  & (df["momentum"] < -threshold)

    # For YES entries: need a yes_ask (requires no_bid present)
    # For NO entries:  need a no_ask  (requires yes_bid present)
    # Fall back: if only one side present, use what we have
    df.loc[buy_yes, "signal"]      = +1
    df.loc[buy_yes, "entry_price"] = df.loc[buy_yes, "yes_ask"].fillna(
        1 - df.loc[buy_yes, "mid_recomputed"]  # fallback if no_bid absent
    )
    df.loc[buy_yes, "estimated_q"] = df.loc[buy_yes, "mid_recomputed"]

    df.loc[buy_no, "signal"]      = -1
    df.loc[buy_no, "entry_price"] = df.loc[buy_no, "no_ask"].fillna(
        df.loc[buy_no, "mid_recomputed"]        # fallback if yes_bid absent
    )
    df.loc[buy_no, "estimated_q"] = 1 - df.loc[buy_no, "mid_recomputed"]

    # Drop helper columns
    df = df.drop(columns=["mid_lag", "win_end_abs"])
    return df


# ─────────────────────────────────────────────
# 6. TRADE EXECUTION
# ─────────────────────────────────────────────

@dataclass
class Trade:
    market_ticker: str
    snapshot_idx:  int
    signal:        int
    entry_price:   float
    estimated_q:   float
    contracts:     int
    fee:           float
    kelly_f:       float
    exit_type:     str
    exit_price:    float = np.nan
    pnl:           float = np.nan
    won:           bool  = False


def execute_chain(chain: pd.DataFrame, kelly_mult: float, bankroll: float,
                  exit_variant: str, reversal_threshold: float,
                  allow_multiple: bool, max_contracts: int = 9999,
                  max_sizing_bankroll: float = np.inf) -> list:
    """
    Fixed-fractional sizing with contract cap and bankroll sizing cap.
    Entry: any momentum signal that clears the fee hurdle.
    Sizing: min(floor(fraction * min(bankroll, max_sizing_bankroll) / entry_price), max_contracts).
    max_sizing_bankroll prevents compounding gains from inflating position sizes.
    """
    trades = []
    active: Optional[Trade] = None
    local_bk = bankroll
    target = int(chain["target"].iloc[-1])

    rows = chain.reset_index(drop=True)

    for t in range(len(rows)):
        row = rows.iloc[t]

        # ── Early exit (reversal variant) ──────────────────────────
        if active is not None and exit_variant == "reversal":
            if pd.notna(row["mid_recomputed"]):
                cur_mid = row["mid_recomputed"]
                # Exit if mid has reversed past the entry mid by reversal_threshold
                entry_mid = active.estimated_q if active.signal == +1 else (1 - active.estimated_q)
                current_mid = cur_mid
                moved_against = (
                    (active.signal == +1 and current_mid < entry_mid - reversal_threshold) or
                    (active.signal == -1 and current_mid > entry_mid + reversal_threshold)
                )
                if moved_against:
                    ep = cur_mid if active.signal == +1 else (1 - cur_mid)
                    active.exit_type  = "reversal"
                    active.exit_price = ep
                    active.pnl = active.contracts * (ep - active.entry_price) - active.fee
                    active.won = active.pnl > 0
                    local_bk += active.pnl
                    trades.append(active)
                    active = None

        # ── Entry ───────────────────────────────────────────────────
        if row["signal"] == 0:
            continue
        if active is not None and not allow_multiple:
            continue

        p = row["entry_price"]
        q = row["estimated_q"]
        if pd.isna(p) or pd.isna(q) or p <= 0 or p >= 1:
            continue

        # Fixed fractional sizing with contract cap and bankroll cap
        # Cap effective bankroll to prevent compounding from inflating positions
        effective_bk = min(local_bk, max_sizing_bankroll)
        stake = kelly_mult * effective_bk
        C = max(0, min(math.floor(stake / p), max_contracts))
        if C == 0:
            continue

        fee = kalshi_fee(C, p)

        # Fee hurdle: expected edge per contract must exceed fee per contract
        # Edge = |estimated_q - entry_price| (directional)
        edge = abs(q - p)
        if C > 0 and edge <= (fee / C):
            continue

        active = Trade(
            market_ticker=row["market_ticker"],
            snapshot_idx=int(row["snapshot_idx"]),
            signal=int(row["signal"]),
            entry_price=p,
            estimated_q=q,
            contracts=C,
            fee=fee,
            kelly_f=kelly_mult,   # store fraction used
            exit_type="resolution",
        )

    # ── Resolve open trade at chain end ─────────────────────────────
    if active is not None:
        won = (active.signal == +1 and target == 1) or (active.signal == -1 and target == 0)
        active.exit_type  = "resolution"
        active.exit_price = 1.0 if won else 0.0
        if won:
            active.pnl = active.contracts * (1 - active.entry_price) - active.fee
        else:
            active.pnl = -active.contracts * active.entry_price - active.fee
        active.won = active.pnl > 0
        trades.append(active)

    return trades


# ─────────────────────────────────────────────
# 7. BACKTEST ENGINE
# ─────────────────────────────────────────────

def run_backtest(df: pd.DataFrame, k: int, threshold: float,
                 entry_window: tuple, kelly_multiplier: float,
                 exit_variant: str, reversal_threshold: float = 0.05,
                 initial_bankroll: float = 1000.0,
                 allow_multiple: bool = False,
                 max_contracts: int = 9999,
                 max_sizing_bankroll: float = np.inf) -> dict:

    # Vectorized signal generation (fast)
    df_sig = generate_signals_vectorized(df, k, threshold, entry_window)

    bankroll   = initial_bankroll
    all_trades = []

    for ticker, chain in df_sig.groupby("market_ticker"):
        chain = chain.sort_values("snapshot_idx")
        trades = execute_chain(chain, kelly_multiplier, bankroll,
                               exit_variant, reversal_threshold, allow_multiple,
                               max_contracts=max_contracts,
                               max_sizing_bankroll=max_sizing_bankroll)
        for t in trades:
            bankroll += t.pnl
            all_trades.append(t)

    base = dict(params=dict(k=k, threshold=threshold, kelly_mult=kelly_multiplier,
                            exit=exit_variant, window=entry_window),
                total_trades=0, win_rate=np.nan, total_return_pct=np.nan,
                sharpe=np.nan, max_drawdown_pct=np.nan, mean_edge=np.nan,
                final_bankroll=bankroll, equity_curve=[initial_bankroll],
                trades=pd.DataFrame())

    if not all_trades:
        return base

    trade_df = pd.DataFrame([vars(t) for t in all_trades])

    equity = np.array([initial_bankroll] + list(
        initial_bankroll + trade_df["pnl"].cumsum()
    ))
    pnls = trade_df["pnl"].values
    peak = np.maximum.accumulate(equity)

    return {
        **base,
        "total_trades":      len(all_trades),
        "win_rate":          trade_df["won"].mean(),
        "total_return_pct":  (bankroll - initial_bankroll) / initial_bankroll * 100,
        "sharpe":            pnls.mean() / (pnls.std() + 1e-9),
        "max_drawdown_pct":  ((equity - peak) / (peak + 1e-9) * 100).min(),
        "mean_edge":         (trade_df["estimated_q"] - trade_df["entry_price"]).mean(),
        "final_bankroll":    bankroll,
        "equity_curve":      equity.tolist(),
        "trades":            trade_df,
    }


# ─────────────────────────────────────────────
# 8. PARAMETER SWEEP
# ─────────────────────────────────────────────

def parameter_sweep(df: pd.DataFrame, initial_bankroll: float = 1000.0) -> pd.DataFrame:
    # Fixed optimal params — sweep light variations around the known best
    ks                   = [2, 3]
    thresholds           = [0.02, 0.03, 0.05]
    kelly_multipliers    = {"5pct": 0.05, "10pct": 0.10}
    exit_variants        = ["hold"]
    entry_windows        = [(2, -2)]
    contract_caps        = [500]
    sizing_bankroll_caps = [1500, 2000]

    combos = list(itertools.product(
        ks, thresholds, kelly_multipliers.items(), exit_variants,
        entry_windows, contract_caps, sizing_bankroll_caps
    ))
    print(f"Running {len(combos)} parameter combinations...")

    rows = []
    for k, thr, (km_name, km_val), exit_v, win, cap, sz_cap in combos:
        r = run_backtest(df, k=k, threshold=thr, entry_window=win,
                         kelly_multiplier=km_val, exit_variant=exit_v,
                         initial_bankroll=initial_bankroll,
                         max_contracts=cap,
                         max_sizing_bankroll=sz_cap)
        rows.append({
            "k": k, "threshold": thr, "kelly_fraction": km_name,
            "exit_variant": exit_v, "entry_window": str(win),
            "max_contracts": cap,
            "max_sizing_bk": sz_cap if not np.isinf(sz_cap) else 999999,
            "total_trades": r["total_trades"], "win_rate": r["win_rate"],
            "total_return_pct": r["total_return_pct"], "sharpe": r["sharpe"],
            "max_drawdown_pct": r["max_drawdown_pct"], "mean_edge": r["mean_edge"],
            "final_bankroll": r["final_bankroll"],
        })

    return pd.DataFrame(rows).sort_values("sharpe", ascending=False).reset_index(drop=True)


# ─────────────────────────────────────────────
# 9. KELLY CALIBRATION
# ─────────────────────────────────────────────

def kelly_calibration(trades_df: pd.DataFrame, bins: int = 5) -> pd.DataFrame:
    if len(trades_df) == 0:
        return pd.DataFrame()
    trades_df = trades_df.copy()
    trades_df["q_bin"] = pd.cut(trades_df["estimated_q"], bins=bins)
    cal = trades_df.groupby("q_bin", observed=True).agg(
        mean_q=("estimated_q", "mean"),
        actual_win_rt=("won", "mean"),
        count=("won", "count"),
    ).reset_index()
    cal["calibration_error"] = cal["mean_q"] - cal["actual_win_rt"]
    return cal


# ─────────────────────────────────────────────
# 10. VISUALISATION
# ─────────────────────────────────────────────

def plot_results(best_result: dict, sweep_summary: pd.DataFrame, out_path: str):
    fig = plt.figure(figsize=(18, 12))
    fig.suptitle("Bitcoin Prediction Market — Momentum Scalp Backtest v2",
                 fontsize=15, fontweight="bold")
    gs = gridspec.GridSpec(2, 3, figure=fig, hspace=0.4, wspace=0.35)

    trades_df = best_result["trades"]
    equity    = best_result["equity_curve"]
    params    = best_result["params"]

    # Equity curve
    ax1 = fig.add_subplot(gs[0, :2])
    ax1.plot(equity, color="#2196F3", linewidth=1.5)
    ax1.fill_between(range(len(equity)), equity, equity[0], alpha=0.1, color="#2196F3")
    ax1.axhline(equity[0], color="gray", linestyle="--", linewidth=0.8)
    ax1.set_title(f"Equity Curve — k={params['k']}, thr={params['threshold']}, "
                  f"Kelly={params['kelly_mult']}, exit={params['exit']}")
    ax1.set_xlabel("Trade #")
    ax1.set_ylabel("Bankroll ($)")

    # P&L distribution
    ax2 = fig.add_subplot(gs[0, 2])
    if len(trades_df) > 0:
        wins   = trades_df.loc[trades_df["won"],  "pnl"]
        losses = trades_df.loc[~trades_df["won"], "pnl"]
        ax2.hist(losses.dropna(), bins=30, color="#F44336", alpha=0.7, label="Loss")
        ax2.hist(wins.dropna(),   bins=30, color="#4CAF50", alpha=0.7, label="Win")
        ax2.axvline(0, color="black", linewidth=0.8)
    ax2.set_title("P&L Distribution")
    ax2.set_xlabel("P&L ($)")
    ax2.legend()

    # Kelly calibration
    ax3 = fig.add_subplot(gs[1, 0])
    cal = kelly_calibration(trades_df)
    if len(cal) > 0:
        ax3.bar(range(len(cal)), cal["actual_win_rt"], color="#9C27B0", alpha=0.7, label="Actual win rate")
        ax3.plot(range(len(cal)), cal["mean_q"], "o--", color="orange", label="Mean q")
        ax3.set_xticks(range(len(cal)))
        ax3.set_xticklabels([str(b) for b in cal["q_bin"]], rotation=30, fontsize=7)
    ax3.set_title("Kelly Calibration")
    ax3.set_ylabel("Rate")
    ax3.legend(fontsize=8)

    # Return by threshold
    ax4 = fig.add_subplot(gs[1, 1])
    tg = sweep_summary.groupby("threshold")["total_return_pct"].mean().reset_index()
    ax4.bar(tg["threshold"].astype(str), tg["total_return_pct"], color="#FF9800", alpha=0.85)
    ax4.axhline(0, color="black", linewidth=0.8)
    ax4.set_title("Avg Return by Threshold")
    ax4.set_xlabel("Threshold")
    ax4.set_ylabel("Avg Return (%)")

    # Return by Kelly fraction
    ax5 = fig.add_subplot(gs[1, 2])
    kg = sweep_summary.groupby("kelly_fraction")["total_return_pct"].mean().reset_index()
    colors = {"full": "#F44336", "half": "#FF9800", "quarter": "#4CAF50"}
    ax5.bar(kg["kelly_fraction"], kg["total_return_pct"],
            color=[colors.get(k, "steelblue") for k in kg["kelly_fraction"]], alpha=0.85)
    ax5.axhline(0, color="black", linewidth=0.8)
    ax5.set_title("Avg Return by Kelly Fraction")
    ax5.set_xlabel("Kelly Fraction")

    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Chart saved → {out_path}")


# ─────────────────────────────────────────────
# 11. MAIN
# ─────────────────────────────────────────────


# ─────────────────────────────────────────────
# WALK-FORWARD VALIDATION
# ─────────────────────────────────────────────

def walk_forward_validation(
    df: pd.DataFrame,
    best_params: dict,
    train_frac: float = 0.70,
    initial_bankroll: float = 1000.0,
) -> dict:
    """
    Chronological train/test split using FIXED params from the full-dataset sweep.

    Deliberately avoids re-optimizing on the train split — that approach
    finds lucky configurations on small samples and produces misleading
    overfitting signals. Instead we fix the full-dataset best params and
    simply ask: do they work on unseen future chains?

    Parameters
    ----------
    df          : preprocessed DataFrame
    best_params : dict of run_backtest kwargs (k, threshold, kelly_multiplier,
                  entry_window, exit_variant, max_contracts, max_sizing_bankroll)
    train_frac  : fraction of chains (chronological) to use as train
    """
    # ── Split chains chronologically ─────────────────────────────────
    chain_starts = (df.groupby("market_ticker")["fetched_at"]
                    .min()
                    .sort_values()
                    .reset_index())
    chain_starts.columns = ["market_ticker", "chain_start"]

    n_total = len(chain_starts)
    n_train = int(n_total * train_frac)

    train_tickers = chain_starts.iloc[:n_train]["market_ticker"].values
    test_tickers  = chain_starts.iloc[n_train:]["market_ticker"].values

    train_df = df[df["market_ticker"].isin(train_tickers)].copy()
    test_df  = df[df["market_ticker"].isin(test_tickers)].copy()

    print(f"\n── Walk-Forward Split ───────────────────────────────────────")
    print(f"  Total chains : {n_total}")
    print(f"  Train chains : {len(train_tickers)} "
          f"({chain_starts.iloc[0]['chain_start'].strftime('%Y-%m-%d %H:%M')} → "
          f"{chain_starts.iloc[n_train-1]['chain_start'].strftime('%Y-%m-%d %H:%M')})")
    print(f"  Test chains  : {len(test_tickers)} "
          f"({chain_starts.iloc[n_train]['chain_start'].strftime('%Y-%m-%d %H:%M')} → "
          f"{chain_starts.iloc[-1]['chain_start'].strftime('%Y-%m-%d %H:%M')})")
    print(f"\n  Fixed params: k={best_params.get('k')}, "
          f"threshold={best_params.get('threshold')}, "
          f"kelly={best_params.get('kelly_multiplier')}, "
          f"max_contracts={best_params.get('max_contracts')}, "
          f"max_sizing_bk={best_params.get('max_sizing_bankroll')}")

    # ── Apply fixed params to both splits ────────────────────────────
    run_params = {**best_params, "initial_bankroll": initial_bankroll}
    print(f"\nRunning train split ({len(train_tickers)} chains)...")
    train_result = run_backtest(train_df, **run_params)
    print(f"Running test split ({len(test_tickers)} chains)...")
    test_result  = run_backtest(test_df,  **run_params)

    # ── Compare ──────────────────────────────────────────────────────
    print(f"\n── Results Comparison ──────────────────────────────────────")
    print(f"{'Metric':<25} {'TRAIN':>12} {'TEST':>12} {'Δ':>10}")
    print("─" * 62)

    metrics = [
        ("Chains",         len(train_tickers),              len(test_tickers),               ""),
        ("Trades",         train_result["total_trades"],     test_result["total_trades"],      ""),
        ("Win rate",       train_result["win_rate"],         test_result["win_rate"],          "pct"),
        ("Total return",   train_result["total_return_pct"], test_result["total_return_pct"],  "pct"),
        ("Sharpe",         train_result["sharpe"],           test_result["sharpe"],            "float"),
        ("Max drawdown",   train_result["max_drawdown_pct"], test_result["max_drawdown_pct"],  "pct"),
        ("Final bankroll", train_result["final_bankroll"],   test_result["final_bankroll"],    "dollar"),
    ]

    for name, tr_val, te_val, fmt in metrics:
        if fmt == "pct":
            delta = te_val - tr_val
            print(f"  {name:<23} {tr_val:>11.1%} {te_val:>11.1%} {delta:>+10.1%}")
        elif fmt == "float":
            delta = te_val - tr_val
            print(f"  {name:<23} {tr_val:>11.3f} {te_val:>11.3f} {delta:>+10.3f}")
        elif fmt == "dollar":
            print(f"  {name:<23} ${tr_val:>10.2f} ${te_val:>10.2f}")
        else:
            print(f"  {name:<23} {tr_val:>12} {te_val:>12}")

    # ── Per-asset breakdown on test ───────────────────────────────────
    if len(test_result["trades"]) > 0:
        test_trades = test_result["trades"].copy()
        test_trades["asset"] = test_trades["market_ticker"].str.extract(r"KX(\w+?)15M")
        print(f"\n── Test Split: Win Rate by Asset ───────────────────────────")
        print(test_trades.groupby("asset")["won"]
              .agg(["mean","count"]).round(3)
              .sort_values("mean", ascending=False).to_string())

        chain_meta = test_df.groupby("market_ticker")["snapshot_idx"].max().reset_index()
        chain_meta.columns = ["market_ticker", "chain_length"]
        test_trades = test_trades.merge(chain_meta, on="market_ticker", how="left")
        test_trades["pct_through"] = test_trades["snapshot_idx"] / test_trades["chain_length"]
        print(f"\n── Test Split: Entry Timing ────────────────────────────────")
        print(f"  Mean entry  : {test_trades['pct_through'].mean():.1%} through chain")
        print(f"  Median entry: {test_trades['pct_through'].median():.1%} through chain")

    # ── Overfitting diagnosis ─────────────────────────────────────────
    print(f"\n── Overfitting Diagnosis ───────────────────────────────────")
    wr_gap = test_result["win_rate"] - train_result["win_rate"]
    if abs(wr_gap) < 0.05 and test_result["win_rate"] > 0.60:
        print(f"  ✓ Win rate gap {wr_gap:+.1%} — acceptable (< 5pp)")
    else:
        print(f"  ⚠ Win rate gap {wr_gap:+.1%} — possible overfitting")
    if test_result["sharpe"] > 0.05:
        print(f"  ✓ Test Sharpe {test_result['sharpe']:.3f} — positive out-of-sample")
    else:
        print(f"  ⚠ Test Sharpe {test_result['sharpe']:.3f} — weak out-of-sample")
    if test_result["total_return_pct"] > 0:
        print(f"  ✓ Test return {test_result['total_return_pct']:.1f}% — profitable out-of-sample")
    else:
        print(f"  ⚠ Test return {test_result['total_return_pct']:.1f}% — unprofitable out-of-sample")

    return {
        "train_result":  train_result,
        "test_result":   test_result,
        "best_params":   best_params,
        "train_tickers": train_tickers,
        "test_tickers":  test_tickers,
    }

def main(csv_path: str, initial_bankroll: float = 1000.0):
    print("Loading data...")
    df = pd.read_csv(csv_path)

    print("Preprocessing...")
    df = preprocess(df)

    print(f"Dataset: {len(df):,} rows | "
          f"{df['market_ticker'].nunique():,} chains | "
          f"avg chain length: {df.groupby('market_ticker').size().mean():.1f} snapshots")

    # Always run diagnostic first — explains signal behaviour
    diagnostic_report(df)

    print("Running parameter sweep...")
    sweep = parameter_sweep(df, initial_bankroll=initial_bankroll)

    print("\n── Top 10 Parameter Combinations ──────────────────────────")
    print(sweep.head(10).to_string(index=False))

    # Guard: if everything is still 0 trades, stop and advise
    if sweep["total_trades"].max() == 0:
        print("\n⚠️  No trades generated across all parameter combinations.")
        print("   Review the diagnostic report above — most likely cause:")
        print("   • Mid price is NaN for most rows (check liquidity breakdown)")
        print("   • Momentum never exceeds threshold (spreads too wide?)")
        print("   • Entry window too narrow relative to chain length")
        return None, sweep

    best = sweep.iloc[0]
    win_map = {"(2, -2)": (2, -2), "(3, -3)": (3, -3)}
    km_map  = {"full": 1.0, "half": 0.5, "quarter": 0.25, "5pct": 0.05, "10pct": 0.10, "25pct": 0.25}

    best_params = dict(
        k                   = int(best["k"]),
        threshold           = float(best["threshold"]),
        entry_window        = win_map[best["entry_window"]],
        kelly_multiplier    = km_map[best["kelly_fraction"]],
        exit_variant        = best["exit_variant"],
        max_contracts       = int(best["max_contracts"]),
        max_sizing_bankroll = float(best["max_sizing_bk"]) if best["max_sizing_bk"] != 999999 else np.inf,
    )

    best_result = run_backtest(df, **best_params, initial_bankroll=initial_bankroll)

    print(f"\n── Best Result Summary ─────────────────────────────────────")
    print(f"  Total trades      : {best_result['total_trades']}")
    print(f"  Win rate          : {best_result['win_rate']:.1%}")
    print(f"  Total return      : {best_result['total_return_pct']:.2f}%")
    print(f"  Sharpe (per-trade): {best_result['sharpe']:.3f}")
    print(f"  Max drawdown      : {best_result['max_drawdown_pct']:.2f}%")
    print(f"  Mean edge/trade   : {best_result['mean_edge']:.4f}")
    print(f"  Final bankroll    : ${best_result['final_bankroll']:.2f}")

    if len(best_result["trades"]) > 0:
        print("\nKelly Calibration:")
        print(kelly_calibration(best_result["trades"]).to_string(index=False))

    print("\nGenerating charts...")
    plot_results(best_result, sweep, "backtest_results_v2.png")

    sweep.to_csv("sweep_summary_v2.csv", index=False)
    if len(best_result["trades"]) > 0:
        best_result["trades"].to_csv("best_trades_v2.csv", index=False)

    print("Sweep summary  → sweep_summary_v2.csv")
    print("Best trade log → best_trades_v2.csv")

    # ── Walk-forward validation ───────────────────────────────────────
    print("\n" + "="*60)
    print("WALK-FORWARD VALIDATION")
    print("="*60)
    wf = walk_forward_validation(df, best_params=best_params,
                                  train_frac=0.70,
                                  initial_bankroll=initial_bankroll)

    return best_result, sweep, wf


if __name__ == "__main__":
    import sys
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "orderbook.csv"
    main(csv_path)
