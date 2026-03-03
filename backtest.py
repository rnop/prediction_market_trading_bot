# ─────────────────────────────────────────────
# BTC/ETH/SOL/XRP Prediction Market — Momentum Signal Backtest
# OBFUSCATED VERSION
# ─────────────────────────────────────────────

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


MAX_KELLY_FRACTION = 0.25  

def contracts_from_kelly(f, kelly_mult, bankroll, entry_price):
    effective_f = min(f * kelly_mult, MAX_KELLY_FRACTION)
    stake = effective_f * bankroll
    if stake <= 0 or entry_price <= 0:
        return 0
    return max(0, math.floor(stake / entry_price))


# ─────────────────────────────────────────────
# 3. PREPROCESSING - OBFUSCATED 
# ─────────────────────────────────────────────

def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    return df


# ─────────────────────────────────────────────
# 4. DIAGNOSTIC REPORT - MOMENTUM SIGNAL OBFUSCATED
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

    # Mid price distribution
    mid_valid = df["mid_recomputed"].dropna()
    print(f"\n  Mid price stats (n={len(mid_valid):,}):")
    print(f"    {mid_valid.describe().round(3).to_string()}")

    # Target balance
    targets = df.groupby("market_ticker")["target"].first()
    print(f"\n  Target balance: YES={targets.mean():.1%}  NO={(1-targets.mean()):.1%}")

    # Momentum signal rates - OBFUSCATED
    print("────────────────────────────────────────────────────────────\n")



# ─────────────────────────────────────────────
# 5. VECTORIZED SIGNAL GENERATION - OBFUSCATED
# ─────────────────────────────────────────────

def generate_signals_vectorized(df: pd.DataFrame, k: int, threshold: float,
                                 entry_window: tuple) -> pd.DataFrame:
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

    # Signal generation
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
# 10. VISUALIZATION
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
# 11. MAIN WALK-FORWARD VALIDATION
# ─────────────────────────────────────────────

def walk_forward_validation(
    df: pd.DataFrame,
    best_params: dict,
    train_frac: float = 0.70,
    initial_bankroll: float = 1000.0,
) -> dict:
    """
    Chronological train/test split using FIXED params from the full-dataset parameter sweep.

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

    # Always run diagnostic first
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
