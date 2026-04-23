"""Measurement functions for the pilot.

All functions operate on a token's daily DataFrame with columns:
  date, price_usd, volume_usd, mcap_usd
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def _tail(df: pd.DataFrame, window: int) -> pd.DataFrame:
    return df.sort_values("date").tail(window)


def amihud_illiquidity(df: pd.DataFrame, window: int = 90) -> float:
    """Mean |daily return| / daily dollar volume over the last `window` days.

    Amihud (2002). Higher = less liquid. We scale by 1e6 to avoid printing
    tiny floating-point numbers in the output; the scaling is cosmetic.
    """
    sub = _tail(df, window + 1).copy()
    sub["ret"] = sub["price_usd"].pct_change()
    sub = sub.dropna(subset=["ret", "volume_usd"])
    sub = sub[sub["volume_usd"] > 0]
    if len(sub) < 10:
        return np.nan
    return (sub["ret"].abs() / sub["volume_usd"]).mean() * 1e6


def days_to_liquidate(df: pd.DataFrame, mcap_fraction: float = 0.01, window: int = 90) -> float:
    """Days of trading required to move `mcap_fraction` of reported mcap.

    Uses median daily dollar volume over the last `window` days as the
    denominator, and the latest reported market cap as the numerator.
    """
    sub = _tail(df, window)
    if sub.empty or sub["volume_usd"].median() <= 0:
        return np.nan
    latest_mcap = sub["mcap_usd"].dropna().iloc[-1] if sub["mcap_usd"].notna().any() else np.nan
    if not np.isfinite(latest_mcap) or latest_mcap <= 0:
        return np.nan
    return (latest_mcap * mcap_fraction) / sub["volume_usd"].median()


def realized_vol_annualized(df: pd.DataFrame, window: int = 90) -> float:
    """Annualized std dev of daily log returns over the last `window` days."""
    sub = _tail(df, window + 1).copy()
    sub["logret"] = np.log(sub["price_usd"]).diff()
    sub = sub.dropna(subset=["logret"])
    if len(sub) < 10:
        return np.nan
    return sub["logret"].std() * np.sqrt(365)


def max_drawdown(df: pd.DataFrame, window: int = 90) -> float:
    """Max peak-to-trough drawdown over the last `window` days, as a negative number."""
    sub = _tail(df, window)
    if sub.empty:
        return np.nan
    peak = sub["price_usd"].cummax()
    dd = (sub["price_usd"] / peak) - 1.0
    return dd.min()


def latest_mcap(df: pd.DataFrame) -> float:
    s = df["mcap_usd"].dropna()
    return float(s.iloc[-1]) if not s.empty else np.nan


def median_volume(df: pd.DataFrame, window: int = 90) -> float:
    sub = _tail(df, window)
    return float(sub["volume_usd"].median()) if not sub.empty else np.nan


def summarize(histories, min_mcap_usd: float = 1e5) -> pd.DataFrame:
    """Build the per-token summary table used by plots and the panel output.

    Tokens with reported market cap below `min_mcap_usd` are dropped. CoinGecko
    occasionally returns 0 or stale-near-zero mcap values for tokens that have
    been delisted, rebranded, or had their tracking suspended (e.g., MKR after
    the Sky rebrand). These are not informative for the descriptive analysis.
    """
    rows = []
    for h in histories:
        rows.append(
            {
                "coingecko_id": h.coingecko_id,
                "symbol": h.symbol,
                "mcap_usd": latest_mcap(h.df),
                "median_daily_volume_usd": median_volume(h.df, window=90),
                "amihud_illiq": amihud_illiquidity(h.df, window=90),
                "days_to_liquidate_1pct": days_to_liquidate(h.df, mcap_fraction=0.01, window=90),
                "realized_vol_ann_90d": realized_vol_annualized(h.df, window=90),
                "max_drawdown_90d": max_drawdown(h.df, window=90),
                "n_obs": len(h.df),
            }
        )
    df = pd.DataFrame(rows)
    n_before = len(df)
    df = df[df["mcap_usd"].fillna(0) >= min_mcap_usd]
    df = df.dropna(subset=["amihud_illiq", "days_to_liquidate_1pct"])
    n_after = len(df)
    if n_after < n_before:
        print(f"  filtered {n_before - n_after} tokens with mcap < ${min_mcap_usd:,.0f} or missing measures")
    return df.sort_values("mcap_usd", ascending=False).reset_index(drop=True)
