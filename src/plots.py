"""Descriptive figures for the pilot.

Two headline plots:
  1. Amihud illiquidity vs. reported market cap (log-log scatter).
     Tokens far above the trend are "fake-mcap" suspects: reported mcap
     is larger than their observed liquidity supports.
  2. Days-to-liquidate-1% by market-cap decile.
     If reported mcap were well-supported by trading depth, days-to-liquidate
     should scale roughly with mcap. If mid-cap tokens have disproportionately
     large days-to-liquidate, that is the quantitative footprint of the
     "fake $100M" pattern.

Optional third plot: days-to-next-unlock distribution, as a feasibility
check that the full project's event-study design has adequate coverage
for the sample universe.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

plt.rcParams.update(
    {
        "figure.figsize": (8, 5.5),
        "figure.dpi": 110,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.alpha": 0.25,
        "font.size": 10,
    }
)


def fig1_amihud_vs_mcap(summary: pd.DataFrame, out_path: str | Path) -> None:
    sub = summary.dropna(subset=["amihud_illiq", "mcap_usd"]).copy()
    sub = sub[(sub["amihud_illiq"] > 0) & (sub["mcap_usd"] > 0)]
    if sub.empty:
        print("  ! fig1: no valid rows")
        return

    x = np.log10(sub["mcap_usd"].to_numpy())
    y = np.log10(sub["amihud_illiq"].to_numpy())

    # OLS trend
    b1, b0 = np.polyfit(x, y, 1)
    resid = y - (b0 + b1 * x)
    sub = sub.assign(_resid=resid)
    # Flag top 10% most-illiquid-for-their-size as suspects
    k = max(1, int(0.10 * len(sub)))
    suspects = sub.nlargest(k, "_resid")

    fig, ax = plt.subplots()
    ax.scatter(x, y, alpha=0.55, s=26, color="#4a5d7a", label="tokens")
    xs = np.linspace(x.min(), x.max(), 50)
    ax.plot(xs, b0 + b1 * xs, color="#b8462c", linewidth=1.3, label=f"OLS fit (slope={b1:.2f})")

    # Label suspects
    for _, r in suspects.iterrows():
        ax.annotate(
            r["symbol"],
            (np.log10(r["mcap_usd"]), np.log10(r["amihud_illiq"])),
            xytext=(4, 4),
            textcoords="offset points",
            fontsize=8,
            color="#b8462c",
        )

    ax.set_xlabel("log10 reported market cap (USD)")
    ax.set_ylabel("log10 Amihud illiquidity  (higher = less liquid)")
    ax.set_title("Liquidity vs. reported market cap\nOutliers above the line = mcap not supported by observed liquidity")
    ax.legend(loc="lower left", frameon=False)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out_path}")


def fig2_days_to_liquidate_by_decile(summary: pd.DataFrame, out_path: str | Path) -> None:
    """Scatter of days-to-liquidate vs. market cap on log-log axes.

    Replaces an earlier decile boxplot, which was visually dominated by one
    extreme small-cap outlier and obscured the cross-sectional structure.
    The scatter form makes the long-tail story visible directly: tokens with
    days-to-liquidate >> 1 are mechanical-mcap suspects, regardless of their
    decile bucket.
    """
    sub = summary.dropna(subset=["days_to_liquidate_1pct", "mcap_usd"]).copy()
    sub = sub[(sub["days_to_liquidate_1pct"] > 0) & (sub["mcap_usd"] > 0)]
    if len(sub) < 10:
        print("  ! fig2: too few valid rows")
        return

    x = np.log10(sub["mcap_usd"].to_numpy())
    y = np.log10(sub["days_to_liquidate_1pct"].to_numpy())

    # OLS reference trend
    b1, b0 = np.polyfit(x, y, 1)

    fig, ax = plt.subplots()
    ax.scatter(x, y, alpha=0.55, s=26, color="#4a5d7a", label="tokens")
    xs = np.linspace(x.min(), x.max(), 50)
    ax.plot(xs, b0 + b1 * xs, color="#b8462c", linewidth=1.3, label=f"OLS fit (slope={b1:.2f})")

    # Reference: 1 day mark and 30 day mark
    ax.axhline(0, color="#888", linestyle=":", linewidth=0.8, alpha=0.7)
    ax.text(x.min() + 0.05, 0.05, "1 day", fontsize=8, color="#888")
    ax.axhline(np.log10(30), color="#888", linestyle=":", linewidth=0.8, alpha=0.7)
    ax.text(x.min() + 0.05, np.log10(30) + 0.05, "30 days", fontsize=8, color="#888")

    # Label the worst suspects: top 8 by days-to-liquidate
    suspects = sub.nlargest(8, "days_to_liquidate_1pct")
    for _, r in suspects.iterrows():
        ax.annotate(
            r["symbol"],
            (np.log10(r["mcap_usd"]), np.log10(r["days_to_liquidate_1pct"])),
            xytext=(4, 4),
            textcoords="offset points",
            fontsize=8,
            color="#b8462c",
        )

    ax.set_xlabel("log10 reported market cap (USD)")
    ax.set_ylabel("log10 days to liquidate 1% of mcap\nat median daily volume")
    ax.set_title("Time to liquidate 1% of reported market cap\nAnything above the dotted line takes >1 day; suspects labeled")
    ax.legend(loc="lower left", frameon=False)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out_path}")


def fig3_unlock_calendar(events_df: pd.DataFrame, out_path: str | Path, lookback_days: int = 365) -> None:
    """Histogram of past unlock events over the last `lookback_days`, weighted by USD value.

    This is the feasibility check for the full project's event-study design:
    if the histogram is dense, the proposed identification strategy has
    adequate event coverage in the sample. We deliberately use historical
    events because (i) the equity-IPO-lockup-style event study requires
    realized post-event returns, and (ii) tokenomist's free trial returns
    only historical data; future events require a paid tier.
    """
    if events_df.empty:
        print("  (fig3 skipped: no tokenomist data)")
        return

    today = pd.Timestamp.utcnow().tz_localize(None).normalize()
    cutoff = today - pd.Timedelta(days=lookback_days)
    sub = events_df[(events_df["unlockDate"] >= cutoff) & (events_df["unlockDate"] <= today)].copy()
    if sub.empty:
        print(f"  (fig3 skipped: no unlocks in last {lookback_days} days)")
        return

    # Bucket by week
    sub["week"] = sub["unlockDate"].dt.to_period("W").dt.start_time
    weekly = sub.groupby("week").agg(
        n_events=("tokenSymbol", "nunique"),
        total_value_usd=("cliffValue", "sum"),
    ).reset_index()

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9, 6), sharex=True)
    ax1.bar(weekly["week"], weekly["n_events"], width=5, color="#4a5d7a", alpha=0.8, edgecolor="white")
    ax1.set_ylabel("# unique tokens with\nunlocks this week")
    ax1.set_title(
        f"Unlock event calendar over last {lookback_days} days  "
        f"(n={sub['tokenSymbol'].nunique()} tokens, {len(sub)} (event × allocation) rows)"
    )
    ax2.bar(weekly["week"], weekly["total_value_usd"] / 1e6, width=5, color="#b8462c", alpha=0.8, edgecolor="white")
    ax2.set_ylabel("total unlock value\nthis week ($M)")
    ax2.set_xlabel("week")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out_path}")


def fig4_locked_share(supply_df: pd.DataFrame, out_path: str | Path) -> None:
    """Sorted horizontal bar chart of locked supply share, with token symbols labeled.

    Histograms are too chunky for small panels (N=15-20). A sorted bar chart
    surfaces both the cross-sectional spread and the token-level identification
    of which projects have the most overhang to work through.
    """
    if supply_df.empty or "locked_share_of_trackable" not in supply_df.columns:
        print("  (fig4 skipped: no supply-side data)")
        return
    sub = supply_df.dropna(subset=["locked_share_of_trackable"]).copy()
    if sub.empty:
        print("  (fig4 skipped: no valid locked-share rows)")
        return

    sub = sub.sort_values("locked_share_of_trackable", ascending=True)
    pct = sub["locked_share_of_trackable"] * 100
    n = len(sub)

    # Color: red for high overhang, blue for low. Threshold at 25% locked.
    colors = ["#b8462c" if p >= 25 else "#4a5d7a" for p in pct]

    fig, ax = plt.subplots(figsize=(8, max(4, 0.3 * n)))
    bars = ax.barh(sub["symbol"].astype(str), pct, color=colors, alpha=0.8, edgecolor="white")

    # Label each bar with its percentage
    for bar, p in zip(bars, pct):
        ax.text(p + 0.5, bar.get_y() + bar.get_height() / 2, f"{p:.1f}%",
                va="center", fontsize=8, color="#444")

    ax.axvline(25, color="#888", linestyle=":", linewidth=0.8, alpha=0.7)
    ax.set_xlabel("currently locked share of trackable supply (%)")
    ax.set_title(f"Supply overhang by token (n={n}, tokenomist coverage)\nRed = ≥25% of supply still locked")
    ax.set_xlim(0, max(pct.max() * 1.15, 50))
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out_path}")


def fig5_cumulative_dilution(dilution_df: "pd.DataFrame", out_path: str | Path,
                              lookback_days: int = 365) -> None:
    """Cumulative supply released per token over the lookback window, expressed
    as a share of contemporaneous market cap. Shown as a horizontal bar chart
    with the 100%-of-mcap reference line marked.
    """
    if dilution_df.empty:
        print("  (fig5 skipped: no dilution data)")
        return
    sub = dilution_df.sort_values("cumulative_dilution_pct", ascending=True).copy()
    n = len(sub)

    # Color: red where cumulative dilution exceeded 100% of mcap
    colors = ["#b8462c" if p >= 100 else "#4a5d7a" for p in sub["cumulative_dilution_pct"]]

    fig, ax = plt.subplots(figsize=(8, max(3, 0.45 * n)))
    bars = ax.barh(sub["tokenSymbol"].astype(str), sub["cumulative_dilution_pct"],
                    color=colors, alpha=0.85, edgecolor="white")
    for bar, p, n_ev in zip(bars, sub["cumulative_dilution_pct"], sub["n_events"]):
        ax.text(p + 2, bar.get_y() + bar.get_height() / 2,
                f"{p:.0f}%  ({int(n_ev)} events)",
                va="center", fontsize=9, color="#444")

    ax.axvline(100, color="#888", linestyle=":", linewidth=0.9, alpha=0.7)
    ax.text(100, -0.6, "100% of mcap", fontsize=8, color="#888")
    ax.set_xlabel(f"cumulative supply released over last {lookback_days} days,\n"
                   "summed value-to-mcap across events (%)")
    ax.set_title(
        f"Supply released as share of market cap, last {lookback_days} days  "
        f"(n={n} tokens)"
    )
    ax.set_xlim(0, max(sub["cumulative_dilution_pct"].max() * 1.25, 110))
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out_path}")
