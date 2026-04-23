"""End-to-end pipeline for the pilot.

Run with:
    python -m src.run_pilot

Reads data/tokens.csv, fetches histories from CoinGecko, computes per-token
summary statistics, writes data/panel.csv, and saves the two headline figures
under figures/. If tokenomist CSV exports are present in data/tokenomist_csv/,
also writes data/unlock_summary.csv and a third figure.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src import data_fetch, measures, plots


def main() -> None:
    Path("data").mkdir(exist_ok=True)
    Path("figures").mkdir(exist_ok=True)

    print("== loading universe ==")
    universe = data_fetch.load_universe("data/tokens.csv")
    print(f"  {len(universe)} tokens in universe")

    print("\n== fetching histories from CoinGecko ==")
    # Pilot runs on the CoinGecko free Demo tier (365-day cap). The full
    # project will pull from January 2022 onward under the grant budget,
    # which justifies paying for Analyst-tier API access. If COINGECKO_API_KEY
    # is set, the fetcher will automatically pull the full requested window.
    histories = data_fetch.fetch_all(universe, days=365)
    print(f"\n  successfully fetched {len(histories)} / {len(universe)} tokens")

    if not histories:
        print("no histories fetched; aborting.")
        return

    print("\n== computing per-token summary statistics ==")
    summary = measures.summarize(histories)
    summary.to_csv("data/panel.csv", index=False)
    print(f"  wrote data/panel.csv ({len(summary)} rows)")
    print("\n  head of summary:")
    print(summary.head(10).to_string(index=False))

    print("\n== generating figures ==")
    plots.fig1_amihud_vs_mcap(summary, "figures/fig1_amihud_vs_mcap.png")
    plots.fig2_days_to_liquidate_by_decile(summary, "figures/fig2_days_to_liquidate_by_decile.png")

    print("\n== (optional) tokenomist API: unlock events and supply breakdown ==")
    try:
        from src import tokenomist
    except Exception as e:
        print(f"  ! could not import tokenomist client: {e}")
        tokenomist = None

    if tokenomist is not None:
        try:
            tokens_df = tokenomist.list_tokens()
        except RuntimeError as e:
            print(f"  ! {e}")
            tokens_df = None

        if tokens_df is not None and not tokens_df.empty:
            tier = tokenomist.detect_tier(tokens_df)
            print(f"  tokenomist coverage: {len(tokens_df)} tokens ({tier})")

            # Save the supply-side breakdown
            supply_df = tokenomist.supply_breakdown(tokens_df)
            supply_df.to_csv("data/tokenomist_supply.csv", index=False)
            print(f"  wrote data/tokenomist_supply.csv")

            # Match against the CoinGecko universe by symbol (case-insensitive)
            cg_symbols = {s.upper() for s in summary["symbol"].dropna()}
            matched = tokens_df[tokens_df["symbol"].str.upper().isin(cg_symbols)]
            print(f"  intersection with CoinGecko universe: {len(matched)} tokens")

            if not matched.empty:
                # Pull unlock events for the intersection
                events_df = tokenomist.fetch_all_unlock_events(matched["id"].tolist(), verbose=True)
                if not events_df.empty:
                    events_df.to_csv("data/tokenomist_unlock_events.csv", index=False)
                    print(f"  wrote data/tokenomist_unlock_events.csv ({len(events_df)} rows)")

                    nxt = tokenomist.next_unlock_per_token(events_df)
                    nxt.to_csv("data/tokenomist_next_unlock.csv", index=False)
                    print(f"  wrote data/tokenomist_next_unlock.csv ({len(nxt)} tokens with future unlocks)")

                    plots.fig3_unlock_calendar(events_df, "figures/fig3_unlock_calendar.png")
                    plots.fig4_locked_share(supply_df[supply_df["symbol"].str.upper().isin(cg_symbols)],
                                            "figures/fig4_locked_share.png")

    print("\n== done. ==")
    print("\nReview figures/ and data/panel.csv, then commit the repo.")


if __name__ == "__main__":
    main()
