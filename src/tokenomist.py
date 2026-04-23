"""Tokenomist API client.

Two endpoints we use:
  - GET /v4/token/list  → list every token tokenomist covers, with their
    internal id, circulating supply, locked/unlocked amounts, etc.
  - GET /v4/unlock/events?tokenId=<id>  → scheduled cliff unlock events for
    that token, with date, dollar value, allocation breakdown.

Authentication via the TOKENOMIST_API_KEY environment variable.
Rate limit: 60 req/min on all paid tiers including free trial.
Free trial is capped at 50 tokens of coverage.

Per tokenomist's terms, all data is for personal/non-commercial,
non-redistribution use. Cached responses are stored under data/cache/
which is gitignored.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import requests

API_BASE = "https://api.tokenomist.ai"
API_KEY = os.environ.get("TOKENOMIST_API_KEY", "").strip()
SECONDS_BETWEEN_CALLS = 1.1  # 60/min limit; 1.1s gives us safety margin
CACHE_DIR = Path("data/cache/tokenomist")
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _headers() -> dict:
    if not API_KEY:
        raise RuntimeError(
            "TOKENOMIST_API_KEY not set. Add it to your .env file or export it. "
            "Get a free trial key at https://tokenomist.ai/pricing."
        )
    return {"x-api-key": API_KEY, "Accept": "application/json"}


def _cached_get(url: str, params: dict | None, cache_key: str, max_age_hours: int = 24) -> dict | None:
    cache_path = CACHE_DIR / f"{cache_key}.json"
    if cache_path.exists():
        age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
        if age_hours <= max_age_hours:
            with cache_path.open() as f:
                return json.load(f)

    try:
        resp = requests.get(url, params=params, headers=_headers(), timeout=30)
    except requests.RequestException as e:
        print(f"  ! tokenomist network error: {e}")
        return None

    if resp.status_code == 429:
        print(f"  ! tokenomist rate-limited, sleeping 60s")
        time.sleep(60)
        return _cached_get(url, params, cache_key, max_age_hours)
    if resp.status_code == 401:
        print(f"  ! tokenomist 401 — check TOKENOMIST_API_KEY")
        return None
    if resp.status_code != 200:
        print(f"  ! tokenomist HTTP {resp.status_code}: {resp.text[:200]}")
        return None

    payload = resp.json()
    with cache_path.open("w") as f:
        json.dump(payload, f)
    return payload


def list_tokens() -> pd.DataFrame:
    """Fetch the full list of tokens covered by the user's tokenomist subscription.

    Returns a DataFrame with columns including: id, name, symbol, circulatingSupply,
    marketCap, maxSupply, totalLockedAmount, tbdLockedAmount, unlockedAmount,
    untrackedAmount.

    On free trial this returns up to 50 rows; on paid tiers all covered tokens.
    """
    payload = _cached_get(f"{API_BASE}/v4/token/list", params=None, cache_key="token_list")
    if payload is None or not payload.get("status"):
        return pd.DataFrame()
    return pd.DataFrame(payload["data"])


def detect_tier(token_list_df: pd.DataFrame) -> str:
    """Heuristic tier detection from the size of the returned token list."""
    n = len(token_list_df)
    if n == 0:
        return "unknown (empty response)"
    if n <= 60:
        return f"likely free trial ({n} tokens; trial cap is 50)"
    return f"paid tier ({n} tokens)"


def unlock_events(token_id: str, start: str | None = None, end: str | None = None) -> pd.DataFrame:
    """Fetch all scheduled cliff unlock events for one token.

    Parameters
    ----------
    token_id : tokenomist's internal id (from list_tokens(), e.g. 'arbitrum').
    start, end : optional 'YYYY-MM-DD' filters; defaults are full available range.

    Returns a long-form DataFrame with one row per (event, allocation), columns:
      tokenId, tokenSymbol, tokenName, unlockDate, allocationName,
      standardAllocationName, cliffAmount, cliffValue, valueToMarketCap,
      referencePrice, unlockPrecision.
    """
    params: dict = {"tokenId": token_id}
    if start: params["start"] = start
    if end: params["end"] = end

    cache_key = f"unlock_{token_id}_{start or 'all'}_{end or 'all'}"
    payload = _cached_get(f"{API_BASE}/v4/unlock/events", params=params, cache_key=cache_key)
    if payload is None or not payload.get("status"):
        return pd.DataFrame()

    rows = []
    for event in payload.get("data", []):
        token_symbol = event.get("tokenSymbol")
        token_name = event.get("tokenName")
        cliff = event.get("cliffUnlocks") or {}
        event_v2m = cliff.get("valueToMarketCap")
        for alloc in cliff.get("allocationBreakdown", []):
            rows.append({
                "tokenId": token_id,
                "tokenSymbol": token_symbol,
                "tokenName": token_name,
                "unlockDate": alloc.get("unlockDate") or event.get("unlockDate"),
                "allocationName": alloc.get("allocationName"),
                "standardAllocationName": alloc.get("standardAllocationName"),
                "cliffAmount": alloc.get("cliffAmount"),
                "cliffValue": alloc.get("cliffValue"),
                "valueToMarketCap": event_v2m,
                "referencePrice": alloc.get("referencePrice"),
                "unlockPrecision": alloc.get("unlockPrecision"),
            })

    df = pd.DataFrame(rows)
    if not df.empty:
        df["unlockDate"] = pd.to_datetime(df["unlockDate"], errors="coerce", utc=True).dt.tz_localize(None)
    return df


def fetch_all_unlock_events(token_ids: list[str], verbose: bool = True) -> pd.DataFrame:
    """Pull unlock events for many tokens, throttled to fit the 60/min rate limit."""
    frames = []
    n = len(token_ids)
    for i, tid in enumerate(token_ids):
        if verbose:
            print(f"[{i+1:>3}/{n}] tokenomist unlocks: {tid}")
        df = unlock_events(tid)
        if not df.empty:
            frames.append(df)
        time.sleep(SECONDS_BETWEEN_CALLS)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ---------- summary helpers ----------


def next_unlock_per_token(events_df: pd.DataFrame) -> pd.DataFrame:
    """For each token, the next scheduled unlock event aggregated across allocations."""
    if events_df.empty:
        return pd.DataFrame()
    today = pd.Timestamp.utcnow().tz_localize(None).normalize()
    future = events_df[events_df["unlockDate"] >= today].copy()
    if future.empty:
        return pd.DataFrame()
    # Sum allocations on the same date (per-token next unlock)
    by_event = (
        future.groupby(["tokenSymbol", "unlockDate"], as_index=False)
        .agg(
            cliffAmount=("cliffAmount", "sum"),
            cliffValue=("cliffValue", "sum"),
            valueToMarketCap=("valueToMarketCap", "first"),
            n_allocations=("allocationName", "nunique"),
        )
    )
    idx = by_event.groupby("tokenSymbol")["unlockDate"].idxmin()
    nxt = by_event.loc[idx].copy()
    nxt["days_to_next_unlock"] = (nxt["unlockDate"] - today).dt.days
    return nxt.sort_values("days_to_next_unlock").reset_index(drop=True)


def supply_breakdown(token_list_df: pd.DataFrame) -> pd.DataFrame:
    """Compact supply-side view: locked share per token from tokenomist's own labels.

    Returns one row per token with the fraction of trackable supply currently
    locked. This is a richer supply-side overhang measure than CoinGecko's
    circulating supply alone.
    """
    if token_list_df.empty:
        return pd.DataFrame()
    df = token_list_df.copy()
    locked = df["totalLockedAmount"].fillna(0) + df["tbdLockedAmount"].fillna(0)
    unlocked = df["unlockedAmount"].fillna(0)
    untracked = df["untrackedAmount"].fillna(0)
    trackable = locked + unlocked  # untracked excluded from the denominator
    df["locked_share_of_trackable"] = locked.divide(trackable.replace(0, pd.NA))
    df["untracked_share_of_total"] = untracked.divide(
        (locked + unlocked + untracked).replace(0, pd.NA)
    )
    return df[[
        "id", "symbol", "name",
        "circulatingSupply", "marketCap", "maxSupply",
        "totalLockedAmount", "tbdLockedAmount", "unlockedAmount", "untrackedAmount",
        "locked_share_of_trackable", "untracked_share_of_total",
    ]]


def cumulative_dilution(events_df: pd.DataFrame, lookback_days: int = 365) -> pd.DataFrame:
    """Cumulative supply released per token over the last `lookback_days`,
    expressed as a share of contemporaneous market capitalization.

    For each unlock event in the window, tokenomist reports `valueToMarketCap`:
    the dollar value of that event's released supply divided by the token's
    market capitalization at the time of the event. Summing these per token
    gives a units-friendly approximation of "how much supply, in market-cap-
    equivalent terms, has flooded the float over the period."

    Caveat: the denominator (mcap) varies across events, so the sum is not a
    strict ratio. It is best read as a magnitude. Values >100% indicate that
    cumulative released supply over the period was worth more than the average
    market cap of the token over that period.

    Returns a per-token frame with cumulative_dilution_pct (the sum, scaled to
    percent), cliff_value_total_usd, and event count.
    """
    if events_df.empty:
        return pd.DataFrame()
    df = events_df.copy()
    df["unlockDate"] = pd.to_datetime(df["unlockDate"], errors="coerce", utc=True).dt.tz_localize(None)
    today = pd.Timestamp.utcnow().tz_localize(None).normalize()
    cutoff = today - pd.Timedelta(days=lookback_days)
    df = df[(df["unlockDate"] >= cutoff) & (df["unlockDate"] <= today)]
    if df.empty:
        return pd.DataFrame()

    # valueToMarketCap is repeated per allocation within an event; collapse to
    # one row per (token, event date) before summing across events.
    per_event = (
        df.groupby(["tokenSymbol", "unlockDate"], as_index=False)
        .agg(
            valueToMarketCap=("valueToMarketCap", "first"),
            cliff_value_usd=("cliffValue", "sum"),
        )
    )
    out = (
        per_event.groupby("tokenSymbol", as_index=False)
        .agg(
            cumulative_dilution_pct=("valueToMarketCap", "sum"),
            cliff_value_total_usd=("cliff_value_usd", "sum"),
            n_events=("unlockDate", "count"),
        )
        .sort_values("cumulative_dilution_pct", ascending=False)
        .reset_index(drop=True)
    )
    return out
