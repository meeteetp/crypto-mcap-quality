"""Data fetching for the pilot.

CoinGecko is used for price/volume/market-cap history. The client supports
three modes:

  - No API key (default): public endpoint, throttled to 5-15 req/min globally.
  - Demo API key: set COINGECKO_API_KEY in the environment. Stable 30 req/min,
    10K calls/month, daily history capped at ~365 days. Free to register.
  - Pro API key: set COINGECKO_API_KEY *and* COINGECKO_PLAN=pro. Hits the
    pro-api.coingecko.com endpoint, 500+ req/min, full historical depth.

Tokenomist CSV exports (optional) are loaded from data/tokenomist_csv/ if
present. Per tokenomist's terms, these are for personal use only and must not
be committed to the repository.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

# Endpoint and throttling depend on whether/which key is configured.
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "").strip()
COINGECKO_PLAN = os.environ.get("COINGECKO_PLAN", "demo").strip().lower()
USING_PRO = bool(COINGECKO_API_KEY) and COINGECKO_PLAN == "pro"
USING_DEMO = bool(COINGECKO_API_KEY) and not USING_PRO

if USING_PRO:
    COINGECKO_BASE = "https://pro-api.coingecko.com/api/v3"
    SECONDS_BETWEEN_CALLS = 0.2
    MAX_DAYS = 3650  # effectively unlimited; full history available
elif USING_DEMO:
    COINGECKO_BASE = "https://api.coingecko.com/api/v3"
    SECONDS_BETWEEN_CALLS = 2.2  # 30 req/min stable
    MAX_DAYS = 365
else:
    COINGECKO_BASE = "https://api.coingecko.com/api/v3"
    SECONDS_BETWEEN_CALLS = 6.0  # public endpoint is throttled aggressively
    MAX_DAYS = 365

CACHE_DIR = Path("data/cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _headers() -> dict:
    h = {"Accept": "application/json"}
    if USING_PRO:
        h["x-cg-pro-api-key"] = COINGECKO_API_KEY
    elif USING_DEMO:
        h["x-cg-demo-api-key"] = COINGECKO_API_KEY
    return h


def _plan_label() -> str:
    if USING_PRO:
        return f"Pro (full history, ~500/min)"
    if USING_DEMO:
        return f"Demo (≤{MAX_DAYS} days, 30/min, 10K/month)"
    return f"Public no-key (≤{MAX_DAYS} days, 5-15/min)"


@dataclass
class TokenHistory:
    """Daily time series for a single token over a window."""

    coingecko_id: str
    symbol: str
    df: pd.DataFrame  # columns: date, price_usd, volume_usd, mcap_usd


def _cache_path(coingecko_id: str, days: int) -> Path:
    return CACHE_DIR / f"{coingecko_id}_{days}d.json"


def _load_cache(coingecko_id: str, days: int, max_age_hours: int = 24) -> dict | None:
    p = _cache_path(coingecko_id, days)
    if not p.exists():
        return None
    age_hours = (time.time() - p.stat().st_mtime) / 3600
    if age_hours > max_age_hours:
        return None
    with p.open() as f:
        return json.load(f)


def _save_cache(coingecko_id: str, days: int, payload: dict) -> None:
    with _cache_path(coingecko_id, days).open("w") as f:
        json.dump(payload, f)


def fetch_market_chart(coingecko_id: str, days: int = 365) -> dict | None:
    """Fetch daily market chart for a single coin from CoinGecko.

    Returns the raw response dict with 'prices', 'market_caps', 'total_volumes',
    each a list of [ms_timestamp, value] pairs. Cached locally for 24h.

    `days` is silently capped at MAX_DAYS for the active plan.
    """
    effective_days = min(days, MAX_DAYS)
    cached = _load_cache(coingecko_id, effective_days)
    if cached is not None:
        return cached

    url = f"{COINGECKO_BASE}/coins/{coingecko_id}/market_chart"
    params = {"vs_currency": "usd", "days": effective_days, "interval": "daily"}
    try:
        resp = requests.get(url, params=params, headers=_headers(), timeout=30)
    except requests.RequestException as e:
        print(f"  ! network error for {coingecko_id}: {e}")
        return None

    if resp.status_code == 429:
        print(f"  ! rate limited on {coingecko_id}, sleeping 60s")
        time.sleep(60)
        return fetch_market_chart(coingecko_id, days)
    if resp.status_code == 401:
        print(f"  ! 401 unauthorized — check COINGECKO_API_KEY and COINGECKO_PLAN")
        return None
    if resp.status_code != 200:
        print(f"  ! {coingecko_id}: HTTP {resp.status_code}")
        return None

    payload = resp.json()
    _save_cache(coingecko_id, effective_days, payload)
    return payload


def payload_to_dataframe(payload: dict) -> pd.DataFrame:
    """Convert a CoinGecko market_chart payload to a tidy DataFrame."""
    prices = pd.DataFrame(payload["prices"], columns=["ts_ms", "price_usd"])
    volumes = pd.DataFrame(payload["total_volumes"], columns=["ts_ms", "volume_usd"])
    mcaps = pd.DataFrame(payload["market_caps"], columns=["ts_ms", "mcap_usd"])
    df = prices.merge(volumes, on="ts_ms").merge(mcaps, on="ts_ms")
    df["date"] = pd.to_datetime(df["ts_ms"], unit="ms").dt.tz_localize(None).dt.normalize()
    df = df.drop(columns=["ts_ms"]).drop_duplicates(subset=["date"]).sort_values("date")
    return df.reset_index(drop=True)


def load_universe(path: str = "data/tokens.csv") -> pd.DataFrame:
    return pd.read_csv(path).drop_duplicates(subset=["coingecko_id"]).reset_index(drop=True)


def fetch_all(universe: pd.DataFrame, days: int = 365) -> list[TokenHistory]:
    """Pull history for every token in the universe, with throttling."""
    effective_days = min(days, MAX_DAYS)
    print(f"  CoinGecko plan: {_plan_label()}")
    if days > MAX_DAYS:
        print(f"  ! requested {days} days; capping at {MAX_DAYS} for current plan")
    print(f"  effective window: {effective_days} days")

    out: list[TokenHistory] = []
    n = len(universe)
    for i, row in universe.iterrows():
        print(f"[{i+1:>3}/{n}] {row['symbol']:<10} {row['coingecko_id']}")
        payload = fetch_market_chart(row["coingecko_id"], days=effective_days)
        if payload is None:
            continue
        df = payload_to_dataframe(payload)
        if len(df) < 30:
            print(f"  ! insufficient history ({len(df)} rows), skipping")
            continue
        out.append(TokenHistory(coingecko_id=row["coingecko_id"], symbol=row["symbol"], df=df))
        time.sleep(SECONDS_BETWEEN_CALLS)
    return out


# ---------- tokenomist CSV support (optional) ----------


def load_tokenomist_csvs(directory: str = "data/tokenomist_csv") -> pd.DataFrame:
    """Load any tokenomist CSV exports in the given directory.

    Tokenomist's CSV format includes columns that may change over time. This
    loader is deliberately tolerant: it returns a long-form frame with at
    minimum a symbol, a release date, and a released amount. Extra columns
    are kept for inspection. Returns an empty frame if no files are present.
    """
    d = Path(directory)
    if not d.exists():
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for csv in d.glob("*.csv"):
        try:
            df = pd.read_csv(csv)
        except Exception as e:
            print(f"  ! could not parse {csv.name}: {e}")
            continue

        # Tokenomist exports typically embed the token symbol in the filename;
        # use as a fallback if no symbol column is present.
        if "symbol" not in df.columns and "token" not in df.columns:
            df["symbol"] = csv.stem.upper().split("_")[0]

        df["_source_file"] = csv.name
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True, sort=False)


def next_unlock_summary(tokenomist_df: pd.DataFrame) -> pd.DataFrame:
    """Best-effort summary of the next scheduled unlock per token.

    Looks for columns that plausibly represent (date, amount) and returns a
    per-symbol frame with days-to-next-unlock and size as a share of circulating
    supply where inferrable.
    """
    if tokenomist_df.empty:
        return pd.DataFrame()

    df = tokenomist_df.copy()
    date_col = next(
        (c for c in df.columns if c.lower() in {"date", "unlock_date", "release_date", "timestamp"}),
        None,
    )
    amount_col = next(
        (c for c in df.columns if "amount" in c.lower() or "release" in c.lower() or "unlock" in c.lower()),
        None,
    )
    symbol_col = next(
        (c for c in df.columns if c.lower() in {"symbol", "token", "ticker"}),
        None,
    )
    if date_col is None or symbol_col is None:
        print("  ! tokenomist schema unrecognized; expected columns 'date' and 'symbol'")
        print(f"    saw: {list(df.columns)}")
        return pd.DataFrame()

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])
    today = pd.Timestamp.today().normalize()
    df = df[df[date_col] >= today]
    if df.empty:
        return pd.DataFrame()

    idx = df.groupby(symbol_col)[date_col].idxmin()
    nxt = df.loc[idx, [symbol_col, date_col] + ([amount_col] if amount_col else [])].copy()
    nxt["days_to_next_unlock"] = (nxt[date_col] - today).dt.days
    nxt = nxt.rename(columns={symbol_col: "symbol", date_col: "next_unlock_date"})
    if amount_col:
        nxt = nxt.rename(columns={amount_col: "next_unlock_amount"})
    return nxt.reset_index(drop=True)
