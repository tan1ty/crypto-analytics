from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Optional, Union

import pandas as pd
import requests


BINANCE_BASE_URL = "https://api.binance.com"


def to_millis(dt: Union[str, int, float, datetime]) -> int:
    """
    Accepts:
      - datetime (aware/naive; naive assumed UTC)
      - ISO string like "2024-01-01 00:00:00" or "2024-01-01T00:00:00"
      - seconds (int/float) or milliseconds (int) timestamps
    Returns milliseconds since epoch (UTC).
    """
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

    if isinstance(dt, (int, float)):
        # Heuristic: if it's too small, treat as seconds
        return int(dt * 1000) if dt < 10_000_000_000 else int(dt)

    if isinstance(dt, str):
        s = dt.strip().replace("T", " ")
        parsed = datetime.fromisoformat(s)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)

    raise TypeError(f"Unsupported time type: {type(dt)}")


def fetch_klines(
    symbol: str,
    interval: str,
    start_time: Union[str, int, float, datetime],
    end_time: Optional[Union[str, int, float, datetime]] = None,
    limit: int = 1000,
    session: Optional[requests.Session] = None,
    sleep_seconds: float = 0.2,
) -> pd.DataFrame:
    """
    Fetches historical klines (OHLCV) from Binance REST API and returns a DataFrame.

    Binance endpoint returns up to `limit` candles per request, so we paginate from start_time to end_time.
    """
    start_ms = to_millis(start_time)
    end_ms = to_millis(end_time) if end_time is not None else None

    sess = session or requests.Session()
    url = f"{BINANCE_BASE_URL}/api/v3/klines"

    rows = []
    cur = start_ms

    while True:
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "startTime": cur,
            "limit": limit,
        }
        if end_ms is not None:
            params["endTime"] = end_ms

        r = sess.get(url, params=params, timeout=30)
        r.raise_for_status()
        chunk = r.json()

        if not chunk:
            break

        rows.extend(chunk)

        last_open_time = chunk[-1][0]
        next_open_time = last_open_time + 1  # move forward (ms) to avoid duplicates

        if end_ms is not None and next_open_time >= end_ms:
            break

        if next_open_time <= cur:
            # Safety to avoid infinite loop if API behaves unexpectedly
            break

        cur = next_open_time
        time.sleep(sleep_seconds)

    if not rows:
        return pd.DataFrame(columns=["open_time","open","high","low","close","volume","close_time"])

    df = pd.DataFrame(
        rows,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_asset_volume",
            "taker_buy_quote_asset_volume",
            "ignore",
        ],
    )

    # Types
    numeric_cols = ["open", "high", "low", "close", "volume"]
    df[numeric_cols] = df[numeric_cols].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)

    # Keep only what you asked for (OHLCV + times)
    df = df[["open_time", "open", "high", "low", "close", "volume", "close_time"]].copy()
    return df


def save_klines_csv(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, index=False)


if __name__ == "__main__":
    symbol = "BTCUSDT"
    interval = "1h"
    start = "2024-01-01 00:00:00"
    end = "2024-01-08 00:00:00"

    df = fetch_klines(symbol, interval, start, end)
    out_path = f"data/raw/binance_{symbol}_{interval}.csv"
    save_klines_csv(df, out_path)
    print(df.head())
    print(f"Saved: {out_path} | rows={len(df)}")
