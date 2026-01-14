from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Optional, Union

import pandas as pd
import requests

import argparse
from .config import SETTINGS, Settings

BINANCE_BASE_URL = "https://api.binance.com"


def to_millis(dt: Union[str, int, float, datetime]) -> int:
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

    if isinstance(dt, (int, float)):
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
        next_open_time = last_open_time + 1

        if end_ms is not None and next_open_time >= end_ms:
            break

        if next_open_time <= cur:
            break

        cur = next_open_time
        time.sleep(sleep_seconds)

    if not rows:
        return pd.DataFrame(columns=["open_time", "open", "high", "low", "close", "volume", "close_time"])

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

    numeric_cols = ["open", "high", "low", "close", "volume"]
    df[numeric_cols] = df[numeric_cols].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)

    return df[["open_time", "open", "high", "low", "close", "volume", "close_time"]].copy()


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def load_existing_csv(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path, parse_dates=["open_time", "close_time"])
    # на всякий випадок: приводимо до UTC
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], utc=True)
    return df


def update_klines_csv(
    symbol: str,
    interval: str,
    path: str,
    start_time_if_empty: Union[str, datetime],
    end_time: Optional[Union[str, datetime]] = None,
) -> pd.DataFrame:
    """
    Якщо файл існує — визначаємо останній open_time і дотягуємо тільки нові свічки.
    Якщо файлу нема — качаємо з start_time_if_empty.
    """
    ensure_parent_dir(path)

    existing = load_existing_csv(path)
    if existing is None or existing.empty:
        fetch_start = start_time_if_empty
        base = pd.DataFrame()
    else:
        last_open = existing["open_time"].max()
        # наступна свічка після останньої (щоб не дублювати)
        fetch_start = (last_open.to_pydatetime()).replace(tzinfo=timezone.utc)
        base = existing

    new_df = fetch_klines(symbol, interval, fetch_start, end_time=end_time)

    if base.empty:
        merged = new_df
    else:
        merged = pd.concat([base, new_df], ignore_index=True)

    # прибрати дублікати по open_time (на випадок перетинів)
    merged = merged.drop_duplicates(subset=["open_time"], keep="last")
    merged = merged.sort_values("open_time").reset_index(drop=True)

    merged.to_csv(path, index=False)
    return merged


if __name__ == "__main__":
    symbol = "BTCUSDT"
    interval = "1h"
    out_path = f"data/raw/binance_{symbol}_{interval}.csv"

    # якщо файл ще не існує — з якої дати починати історію
    start_if_empty = "2024-01-01 00:00:00"

    df = update_klines_csv(
        symbol=symbol,
        interval=interval,
        path=out_path,
        start_time_if_empty=start_if_empty,
        end_time=None,  # None = качати аж до "зараз" (Binance сам віддає доступне)
    )

    print(df.tail())
    print(f"Saved/updated: {out_path} | rows={len(df)}")

def update_data(symbol: str, interval: str, out_path: str, start_if_empty: str) -> None:
    df = update_klines_csv(
        symbol=symbol,
        interval=interval,
        path=out_path,
        start_time_if_empty=start_if_empty,
        end_time=None,
    )
    print(f"Saved/updated: {out_path} | rows={len(df)}")


def main() -> None:
    p = argparse.ArgumentParser(description="Fetch & update Binance klines (OHLCV) into CSV.")
    p.add_argument("--symbol", default=SETTINGS.symbol)
    p.add_argument("--interval", default=SETTINGS.interval)
    p.add_argument("--out", default=None, help="Output CSV path (optional).")
    p.add_argument("--start-if-empty", default=SETTINGS.start_if_empty)
    args = p.parse_args()

    # якщо шлях не заданий — беремо з config-логіки
    if args.out is None:
        s = Settings(symbol=args.symbol, interval=args.interval, data_dir=SETTINGS.data_dir, start_if_empty=args.start_if_empty)
        out_path = s.out_path()
    else:
        out_path = args.out

    update_data(args.symbol, args.interval, out_path, args.start_if_empty)


if __name__ == "__main__":
    main()