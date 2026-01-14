from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    exchange: str = "binance"
    symbol: str = "BTCUSDT"
    interval: str = "1h"
    data_dir: str = "data/raw"
    start_if_empty: str = "2024-01-01 00:00:00"

 
    def out_path(self, symbol: str | None = None, interval: str | None = None) -> str:
        s = (symbol or self.symbol).upper()
        i = interval or self.interval
        return f"{self.data_dir}/{self.exchange}_{s}_{i}.csv"


SETTINGS = Settings()
