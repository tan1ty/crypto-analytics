from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    symbol: str = "BTCUSDT"
    interval: str = "1h"
    data_dir: str = "data/raw"
    start_if_empty: str = "2024-01-01 00:00:00"

    def out_path(self) -> str:
        return f"{self.data_dir}/binance_{self.symbol}_{self.interval}.csv"


SETTINGS = Settings()
