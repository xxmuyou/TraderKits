from dataclasses import dataclass
from datetime import datetime

@dataclass
class KlinesColumns:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_asset_volume: float
    count: int
    taker_buy_volume: float
    taker_buy_quote_volume: float
    