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
    
    
@dataclass
class MetricsColumns:
    timestamp: int
    sum_open_interest: float
    sum_open_interest_value: float
    count_toptrader_long_short_ratio: float
    count_long_short_ratio: float
    sum_taker_long_short_vol_ratio: float
    sum_toptrader_long_short_ratio: float