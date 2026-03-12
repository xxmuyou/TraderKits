from enum import Enum
from typing import Set
from datetime import timedelta

class DataTypes(str, Enum):
    @classmethod
    def has_value(cls, value: str) -> bool:
        return value in cls._value2member_map_


class BaseUrls(DataTypes):
    FUTURE = "https://data.binance.vision/data/futures/"
    SPOTS = "https://data.binance.vision/data/spot/"


class FrequencyTypes(DataTypes):
    DAILY = "daily"
    MONTHLY = "monthly"
    
    @classmethod
    def has_value(cls, value: str) -> bool:
        return value in cls._value2member_map_


class MarginTypes(DataTypes):
    UM = "um"
    CM = "cm"
    
    @classmethod
    def has_value(cls, value: str) -> bool:
        return value in cls._value2member_map_


class FuturesDataItems(DataTypes):
    AGG_TRADES = "aggTrades"
    BOOK_DEPTH = "bookDepth"
    BOOK_TICKER = "bookTicker"
    INDEX_PRICE_KLINES = "indexPriceKlines"
    KLINES = "klines"
    MARK_PRICE_KLINES = "markPriceKlines"
    TRADES = "trades"
    METRICS = "metrics"
    PREMIUM_INDEX_KLINES = "premiumIndexKlines"
    
    
class SpotDataItems(DataTypes):
    KLINES = "klines"
    AGG_TRADES = "aggTrades"
    TRADES = "trades"
    

FUTURE_UM_KLINE_INTERVALS: Set[str] = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"}

SPOT_KLINE_INTERVALS: Set[str] = {"12h", "15m", "1d", "1h", "1m", "1s", "2h", "30m", "3m", "4h", "5m", "6h", "8h"}

INTERVAL_TO_STR_DICT: dict[timedelta, str] = {
    timedelta(seconds=1): "1s",
    timedelta(minutes=1): "1m",
    timedelta(minutes=3): "3m",
    timedelta(minutes=5): "5m",
    timedelta(minutes=15): "15m",
    timedelta(minutes=30): "30m",
    timedelta(hours=1): "1h",
    timedelta(hours=2): "2h",
    timedelta(hours=4): "4h",
    timedelta(hours=6): "6h",
    timedelta(hours=8): "8h",
    timedelta(hours=12): "12h",
    timedelta(days=1): "1d",
}