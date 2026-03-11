from enum import Enum
from typing import Set

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
    

KLINE_INTERVALS: Set[str] = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"}