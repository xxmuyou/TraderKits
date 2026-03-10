from enum import Enum

class BaseUrls(str, Enum):
    FUTURE = "https://data.binance.vision/data/futures/"
    SPOTS = "https://data.binance.vision/data/spot/"

    
class FrequencyTypes(str, Enum):
    DAILY = "daily"
    MONTHLY = "monthly"


class MarginTypes(str, Enum):
    UM = "um"
    CM = "cm"


class FuturesDataItems(str, Enum):
    AGG_TRADES = "aggTrades"
    BOOK_DEPTH = "bookDepth"
    BOOK_TICKER = "bookTicker"
    INDEX_PRICE_KLINES = "indexPriceKlines"
    KLINES = "klines"
    MARK_PRICE_KLINES = "markPriceKlines"
    TRADES = "trades"
    METRICS = "metrics"
    PREMIUM_INDEX_KLINES = "premiumIndexKlines"
    
    
class SpotDataItems(str, Enum):
    KLINES = "klines"
    AGG_TRADES = "aggTrades"
    TRADES = "trades"