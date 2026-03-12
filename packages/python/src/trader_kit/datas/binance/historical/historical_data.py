import asyncio
import dataclasses
import io
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from datetime import datetime, timedelta
from typing import Any, Callable, Iterable, Mapping, Literal, Tuple, List, Set
from collections import defaultdict

import zipfile
import hashlib

import pandas as pd
import numpy as np

from .schemas import KlinesColumns
from ....utils import logger, async_get
from .constants import BaseUrls, FrequencyTypes, FuturesDataItems, SpotDataItems, MarginTypes, INTERVAL_TO_STR_DICT, FUTURE_UM_KLINE_INTERVALS, SPOT_KLINE_INTERVALS


KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore"
]



class HistoricalDataFetcher:
    @staticmethod
    async def fetch_future_um_klines(
        symbols: Iterable[str],
        start_dt: datetime,
        end_dt: datetime,
        interval: timedelta,
        frequency: Literal["daily", "monthly"] = "daily",
        *,
        max_concurrency: int = 16,
        max_workers: int = 8,
        timeout: float = 10.0,
    ) -> Mapping[str, pd.DataFrame]:
        """
        Fetch futures klines for multiple symbols over a date range.

        Pipeline:
            1. Build one URL per (symbol, date) combination.
            2. Fetch all zip + checksum files concurrently via :func:`_fetch_zips_async`.
            3. Verify checksums and decompress concurrently in a thread pool.
            4. Concatenate and return per-symbol DataFrames.

        Args:
            symbols: Trading pairs, e.g. ``["BTCUSDT", "ETHUSDT"]``.
            start_dt: Inclusive start of the date range.
            end_dt: Inclusive end of the date range.
            interval: Kline interval as a timedelta, e.g. ``timedelta(minutes=1)``, ``timedelta(hours=1)``, ``timedelta(days=1)``.
            frequency: ``"daily"`` for per-day files or ``"monthly"`` for per-month files.
            max_concurrency: Maximum concurrent HTTP coroutines.
            max_workers: Maximum threads in the decompression pool.
            timeout: Per-request HTTP timeout in seconds.

        Returns:
            ``{symbol: DataFrame}`` with all requested periods concatenated.
        """
        interval_str = convert_interval_to_str(interval, FUTURE_UM_KLINE_INTERVALS)
        if FrequencyTypes.has_value(frequency):
            frequency_enum = FrequencyTypes(frequency)
        else:
            raise ValueError(f"Invalid frequency: {frequency}. Must be 'daily' or 'monthly'.")
        margin_type_enum = MarginTypes.UM
        tasks = [
            (symbol, _build_future_kline_url(
                BaseUrls.FUTURE, symbol, interval_str, date_str, FuturesDataItems.KLINES, margin_type_enum, frequency_enum
            ))
            for symbol in symbols
            for date_str in iter_dates(start_dt, end_dt, frequency_enum)
        ]
        
        # Step 2: Fetch all zip + checksum files concurrently
        fetched_results = await fetch_urls_async(tasks, max_concurrency=max_concurrency, timeout=timeout)
        logger.info("Fetched files successfully.")
        # Step 3: Drain fetch queue, then process each item in a thread pool.
        # Each thread: verify checksum → unzip → build List[KlinesColumns].
        raw_tasks: list[tuple[str, bytes, str]] = []
        while not fetched_results.empty():
            raw_tasks.append(fetched_results.get_nowait())

        processed_queue = run_in_thread_pool(
            _process_kline_raw_data,
            raw_tasks,
            max_workers=max_workers,
        )

        # Step 4: Aggregate per-symbol DataFrames (each thread already built one).
        aggregated = _aggregate_symbol_dataframes(processed_queue)
        return _filter_and_localize_timestamps(aggregated, start_dt, end_dt)

    @staticmethod
    async def fetch_spot_klines(
        symbols: Iterable[str],
        start_dt: datetime,
        end_dt: datetime,
        interval: timedelta,
        *,
        max_concurrency: int = 16,
        max_workers: int = 8,
        timeout: float = 10.0,
    ) -> Mapping[str, pd.DataFrame]:
        interval_str = convert_interval_to_str(interval, SPOT_KLINE_INTERVALS)
        frequency_enum = FrequencyTypes.DAILY
        tasks = [
            (symbol, _build_spot_kline_url(
                BaseUrls.SPOTS, symbol, interval_str, date_str, SpotDataItems.KLINES, frequency_enum
            ))
            for symbol in symbols
            for date_str in iter_dates(start_dt, end_dt, frequency_enum)
        ]

        # Step 2: Fetch all zip + checksum files concurrently
        fetched_results = await fetch_urls_async(tasks, max_concurrency=max_concurrency, timeout=timeout)
        logger.info("Fetched files successfully.")

        # Step 3: Drain fetch queue, then process each item in a thread pool.
        raw_tasks: list[tuple[str, bytes, str]] = []
        while not fetched_results.empty():
            raw_tasks.append(fetched_results.get_nowait())

        processed_queue = run_in_thread_pool(
            _process_kline_raw_data,
            raw_tasks,
            max_workers=max_workers,
        )

        # Step 4: Aggregate per-symbol DataFrames (each thread already built one).
        aggregated = _aggregate_symbol_dataframes(processed_queue)
        return _filter_and_localize_timestamps(aggregated, start_dt, end_dt)
        
    @staticmethod
    def fetch_future_metrics(
        symbols: Iterable[str],
        start_dt: datetime,
        end_dt: datetime,
        interval: timedelta,
    ) -> Mapping[str, pd.DataFrame]:
        ...
        
    @staticmethod
    def fetch_premium_index_klines(
        symbols: Iterable[str],
        start_dt: datetime,
        end_dt: datetime,
        interval: timedelta,
        frequency: Literal["daily", "monthly"]="daily",
    ) -> Mapping[str, pd.DataFrame]:
        ...
    
    @staticmethod
    def fetch_index_price_klines(
        symbols: Iterable[str],
        start_dt: datetime,
        end_dt: datetime,
        interval: timedelta,
        frequency: Literal["daily", "monthly"]="daily",
    ) -> Mapping[str, pd.DataFrame]:
        ...
    
    @staticmethod
    def fetch_agg_trades(
        symbols: Iterable[str],
        start_dt: datetime,
        end_dt: datetime,
    ) -> Mapping[str, pd.DataFrame]:
        ...
    

# ---------------------------------------------------------------------------
# Module-level helpers — reusable across all HistoricalDataFetcher methods
# ---------------------------------------------------------------------------

# -------------spot-kline-specific helpers --------------------------------------------------
def _build_spot_kline_url(
    base_url: BaseUrls.SPOTS,
    symbol: str,
    interval: str,
    datetime_str: str,
    kline_item: SpotDataItems,
    frequency: FrequencyTypes.DAILY,
) -> str:
    """Construct the Binance Vision zip URL for a single spot kline file."""
    return (
        f"{base_url.value}"
        f"{frequency.value}/"
        f"{kline_item.value}/"
        f"{symbol}/"
        f"{interval}/"
        f"{symbol}-{interval}-{datetime_str}.zip"
    )

# Reference: https://data.binance.vision/?prefix=data/spot/daily/klines/BTCUSD/
# -------------kline-specific helpers --------------------------------------------------

def _aggregate_symbol_dataframes(result_queue: queue.Queue[Tuple[str, pd.DataFrame]]) -> dict[str, pd.DataFrame]:
    symbol_dfs: dict[str, list[pd.DataFrame]] = defaultdict(list)
    while not result_queue.empty():
        result = result_queue.get_nowait()
        if result is not None:
            sym, df = result
            symbol_dfs[sym].append(df)
    return {
        sym: pd.concat(dfs, ignore_index=True)
        for sym, dfs in symbol_dfs.items()
    }


def _filter_and_localize_timestamps(
    symbol_dfs: dict[str, pd.DataFrame],
    start_dt: datetime,
    end_dt: datetime,
    *,
    timestamp_col: str = "timestamp",
) -> dict[str, pd.DataFrame]:
    """Filter each DataFrame to [start_dt, end_dt] and convert the
    millisecond timestamp column to human-readable ``datetime64``."""
    start_ms = start_dt.timestamp() * 1000
    end_ms = end_dt.timestamp() * 1000
    result: dict[str, pd.DataFrame] = {}
    for sym, df in symbol_dfs.items():
        mask = (df[timestamp_col] >= start_ms) & (df[timestamp_col] <= end_ms)
        df = df.loc[mask].copy()
        df[timestamp_col] = pd.to_datetime(df[timestamp_col], unit="ms")
        result[sym] = df.sort_values(timestamp_col).reset_index(drop=True)
    return result


def _process_kline_raw_data(symbol: str, zip_content: bytes, checksum: str) -> Tuple[str, pd.DataFrame]:
    if verify_checksum(zip_content, checksum):
        df = read_zip_file(zip_content, symbol)
        df = _check_dataframe_columns(df, KLINE_COLUMNS)
        klines = _convert_df_to_klines_schemas(df)
        return (symbol, _klines_to_dataframe(klines))


def _convert_df_to_klines_schemas(df: pd.DataFrame) -> List[KlinesColumns]:
    default_values = np.nan
    return [
        KlinesColumns(
            timestamp=row.get("close_time", default_values),
            open=row.get("open", default_values),
            high=row.get("high", default_values),
            low=row.get("low", default_values),
            close=row.get("close", default_values),
            volume=row.get("volume", default_values),
            quote_asset_volume=row.get("quote_asset_volume", default_values),
            count=row.get("count", default_values),
            taker_buy_volume=row.get("taker_buy_volume", default_values),
            taker_buy_quote_volume=row.get("taker_buy_quote_volume", default_values),
        )
        for _, row in df.iterrows()
    ]


def _klines_to_dataframe(klines: List[KlinesColumns]) -> pd.DataFrame:
    """Convert List[KlinesColumns] to DataFrame column-by-column via numpy.

    Extracting each field into a numpy array first is significantly faster
    than the row-by-row ``pd.DataFrame([vars(k) for k in klines])`` approach.
    """
    fields = [f.name for f in dataclasses.fields(KlinesColumns)]
    if not klines:
        return pd.DataFrame(columns=fields)
    return pd.DataFrame(
        {
            col: np.fromiter((getattr(k, col) for k in klines), dtype=np.float64, count=len(klines))
            for col in fields
        }
    )


def _build_future_kline_url(
    base_url: BaseUrls.FUTURE,
    symbol: str,
    interval: str,
    datetime_str: str,
    kline_item: FuturesDataItems,
    margin_type: MarginTypes,
    frequency: FrequencyTypes,
) -> str:
    """Construct the Binance Vision zip URL for a single futures kline file."""
    return (
        f"{base_url.value}"
        f"{margin_type.value}/"
        f"{frequency.value}/"
        f"{kline_item.value}/"
        f"{symbol}/"
        f"{interval}/"
        f"{symbol}-{interval}-{datetime_str}.zip"
    )


def convert_interval_to_str(interval: timedelta, interval_set: Set[str]) -> str:
    """Check if the given timedelta interval corresponds to a supported KLINE_INTERVAL."""
    interval_str = INTERVAL_TO_STR_DICT.get(interval)
    if interval_str and interval_str in interval_set:
        return interval_str
    else:
        raise ValueError(
            f"Unsupported interval: {interval}. Supported intervals are: {interval_set}."
        )


#-----------------general-purpose helpers --------------------------------------------------

def _check_dataframe_columns(df: pd.DataFrame, expected_columns: List[str]) -> pd.DataFrame:
    if not any(col in df.columns for col in expected_columns):
        logger.warning(f"input_df columns: {df.columns} is not in expected_columns, fix columns to : {expected_columns}")
        df.columns = pd.to_numeric(df.columns, errors='coerce')
        df = df.T.reset_index().T
        df = df.reset_index(drop=True)
        df.columns = expected_columns
    return df.fillna(0)


def iter_dates(start_dt: datetime, end_dt: datetime, frequency: FrequencyTypes) -> Iterable[str]:
    """
    Yield date strings between *start_dt* and *end_dt* (inclusive) for the
    given *interval* and *frequency*.

    Args:
        start_dt: Inclusive start of the date range.
        end_dt: Inclusive end of the date range.
        frequency: ``"daily"`` yields ``"YYYY-MM-DD"`` strings stepping one
            day at a time; ``"monthly"`` yields ``"YYYY-MM"`` strings stepping
            one month at a time.

    Yields:
        Date strings suitable for use in Binance Vision file names.

    Raises:
        ValueError: If *frequency* is not ``"daily"`` or ``"monthly"``.
    """

    # ---- date iteration ------------------------------------------------------
    current = start_dt
    if frequency == "daily":
        while current <= end_dt:
            yield current.strftime("%Y-%m-%d")
            current += timedelta(days=1)
    else:  # monthly
        while current <= end_dt:
            yield current.strftime("%Y-%m")
            # Advance to the first day of the next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1, day=1)
            else:
                current = current.replace(month=current.month + 1, day=1)


def verify_checksum(content: bytes, checksum_text: str) -> bool:
    """
    Verify the integrity of the downloaded content using SHA256.

    Args:
        content (bytes): The raw binary content of the downloaded file.
        checksum_text (str): The raw text content from the .CHECKSUM file 
            (usually contains the hash and the filename).

    Returns:
        bool: True if verification succeeds.

    Raises:
        ValueError: If the calculated hash does not match the expected hash 
            from the checksum file.
    """
    if not checksum_text:
        raise ValueError("Checksum text is empty. Cannot verify integrity.")

    # Binance .CHECKSUM files typically follow the format: "hash_value  filename"
    # We only need the first part (the actual hash).
    expected_hash = checksum_text.strip().split()[0].lower()
    
    # Calculate the SHA256 hash of the downloaded bytes
    actual_hash = hashlib.sha256(content).hexdigest().lower()

    if expected_hash != actual_hash:
        raise ValueError(
            f"Data integrity check failed \n"
            f"Expected SHA256: {expected_hash}\n"
            f"Actual SHA256:   {actual_hash}\n"
            "The downloaded file might be corrupted."
        )
    
    return True

def read_zip_file(zip_content: bytes, symbol: str) -> pd.DataFrame:
    """
    Read CSV data from a ZIP file content in memory.
    
    Args:
        zip_content: The ZIP file content as bytes
        symbol: The instrument ID (used to identify the correct file in the ZIP)
        
    Returns:
        DataFrame containing the CSV data
    """
    with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_ref:
        # Get the list of files in the ZIP
        file_list = zip_ref.namelist()
        
        # Find the CSV file (assuming there's only one CSV file or we want the first one)
        csv_files = [f for f in file_list if f.endswith('.csv')]
        if not csv_files:
            raise ValueError(f"No CSV file found in ZIP for {symbol}")
        
        # Read the CSV file directly from the ZIP
        with zip_ref.open(csv_files[0]) as csv_file:
            return pd.read_csv(csv_file)


def run_in_thread_pool(
    fn: Callable[..., Any],
    tasks: Iterable[tuple[Any, ...]],
    *,
    max_workers: int = 8,
    timeout: float | None = None,
) -> queue.Queue[Any]:
    """
    Execute *fn* concurrently for each argument tuple in *tasks* using a
    thread pool and collect the results in a :class:`queue.Queue`.

    Args:
        fn: The callable to run in each thread. Each element of *tasks* is
            unpacked as positional arguments: ``fn(*args)``.
        tasks: An iterable of argument tuples, one per task.
        max_workers: Maximum number of threads in the pool.
        timeout: Total seconds to wait for all futures to complete.
            ``None`` means wait indefinitely. Tasks that have not finished
            when the timeout expires are logged but their results are omitted.

    Returns:
        A :class:`queue.Queue` containing the return value of every
        successfully completed task (in completion order).
    """
    results: queue.Queue[Any] = queue.Queue()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_args: dict = {executor.submit(fn, *args): args for args in tasks}

        try:
            for future in as_completed(future_to_args, timeout=timeout):
                args = future_to_args[future]
                try:
                    results.put(future.result())
                except Exception as exc:
                    logger.exception(
                        "Task with args {} raised an exception: {}", args, exc
                    )
        except FuturesTimeoutError:
            pending = sum(1 for f in future_to_args if not f.done())
            logger.exception(
                "Thread pool timed out after {} s; {} task(s) did not complete.",
                timeout,
                pending,
            )

    return results


async def fetch_urls_async(
    tasks: Iterable[Tuple[str, str]],
    *,
    max_concurrency: int = 8,
    timeout: float = 10.0,
) -> queue.Queue[Tuple[str, bytes, str]]:
    """
    Fetch multiple URLs concurrently with a bounded semaphore.

    Args:
        tasks: An iterable of tuples, each containing the symbol and URL.
        max_concurrency: Maximum number of concurrent coroutines.
        timeout: Per-request timeout in seconds passed to :func:`async_get`.

    Returns:
        A :class:`queue.Queue` containing tuples of the form ``(symbol, zip_file, checksum)``
        for every successfully completed request (in completion order).
    """
    results: queue.Queue[Tuple[str, bytes, str]] = queue.Queue()
    semaphore = asyncio.Semaphore(max_concurrency)

    async def _fetch(symbol: str, url: str) -> None:
        async with semaphore:
            try:
                zip_file, checksum = await asyncio.gather(
                    async_get(url, timeout=timeout),
                    async_get(url + ".CHECKSUM", timeout=timeout),
                )
                results.put((symbol, zip_file.content, checksum.text))
            except Exception as exc:
                logger.exception("Failed to fetch {}: {}", url, exc)

    await asyncio.gather(*(_fetch(symbol, url) for symbol, url in tasks))
    return results


def convert_datetime_to_string(dt: datetime) -> str:
    """
    Convert a :class:`datetime` object to a date string in ``YYYY-MM-DD`` format.

    Args:
        dt: The datetime to convert.

    Returns:
        A string such as ``"2026-03-07"``.
    """
    return dt.strftime("%Y-%m-%d")

if __name__ == "__main__":
    # Example usage
    symbols = ["BTCUSDT", "ETHUSDT"]
    interval = timedelta(minutes=15)
    start_dt = datetime(2025, 9, 1)
    end_dt = datetime(2025, 10, 3)
    
    async def main():
        df = await HistoricalDataFetcher.fetch_spot_klines(
            symbols=symbols,
            start_dt=start_dt,
            end_dt=end_dt,
            interval=interval
        )
        print(df)
        
    asyncio.run(main())
