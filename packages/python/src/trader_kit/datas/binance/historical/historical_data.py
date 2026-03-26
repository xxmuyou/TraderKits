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

from .schemas import KlinesColumns, MetricsColumns
from ....utils import logger, async_get
from .constants import BaseUrls, FrequencyTypes, FuturesDataItems, SpotDataItems, MarginTypes, INTERVAL_TO_STR_DICT, FUTURE_UM_KLINE_INTERVALS, SPOT_KLINE_INTERVALS


KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore"
]

METRICS_COLUMNS = [
    "create_time", "symbol", "sum_open_interest", "sum_open_interest_value", "count_toptrader_long_short_ratio", "sum_toptrader_long_short_ratio", "count_long_short_ratio", "sum_taker_long_short_vol_ratio"
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
    async def fetch_future_metrics(
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
        Fetch futures metrics (open interest, long/short ratios, etc.) for multiple
        symbols over a date range.

        Raw metrics files are sampled at 5-minute resolution. Missing 5-minute slots
        within each daily file are back-filled with NaN. The cleaned series is then
        resampled (mean-aggregated) to *interval*.

        Pipeline:
            1. Build one URL per (symbol, date) combination.
            2. Fetch all zip + checksum files concurrently via :func:`fetch_urls_async`.
            3. Verify checksums, decompress, and fill 5-min gaps in a thread pool.
            4. Aggregate per-symbol DataFrames and resample to *interval*.

        Args:
            symbols: Trading pairs, e.g. ``["BTCUSDT", "ETHUSDT"]``.
            start_dt: Inclusive start of the date range.
            end_dt: Inclusive end of the date range.
            interval: Resampling interval, e.g. ``timedelta(minutes=5)``, ``timedelta(hours=1)``.
                      Must be a multiple of 5 minutes.
            frequency: ``"daily"`` for per-day files or ``"monthly"`` for per-month files.
            max_concurrency: Maximum concurrent HTTP coroutines.
            max_workers: Maximum threads in the decompression / cleaning pool.
            timeout: Per-request HTTP timeout in seconds.

        Returns:
            ``{symbol: DataFrame}`` with metrics columns resampled to *interval*.
        """
        if FrequencyTypes.has_value(frequency):
            frequency_enum = FrequencyTypes(frequency)
        else:
            raise ValueError(f"Invalid frequency: {frequency}. Must be 'daily' or 'monthly'.")

        margin_type_enum = MarginTypes.UM
        tasks = [
            (symbol, _build_future_metrics_url(
                BaseUrls.FUTURE, symbol, date_str, margin_type_enum, frequency_enum
            ))
            for symbol in symbols
            for date_str in iter_dates(start_dt, end_dt, frequency_enum)
        ]

        # Step 2: Fetch all zip + checksum files concurrently
        fetched_results = await fetch_urls_async(tasks, max_concurrency=max_concurrency, timeout=timeout)
        logger.info("Fetched metrics files successfully.")

        # Step 3: Drain fetch queue, then clean each file in a thread pool.
        # Each thread: verify checksum → unzip → fill 5-min gaps → build List[MetricsColumns].
        raw_tasks: list[tuple[str, bytes, str]] = []
        while not fetched_results.empty():
            raw_tasks.append(fetched_results.get_nowait())

        processed_queue = run_in_thread_pool(
            _process_metrics_raw_data,
            raw_tasks,
            max_workers=max_workers,
        )

        # Step 4: Aggregate per-symbol DataFrames and resample to requested interval
        aggregated = _aggregate_symbol_dataframes(processed_queue)
        return _resample_metrics_to_interval(aggregated, interval, start_dt, end_dt)

        
    @staticmethod
    async def fetch_premium_index_klines(
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
        Fetch futures index price klines for multiple symbols over a date range.

        Pipeline:
            1. Build one URL per (symbol, date) combination.
            2. Fetch all zip + checksum files concurrently via :func:`fetch_urls_async`.
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
                BaseUrls.FUTURE, symbol, interval_str, date_str, FuturesDataItems.INDEX_PRICE_KLINES, margin_type_enum, frequency_enum
            ))
            for symbol in symbols
            for date_str in iter_dates(start_dt, end_dt, frequency_enum)
        ]

        # Step 2: Fetch all zip + checksum files concurrently
        fetched_results = await fetch_urls_async(tasks, max_concurrency=max_concurrency, timeout=timeout)
        logger.info("Fetched files successfully.")
        # Step 3: Drain fetch queue, then process each item in a thread pool.
        # Each thread: verify checksum -> unzip -> build List[KlinesColumns].
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


# ---------------------------------------------------------------------------
# Metrics-specific helpers
# ---------------------------------------------------------------------------

# The raw metrics data uses a fixed 5-minute sampling cadence.
_METRICS_RAW_INTERVAL = timedelta(minutes=5)


def _build_future_metrics_url(
    base_url: BaseUrls,
    symbol: str,
    date_str: str,
    margin_type: MarginTypes,
    frequency: FrequencyTypes,
) -> str:
    """Construct the Binance Vision zip URL for a single futures metrics file.

    Example:
        https://data.binance.vision/data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-2025-01-01.zip
    """
    return (
        f"{base_url.value}"
        f"{margin_type.value}/"
        f"{frequency.value}/"
        f"{FuturesDataItems.METRICS.value}/"
        f"{symbol}/"
        f"{symbol}-metrics-{date_str}.zip"
    )


def _align_metrics_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure *df* carries the expected :data:`METRICS_COLUMNS` column names.

    Binance metrics CSVs ship with a header row, so this is normally a no-op.
    The fallback handles the rare case where the file is header-less.
    """
    if not any(col in df.columns for col in METRICS_COLUMNS):
        df = df.copy()
        df.columns = pd.to_numeric(df.columns, errors="coerce")
        df = df.T.reset_index().T.reset_index(drop=True)
        df.columns = METRICS_COLUMNS
    return df


def _fill_5min_intervals(df: pd.DataFrame) -> pd.DataFrame:
    """Guarantee a row for every 5-minute slot within the day covered by *df*.

    Missing slots are inserted with NaN for all value columns.  The daily
    window is inferred from the earliest timestamp present in *df*.

    Args:
        df: DataFrame whose first column is ``create_time`` (parsed to
            ``datetime64``).

    Returns:
        A new DataFrame with ``create_time`` as a regular column and a
        complete 5-minute timeline.
    """
    df = df.copy()
    df["create_time"] = pd.to_datetime(df["create_time"])

    # Drop the symbol column — it is redundant (the caller already tracks the symbol).
    df = df.drop(columns=["symbol"], errors="ignore")
    df = df.set_index("create_time")

    # Build the complete 5-minute grid for the calendar day of the first row.
    day_start: pd.Timestamp = df.index.min().normalize()
    day_end: pd.Timestamp = day_start + timedelta(hours=23, minutes=55)
    full_index = pd.date_range(start=day_start, end=day_end, freq="5min")

    # Reindex — rows for missing timestamps become NaN.
    df = df.reindex(full_index)
    df.index.name = "create_time"
    return df.reset_index()


def _to_float(val) -> float:
    """Safely cast *val* to ``float``, returning ``nan`` for non-numeric or NaT values."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return np.nan


def _convert_df_to_metrics_schemas(df: pd.DataFrame) -> List[MetricsColumns]:
    """Row-wise conversion of a cleaned metrics DataFrame to :class:`MetricsColumns`.

    ``create_time`` is converted to a millisecond Unix timestamp stored as
    ``float`` so that it is compatible with the NaN-aware ``float64`` arrays
    used throughout the pipeline.
    """
    rows = []
    for _, row in df.iterrows():
        ct = row["create_time"]
        ts = ct.timestamp() * 1000 if pd.notna(ct) else np.nan
        rows.append(
            MetricsColumns(
                timestamp=ts,
                sum_open_interest=_to_float(row.get("sum_open_interest")),
                sum_open_interest_value=_to_float(row.get("sum_open_interest_value")),
                count_toptrader_long_short_ratio=_to_float(row.get("count_toptrader_long_short_ratio")),
                sum_toptrader_long_short_ratio=_to_float(row.get("sum_toptrader_long_short_ratio")),
                count_long_short_ratio=_to_float(row.get("count_long_short_ratio")),
                sum_taker_long_short_vol_ratio=_to_float(row.get("sum_taker_long_short_vol_ratio")),
            )
        )
    return rows


def _metrics_to_dataframe(metrics: List[MetricsColumns]) -> pd.DataFrame:
    """Convert ``List[MetricsColumns]`` to a ``DataFrame`` via numpy arrays.

    Uses the same column-extraction strategy as :func:`_klines_to_dataframe`
    for consistency and performance.
    """
    fields = [f.name for f in dataclasses.fields(MetricsColumns)]
    if not metrics:
        return pd.DataFrame(columns=fields)
    return pd.DataFrame(
        {
            col: np.array([getattr(m, col) for m in metrics], dtype=np.float64)
            for col in fields
        }
    )


def _process_metrics_raw_data(
    symbol: str, zip_content: bytes, checksum: str
) -> Tuple[str, pd.DataFrame] | None:
    """Verify checksum, decompress, align columns, fill 5-min gaps, and schema-convert.

    Returns ``None`` if the checksum verification or any subsequent step fails
    (exceptions are propagated via the thread pool and logged by the caller).
    """
    if verify_checksum(zip_content, checksum):
        df = read_zip_file(zip_content, symbol)
        df = _align_metrics_columns(df)
        df = _fill_5min_intervals(df)
        metrics = _convert_df_to_metrics_schemas(df)
        return (symbol, _metrics_to_dataframe(metrics))
    return None


def _resample_metrics_to_interval(
    symbol_dfs: dict[str, pd.DataFrame],
    interval: timedelta,
    start_dt: datetime,
    end_dt: datetime,
) -> dict[str, pd.DataFrame]:
    """Resample 5-minute metrics DataFrames to the requested *interval*.

    Each interval bucket is aligned to Unix-epoch boundaries (floor division).
    Value columns are aggregated by mean; the bucket's representative timestamp
    is the start of the interval window, converted to ``datetime64``.

    Args:
        symbol_dfs: Per-symbol DataFrames with 5-minute resolution and a
            millisecond ``timestamp`` column.
        interval: Target resampling interval.
        start_dt: Inclusive lower bound used to filter the output.
        end_dt: Inclusive upper bound used to filter the output.

    Returns:
        ``{symbol: DataFrame}`` with timestamps converted to ``datetime64``.
    """
    interval_ms = int(interval.total_seconds() * 1000)
    value_cols = [f.name for f in dataclasses.fields(MetricsColumns) if f.name != "timestamp"]
    # Use pd.Timestamp for start/end so the UTC interpretation matches the data
    # timestamps produced by pd.Timestamp.timestamp() in _convert_df_to_metrics_schemas.
    start_ms = pd.Timestamp(start_dt).timestamp() * 1000
    end_ms = pd.Timestamp(end_dt).timestamp() * 1000
    result: dict[str, pd.DataFrame] = {}

    for sym, df in symbol_dfs.items():
        df = df.copy()
        # Snap each 5-min timestamp down to the nearest interval boundary.
        df["interval_ts"] = (df["timestamp"] // interval_ms) * interval_ms

        # Aggregate each metric column by mean within each interval window.
        grouped = (
            df.groupby("interval_ts")[value_cols]
            .mean()
            .reset_index()
            .rename(columns={"interval_ts": "timestamp"})
        )

        # Filter to [start_dt, end_dt].
        mask = (grouped["timestamp"] >= start_ms) & (grouped["timestamp"] <= end_ms)
        filtered = grouped.loc[mask].copy()

        # Convert millisecond timestamp to human-readable datetime64.
        filtered["timestamp"] = pd.to_datetime(filtered["timestamp"], unit="ms")
        result[sym] = filtered.sort_values("timestamp").reset_index(drop=True)

    return result


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
        df = await HistoricalDataFetcher.fetch_premium_index_klines(
            symbols=symbols,
            start_dt=start_dt,
            end_dt=end_dt,
            interval=interval
        )
        print(df)
        
    asyncio.run(main())
