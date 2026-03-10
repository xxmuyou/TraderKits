import io
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from datetime import datetime
from typing import Any, Callable, Iterable

import zipfile
import hashlib

import pandas as pd
import numpy as np

from ....utils import logger, sync_get
from .constants import BaseUrls, FrequencyTypes, FuturesDataItems, MarginTypes

class HistoricalDataFetcher:
    ...
    
    
def fetch_future_klines(
    symbol: str,
    interval: str,
    dt: datetime,
    *,
    margin_type: MarginTypes = MarginTypes.UM,
    frequency: FrequencyTypes = FrequencyTypes.DAILY,
    checksum: bool = True,
) -> pd.DataFrame:
    """
    Fetch a single period of futures klines from Binance Vision.

    URL pattern::

        {BaseUrls.FUTURE}{margin_type}/{frequency}/klines/{symbol}/{interval}/
        {symbol}-{interval}-{date}.zip

    Args:
        symbol: Trading pair, e.g. ``"BTCUSDT"``.
        interval: Kline interval, e.g. ``"1m"``, ``"1h"``, ``"1d"``.
        dt: The target datetime. For DAILY the date portion is used;
            for MONTHLY only year and month are used.
        margin_type: ``MarginTypes.UM`` (USDT-margined) or ``MarginTypes.CM`` (coin-margined).
        frequency: ``FrequencyTypes.DAILY`` or ``FrequencyTypes.MONTHLY``.
        checksum: Whether to verify the SHA256 checksum after download.

    Returns:
        A :class:`pd.DataFrame` containing the klines for the requested period.
    """
    date_str = (
        dt.strftime("%Y-%m-%d")
        if frequency == FrequencyTypes.DAILY
        else dt.strftime("%Y-%m")
    )

    url = (
        f"{BaseUrls.FUTURE.value}"
        f"{margin_type.value}/"
        f"{frequency.value}/"
        f"{FuturesDataItems.KLINES.value}/"
        f"{symbol}/"
        f"{interval}/"
        f"{symbol}-{interval}-{date_str}.zip"
    )
    logger.debug("Fetching futures klines: {}", url)
    return fetch_signal_data(symbol, date_str, url, checksum=checksum)

def fetch_signal_data(
    symbol: str,
    date: str,
    download_url: str,
    checksum: bool = True,
) -> pd.DataFrame:
    # Fetch the ZIP file directly into memory
    response = sync_get(
        download_url,
        max_retries=3,
        timeout=5
    )

    # If checksum verification is required
    if checksum:
        checksum_url = download_url + ".CHECKSUM"
        try:
            checksum_response = sync_get(
                checksum_url,
                max_retries=3,
                timeout=5
            )
            if verify_checksum(response.content, checksum_response.text, download_url):
                logger.info(f"Checksum verification passed for {download_url}")
            else:
                logger.exception(f"Checksum verification failed for {download_url}, but proceeding with caution.")
                raise ValueError(f"Checksum verification failed for {download_url}")

        except Exception as exc:
            logger.exception(f"Failed to fetch checksum file for {download_url}: {exc}")
            return pd.DataFrame()  # No checksum file available, skip verification

    # Read the ZIP file from memory
    return read_zip_file(response.content, symbol, date)


def verify_checksum(content: bytes, checksum_text: str, source_url: str) -> bool:
    """
    Verify the integrity of the downloaded content using SHA256.

    Args:
        content (bytes): The raw binary content of the downloaded file.
        checksum_text (str): The raw text content from the .CHECKSUM file 
            (usually contains the hash and the filename).
        source_url (str): The original download URL, used for error reporting.

    Returns:
        bool: True if verification succeeds.

    Raises:
        ValueError: If the calculated hash does not match the expected hash 
            from the checksum file.
    """
    if not checksum_text:
        logger.warning(f"Empty checksum content for {source_url}. Skipping.")
        return True
        
    # Binance .CHECKSUM files typically follow the format: "hash_value  filename"
    # We only need the first part (the actual hash).
    expected_hash = checksum_text.strip().split()[0].lower()
    
    # Calculate the SHA256 hash of the downloaded bytes
    actual_hash = hashlib.sha256(content).hexdigest().lower()

    if expected_hash != actual_hash:
        raise ValueError(
            f"Data integrity check failed for: {source_url}\n"
            f"Expected SHA256: {expected_hash}\n"
            f"Actual SHA256:   {actual_hash}\n"
            "The downloaded file might be corrupted."
        )
    
    return True

def read_zip_file(zip_content: bytes, symbol: str, date: str) -> pd.DataFrame:
    """
    Read CSV data from a ZIP file content in memory.
    
    Args:
        zip_content: The ZIP file content as bytes
        symbol: The instrument ID (used to identify the correct file in the ZIP)
        date: The date (used to identify the correct file in the ZIP)
        
    Returns:
        DataFrame containing the CSV data
    """
    with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_ref:
        # Get the list of files in the ZIP
        file_list = zip_ref.namelist()
        
        # Find the CSV file (assuming there's only one CSV file or we want the first one)
        csv_files = [f for f in file_list if f.endswith('.csv')]
        if not csv_files:
            raise ValueError(f"No CSV file found in ZIP for {symbol} on {date}")
        
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
    symbol = "BTCUSDT"
    interval = "5m"
    dt = datetime(2023, 9, 1)
    
    df = fetch_future_klines(symbol, interval, dt)
    print(df.head())