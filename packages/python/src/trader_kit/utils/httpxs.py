import asyncio
from typing import Any
import time

import httpx

from .setup_logger import logger


async def async_get(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 10.0,
    retry_codes: tuple[int, ...] = (429, 500, 502, 503, 504),
    max_retries: int = 3,
    retry_delay: float = 0.5,
) -> httpx.Response:
    """
    Async HTTP GET with retry and timeout support.

    :param url: Target URL.
    :param params: Query parameters.
    :param headers: Request headers.
    :param timeout: Seconds before a request times out.
    :param retry_codes: HTTP status codes that trigger a retry.
    :param max_retries: Maximum number of retry attempts.
    :param retry_delay: Base delay (seconds) between retries; doubles on each attempt.
    :return: The successful :class:`httpx.Response`.
    :raises httpx.HTTPStatusError: When a non-retryable HTTP error status is received.
    :raises httpx.TimeoutException: When every attempt times out.
    :raises httpx.RequestError: When a network-level error occurs on every attempt.
    """
    attempt = 0
    last_exc: Exception | None = None

    async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
        while attempt <= max_retries:
            try:
                response = await client.get(url, params=params)

                if response.status_code in retry_codes:
                    logger.warning(
                        "Retryable status {} from {} (attempt {}/{})",
                        response.status_code,
                        url,
                        attempt + 1,
                        max_retries + 1,
                    )
                    last_exc = httpx.HTTPStatusError(
                        f"Retryable status {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                    # Fall through to retry logic below

                else:
                    # Raise immediately for non-retryable 4xx/5xx errors
                    response.raise_for_status()
                    return response

            except httpx.TimeoutException as exc:
                logger.exception(
                    "Request timed out ({} s) for {} (attempt {}/{}): {}",
                    timeout,
                    url,
                    attempt + 1,
                    max_retries + 1,
                    exc,
                )
                last_exc = exc

            except httpx.HTTPStatusError as exc:
                # Non-retryable HTTP error — fail immediately
                logger.exception(
                    "Non-retryable HTTP error {} for {}: {}",
                    exc.response.status_code,
                    url,
                    exc,
                )
                raise

            except httpx.ConnectError as exc:
                logger.exception(
                    "Connection error for {} (attempt {}/{}): {}",
                    url,
                    attempt + 1,
                    max_retries + 1,
                    exc,
                )
                last_exc = exc

            except httpx.RequestError as exc:
                # Catch-all for other network-level errors (DNS, proxy, etc.)
                logger.exception(
                    "Request error for {} (attempt {}/{}): {}",
                    url,
                    attempt + 1,
                    max_retries + 1,
                    exc,
                )
                last_exc = exc

            attempt += 1
            if attempt <= max_retries:
                delay = retry_delay * (2 ** (attempt - 1))
                logger.debug("Retrying in {:.1f}s ...", delay)
                await asyncio.sleep(delay)

    logger.error("All {} attempts failed for {}", max_retries + 1, url)
    raise last_exc  # type: ignore[misc]


def sync_get(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 10.0,
    retry_codes: tuple[int, ...] = (429, 500, 502, 503, 504),
    max_retries: int = 3,
    retry_delay: float = 0.5,
) -> httpx.Response:
    """
    Synchronous HTTP GET with retry and timeout support.

    :param url: Target URL.
    :param params: Query parameters.
    :param headers: Request headers.
    :param timeout: Seconds before a request times out.
    :param retry_codes: HTTP status codes that trigger a retry.
    :param max_retries: Maximum number of retry attempts.
    :param retry_delay: Base delay (seconds) between retries; doubles on each attempt.
    :return: The successful :class:`httpx.Response`.
    :raises httpx.HTTPStatusError: When a non-retryable HTTP error status is received.
    :raises httpx.TimeoutException: When every attempt times out.
    :raises httpx.RequestError: When a network-level error occurs on every attempt.
    """

    attempt = 0
    last_exc: Exception | None = None

    with httpx.Client(timeout=timeout, headers=headers) as client:
        while attempt <= max_retries:
            try:
                response = client.get(url, params=params)

                if response.status_code in retry_codes:
                    logger.warning(
                        "Retryable status {} from {} (attempt {}/{})",
                        response.status_code,
                        url,
                        attempt + 1,
                        max_retries + 1,
                    )
                    last_exc = httpx.HTTPStatusError(
                        f"Retryable status {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                    # Fall through to retry logic below

                else:
                    # Raise immediately for non-retryable 4xx/5xx errors
                    response.raise_for_status()
                    return response

            except httpx.TimeoutException as exc:
                logger.exception(
                    "Request timed out ({} s) for {} (attempt {}/{}): {}",
                    timeout,
                    url,
                    attempt + 1,
                    max_retries + 1,
                    exc,
                )
                last_exc = exc

            except httpx.HTTPStatusError as exc:
                # Non-retryable HTTP error — fail immediately
                logger.exception(
                    "Non-retryable HTTP error {} for {}: {}",
                    exc.response.status_code,
                    url,
                    exc,
                )
                raise

            except httpx.ConnectError as exc:
                logger.exception(
                    "Connection error for {} (attempt {}/{}): {}",
                    url,
                    attempt + 1,
                    max_retries + 1,
                    exc,
                )
                last_exc = exc

            except httpx.RequestError as exc:
                # Catch-all for other network-level errors (DNS, proxy, etc.)
                logger.exception(
                    "Request error for {} (attempt {}/{}): {}",
                    url,
                    attempt + 1,
                    max_retries + 1,
                    exc,
                )
                last_exc = exc

            attempt += 1
            if attempt <= max_retries:
                delay = retry_delay * (2 ** (attempt - 1))
                logger.debug("Retrying in {:.1f}s ...", delay)
                time.sleep(delay)

    logger.error("All {} attempts failed for {}", max_retries + 1, url)
    raise last_exc  # type: ignore[misc]
