# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from io import IOBase
from typing import Awaitable, Callable, Optional, TypeVar, Union

from aiohttp import ClientResponse
from requests import Response
from tenacity import AsyncRetrying, Retrying, RetryCallState, stop_after_attempt, wait_random_exponential

from azure.kusto.data.client_base import ExecuteRequestParams
from azure.kusto.data.exceptions import KustoApiError, KustoNetworkError, KustoServiceError, KustoThrottlingError

_LOGGER = logging.getLogger(__name__)
_MAX_ATTEMPTS = 5
_T = TypeVar("_T")


def _http_response(error: Optional[BaseException]) -> Optional[Union[Response, ClientResponse]]:
    if isinstance(error, KustoServiceError):
        return error.get_raw_http_response()
    if isinstance(error, KustoThrottlingError) and len(error.args) > 1 and isinstance(error.args[1], (Response, ClientResponse)):
        return error.args[1]
    return None


def _retry_after(error: Optional[BaseException]) -> float:
    response = _http_response(error)
    if response is None:
        return 0
    header = response.headers.get("Retry-After")
    if header is None:
        return 0
    try:
        return max(0, float(int(header)))
    except (ValueError, OverflowError):
        try:
            retry_at = parsedate_to_datetime(header)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            return max(0, (retry_at - datetime.now(timezone.utc)).total_seconds())
        except (ValueError, TypeError, OverflowError):
            _LOGGER.warning("Ignoring invalid Retry-After header on an E2E Kusto response.")
            return 0


class _RequestRetry:
    def __init__(self, request: ExecuteRequestParams):
        payload = request.payload
        self._stream = payload if isinstance(payload, IOBase) else None
        self._position = self._stream.tell() if self._stream is not None and self._stream.seekable() else None
        self._replayable = payload is None or isinstance(payload, (bytes, str)) or self._position is not None
        query = request.json_payload.get("csl") if isinstance(request.json_payload, dict) else None
        self._read_only = (
            isinstance(query, str) and bool(query.strip()) and (not query.lstrip().startswith(".") or query.lstrip().lower().startswith((".show ", ".get ")))
        )
        self._backoff = wait_random_exponential(multiplier=1, max=30)

    def should_retry(self, state: RetryCallState) -> bool:
        error = state.outcome.exception() if state.outcome is not None else None
        if not self._replayable or (self._stream is not None and self._stream.closed):
            if isinstance(error, KustoThrottlingError):
                _LOGGER.warning("Cannot retry a throttled E2E Kusto request because its body cannot be replayed.")
            return False
        if isinstance(error, KustoThrottlingError):
            return True
        if not self._read_only:
            return False
        if isinstance(error, KustoApiError) and error.get_api_error().permanent:
            return False
        if isinstance(error, KustoNetworkError):
            return True
        response = _http_response(error)
        if response is None:
            return False
        status = response.status_code if isinstance(response, Response) else response.status
        return status in (408, 500, 502, 503, 504)

    def wait(self, state: RetryCallState) -> float:
        error = state.outcome.exception() if state.outcome is not None else None
        return max(self._backoff(state), _retry_after(error))

    def before_sleep(self, state: RetryCallState) -> None:
        if self._stream is not None and self._position is not None:
            self._stream.seek(self._position)
        error = state.outcome.exception() if state.outcome is not None else None
        delay = state.next_action.sleep if state.next_action is not None else 0
        _LOGGER.warning(
            "Retrying E2E Kusto request after %s; attempt %d/%d in %.2fs.",
            type(error).__name__,
            state.attempt_number + 1,
            _MAX_ATTEMPTS,
            delay,
        )


def retry_request(request: ExecuteRequestParams, operation: Callable[[], _T]) -> _T:
    policy = _RequestRetry(request)
    return Retrying(stop=stop_after_attempt(_MAX_ATTEMPTS), retry=policy.should_retry, wait=policy.wait, before_sleep=policy.before_sleep, reraise=True)(
        operation
    )


async def retry_request_async(request: ExecuteRequestParams, operation: Callable[[], Awaitable[_T]]) -> _T:
    policy = _RequestRetry(request)
    return await AsyncRetrying(
        stop=stop_after_attempt(_MAX_ATTEMPTS), retry=policy.should_retry, wait=policy.wait, before_sleep=policy.before_sleep, reraise=True
    )(operation)
