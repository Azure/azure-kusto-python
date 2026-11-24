# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from io import BytesIO
from unittest.mock import AsyncMock, Mock, patch

import pytest
from aioresponses import aioresponses
from requests import Response

from azure.kusto.data import ClientRequestProperties, KustoClient
from azure.kusto.data.aio import KustoClient as AsyncKustoClient
from azure.kusto.data.client_base import ExecuteRequestParams
from azure.kusto.data.exceptions import KustoApiError, KustoNetworkError, KustoServiceError, KustoThrottlingError
from e2e_retry import retry_request, retry_request_async


@pytest.fixture(autouse=True)
def retry_sleeps():
    with patch("time.sleep") as sync_sleep, patch("asyncio.sleep", new_callable=AsyncMock) as async_sleep:
        yield sync_sleep, async_sleep


def make_request(query="table | count", stream=None):
    with KustoClient("https://dummy.kusto.windows.net") as client:
        return ExecuteRequestParams(
            stream,
            {"csl": query} if stream is None else None,
            {},
            "test;",
            ClientRequestProperties(),
            timedelta(seconds=1),
            timedelta(seconds=1),
            timedelta(0),
            client.client_details,
        )


def make_error(status, headers=None):
    response = Response()
    response.status_code = status
    response.headers.update(headers or {})
    if status == 429:
        return KustoThrottlingError("Throttled", response)
    return KustoServiceError("Request failed", response)


@pytest.mark.parametrize("status", [429, 503])
def test_retry_transient_query(status, caplog):
    operation = Mock(side_effect=[make_error(status), "ok"])

    assert retry_request(make_request(), operation) == "ok"

    assert operation.call_count == 2
    assert "attempt 2/5" in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 503])
async def test_retry_transient_query_async(status):
    operation = AsyncMock(side_effect=[make_error(status), "ok"])

    assert await retry_request_async(make_request(), operation) == "ok"

    assert operation.await_count == 2


@pytest.mark.asyncio
async def test_async_client_throttling_response_respects_retry_after(retry_sleeps, aiohttp_mock_response):
    endpoint = "https://dummy.kusto.windows.net/v1/rest/mgmt"
    with aioresponses() as responses:
        responses.post(endpoint, status=429, payload={}, headers={"Retry-After": "60"}, response_class=aiohttp_mock_response)
        responses.post(
            endpoint,
            payload={"Tables": [{"TableName": "Table_0", "Columns": [{"ColumnName": "TableName", "DataType": "String"}], "Rows": []}]},
            response_class=aiohttp_mock_response,
        )
        async with AsyncKustoClient("https://dummy.kusto.windows.net") as client:
            await retry_request_async(make_request(".show tables"), lambda: client.execute_mgmt("db", ".show tables"))

        assert sum(len(calls) for calls in responses.requests.values()) == 2
        assert retry_sleeps[1].await_args.args[0] == 60


def test_retry_exhaustion_preserves_failure_and_exponential_delays(retry_sleeps):
    error = make_error(429)
    operation = Mock(side_effect=error)
    with patch("random.uniform", side_effect=lambda low, high: high):
        with pytest.raises(KustoThrottlingError) as raised:
            retry_request(make_request(), operation)

    assert raised.value is error
    assert operation.call_count == 5
    assert [call.args[0] for call in retry_sleeps[0].call_args_list] == [1, 2, 4, 8]


@pytest.mark.parametrize("status", [400, 401, 403])
def test_permanent_failure_is_not_retried(status):
    error = make_error(status)
    operation = Mock(side_effect=error)

    with pytest.raises(KustoServiceError) as raised:
        retry_request(make_request(), operation)

    assert raised.value is error
    assert operation.call_count == 1


def test_permanent_api_failure_is_not_retried():
    response = Response()
    response.status_code = 500
    error = KustoApiError({"error": {"code": "Permanent", "message": "Permanent failure", "@permanent": True}}, http_response=response)
    operation = Mock(side_effect=error)

    with pytest.raises(KustoApiError):
        retry_request(make_request(), operation)

    assert operation.call_count == 1


@pytest.mark.parametrize("query", [".ingest inline into table t <| 1", ".create table t (x:int)"])
def test_ambiguous_write_failure_is_not_retried(query):
    operation = Mock(side_effect=make_error(503))

    with pytest.raises(KustoServiceError):
        retry_request(make_request(query), operation)

    assert operation.call_count == 1


@pytest.mark.parametrize("query, retryable", [("table | count", True), (".ingest inline into table t <| 1", False)])
def test_network_failure_is_retried_only_for_reads(query, retryable):
    operation = Mock(side_effect=[KustoNetworkError("https://dummy.kusto.windows.net"), "ok"])

    if retryable:
        assert retry_request(make_request(query), operation) == "ok"
        assert operation.call_count == 2
    else:
        with pytest.raises(KustoNetworkError):
            retry_request(make_request(query), operation)
        assert operation.call_count == 1


def test_throttled_stream_is_rewound_to_original_position():
    stream = BytesIO(b"prefix-data")
    stream.seek(7)
    bodies = []

    def ingest():
        bodies.append(stream.read())
        if len(bodies) == 1:
            raise make_error(429)
        return "ok"

    assert retry_request(make_request(stream=stream), ingest) == "ok"
    assert bodies == [b"data", b"data"]


def test_non_replayable_stream_is_not_retried():
    class NonReplayableStream(BytesIO):
        def seekable(self):
            return False

    operation = Mock(side_effect=make_error(429))

    with pytest.raises(KustoThrottlingError):
        retry_request(make_request(stream=NonReplayableStream(b"data")), operation)

    assert operation.call_count == 1


@pytest.mark.parametrize("http_date", [False, True])
def test_retry_after_is_respected(http_date, retry_sleeps):
    header = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=60), usegmt=True) if http_date else "60"
    operation = Mock(side_effect=[make_error(429, {"Retry-After": header}), "ok"])

    assert retry_request(make_request(), operation) == "ok"

    assert 58 <= retry_sleeps[0].call_args.args[0] <= 60


def test_invalid_retry_after_warns_and_uses_backoff(caplog):
    operation = Mock(side_effect=[make_error(429, {"Retry-After": "invalid"}), "ok"])

    assert retry_request(make_request(), operation) == "ok"

    assert "invalid Retry-After" in caplog.text
