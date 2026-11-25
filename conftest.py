# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
from typing import Iterator, Optional
from unittest.mock import Mock

import pytest
from aiohttp import ClientResponse

from azure.kusto.data import ClientRequestProperties, KustoClient
from azure.kusto.data.aio import KustoClient as AsyncKustoClient
from azure.kusto.data.client_base import ExecuteRequestParams
from e2e_retry import retry_request, retry_request_async


@pytest.fixture
def aiohttp_mock_response() -> type[ClientResponse]:
    # aioresponses does not yet supply aiohttp 3.14's required stream_writer.
    class MockClientResponse(ClientResponse):
        def __init__(self, *args, **kwargs):
            kwargs["stream_writer"] = Mock(output_size=0)
            super().__init__(*args, **kwargs)

    return MockClientResponse


@pytest.fixture(scope="module", autouse=True)
def e2e_request_retries(request: pytest.FixtureRequest) -> Iterator[None]:
    if request.path.name not in ("test_e2e_data.py", "test_e2e_ingest.py"):
        yield
        return

    # Module scope covers the requests made by E2E class setup as well as tests.
    sync_execute = KustoClient._execute
    async_execute = AsyncKustoClient._execute

    def execute(
        client: KustoClient, endpoint: str, request: ExecuteRequestParams, properties: Optional[ClientRequestProperties] = None, stream_response: bool = False
    ):
        return retry_request(request, lambda: sync_execute(client, endpoint, request, properties, stream_response))

    async def execute_async(
        client: AsyncKustoClient,
        endpoint: str,
        request: ExecuteRequestParams,
        properties: Optional[ClientRequestProperties] = None,
        stream_response: bool = False,
    ):
        return await retry_request_async(request, lambda: async_execute(client, endpoint, request, properties, stream_response))

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(KustoClient, "_execute", execute)
        monkeypatch.setattr(AsyncKustoClient, "_execute", execute_async)
        yield
