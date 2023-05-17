import pytest

from azure.kusto.data._telemetry import MonitoredActivity, Span
from azure.kusto.data.client_request_properties import ClientRequestProperties


def test_run_none_invoker():
    # Edge case test for invoke method with None invoker function
    with pytest.raises(TypeError):
        MonitoredActivity.invoke(None, "test_span")


@pytest.mark.asyncio
async def test_run_async_valid_invoker():
    # Happy path test for invoke_async method with valid invoker function and name of span
    async def invoker():
        return "Hello World"

    span = await MonitoredActivity.invoke_async(invoker, "test_span")
    assert span == "Hello World"


def test_run_valid_invoker():
    # Happy path test for invoke method with valid invoker function and name of span

    def invoker():
        return "Hello World"

    span = MonitoredActivity.invoke(invoker, "test_span")
    assert span == "Hello World"


@pytest.mark.asyncio
async def test_run_async_none_invoker():
    # Edge case test for invoke_async method with None invoker function
    with pytest.raises(TypeError):
        await MonitoredActivity.invoke_async(None, "test_span")


def test_run_sync_behavior():
    # General behavior test for invoke method running the span synchronously
    def invoker():
        return "Hello World"

    span = MonitoredActivity.invoke(invoker, "test_span")
    assert span == "Hello World"


@pytest.mark.asyncio
async def test_run_async_behavior():
    # General behavior test for invoke_async method running the span asynchronously
    async def invoker():
        return "Hello World"

    span = await MonitoredActivity.invoke_async(invoker, "test_span")
    assert span == "Hello World"


def test_tracing_attributes_parameter():
    def invoker():
        return "Hello World"

    tracing_attributes = {"key": "value"}
    result = MonitoredActivity.invoke(invoker, tracing_attributes=tracing_attributes)
    assert result == "Hello World"


def test_get_client_request_properties_attributes():
    attributes = ClientRequestProperties().get_tracing_attributes()
    keynames = {"client_request_id"}
    assert isinstance(attributes, dict)
    for key, val in attributes.items():
        assert key in keynames
        assert isinstance(val, str)
    for key in keynames:
        assert key in attributes.keys()


def test_create_query_attributes():
    attributes = Span.create_query_attributes("cluster_test", "database_test", ClientRequestProperties())
    keynames = {"kusto_cluster", "database", "client_request_id"}
    assert isinstance(attributes, dict)
    for key, val in attributes.items():
        assert isinstance(val, str)
    for key in keynames:
        assert key in attributes.keys()
    attributes = Span.create_query_attributes("cluster_test", "database_test")
    keynames = {"kusto_cluster", "database"}
    assert isinstance(attributes, dict)
    for key, val in attributes.items():
        assert isinstance(val, str)
    for key in keynames:
        assert key in attributes.keys()


def test_create_ingest_attributes():
    attributes = Span.create_streaming_ingest_attributes("cluster_test", "database_test", "table", ClientRequestProperties())
    keynames = {"kusto_cluster", "database", "table", "client_request_id"}
    assert isinstance(attributes, dict)
    for key, val in attributes.items():
        assert isinstance(val, str)
    for key in keynames:
        assert key in attributes.keys()
    attributes = Span.create_streaming_ingest_attributes("cluster_test", "database_test", "table")
    keynames = {"kusto_cluster", "database", "table"}
    assert isinstance(attributes, dict)
    for key, val in attributes.items():
        assert isinstance(val, str)
    for key in keynames:
        assert key in attributes.keys()


def test_create_http_attributes():
    attributes = Span.create_http_attributes("method_test", "url_test")
    assert attributes == {"component": "http", "http.method": "method_test", "http.url": "url_test"}
    headers = {"User-Agent": "user_agent_test"}
    attributes = Span.create_http_attributes("method_test", "url_test", headers)
    assert attributes == {"component": "http", "http.method": "method_test", "http.url": "url_test", "http.user_agent": "user_agent_test"}
