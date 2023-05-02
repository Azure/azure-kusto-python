import pytest

from azure.kusto.data._telemetry import Span, SpanAttributes
from azure.kusto.data.client_request_properties import ClientRequestProperties


class TestTelemetry:
    """
    Tests for telemetry class to make sure adding tracing doesn't impact functionality of original code
    """

    @staticmethod
    def plus_one(num):
        SpanAttributes.add_attributes(tracing_attributes={"foo": "bar"})
        return num + 1

    @staticmethod
    async def plus_one_async(num):
        SpanAttributes.add_attributes(tracing_attributes={"foo": "bar"})
        return num + 1

    @staticmethod
    def test_run_valid_invoker():
        # Happy path test for run method with valid invoker function and name of span
        def invoker():
            return "Hello World"

        span = Span.run(invoker, "test_span")
        assert span is not None

    @staticmethod
    def test_run_async_valid_invoker():
        # Happy path test for run_async method with valid invoker function and name of span
        async def invoker():
            return "Hello World"

        span = Span.run_async(invoker, "test_span")
        assert span is not None

    @staticmethod
    def test_run_none_invoker():
        # Edge case test for run method with None invoker function
        with pytest.raises(TypeError):
            Span.run(None, "test_span")

    @staticmethod
    def test_run_async_none_invoker():
        # Edge case test for run_async method with None invoker function
        with pytest.raises(TypeError):
            Span.run_async(None, "test_span")

    @staticmethod
    def test_run_sync_behavior():
        # General behavior test for run method running the span synchronously
        def invoker():
            return "Hello World"

        span = Span.run(invoker, "test_span")
        assert span == "Hello World"

    @staticmethod
    async def test_run_async_behavior():
        # General behavior test for run_async method running the span asynchronously
        async def invoker():
            return "Hello World"

        span = await Span.run_async(invoker, "test_span")
        assert span == "Hello World"

    @staticmethod
    def test_tracing_attributes_parameter():
        def invoker():
            return "Hello World"

        tracing_attributes = {"key": "value"}
        result = Span.run(invoker, tracing_attributes=tracing_attributes)
        assert result == "Hello World"

    @staticmethod
    def test_get_client_request_properties_attributes():
        attributes = ClientRequestProperties().get_tracing_attributes()
        keynames = {"client_request_id"}
        assert isinstance(attributes, dict)
        for key, val in attributes.items():
            assert key in keynames
            assert isinstance(val, str)
        for key in keynames:
            assert key in attributes.keys()

    @staticmethod
    def test_create_query_attributes():
        attributes = SpanAttributes.create_query_attributes("cluster_test", "database_test", ClientRequestProperties())
        keynames = {"kusto_cluster", "database", "client_request_id"}
        assert isinstance(attributes, dict)
        for key, val in attributes.items():
            assert isinstance(val, str)
        for key in keynames:
            assert key in attributes.keys()
        attributes = SpanAttributes.create_query_attributes("cluster_test", "database_test")
        keynames = {"kusto_cluster", "database"}
        assert isinstance(attributes, dict)
        for key, val in attributes.items():
            assert isinstance(val, str)
        for key in keynames:
            assert key in attributes.keys()

    @staticmethod
    def test_create_ingest_attributes():
        attributes = SpanAttributes.create_streaming_ingest_attributes("cluster_test", "database_test", "table", ClientRequestProperties())
        keynames = {"kusto_cluster", "database", "table", "client_request_id"}
        assert isinstance(attributes, dict)
        for key, val in attributes.items():
            assert isinstance(val, str)
        for key in keynames:
            assert key in attributes.keys()
        attributes = SpanAttributes.create_streaming_ingest_attributes("cluster_test", "database_test", "table")
        keynames = {"kusto_cluster", "database", "table"}
        assert isinstance(attributes, dict)
        for key, val in attributes.items():
            assert isinstance(val, str)
        for key in keynames:
            assert key in attributes.keys()

    @staticmethod
    def test_create_http_attributes():
        attributes = SpanAttributes.create_http_attributes("method_test", "url_test")
        assert attributes == {"component": "http", "http.method": "method_test", "http.url": "url_test"}
        headers = {"User-Agent": "user_agent_test"}
        attributes = SpanAttributes.create_http_attributes("method_test", "url_test", headers)
        assert attributes == {"component": "http", "http.method": "method_test", "http.url": "url_test", "http.user_agent": "user_agent_test"}
