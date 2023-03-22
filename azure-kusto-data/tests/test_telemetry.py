import pytest

from azure.kusto.data._telemetry import KustoTracing, KustoTracingAttributes
from azure.kusto.data.client_request_properties import ClientRequestProperties


class TestTelemetry:
    """
    Tests for telemetry class to make sure adding tracing doesn't impact functionality of original code
    """

    @staticmethod
    def plus_one(num):
        KustoTracingAttributes.add_attributes(tracing_attributes={"foo": "bar"})
        return num + 1

    @staticmethod
    async def plus_one_async(num):
        KustoTracingAttributes.add_attributes(tracing_attributes={"foo": "bar"})
        return num + 1

    @staticmethod
    def test_call_func_tracing():
        res = KustoTracing.call_func_tracing(TestTelemetry.plus_one, 1, name_of_span="plus_one")
        assert res == 2

    @staticmethod
    def test_prepare_func_tracing():
        res = KustoTracing.prepare_func_tracing(TestTelemetry.plus_one, name_of_span="plus_one")
        assert res(1) == 2

    @staticmethod
    @pytest.mark.asyncio
    async def test_call_func_tracing_async():
        res = KustoTracing.call_func_tracing_async(TestTelemetry.plus_one_async, 1, name_of_span="plus_one")
        assert await res == 2

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
        attributes = KustoTracingAttributes.create_query_attributes("cluster_test", "database_test", ClientRequestProperties())
        keynames = {"kusto_cluster", "database", "client_request_id"}
        assert isinstance(attributes, dict)
        for key, val in attributes.items():
            assert isinstance(val, str)
        for key in keynames:
            assert key in attributes.keys()
        attributes = KustoTracingAttributes.create_query_attributes("cluster_test", "database_test")
        keynames = {"kusto_cluster", "database"}
        assert isinstance(attributes, dict)
        for key, val in attributes.items():
            assert isinstance(val, str)
        for key in keynames:
            assert key in attributes.keys()

    @staticmethod
    def test_create_ingest_attributes():
        attributes = KustoTracingAttributes.create_streaming_ingest_attributes("cluster_test", "database_test", "table", ClientRequestProperties())
        keynames = {"kusto_cluster", "database", "table", "client_request_id"}
        assert isinstance(attributes, dict)
        for key, val in attributes.items():
            assert isinstance(val, str)
        for key in keynames:
            assert key in attributes.keys()
        attributes = KustoTracingAttributes.create_streaming_ingest_attributes("cluster_test", "database_test", "table")
        keynames = {"kusto_cluster", "database", "table"}
        assert isinstance(attributes, dict)
        for key, val in attributes.items():
            assert isinstance(val, str)
        for key in keynames:
            assert key in attributes.keys()

    @staticmethod
    def test_create_http_attributes():
        attributes = KustoTracingAttributes.create_http_attributes("method_test", "url_test", 0)
        assert attributes == {"component": "http", "http.method": "method_test", "http.url": "url_test", "http.max_redirects": 0}
        headers = {"User-Agent": "user_agent_test"}
        attributes = KustoTracingAttributes.create_http_attributes("method_test", "url_test", 0, headers)
        assert attributes == {
            "component": "http",
            "http.method": "method_test",
            "http.url": "url_test",
            "http.user_agent": "user_agent_test",
            "http.max_redirects": 0,
        }
