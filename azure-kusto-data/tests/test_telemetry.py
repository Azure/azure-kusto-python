import unittest

from azure.kusto.data._telemetry import KustoTracing, KustoTracingAttributes
from azure.kusto.data.client_request_properties import ClientRequestProperties


class TestTelemetry(unittest.TestCase):
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
    async def test_call_func_tracing_async():
        res = KustoTracing.call_func_tracing_async(TestTelemetry.plus_one_async, 1, name_of_span="plus_one")
        assert await res == 2

    @staticmethod
    def test_create_query_attributes():
        client_request_properties = ClientRequestProperties()
        assert isinstance(client_request_properties.get_tracing_attributes(), dict)
        attributes = KustoTracingAttributes.create_query_attributes("cluster_test", "database_test", client_request_properties)
        assert isinstance(attributes, dict)
        for key, val in attributes.items():
            assert isinstance(key, str)
            assert isinstance(val, str)
            assert str.lower(key) == key
            assert " " not in key

    @staticmethod
    def test_create_ingest_attributes():
        client_request_properties = ClientRequestProperties()
        assert isinstance(client_request_properties.get_tracing_attributes(), dict)
        attributes = KustoTracingAttributes.create_streaming_ingest_attributes("cluster_test", "database_test", "table_test", client_request_properties)
        assert isinstance(attributes, dict)
        for key, val in attributes.items():
            assert isinstance(key, str)
            assert isinstance(val, str)
            assert str.lower(key) == key
            assert " " not in key

    @staticmethod
    def test_create_http_attributes():
        attributes = KustoTracingAttributes.create_http_attributes("method_test", "url_test")
        assert isinstance(attributes, dict)
        for key, val in attributes.items():
            assert isinstance(key, str)
            assert isinstance(val, str)
            assert str.lower(key) == key
            assert " " not in key
        headers = {"User-Agent": "user_agent_test"}
        headers_test = False
        attributes = KustoTracingAttributes.create_http_attributes("method_test", "url_test", headers)
        assert isinstance(attributes, dict)
        for key, val in attributes.items():
            if val == "user_agent_test":
                headers_test = True
            assert isinstance(key, str)
            assert isinstance(val, str)
            assert str.lower(key) == key
            assert " " not in key
        assert headers_test

    @staticmethod
    def test_create_cloud_info_attributes():
        attributes = KustoTracingAttributes.create_cloud_info_attributes("url")
        assert isinstance(attributes, dict)
        for key, val in attributes.items():
            assert isinstance(key, str)
            assert isinstance(val, str)
            assert str.lower(key) == key
            assert " " not in key

    @staticmethod
    def test_create_cluster_attributes():
        attributes = KustoTracingAttributes.create_cloud_info_attributes("cluster_uri")
        assert isinstance(attributes, dict)
        for key, val in attributes.items():
            assert isinstance(key, str)
            assert isinstance(val, str)
            assert str.lower(key) == key
            assert " " not in key
