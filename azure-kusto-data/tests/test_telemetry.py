import unittest

from azure.kusto.data._telemetry import KustoTracing, KustoTracingAttributes


class TestTelemetry(unittest.TestCase):
    """
    Tests for telemetry class to make sure adding tracing doesn't impact functionality of original code
    """

    @staticmethod
    def plus_one(num):
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
