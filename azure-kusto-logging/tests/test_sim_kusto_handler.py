"""Simulated logging
"""
import unittest
import logging

from mock import patch

from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.logging import KustoHandler


def mocked_client_execute(*args, **kwargs):
    class MockResponse:
        """Mock class for KustoResponse."""

        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.text = str(json_data)
            self.status_code = status_code
            self.headers = None

        def json(self):
            """Get json data from response."""
            return self.json_data

    if "https://somecluster.kusto.windows.net/v2/" in args[0]:
        return MockResponse(None, 200)

    if "https://somecluster.kusto.windows.net/v1/" in args[0]:
        return MockResponse(None, 200)

    return MockResponse(None, 404)


class KustoHandlerTests(unittest.TestCase):
    """Tests class for KustoHandler.
    Because of the mock, each test must flush (or close)...
    ... or the framework will try to do it during teardown and will show and exception
    """

    @classmethod
    def setup_class(cls):
        cls.kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication("https://somecluster.kusto.windows.net", "a", "b", "c")
        cls.kh = KustoHandler(kcsb=cls.kcsb, database="tst", table="tbl", useStreaming=True, capacity=8192)
        logging.getLogger().addHandler(cls.kh)
        logging.getLogger().setLevel(logging.INFO)

    @classmethod
    def teardown_class(cls):
        logging.getLogger().removeHandler(cls.kh)

    @patch("azure.kusto.data.KustoClient._execute", side_effect=mocked_client_execute)
    def test_info_logging(self, mock_execute):
        prev_rows = len(self.kh.buffer)
        logging.info("Test1")
        assert len(self.kh.buffer) == prev_rows + 1
        self.kh.flush()

    @patch("azure.kusto.data.KustoClient._execute", side_effect=mocked_client_execute)
    def test_info_logging_again(self, mock_execute):
        info_msg = "Test2"
        prev_rows = len(self.kh.buffer)
        logging.info(info_msg)
        assert len(self.kh.buffer) == prev_rows + 1
        assert __name__ in self.kh.buffer[-1].__dict__["filename"]
        assert info_msg == self.kh.buffer[-1].__dict__["message"]
        self.kh.flush()

    @patch("azure.kusto.data.KustoClient._execute", side_effect=mocked_client_execute)
    def test_debug_logging(self, mock_execute):
        prev_rows = len(self.kh.buffer)
        logging.debug("Test3")  # Won't appear
        assert len(self.kh.buffer) == prev_rows
        self.kh.flush()  # not strictly necessary

    @patch("azure.kusto.data.KustoClient._execute", side_effect=mocked_client_execute)
    def test_flush(self, mock_execute):
        self.kh.flush()
        assert len(self.kh.buffer) == 0

    @patch("azure.kusto.data.KustoClient._execute", side_effect=mocked_client_execute)
    def test_close(self, mock_execute):
        self.kh.flush()
        assert len(self.kh.buffer) == 0
        info_msg = "Test Close"
        prev_rows = len(self.kh.buffer)
        logging.info(info_msg)
        assert len(self.kh.buffer) == prev_rows + 1
        self.kh.close()
        assert len(self.kh.buffer) == 0
