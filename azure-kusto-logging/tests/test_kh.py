""" Logging testing on a kusto cluster.
"""
import logging
import pytest

from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.logging import KustoHandler

from test_setup import BaseTestKustoLogging


class TestKustoHandlerLogging(BaseTestKustoLogging):
    @classmethod
    def setup_class(cls):
        super().setup_class()
        if not cls.is_live_testing_ready:
            pytest.skip("No backend end available", allow_module_level=True)
        cls.kh = KustoHandler(kcsb=cls.kcsb, database=cls.test_db, table=cls.test_table, useStreaming=True)
        cls.kh.setLevel(logging.DEBUG)
        logger = logging.getLogger()
        logging.getLogger().addHandler(cls.kh)
        logging.getLogger().setLevel(logging.DEBUG)

    @classmethod
    def teardown_class(cls):
        logging.getLogger().removeHandler(cls.kh)
        super().teardown_class()

    def test_info_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        caplog.set_level(logging.CRITICAL, logger="msal.application")
        nb_of_tests = 3
        for i in range(0, nb_of_tests):
            logging.info("Test %s info %d", __file__, i)
        self.kh.flush()
        self.assert_rows_added(nb_of_tests, logging.INFO)

    def test_debug_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        caplog.set_level(logging.CRITICAL, logger="msal.application")
        nb_of_tests = 4
        for i in range(0, nb_of_tests):
            logging.debug("Test debug %d", i)
        self.kh.flush()
        self.assert_rows_added(nb_of_tests, logging.DEBUG)

    def test_error_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        caplog.set_level(logging.CRITICAL, logger="msal.application")
        nb_of_tests = 2
        for i in range(0, nb_of_tests):
            logging.error("Test error %d", i)
        self.kh.flush()
        self.assert_rows_added(nb_of_tests, logging.ERROR)

    def test_critical_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        caplog.set_level(logging.CRITICAL, logger="msal.application")

        nb_of_tests = 1
        for i in range(0, nb_of_tests):
            logging.critical("Test critical %d", i)
        self.kh.flush()
        self.assert_rows_added(nb_of_tests, logging.CRITICAL)
