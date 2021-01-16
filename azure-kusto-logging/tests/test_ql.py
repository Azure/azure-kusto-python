"""Simulated testing without a Kusto cluster"""
import logging
import time
import threading
from queue import Queue
from logging.handlers import QueueHandler, QueueListener

import pytest

from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.logging import KustoHandler

from test_setup import BaseTestKustoLogging


def do_logging(numberOfMessages):
    nb_of_tests = numberOfMessages
    for i in range(nb_of_tests):
        logging.warning("Test {} warning {} from thread {}".format(__file__, i, threading.get_ident()))


class TestKustoQueueListenerMemoryHandlerLogging(BaseTestKustoLogging):
    @classmethod
    def setup_class(cls):
        super().setup_class()
        if not cls.is_live_testing_ready:
            pytest.skip("No backend end available", allow_module_level=True)

        queue_cap = 5000
        cls.kh = KustoHandler(
            kcsb=cls.kcsb, database=cls.test_db, table=cls.test_table, useStreaming=True, capacity=queue_cap, flushLevel=logging.CRITICAL, retries=[]
        )
        cls.kh.setLevel(logging.DEBUG)

        cls.q = Queue()
        cls.qh = QueueHandler(cls.q)

        cls.ql = QueueListener(cls.q, cls.kh)
        cls.ql.start()

        logger = logging.getLogger()
        logger.addHandler(cls.qh)
        logger.setLevel(logging.DEBUG)

    @classmethod
    def teardown_class(cls):
        cls.ql.stop()
        cls.qh.flush()
        retries = 50
        while retries:
            time.sleep(1)
            if cls.q.empty():
                break
        logging.getLogger().removeHandler(cls.ql)
        super().teardown_class()

    def test_info_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        nb_of_tests = 30000
        for i in range(0, nb_of_tests):
            logging.info("Test %s info %d", __file__, i)
        logging.critical("Flush")
        self.assert_rows_added(nb_of_tests, logging.INFO, timeout=10000)

    def test_debug_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        caplog.set_level(logging.CRITICAL, logger="msal.application")
        nb_of_tests = 40000
        for i in range(0, nb_of_tests):
            logging.debug("Test debug %d", i)
        logging.critical("Flush")
        self.assert_rows_added(nb_of_tests, logging.DEBUG, timeout=500)

    def test_error_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        caplog.set_level(logging.CRITICAL, logger="msal.application")

        nb_of_tests = 20000
        for i in range(0, nb_of_tests):
            logging.error("Test error %d", i)
        logging.critical("Flush")
        self.assert_rows_added(nb_of_tests, logging.ERROR, timeout=500)

    def test_critical_logging(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        caplog.set_level(logging.CRITICAL, logger="msal.application")

        nb_of_tests = 20
        for i in range(0, nb_of_tests):
            logging.critical("Test critical %d", i)
        self.assert_rows_added(nb_of_tests, logging.CRITICAL)

    def test_mt_warning_logging(self, caplog):
        """multithreading test"""
        caplog.set_level(logging.CRITICAL, logger="adal-python")
        caplog.set_level(logging.CRITICAL, logger="urllib3.connectionpool")
        caplog.set_level(logging.CRITICAL, logger="msal.application")

        logging_threads = []
        expected_results = 0
        for i in range(16):
            nb_of_logging = i * 100
            x = threading.Thread(target=do_logging, args=(nb_of_logging,))
            x.start()
            expected_results += nb_of_logging
            logging_threads.append(x)
        for t in logging_threads:
            t.join()
        logging.critical("Flush")
        self.assert_rows_added(expected_results, logging.WARNING)
