"""Tests client request properties serialization"""

import unittest
from datetime import timedelta
from azure.kusto.data.request import ClientRequestProperties


class ClientRequestPropertiesTests(unittest.TestCase):
    """ClientRequestProperties Tests"""

    def test_properties(self):
        """positive tests"""
        defer = False
        timeout = timedelta(seconds=10)

        crp = ClientRequestProperties()
        crp.set_option(ClientRequestProperties.results_defer_partial_query_failures, defer)
        crp.set_option(ClientRequestProperties.request_timeout, timeout)

        result = crp.to_json()

        assert '"{0}": false'.format(crp.results_defer_partial_query_failures) in result
        assert '"{0}": "0:00:10"'.format(ClientRequestProperties.request_timeout) in result
