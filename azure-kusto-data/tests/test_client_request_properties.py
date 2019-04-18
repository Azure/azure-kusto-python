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
        crp.set_option(ClientRequestProperties.OptionDeferPartialQueryFailures, defer)
        crp.set_option(ClientRequestProperties.OptionServerTimeout, timeout)

        result = crp.to_json()

        assert '"{0}": false'.format(crp.OptionDeferPartialQueryFailures) in result
        assert '"{0}": "0:00:10"'.format(ClientRequestProperties.OptionServerTimeout) in result
