# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License

import unittest
from datetime import timedelta

from azure.kusto.data import ClientRequestProperties


class ClientRequestPropertiesTests(unittest.TestCase):
    """ClientRequestProperties Tests"""

    def test_properties(self):
        """positive tests"""
        defer = False
        timeout = timedelta(seconds=10)

        crp = ClientRequestProperties()
        crp.set_option(ClientRequestProperties.results_defer_partial_query_failures_option_name, defer)
        crp.set_option(ClientRequestProperties.request_timeout_option_name, timeout)

        result = crp.to_json()

        assert '"{0}": false'.format(crp.results_defer_partial_query_failures_option_name) in result
        assert '"{0}": "0:00:10"'.format(ClientRequestProperties.request_timeout_option_name) in result

        assert crp.client_request_id is None
        assert crp.application is None
        assert crp.user is None

        crp.client_request_id = "CRID"
        assert crp.client_request_id == "CRID"

        crp.application = "myApp"
        assert crp.application == "myApp"

        crp.user = "myUser"
        assert crp.user == "myUser"
