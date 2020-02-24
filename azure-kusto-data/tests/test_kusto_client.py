"""Tests for KustoClient."""
import unittest

import pytest
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data.request import KustoClient, ClientRequestProperties
from mock import patch

from .kusto_client_common import KustoClientTestsMixin, mocked_requests_post

PANDAS = False
try:
    import pandas

    PANDAS = True
except:
    pass


class KustoClientTests(unittest.TestCase, KustoClientTestsMixin):
    """Tests class for KustoClient API"""

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_sanity_query(self, mock_post):
        """Test query V2."""
        client = KustoClient(self.HOST)
        response = client.execute_query("PythonTest", "Deft")
        self._assert_sanity_query_response(response)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_sanity_control_command(self, mock_post):
        """Tests contol command."""
        client = KustoClient(self.HOST)
        response = client.execute_mgmt("NetDefaultDB", ".show version")
        self._assert_sanity_control_command_response(response)

    @pytest.mark.skipif(not PANDAS, reason="requires pandas")
    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_sanity_data_frame(self, mock_post):
        """Tests KustoResponse to pandas.DataFrame."""
        client = KustoClient(self.HOST)
        response = client.execute_query("PythonTest", "Deft")
        data_frame = dataframe_from_result_table(response.primary_results[0])
        self._assert_sanity_data_frame_response(data_frame)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_partial_results(self, mock_post):
        """Tests partial results."""
        client = KustoClient(self.HOST)
        query = """set truncationmaxrecords = 5;
range x from 1 to 10 step 1"""
        properties = ClientRequestProperties()
        properties.set_option(ClientRequestProperties.results_defer_partial_query_failures_option_name, False)
        self.assertRaises(KustoServiceError, client.execute_query, "PythonTest", query, properties)
        properties.set_option(ClientRequestProperties.results_defer_partial_query_failures_option_name, True)
        response = client.execute_query("PythonTest", query, properties)
        self._assert_partial_results_response(response)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_admin_then_query(self, mock_post):
        """Tests admin then query."""
        client = KustoClient(self.HOST)
        query = ".show tables | project DatabaseName, TableName"
        response = client.execute_mgmt("PythonTest", query)
        self._assert_admin_then_query_response(response)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_dynamic(self, mock_post):
        """Tests dynamic responses."""
        client = KustoClient(self.HOST)
        query = """print dynamic(123), dynamic("123"), dynamic("test bad json"),"""
        """ dynamic(null), dynamic('{"rowId":2,"arr":[0,2]}'), dynamic({"rowId":2,"arr":[0,2]})"""
        row = client.execute_query("PythonTest", query).primary_results[0].rows[0]
        self._assert_dynamic_response(row)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_empty_result(self, mock_post):
        """Tests dynamic responses."""
        client = KustoClient(self.HOST)
        query = """print 'a' | take 0"""
        response = client.execute_query("PythonTest", query)
        assert response.primary_results[0]

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_null_values_in_data(self, mock_post):
        """Tests response with null values in non nullable column types"""
        client = KustoClient(self.HOST)
        query = "PrimaryResultName"
        response = client.execute_query("PythonTest", query)

        assert response is not None
