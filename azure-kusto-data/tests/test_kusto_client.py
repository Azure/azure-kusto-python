# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import sys

import pytest
from mock import patch

from azure.kusto.data import KustoClient, ClientRequestProperties
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data.response import KustoStreamingResponseDataSet
from tests.kusto_client_common import KustoClientTestsMixin, mocked_requests_post, get_response_first_primary_result, get_table_first_row

PANDAS = False
try:
    import pandas

    PANDAS = True
except:
    pass


@pytest.fixture(params=[KustoClient.execute_query, KustoClient.execute_streaming_query])
def method(request):
    return request.param


class TestKustoClient(KustoClientTestsMixin):
    """Tests class for KustoClient API"""

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_sanity_query(self, mock_post, method):
        """Test query V2."""
        client = KustoClient(self.HOST)
        response = method.__call__(client, "PythonTest", "Deft")
        self._assert_sanity_query_response(response)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_sanity_control_command(self, mock_post):
        """Tests contol command."""
        client = KustoClient(self.HOST)
        response = client.execute_mgmt("NetDefaultDB", ".show version")
        self._assert_sanity_control_command_response(response)

    @pytest.mark.skipif(not PANDAS, reason="requires pandas")
    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_sanity_data_frame(self, mock_post, method):
        """Tests KustoResponse to pandas.DataFrame."""
        client = KustoClient(self.HOST)
        response = method.__call__(client, "PythonTest", "Deft")
        data_frame = dataframe_from_result_table(get_response_first_primary_result(response))
        self._assert_sanity_data_frame_response(data_frame)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_partial_results(self, mock_post, method):
        """Tests partial results."""
        client = KustoClient(self.HOST)
        query = """set truncationmaxrecords = 5;
range x from 1 to 10 step 1"""
        properties = ClientRequestProperties()
        properties.set_option(ClientRequestProperties.results_defer_partial_query_failures_option_name, False)
        with pytest.raises(KustoServiceError):
            response = method.__call__(client, "PythonTest", query, properties=properties)
            if type(response) == KustoStreamingResponseDataSet:
                results = list(get_response_first_primary_result(response))
        properties.set_option(ClientRequestProperties.results_defer_partial_query_failures_option_name, True)
        response = method.__call__(client, "PythonTest", query, properties=properties)
        self._assert_partial_results_response(response)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_admin_then_query(self, mock_post):
        """Tests admin then query."""
        client = KustoClient(self.HOST)
        query = ".show tables | project DatabaseName, TableName"
        response = client.execute_mgmt("PythonTest", query)
        self._assert_admin_then_query_response(response)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_dynamic(self, mock_post, method):
        """Tests dynamic responses."""
        client = KustoClient(self.HOST)
        query = """print dynamic(123), dynamic("123"), dynamic("test bad json"),"""
        """ dynamic(null), dynamic('{"rowId":2,"arr":[0,2]}'), dynamic({"rowId":2,"arr":[0,2]})"""
        row = get_table_first_row(get_response_first_primary_result(method.__call__(client, "PythonTest", query)))
        self._assert_dynamic_response(row)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_empty_result(self, mock_post, method):
        """Tests dynamic responses."""
        client = KustoClient(self.HOST)
        query = """print 'a' | take 0"""
        response = method.__call__(client, "PythonTest", query)
        assert get_response_first_primary_result(response)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_null_values_in_data(self, mock_post, method):
        """Tests response with null values in non nullable column types"""
        client = KustoClient(self.HOST)
        query = "PrimaryResultName"
        response = method.__call__(client, "PythonTest", query)

        assert response is not None

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_unidentifiable_os(self, mock_post, method):
        """Tests unidentifiable OS doesn't fail when composing its socket options"""
        with patch.object(sys, "platform", "win3.1"):
            client = KustoClient("https://somecluster.kusto.windows.net")
            query = """print dynamic(123)"""
            row = get_table_first_row(get_response_first_primary_result(method.__call__(client, "PythonTest", query)))
            assert isinstance(row[0], int)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_identifiable_os(self, mock_post, method):
        """Tests identifiable OS doesn't fail when composing its socket options"""
        with patch.object(sys, "platform", "win32"):
            client = KustoClient("https://somecluster.kusto.windows.net")
            query = """print dynamic(123)"""
            row = get_table_first_row(get_response_first_primary_result(method.__call__(client, "PythonTest", query)))
            assert isinstance(row[0], int)
