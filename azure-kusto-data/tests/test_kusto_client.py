# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import sys
from unittest.mock import patch

import pandas
import pytest
import requests

from azure.kusto.data import ClientRequestProperties, KustoClient, KustoConnectionStringBuilder
from azure.kusto.data._cloud_settings import CloudSettings
from azure.kusto.data.exceptions import KustoClosedError, KustoMultiApiError, KustoNetworkError, KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data.response import KustoStreamingResponseDataSet
from tests.kusto_client_common import KustoClientTestsMixin, mocked_requests_post, get_response_first_primary_result, get_table_first_row, proxy_kcsb


@pytest.fixture(params=[KustoClient.execute_query, KustoClient.execute_streaming_query])
def method(request):
    return request.param


class TestKustoClient(KustoClientTestsMixin):
    """Tests class for KustoClient API"""

    def test_throws_on_close(self):
        """Test query V2."""
        with KustoClient(self.HOST) as client:
            pass
        with pytest.raises(KustoClosedError):
            client.execute_query("PythonTest", "Deft")

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_sanity_query(self, mock_post, method):
        """Test query V2."""
        with KustoClient(self.HOST) as client:
            response = method.__call__(client, "PythonTest", "Deft")
            self._assert_sanity_query_response(response)
            self._assert_client_request_id(mock_post.call_args[-1])

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_raise_network(self, mock_post, method):
        """Test query V2."""
        with KustoClient(self.HOST) as client:
            with pytest.raises(KustoNetworkError):
                response = method.__call__(client, "PythonTest", "raiseNetwork")

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_sanity_control_command(self, mock_post):
        """Tests contol command."""
        with KustoClient(self.HOST) as client:
            response = client.execute_mgmt("NetDefaultDB", ".show version")
            self._assert_sanity_control_command_response(response)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_sanity_data_frame(self, mock_post, method):
        """Tests KustoResponse to pandas.DataFrame."""
        with KustoClient(self.HOST) as client:
            response = method.__call__(client, "PythonTest", "Deft")
            data_frame = dataframe_from_result_table(get_response_first_primary_result(response))
            self._assert_sanity_data_frame_response(data_frame)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_pandas_bool(self, mock_post):
        """Tests KustoResponse to pandas.DataFrame."""
        with KustoClient(self.HOST) as client:
            from pandas import DataFrame, Series
            from pandas.testing import assert_frame_equal

            columns = ["xbool"]
            response = client.execute_query("PythonTest", "pandas_bool")
            result = get_response_first_primary_result(response)
            assert [x["xbool"] for x in result.rows] == [None, True, False]

            # Without flag - nulls are converted to False

            data_frame = dataframe_from_result_table(result)
            expected_dict = {
                "xbool": Series([False, True, False], dtype=bool),
            }
            expected_data_frame = DataFrame(expected_dict, columns=columns, copy=True)
            assert_frame_equal(data_frame, expected_data_frame)

            # With flag - nulls are converted to pandas.NA

            data_frame = dataframe_from_result_table(result, nullable_bools=True)
            expected_dict = {
                "xbool": Series([pandas.NA, True, False], dtype="boolean"),
            }
            expected_data_frame = DataFrame(expected_dict, columns=columns, copy=True)
            assert_frame_equal(data_frame, expected_data_frame)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_partial_results(self, mock_post, method):
        """Tests partial results."""
        with KustoClient(self.HOST) as client:
            query = """set truncationmaxrecords = 5;
    range x from 1 to 10 step 1"""
            properties = ClientRequestProperties()
            properties.set_option(ClientRequestProperties.results_defer_partial_query_failures_option_name, False)
            with pytest.raises(KustoMultiApiError) as e:
                response = method.__call__(client, "PythonTest", query, properties=properties)
                if type(response) == KustoStreamingResponseDataSet:
                    results = list(get_response_first_primary_result(response))
            errors = e.value.get_api_errors()
            assert len(errors) == 1
            assert errors[0].code == "LimitsExceeded"

            properties.set_option(ClientRequestProperties.results_defer_partial_query_failures_option_name, True)
            response = method.__call__(client, "PythonTest", query, properties=properties)
            self._assert_partial_results_response(response)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_admin_then_query(self, mock_post):
        """Tests admin then query."""
        with KustoClient(self.HOST) as client:
            query = ".show tables | project DatabaseName, TableName"
            response = client.execute_mgmt("PythonTest", query)
            self._assert_admin_then_query_response(response)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_dynamic(self, mock_post, method):
        """Tests dynamic responses."""
        with KustoClient(self.HOST) as client:
            query = """print dynamic(123), dynamic("123"), dynamic("test bad json"),"""
            """ dynamic(null), dynamic('{"rowId":2,"arr":[0,2]}'), dynamic({"rowId":2,"arr":[0,2]})"""
            row = get_table_first_row(get_response_first_primary_result(method.__call__(client, "PythonTest", query)))
            self._assert_dynamic_response(row)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_json_401(self, mock_post, method):
        """Tests 401 permission errors."""
        with KustoClient(self.HOST) as client:
            with pytest.raises(KustoServiceError, match=f"401. Missing adequate access rights."):
                query = "execute_401"
                response = method.__call__(client, "PythonTest", query)
                get_response_first_primary_result(response)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_empty_result(self, mock_post, method):
        """Tests dynamic responses."""
        with KustoClient(self.HOST) as client:
            query = """print 'a' | take 0"""
            response = method.__call__(client, "PythonTest", query)
            assert get_response_first_primary_result(response)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_null_values_in_data(self, mock_post, method):
        """Tests response with null values in non nullable column types"""
        with KustoClient(self.HOST) as client:
            query = "PrimaryResultName"
            response = method.__call__(client, "PythonTest", query)

            assert response is not None

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_unidentifiable_os(self, mock_post, method):
        """Tests unidentifiable OS doesn't fail when composing its socket options"""
        with patch.object(sys, "platform", "win3.1"):
            with KustoClient("https://somecluster.kusto.windows.net") as client:
                query = """print dynamic(123)"""
                row = get_table_first_row(get_response_first_primary_result(method.__call__(client, "PythonTest", query)))
                assert isinstance(row[0], int)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_identifiable_os(self, mock_post, method):
        """Tests identifiable OS doesn't fail when composing its socket options"""
        with patch.object(sys, "platform", "win32"):
            with KustoClient("https://somecluster.kusto.windows.net") as client:
                query = """print dynamic(123)"""
                row = get_table_first_row(get_response_first_primary_result(method.__call__(client, "PythonTest", query)))
                assert isinstance(row[0], int)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_custom_request_id(self, mock_post, method):
        """Test query V2."""
        with KustoClient(self.HOST) as client:
            properties = ClientRequestProperties()
            request_id = "test_request_id"
            properties.client_request_id = request_id
            response = method.__call__(client, "PythonTest", "Deft", properties=properties)
            self._assert_sanity_query_response(response)
            self._assert_client_request_id(mock_post.call_args[-1], value=request_id)

    @patch("requests.Session.get", side_effect=mocked_requests_post)
    def test_proxy_token_providers(self, mock_get, proxy_kcsb):
        """Test query V2."""
        proxy = "https://my_proxy.sample"
        kcsb, auth_supports_proxy = proxy_kcsb
        with KustoClient(kcsb) as client:
            client.set_proxy(proxy)

            assert client._proxy_url == proxy

            expected_dict = {"http": proxy, "https": proxy}
            if not auth_supports_proxy:
                return

            assert client._aad_helper.token_provider._proxy_dict == expected_dict
            assert client._session.proxies == expected_dict

            CloudSettings._cloud_cache.clear()

            client._aad_helper.token_provider._init_resources()

            mock_get.assert_called_with("https://somecluster.kusto.windows.net/v1/rest/auth/metadata", proxies=expected_dict, allow_redirects=False)

    @patch("requests.Session.get", side_effect=mocked_requests_post)
    def test_kusto_client_with_custom_session(self, mock_get, proxy_kcsb):
        """Test query V2."""
        proxy = "https://my_proxy.sample"
        kcsb, auth_supports_proxy = proxy_kcsb
        original_session = requests.Session()
        original_session.cert = ("foo", "bar")
        original_session.verify = "/foo/bar/baz"
        original_session.proxies = {"http": proxy, "https": proxy}
        with KustoClient(kcsb, original_session) as client:
            client.set_proxy(proxy)

            assert client._proxy_url == proxy

            expected_dict = original_session.proxies
            if not auth_supports_proxy:
                return

            assert client._aad_helper.token_provider._proxy_dict == expected_dict

            # assert that the same instance of requests.Session is used with the token provider
            assert client._aad_helper.token_provider._session == original_session
            assert client._session.proxies == expected_dict
            assert client._session.cert == original_session.cert
            assert client._session.verify == original_session.verify

            CloudSettings._cloud_cache.clear()

            client._aad_helper.token_provider._init_resources()

            mock_get.assert_called_with("https://somecluster.kusto.windows.net/v1/rest/auth/metadata", proxies=expected_dict, allow_redirects=False)

    def test_proxy_url_parsing(self):
        """Test Proxy URL Parsing"""
        tests = {
            "https://kusto.test.com": ("https://kusto.test.com", "NetDefaultDB"),
            "https://kusto.test.com/": ("https://kusto.test.com", "NetDefaultDB"),
            "https://kusto.test.com/test": ("https://kusto.test.com", "test"),
            "https://kusto.test.com/test/test2": ("https://kusto.test.com/test/test2", "NetDefaultDB"),
            "https://kusto.test.com:4242": ("https://kusto.test.com:4242", "NetDefaultDB"),
            "https://kusto.test.com:4242/": ("https://kusto.test.com:4242", "NetDefaultDB"),
            "https://kusto.test.com:4242/test": ("https://kusto.test.com:4242", "test"),
            "https://kusto.test.com:4242/test/test2": ("https://kusto.test.com:4242/test/test2", "NetDefaultDB"),
            "https://kusto.test.com;fed=true": ("https://kusto.test.com", "NetDefaultDB"),
            "https://kusto.test.com/;fed=true": ("https://kusto.test.com", "NetDefaultDB"),
            "https://kusto.test.com/test;fed=true": ("https://kusto.test.com", "test"),
            "https://kusto.test.com/test/test2;fed=true": ("https://kusto.test.com/test/test2", "NetDefaultDB"),
            "https://kusto.test.com:4242;fed=true": ("https://kusto.test.com:4242", "NetDefaultDB"),
            "https://kusto.test.com:4242/;fed=true": ("https://kusto.test.com:4242", "NetDefaultDB"),
            "https://kusto.test.com:4242/test;fed=true": ("https://kusto.test.com:4242", "test"),
            "https://kusto.test.com:4242/test/test2;fed=true": ("https://kusto.test.com:4242/test/test2", "NetDefaultDB"),
            "https://ade.loganalytics.io/subscriptions/da45f7ac-97c0-4fff-ac66-3b6810eb4f65/resourcegroups"
            "/some_resource_group/providers/microsoft.operationalinsights/workspaces/some_workspace": (
                "https://ade.loganalytics.io/subscriptions/da45f7ac-97c0-4fff-ac66-3b6810eb4f65/resourcegroups"
                "/some_resource_group/providers/microsoft.operationalinsights/workspaces/some_workspace",
                "NetDefaultDB",
            ),
            "https://ade.loganalytics.io/subscriptions/da45f7ac-97c0-4fff-ac66-3b6810eb4f65/resourcegroups"
            "/some_resource_group/providers/microsoft.operationalinsights/workspaces/some_workspace/": (
                "https://ade.loganalytics.io/subscriptions/da45f7ac-97c0-4fff-ac66-3b6810eb4f65/resourcegroups"
                "/some_resource_group/providers/microsoft.operationalinsights/workspaces/some_workspace",
                "NetDefaultDB",
            ),
            "https://ade.loganalytics.io:4242/subscriptions/da45f7ac-97c0-4fff-ac66-3b6810eb4f65/resourcegroups"
            "/some_resource_group/providers/microsoft.operationalinsights/workspaces/some_workspace/": (
                "https://ade.loganalytics.io:4242/subscriptions/da45f7ac-97c0-4fff-ac66-3b6810eb4f65/resourcegroups"
                "/some_resource_group/providers/microsoft.operationalinsights/workspaces/some_workspace",
                "NetDefaultDB",
            ),
            "https://ade.loganalytics.io:4242/subscriptions/da45f7ac-97c0-4fff-ac66-3b6810eb4f65/resourcegroups"
            "/some_resource_group/providers/microsoft.operationalinsights/workspaces/some_workspace/;fed=true": (
                "https://ade.loganalytics.io:4242/subscriptions/da45f7ac-97c0-4fff-ac66-3b6810eb4f65"
                "/resourcegroups/some_resource_group/providers/microsoft.operationalinsights/workspaces"
                "/some_workspace",
                "NetDefaultDB",
            ),
            "https://kusto.aria.microsoft.com": ("https://kusto.aria.microsoft.com", "NetDefaultDB"),
            "https://kusto.aria.microsoft.com/": ("https://kusto.aria.microsoft.com", "NetDefaultDB"),
            "https://kusto.aria.microsoft.com/;fed=true": ("https://kusto.aria.microsoft.com", "NetDefaultDB"),
        }

        for actual_url, expected in tests.items():
            kcsb = KustoConnectionStringBuilder(actual_url)
            assert (kcsb.data_source, kcsb.initial_catalog) == expected
