"""Tests for KustoClient."""
import json
import sys
from unittest.mock import patch

import pytest

from azure.kusto.data._cloud_settings import CloudSettings
from azure.kusto.data._decorators import aio_documented_by
from azure.kusto.data.client import ClientRequestProperties
from azure.kusto.data.exceptions import KustoMultiApiError
from azure.kusto.data.helpers import dataframe_from_result_table
from ..kusto_client_common import KustoClientTestsMixin, mocked_requests_post, proxy_kcsb
from ..test_kusto_client import TestKustoClient as KustoClientTestsSync

PANDAS = False
try:
    import pandas

    PANDAS = True
except:
    pass

run_aio_tests = False
try:
    from azure.kusto.data.aio.client import KustoClient
    from aioresponses import aioresponses, CallbackResult

    run_aio_tests = True
except:
    pass

if sys.version_info < (3, 6):
    run_aio_tests = False


@pytest.mark.skipif(not run_aio_tests, reason="requires aio")
@aio_documented_by(KustoClientTestsSync)
class TestKustoClient(KustoClientTestsMixin):
    @staticmethod
    def _mock_callback(url, **kwargs):
        body = json.dumps(mocked_requests_post(str(url), **kwargs).json())
        return CallbackResult(status=200, body=body)

    def _mock_query(self, aioresponses_mock):
        url = "{host}/v2/rest/query".format(host=self.HOST)
        aioresponses_mock.post(url, callback=self._mock_callback)

    def _mock_mgmt(self, aioresponses_mock):
        url = "{host}/v1/rest/mgmt".format(host=self.HOST)
        aioresponses_mock.post(url, callback=self._mock_callback)

    def _mock_cloud_info(self, aioresponses_mock):
        url = "{host}/v1/rest/auth/metadata".format(host=self.HOST)
        aioresponses_mock.get(url, callback=self._mock_callback)

    @aio_documented_by(KustoClientTestsSync.test_sanity_query)
    @pytest.mark.asyncio
    async def test_sanity_query(self):
        with aioresponses() as aioresponses_mock:
            self._mock_query(aioresponses_mock)
            async with KustoClient(self.HOST) as client:
                response = await client.execute_query("PythonTest", "Deft")
            first_request = next(iter(aioresponses_mock.requests.values()))
            self._assert_client_request_id(first_request[0].kwargs)
        self._assert_sanity_query_response(response)

    @aio_documented_by(KustoClientTestsSync.test_sanity_control_command)
    @pytest.mark.asyncio
    async def test_sanity_control_command(self):
        with aioresponses() as aioresponses_mock:
            self._mock_mgmt(aioresponses_mock)
            async with KustoClient(self.HOST) as client:
                response = await client.execute_mgmt("NetDefaultDB", ".show version")
        self._assert_sanity_control_command_response(response)

    @pytest.mark.skipif(not PANDAS, reason="requires pandas")
    @aio_documented_by(KustoClientTestsSync.test_sanity_data_frame)
    @pytest.mark.asyncio
    async def test_sanity_data_frame(self):
        with aioresponses() as aioresponses_mock:
            self._mock_query(aioresponses_mock)
            async with KustoClient(self.HOST) as client:
                response = await client.execute_query("PythonTest", "Deft")
        data_frame = dataframe_from_result_table(response.primary_results[0])
        self._assert_sanity_data_frame_response(data_frame)

    @aio_documented_by(KustoClientTestsSync.test_partial_results)
    @pytest.mark.asyncio
    async def test_partial_results(self):
        async with KustoClient(self.HOST) as client:
            query = """set truncationmaxrecords = 5;
range x from 1 to 10 step 1"""
            properties = ClientRequestProperties()
            properties.set_option(ClientRequestProperties.results_defer_partial_query_failures_option_name, False)
            with aioresponses() as aioresponses_mock:
                self._mock_query(aioresponses_mock)
                with pytest.raises(KustoMultiApiError) as e:
                    await client.execute_query("PythonTest", query, properties)
                errors = e.value.get_api_errors()
                assert len(errors) == 1
                assert errors[0].code == "LimitsExceeded"

                properties.set_option(ClientRequestProperties.results_defer_partial_query_failures_option_name, True)
                self._mock_query(aioresponses_mock)
                response = await client.execute_query("PythonTest", query, properties)
            self._assert_partial_results_response(response)

    @aio_documented_by(KustoClientTestsSync.test_admin_then_query)
    @pytest.mark.asyncio
    async def test_admin_then_query(self):
        with aioresponses() as aioresponses_mock:
            self._mock_mgmt(aioresponses_mock)
            async with KustoClient(self.HOST) as client:
                query = ".show tables | project DatabaseName, TableName"
                response = await client.execute_mgmt("PythonTest", query)
        self._assert_admin_then_query_response(response)

    @aio_documented_by(KustoClientTestsSync.test_dynamic)
    @pytest.mark.asyncio
    async def test_dynamic(self):
        with aioresponses() as aioresponses_mock:
            self._mock_query(aioresponses_mock)
            async with KustoClient(self.HOST) as client:
                query = """print dynamic(123), dynamic("123"), dynamic("test bad json"),"""
                """ dynamic(null), dynamic('{"rowId":2,"arr":[0,2]}'), dynamic({"rowId":2,"arr":[0,2]})"""
                response = await client.execute_query("PythonTest", query)
        row = response.primary_results[0].rows[0]
        self._assert_dynamic_response(row)

    @aio_documented_by(KustoClientTestsSync.test_empty_result)
    @pytest.mark.asyncio
    async def test_empty_result(self):
        with aioresponses() as aioresponses_mock:
            self._mock_query(aioresponses_mock)
            async with KustoClient(self.HOST) as client:
                query = """print 'a' | take 0"""
                response = await client.execute_query("PythonTest", query)
            assert response.primary_results[0]

    @aio_documented_by(KustoClientTestsSync.test_null_values_in_data)
    @pytest.mark.asyncio
    async def test_null_values_in_data(self):
        with aioresponses() as aioresponses_mock:
            self._mock_query(aioresponses_mock)
            async with KustoClient(self.HOST) as client:
                query = "PrimaryResultName"
                response = await client.execute_query("PythonTest", query)
            assert response is not None

    @aio_documented_by(KustoClientTestsSync.test_sanity_query)
    @pytest.mark.asyncio
    async def test_request_id(self):
        with aioresponses() as aioresponses_mock:
            properties = ClientRequestProperties()
            request_id = "test_request_id"
            properties.client_request_id = request_id
            self._mock_query(aioresponses_mock)
            async with KustoClient(self.HOST) as client:
                response = await client.execute_query("PythonTest", "Deft", properties=properties)
            first_request = next(iter(aioresponses_mock.requests.values()))
            self._assert_client_request_id(first_request[0].kwargs, value=request_id)
        self._assert_sanity_query_response(response)

    @aio_documented_by(KustoClientTestsSync.test_proxy_token_providers)
    @pytest.mark.asyncio
    async def test_proxy_token_providers(self, proxy_kcsb):
        """Test query V2."""
        proxy = "https://my_proxy.sample"
        kcsb, auth_supports_proxy = proxy_kcsb
        async with KustoClient(kcsb) as client:
            client.set_proxy(proxy)

            assert client._proxy_url == proxy

            expected_dict = {"http": proxy, "https": proxy}
            if not auth_supports_proxy:
                return

            assert client._aad_helper.token_provider._proxy_dict == expected_dict

            CloudSettings._cloud_cache.clear()
            with patch("requests.get", side_effect=mocked_requests_post) as mock_get:
                client._aad_helper.token_provider._init_resources()

                mock_get.assert_called_with("https://somecluster.kusto.windows.net/v1/rest/auth/metadata", proxies=expected_dict)
