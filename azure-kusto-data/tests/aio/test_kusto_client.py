"""Tests for KustoClient."""
import json
import sys
import unittest

import pytest
from azure.kusto.data._decorators import aio_documented_by
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data.client import ClientRequestProperties

from ..kusto_client_common import KustoClientTestsMixin, mocked_requests_post
from ..test_kusto_client import KustoClientTests as KustoClientTestsSync

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

    @aio_documented_by(KustoClientTestsSync.test_sanity_query)
    @pytest.mark.asyncio
    async def test_sanity_query(self):
        with aioresponses() as aioresponses_mock:
            self._mock_query(aioresponses_mock)
            client = KustoClient(self.HOST)
            response = await client.execute_query("PythonTest", "Deft")
        self._assert_sanity_query_response(response)

    @aio_documented_by(KustoClientTestsSync.test_sanity_control_command)
    @pytest.mark.asyncio
    async def test_sanity_control_command(self):
        with aioresponses() as aioresponses_mock:
            self._mock_mgmt(aioresponses_mock)
            client = KustoClient(self.HOST)
            response = await client.execute_mgmt("NetDefaultDB", ".show version")
        self._assert_sanity_control_command_response(response)

    @pytest.mark.skipif(not PANDAS, reason="requires pandas")
    @aio_documented_by(KustoClientTestsSync.test_sanity_data_frame)
    @pytest.mark.asyncio
    async def test_sanity_data_frame(self):
        with aioresponses() as aioresponses_mock:
            self._mock_query(aioresponses_mock)
            client = KustoClient(self.HOST)
            response = await client.execute_query("PythonTest", "Deft")
        data_frame = dataframe_from_result_table(response.primary_results[0])
        self._assert_sanity_data_frame_response(data_frame)

    @aio_documented_by(KustoClientTestsSync.test_partial_results)
    @pytest.mark.asyncio
    async def test_partial_results(self):
        client = KustoClient(self.HOST)
        query = """set truncationmaxrecords = 5;
range x from 1 to 10 step 1"""
        properties = ClientRequestProperties()
        properties.set_option(ClientRequestProperties.results_defer_partial_query_failures_option_name, False)
        with aioresponses() as aioresponses_mock:
            self._mock_query(aioresponses_mock)
            with pytest.raises(KustoServiceError):
                await client.execute_query("PythonTest", query, properties)
            properties.set_option(ClientRequestProperties.results_defer_partial_query_failures_option_name, True)
            self._mock_query(aioresponses_mock)
            response = await client.execute_query("PythonTest", query, properties)
        self._assert_partial_results_response(response)

    @aio_documented_by(KustoClientTestsSync.test_admin_then_query)
    @pytest.mark.asyncio
    async def test_admin_then_query(self):
        with aioresponses() as aioresponses_mock:
            self._mock_mgmt(aioresponses_mock)
            client = KustoClient(self.HOST)
            query = ".show tables | project DatabaseName, TableName"
            response = await client.execute_mgmt("PythonTest", query)
        self._assert_admin_then_query_response(response)

    @aio_documented_by(KustoClientTestsSync.test_dynamic)
    @pytest.mark.asyncio
    async def test_dynamic(self):
        with aioresponses() as aioresponses_mock:
            self._mock_query(aioresponses_mock)
            client = KustoClient(self.HOST)
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
            client = KustoClient(self.HOST)
            query = """print 'a' | take 0"""
            response = await client.execute_query("PythonTest", query)
        assert response.primary_results[0]

    @aio_documented_by(KustoClientTestsSync.test_null_values_in_data)
    @pytest.mark.asyncio
    async def test_null_values_in_data(self):
        with aioresponses() as aioresponses_mock:
            self._mock_query(aioresponses_mock)
            client = KustoClient(self.HOST)
            query = "PrimaryResultName"
            response = await client.execute_query("PythonTest", query)
        assert response is not None
