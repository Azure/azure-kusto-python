# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License

import asyncio
import json
import os
import platform
from datetime import datetime
from typing import Optional, ClassVar

import pytest
from azure.identity import DefaultAzureCredential

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data._cloud_settings import CloudSettings
from azure.kusto.data._models import WellKnownDataSet
from azure.kusto.data._token_providers import AsyncDefaultAzureCredential
from azure.kusto.data.aio import KustoClient as AsyncKustoClient
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.kusto_trusted_endpoints import MatchRule, well_known_kusto_endpoints
from azure.kusto.data.streaming_response import FrameType


@pytest.fixture(params=["ManagedStreaming", "NormalClient"])
def is_managed_streaming(request):
    return request.param == "ManagedStreaming"


class TestE2E:
    """A class to define mappings to deft table."""

    input_folder_path: ClassVar[str]
    streaming_test_table: ClassVar[str]
    streaming_test_table_query: ClassVar[str]
    ai_test_table_cmd: ClassVar[str]
    test_streaming_data: ClassVar[list]
    engine_cs: ClassVar[Optional[str]]
    ai_engine_cs: ClassVar[Optional[str]]
    app_id: ClassVar[Optional[str]]
    app_key: ClassVar[Optional[str]]
    auth_id: ClassVar[Optional[str]]
    test_db: ClassVar[Optional[str]]
    ai_test_db: ClassVar[Optional[str]]

    CHUNK_SIZE = 1024

    @staticmethod
    def table_json_mapping_reference():
        """A method to get json mappings reference to test table."""
        return """'['
                    '    { "column" : "rownumber", "datatype" : "int", "Properties":{"Path":"$.rownumber"}},'
                    '    { "column" : "rowguid", "datatype" : "string", "Properties":{"Path":"$.rowguid"}},'
                    '    { "column" : "xdouble", "datatype" : "real", "Properties":{"Path":"$.xdouble"}},'
                    '    { "column" : "xfloat", "datatype" : "real", "Properties":{"Path":"$.xfloat"}},'
                    '    { "column" : "xbool", "datatype" : "bool", "Properties":{"Path":"$.xbool"}},'
                    '    { "column" : "xint16", "datatype" : "int", "Properties":{"Path":"$.xint16"}},'
                    '    { "column" : "xint32", "datatype" : "int", "Properties":{"Path":"$.xint32"}},'
                    '    { "column" : "xint64", "datatype" : "long", "Properties":{"Path":"$.xint64"}},'
                    '    { "column" : "xuint8", "datatype" : "long", "Properties":{"Path":"$.xuint8"}},'
                    '    { "column" : "xuint16", "datatype" : "long", "Properties":{"Path":"$.xuint16"}},'
                    '    { "column" : "xuint32", "datatype" : "long", "Properties":{"Path":"$.xuint32"}},'
                    '    { "column" : "xuint64", "datatype" : "long", "Properties":{"Path":"$.xuint64"}},'
                    '    { "column" : "xdate", "datatype" : "datetime", "Properties":{"Path":"$.xdate"}},'
                    '    { "column" : "xsmalltext", "datatype" : "string", "Properties":{"Path":"$.xsmalltext"}},'
                    '    { "column" : "xtext", "datatype" : "string", "Properties":{"Path":"$.xtext"}},'
                    '    { "column" : "xnumberAsText", "datatype" : "string", "Properties":{"Path":"$.rowguid"}},'
                    '    { "column" : "xtime", "datatype" : "timespan", "Properties":{"Path":"$.xtime"}},'
                    '    { "column" : "xtextWithNulls", "datatype" : "string", "Properties":{"Path":"$.xtextWithNulls"}},'
                    '    { "column" : "xdynamicWithNulls", "datatype" : "dynamic", "Properties":{"Path":"$.xdynamicWithNulls"}},'
                    ']'"""

    @staticmethod
    def application_insights_tables():
        """A method to get the tables of an application insights instance"""
        return [
            "availabilityResults",
            "browserTimings",
            "customEvents",
            "customMetrics",
            "dependencies",
            "exceptions",
            "pageViews",
            "performanceCounters",
            "requests",
            "traces",
        ]

    @staticmethod
    def get_file_path() -> str:
        current_dir = os.getcwd()
        path_parts = ["azure-kusto-data", "tests", "input"]
        missing_path_parts = []
        for path_part in path_parts:
            if path_part not in current_dir:
                missing_path_parts.append(path_part)
        return os.path.join(current_dir, *missing_path_parts)

    @classmethod
    def engine_kcsb_from_env(cls, app_insights=False, is_async=False) -> KustoConnectionStringBuilder:
        engine = cls.engine_cs if not app_insights else cls.ai_engine_cs
        if all([cls.app_id, cls.app_key, cls.auth_id]):
            return KustoConnectionStringBuilder.with_azure_token_credential(
                engine, credential=DefaultAzureCredential() if not is_async else AsyncDefaultAzureCredential()
            )
        else:
            return KustoConnectionStringBuilder.with_interactive_login(engine)

    @classmethod
    def setup_class(cls):
        cls.engine_cs = os.environ.get("ENGINE_CONNECTION_STRING") or ""
        cls.ai_engine_cs = os.environ.get("APPLICATION_INSIGHTS_ENGINE_CONNECTION_STRING") or ""
        cls.app_id = os.environ.get("APP_ID")
        if cls.app_id:
            os.environ["AZURE_CLIENT_ID"] = cls.app_id
        cls.app_key = os.environ.get("APP_KEY")
        if cls.app_key:
            os.environ["AZURE_CLIENT_SECRET"] = cls.app_key
        cls.auth_id = os.environ.get("AUTH_ID")
        if cls.auth_id:
            os.environ["AZURE_TENANT_ID"] = cls.auth_id
        os.environ["AZURE_AUTHORITY_HOST"] = "login.microsoftonline.com"
        cls.test_db = os.environ.get("TEST_DATABASE")
        cls.ai_test_db = os.environ.get("APPLICATION_INSIGHTS_TEST_DATABASE")  # name of e2e database could be changed

        if not all([cls.engine_cs, cls.test_db, cls.ai_engine_cs, cls.ai_test_db]):
            pytest.skip("E2E environment is missing")

        # Init clients
        cls.streaming_test_table = "BigChunkus"
        cls.streaming_test_table_query = cls.streaming_test_table + " | order by timestamp"

        cls.ai_test_table_cmd = ".show tables"

        cls.input_folder_path = cls.get_file_path()

        with open(os.path.join(cls.input_folder_path, "big.json")) as f:
            cls.test_streaming_data = json.load(f)

    @classmethod
    async def get_async_client(cls, app_insights=False) -> AsyncKustoClient:
        return AsyncKustoClient(cls.engine_kcsb_from_env(app_insights, is_async=True))

    @classmethod
    def get_client(cls, app_insights=False) -> KustoClient:
        kcsb = cls.engine_kcsb_from_env(app_insights, is_async=False)
        kcsb.user_name_for_tracing = "E2E_Test_ø"
        return KustoClient(kcsb)

    @staticmethod
    def normalize_row(row):
        result = []
        for r in row:
            if type(r) == bool:
                result.append(int(r))
            elif type(r) == datetime:
                result.append(r.strftime("%Y-%m-%dT%H:%M:%SZ"))
            else:
                result.append(r)
        return result

    # assertions

    def test_streaming_query(self):
        with self.get_client() as client:
            result = client.execute_streaming_query(self.test_db, self.streaming_test_table_query + ";" + self.streaming_test_table_query)
            counter = 0

            result.set_skip_incomplete_tables(True)
            for primary in result.iter_primary_results():
                counter += 1
                for row in self.test_streaming_data:
                    assert row == self.normalize_row(next(primary).to_list())

            assert counter == 2

            assert result.finished
            assert result.errors_count == 0
            assert result.get_exceptions() == []

    @pytest.mark.asyncio
    async def test_streaming_query_async(self):
        async with await self.get_async_client() as client:
            result = await client.execute_streaming_query(self.test_db, self.streaming_test_table_query + ";" + self.streaming_test_table_query)
            counter = 0

            result.set_skip_incomplete_tables(True)
            async for primary in result.iter_primary_results():
                counter += 1
                streaming_data_iter = iter(self.test_streaming_data)
                async for row in primary:
                    expected_row = next(streaming_data_iter, None)
                    if expected_row is None:
                        break

                    assert expected_row == self.normalize_row(row.to_list())

            assert counter == 2
            assert result.finished
            assert result.errors_count == 0
            assert result.get_exceptions() == []

    def test_streaming_query_internal(self):
        with self.get_client() as client:
            frames = client._execute_streaming_query_parsed(self.test_db, self.streaming_test_table_query)

            initial_frame = next(frames)
            expected_initial_frame = {
                "FrameType": FrameType.DataSetHeader,
                "IsProgressive": False,
                "Version": "v2.0",
            }
            assert initial_frame == expected_initial_frame
            query_props = next(frames)
            assert query_props["FrameType"] == FrameType.DataTable
            assert query_props["TableKind"] == WellKnownDataSet.QueryProperties.value
            assert type(query_props["Columns"]) == list
            assert type(query_props["Rows"]) == list
            assert len(query_props["Rows"][0]) == len(query_props["Columns"])

            primary_result = next(frames)
            assert primary_result["FrameType"] == FrameType.DataTable
            assert primary_result["TableKind"] == WellKnownDataSet.PrimaryResult.value
            assert type(primary_result["Columns"]) == list
            assert type(primary_result["Rows"]) != list

            row = next(primary_result["Rows"])
            assert len(row) == len(primary_result["Columns"])

    @pytest.mark.asyncio
    async def test_streaming_query_internal_async(self):
        async with await self.get_async_client() as client:
            frames = await client._execute_streaming_query_parsed(self.test_db, self.streaming_test_table_query)
            frames.__aiter__()
            initial_frame = await frames.__anext__()
            expected_initial_frame = {
                "FrameType": FrameType.DataSetHeader,
                "IsProgressive": False,
                "Version": "v2.0",
            }
            assert initial_frame == expected_initial_frame
            query_props = await frames.__anext__()
            assert query_props["FrameType"] == FrameType.DataTable
            assert query_props["TableKind"] == WellKnownDataSet.QueryProperties.value
            assert type(query_props["Columns"]) == list
            assert type(query_props["Rows"]) == list
            assert len(query_props["Rows"][0]) == len(query_props["Columns"])

            primary_result = await frames.__anext__()
            assert primary_result["FrameType"] == FrameType.DataTable
            assert primary_result["TableKind"] == WellKnownDataSet.PrimaryResult.value
            assert type(primary_result["Columns"]) == list
            assert type(primary_result["Rows"]) != list

            row = await primary_result["Rows"].__anext__()
            assert len(row) == len(primary_result["Columns"])

    def test_log_analytics_query(self):
        with self.get_client(True) as client:
            result = client.execute_mgmt(self.ai_test_db, self.ai_test_table_cmd)
            counter = 0
            expected_table_name = iter(self.application_insights_tables())

            for primary in result.primary_results:
                counter += 1
                for row in primary.rows:
                    assert row["TableName"] == expected_table_name.__next__()

            assert counter == 1
            assert result.errors_count == 0
            assert result.get_exceptions() == []

    @pytest.mark.asyncio
    async def test_log_analytics_query_async(self):
        async with await self.get_async_client(True) as ai_client:
            result = await ai_client.execute_mgmt(self.ai_test_db, self.ai_test_table_cmd)
            counter = 0
            expected_table_name = iter(self.application_insights_tables())

            for primary in result.primary_results:
                counter += 1
                for row in primary.rows:
                    assert row["TableName"] == expected_table_name.__next__()

            assert counter == 1
            assert result.errors_count == 0
            assert result.get_exceptions() == []

    def test_cloud_info(self):
        cloud_info = CloudSettings.get_cloud_info_for_cluster(self.engine_cs)
        assert cloud_info is not CloudSettings.DEFAULT_CLOUD
        assert cloud_info == CloudSettings.DEFAULT_CLOUD
        assert cloud_info is CloudSettings.get_cloud_info_for_cluster(self.engine_cs)

    def test_cloud_info_404(self):
        cloud_info = CloudSettings.get_cloud_info_for_cluster("https://httpstat.us/404")
        assert cloud_info is CloudSettings.DEFAULT_CLOUD

    @pytest.mark.parametrize("code", [301, 302, 307, 308])
    def test_no_redirects_fail_in_cloud(self, code):
        with KustoClient(
            KustoConnectionStringBuilder.with_azure_token_credential(f"https://statusreturner.azurewebsites.net/{code}/nocloud/test", DefaultAzureCredential())
        ) as client:
            with pytest.raises(KustoServiceError) as ex:
                client.execute("db", "table")
            assert ex.value.http_response.status_code == code

    @pytest.mark.parametrize("code", [301, 302, 307, 308])
    def test_no_redirects_fail_in_client(self, code):
        well_known_kusto_endpoints.add_trusted_hosts([MatchRule("statusreturner.azurewebsites.net", False)], False)
        with KustoClient(
            KustoConnectionStringBuilder.with_azure_token_credential(f"https://statusreturner.azurewebsites.net/{code}/test", DefaultAzureCredential())
        ) as client:
            with pytest.raises(KustoServiceError) as ex:
                client.execute("db", "table")
            assert ex.value.http_response.status_code == code

    @pytest.mark.asyncio
    @pytest.mark.parametrize("code", [301, 302, 307, 308])
    async def test_no_redirects_fail_in_cloud(self, code):
        async with AsyncKustoClient(
            KustoConnectionStringBuilder.with_azure_token_credential(
                f"https://statusreturner.azurewebsites.net/{code}/nocloud/test", AsyncDefaultAzureCredential()
            )
        ) as client:
            with pytest.raises(KustoServiceError) as ex:
                await client.execute("db", "table")
            assert ex.value.http_response.status_code == code

    @pytest.mark.asyncio
    @pytest.mark.parametrize("code", [301, 302, 307, 308])
    async def test_no_redirects_fail_in_client(self, code):
        well_known_kusto_endpoints.add_trusted_hosts([MatchRule("statusreturner.azurewebsites.net", False)], False)
        async with AsyncKustoClient(
            KustoConnectionStringBuilder.with_azure_token_credential(f"https://statusreturner.azurewebsites.net/{code}/test", AsyncDefaultAzureCredential())
        ) as client:
            with pytest.raises(KustoServiceError) as ex:
                await client.execute("db", "table")
            assert ex.value.http_response.status == code
