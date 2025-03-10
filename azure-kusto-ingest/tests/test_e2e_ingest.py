# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import asyncio
import io
import os
import pathlib
import platform
import pytest
import random
import sys
import time
import uuid
from azure.identity import DefaultAzureCredential
from typing import Optional, ClassVar, Callable

from azure.kusto.data.env_utils import get_env, prepare_app_key_auth
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.aio import KustoClient as AsyncKustoClient
from azure.kusto.data.data_format import DataFormat, IngestionMappingKind
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.ingest import (
    QueuedIngestClient,
    KustoStreamingIngestClient,
    IngestionProperties,
    ColumnMapping,
    ValidationPolicy,
    ValidationOptions,
    ValidationImplications,
    ReportLevel,
    ReportMethod,
    FileDescriptor,
    BlobDescriptor,
    StreamDescriptor,
    ManagedStreamingIngestClient,
)
from azure.kusto.ingest._resource_manager import _ResourceUri


@pytest.fixture(params=["ManagedStreaming", "NormalClient"])
def is_managed_streaming(request):
    return request.param == "ManagedStreaming"


@pytest.fixture(scope="module")
def event_loop_policy(request):
    if platform.system() == "Windows":
        return asyncio.WindowsSelectorEventLoopPolicy()
    return asyncio.DefaultEventLoopPolicy()


class TestE2E:
    """A class to define mappings to deft table."""

    input_folder_path: ClassVar[str]
    engine_cs: ClassVar[Optional[str]]
    dm_cs: ClassVar[Optional[str]]
    test_db: ClassVar[Optional[str]]
    client: ClassVar[KustoClient]
    test_table: ClassVar[str]
    current_count: ClassVar[int]
    ingest_client: ClassVar[QueuedIngestClient]
    streaming_ingest_client: ClassVar[KustoStreamingIngestClient]
    managed_streaming_ingest_client: ClassVar[ManagedStreamingIngestClient]
    csv_file_path: ClassVar[str]
    tsv_file_path: ClassVar[str]
    zipped_csv_file_path: ClassVar[str]
    json_file_path: ClassVar[str]
    zipped_json_file_path: ClassVar[str]
    cred: ClassVar[Callable[[], DefaultAzureCredential]]
    async_cred: ClassVar[Callable[[], DefaultAzureCredential]]
    dm_kcsb: KustoConnectionStringBuilder

    CHUNK_SIZE = 1024

    @staticmethod
    def get_test_table_csv_mappings():
        """A method to define csv mappings to test table."""
        mappings = list()
        mappings.append(ColumnMapping(column_name="rownumber", column_type="int", ordinal=0))
        mappings.append(ColumnMapping(column_name="rowguid", column_type="string", ordinal=1))
        mappings.append(ColumnMapping(column_name="xdouble", column_type="real", ordinal=2))
        mappings.append(ColumnMapping(column_name="xfloat", column_type="real", ordinal=3))
        mappings.append(ColumnMapping(column_name="xbool", column_type="bool", ordinal=4))
        mappings.append(ColumnMapping(column_name="xint16", column_type="int", ordinal=5))
        mappings.append(ColumnMapping(column_name="xint32", column_type="int", ordinal=6))
        mappings.append(ColumnMapping(column_name="xint64", column_type="long", ordinal=7))
        mappings.append(ColumnMapping(column_name="xuint8", column_type="long", ordinal=8))
        mappings.append(ColumnMapping(column_name="xuint16", column_type="long", ordinal=9))
        mappings.append(ColumnMapping(column_name="xuint32", column_type="long", ordinal=10))
        mappings.append(ColumnMapping(column_name="xuint64", column_type="long", ordinal=11))
        mappings.append(ColumnMapping(column_name="xdate", column_type="datetime", ordinal=12))
        mappings.append(ColumnMapping(column_name="xsmalltext", column_type="string", ordinal=13))
        mappings.append(ColumnMapping(column_name="xtext", column_type="string", ordinal=14))
        mappings.append(ColumnMapping(column_name="xnumberAsText", column_type="string", ordinal=15))
        mappings.append(ColumnMapping(column_name="xtime", column_type="timespan", ordinal=16))
        mappings.append(ColumnMapping(column_name="xtextWithNulls", column_type="string", ordinal=17))
        mappings.append(ColumnMapping(column_name="xdynamicWithNulls", column_type="dynamic", ordinal=18))
        return mappings

    @staticmethod
    def table_json_mappings():
        """A method to define json mappings to test table."""
        mappings = list()
        mappings.append(ColumnMapping(column_name="rownumber", path="$.rownumber", column_type="int"))
        mappings.append(ColumnMapping(column_name="rowguid", path="$.rowguid", column_type="string"))
        mappings.append(ColumnMapping(column_name="xdouble", path="$.xdouble", column_type="real"))
        mappings.append(ColumnMapping(column_name="xfloat", path="$.xfloat", column_type="real"))
        mappings.append(ColumnMapping(column_name="xbool", path="$.xbool", column_type="bool"))
        mappings.append(ColumnMapping(column_name="xint16", path="$.xint16", column_type="int"))
        mappings.append(ColumnMapping(column_name="xint32", path="$.xint32", column_type="int"))
        mappings.append(ColumnMapping(column_name="xint64", path="$.xint64", column_type="long"))
        mappings.append(ColumnMapping(column_name="xuint8", path="$.xuint8", column_type="long"))
        mappings.append(ColumnMapping(column_name="xuint16", path="$.xuint16", column_type="long"))
        mappings.append(ColumnMapping(column_name="xuint32", path="$.xuint32", column_type="long"))
        mappings.append(ColumnMapping(column_name="xuint64", path="$.xuint64", column_type="long"))
        mappings.append(ColumnMapping(column_name="xdate", path="$.xdate", column_type="datetime"))
        mappings.append(ColumnMapping(column_name="xsmalltext", path="$.xsmalltext", column_type="string"))
        mappings.append(ColumnMapping(column_name="xtext", path="$.xtext", column_type="string"))
        mappings.append(ColumnMapping(column_name="xnumberAsText", path="$.xnumberAsText", column_type="string"))
        mappings.append(ColumnMapping(column_name="xtime", path="$.xtime", column_type="timespan"))
        mappings.append(ColumnMapping(column_name="xtextWithNulls", path="$.xtextWithNulls", column_type="string"))
        mappings.append(ColumnMapping(column_name="xdynamicWithNulls", path="$.xdynamicWithNulls", column_type="dynamic"))
        return mappings

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
    def get_file_path() -> str:
        current_dir = os.getcwd()
        path_parts = ["azure-kusto-ingest", "tests", "input"]
        missing_path_parts = []
        for path_part in path_parts:
            if path_part not in current_dir:
                missing_path_parts.append(path_part)
        return os.path.join(current_dir, *missing_path_parts)

    @classmethod
    def engine_kcsb_from_env(cls, is_async=False) -> KustoConnectionStringBuilder:
        return KustoConnectionStringBuilder.with_azure_token_credential(
            cls.engine_cs,
            credential=cls.cred() if not is_async else cls.async_cred(),
        )

    @classmethod
    def dm_kcsb_from_env(cls) -> KustoConnectionStringBuilder:
        return KustoConnectionStringBuilder.with_azure_token_credential(cls.dm_cs, credential=cls.cred())

    @classmethod
    def setup_class(cls):
        # DM CS can be composed from engine CS
        cls.engine_cs = get_env("ENGINE_CONNECTION_STRING")
        cls.dm_cs = get_env("DM_CONNECTION_STRING", default=cls.engine_cs.replace("//", "//ingest-"))

        # Called to set the env variables for the default azure credentials
        prepare_app_key_auth(optional=True)

        cls.test_db = get_env("TEST_DATABASE")
        cls.test_blob = get_env("TEST_BLOB", optional=True)

        cls.cred = lambda: DefaultAzureCredential(exclude_interactive_browser_credential=False)
        # Async credentials don't support interactive browser authentication for now, so until they do, we'll use the sync default credential for async tests
        cls.async_cred = lambda: DefaultAzureCredential(exclude_interactive_browser_credential=False)

        # Init clients
        python_version = "_".join([str(v) for v in sys.version_info[:3]])
        cls.test_table = "python_test_{0}_{1}_{2}".format(python_version, str(int(time.time())), random.randint(1, 100000))

        cls.client = KustoClient(cls.engine_kcsb_from_env())
        cls.ingest_client = QueuedIngestClient(cls.dm_kcsb_from_env())
        cls.streaming_ingest_client = KustoStreamingIngestClient(cls.engine_kcsb_from_env())
        cls.managed_streaming_ingest_client = ManagedStreamingIngestClient(cls.engine_kcsb_from_env(), cls.dm_kcsb_from_env())

        cls.input_folder_path = cls.get_file_path()

        cls.csv_file_path = os.path.join(cls.input_folder_path, "dataset.csv")
        cls.tsv_file_path = os.path.join(cls.input_folder_path, "dataset.tsv")
        cls.zipped_csv_file_path = os.path.join(cls.input_folder_path, "dataset.csv.gz")
        cls.json_file_path = os.path.join(cls.input_folder_path, "dataset.json")
        cls.zipped_json_file_path = os.path.join(cls.input_folder_path, "dataset.jsonz.gz")

        cls.current_count = 0

        cls.client.execute(
            cls.test_db,
            f".create table {cls.test_table} (rownumber: int, rowguid: string, xdouble: real, xfloat: real, xbool: bool, xint16: int, xint32: int, xint64: long, xuint8: long, xuint16: long, xuint32: long, xuint64: long, xdate: datetime, xsmalltext: string, xtext: string, xnumberAsText: string, xtime: timespan, xtextWithNulls: string, xdynamicWithNulls: dynamic)",
        )
        cls.client.execute(cls.test_db, f".create table {cls.test_table} ingestion json mapping 'JsonMapping' {cls.table_json_mapping_reference()}")

        cls.client.execute(cls.test_db, f".alter table {cls.test_table} policy streamingingestion enable ")

        # Clear the cache to guarantee that subsequent streaming ingestion requests incorporate database and table schema changes
        # See https://docs.microsoft.com/azure/data-explorer/kusto/management/data-ingestion/clear-schema-cache-command
        cls.client.execute(cls.test_db, ".clear database cache streamingingestion schema")

    @classmethod
    def teardown_class(cls):
        cls.client.execute(cls.test_db, ".drop table {} ifexists".format(cls.test_table))
        cls.client.close()
        cls.ingest_client.close()
        cls.streaming_ingest_client.close()
        cls.managed_streaming_ingest_client.close()

    @classmethod
    async def get_async_client(cls) -> AsyncKustoClient:
        return AsyncKustoClient(cls.engine_kcsb_from_env(is_async=True))

    # assertions
    @classmethod
    async def assert_rows_added(cls, expected: int, timeout=60):
        actual = 0
        while timeout > 0:
            time.sleep(1)
            timeout -= 1

            try:
                command = "{} | count".format(cls.test_table)
                response = cls.client.execute(cls.test_db, command)
                async_client = await cls.get_async_client()
                response_from_async = await async_client.execute(cls.test_db, command)
            except KustoServiceError:
                continue

            if response is not None:
                row = response.primary_results[0][0]
                row_async = response_from_async.primary_results[0][0]
                actual = int(row["Count"]) - cls.current_count
                # this is done to allow for data to arrive properly
                if actual >= expected:
                    assert row_async == row, "Mismatch answers between async('{0}') and sync('{1}') clients".format(row_async, row)
                    break

        cls.current_count += actual
        assert actual == expected, "Row count expected = {0}, while actual row count = {1}".format(expected, actual)

    @pytest.mark.asyncio
    async def test_csv_ingest_existing_table(self, is_managed_streaming):
        csv_ingest_props = IngestionProperties(
            self.test_db,
            self.test_table,
            data_format=DataFormat.CSV,
            column_mappings=self.get_test_table_csv_mappings(),
            report_level=ReportLevel.FailuresAndSuccesses,
            flush_immediately=True,
        )

        client = self.streaming_ingest_client if is_managed_streaming else self.ingest_client

        for f in [self.csv_file_path, self.zipped_csv_file_path]:
            client.ingest_from_file(f, csv_ingest_props)

        await self.assert_rows_added(20)

    @pytest.mark.asyncio
    async def test_csv_ingest_ignore_first_record(self, is_managed_streaming):
        csv_ingest_props = IngestionProperties(
            self.test_db,
            self.test_table,
            data_format=DataFormat.CSV,
            column_mappings=self.get_test_table_csv_mappings(),
            report_level=ReportLevel.FailuresAndSuccesses,
            flush_immediately=True,
            ignore_first_record=True,
        )

        for f in [self.csv_file_path, self.zipped_csv_file_path]:
            self.ingest_client.ingest_from_file(f, csv_ingest_props)

        await self.assert_rows_added(18)

    @pytest.mark.asyncio
    async def test_json_ingest_existing_table(self):
        json_ingestion_props = IngestionProperties(
            self.test_db,
            self.test_table,
            flush_immediately=True,
            data_format=DataFormat.JSON,
            column_mappings=self.table_json_mappings(),
            report_level=ReportLevel.FailuresAndSuccesses,
        )

        for f in [self.json_file_path, self.zipped_json_file_path]:
            self.ingest_client.ingest_from_file(f, json_ingestion_props)

        await self.assert_rows_added(4)

    @pytest.mark.asyncio
    async def test_json_ingest_existing_table_no_mapping(self):
        json_ingestion_props = IngestionProperties(
            self.test_db,
            self.test_table,
            flush_immediately=True,
            data_format=DataFormat.JSON,
            report_level=ReportLevel.FailuresAndSuccesses,
        )

        for f in [self.json_file_path, self.zipped_json_file_path]:
            self.ingest_client.ingest_from_file(f, json_ingestion_props)

        await self.assert_rows_added(4)

    @pytest.mark.asyncio
    async def test_ingest_complicated_props(self):
        validation_policy = ValidationPolicy(
            validation_options=ValidationOptions.ValidateCsvInputConstantColumns, validation_implications=ValidationImplications.Fail
        )
        json_ingestion_props = IngestionProperties(
            self.test_db,
            self.test_table,
            data_format=DataFormat.JSON,
            column_mappings=self.table_json_mappings(),
            additional_tags=["a", "b"],
            ingest_if_not_exists=["aaaa", "bbbb"],
            ingest_by_tags=["ingestByTag"],
            drop_by_tags=["drop", "drop-by"],
            flush_immediately=False,
            report_level=ReportLevel.FailuresAndSuccesses,
            report_method=ReportMethod.Queue,
            validation_policy=validation_policy,
        )

        file_paths = [self.json_file_path, self.zipped_json_file_path]
        fds = [FileDescriptor(fp, 0, uuid.uuid4()) for fp in file_paths]

        for fd in fds:
            self.ingest_client.ingest_from_file(fd, json_ingestion_props)

        await self.assert_rows_added(4)

    @pytest.mark.asyncio
    async def test_ingest_from_stream(self, is_managed_streaming):
        validation_policy = ValidationPolicy(
            validation_options=ValidationOptions.ValidateCsvInputConstantColumns, validation_implications=ValidationImplications.Fail
        )
        json_ingestion_props = IngestionProperties(
            self.test_db,
            self.test_table,
            data_format=DataFormat.JSON,
            column_mappings=self.table_json_mappings(),
            additional_tags=["a", "b"],
            ingest_if_not_exists=["aaaa", "bbbb"],
            ingest_by_tags=["ingestByTag"],
            drop_by_tags=["drop", "drop-by"],
            flush_immediately=False,
            report_level=ReportLevel.FailuresAndSuccesses,
            report_method=ReportMethod.Queue,
            validation_policy=validation_policy,
        )
        text = io.StringIO(pathlib.Path(self.json_file_path).read_text())
        zipped = io.BytesIO(pathlib.Path(self.zipped_json_file_path).read_bytes())

        client = self.managed_streaming_ingest_client if is_managed_streaming else self.ingest_client

        client.ingest_from_stream(text, json_ingestion_props)
        client.ingest_from_stream(StreamDescriptor(zipped, is_compressed=True), json_ingestion_props)

        await self.assert_rows_added(4)

    @pytest.mark.asyncio
    async def test_json_ingestion_ingest_by_tag(self):
        json_ingestion_props = IngestionProperties(
            self.test_db,
            self.test_table,
            data_format=DataFormat.JSON,
            column_mappings=self.table_json_mappings(),
            ingest_if_not_exists=["ingestByTag"],
            report_level=ReportLevel.FailuresAndSuccesses,
            drop_by_tags=["drop", "drop-by"],
            flush_immediately=True,
        )

        for f in [self.json_file_path, self.zipped_json_file_path]:
            self.ingest_client.ingest_from_file(f, json_ingestion_props)

        await self.assert_rows_added(0)

    @pytest.mark.asyncio
    async def test_tsv_ingestion_csv_mapping(self):
        tsv_ingestion_props = IngestionProperties(
            self.test_db,
            self.test_table,
            flush_immediately=True,
            data_format=DataFormat.TSV,
            column_mappings=self.get_test_table_csv_mappings(),
            report_level=ReportLevel.FailuresAndSuccesses,
        )

        self.ingest_client.ingest_from_file(self.tsv_file_path, tsv_ingestion_props)

        await self.assert_rows_added(10)

    @pytest.mark.asyncio
    async def test_ingest_blob(self):
        if not self.test_blob:
            pytest.skip("Provide blob SAS uri with 'dataset.csv'")

        csv_ingest_props = IngestionProperties(
            self.test_db,
            self.test_table,
            data_format=DataFormat.CSV,
            column_mappings=self.get_test_table_csv_mappings(),
            report_level=ReportLevel.FailuresAndSuccesses,
            flush_immediately=True,
        )

        blob_len = 1578
        self.ingest_client.ingest_from_blob(BlobDescriptor(self.test_blob, blob_len), csv_ingest_props)

        await self.assert_rows_added(10)

        # Don't provide size hint
        self.ingest_client.ingest_from_blob(BlobDescriptor(self.test_blob, size=None), csv_ingest_props)

        await self.assert_rows_added(10)

    @pytest.mark.asyncio
    async def test_streaming_ingest_from_opened_file(self, is_managed_streaming):
        ingestion_properties = IngestionProperties(database=self.test_db, table=self.test_table, data_format=DataFormat.CSV)

        client = self.managed_streaming_ingest_client if is_managed_streaming else self.streaming_ingest_client
        with open(self.csv_file_path, "r") as stream:
            client.ingest_from_stream(stream, ingestion_properties=ingestion_properties)

        await self.assert_rows_added(10, timeout=120)

    @pytest.mark.asyncio
    async def test_streaming_ingest_from_csv_file(self):
        ingestion_properties = IngestionProperties(database=self.test_db, table=self.test_table, flush_immediately=True, data_format=DataFormat.CSV)

        for f in [self.csv_file_path, self.zipped_csv_file_path]:
            self.streaming_ingest_client.ingest_from_file(f, ingestion_properties=ingestion_properties)

        await self.assert_rows_added(20, timeout=120)

    @pytest.mark.asyncio
    async def test_streaming_ingest_from_json_file(self):
        ingestion_properties = IngestionProperties(
            database=self.test_db,
            table=self.test_table,
            flush_immediately=True,
            data_format=DataFormat.JSON,
            ingestion_mapping_reference="JsonMapping",
            ingestion_mapping_kind=IngestionMappingKind.JSON,
        )

        for f in [self.json_file_path, self.zipped_json_file_path]:
            self.streaming_ingest_client.ingest_from_file(f, ingestion_properties=ingestion_properties)

        await self.assert_rows_added(4, timeout=120)

    @pytest.mark.asyncio
    async def test_streaming_ingest_from_csv_io_streams(self):
        ingestion_properties = IngestionProperties(database=self.test_db, table=self.test_table, data_format=DataFormat.CSV)
        byte_sequence = b'0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,"Zero",0,00:00:00,,null'
        bytes_stream = io.BytesIO(byte_sequence)
        self.streaming_ingest_client.ingest_from_stream(bytes_stream, ingestion_properties=ingestion_properties)

        str_sequence = '0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,"Zero",0,00:00:00,,null'
        str_stream = io.StringIO(str_sequence)
        self.streaming_ingest_client.ingest_from_stream(str_stream, ingestion_properties=ingestion_properties)

        await self.assert_rows_added(2, timeout=120)

    @pytest.mark.asyncio
    async def test_streaming_ingest_from_json_io_streams(self):
        ingestion_properties = IngestionProperties(
            database=self.test_db,
            table=self.test_table,
            data_format=DataFormat.JSON,
            flush_immediately=True,
            ingestion_mapping_reference="JsonMapping",
            ingestion_mapping_kind=IngestionMappingKind.JSON,
        )

        byte_sequence = b'{"rownumber": 0, "rowguid": "00000000-0000-0000-0001-020304050607", "xdouble": 0.0, "xfloat": 0.0, "xbool": 0, "xint16": 0, "xint32": 0, "xint64": 0, "xunit8": 0, "xuint16": 0, "xunit32": 0, "xunit64": 0, "xdate": "2014-01-01T01:01:01Z", "xsmalltext": "Zero", "xtext": "Zero", "xnumberAsText": "0", "xtime": "00:00:00", "xtextWithNulls": null, "xdynamicWithNulls": ""}'
        bytes_stream = io.BytesIO(byte_sequence)
        self.streaming_ingest_client.ingest_from_stream(bytes_stream, ingestion_properties=ingestion_properties)

        str_sequence = '{"rownumber": 0, "rowguid": "00000000-0000-0000-0001-020304050607", "xdouble": 0.0, "xfloat": 0.0, "xbool": 0, "xint16": 0, "xint32": 0, "xint64": 0, "xunit8": 0, "xuint16": 0, "xunit32": 0, "xunit64": 0, "xdate": "2014-01-01T01:01:01Z", "xsmalltext": "Zero", "xtext": "Zero", "xnumberAsText": "0", "xtime": "00:00:00", "xtextWithNulls": null, "xdynamicWithNulls": ""}'
        str_stream = io.StringIO(str_sequence)
        self.streaming_ingest_client.ingest_from_stream(str_stream, ingestion_properties=ingestion_properties)

        await self.assert_rows_added(2, timeout=120)

    @pytest.mark.asyncio
    async def test_streaming_ingest_from_dataframe(self):
        from pandas import DataFrame

        fields = [
            "rownumber",
            "rowguid",
            "xdouble",
            "xfloat",
            "xbool",
            "xint16",
            "xint32",
            "xint64",
            "xunit8",
            "xuint16",
            "xunit32",
            "xunit64",
            "xdate",
            "xsmalltext",
            "xtext",
            "xnumberAsText",
            "xtime",
            "xtextWithNulls",
            "xdynamicWithNulls",
        ]

        guid = uuid.uuid4()

        dynamic_value = ["me@dummy.com", "you@dummy.com", "them@dummy.com"]
        rows = [[0, str(guid), 0.0, 0.0, 0, 0, 0, 0, 0, 0, 0, 0, "2014-01-01T01:01:01Z", "Zero", "Zero", "0", "00:00:00", None, dynamic_value]]
        df = DataFrame(data=rows, columns=fields)
        ingestion_properties = IngestionProperties(database=self.test_db, table=self.test_table, flush_immediately=True)
        self.ingest_client.ingest_from_dataframe(df, ingestion_properties)

        await self.assert_rows_added(1, timeout=120)

        a = self.client.execute(self.test_db, f"{self.test_table} | where rowguid == '{guid}'")
        assert a.primary_results[0].rows[0]["xdynamicWithNulls"] == dynamic_value

    @pytest.mark.asyncio
    async def test_streaming_ingest_from_blob(self, is_managed_streaming):
        ingestion_properties = IngestionProperties(
            database=self.test_db,
            table=self.test_table,
            data_format=DataFormat.JSON,
            ingestion_mapping_reference="JsonMapping",
            ingestion_mapping_kind=IngestionMappingKind.JSON,
        )
        export_containers_list = self.ingest_client._resource_manager._kusto_client.execute("NetDefaultDB", ".show export containers")
        containers = [_ResourceUri(s["StorageRoot"]) for s in export_containers_list.primary_results[0]]

        for c in containers:
            self.ingest_client._resource_manager._ranked_storage_account_set.add_storage_account(c.storage_account_name)

        with FileDescriptor(self.json_file_path).open(False) as stream:
            blob_descriptor = self.ingest_client.upload_blob(
                containers,
                FileDescriptor(self.json_file_path),
                ingestion_properties.database,
                ingestion_properties.table,
                stream,
                None,
                10 * 60,
                3,
            )
            if is_managed_streaming:
                self.managed_streaming_ingest_client.ingest_from_blob(blob_descriptor, ingestion_properties)
            else:
                self.streaming_ingest_client.ingest_from_blob(blob_descriptor, ingestion_properties)

        await self.assert_rows_added(2, timeout=120)
