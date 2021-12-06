# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import io
import json
import os
import uuid

import pytest
import responses

from azure.kusto.ingest import KustoStreamingIngestClient, IngestionProperties, DataFormat, IngestionStatus, ManagedStreamingIngestClient
from azure.kusto.ingest.exceptions import KustoMissingMappingReferenceError

pandas_installed = False
try:
    import pandas

    pandas_installed = True
except:
    pass

UUID_REGEX = "[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
BLOB_NAME_REGEX = "database__table__" + UUID_REGEX + "__dataset.csv.gz"
BLOB_URL_REGEX = "https://storageaccount.blob.core.windows.net/tempstorage/database__table__" + UUID_REGEX + "__dataset.csv.gz[?]sas"


def request_callback(request, client_type, custom_request_id=None):
    request_id = request.headers["x-ms-client-request-id"]
    if custom_request_id:
        assert custom_request_id == request_id
    elif client_type == KustoStreamingIngestClient:
        [prefix, request_uuid] = request_id.split(";")
        assert prefix == "KPC.execute_streaming_ingest"
        uuid.UUID(request_uuid)
    elif client_type == ManagedStreamingIngestClient:
        assert_managed_streaming_request_id(request_id)

    response_status = 200
    response_headers = dict()
    response_body = {
        "Tables": [
            {
                "TableName": "Table_0",
                "Columns": [
                    {"ColumnName": "ConsumedRecordsCount", "DataType": "Int64"},
                    {"ColumnName": "UpdatePolicyStatus", "DataType": "String"},
                    {"ColumnName": "UpdatePolicyFailureCode", "DataType": "String"},
                    {"ColumnName": "UpdatePolicyFailureReason", "DataType": "String"},
                ],
                "Rows": [[0, "Inactive", "Unknown", None]],
            }
        ]
    }

    return response_status, response_headers, json.dumps(response_body)


def assert_managed_streaming_request_id(request_id: str, retry: int = 0):
    [prefix, request_uuid, suffix] = request_id.split(";")
    assert prefix == "KPC.execute_managed_streaming_ingest"
    uuid.UUID(request_uuid)
    assert int(suffix) == retry


@pytest.fixture(params=[KustoStreamingIngestClient, ManagedStreamingIngestClient])
def ingest_client_class(request):
    if request.param == ManagedStreamingIngestClient:
        return ManagedStreamingIngestClient.from_engine_kcsb
    return request.param


class TestKustoStreamingIngestClient:
    @responses.activate
    def test_streaming_ingest_from_file(self, ingest_client_class):
        responses.add_callback(
            responses.POST, "https://somecluster.kusto.windows.net/v1/rest/ingest/database/table", callback=lambda r: request_callback(r, ingest_client_class)
        )

        ingest_client = ingest_client_class("https://somecluster.kusto.windows.net")
        ingestion_properties = IngestionProperties(database="database", table="table", data_format=DataFormat.CSV)

        # ensure test can work when executed from within directories
        current_dir = os.getcwd()
        path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.csv"]
        missing_path_parts = []
        for path_part in path_parts:
            if path_part not in current_dir:
                missing_path_parts.append(path_part)

        file_path = os.path.join(current_dir, *missing_path_parts)

        result = ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)
        assert result.status == IngestionStatus.SUCCESS

        path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.csv.gz"]
        missing_path_parts = []
        for path_part in path_parts:
            if path_part not in current_dir:
                missing_path_parts.append(path_part)

        file_path = os.path.join(current_dir, *missing_path_parts)

        result = ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)
        assert result.status == IngestionStatus.SUCCESS

        ingestion_properties = IngestionProperties(database="database", table="table", data_format=DataFormat.JSON, ingestion_mapping_reference="JsonMapping")

        path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.json"]
        missing_path_parts = []
        for path_part in path_parts:
            if path_part not in current_dir:
                missing_path_parts.append(path_part)

        file_path = os.path.join(current_dir, *missing_path_parts)

        result = ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)
        assert result.status == IngestionStatus.SUCCESS

        path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.jsonz.gz"]
        missing_path_parts = []
        for path_part in path_parts:
            if path_part not in current_dir:
                missing_path_parts.append(path_part)

        file_path = os.path.join(current_dir, *missing_path_parts)

        result = ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)
        assert result.status == IngestionStatus.SUCCESS

        ingestion_properties = IngestionProperties(database="database", table="table", data_format=DataFormat.TSV)

        path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.tsv"]
        missing_path_parts = []
        for path_part in path_parts:
            if path_part not in current_dir:
                missing_path_parts.append(path_part)

        file_path = os.path.join(current_dir, *missing_path_parts)

        result = ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)
        assert result.status == IngestionStatus.SUCCESS

    @pytest.mark.skipif(not pandas_installed, reason="requires pandas")
    @responses.activate
    def test_streaming_ingest_from_dataframe(self, ingest_client_class):
        responses.add_callback(
            responses.POST, "https://somecluster.kusto.windows.net/v1/rest/ingest/database/table", callback=lambda r: request_callback(r, ingest_client_class)
        )

        ingest_client = ingest_client_class("https://somecluster.kusto.windows.net")
        ingestion_properties = IngestionProperties(database="database", table="table", data_format=DataFormat.CSV)

        from pandas import DataFrame

        fields = ["id", "name", "value"]
        rows = [[1, "abc", 15.3], [2, "cde", 99.9]]
        df = DataFrame(data=rows, columns=fields)

        result = ingest_client.ingest_from_dataframe(df, ingestion_properties)
        assert result.status == IngestionStatus.SUCCESS

    @responses.activate
    def test_streaming_ingest_from_stream(self, ingest_client_class):
        responses.add_callback(
            responses.POST, "https://somecluster.kusto.windows.net/v1/rest/ingest/database/table", callback=lambda r: request_callback(r, ingest_client_class)
        )

        ingest_client = ingest_client_class("https://somecluster.kusto.windows.net")
        ingestion_properties = IngestionProperties(database="database", table="table", data_format=DataFormat.CSV)

        byte_sequence = b"56,56,56"
        bytes_stream = io.BytesIO(byte_sequence)
        result = ingest_client.ingest_from_stream(bytes_stream, ingestion_properties=ingestion_properties)
        assert result.status == IngestionStatus.SUCCESS

        str_sequence = u"57,57,57"
        str_stream = io.StringIO(str_sequence)
        result = ingest_client.ingest_from_stream(str_stream, ingestion_properties=ingestion_properties)
        assert result.status == IngestionStatus.SUCCESS

        byte_sequence = b'{"Name":"Ben","Age":"56","Weight":"75"}'
        bytes_stream = io.BytesIO(byte_sequence)

        with pytest.raises(KustoMissingMappingReferenceError):
            ingestion_properties = IngestionProperties(database="database", table="table", data_format=DataFormat.JSON)

        ingestion_properties.ingestion_mapping_reference = "JsonMapping"
        result = ingest_client.ingest_from_stream(bytes_stream, ingestion_properties=ingestion_properties)
        assert result.status == IngestionStatus.SUCCESS

        str_sequence = u'{"Name":"Ben","Age":"56","Weight":"75"}'
        str_stream = io.StringIO(str_sequence)
        result = ingest_client.ingest_from_stream(str_stream, ingestion_properties=ingestion_properties)
        assert result.status == IngestionStatus.SUCCESS
