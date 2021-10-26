# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import io
import json
import os

import pytest
import responses

from azure.kusto.ingest import KustoStreamingIngestClient, IngestionProperties, DataFormat, IngestionResultKind, ManagedStreamingIngestClient
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


def request_callback(request):
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


@pytest.fixture(params=[KustoStreamingIngestClient, ManagedStreamingIngestClient])
def ingest_client_class(request):
    return request.param


class TestKustoStreamingIngestClient:
    @responses.activate
    def test_streaming_ingest_from_file(self, ingest_client_class):
        responses.add_callback(responses.POST, "https://somecluster.kusto.windows.net/v1/rest/ingest/database/table", callback=request_callback)

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
        assert result.kind == IngestionResultKind.STREAMING

        path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.csv.gz"]
        missing_path_parts = []
        for path_part in path_parts:
            if path_part not in current_dir:
                missing_path_parts.append(path_part)

        file_path = os.path.join(current_dir, *missing_path_parts)

        result = ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)
        assert result.kind == IngestionResultKind.STREAMING

        ingestion_properties = IngestionProperties(database="database", table="table", data_format=DataFormat.JSON, ingestion_mapping_reference="JsonMapping")

        path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.json"]
        missing_path_parts = []
        for path_part in path_parts:
            if path_part not in current_dir:
                missing_path_parts.append(path_part)

        file_path = os.path.join(current_dir, *missing_path_parts)

        result = ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)
        assert result.kind == IngestionResultKind.STREAMING

        path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.jsonz.gz"]
        missing_path_parts = []
        for path_part in path_parts:
            if path_part not in current_dir:
                missing_path_parts.append(path_part)

        file_path = os.path.join(current_dir, *missing_path_parts)

        result = ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)
        assert result.kind == IngestionResultKind.STREAMING

        ingestion_properties = IngestionProperties(database="database", table="table", data_format=DataFormat.TSV)

        path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.tsv"]
        missing_path_parts = []
        for path_part in path_parts:
            if path_part not in current_dir:
                missing_path_parts.append(path_part)

        file_path = os.path.join(current_dir, *missing_path_parts)

        result = ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)
        assert result.kind == IngestionResultKind.STREAMING

    @pytest.mark.skipif(not pandas_installed, reason="requires pandas")
    @responses.activate
    def test_streaming_ingest_from_dataframe(self, ingest_client_class):
        responses.add_callback(responses.POST, "https://somecluster.kusto.windows.net/v1/rest/ingest/database/table", callback=request_callback)

        ingest_client = ingest_client_class("https://somecluster.kusto.windows.net")
        ingestion_properties = IngestionProperties(database="database", table="table", data_format=DataFormat.CSV)

        from pandas import DataFrame

        fields = ["id", "name", "value"]
        rows = [[1, "abc", 15.3], [2, "cde", 99.9]]
        df = DataFrame(data=rows, columns=fields)

        result = ingest_client.ingest_from_dataframe(df, ingestion_properties)
        assert result.kind == IngestionResultKind.STREAMING

    @responses.activate
    def test_streaming_ingest_from_stream(self, ingest_client_class):
        responses.add_callback(responses.POST, "https://somecluster.kusto.windows.net/v1/rest/ingest/database/table", callback=request_callback)

        ingest_client = ingest_client_class("https://somecluster.kusto.windows.net")
        ingestion_properties = IngestionProperties(database="database", table="table", data_format=DataFormat.CSV)

        byte_sequence = b"56,56,56"
        bytes_stream = io.BytesIO(byte_sequence)
        result = ingest_client.ingest_from_stream(bytes_stream, ingestion_properties=ingestion_properties)
        assert result.kind == IngestionResultKind.STREAMING

        str_sequence = u"57,57,57"
        str_stream = io.StringIO(str_sequence)
        result = ingest_client.ingest_from_stream(str_stream, ingestion_properties=ingestion_properties)
        assert result.kind == IngestionResultKind.STREAMING

        byte_sequence = b'{"Name":"Ben","Age":"56","Weight":"75"}'
        bytes_stream = io.BytesIO(byte_sequence)

        with pytest.raises(KustoMissingMappingReferenceError):
            ingestion_properties = IngestionProperties(database="database", table="table", data_format=DataFormat.JSON)

        ingestion_properties.ingestion_mapping_reference = "JsonMapping"
        result = ingest_client.ingest_from_stream(bytes_stream, ingestion_properties=ingestion_properties)
        assert result.kind == IngestionResultKind.STREAMING

        str_sequence = u'{"Name":"Ben","Age":"56","Weight":"75"}'
        str_stream = io.StringIO(str_sequence)
        result = ingest_client.ingest_from_stream(str_stream, ingestion_properties=ingestion_properties)
        assert result.kind == IngestionResultKind.STREAMING
