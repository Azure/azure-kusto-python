# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import pytest
import os
import unittest
import json
import base64
from mock import patch
import responses
import io
from azure.kusto.ingest import KustoIngestClient, IngestionProperties, DataFormat

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
    body = json.loads(request.body.decode()) if type(request.body) == bytes else json.loads(request.body)
    response_status = 400
    response_headers = []
    response_body = {}

    if ".get ingestion resources" in body["csl"]:
        response_status = 200
        response_body = {
            "Tables": [
                {
                    "TableName": "Table_0",
                    "Columns": [{"ColumnName": "ResourceTypeName", "DataType": "String"}, {"ColumnName": "StorageRoot", "DataType": "String"}],
                    "Rows": [
                        [
                            "SecuredReadyForAggregationQueue",
                            "https://storageaccount.queue.core.windows.net/readyforaggregation-secured?sp=rl&st=2020-05-20T13:38:37Z&se=2020-05-21T13:38:37Z&sv=2019-10-10&sr=c&sig=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                        ],
                        [
                            "SecuredReadyForAggregationQueue",
                            "https://storageaccount.queue.core.windows.net/readyforaggregation-secured?sp=rl&st=2020-05-20T13:38:37Z&se=2020-05-21T13:38:37Z&sv=2019-10-10&sr=c&sig=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                        ],
                        [
                            "SecuredReadyForAggregationQueue",
                            "https://storageaccount.queue.core.windows.net/readyforaggregation-secured?sp=rl&st=2020-05-20T13:38:37Z&se=2020-05-21T13:38:37Z&sv=2019-10-10&sr=c&sig=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                        ],
                        [
                            "SecuredReadyForAggregationQueue",
                            "https://storageaccount.queue.core.windows.net/readyforaggregation-secured?sp=rl&st=2020-05-20T13:38:37Z&se=2020-05-21T13:38:37Z&sv=2019-10-10&sr=c&sig=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                        ],
                        [
                            "SecuredReadyForAggregationQueue",
                            "https://storageaccount.queue.core.windows.net/readyforaggregation-secured?sp=rl&st=2020-05-20T13:38:37Z&se=2020-05-21T13:38:37Z&sv=2019-10-10&sr=c&sig=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                        ],
                        [
                            "FailedIngestionsQueue",
                            "https://storageaccount.queue.core.windows.net/failedingestions?sp=rl&st=2020-05-20T13:38:37Z&se=2020-05-21T13:38:37Z&sv=2019-10-10&sr=c&sig=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                        ],
                        [
                            "SuccessfulIngestionsQueue",
                            "https://storageaccount.queue.core.windows.net/successfulingestions?sp=rl&st=2020-05-20T13:38:37Z&se=2020-05-21T13:38:37Z&sv=2019-10-10&sr=c&sig=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                        ],
                        [
                            "TempStorage",
                            "https://storageaccount.blob.core.windows.net/tempstorage?sp=rl&st=2020-05-20T13:38:37Z&se=2020-05-21T13:38:37Z&sv=2019-10-10&sr=c&sig=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                        ],
                        [
                            "TempStorage",
                            "https://storageaccount.blob.core.windows.net/tempstorage?sp=rl&st=2020-05-20T13:38:37Z&se=2020-05-21T13:38:37Z&sv=2019-10-10&sr=c&sig=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                        ],
                        [
                            "TempStorage",
                            "https://storageaccount.blob.core.windows.net/tempstorage?sp=rl&st=2020-05-20T13:38:37Z&se=2020-05-21T13:38:37Z&sv=2019-10-10&sr=c&sig=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                        ],
                        [
                            "TempStorage",
                            "https://storageaccount.blob.core.windows.net/tempstorage?sp=rl&st=2020-05-20T13:38:37Z&se=2020-05-21T13:38:37Z&sv=2019-10-10&sr=c&sig=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                        ],
                        [
                            "TempStorage",
                            "https://storageaccount.blob.core.windows.net/tempstorage?sp=rl&st=2020-05-20T13:38:37Z&se=2020-05-21T13:38:37Z&sv=2019-10-10&sr=c&sig=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                        ],
                        [
                            "IngestionsStatusTable",
                            "https://storageaccount.table.core.windows.net/ingestionsstatus?sp=rl&st=2020-05-20T13:38:37Z&se=2020-05-21T13:38:37Z&sv=2019-10-10&sr=c&sig=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                        ],
                    ],
                }
            ]
        }

    if ".get kusto identity token" in body["csl"]:
        response_status = 200
        response_body = {
            "Tables": [{"TableName": "Table_0", "Columns": [{"ColumnName": "AuthorizationContext", "DataType": "String"}], "Rows": [["authorization_context"]]}]
        }

    return (response_status, response_headers, json.dumps(response_body))


class KustoIngestClientTests(unittest.TestCase):
    MOCKED_UUID_4 = "1111-111111-111111-1111"
    MOCKED_PID = 64
    MOCKED_TIME = 100

    @responses.activate
    @patch("azure.kusto.data.security._AadHelper.acquire_authorization_header", return_value=None)
    @patch("azure.storage.blob.BlobClient.upload_blob")
    @patch("azure.storage.queue.QueueClient.send_message")
    @patch("uuid.uuid4", return_value=MOCKED_UUID_4)
    def test_sanity_ingest_from_file(self, mock_uuid, mock_put_message_in_queue, mock_upload_blob_from_stream, mock_aad):
        responses.add_callback(
            responses.POST, "https://ingest-somecluster.kusto.windows.net/v1/rest/mgmt", callback=request_callback, content_type="application/json"
        )

        ingest_client = KustoIngestClient("https://ingest-somecluster.kusto.windows.net")
        ingestion_properties = IngestionProperties(database="database", table="table", data_format=DataFormat.CSV)

        # ensure test can work when executed from within directories
        current_dir = os.getcwd()
        path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.csv"]
        missing_path_parts = []
        for path_part in path_parts:
            if path_part not in current_dir:
                missing_path_parts.append(path_part)

        file_path = os.path.join(current_dir, *missing_path_parts)

        ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)

        # mock_put_message_in_queue
        assert mock_put_message_in_queue.call_count == 1

        put_message_in_queue_mock_kwargs = mock_put_message_in_queue.call_args_list[0][1]

        queued_message_json = json.loads(put_message_in_queue_mock_kwargs["content"])
        expected_url = "https://storageaccount.blob.core.windows.net/tempstorage/database__table__1111-111111-111111-1111__dataset.csv.gz?sp=rl&st=2020-05-20T13%3A38%3A37Z&se=2020-05-21T13%3A38%3A37Z&sv=2019-10-10&sr=c&sig=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        # mock_upload_blob_from_stream
        assert queued_message_json["BlobPath"] == expected_url
        assert queued_message_json["DatabaseName"] == "database"
        assert queued_message_json["IgnoreSizeLimit"] is False
        assert queued_message_json["AdditionalProperties"]["format"] == "csv"
        assert queued_message_json["FlushImmediately"] is False
        assert queued_message_json["TableName"] == "table"
        assert queued_message_json["RawDataSize"] > 0
        assert queued_message_json["RetainBlobOnSuccess"] is True

        upload_blob_kwargs = mock_upload_blob_from_stream.call_args_list[0][1]

        assert type(upload_blob_kwargs["data"]) == io.BytesIO

    @responses.activate
    @pytest.mark.skipif(not pandas_installed, reason="requires pandas")
    @patch("azure.storage.blob.BlobClient.upload_blob")
    @patch("azure.storage.queue.QueueClient.send_message")
    @patch("uuid.uuid4", return_value=MOCKED_UUID_4)
    @patch("time.time", return_value=MOCKED_TIME)
    @patch("os.getpid", return_value=MOCKED_PID)
    def test_simple_ingest_from_dataframe(self, mock_pid, mock_time, mock_uuid, mock_put_message_in_queue, mock_upload_blob_from_stream):
        responses.add_callback(
            responses.POST, "https://ingest-somecluster.kusto.windows.net/v1/rest/mgmt", callback=request_callback, content_type="application/json"
        )

        ingest_client = KustoIngestClient("https://ingest-somecluster.kusto.windows.net")
        ingestion_properties = IngestionProperties(database="database", table="table", data_format=DataFormat.CSV)

        from pandas import DataFrame

        fields = ["id", "name", "value"]
        rows = [[1, "abc", 15.3], [2, "cde", 99.9]]
        df = DataFrame(data=rows, columns=fields)

        ingest_client.ingest_from_dataframe(df, ingestion_properties=ingestion_properties)

        # mock_put_message_in_queue
        assert mock_put_message_in_queue.call_count == 1

        put_message_in_queue_mock_kwargs = mock_put_message_in_queue.call_args_list[0][1]

        queued_message_json = json.loads(put_message_in_queue_mock_kwargs["content"])
        expected_url = f"https://storageaccount.blob.core.windows.net/tempstorage/database__table__1111-111111-111111-1111__df_{id(df)}_100_64.csv.gz?sp=rl&st=2020-05-20T13%3A38%3A37Z&se=2020-05-21T13%3A38%3A37Z&sv=2019-10-10&sr=c&sig=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        # mock_upload_blob_from_stream
        assert queued_message_json["BlobPath"] == expected_url
        assert queued_message_json["DatabaseName"] == "database"
        assert queued_message_json["IgnoreSizeLimit"] is False
        assert queued_message_json["AdditionalProperties"]["format"] == "csv"
        assert queued_message_json["FlushImmediately"] is False
        assert queued_message_json["TableName"] == "table"
        assert queued_message_json["RawDataSize"] > 0
        assert queued_message_json["RetainBlobOnSuccess"] is True

        upload_blob_kwargs = mock_upload_blob_from_stream.call_args_list[0][1]

        assert type(upload_blob_kwargs["data"]) == io.BufferedReader
