import io
import json
import os
from tempfile import NamedTemporaryFile

import pytest
import responses
from mock import patch

from azure.kusto.data.exceptions import KustoApiError
from azure.kusto.ingest import ManagedStreamingIngestClient, IngestionProperties, DataFormat, IngestionResultKind, BlobDescriptor
from tests.test_kusto_ingest_client import request_callback as queued_request_callback, assert_queued_upload
from tests.test_kusto_streaming_ingest_client import request_callback as streaming_request_callback


class TransientResponseHelper:
    def __init__(self, times_to_fail):
        self.times_to_fail = times_to_fail
        self.total_calls = 0


def transient_error_callback(helper: TransientResponseHelper):
    response_headers = dict()

    helper.total_calls += 1

    if helper.total_calls <= helper.times_to_fail:
        response_status = 400
        response_body = {
            "error": {
                "code": "General_InternalServerError",
                "message": "Unknown error",
                "@type": "Kusto.DataNode.Exceptions.InternalServerError",
                "@message": "InternalServerError",
                "@context": {
                    "timestamp": "2021-10-21T14:12:00.7878847Z",
                    "serviceAlias": "SomeCluster",
                    "machineName": "KEngine000000",
                    "processName": "Kusto.WinSvc.Svc",
                    "processId": 3040,
                    "threadId": 7524,
                    "appDomainName": "Kusto.WinSvc.Svc.exe",
                    "clientRequestId": "KNC.executeStreamingIngest;a4d80ca5-8729-404b-b745-0d5555737483",
                    "activityId": "bace3d9d-d949-457e-b741-1d2c6bc56658",
                    "subActivityId": "bace3d9d-d949-457e-b741-1d2c6bc56658",
                    "activityType": "PO.OWIN.CallContext",
                    "parentActivityId": "bace3d9d-d949-457e-b741-1d2c6bc56658",
                    "activityStack": "(Activity stack: CRID=KNC.executeStreamingIngest;a4d80ca5-8729-404b-b745-0d5555737493 ARID=bace3d9d-d949-457e-b741-1d2c6bc56678 > PO.OWIN.CallContext/bace3d9d-d949-457e-b741-1d2c6bc56678 > PO.OWIN.CallContext/bace3d9d-d949-457e-b741-1d2c6bc56678)",
                },
                "@permanent": False,
            }
        }
    else:
        response_status = 200
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


class TestManagedStreamingIngestClient:
    MOCKED_UUID_4 = "1111-111111-111111-1111"

    @responses.activate
    @patch("azure.kusto.data.security._AadHelper.acquire_authorization_header", return_value=None)
    @patch("azure.storage.blob.BlobClient.upload_blob")
    @patch("azure.storage.queue.QueueClient.send_message")
    @patch("uuid.uuid4", return_value=MOCKED_UUID_4)
    def test_fallback_big_file(self, mock_uuid, mock_put_message_in_queue, mock_upload_blob_from_stream, mock_aad):
        responses.add_callback(
            responses.POST, "https://ingest-somecluster.kusto.windows.net/v1/rest/mgmt", callback=queued_request_callback, content_type="application/json"
        )
        responses.add_callback(
            responses.POST,
            "https://somecluster.kusto.windows.net/v1/rest/ingest/database/table",
            callback=streaming_request_callback,
            content_type="application/json",
        )

        data_format = DataFormat.ORC  # Using orc to avoid compression
        ingest_client = ManagedStreamingIngestClient("https://ingest-somecluster.kusto.windows.net")
        ingestion_properties = IngestionProperties(database="database", table="table", data_format=data_format)

        f = NamedTemporaryFile(dir=".", mode="wb", delete=False)
        try:
            f.write(b"\0" * (5 * 1024 * 1024))
            f.close()
            result = ingest_client.ingest_from_file(f.name, ingestion_properties=ingestion_properties)
        finally:
            os.unlink(f.name)

        assert result.kind == IngestionResultKind.QUEUED
        assert result.reason == "Stream exceeded max size, defaulting to queued ingestion"

        assert_queued_upload(
            mock_put_message_in_queue,
            mock_upload_blob_from_stream,
            "https://storageaccount.blob.core.windows.net/tempstorage/database__table__1111-111111-111111-1111__{}?".format(os.path.basename(f.name)),
            format=data_format.value,
        )

    @responses.activate
    @patch("azure.kusto.data.security._AadHelper.acquire_authorization_header", return_value=None)
    @patch("azure.storage.blob.BlobClient.upload_blob")
    @patch("azure.storage.queue.QueueClient.send_message")
    @patch("uuid.uuid4", return_value=MOCKED_UUID_4)
    def test_fallback_big_stream(self, mock_uuid, mock_put_message_in_queue, mock_upload_blob_from_stream, mock_aad):
        responses.add_callback(
            responses.POST, "https://ingest-somecluster.kusto.windows.net/v1/rest/mgmt", callback=queued_request_callback, content_type="application/json"
        )
        responses.add_callback(
            responses.POST,
            "https://somecluster.kusto.windows.net/v1/rest/ingest/database/table",
            callback=streaming_request_callback,
            content_type="application/json",
        )

        data_format = DataFormat.ORC  # Using orc to avoid compression
        ingest_client = ManagedStreamingIngestClient("https://ingest-somecluster.kusto.windows.net")
        ingestion_properties = IngestionProperties(database="database", table="table", data_format=data_format)

        stream = io.BytesIO(b"\0" * (5 * 1024 * 1024))
        result = ingest_client.ingest_from_stream(stream, ingestion_properties=ingestion_properties)

        assert result.kind == IngestionResultKind.QUEUED
        assert result.reason == "Stream exceeded max size, defaulting to queued ingestion"

        assert_queued_upload(
            mock_put_message_in_queue,
            mock_upload_blob_from_stream,
            "https://storageaccount.blob.core.windows.net/tempstorage/database__table__1111-111111-111111-1111__stream?",
            format=data_format.value,
        )

    @responses.activate
    @patch("azure.kusto.data.security._AadHelper.acquire_authorization_header", return_value=None)
    @patch("azure.storage.blob.BlobClient.upload_blob")
    @patch("azure.storage.queue.QueueClient.send_message")
    @patch("uuid.uuid4", return_value=MOCKED_UUID_4)
    def test_fallback_transient_errors_limit(self, mock_uuid, mock_put_message_in_queue, mock_upload_blob_from_stream, mock_aad):
        responses.add_callback(
            responses.POST, "https://ingest-somecluster.kusto.windows.net/v1/rest/mgmt", callback=queued_request_callback, content_type="application/json"
        )
        helper = TransientResponseHelper(times_to_fail=ManagedStreamingIngestClient.ATTEMPTS_COUNT)
        responses.add_callback(
            responses.POST,
            "https://somecluster.kusto.windows.net/v1/rest/ingest/database/table",
            callback=lambda x: transient_error_callback(helper),
            content_type="application/json",
        )

        ingest_client = ManagedStreamingIngestClient("https://ingest-somecluster.kusto.windows.net", sleep_base=0)
        ingestion_properties = IngestionProperties(database="database", table="table")

        # ensure test can work when executed from within directories
        current_dir = os.getcwd()
        path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.csv"]
        missing_path_parts = []
        for path_part in path_parts:
            if path_part not in current_dir:
                missing_path_parts.append(path_part)

        file_path = os.path.join(current_dir, *missing_path_parts)

        result = ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)

        assert result.kind == IngestionResultKind.QUEUED
        assert result.reason == "Streaming ingestion exceeded maximum retry count, defaulting to queued ingestion"

        assert_queued_upload(
            mock_put_message_in_queue,
            mock_upload_blob_from_stream,
            "https://storageaccount.blob.core.windows.net/tempstorage/database__table__1111-111111-111111-1111__dataset.csv.gz?",
        )

        assert helper.total_calls == ManagedStreamingIngestClient.ATTEMPTS_COUNT

    @responses.activate
    @patch("azure.kusto.data.security._AadHelper.acquire_authorization_header", return_value=None)
    @patch("azure.storage.blob.BlobClient.upload_blob")
    @patch("azure.storage.queue.QueueClient.send_message")
    @patch("uuid.uuid4", return_value=MOCKED_UUID_4)
    def test_fallback_transient_single_error(self, mock_uuid, mock_put_message_in_queue, mock_upload_blob_from_stream, mock_aad):
        responses.add_callback(
            responses.POST, "https://ingest-somecluster.kusto.windows.net/v1/rest/mgmt", callback=queued_request_callback, content_type="application/json"
        )
        FAILED_ATTEMPTS = 2
        helper = TransientResponseHelper(times_to_fail=FAILED_ATTEMPTS)
        responses.add_callback(
            responses.POST,
            "https://somecluster.kusto.windows.net/v1/rest/ingest/database/table",
            callback=lambda x: transient_error_callback(helper),
            content_type="application/json",
        )

        ingest_client = ManagedStreamingIngestClient("https://ingest-somecluster.kusto.windows.net", sleep_base=0)
        ingestion_properties = IngestionProperties(database="database", table="table")

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

        assert helper.total_calls == (FAILED_ATTEMPTS + 1)

    @responses.activate
    def test_permanent_error(self):
        responses.add(
            responses.POST,
            "https://somecluster.kusto.windows.net/v1/rest/ingest/database/table",
            status=400,
            json={
                "error": {
                    "code": "BadRequest",
                    "message": "Request is invalid and cannot be executed.",
                    "@type": "Kusto.Common.Svc.Exceptions.AdminCommandWrongEndpointException",
                    "@message": "Cannot get ingestion resources from this service endpoint. The appropriate endpoint is most likely "
                    "'https://ingest-somecluster.kusto.windows.net/'.",
                    "@context": {
                        "timestamp": "2021-10-12T06:05:35.6602087Z",
                        "serviceAlias": "SomeCluster",
                        "machineName": "KEngine000000",
                        "processName": "Kusto.WinSvc.Svc",
                        "processId": 2648,
                        "threadId": 472,
                        "appDomainName": "Kusto.WinSvc.Svc.exe",
                        "clientRequestId": "KPC.execute;a3dfb878-9d2b-49d6-89a5-e9b3a9f1f674",
                        "activityId": "87eb8fc9-78b3-4580-bcc8-6c90482f9118",
                        "subActivityId": "bbfb038b-4467-4f96-afd4-945904fc6278",
                        "activityType": "DN.AdminCommand.IngestionResourcesGetCommand",
                        "parentActivityId": "00e678e9-4204-4143-8c94-6afd94c27430",
                        "activityStack": "(Activity stack: CRID=KPC.execute;a3dfb878-9d2b-49d6-89a5-e9b3a9f1f674 ARID=87eb8fc9-78b3-4580-bcc8-6c90482f9118 > DN.Admin.Client.ExecuteControlCommand/833dfb85-5d67-44b7-882d-eb2283e65780 > P.WCF.Service.ExecuteControlCommand..IInterNodeCommunicationAdminContract/3784e74f-1d89-4c15-adef-0a360c4c431e > DN.FE.ExecuteControlCommand/00e678e9-4204-4143-8c94-6afd94c27430 > DN.AdminCommand.IngestionResourcesGetCommand/bbfb038b-4467-4f96-afd4-945904fc6278)",
                    },
                    "@permanent": True,
                }
            },
            content_type="application/json",
        )

        ingest_client = ManagedStreamingIngestClient("https://somecluster.kusto.windows.net")
        ingestion_properties = IngestionProperties(database="database", table="table", data_format=DataFormat.CSV)

        current_dir = os.getcwd()
        path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.csv"]
        missing_path_parts = []
        for path_part in path_parts:
            if path_part not in current_dir:
                missing_path_parts.append(path_part)

        file_path = os.path.join(current_dir, *missing_path_parts)

        with pytest.raises(KustoApiError) as ex:
            ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)
            assert ex.value.get_api_error().permanent == True

    @responses.activate
    @patch("azure.kusto.data.security._AadHelper.acquire_authorization_header", return_value=None)
    @patch("azure.storage.queue.QueueClient.send_message")
    @patch("uuid.uuid4", return_value=MOCKED_UUID_4)
    def test_blob_ingestion(self, mock_uuid, mock_put_message_in_queue, mock_aad):
        responses.add_callback(
            responses.POST, "https://ingest-somecluster.kusto.windows.net/v1/rest/mgmt", callback=queued_request_callback, content_type="application/json"
        )

        ingest_client = ManagedStreamingIngestClient("https://ingest-somecluster.kusto.windows.net")
        ingestion_properties = IngestionProperties(database="database", table="table")

        blob_path = "https://storageaccount.blob.core.windows.net/tempstorage/database__table__1111-111111-111111-1111__tmpbvk40leg?sp=rl&st=2020-05-20T13" \
                    "%3A38%3A37Z&se=2020-05-21T13%3A38%3A37Z&sv=2019-10-10&sr=c&sig=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx "
        result = ingest_client.ingest_from_blob(BlobDescriptor(blob_path, 1), ingestion_properties=ingestion_properties)

        assert result.kind == IngestionResultKind.QUEUED
        assert result.reason == "ingest_from_blob always uses queued ingestion"

        assert_queued_upload(
            mock_put_message_in_queue,
            mock_upload_blob_from_stream=None,
            expected_url="https://storageaccount.blob.core.windows.net/tempstorage/database__table__1111-111111-111111-1111__tmpbvk40leg?",
        )
