import io
import json
import os
import uuid
from tempfile import NamedTemporaryFile
from unittest.mock import patch

import pytest
import responses

from azure.kusto.data.data_format import DataFormat
from azure.kusto.data.exceptions import KustoApiError
from azure.kusto.ingest import ManagedStreamingIngestClient, IngestionProperties, IngestionStatus, BlobDescriptor
from .test_kusto_ingest_client import request_callback as queued_request_callback, assert_queued_upload, request_callback_throw_transient
from .test_kusto_streaming_ingest_client import request_callback as streaming_request_callback, assert_managed_streaming_request_id


class TransientResponseHelper:
    def __init__(self, times_to_fail):
        self.times_to_fail = times_to_fail
        self.total_calls = 0


@pytest.fixture(params=["Blob", "File"])
def is_blob(request):
    return request.param == "Blob"


def transient_error_callback(helper: TransientResponseHelper, request, custom_request_id=None):
    if custom_request_id:
        assert request.headers["x-ms-client-request-id"] == custom_request_id
    else:
        assert_managed_streaming_request_id(request.headers["x-ms-client-request-id"], helper.total_calls)

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
    MOCKED_UUID_4 = uuid.UUID("11111111-1111-1111-1111-111111111111")

    @responses.activate
    @patch("azure.kusto.data.security._AadHelper.acquire_authorization_header", return_value=None)
    @patch("azure.storage.blob.BlobClient.upload_blob")
    @patch("azure.storage.queue.QueueClient.send_message")
    @patch("uuid.uuid4", return_value=MOCKED_UUID_4)
    def test_fallback_big_file(self, mock_uuid, mock_put_message_in_queue, mock_upload_blob_from_stream, mock_aad, is_blob):
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

        initial_bytes = bytearray(os.urandom(5 * 1024 * 1024))

        def check_bytes(data, **kwargs):
            assert kwargs["timeout"] == 10 * 60
            assert data.read() == initial_bytes

        mock_upload_blob_from_stream.side_effect = check_bytes

        f = NamedTemporaryFile(dir=".", mode="wb", delete=False)
        blob_url = "https://storageaccount.blob.core.windows.net/tempstorage/database__table__11111111-1111-1111-1111-111111111111__{}?".format(
            os.path.basename(f.name)
        )

        try:
            if is_blob:
                result = ingest_client.ingest_from_blob(BlobDescriptor(blob_url + "sas", 5 * 1024 * 1024), ingestion_properties=ingestion_properties)
                f.close()
            else:
                f.write(initial_bytes)
                f.close()
                result = ingest_client.ingest_from_file(f.name, ingestion_properties=ingestion_properties)
        except Exception as e:
            print(e)
        finally:
            os.unlink(f.name)

        assert result.status == IngestionStatus.QUEUED

        assert_queued_upload(
            mock_put_message_in_queue,
            mock_upload_blob_from_stream if not is_blob else None,
            blob_url,
            format=data_format.kusto_value,
        )

        if not is_blob:
            mock_upload_blob_from_stream.assert_called()

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
        ingest_client = ManagedStreamingIngestClient("https://somecluster.kusto.windows.net")
        ingestion_properties = IngestionProperties(database="database", table="table", data_format=data_format)

        initial_bytes = bytearray(os.urandom(5 * 1024 * 1024))
        stream = io.BytesIO(initial_bytes)

        def check_bytes(data, **kwargs):
            assert kwargs["timeout"] == 10 * 60
            assert data.read() == initial_bytes

        mock_upload_blob_from_stream.side_effect = check_bytes

        result = ingest_client.ingest_from_stream(stream, ingestion_properties=ingestion_properties)

        assert result.status == IngestionStatus.QUEUED

        assert_queued_upload(
            mock_put_message_in_queue,
            mock_upload_blob_from_stream,
            "https://storageaccount.blob.core.windows.net/tempstorage/database__table__11111111-1111-1111-1111-111111111111__stream?",
            format=data_format.kusto_value,
            check_raw_data=False,
        )

        mock_upload_blob_from_stream.assert_called()

    @responses.activate
    @patch("azure.kusto.data.security._AadHelper.acquire_authorization_header", return_value=None)
    @patch("azure.storage.blob.BlobClient.upload_blob")
    @patch("azure.storage.queue.QueueClient.send_message")
    @patch("uuid.uuid4", return_value=MOCKED_UUID_4)
    def test_fallback_transient_errors_limit(self, mock_uuid, mock_put_message_in_queue, mock_upload_blob_from_stream, mock_aad):
        total_attempts = 3

        responses.add_callback(
            responses.POST, "https://ingest-somecluster.kusto.windows.net/v1/rest/mgmt", callback=queued_request_callback, content_type="application/json"
        )

        ingest_client = ManagedStreamingIngestClient("https://somecluster.kusto.windows.net")
        ingest_client._set_retry_settings(0)
        ingestion_properties = IngestionProperties(database="database", table="table")

        helper = TransientResponseHelper(times_to_fail=total_attempts)
        responses.add_callback(
            responses.POST,
            "https://somecluster.kusto.windows.net/v1/rest/ingest/database/table",
            callback=lambda request: transient_error_callback(helper, request),
            content_type="application/json",
        )

        # ensure test can work when executed from within directories
        current_dir = os.getcwd()
        path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.csv"]
        missing_path_parts = []
        for path_part in path_parts:
            if path_part not in current_dir:
                missing_path_parts.append(path_part)

        file_path = os.path.join(current_dir, *missing_path_parts)

        result = ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)

        assert result.status == IngestionStatus.QUEUED

        assert_queued_upload(
            mock_put_message_in_queue,
            mock_upload_blob_from_stream,
            "https://storageaccount.blob.core.windows.net/tempstorage/database__table__11111111-1111-1111-1111-111111111111__dataset.csv.gz?",
        )

        assert helper.total_calls == total_attempts

    @responses.activate
    @patch("azure.kusto.data.security._AadHelper.acquire_authorization_header", return_value=None)
    @patch("azure.storage.blob.BlobClient.upload_blob")
    @patch("azure.storage.queue.QueueClient.send_message")
    @patch("uuid.uuid4", return_value=MOCKED_UUID_4)
    def test_fallback_transient_single_error(self, mock_uuid, mock_put_message_in_queue, mock_upload_blob_from_stream, mock_aad):
        total_failures = 2

        responses.add_callback(
            responses.POST, "https://ingest-somecluster.kusto.windows.net/v1/rest/mgmt", callback=queued_request_callback, content_type="application/json"
        )
        helper = TransientResponseHelper(times_to_fail=total_failures)
        responses.add_callback(
            responses.POST,
            "https://somecluster.kusto.windows.net/v1/rest/ingest/database/table",
            callback=lambda request: transient_error_callback(helper, request),
            content_type="application/json",
        )

        ingest_client = ManagedStreamingIngestClient("https://somecluster.kusto.windows.net")
        ingest_client._set_retry_settings(0)
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

        assert result.status == IngestionStatus.SUCCESS

        assert helper.total_calls == total_failures + 1

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

        ingest_client = ManagedStreamingIngestClient("https://ingest-somecluster.kusto.windows.net")
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
        responses.add_callback(
            responses.POST,
            "https://somecluster.kusto.windows.net/v1/rest/ingest/database/table?streamFormat=csv&sourceKind=uri",
            callback=request_callback_throw_transient,
            content_type="application/json",
        )

        ingest_client = ManagedStreamingIngestClient("https://ingest-somecluster.kusto.windows.net")
        ingestion_properties = IngestionProperties(database="database", table="table")

        blob_path = (
            "https://storageaccount.blob.core.windows.net/tempstorage/database__table__11111111-1111-1111-1111-111111111111__tmpbvk40leg?sp=rl&st=2020-05-20T13"
            "%3A38%3A37Z&se=2020-05-21T13%3A38%3A37Z&sv=2019-10-10&sr=c&sig=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx "
        )
        result = ingest_client.ingest_from_blob(BlobDescriptor(blob_path, 1), ingestion_properties=ingestion_properties)

        assert result.status == IngestionStatus.QUEUED

        assert_queued_upload(
            mock_put_message_in_queue,
            mock_upload_blob_from_stream=None,
            expected_url="https://storageaccount.blob.core.windows.net/tempstorage/database__table__11111111-1111-1111-1111-111111111111__tmpbvk40leg?",
        )

    def test_client_uri_from_query_endpoint(self):
        client = ManagedStreamingIngestClient("https://somecluster.kusto.windows.net")
        assert client.queued_client._connection_datasource == "https://ingest-somecluster.kusto.windows.net", (
            "Client URI was not extracted correctly from query endpoint"
        )

        assert client.queued_client._resource_manager._kusto_client._kusto_cluster == "https://ingest-somecluster.kusto.windows.net/"

        assert client.streaming_client._kusto_client._kusto_cluster == "https://somecluster.kusto.windows.net/", (
            "Client URI was not extracted correctly from ingestion endpoint"
        )

    def test_client_uri_from_ingestion_endpoint(self):
        client = ManagedStreamingIngestClient("https://ingest-somecluster.kusto.windows.net")
        assert client.queued_client._connection_datasource == "https://ingest-somecluster.kusto.windows.net", (
            "Client URI was not extracted correctly from query endpoint"
        )

        assert client.queued_client._resource_manager._kusto_client._kusto_cluster == "https://ingest-somecluster.kusto.windows.net/"

        assert client.streaming_client._kusto_client._kusto_cluster == "https://somecluster.kusto.windows.net/", (
            "Client URI was not extracted correctly from ingestion endpoint"
        )

    def test_from_ingestion_endpoint_reserved_hostname(self):
        client = ManagedStreamingIngestClient("https://localhost:8080")
        assert client.queued_client._connection_datasource == "https://localhost:8080", "Client URI was not extracted correctly from query endpoint"
        assert client.queued_client._resource_manager._kusto_client._kusto_cluster == "https://localhost:8080/"
        assert client.streaming_client._kusto_client._kusto_cluster == "https://localhost:8080/"

        client = ManagedStreamingIngestClient("https://192.168.14.4")
        assert client.queued_client._connection_datasource == "https://192.168.14.4", "Client URI was not extracted correctly from query endpoint"
        assert client.queued_client._resource_manager._kusto_client._kusto_cluster == "https://192.168.14.4/"
        assert client.streaming_client._kusto_client._kusto_cluster == "https://192.168.14.4/"

        client = ManagedStreamingIngestClient("https://onebox.dev.kusto.windows.net")
        assert client.queued_client._connection_datasource == "https://onebox.dev.kusto.windows.net", (
            "Client URI was not extracted correctly from query endpoint"
        )
        assert client.queued_client._resource_manager._kusto_client._kusto_cluster == "https://onebox.dev.kusto.windows.net/"
        assert client.streaming_client._kusto_client._kusto_cluster == "https://onebox.dev.kusto.windows.net/"
