import os
import unittest
import json
import base64
from mock import patch
from six import text_type
import responses
from azure.kusto.ingest import KustoIngestClient, IngestionProperties, DataFormat


UUID_REGEX = "[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
BLOB_NAME_REGEX = "database__table__" + UUID_REGEX + "__dataset.csv.gz"
BLOB_URL_REGEX = (
    "https://storageaccount.blob.core.windows.net/tempstorage/database__table__"
    + UUID_REGEX
    + "__dataset.csv.gz[?]sas"
)


def request_callback(request):
    if ".get ingestion resources" in request.body["json"]["csl"]:
        return {
            "Tables": [{
                "TableName": "Table_0",
                "Columns": [{
                    "ColumnName": "ResourceTypeName",
                    "DataType": "String"
                }, {
                    "ColumnName": "StorageRoot",
                    "DataType": "String"
                }],
                "Rows": [[
                    "SecuredReadyForAggregationQueue",
                    "https://storageaccount.queue.core.windows.net/readyforaggregation-secured?sas"
                ], [
                    "SecuredReadyForAggregationQueue",
                    "https://storageaccount.queue.core.windows.net/readyforaggregation-secured?sas"
                ], [
                    "SecuredReadyForAggregationQueue",
                    "https://storageaccount.queue.core.windows.net/readyforaggregation-secured?sas"
                ], [
                    "SecuredReadyForAggregationQueue",
                    "https://storageaccount.queue.core.windows.net/readyforaggregation-secured?sas"
                ], [
                    "SecuredReadyForAggregationQueue",
                    "https://storageaccount.queue.core.windows.net/readyforaggregation-secured?sas"
                ], [
                    "FailedIngestionsQueue",
                    "https://storageaccount.queue.core.windows.net/failedingestions?sas"
                ], [
                    "SuccessfulIngestionsQueue",
                    "https://storageaccount.queue.core.windows.net/successfulingestions?sas"
                ], [
                    "TempStorage",
                    "https://storageaccount.blob.core.windows.net/tempstorage?sas"
                ], [
                    "TempStorage",
                    "https://storageaccount.blob.core.windows.net/tempstorage?sas"
                ], [
                    "TempStorage",
                    "https://storageaccount.blob.core.windows.net/tempstorage?sas"
                ], [
                    "TempStorage",
                    "https://storageaccount.blob.core.windows.net/tempstorage?sas"
                ], [
                    "TempStorage",
                    "https://storageaccount.blob.core.windows.net/tempstorage?sas"
                ], [
                    "IngestionsStatusTable",
                    "https://storageaccount.table.core.windows.net/ingestionsstatus?sas"
                ]
                ]
            }]
        }
    if ".get kusto identity token" in request.body["json"]["csl"]:
        return {
            "Tables": [
                {
                    "TableName": "Table_0",
                    "Columns": [{
                        "ColumnName": "AuthorizationContext",
                        "DataType": "String"
                    }],
                    "Rows": [["authorization_context"]]
                }
            ]
        }


def mocked_create_blob_from_stream(self, *args, **kwargs):
    """Mock to replace BlockBlobService.create_blob_from_stream"""

    pass
    # tc = unittest.TestCase("__init__")

    # tc.assertEqual(self.account_name, "storageaccount")
    # tc.assertEqual(self.sas_token, "sas")
    # tc.assertEqual(kwargs["container_name"], "tempstorage")
    # tc.assertIsNotNone(kwargs["blob_name"])
    # tc.assertRegexpMatches(kwargs["blob_name"], BLOB_NAME_REGEX)
    # tc.assertIsNotNone(kwargs["stream"])


def mocked_queue_put_message(self, *args, **kwargs):
    """Mock to replace QueueService.put_message"""

    pass
    # tc = unittest.TestCase("__init__")

    # tc.assertEqual(self.account_name, "storageaccount")
    # tc.assertEqual(self.sas_token, "sas")
    # tc.assertEqual(kwargs["queue_name"], "readyforaggregation-secured")
    # tc.assertIsNotNone(kwargs["content"])

    # encoded = kwargs["content"]
    # ingestion_blob_info_json = base64.b64decode(encoded.encode("utf-8")).decode("utf-8")

    # result = json.loads(ingestion_blob_info_json)
    # tc.assertIsNotNone(result)
    # tc.assertIsInstance(result, dict)
    # tc.assertRegexpMatches(result["BlobPath"], BLOB_URL_REGEX)
    # tc.assertEquals(result["DatabaseName"], "database")
    # tc.assertEquals(result["TableName"], "table")
    # tc.assertGreater(result["RawDataSize"], 0)
    # tc.assertEquals(
    #     result["AdditionalProperties"]["authorizationContext"], "authorization_context"
    # )


class KustoIngestClientTests(unittest.TestCase):

    @responses.activate
    @patch("azure.kusto.data.security._AadHelper.acquire_token", returns=None)
    @patch("azure.storage.blob.BlockBlobService.create_blob_from_stream", autospec=True)
    @patch("azure.storage.queue.QueueService.put_message", autospec=True)
    def test_sanity_ingest_from_files(self, mock_aad, mock_block_blob, mock_queue):
        responses.add_callback(
            responses.POST,
            'https://ingest-somecluster.kusto.windows.net/v1/rest/mgmt',
            callback=request_callback,
            content_type='application/json'
        )

        ingest_client = KustoIngestClient("https://ingest-somecluster.kusto.windows.net")
        ingestion_properties = IngestionProperties(database="database", table="table", dataFormat=DataFormat.csv)

        file_path = os.path.join(os.getcwd(), "azure-kusto-ingest", "tests", "input", "dataset.csv")

        ingest_client.ingest_from_files([file_path], ingestion_properties=ingestion_properties)

        print('hello')
