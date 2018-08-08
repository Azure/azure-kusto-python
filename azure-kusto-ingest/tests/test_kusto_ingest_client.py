"""Test for KustoIngestClient."""

import os
import json
import unittest
from mock import patch
import base64
import re
from six import text_type

from azure.kusto.data import KustoClient, KustoServiceError
from azure.kusto.ingest import KustoIngestClient, IngestionProperties, DataFormat


UUID_REGEX = "[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
BLOB_NAME_REGEX = "database__table__" + UUID_REGEX + "__dataset.csv.gz"
BLOB_URL_REGEX = (
    "https://storageaccount.blob.core.windows.net/tempstorage/database__table__"
    + UUID_REGEX
    + "__dataset.csv.gz[?]sas"
)


def mocked_aad_helper(*args, **kwargs):
    """Mock to replace _AadHelper._acquire_token"""
    return None


def mocked_requests_post(*args, **kwargs):
    """Mock to replace requests.post"""

    class MockResponse:
        """Mock class for KustoResponse."""

        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.text = text_type(json_data)
            self.status_code = status_code
            self.headers = None

        def json(self):
            """Get json data from response."""
            return self.json_data

    if args[0] == "https://ingest-somecluster.kusto.windows.net/v1/rest/mgmt":
        if ".get ingestion resources" in kwargs["json"]["csl"]:
            file_name = "ingestionresourcesresult.json"
        if ".get kusto identity token" in kwargs["json"]["csl"]:
            file_name = "identitytokenresult.json"

        with open(
            os.path.join(os.path.dirname(__file__), "input", file_name), "r"
        ) as response_file:
            data = response_file.read()
        return MockResponse(json.loads(data), 200)

    return MockResponse(None, 404)


def mocked_create_blob_from_stream(self, *args, **kwargs):
    """Mock to replace BlockBlobService.create_blob_from_stream"""

    tc = unittest.TestCase("__init__")

    tc.assertEqual(self.account_name, "storageaccount")
    tc.assertEqual(self.sas_token, "sas")
    tc.assertEqual(kwargs["container_name"], "tempstorage")
    tc.assertIsNotNone(kwargs["blob_name"])
    tc.assertRegexpMatches(kwargs["blob_name"], BLOB_NAME_REGEX)
    tc.assertIsNotNone(kwargs["stream"])


def mocked_queue_put_message(self, *args, **kwargs):
    """Mock to replace QueueService.put_message"""

    tc = unittest.TestCase("__init__")

    tc.assertEqual(self.account_name, "storageaccount")
    tc.assertEqual(self.sas_token, "sas")
    tc.assertEqual(kwargs["queue_name"], "readyforaggregation-secured")
    tc.assertIsNotNone(kwargs["content"])

    encoded = kwargs["content"]
    ingestion_blob_info_json = base64.b64decode(encoded.encode("utf-8")).decode("utf-8")

    result = json.loads(ingestion_blob_info_json)
    tc.assertIsNotNone(result)
    tc.assertIsInstance(result, dict)
    tc.assertRegexpMatches(result["BlobPath"], BLOB_URL_REGEX)
    tc.assertEquals(result["DatabaseName"], "database")
    tc.assertEquals(result["TableName"], "table")
    tc.assertGreater(result["RawDataSize"], 0)
    tc.assertEquals(result["AdditionalProperties"]["authorizationContext"], "authorization_context")


class KustoIngestClientTests(unittest.TestCase):
    """Test class for KustoIngestClient."""

    @patch("requests.post", side_effect=mocked_requests_post)
    @patch("azure.kusto.data.aad_helper._AadHelper.acquire_token", side_effect=mocked_aad_helper)
    @patch(
        "azure.storage.blob.BlockBlobService.create_blob_from_stream",
        autospec=True,
        side_effect=mocked_create_blob_from_stream,
    )
    @patch(
        "azure.storage.queue.QueueService.put_message",
        autospec=True,
        side_effect=mocked_queue_put_message,
    )
    def test_sanity_ingest(self, mock_post, mock_aad, mock_block_blob, mock_queue):
        """Test simple ingest"""

        ingest_client = KustoIngestClient("https://ingest-somecluster.kusto.windows.net")

        ingestion_properties = IngestionProperties(
            database="database", table="table", dataFormat=DataFormat.csv
        )

        file_path = os.path.join(os.getcwd(), "azure-kusto-ingest", "tests", "input", "dataset.csv")

        ingest_client.ingest_from_multiple_files(
            [file_path], delete_sources_on_success=False, ingestion_properties=ingestion_properties
        )
