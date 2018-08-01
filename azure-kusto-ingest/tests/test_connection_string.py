"""Tests class _connection_string."""

import unittest
from azure.kusto.ingest.connection_string import _ConnectionString


class ConnectionStringTests(unittest.TestCase):
    """Tests class connection_string."""

    def test_blob_uri(self):
        """Tests parsing blob uris."""
        storage_name = "storageaccountname"
        container_name = "containername"
        container_sas = "somesas"

        uri = "https://{}.blob.core.windows.net/{}?{}".format(
            storage_name, container_name, container_sas
        )
        connection_string = _ConnectionString.parse(uri)
        self.assertEqual(connection_string.storage_account_name, storage_name)
        self.assertEqual(connection_string.object_type, "blob")
        self.assertEqual(connection_string.sas, container_sas)
        self.assertEqual(connection_string.object_name, container_name)

    def test_queue_uri(self):
        """Tests parsing queues uris."""
        storage_name = "storageaccountname"
        queue_name = "queuename"
        queue_sas = "somesas"

        uri = "https://{}.queue.core.windows.net/{}?{}".format(storage_name, queue_name, queue_sas)
        connection_string = _ConnectionString.parse(uri)
        self.assertEqual(connection_string.storage_account_name, storage_name)
        self.assertEqual(connection_string.object_type, "queue")
        self.assertEqual(connection_string.sas, queue_sas)
        self.assertEqual(connection_string.object_name, queue_name)
