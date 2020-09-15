# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import unittest
from azure.kusto.ingest._resource_manager import _ResourceUri


class ResourceUriTests(unittest.TestCase):
    """Tests class connection_string."""

    def test_blob_uri(self):
        """Tests parsing blob uris."""
        storage_name = "storageaccountname"
        container_name = "containername"
        endpoint_suffix = "core.windows.net"
        container_sas = "somesas"

        uri = "https://{}.blob.{}/{}?{}".format(storage_name, endpoint_suffix, container_name, container_sas)
        connection_string = _ResourceUri.parse(uri)
        assert connection_string.storage_account_name == storage_name
        assert connection_string.object_type == "blob"
        assert connection_string.endpoint_suffix == endpoint_suffix
        assert connection_string.sas == container_sas
        assert connection_string.object_name == container_name

    def test_queue_uri(self):
        """Tests parsing queues uris."""
        storage_name = "storageaccountname"
        queue_name = "queuename"
        endpoint_suffix = "core.windows.net"
        queue_sas = "somesas"

        uri = "https://{}.queue.{}/{}?{}".format(storage_name, endpoint_suffix, queue_name, queue_sas)
        connection_string = _ResourceUri.parse(uri)
        assert connection_string.storage_account_name == storage_name
        assert connection_string.object_type == "queue"
        assert connection_string.endpoint_suffix == endpoint_suffix
        assert connection_string.sas == queue_sas
        assert connection_string.object_name == queue_name
