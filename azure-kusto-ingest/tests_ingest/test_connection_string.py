# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License

from azure.kusto.ingest._resource_manager import _ResourceUri


def test_blob_uri():
    """Tests parsing blob uris."""
    storage_name = "storageaccountname"
    container_name = "containername"
    endpoint_suffix = "core.windows.net"
    container_sas = "somesas"

    uri = "https://{}.blob.{}/{}?{}".format(storage_name, endpoint_suffix, container_name, container_sas)
    connection_string = _ResourceUri(uri)
    assert connection_string.account_uri == "https://storageaccountname.blob.core.windows.net/?somesas"
    assert connection_string.object_name == container_name


def test_queue_uri():
    """Tests parsing queues uris."""
    storage_name = "storageaccountname"
    queue_name = "queuename"
    endpoint_suffix = "core.windows.net"
    queue_sas = "somesas"

    uri = "https://{}.queue.{}/{}?{}".format(storage_name, endpoint_suffix, queue_name, queue_sas)
    connection_string = _ResourceUri(uri)
    assert connection_string.account_uri == "https://storageaccountname.queue.core.windows.net/?somesas"
    assert connection_string.object_name == queue_name


def test_gov_cloud_uri():
    """Tests parsing queues uris."""
    storage_name = "storageaccountname"
    queue_name = "queuename"
    endpoint_suffix = "core.eaglex.ic.gov"
    queue_sas = "somesas"

    uri = "https://{}.queue.{}/{}?{}".format(storage_name, endpoint_suffix, queue_name, queue_sas)
    connection_string = _ResourceUri(uri)
    assert connection_string.account_uri == "https://storageaccountname.queue.core.eaglex.ic.gov/?somesas"
    assert connection_string.object_name == queue_name
