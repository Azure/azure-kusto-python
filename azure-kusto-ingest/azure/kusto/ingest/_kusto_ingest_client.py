"""Kusto ingest client for Python."""

import base64
import random
import uuid
from six import text_type

from azure.storage.common import CloudStorageAccount

from azure.kusto.data.request import KustoClient
from ._descriptors import BlobDescriptor, FileDescriptor
from ._ingestion_blob_info import _IngestionBlobInfo
from ._resource_manager import _ResourceManager


class KustoIngestClient(object):
    """Kusto ingest client for Python.
    KustoIngestClient works with both 2.x and 3.x flavors of Python.
    All primitive types are supported.

    Tests are run using pytest.
    """

    def __init__(self, kcsb):
        """Kusto Ingest Client constructor.
        :param kcsb: The connection string to initialize KustoClient.
        """
        kusto_client = KustoClient(kcsb)
        self._resource_manager = _ResourceManager(kusto_client)

    def ingest_from_multiple_files(self, files, delete_sources_on_success, ingestion_properties):
        """Enqueuing an ingest command from local files.
        :param files: List of FileDescriptor or file paths. The list of files to be ingested.
        :param bool delete_sources_on_success: After a successful ingest,
            whether to delete the origin files.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        blobs = list()
        file_descriptors = list()
        for file in files:
            if isinstance(file, FileDescriptor):
                descriptor = file
            else:
                descriptor = FileDescriptor(file, deleteSourcesOnSuccess=delete_sources_on_success)
            file_descriptors.append(descriptor)
            blob_name = (
                ingestion_properties.database
                + "__"
                + ingestion_properties.table
                + "__"
                + text_type(uuid.uuid4())
                + "__"
                + descriptor.stream_name
            )
            containers = self._resource_manager.get_containers()
            container_details = random.choice(containers)
            storage_client = CloudStorageAccount(
                container_details.storage_account_name, sas_token=container_details.sas
            )
            blob_service = storage_client.create_block_blob_service()
            blob_service.create_blob_from_stream(
                container_name=container_details.object_name,
                blob_name=blob_name,
                stream=descriptor.zipped_stream,
            )
            url = blob_service.make_blob_url(
                container_details.object_name, blob_name, sas_token=container_details.sas
            )
            blobs.append(BlobDescriptor(url, descriptor.size))
        self.ingest_from_multiple_blobs(blobs, delete_sources_on_success, ingestion_properties)
        for descriptor in file_descriptors:
            descriptor.delete_files(True)

    def ingest_from_multiple_blobs(self, blobs, delete_sources_on_success, ingestion_properties):
        """Enqueuing an ingest command from azure blobs.
        :param files: List of BlobDescriptor. The list of blobs to be ingested. Please provide the
            raw blob size to each of the descriptors.
        :param bool delete_sources_on_success: After a successful ingest,
            whether to delete the origin files.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        for blob in blobs:
            queues = self._resource_manager.get_ingestion_queues()
            queue_details = random.choice(queues)
            storage_client = CloudStorageAccount(
                queue_details.storage_account_name, sas_token=queue_details.sas
            )
            queue_service = storage_client.create_queue_service()
            authorization_context = self._resource_manager.get_authorization_context()
            ingestion_blob_info = _IngestionBlobInfo(
                blob, ingestion_properties, delete_sources_on_success, authorization_context
            )
            ingestion_blob_info_json = ingestion_blob_info.to_json()
            encoded = base64.b64encode(ingestion_blob_info_json.encode("utf-8")).decode("utf-8")
            queue_service.put_message(queue_name=queue_details.object_name, content=encoded)
