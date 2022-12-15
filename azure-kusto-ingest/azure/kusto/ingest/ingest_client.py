# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import copy
import random
from typing import Union, AnyStr, IO, List, Optional, Dict

from azure.storage.queue import QueueServiceClient, TextBase64EncodePolicy

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoClosedError
from ._resource_manager import _ResourceManager, _ResourceUri
from .base_ingest_client import BaseIngestClient, IngestionResult, IngestionStatus
from .descriptors import BlobDescriptor, FileDescriptor, StreamDescriptor
from .ingestion_blob_info import IngestionBlobInfo
from .ingestion_properties import IngestionProperties


class QueuedIngestClient(BaseIngestClient):
    """
    Queued ingest client provides methods to allow queued ingestion into kusto (ADX).
    To learn more about the different types of ingestions and when to use each, visit:
    https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
    """

    _SERVICE_CLIENT_TIMEOUT_SECONDS = 10 * 60

    def __init__(self, kcsb: Union[str, KustoConnectionStringBuilder]):
        """Kusto Ingest Client constructor.
        :param kcsb: The connection string to initialize KustoClient.
        """
        super().__init__()
        if not isinstance(kcsb, KustoConnectionStringBuilder):
            kcsb = KustoConnectionStringBuilder(kcsb)
        else:
            kcsb = copy.deepcopy(kcsb)
        kcsb.data_source = self.get_ingestion_endpoint(kcsb.data_source)
        self._proxy_dict: Optional[Dict[str, str]] = None
        self._connection_datasource = kcsb.data_source
        self._resource_manager = _ResourceManager(KustoClient(kcsb))

    def close(self) -> None:
        self._resource_manager.close()
        super().close()

    def set_proxy(self, proxy_url: str):
        self._resource_manager.set_proxy(proxy_url)
        self._proxy_dict = {"http": proxy_url, "https": proxy_url}

    def ingest_from_file(self, file_descriptor: Union[FileDescriptor, str], ingestion_properties: IngestionProperties) -> IngestionResult:
        """Enqueue an ingest command from local files.
        To learn more about ingestion methods go to:
        https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
        :param file_descriptor: a FileDescriptor to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        super().ingest_from_file(file_descriptor, ingestion_properties)

        containers = self._get_containers()

        file_descriptor, should_compress = BaseIngestClient._prepare_file(file_descriptor, ingestion_properties)
        with file_descriptor.open(should_compress) as stream:
            blob_descriptor = BlobDescriptor.upload_from_different_descriptor(
                containers,
                file_descriptor,
                ingestion_properties.database,
                ingestion_properties.table,
                stream,
                self._proxy_dict,
                self._SERVICE_CLIENT_TIMEOUT_SECONDS,
            )
        return self.ingest_from_blob(blob_descriptor, ingestion_properties=ingestion_properties)

    def ingest_from_stream(self, stream_descriptor: Union[StreamDescriptor, IO[AnyStr]], ingestion_properties: IngestionProperties) -> IngestionResult:
        """Ingest from io streams.
        :param stream_descriptor: An object that contains a description of the stream to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        super().ingest_from_stream(stream_descriptor, ingestion_properties)

        containers = self._get_containers()

        stream_descriptor = BaseIngestClient._prepare_stream(stream_descriptor, ingestion_properties)
        blob_descriptor = BlobDescriptor.upload_from_different_descriptor(
            containers,
            stream_descriptor,
            ingestion_properties.database,
            ingestion_properties.table,
            stream_descriptor.stream,
            self._proxy_dict,
            self._SERVICE_CLIENT_TIMEOUT_SECONDS,
        )
        return self.ingest_from_blob(blob_descriptor, ingestion_properties=ingestion_properties)

    def ingest_from_blob(self, blob_descriptor: BlobDescriptor, ingestion_properties: IngestionProperties) -> IngestionResult:
        """Enqueue an ingest command from azure blobs.
        To learn more about ingestion methods go to:
        https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
        :param azure.kusto.ingest.BlobDescriptor blob_descriptor: An object that contains a description of the blob to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        if self._is_closed:
            raise KustoClosedError()

        queues = self._resource_manager.get_ingestion_queues()

        random_queue = random.choice(queues)
        with QueueServiceClient(random_queue.account_uri, proxies=self._proxy_dict) as queue_service:
            authorization_context = self._resource_manager.get_authorization_context()
            ingestion_blob_info = IngestionBlobInfo(blob_descriptor, ingestion_properties=ingestion_properties, auth_context=authorization_context)
            ingestion_blob_info_json = ingestion_blob_info.to_json()
            with queue_service.get_queue_client(queue=random_queue.object_name, message_encode_policy=TextBase64EncodePolicy()) as queue_client:
                queue_client.send_message(content=ingestion_blob_info_json, timeout=self._SERVICE_CLIENT_TIMEOUT_SECONDS)

        return IngestionResult(
            IngestionStatus.QUEUED, ingestion_properties.database, ingestion_properties.table, blob_descriptor.source_id, blob_descriptor.path
        )

    def _get_containers(self) -> List[_ResourceUri]:
        return self._resource_manager.get_containers()
