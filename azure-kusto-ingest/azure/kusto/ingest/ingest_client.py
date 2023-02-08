# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import random
from typing import Union, AnyStr, IO, List, Optional, Dict
from urllib.parse import urlparse

from azure.core.tracing.decorator import distributed_trace
from azure.core.tracing import SpanKind
from azure.storage.queue import QueueServiceClient, TextBase64EncodePolicy

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data._telemetry import KustoTracing
from azure.kusto.data.exceptions import KustoClosedError, KustoServiceError

from ._ingest_telemetry import IngestTracingAttributes
from ._resource_manager import _ResourceManager, _ResourceUri
from .base_ingest_client import BaseIngestClient, IngestionResult, IngestionStatus
from .descriptors import BlobDescriptor, FileDescriptor, StreamDescriptor
from .exceptions import KustoInvalidEndpointError
from .ingestion_blob_info import IngestionBlobInfo
from .ingestion_properties import IngestionProperties


class QueuedIngestClient(BaseIngestClient):
    """
    Queued ingest client provides methods to allow queued ingestion into kusto (ADX).
    To learn more about the different types of ingestions and when to use each, visit:
    https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
    """

    _INGEST_PREFIX = "ingest-"
    _EXPECTED_SERVICE_TYPE = "DataManagement"
    _SERVICE_CLIENT_TIMEOUT_SECONDS = 10 * 60

    def __init__(self, kcsb: Union[str, KustoConnectionStringBuilder]):
        """Kusto Ingest Client constructor.
        :param kcsb: The connection string to initialize KustoClient.
        """
        super().__init__()
        if not isinstance(kcsb, KustoConnectionStringBuilder):
            kcsb = KustoConnectionStringBuilder(kcsb)
        self._proxy_dict: Optional[Dict[str, str]] = None
        self._connection_datasource = kcsb.data_source
        self._resource_manager = _ResourceManager(KustoClient(kcsb))
        self._endpoint_service_type = None
        self._suggested_endpoint_uri = None

    def close(self) -> None:
        self._resource_manager.close()
        super().close()

    def set_proxy(self, proxy_url: str):
        self._resource_manager.set_proxy(proxy_url)
        self._proxy_dict = {"http": proxy_url, "https": proxy_url}

    @distributed_trace(name_of_span="QueuedIngestClient.ingest_from_file", kind=SpanKind.CLIENT)
    def ingest_from_file(self, file_descriptor: Union[FileDescriptor, str], ingestion_properties: IngestionProperties) -> IngestionResult:
        """Enqueue an ingest command from local files.
        To learn more about ingestion methods go to:
        https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
        :param file_descriptor: a FileDescriptor to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        file_descriptor = FileDescriptor.get_instance(file_descriptor)
        IngestTracingAttributes.set_ingest_descriptor_attributes(file_descriptor, ingestion_properties)

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

    @distributed_trace(name_of_span="QueuedIngestClient.ingest_from_stream", kind=SpanKind.CLIENT)
    def ingest_from_stream(self, stream_descriptor: Union[StreamDescriptor, IO[AnyStr]], ingestion_properties: IngestionProperties) -> IngestionResult:
        """Ingest from io streams.
        :param stream_descriptor: An object that contains a description of the stream to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        stream_descriptor = StreamDescriptor.get_instance(stream_descriptor)
        IngestTracingAttributes.set_ingest_descriptor_attributes(stream_descriptor, ingestion_properties)

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

    @distributed_trace(name_of_span="QueuedIngestClient.ingest_from_blob", kind=SpanKind.CLIENT)
    def ingest_from_blob(self, blob_descriptor: BlobDescriptor, ingestion_properties: IngestionProperties) -> IngestionResult:
        """Enqueue an ingest command from azure blobs.
        To learn more about ingestion methods go to:
        https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
        :param azure.kusto.ingest.BlobDescriptor blob_descriptor: An object that contains a description of the blob to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        IngestTracingAttributes.set_ingest_descriptor_attributes(blob_descriptor, ingestion_properties)

        if self._is_closed:
            raise KustoClosedError()

        try:
            queues = self._resource_manager.get_ingestion_queues()
        except KustoServiceError as ex:
            self._validate_endpoint_service_type()
            raise ex

        random_queue = random.choice(queues)
        with QueueServiceClient(random_queue.account_uri, proxies=self._proxy_dict) as queue_service:
            authorization_context = self._resource_manager.get_authorization_context()
            ingestion_blob_info = IngestionBlobInfo(blob_descriptor, ingestion_properties=ingestion_properties, auth_context=authorization_context)
            ingestion_blob_info_json = ingestion_blob_info.to_json()
            with queue_service.get_queue_client(queue=random_queue.object_name, message_encode_policy=TextBase64EncodePolicy()) as queue_client:
                # trace enqueuing of blob for ingestion
                enqueue_trace_attributes = IngestTracingAttributes.create_enqueue_request_attributes(queue_client.queue_name, blob_descriptor.source_id)
                KustoTracing.call_func_tracing(
                    queue_client.send_message,
                    content=ingestion_blob_info_json,
                    timeout=self._SERVICE_CLIENT_TIMEOUT_SECONDS,
                    name_of_span="QueuedIngestClient.enqueue_request",
                    tracing_attributes=enqueue_trace_attributes,
                )

        return IngestionResult(
            IngestionStatus.QUEUED, ingestion_properties.database, ingestion_properties.table, blob_descriptor.source_id, blob_descriptor.path
        )

    def _get_containers(self) -> List[_ResourceUri]:
        try:
            containers = self._resource_manager.get_containers()
        except KustoServiceError as ex:
            self._validate_endpoint_service_type()
            raise ex
        return containers

    def _validate_endpoint_service_type(self):
        if not self._hostname_starts_with_ingest(self._connection_datasource):
            if not self._endpoint_service_type:
                self._endpoint_service_type = self._retrieve_service_type()

            if self._EXPECTED_SERVICE_TYPE != self._endpoint_service_type:
                if not self._suggested_endpoint_uri:
                    self._suggested_endpoint_uri = self._generate_endpoint_suggestion(self._connection_datasource)
                    if not self._suggested_endpoint_uri:
                        raise KustoInvalidEndpointError(self._EXPECTED_SERVICE_TYPE, self._endpoint_service_type)
                raise KustoInvalidEndpointError(self._EXPECTED_SERVICE_TYPE, self._endpoint_service_type, self._suggested_endpoint_uri)

    def _retrieve_service_type(self) -> str:
        return self._resource_manager.retrieve_service_type()

    def _generate_endpoint_suggestion(self, datasource: str) -> Optional[str]:
        """The default is not passing a suggestion to the exception String"""
        endpoint_uri_to_suggest_str = None
        if datasource.strip():
            try:
                endpoint_uri_to_suggest = urlparse(datasource)  # Standardize URL formatting
                endpoint_uri_to_suggest = urlparse(endpoint_uri_to_suggest.scheme + "://" + self._INGEST_PREFIX + endpoint_uri_to_suggest.hostname)
                endpoint_uri_to_suggest_str = endpoint_uri_to_suggest.geturl()
            except Exception:
                # TODO: Add logging infrastructure so we can tell the user as a warning:
                #   "Couldn't generate suggested endpoint due to problem parsing datasource, with exception: {ex}. The correct endpoint is usually the Engine endpoint with '{self._INGEST_PREFIX}' prepended to the hostname."
                pass
        return endpoint_uri_to_suggest_str

    def _hostname_starts_with_ingest(self, datasource: str) -> bool:
        datasource_uri = urlparse(datasource)
        hostname = datasource_uri.hostname
        return hostname and hostname.startswith(self._INGEST_PREFIX)
