# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
from typing import Union, AnyStr, Optional
from typing import IO

from azure.core.tracing.decorator import distributed_trace
from azure.core.tracing import SpanKind

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties

from ._ingest_telemetry import IngestTracingAttributes
from .base_ingest_client import BaseIngestClient, IngestionResult, IngestionStatus
from .descriptors import FileDescriptor, StreamDescriptor
from .ingestion_properties import IngestionProperties


class KustoStreamingIngestClient(BaseIngestClient):
    """Kusto streaming ingest client for Python.
    KustoStreamingIngestClient works with both 2.x and 3.x flavors of Python.
    All primitive types are supported.
    Tests are run using pytest.
    """

    def __init__(self, kcsb: Union[KustoConnectionStringBuilder, str]):
        """Kusto Streaming Ingest Client constructor.
        :param KustoConnectionStringBuilder kcsb: The connection string to initialize KustoClient.
        """
        super().__init__()
        self._kusto_client = KustoClient(kcsb)

    def close(self):
        if not self._is_closed:
            self._kusto_client.close()
        super().close()

    def set_proxy(self, proxy_url: str):
        self._kusto_client.set_proxy(proxy_url)

    @distributed_trace(kind=SpanKind.CLIENT)
    def ingest_from_file(self, file_descriptor: Union[FileDescriptor, str], ingestion_properties: IngestionProperties) -> IngestionResult:
        """Ingest from local files.
        :param file_descriptor: a FileDescriptor to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        file_descriptor = FileDescriptor.get_instance(file_descriptor)
        IngestTracingAttributes.set_ingest_descriptor_attributes(file_descriptor, ingestion_properties)

        super().ingest_from_file(file_descriptor, ingestion_properties)

        stream_descriptor = StreamDescriptor.from_file_descriptor(file_descriptor)

        with stream_descriptor.stream:
            return self.ingest_from_stream(stream_descriptor, ingestion_properties)

    @distributed_trace(kind=SpanKind.CLIENT)
    def ingest_from_stream(self, stream_descriptor: Union[StreamDescriptor, IO[AnyStr]], ingestion_properties: IngestionProperties) -> IngestionResult:
        """Ingest from io streams.
        :param azure.kusto.ingest.StreamDescriptor stream_descriptor: An object that contains a description of the stream to
               be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        stream_descriptor = StreamDescriptor.get_instance(stream_descriptor)
        IngestTracingAttributes.set_ingest_descriptor_attributes(stream_descriptor, ingestion_properties)

        super().ingest_from_stream(stream_descriptor, ingestion_properties)

        return self._ingest_from_stream_with_client_request_id(stream_descriptor, ingestion_properties, None)

    def _ingest_from_stream_with_client_request_id(
        self, stream_descriptor: Union[StreamDescriptor, IO[AnyStr]], ingestion_properties: IngestionProperties, client_request_id: Optional[str]
    ) -> IngestionResult:
        stream_descriptor = BaseIngestClient._prepare_stream(stream_descriptor, ingestion_properties)
        additional_properties = None
        if client_request_id:
            additional_properties = ClientRequestProperties()
            additional_properties.client_request_id = client_request_id

        self._kusto_client.execute_streaming_ingest(
            ingestion_properties.database,
            ingestion_properties.table,
            stream_descriptor.stream,
            ingestion_properties.format.name,
            additional_properties,
            mapping_name=ingestion_properties.ingestion_mapping_reference,
        )

        return IngestionResult(IngestionStatus.SUCCESS, ingestion_properties.database, ingestion_properties.table, stream_descriptor.source_id)
