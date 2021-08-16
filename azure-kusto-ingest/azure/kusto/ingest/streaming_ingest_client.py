# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
from typing import Union, AnyStr

from typing.io import IO

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from .base_ingest_client import BaseIngestClient
from .descriptors import FileDescriptor, StreamDescriptor
from .ingestion_properties import IngestionProperties


class KustoStreamingIngestClient(BaseIngestClient):
    """Kusto streaming ingest client for Python.
    KustoStreamingIngestClient works with both 2.x and 3.x flavors of Python.
    All primitive types are supported.
    Tests are run using pytest.
    """

    def __init__(self, kcsb: KustoConnectionStringBuilder):
        """Kusto Streaming Ingest Client constructor.
        :param KustoConnectionStringBuilder kcsb: The connection string to initialize KustoClient.
        """
        self._kusto_client = KustoClient(kcsb)

    def ingest_from_file(self, file_descriptor: Union[FileDescriptor, str], ingestion_properties: IngestionProperties):
        """Ingest from local files.
        :param file_descriptor: a FileDescriptor to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """

        if isinstance(file_descriptor, FileDescriptor):
            descriptor = file_descriptor
        else:
            descriptor = FileDescriptor(file_descriptor)

        stream = open(descriptor.path, "rb")

        is_compressed = descriptor.path.endswith(".gz") or descriptor.path.endswith(".zip")
        stream_descriptor = StreamDescriptor(stream, descriptor.source_id, is_compressed)

        self.ingest_from_stream(stream_descriptor, ingestion_properties)

        if stream is not None:
            stream.close()

    def ingest_from_stream(self, stream_descriptor: Union[IO[AnyStr], StreamDescriptor], ingestion_properties: IngestionProperties):
        """Ingest from io streams.
        :param azure.kusto.ingest.StreamDescriptor stream_descriptor: An object that contains a description of the stream to
               be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        if not isinstance(stream_descriptor, StreamDescriptor):
            stream_descriptor = StreamDescriptor(stream_descriptor)
        stream = self._prepare_stream(stream_descriptor, ingestion_properties)

        self._kusto_client.execute_streaming_ingest(
            ingestion_properties.database,
            ingestion_properties.table,
            stream,
            ingestion_properties.format.name,
            mapping_name=ingestion_properties.ingestion_mapping_reference,
        )
