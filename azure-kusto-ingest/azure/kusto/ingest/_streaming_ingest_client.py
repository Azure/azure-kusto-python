"""Kusto Streaming Ingest Client"""
import os
import time
import tempfile

from azure.kusto.data.request import KustoClient, ClientRequestProperties
from ._descriptors import FileDescriptor, StreamDescriptor
from .exceptions import KustoMissingMappingReferenceError, KustoStreamMaxSizeExceededError
from ._ingestion_properties import DataFormat
from io import TextIOWrapper

_1KB = 1024
_1MB = _1KB * _1KB


class KustoStreamingIngestClient(object):
    """Kusto streaming ingest client for Python.
    KustoStreamingIngestClient works with both 2.x and 3.x flavors of Python.
    All primitive types are supported.
    Tests are run using pytest.
    """

    _mapping_required_formats = [DataFormat.json, DataFormat.singlejson, DataFormat.avro]
    _streaming_ingestion_size_limit = 4 * _1MB

    def __init__(self, kcsb):
        """Kusto Streaming Ingest Client constructor.
        :param KustoConnectionStringBuilder kcsb: The connection string to initialize KustoClient.
        """
        self._kusto_client = KustoClient(kcsb)

    def ingest_from_dataframe(self, df, ingestion_properties):
        """Ingest from pandas DataFrame.
        :param pandas.DataFrame df: input dataframe to ingest.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """

        from pandas import DataFrame

        if not isinstance(df, DataFrame):
            raise ValueError("Expected DataFrame instance, found {}".format(type(df)))

        file_name = "df_{timestamp}_{pid}.csv.gz".format(timestamp=int(time.time()), pid=os.getpid())
        temp_file_path = os.path.join(tempfile.gettempdir(), file_name)

        df.to_csv(temp_file_path, index=False, encoding="utf-8", header=False, compression="gzip")

        fd = FileDescriptor(temp_file_path)

        ingestion_properties.format = DataFormat.csv

        stream_descriptor = StreamDescriptor(fd.zipped_stream, fd.size, fd.source_id, True)

        self.ingest_from_stream(stream_descriptor, ingestion_properties)

        fd.delete_files()
        os.unlink(temp_file_path)

    def ingest_from_file(self, file_descriptor, ingestion_properties):
        """Ingest from local files.
        :param file_descriptor: a FileDescriptor to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """

        if isinstance(file_descriptor, FileDescriptor):
            descriptor = file_descriptor
        else:
            descriptor = FileDescriptor(file_descriptor)

        stream_descriptor = StreamDescriptor(descriptor.zipped_stream, descriptor.size, descriptor.source_id, True)

        self.ingest_from_stream(stream_descriptor, ingestion_properties)

        descriptor.delete_files()

    def ingest_from_stream(self, stream_descriptor, ingestion_properties):
        """Ingest from io streams.
        :param azure.kusto.ingest.StreamDescriptor stream_descriptor: An object that contains a description of the stream to
               be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """

        if not isinstance(stream_descriptor, StreamDescriptor):
            stream_descriptor = StreamDescriptor(stream_descriptor)

        if isinstance(stream_descriptor.stream, TextIOWrapper):
            stream = stream_descriptor.stream.buffer
        else:
            stream = stream_descriptor.stream

        if stream_descriptor.size >= self._streaming_ingestion_size_limit:
            raise KustoStreamMaxSizeExceededError()

        if (
            ingestion_properties.format in self._mapping_required_formats
            and ingestion_properties.mapping_reference is None
        ):
            raise KustoMissingMappingReferenceError()

        client_request_properties = ClientRequestProperties()
        client_request_properties.set_option("content_length", str(stream_descriptor.size))
        if stream_descriptor.source_id:
            client_request_properties.set_option("x-ms-client-request-id", str(stream_descriptor.source_id))
        if stream_descriptor.is_zipped_stream:
            client_request_properties.set_option("Content-Encoding", "gzip")

        self._kusto_client.execute_streaming_ingest(
            ingestion_properties.database,
            ingestion_properties.table,
            stream,
            ingestion_properties.format.name,
            client_request_properties,
            ingestion_properties.mapping_reference,
        )
