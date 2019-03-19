"""Kusto StreamingIngest Client"""

import os
import time
import tempfile
import io

from azure.kusto.data.request import KustoClient
from ._ingest_client import KustoIngestClient
from ._descriptors import FileDescriptor
from ._ingestion_properties import DataFormat
from .exceptions import MissingMappingReference

_1KB = 1024
_1MB = _1KB * _1KB


class KustoStreamingIngestClient(object):
    """Kusto streaming ingest client for Python.
    KustoStreamingIngestClient works with both 2.x and 3.x flavors of Python.
    All primitive types are supported.
    Tests are run using pytest.
    """

    def __init__(self, kcsb):
        """Kusto Streaming Ingest Client constructor.
        :param kcsb: The connection string to initialize KustoClient.
        """
        self._kusto_client = KustoClient(kcsb)
        self._queued_ingest_client = KustoIngestClient(kcsb)
        self._streaming_ingestion_size_limit = 4 * _1MB

    def ingest_from_stream(self, stream, ingestion_properties):
        """Perform streaming ingest from in-memory streams.
        :param BytesIO or StringIO stream: File-like object which contains the data to ingest.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """

        if isinstance(stream, io.BytesIO) or isinstance(stream, io.StringIO):
            stream.seek(0, io.SEEK_END)
            stream_size = stream.tell()
            stream.seek(0, io.SEEK_SET)
        else:
            raise ValueError("Expected BytesIO or StringIO instance, found {}".format(type(stream)))

        if stream_size > self._streaming_ingestion_size_limit:
            file_name = "stream_{timestamp}_{pid}.csv".format(timestamp=int(time.time()), pid=os.getpid())
            temp_file_path = os.path.join(tempfile.gettempdir(), file_name)
            if isinstance(stream, io.BytesIO):
                with open(temp_file_path, 'wb') as f:
                    f.writelines(stream.readlines())
            else:
                with open(temp_file_path, 'w') as f:
                    f.writelines(stream.readlines())

            self._queued_ingest_client.ingest_from_file(temp_file_path, ingestion_properties)
            return

        if (ingestion_properties.format == DataFormat.json or ingestion_properties.format == DataFormat.singlejson or
                ingestion_properties.format == DataFormat.avro) and ingestion_properties.mapping_reference is None:
            raise MissingMappingReference

        return self._kusto_client.execute_streaming_ingest(ingestion_properties.database, ingestion_properties.table,
                                                           stream, ingestion_properties.format,
                                                           mapping_name=ingestion_properties.mapping_reference,
                                                           content_length=stream_size)

    def ingest_from_dataframe(self, df, ingestion_properties):
        """Perform streaming ingest from local files.
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

        self.ingest_from_file(fd, ingestion_properties)

        fd.delete_files()
        os.unlink(temp_file_path)

    def ingest_from_file(self, file_descriptor, ingestion_properties):
        """Perform streaming ingest from local files.
        :param file_descriptor: a FileDescriptor to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """

        if (ingestion_properties.format == DataFormat.json or ingestion_properties.format == DataFormat.singlejson or
                ingestion_properties.format == DataFormat.avro) and ingestion_properties.mapping_reference is None:
            raise MissingMappingReference

        if isinstance(file_descriptor, FileDescriptor):
            descriptor = file_descriptor
        else:
            descriptor = FileDescriptor(file_descriptor)

        if descriptor.size > self._streaming_ingestion_size_limit:
            self._queued_ingest_client.ingest_from_file(descriptor, ingestion_properties)
            return

        stream = descriptor.zipped_stream
        return self._kusto_client.execute_streaming_ingest(ingestion_properties.database, ingestion_properties.table,
                                                           stream, ingestion_properties.format,
                                                           mapping_name=ingestion_properties.mapping_reference,
                                                           content_length=str(descriptor.size), content_encoding="gzip")

    def ingest_from_blob(self, blob_descriptor, ingestion_properties):
        """Enqueuing an ingest command from azure blobs.
        :param azure.kusto.ingest.BlobDescriptor blob_descriptor: An object that contains a description of the blob to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        self._queued_ingest_client.ingest_from_blob(blob_descriptor, ingestion_properties)
