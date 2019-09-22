"""Kusto Streaming Ingest Client"""
import os
import time
import tempfile
from gzip import GzipFile

from azure.kusto.data.request import KustoClient, ClientRequestProperties
from ._descriptors import FileDescriptor, StreamDescriptor
from .exceptions import KustoMissingMappingReferenceError
from ._ingestion_properties import DataFormat
from io import TextIOWrapper, BytesIO
from six import string_types, PY2


class KustoStreamingIngestClient(object):
    """Kusto streaming ingest client for Python.
    KustoStreamingIngestClient works with both 2.x and 3.x flavors of Python.
    All primitive types are supported.
    Tests are run using pytest.
    """

    _mapping_required_formats = {DataFormat.JSON, DataFormat.SINGLEJSON, DataFormat.AVRO}

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

        ingestion_properties.format = DataFormat.CSV

        stream_descriptor = StreamDescriptor(fd.zipped_stream, fd.source_id, True)

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

        stream_descriptor = StreamDescriptor(descriptor.zipped_stream, descriptor.source_id, True)

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

        if (
            ingestion_properties.format in self._mapping_required_formats
            and ingestion_properties.ingestion_mapping_reference is None
        ):
            raise KustoMissingMappingReferenceError()

        if not stream_descriptor.is_compressed:
            zipped_stream = BytesIO()
            buffer = stream.read()
            with GzipFile(filename="data", fileobj=zipped_stream, mode="wb") as f_out:
                if isinstance(buffer, string_types):
                    data = bytes(buffer) if PY2 else bytes(buffer, "utf-8")
                    f_out.write(data)
                else:
                    f_out.write(buffer)
            zipped_stream.seek(0)
            stream = zipped_stream

        self._kusto_client.execute_streaming_ingest(
            ingestion_properties.database,
            ingestion_properties.table,
            stream,
            ingestion_properties.format.name,
            mapping_name=ingestion_properties.ingestion_mapping_reference,
        )
