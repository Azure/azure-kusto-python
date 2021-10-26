import os
import tempfile
import time
import uuid
from abc import ABCMeta, abstractmethod
from enum import Enum
from gzip import GzipFile
from io import TextIOWrapper, BytesIO
from typing import TYPE_CHECKING, Union, IO, AnyStr, Optional

from .descriptors import FileDescriptor, StreamDescriptor
from .exceptions import KustoMissingMappingReferenceError
from .ingestion_properties import DataFormat, IngestionProperties

if TYPE_CHECKING:
    import pandas


class IngestionResultKind(Enum):
    QUEUED = "QUEUED"
    STREAMING = "STREAMING"


class IngestionResult:
    def __init__(self, kind: IngestionResultKind, reason: Optional[str] = None):
        self.reason = reason
        self.kind = kind


class BaseIngestClient(metaclass=ABCMeta):
    _mapping_required_formats = {DataFormat.JSON, DataFormat.SINGLEJSON, DataFormat.AVRO, DataFormat.MULTIJSON}

    @abstractmethod
    def ingest_from_file(self, file_descriptor: Union[FileDescriptor, str], ingestion_properties: IngestionProperties) -> IngestionResult:
        """Ingest from local files.
        :param file_descriptor: a FileDescriptor to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        pass

    @abstractmethod
    def ingest_from_stream(self, stream_descriptor: Union[IO[AnyStr], StreamDescriptor], ingestion_properties: IngestionProperties) -> IngestionResult:
        """Ingest from io streams.
        :param azure.kusto.ingest.StreamDescriptor stream_descriptor: An object that contains a description of the stream to
               be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        pass

    def ingest_from_dataframe(self, df: "pandas.DataFrame", ingestion_properties: IngestionProperties) -> IngestionResult:
        """
        Enqueue an ingest command from local files.
        To learn more about ingestion methods go to:
        https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
        :param pandas.DataFrame df: input dataframe to ingest.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """

        from pandas import DataFrame

        if not isinstance(df, DataFrame):
            raise ValueError("Expected DataFrame instance, found {}".format(type(df)))

        file_name = "df_{id}_{timestamp}_{uid}.csv.gz".format(id=id(df), timestamp=int(time.time()), uid=uuid.uuid4())
        temp_file_path = os.path.join(tempfile.gettempdir(), file_name)

        df.to_csv(temp_file_path, index=False, encoding="utf-8", header=False, compression="gzip")

        ingestion_properties.format = DataFormat.CSV

        try:
            return self.ingest_from_file(temp_file_path, ingestion_properties)
        finally:
            os.unlink(temp_file_path)

    def _prepare_stream(self, stream_descriptor: StreamDescriptor, ingestion_properties: IngestionProperties) -> BytesIO:
        if isinstance(stream_descriptor.stream, TextIOWrapper):
            stream = stream_descriptor.stream.buffer
        else:
            stream = stream_descriptor.stream

        # Todo - also throw this error in other types of ingestions?
        if (
            ingestion_properties.format in self._mapping_required_formats
            and ingestion_properties.ingestion_mapping_reference is None
            and ingestion_properties.ingestion_mapping is None
        ):
            raise KustoMissingMappingReferenceError()
        if not stream_descriptor.is_compressed:
            zipped_stream = BytesIO()
            buffer = stream.read()
            with GzipFile(filename="data", fileobj=zipped_stream, mode="wb") as f_out:
                if isinstance(buffer, str):
                    data = bytes(buffer, "utf-8")
                    f_out.write(data)
                else:
                    f_out.write(buffer)
            zipped_stream.seek(0)
            stream = zipped_stream
        return stream

    def _prepare_stream_descriptor_from_file(self, file_descriptor):
        if isinstance(file_descriptor, FileDescriptor):
            descriptor = file_descriptor
        else:
            descriptor = FileDescriptor(file_descriptor)
        stream = open(descriptor.path, "rb")
        is_compressed = descriptor.path.endswith(".gz") or descriptor.path.endswith(".zip")
        stream_descriptor = StreamDescriptor(stream, descriptor.source_id, is_compressed)
        return stream, stream_descriptor
