import os
import tempfile
import time
import uuid
from abc import ABCMeta, abstractmethod
from copy import copy
from enum import Enum
from io import TextIOWrapper
from typing import TYPE_CHECKING, Union, IO, AnyStr, Optional, Tuple

from azure.kusto.data.data_format import DataFormat
from azure.kusto.data.exceptions import KustoClosedError

from .descriptors import FileDescriptor, StreamDescriptor
from .ingestion_properties import IngestionProperties


if TYPE_CHECKING:
    import pandas


class IngestionStatus(Enum):
    """
    The ingestion was queued.
    """

    QUEUED = "QUEUED"
    """
    The ingestion was successfully streamed
    """
    SUCCESS = "SUCCESS"


class IngestionResult:
    """
    The result of an ingestion.
    """

    status: IngestionStatus
    "Will be `Queued` if the ingestion is queued, or `Success` if the ingestion is streaming and successful."

    database: str
    "The name of the database where the ingestion was performed."

    table: str
    "The name of the table where the ingestion was performed."

    source_id: uuid.UUID
    "The source id of the ingestion."

    blob_uri: Optional[str]
    "The blob uri of the ingestion, if exists."

    def __init__(self, status: IngestionStatus, database: str, table: str, source_id: uuid.UUID, blob_uri: Optional[str] = None):
        self.status = status
        self.database = database
        self.table = table
        self.source_id = source_id
        self.blob_uri = blob_uri

    def __repr__(self):
        return f"IngestionResult(status={self.status}, database={self.database}, table={self.table}, source_id={self.source_id}, blob_uri={self.blob_uri})"


class BaseIngestClient(metaclass=ABCMeta):
    def __init__(self):
        self._is_closed: bool = False

    def ingest_from_file(self, file_descriptor: Union[FileDescriptor, str], ingestion_properties: IngestionProperties) -> IngestionResult:
        """Ingest from local files.
        :param file_descriptor: a FileDescriptor to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        if self._is_closed:
            raise KustoClosedError()

    @abstractmethod
    def ingest_from_stream(self, stream_descriptor: Union[StreamDescriptor, IO[AnyStr]], ingestion_properties: IngestionProperties) -> IngestionResult:
        """Ingest from io streams.
        :param stream_descriptor: An object that contains a description of the stream to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        if self._is_closed:
            raise KustoClosedError()

    @abstractmethod
    def set_proxy(self, proxy_url: str):
        """Set proxy for the ingestion client.
        :param str proxy_url: proxy url.
        """
        if self._is_closed:
            raise KustoClosedError()

    def ingest_from_dataframe(self, df: "pandas.DataFrame", ingestion_properties: IngestionProperties) -> IngestionResult:
        """Enqueue an ingest command from local files.
        To learn more about ingestion methods go to:
        https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
        :param pandas.DataFrame df: input dataframe to ingest.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """

        if self._is_closed:
            raise KustoClosedError()

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

    @staticmethod
    def _prepare_stream(stream_descriptor: Union[StreamDescriptor, IO[AnyStr]], ingestion_properties: IngestionProperties) -> StreamDescriptor:
        """
        Prepares a StreamDescriptor instance for ingest operation based on ingestion properties
        :param StreamDescriptor stream_descriptor: Stream descriptor instance
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        :return prepared stream descriptor
        """
        new_descriptor = StreamDescriptor.get_instance(stream_descriptor)

        if isinstance(new_descriptor.stream, TextIOWrapper):
            new_descriptor.stream = new_descriptor.stream.buffer

        should_compress = BaseIngestClient._should_compress(new_descriptor, ingestion_properties)
        if should_compress:
            new_descriptor.compress_stream()

        return new_descriptor

    @staticmethod
    def _prepare_file(file_descriptor: Union[FileDescriptor, str], ingestion_properties: IngestionProperties) -> Tuple[FileDescriptor, bool]:
        """
        Prepares a FileDescriptor instance for ingest operation based on ingestion properties
        :param FileDescriptor file_descriptor: File descriptor instance
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        :return prepared file descriptor
        """
        descriptor = FileDescriptor.get_instance(file_descriptor)

        should_compress = BaseIngestClient._should_compress(descriptor, ingestion_properties)
        return descriptor, should_compress

    @staticmethod
    def _should_compress(new_descriptor: Union[FileDescriptor, StreamDescriptor], ingestion_properties: IngestionProperties) -> bool:
        """
        Checks if descriptor should be compressed based on ingestion properties and current format
        """
        return not new_descriptor.is_compressed and ingestion_properties.format.compressible

    def close(self) -> None:
        self._is_closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
