import os
import tempfile
import time
import uuid
from abc import ABCMeta, abstractmethod
from copy import copy
from enum import Enum
from gzip import GzipFile
from io import TextIOWrapper, BytesIO
from typing import TYPE_CHECKING, Union, IO, AnyStr, Optional

from azure.kusto.data.data_format import DataFormat

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
    @abstractmethod
    def ingest_from_file(self, file_descriptor: Union[FileDescriptor, str], ingestion_properties: IngestionProperties) -> IngestionResult:
        """Ingest from local files.
        :param file_descriptor: a FileDescriptor to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        pass

    @abstractmethod
    def ingest_from_stream(self, stream_descriptor: Union[StreamDescriptor, IO[AnyStr]], ingestion_properties: IngestionProperties) -> IngestionResult:
        """Ingest from io streams.
        :param stream_descriptor: An object that contains a description of the stream to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        pass

    def ingest_from_dataframe(self, df: "pandas.DataFrame", ingestion_properties: IngestionProperties) -> IngestionResult:
        """Enqueue an ingest command from local files.
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

    @staticmethod
    def _prepare_stream(stream_descriptor: Union[StreamDescriptor, IO[AnyStr]], ingestion_properties: IngestionProperties) -> StreamDescriptor:
        if not isinstance(stream_descriptor, StreamDescriptor):
            new_descriptor = StreamDescriptor(stream_descriptor)
        else:
            new_descriptor = copy(stream_descriptor)

        if isinstance(new_descriptor.stream, TextIOWrapper):
            stream = new_descriptor.stream.buffer
        else:
            stream = new_descriptor.stream

        if not new_descriptor.is_compressed and ingestion_properties.format.compressible:
            zipped_stream = BytesIO()
            buffer = stream.read()
            with GzipFile(filename="data", fileobj=zipped_stream, mode="wb") as f_out:
                if isinstance(buffer, str):
                    data = bytes(buffer, "utf-8")
                    f_out.write(data)
                else:
                    f_out.write(buffer)
            zipped_stream.seek(0)
            new_descriptor.is_compressed = True
            new_descriptor.stream_name += ".gz"
            stream = zipped_stream
        new_descriptor.stream = stream

        return new_descriptor
