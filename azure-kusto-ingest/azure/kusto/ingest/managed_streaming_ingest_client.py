from io import SEEK_SET
from typing import TYPE_CHECKING, Union, Optional, IO, AnyStr

from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoApiError
from . import IngestionProperties, BlobDescriptor, StreamDescriptor, FileDescriptor
from .base_ingest_client import BaseIngestClient, IngestionResult, IngestionResultKind, Reason
from .helpers import sleep_with_backoff, read_until_size_or_end, chain_streams
from .ingest_client import QueuedIngestClient
from .streaming_ingest_client import KustoStreamingIngestClient

if TYPE_CHECKING:
    pass

MAX_STREAMING_SIZE = 4 * 1024 * 1024


class FallbackReason(Reason):
    RETRIES_EXCEEDED = ("Streaming ingestion exceeded maximum retry count, defaulting to queued ingestion",)
    STREAMING_MAX_SIZE_EXCEEDED = ("Stream exceeded max size of {}MB, defaulting to queued ingestion".format(MAX_STREAMING_SIZE / 1024 / 1024),)
    STREAMING_INGEST_NOT_SUPPORTED = ("Streaming ingestion not supported for the table, defaulting to queued ingestion",)
    BLOB_INGESTION = "ingest_from_blob always uses queued ingestion"


class ManagedStreamingIngestClient(BaseIngestClient):
    STREAMING_INGEST_EXCEPTION = "Kusto.DataNode.Exceptions.StreamingIngestionRequestException"
    DEFAULT_SLEEP_BASE = 1.0
    DEFAULT_RETRY_COUNT = 3

    def __init__(
        self,
        queued_kcsb: Union[KustoConnectionStringBuilder, str],
        streaming_kcsb: Optional[Union[KustoConnectionStringBuilder, str]] = None,
        sleep_base: float = DEFAULT_SLEEP_BASE,
        retries: int = DEFAULT_RETRY_COUNT,
    ):
        if streaming_kcsb is None:
            kcsb = repr(queued_kcsb) if type(queued_kcsb) == KustoConnectionStringBuilder else queued_kcsb
            streaming_kcsb = KustoConnectionStringBuilder(kcsb.replace("https://ingest-", "https://"))

        self.queued_client = QueuedIngestClient(queued_kcsb)
        self.streaming_client = KustoStreamingIngestClient(streaming_kcsb)

        if sleep_base < 0:
            raise ValueError("sleep_base must not be negative")
        self.sleep_base = sleep_base
        if retries < 0:
            raise ValueError("retries must not be negative")
        self.attempts_count = 1 + retries

    def ingest_from_file(self, file_descriptor: Union[FileDescriptor, str], ingestion_properties: IngestionProperties) -> IngestionResult:
        stream_descriptor = self._prepare_stream_descriptor_from_file(file_descriptor)

        with stream_descriptor.stream:
            return self.ingest_from_stream(stream_descriptor, ingestion_properties)

    def ingest_from_stream(self, stream_descriptor: Union[IO[AnyStr], StreamDescriptor], ingestion_properties: IngestionProperties) -> IngestionResult:

        stream_descriptor = self._prepare_stream(stream_descriptor, ingestion_properties)
        stream = stream_descriptor.stream

        buffered_stream = read_until_size_or_end(stream, MAX_STREAMING_SIZE + 1)

        if len(buffered_stream.getbuffer()) > MAX_STREAMING_SIZE:
            stream_descriptor.stream = chain_streams([buffered_stream, stream])
            self.queued_client.ingest_from_stream(stream_descriptor, ingestion_properties)
            return IngestionResult(IngestionResultKind.QUEUED, FallbackReason.STREAMING_MAX_SIZE_EXCEEDED)

        stream_descriptor.stream = buffered_stream

        reason = FallbackReason.RETRIES_EXCEEDED

        for i in range(self.attempts_count):
            try:
                return self.streaming_client.ingest_from_stream(stream_descriptor, ingestion_properties)
            except KustoApiError as e:
                error = e.get_api_error()
                if error.permanent:
                    if error.type == self.STREAMING_INGEST_EXCEPTION:
                        reason = FallbackReason.STREAMING_INGEST_NOT_SUPPORTED
                        break
                    raise
                stream.seek(0, SEEK_SET)
                if i != (self.attempts_count - 1):
                    sleep_with_backoff(self.sleep_base, i)

        self.queued_client.ingest_from_stream(stream_descriptor, ingestion_properties)
        return IngestionResult(IngestionResultKind.QUEUED, reason)

    def ingest_from_blob(self, blob_descriptor: BlobDescriptor, ingestion_properties: IngestionProperties):
        """
        Enqueue an ingest command from azure blobs.

        For ManagedStreamingIngestClient, this method always uses Queued Ingest, since it would be easier and faster to ingest blobs.

        To learn more about ingestion methods go to:
        https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
        :param azure.kusto.ingest.BlobDescriptor blob_descriptor: An object that contains a description of the blob to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        self.queued_client.ingest_from_blob(blob_descriptor, ingestion_properties)
        return IngestionResult(IngestionResultKind.QUEUED, FallbackReason.BLOB_INGESTION)
