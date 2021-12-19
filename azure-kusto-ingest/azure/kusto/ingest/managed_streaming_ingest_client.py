import uuid
from io import SEEK_SET
from typing import TYPE_CHECKING, Union, IO, AnyStr

from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoApiError

from . import IngestionProperties, BlobDescriptor, StreamDescriptor, FileDescriptor
from ._retry import ExponentialRetry
from ._stream_extensions import read_until_size_or_end, chain_streams
from .base_ingest_client import BaseIngestClient, IngestionResult
from .ingest_client import QueuedIngestClient
from .streaming_ingest_client import KustoStreamingIngestClient

if TYPE_CHECKING:
    pass


class ManagedStreamingIngestClient(BaseIngestClient):
    """
    Managed Streaming Ingestion Client.
    Will try to ingest with streaming, but if it fails, will fall back to queued ingestion.
    Each transient failure will be retried with exponential backoff.

    Managed streaming ingest client will fall back to queued if:
        - Multiple transient errors were encountered when trying to do streaming ingestion
        - The ingestion is too large for streaming ingestion (over 4MB)
        - The ingestion is directly for a blob
    """

    MAX_STREAMING_SIZE_IN_BYTES = 4 * 1024 * 1024
    ATTEMPT_COUNT = 3

    @staticmethod
    def from_engine_kcsb(engine_kcsb: Union[KustoConnectionStringBuilder, str]) -> "ManagedStreamingIngestClient":
        """
        Create a ManagedStreamingIngestClient from a KustoConnectionStringBuilder for the engine.
        This Connection String is used for the streaming ingest client.
        This method will infer the dm connection string (by using the same authentication and adding the ingest- prefix)
        For advanced use cases, use the constructor directly.
        :param engine_kcsb: KustoConnectionStringBuilder for the engine.
        :return: ManagedStreamingIngestClient
        """
        kcsb = repr(engine_kcsb) if type(engine_kcsb) == KustoConnectionStringBuilder else engine_kcsb
        dm_kcsb = KustoConnectionStringBuilder(kcsb.replace("https://", "https://ingest-"))
        return ManagedStreamingIngestClient(engine_kcsb, dm_kcsb)

    @staticmethod
    def from_dm_kcsb(dm_kcsb: Union[KustoConnectionStringBuilder, str]) -> "ManagedStreamingIngestClient":
        """
        Create a ManagedStreamingIngestClient from a KustoConnectionStringBuilder for the dm.
        This Connection String is used for the queued ingest client.
        This method will infer the engine connection string (by using the same authentication and removing the ingest- prefix)
        For advanced use cases, use the constructor directly.
        :param dm_kcsb: KustoConnectionStringBuilder for the dm.
        :return: ManagedStreamingIngestClient
        """
        kcsb = repr(dm_kcsb) if type(dm_kcsb) == KustoConnectionStringBuilder else dm_kcsb
        engine_kcsb = KustoConnectionStringBuilder(kcsb.replace("https://ingest-", "https://"))
        return ManagedStreamingIngestClient(engine_kcsb, dm_kcsb)

    def __init__(self, engine_kcsb: Union[KustoConnectionStringBuilder, str], dm_kcsb: Union[KustoConnectionStringBuilder, str]):
        self.queued_client = QueuedIngestClient(dm_kcsb)
        self.streaming_client = KustoStreamingIngestClient(engine_kcsb)

    def ingest_from_file(self, file_descriptor: Union[FileDescriptor, str], ingestion_properties: IngestionProperties) -> IngestionResult:
        stream_descriptor = StreamDescriptor.from_file_descriptor(file_descriptor)

        with stream_descriptor.stream:
            return self.ingest_from_stream(stream_descriptor, ingestion_properties)

    def ingest_from_stream(self, stream_descriptor: Union[StreamDescriptor, IO[AnyStr]], ingestion_properties: IngestionProperties) -> IngestionResult:
        stream_descriptor = BaseIngestClient._prepare_stream(stream_descriptor, ingestion_properties)
        stream = stream_descriptor.stream

        buffered_stream = read_until_size_or_end(stream, self.MAX_STREAMING_SIZE_IN_BYTES + 1)

        if len(buffered_stream.getbuffer()) > self.MAX_STREAMING_SIZE_IN_BYTES:
            stream_descriptor.stream = chain_streams([buffered_stream, stream])
            return self.queued_client.ingest_from_stream(stream_descriptor, ingestion_properties)

        stream_descriptor.stream = buffered_stream

        retry = self._create_exponential_retry()
        while retry:
            try:
                client_request_id = ManagedStreamingIngestClient._get_request_id(stream_descriptor.source_id, retry.current_attempt)
                return self.streaming_client._ingest_from_stream_with_client_request_id(stream_descriptor, ingestion_properties, client_request_id)
            except KustoApiError as e:
                error = e.get_api_error()
                if error.permanent:
                    raise
                stream.seek(0, SEEK_SET)
                retry.do_backoff()

        return self.queued_client.ingest_from_stream(stream_descriptor, ingestion_properties)

    def ingest_from_blob(self, blob_descriptor: BlobDescriptor, ingestion_properties: IngestionProperties):
        """
        Enqueue an ingest command from azure blobs.

        For ManagedStreamingIngestClient, this method always uses Queued Ingest, since it would be easier and faster to ingest blobs.

        To learn more about ingestion methods go to:
        https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
        :param azure.kusto.ingest.BlobDescriptor blob_descriptor: An object that contains a description of the blob to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        return self.queued_client.ingest_from_blob(blob_descriptor, ingestion_properties)

    @staticmethod
    def _get_request_id(source_id: uuid.UUID, attempt: int):
        return f"KPC.executeManagedStreamingIngest;{source_id};{attempt}"

    @staticmethod
    def _create_exponential_retry():
        return ExponentialRetry(ManagedStreamingIngestClient.ATTEMPT_COUNT)
