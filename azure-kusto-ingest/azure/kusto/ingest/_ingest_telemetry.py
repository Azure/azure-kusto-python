from copy import copy
from typing import Union, AnyStr, IO

from azure.kusto.data._telemetry import KustoTracingAttributes

from .descriptors import FileDescriptor, StreamDescriptor, BlobDescriptor
from .ingestion_properties import IngestionProperties


class IngestTracingAttributes:
    """
    Additional ADX attributes for telemetry spans
    """

    _DATABASE = "Database"
    _TABLE = "Table"
    _BLOB_CONTAINER = "Blob Container"
    _BLOB_URI = "Blob URI"
    _FILE_PATH = "File Path"
    _STREAM_NAME = "Stream Name"
    _SOURCE_ID = "Source ID"

    @classmethod
    def set_ingest_file_attributes(cls, file_descriptor: Union[FileDescriptor, str], ingestion_properties: IngestionProperties):
        if not isinstance(file_descriptor, FileDescriptor):
            descriptor = FileDescriptor(file_descriptor)
        else:
            descriptor = file_descriptor
        KustoTracingAttributes.add_attributes(tracing_attributes={cls._DATABASE: ingestion_properties.database, cls._TABLE: ingestion_properties.table, cls._FILE_PATH: descriptor.stream_name, cls._SOURCE_ID: descriptor.source_id})

    @classmethod
    def set_ingest_stream_attributes(cls, stream_descriptor: Union[StreamDescriptor, IO[AnyStr]], ingestion_properties: IngestionProperties):
        if not isinstance(stream_descriptor, StreamDescriptor):
            descriptor = StreamDescriptor(stream_descriptor)
        else:
            descriptor = copy(stream_descriptor)
        KustoTracingAttributes.add_attributes(tracing_attributes={cls._DATABASE: ingestion_properties.database, cls._TABLE: ingestion_properties.table, cls._FILE_PATH: descriptor.stream_name, cls._SOURCE_ID: descriptor.source_id})

    @classmethod
    def set_ingest_blob_attributes(cls, blob_descriptor: BlobDescriptor, ingestion_properties: IngestionProperties, container_name: str = ""):
        if container_name:
            KustoTracingAttributes.add_attributes(tracing_attributes={cls._BLOB_CONTAINER: container_name})
        else:
            KustoTracingAttributes.add_attributes(tracing_attributes={cls._BLOB_URI: blob_descriptor.path})
        KustoTracingAttributes.add_attributes(
            tracing_attributes={cls._DATABASE: ingestion_properties.database, cls._TABLE: ingestion_properties.table, cls._SOURCE_ID: blob_descriptor.source_id})
