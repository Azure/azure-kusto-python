from copy import copy
from typing import Union, AnyStr, IO
import uuid

from azure.kusto.data._telemetry import KustoTracingAttributes

from .descriptors import FileDescriptor, StreamDescriptor, BlobDescriptor, DescriptorBase
from .ingestion_properties import IngestionProperties


class IngestTracingAttributes:
    """
    Additional ADX attributes for telemetry spans
    """

    _BLOB_CONTAINER = "Blob Container"
    _BLOB_QUEUE_NAME = "Blob Queue Name"
    _SOURCE_ID = "Source ID"

    @classmethod
    def set_ingest_descriptor_attributes(cls, descriptor: DescriptorBase, ingestion_properties: IngestionProperties) -> None:
        KustoTracingAttributes.add_attributes(tracing_attributes={**ingestion_properties.get_tracing_attributes(), **descriptor.get_tracing_attributes()})

    @classmethod
    def set_upload_blob_attributes(cls, blob_container_name: str, blob_descriptor: BlobDescriptor) -> None:
        KustoTracingAttributes.add_attributes(tracing_attributes={cls._BLOB_CONTAINER: blob_container_name, cls._SOURCE_ID: str(blob_descriptor.source_id)})

    @classmethod
    def create_enqueue_request_attributes(cls, queue_name: str, source_id: uuid.UUID) -> dict:
        enqueue_request_attributes = {cls._BLOB_QUEUE_NAME: queue_name, cls._SOURCE_ID: str(source_id)}
        return enqueue_request_attributes
