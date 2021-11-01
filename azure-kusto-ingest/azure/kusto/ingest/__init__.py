# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
from ._version import VERSION as __version__
from .base_ingest_client import IngestionResult, IngestionResultKind
from .descriptors import BlobDescriptor, FileDescriptor, StreamDescriptor
from .exceptions import KustoMissingMappingReferenceError
from .ingest_client import QueuedIngestClient
from .ingestion_properties import (
    DataFormat,
    ValidationPolicy,
    ValidationImplications,
    ValidationOptions,
    ReportLevel,
    ReportMethod,
    IngestionProperties,
    IngestionMappingType,
    ColumnMapping,
    TransformationMethod,
)
from .managed_streaming_ingest_client import ManagedStreamingIngestClient, FallbackReason
from .streaming_ingest_client import KustoStreamingIngestClient
