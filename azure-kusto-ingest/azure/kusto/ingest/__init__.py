# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import logging

from ._version import VERSION as __version__
from .base_ingest_client import IngestionResult, IngestionStatus
from .descriptors import BlobDescriptor, FileDescriptor, StreamDescriptor
from .exceptions import KustoMissingMappingError
from .ingest_client import QueuedIngestClient
from .ingestion_properties import (
    ValidationPolicy,
    ValidationImplications,
    ValidationOptions,
    ReportLevel,
    ReportMethod,
    IngestionProperties,
    IngestionMappingKind,
    ColumnMapping,
    TransformationMethod,
)
from .managed_streaming_ingest_client import ManagedStreamingIngestClient
from .streaming_ingest_client import KustoStreamingIngestClient
from .base_ingest_client import BaseIngestClient
from azure.kusto.data import enable_data_logger

logger = logging.getLogger(__name__)
logger.handlers = [logging.NullHandler()]
logger.propagate = False


def enable_ingest_logger() -> logging.Logger:
    """
    Enables the Kusto ingest logger.
    If you are using azure-kusto-data, you will need to enable that logger separately.
    Note that a handler is still required to be added to this logger or a top level logger for the logs to be emitted.
    :return: The kusto ingest logger
    """
    logger.propagate = True
    return logger


def root_kusto_logger(enable_loggers: bool) -> logging.Logger:
    """
    Returns the root Kusto logger, which is the top level for both the data and ingest loggers.
    :return: The root Kusto logger.
    """
    if enable_loggers:
        enable_data_logger()
        enable_ingest_logger()
    return logging.getLogger("azure.kusto")
