# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import logging
from ._version import VERSION as __version__
from .client import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties
from .data_format import DataFormat

logger = logging.getLogger(__name__)
logger.handlers = [logging.NullHandler()]
logger.propagate = False


def enable_data_logger() -> logging.Logger:
    """
    Enables the Kusto data logger.
    Note that a handler is still required to be added to this logger or a top level logger for the logs to be emitted.
    :return: The Kusto data logger.
    """
    logger.propagate = True
    return logger
