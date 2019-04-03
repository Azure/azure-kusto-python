"""All exceptions can be raised by KustoIngestClient."""

from azure.kusto.data.exceptions import KustoClientError


class KustoDuplicateMappingError(KustoClientError):
    """
    Error to be raised when ingestion properties has both
    ingestion mappings and ingestion mapping reference.
    """

    def __init__(self):
        message = "Ingestion properties contains ingestion mapping and ingestion mapping reference."
        super(KustoDuplicateMappingError, self).__init__(message)


class KustoStreamMaxSizeExceededError(KustoClientError):
    """
    Error to be raised when stream is too big for streaming ingest
    """

    def __init__(self):
        message = "The provided stream exceeded the maximum allowed size for streaming ingest"
        super(KustoStreamMaxSizeExceededError, self).__init__(message)


class KustoMissingMappingReferenceError(KustoClientError):
    """
    Error to be raised when ingestion properties has data format of Json, SingleJson, MultiJson or Avro
    but ingestion mappings reference was not defined.
    """

    def __init__(self):
        message = "When stream format is json, mapping name must be provided."
        super(KustoMissingMappingReferenceError, self).__init__(message)
