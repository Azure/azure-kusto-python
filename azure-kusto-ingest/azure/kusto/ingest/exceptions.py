"""All exceptions can be raised by KustoIngestClient."""

from azure.kusto.data.exceptions import KustoClientError


class KustoMappingAndMappingReferenceError(KustoClientError):
    """
    Error to be raised when ingestion properties include both
    a mapping and a mapping reference
    """

    def __init__(self):
        message = "Ingestion properties contain both an explicit mapping and a mapping reference."
        super(KustoMappingAndMappingReferenceError, self).__init__(message)


class KustoDuplicateMappingError(KustoClientError):
    """
    Error to be raised when ingestion properties include two explicit mappings.
    """

    def __init__(self):
        message = "Ingestion properties contain two explicit mappings."
        super(KustoDuplicateMappingError, self).__init__(message)


class KustoDuplicateMappingReferenceError(KustoClientError):
    """
    Error to be raised when ingestion properties include two mapping references.
    """

    def __init__(self):
        message = "Ingestion properties contain two mapping references."
        super(KustoDuplicateMappingReferenceError, self).__init__(message)


class KustoMissingMappingReferenceError(KustoClientError):
    """
    Error to be raised when ingestion properties has data format of Json, SingleJson, MultiJson or Avro
    but ingestion mappings reference was not defined.
    """

    def __init__(self):
        message = "When stream format is json, mapping name must be provided."
        super(KustoMissingMappingReferenceError, self).__init__(message)
