"""All exceptions can be raised by KustoIngestClient."""

from azure.kusto.data.exceptions import KustoClientError


class KustoDuplicateMappingError(KustoClientError):
    """
    Error to be raised when ingestion properties has both
    ingestion mappings and ingestion mapping reference.
    """

    def __init__(self):
        message = "Ingestion properties contain more than one mapping " \
                  "(i.e. both a mapping and a mapping reference or two references)."
        super(KustoDuplicateMappingError, self).__init__(message)


class KustoMissingMappingReferenceError(KustoClientError):
    """
    Error to be raised when ingestion properties has data format of Json, SingleJson, MultiJson or Avro
    but ingestion mappings reference was not defined.
    """

    def __init__(self):
        message = "When stream format is json, mapping name must be provided."
        super(KustoMissingMappingReferenceError, self).__init__(message)
