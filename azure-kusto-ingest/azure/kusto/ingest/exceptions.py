# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License

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


class KustoInvalidEndpointError(KustoClientError):
    """Raised when trying to ingest to invalid cluster type."""

    def __init__(self, expected_service_type, actual_service_type, suggested_endpoint_url=None):
        message = "You are using '{0}' client type, but the provided endpoint is of ServiceType '{1}'. Initialize the client with the appropriate endpoint URI".format(
            expected_service_type, actual_service_type
        )
        if suggested_endpoint_url:
            message = message + ": '" + suggested_endpoint_url + "'"
        super(KustoInvalidEndpointError, self).__init__(message)
