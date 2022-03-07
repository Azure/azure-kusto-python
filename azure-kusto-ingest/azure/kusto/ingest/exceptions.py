# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
from azure.kusto.data.exceptions import KustoClientError


class KustoMappingError(KustoClientError):
    """
    Raised when the provided mapping arguments are invalid.
    """


class KustoDuplicateMappingError(KustoClientError):
    """
    Raised when ingestion properties include both
    column mappings and a mapping reference
    """

    def __init__(self):
        message = "Ingestion properties can't contain both an explicit mapping and a mapping reference."
        super(KustoDuplicateMappingError, self).__init__(message)


class KustoMissingMappingError(KustoClientError):
    """
    Raised when provided a mapping kind without a mapping reference or column mapping.
    """


class KustoInvalidEndpointError(KustoClientError):
    """Raised when trying to ingest to invalid cluster type."""

    def __init__(self, expected_service_type, actual_service_type, suggested_endpoint_url=None):
        message = f"You are using '{expected_service_type}' client type, but the provided endpoint is of ServiceType '{actual_service_type}'. Initialize the client with the appropriate endpoint URI"
        if suggested_endpoint_url:
            message = message + ": '" + suggested_endpoint_url + "'"
        super(KustoInvalidEndpointError, self).__init__(message)


class KustoFailedAfterRetry(KustoClientError):
    """
    Raised when function execution fails after X number of retries.
    """

    def __init__(self, exp: Exception, max_retry):
        self.inner = exp
        self.max_retry = max_retry
        super(KustoFailedAfterRetry, self).__init__(self.__str__())

    def __str__(self) -> str:
        return f"Exception ({self.inner}) raised after {self.max_retry} tries."
