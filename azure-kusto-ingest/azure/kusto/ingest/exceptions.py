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
