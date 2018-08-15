"""All exceptions can be raised by kusto_ingest_client."""

from azure.kusto.data.exceptions import KustoClientError


class KustoDuplicateMappingError(KustoClientError):
    """
    Error to be raised when ingestion properties has both
    ingestion mappings and ingestion mapping reference.
    """

    def __init__(self):
        message = "Ingestion properties contains ingestion mapping and ingestion mapping reference."
        super(KustoDuplicateMappingError, self).__init__(message)

class NonexistentSourceIdException(KustoClientError):
    """Error to be raised when a non existent source ID was provided to retrieve the status of an ingestion"""
    
    def __init__(self):
        message = "Non existent source ID was provided to retrieve the status of an ingestion."
        super(NonexistentSourceIdException, self).__init__(message)
