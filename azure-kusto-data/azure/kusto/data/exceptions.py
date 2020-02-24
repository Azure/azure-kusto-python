"""All kusto exceptions that can be thrown from this client."""
from typing import Any, List


class KustoError(Exception):
    """Base class for all exceptions raised by the Kusto Python Client Libraries."""


class KustoServiceError(KustoError):
    """Raised when the Kusto service was unable to process a request."""

    def __init__(self, messages, http_response, kusto_response=None):
        super(KustoServiceError, self).__init__(self, messages)
        self.http_response = http_response
        self.kusto_response = kusto_response

    def get_raw_http_response(self):
        """Gets the http response."""
        return self.http_response

    def is_semantic_error(self):
        """Checks if a response is a semantic error."""
        return "Semantic error:" in self.http_response.text

    def has_partial_results(self):
        """Checks if a response exists."""
        return self.kusto_response is not None

    def get_partial_results(self):
        """Gets the Kusto response."""
        return self.kusto_response


class KustoClientError(KustoError):
    """Raised when a Kusto client is unable to send or complete a request."""

    @classmethod
    def msi_token_exception(cls, msi_params, e):
        return cls("Failed to obtain MSI context for [" + str(msi_params) + "]\n" + str(e))


class KustoRequestException(KustoError):
    """Raised when a Kusto request fails"""

    def __init__(self, response, response_json):
        super().__init__()
        self.response = response
        self.response_json = response_json


class KustoAuthenticationError(KustoClientError):
    """Raised when authentication fails."""

    def __init__(self, authentication_method, exception, **kwargs):
        super(KustoAuthenticationError, self).__init__()
        self.authentication_method = authentication_method
        self.authority = kwargs["authority"]
        self.kusto_cluster = kwargs["resource"]
        self.exception = exception
        self.kwargs = kwargs

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "KustoAuthenticationError('{}', '{}', '{}')".format(self.authentication_method, repr(self.exception), self.kwargs)


class KustoAioSyntaxError(SyntaxError):
    """Raised when trying to use aio syntax without installing the needed modules"""

    def __init__(self):
        super().__init__("Aio modules not installed, run 'pip install azure-kusto-data[aio]' to leverage aio capabilities")
