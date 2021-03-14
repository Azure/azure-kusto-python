# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
from typing import List, Union, TYPE_CHECKING
import requests

if TYPE_CHECKING:
    try:
        from aiohttp import ClientResponse
    except ImportError:
        # No aio installed, ignore
        pass


class KustoError(Exception):
    """Base class for all exceptions raised by the Kusto Python Client Libraries."""


class KustoServiceError(KustoError):
    """Raised when the Kusto service was unable to process a request."""

    def __init__(self, messages: Union[str, List[dict]], http_response: "Union[requests.Response, ClientResponse, None]" = None, kusto_response=None):
        super().__init__(messages)
        self.http_response = http_response
        self.kusto_response = kusto_response

    def get_raw_http_response(self):
        """Gets the http response."""
        return self.http_response

    def is_semantic_error(self):
        """Checks if a response is a semantic error."""
        try:
            return "Semantic error:" in self.http_response.text
        except AttributeError:
            return False

    def has_partial_results(self):
        """Checks if a response exists."""
        return self.kusto_response is not None

    def get_partial_results(self):
        """Gets the Kusto response."""
        return self.kusto_response


class KustoClientError(KustoError):
    """Raised when a Kusto client is unable to send or complete a request."""


class KustoAuthenticationError(KustoClientError):
    """Raised when authentication fails."""

    def __init__(self, authentication_method: str, exception: Exception, **kwargs):
        super().__init__()
        self.authentication_method = authentication_method
        self.exception = exception
        if "authority" in kwargs:
            self.authority = kwargs["authority"]
        if "kusto_uri" in kwargs:
            self.kusto_cluster = kwargs["kusto_uri"]
        self.kwargs = kwargs

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "KustoAuthenticationError('{}', '{}', '{}')".format(self.authentication_method, repr(self.exception), self.kwargs)


class KustoAioSyntaxError(SyntaxError):
    """Raised when trying to use aio syntax without installing the needed modules"""

    def __init__(self):
        super().__init__("Aio modules not installed, run 'pip install azure-kusto-data[aio]' to leverage aio capabilities")
