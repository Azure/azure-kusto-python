# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
from typing import List, Union
import requests


class KustoError(Exception):
    """Base class for all exceptions raised by the Kusto Python Client Libraries."""


class KustoServiceError(KustoError):
    """Raised when the Kusto service was unable to process a request."""

    def __init__(self, messages: Union[str, List[dict]], http_response: requests.Response = None, kusto_response=None):
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
        if "resource" in kwargs:
            self.kusto_cluster = kwargs["resource"]
        self.kwargs = kwargs

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "KustoAuthenticationError('{}', '{}', '{}')".format(self.authentication_method, repr(self.exception), self.kwargs)
