# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import json
from typing import List, Union, TYPE_CHECKING, Optional, Dict, Any

if TYPE_CHECKING:
    import requests

    try:
        from aiohttp import ClientResponse
    except ImportError:
        # No aio installed, ignore
        ClientResponse = None
        pass


class KustoError(Exception):
    """Base class for all exceptions raised by the Kusto Python Client Libraries."""


class KustoStreamingQueryError(KustoError):
    ...


class KustoTokenParsingError(KustoStreamingQueryError):
    ...


class KustoServiceError(KustoError):
    """Raised when the Kusto service was unable to process a request."""

    def __init__(
        self,
        messages: Union[str, List[dict]],
        http_response: "Union[requests.Response, ClientResponse, None]" = None,
        kusto_response: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(messages)
        self.http_response = http_response
        self.kusto_response = kusto_response

    def get_raw_http_response(self) -> "Union[requests.Response, ClientResponse, None]":
        """Gets the http response."""
        return self.http_response

    def is_semantic_error(self) -> bool:
        """Checks if a response is a semantic error."""
        try:
            return "Semantic error:" in self.http_response.text
        except AttributeError:
            return False

    def has_partial_results(self) -> bool:
        """Checks if a response exists."""
        return self.kusto_response is not None

    def get_partial_results(self) -> Optional[Dict[str, Any]]:
        """Gets the Kusto response."""
        return self.kusto_response


class OneApiError:
    def __init__(self, code: str, message: str, type: str, description: str, context: dict, permanent: bool) -> None:
        self.code = code
        self.message = message
        self.type = type
        self.description = description
        self.context = context
        self.permanent = permanent

    @staticmethod
    def from_dict(obj: dict) -> "OneApiError":
        try:
            code = obj["code"]
            message = obj["message"]
            type = obj["@type"]
            description = obj["@message"]
            context = obj["@context"]
            permanent = obj["@permanent"]
            return OneApiError(code, message, type, description, context, permanent)
        except Exception as e:
            return OneApiError(
                "FailedToParse", f"Failed to parse one api error. Got {e}. Full object - {json.dumps(obj)}", "FailedToParseOneApiError", "", {}, False
            )


class KustoMultiApiError(KustoServiceError):
    """
    Represents a collection of standard API errors from kusto. Use `get_api_errors()` to retrieve more details.
    """

    def __init__(self, errors: List[dict]):
        self.errors = KustoMultiApiError.parse_errors(errors)
        messages = [error.description for error in self.errors]
        super().__init__(messages[0] if len(self.errors) == 1 else messages)

    def get_api_errors(self) -> List[OneApiError]:
        return self.errors

    @staticmethod
    def parse_errors(errors: List[dict]) -> List[OneApiError]:
        parsed_errors = []
        for error_block in errors:
            one_api_errors = error_block.get("OneApiErrors", None)
            if not one_api_errors:
                continue
            for inner_error in one_api_errors:
                error_dict = inner_error.get("error", None)
                if error_dict:
                    parsed_errors.append(OneApiError.from_dict(error_dict))
        return parsed_errors


class KustoApiError(KustoServiceError):
    """
    Represents a standard API error from kusto. Use `get_api_error()` to retrieve more details.
    """

    def __init__(self, error_dict: dict, message: str = None, http_response: "Union[requests.Response, ClientResponse, None]" = None, kusto_response=None):
        self.error = OneApiError.from_dict(error_dict["error"])
        super().__init__(message or self.error.description, http_response, kusto_response)

    def get_api_error(self) -> OneApiError:
        return self.error


class KustoClientError(KustoError):
    """Raised when a Kusto client is unable to send or complete a request."""


class KustoBlobError(KustoClientError):
    def __init__(self, inner: Exception):
        self.inner = inner

    def message(self) -> str:
        return f"Failed to upload blob: {self.inner}"


class KustoUnsupportedApiError(KustoError):
    """Raised when a Kusto client is unable to send or complete a request."""

    @staticmethod
    def progressive_api_unsupported() -> "KustoUnsupportedApiError":
        return KustoUnsupportedApiError("Progressive API is unsupported - to resolve, set results_progressive_enabled=false")


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


class KustoAsyncUsageError(Exception):
    """Raised when trying to use async methods on a sync object, and vice-versa"""

    def __init__(self, method: str, is_client_async: bool):
        super().__init__("Method {} can't be called from {} client".format(method, "an asynchronous" if is_client_async else "a synchronous"))


class KustoThrottlingError(KustoError):
    """Raised when API call gets throttled by the server."""

    ...


class KustoClientInvalidConnectionStringException(KustoError):
    """Raised when call is made to a non-trusted endpoint."""

    ...


class KustoClosedError(KustoError):
    """Raised when a client is closed."""

    def __init__(self):
        super().__init__("The client cannot be used because it was closed in the past.")
