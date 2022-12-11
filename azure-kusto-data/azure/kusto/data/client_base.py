import abc
import io
import json
import uuid
from copy import copy
from datetime import timedelta
from typing import Union, Optional, Any, NoReturn, ClassVar
from urllib.parse import urljoin

from azure.kusto.data._cloud_settings import CloudSettings
from azure.kusto.data._token_providers import CloudInfoTokenProvider
from requests import Response

from ._version import VERSION
from .client_request_properties import ClientRequestProperties
from .exceptions import KustoServiceError, KustoThrottlingError, KustoApiError
from .kcsb import KustoConnectionStringBuilder
from .response import KustoResponseDataSet, KustoResponseDataSetV2, KustoResponseDataSetV1
from .security import _AadHelper
from .kusto_trusted_endpoints import well_known_kusto_endpoints


class _KustoClientBase(abc.ABC):
    API_VERSION = "2019-02-13"

    _mgmt_default_timeout: ClassVar[timedelta] = timedelta(hours=1, seconds=30)
    _query_default_timeout: ClassVar[timedelta] = timedelta(minutes=4, seconds=30)
    _streaming_ingest_default_timeout: ClassVar[timedelta] = timedelta(minutes=10)
    _client_server_delta: ClassVar[timedelta] = timedelta(seconds=30)

    _aad_helper: _AadHelper
    _endpoint_validated = False

    def __init__(self, kcsb: Union[KustoConnectionStringBuilder, str], is_async):
        self._kcsb = kcsb
        self._proxy_url: Optional[str] = None
        if not isinstance(kcsb, KustoConnectionStringBuilder):
            self._kcsb = KustoConnectionStringBuilder(kcsb)
        self._kusto_cluster = self._kcsb.data_source

        # notice that in this context, federated actually just stands for aad auth, not aad federated auth (legacy code)
        self._aad_helper = _AadHelper(self._kcsb, is_async) if self._kcsb.aad_federated_security else None

        # Create a session object for connection pooling
        self._mgmt_endpoint = urljoin(self._kusto_cluster, "v1/rest/mgmt")
        self._query_endpoint = urljoin(self._kusto_cluster, "v2/rest/query")
        self._streaming_ingest_endpoint = urljoin(self._kusto_cluster, "v1/rest/ingest/")
        self._request_headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip,deflate",
            "x-ms-client-version": "Kusto.Python.Client:" + VERSION,
            "x-ms-version": self.API_VERSION,
        }

        self._is_closed: bool = False

    def close(self):
        if not self._is_closed:
            if self._aad_helper is not None:
                self._aad_helper.close()
        self._is_closed = True

    def set_proxy(self, proxy_url: str):
        self._proxy_url = proxy_url
        if self._aad_helper:
            self._aad_helper.token_provider.set_proxy(proxy_url)

    def validate_endpoint(self):
        if not self._endpoint_validated and self._aad_helper is not None:
            if isinstance(self._aad_helper.token_provider, CloudInfoTokenProvider):
                well_known_kusto_endpoints.validate_trusted_endpoint(
                    self._kusto_cluster, CloudSettings.get_cloud_info_for_cluster(self._kusto_cluster).login_endpoint
                )
            self._endpoint_validated = True

    @staticmethod
    def _kusto_parse_by_endpoint(endpoint: str, response_json: Any) -> KustoResponseDataSet:
        if endpoint.endswith("v2/rest/query"):
            return KustoResponseDataSetV2(response_json)
        return KustoResponseDataSetV1(response_json)

    @staticmethod
    def _handle_http_error(
        exception: Exception,
        endpoint: Optional[str],
        payload: Optional[io.IOBase],
        response: "Union[Response, aiohttp.ClientResponse]",
        status: int,
        response_json: Any,
        response_text: Optional[str],
    ) -> NoReturn:

        if status == 404:
            if payload:
                raise KustoServiceError("The ingestion endpoint does not exist. Please enable streaming ingestion on your cluster.", response) from exception

            raise KustoServiceError(f"The requested endpoint '{endpoint}' does not exist.", response) from exception

        if status == 429:
            raise KustoThrottlingError("The request was throttled by the server.", response) from exception

        if payload:
            message = f"An error occurred while trying to ingest: Status: {status}, Reason: {response.reason}, Text: {response_text}."
            if response_json:
                raise KustoApiError(response_json, message, response) from exception

            raise KustoServiceError(message, response) from exception

        if response_json:
            raise KustoApiError(response_json, http_response=response) from exception

        if response_text:
            raise KustoServiceError(response_text, response) from exception

        raise KustoServiceError("Server error response contains no data.", response) from exception


class ExecuteRequestParams:
    def __init__(
        self,
        database: str,
        payload: Optional[io.IOBase],
        properties: ClientRequestProperties,
        query: str,
        timeout: timedelta,
        request_headers: dict,
        mgmt_default_timeout: timedelta,
        client_server_delta: timedelta,
    ):
        request_headers = copy(request_headers)
        request_headers["Connection"] = "Keep-Alive"
        json_payload = None
        if not payload:
            json_payload = {"db": database, "csl": query}
            if properties:
                json_payload["properties"] = properties.to_json()

            client_request_id_prefix = "KPC.execute;"
            request_headers["Content-Type"] = "application/json; charset=utf-8"
        else:
            if properties:
                request_headers.update(json.loads(properties.to_json())["Options"])

            # Before 3.0 it was KPC.execute_streaming_ingest, but was changed to align with the other SDKs
            client_request_id_prefix = "KPC.executeStreamingIngest;"
            request_headers["Content-Encoding"] = "gzip"
        request_headers["x-ms-client-request-id"] = client_request_id_prefix + str(uuid.uuid4())
        if properties is not None:
            if properties.client_request_id is not None:
                request_headers["x-ms-client-request-id"] = properties.client_request_id
            if properties.application is not None:
                request_headers["x-ms-app"] = properties.application
            if properties.user is not None:
                request_headers["x-ms-user"] = properties.user
            if properties.get_option(ClientRequestProperties.no_request_timeout_option_name, False):
                timeout = mgmt_default_timeout
            else:
                timeout = properties.get_option(ClientRequestProperties.request_timeout_option_name, timeout)

        timeout = (timeout or mgmt_default_timeout) + client_server_delta

        self.json_payload = json_payload
        self.request_headers = request_headers
        self.timeout = timeout
