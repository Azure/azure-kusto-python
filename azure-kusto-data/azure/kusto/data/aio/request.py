import io
from collections import namedtuple
from datetime import timedelta
from typing import Union

from .security import _AadHelper
from .._decorators import documented_by, aio_documented_by
from ..exceptions import KustoAioSyntaxError, KustoServiceError
from ..request import KustoClient as KustoClientSync, _KustoClientBase, KustoConnectionStringBuilder, ClientRequestProperties, ExecuteRequestParams
from ..response import KustoResponseDataSet

try:
    from aiohttp import ClientResponse, ClientSession
except ImportError:
    raise KustoAioSyntaxError()

ResponseTuple = namedtuple("ResponseTuple", ["response", "response_json"])


@documented_by(KustoClientSync)
class KustoClient(_KustoClientBase):
    @documented_by(KustoClientSync.__init__)
    def __init__(self, kcsb: Union[KustoConnectionStringBuilder, str]):
        super().__init__(kcsb)
        # notice that in this context, federated actually just stands for add auth, not aad federated auth (legacy code)
        self._auth_provider = _AadHelper(self._kcsb) if self._kcsb.aad_federated_security else None

    @aio_documented_by(KustoClientSync.execute)
    async def execute(self, database: str, query: str, properties: ClientRequestProperties = None) -> KustoResponseDataSet:
        query = query.strip()
        if query.startswith("."):
            return await self.execute_mgmt(database, query, properties)
        return await self.execute_query(database, query, properties)

    @aio_documented_by(KustoClientSync.execute_query)
    async def execute_query(self, database: str, query: str, properties: ClientRequestProperties = None) -> KustoResponseDataSet:
        return await self._execute(self._query_endpoint, database, query, None, KustoClient._query_default_timeout, properties)

    @aio_documented_by(KustoClientSync.execute_mgmt)
    async def execute_mgmt(self, database: str, query: str, properties: ClientRequestProperties = None) -> KustoResponseDataSet:
        return await self._execute(self._mgmt_endpoint, database, query, None, KustoClient._mgmt_default_timeout, properties)

    @staticmethod
    async def _run_request(endpoint: str, **kwargs) -> ResponseTuple:
        async with ClientSession() as session:
            async with session.post(endpoint, **kwargs) as response:
                response_json = await response.json()

                return ResponseTuple(response, response_json)

    async def _query(self, endpoint: str, **kwargs) -> list:
        raw_response = await self._run_request(endpoint, **kwargs)
        response = raw_response.response
        response_json = raw_response.response_json

        if response.status != 200:
            raise KustoServiceError([response_json], response)

        return response_json

    @aio_documented_by(KustoClientSync._execute)
    async def _execute(
        self, endpoint: str, database: str, query: str, payload: io.IOBase, timeout: timedelta, properties: ClientRequestProperties = None
    ) -> KustoResponseDataSet:
        request_params = ExecuteRequestParams(database, payload, properties, query, timeout, self._request_headers)
        json_payload = request_params.json_payload
        request_headers = request_params.request_headers
        timeout = request_params.timeout

        if self._auth_provider:
            request_headers["Authorization"] = await self._auth_provider.acquire_authorization_header()

        response_json = await self._query(endpoint, headers=request_headers, data=payload, json=json_payload, timeout=timeout.seconds)

        return self._kusto_parse_by_endpoint(endpoint, response_json)
