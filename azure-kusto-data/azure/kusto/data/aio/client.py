import io
from datetime import timedelta
from typing import Union, Optional

from .response import KustoStreamingResponseDataSet
from .._decorators import documented_by, aio_documented_by
from ..aio.streaming_response import StreamingDataSetEnumerator, JsonTokenReader
from ..client import KustoClient as KustoClientSync, _KustoClientBase, KustoConnectionStringBuilder, ClientRequestProperties, ExecuteRequestParams
from ..data_format import DataFormat
from ..exceptions import KustoAioSyntaxError
from ..response import KustoResponseDataSet

try:
    from aiohttp import ClientResponse, ClientSession
except ImportError:
    raise KustoAioSyntaxError()


@documented_by(KustoClientSync)
class KustoClient(_KustoClientBase):
    @documented_by(KustoClientSync.__init__)
    def __init__(self, kcsb: Union[KustoConnectionStringBuilder, str]):
        super().__init__(kcsb, True)

        self._session = ClientSession()

    async def __aenter__(self) -> "KustoClient":
        return self

    def __aexit__(self, exc_type, exc_val, exc_tb):
        return self._session.__aexit__(exc_type, exc_val, exc_tb)

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

    @aio_documented_by(KustoClientSync.execute_streaming_ingest)
    async def execute_streaming_ingest(
        self,
        database: str,
        table: str,
        stream: io.IOBase,
        stream_format: Union[DataFormat, str],
        properties: ClientRequestProperties = None,
        mapping_name: str = None,
    ):
        stream_format = stream_format.kusto_value if isinstance(stream_format, DataFormat) else DataFormat[stream_format.upper()].kusto_value
        endpoint = self._streaming_ingest_endpoint + database + "/" + table + "?streamFormat=" + stream_format
        if mapping_name is not None:
            endpoint = endpoint + "&mappingName=" + mapping_name

        await self._execute(endpoint, database, None, stream, self._streaming_ingest_default_timeout, properties)

    @aio_documented_by(KustoClientSync._execute_streaming_query_parsed)
    async def _execute_streaming_query_parsed(
        self, database: str, query: str, timeout: timedelta = _KustoClientBase._query_default_timeout, properties: Optional[ClientRequestProperties] = None
    ) -> StreamingDataSetEnumerator:
        response = await self._execute(self._query_endpoint, database, query, None, timeout, properties, stream_response=True)
        return StreamingDataSetEnumerator(JsonTokenReader(response.content))

    @aio_documented_by(KustoClientSync.execute_streaming_query)
    async def execute_streaming_query(
        self, database: str, query: str, timeout: timedelta = _KustoClientBase._query_default_timeout, properties: Optional[ClientRequestProperties] = None
    ) -> KustoStreamingResponseDataSet:
        response = await self._execute_streaming_query_parsed(database, query, timeout, properties)
        return KustoStreamingResponseDataSet(response)

    @aio_documented_by(KustoClientSync._execute)
    async def _execute(
        self,
        endpoint: str,
        database: str,
        query: Optional[str],
        payload: Optional[io.IOBase],
        timeout: timedelta,
        properties: ClientRequestProperties = None,
        stream_response: bool = False,
    ) -> Union[KustoResponseDataSet, ClientResponse]:
        """Executes given query against this client"""
        request_params = ExecuteRequestParams(database, payload, properties, query, timeout, self._request_headers)
        json_payload = request_params.json_payload
        request_headers = request_params.request_headers
        timeout = request_params.timeout
        if self._aad_helper:
            request_headers["Authorization"] = await self._aad_helper.acquire_authorization_header_async()

        response = await self._session.post(endpoint, headers=request_headers, data=payload, json=json_payload, timeout=timeout.seconds, proxy=self._proxy_url)

        if stream_response:
            try:
                response.raise_for_status()
                return response
            except Exception as e:
                try:
                    response_text = await response.text()
                except Exception:
                    response_text = None
                try:
                    response_json = await response.json()
                except Exception:
                    response_json = None
                raise self._handle_http_error(e, endpoint, payload, response, response.status, response_json, response_text)

        async with response:
            response_json = None
            try:
                response_json = await response.json()
                response.raise_for_status()
            except Exception as e:
                try:
                    response_text = await response.text()
                except Exception:
                    response_text = None
                raise self._handle_http_error(e, endpoint, payload, response, response.status, response_json, response_text)

            return self._kusto_parse_by_endpoint(endpoint, response_json)
