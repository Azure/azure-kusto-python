from typing import Callable

from azure.core.settings import settings
from azure.core.tracing.decorator import distributed_trace
from azure.core.tracing import SpanKind


def kusto_client_func_tracing(func: Callable, **kwargs):
    name_of_span = kwargs.pop("name_of_span", None)
    tracing_attributes = kwargs.pop("tracing_attributes", {})
    kind = kwargs.pop("trace_kind", SpanKind.CLIENT)

    kusto_trace = distributed_trace(name_of_span=name_of_span, tracing_attributes=tracing_attributes, kind=kind)
    kusto_func = kusto_trace(func)
    return kusto_func(**kwargs)


class KustoTracingAttributes:
    """
    Additional ADX attributes for telemetry spans
    """

    _KUSTO_CLUSTER = "Kusto Cluster"
    _DATABASE = "Database"
    _TABLE = "Table"
    _SPAN_COMPONENT = "component"

    _HTTP_USER_AGENT = "http.user_agent"
    _HTTP_METHOD = "http.method"
    _HTTP_URL = "http.url"
    _HTTP_STATUS_CODE = "http.status_code"

    @classmethod
    def add_attributes(cls, **kwargs) -> None:
        """
        Add ADX attributes to the current span

        :keyword tracing_attributes: Key, val ADX attributes for the current span
        :type tracing_attributes: dictionary
        """
        tracing_attributes = kwargs.pop("tracing_attributes", {})
        span_impl_type = settings.tracing_implementation
        if span_impl_type is not None:
            for key, val in tracing_attributes:
                span_impl_type.add_attribute(key, val)

    @classmethod
    def set_query_attributes(cls, cluster: str, database: str) -> None:
        query_attributes = cls.create_query_attributes(cluster, database)
        cls.add_attributes(tracing_attributes=query_attributes)

    @classmethod
    def set_mgmt_attributes(cls, cluster: str, database: str) -> None:
        mgmt_attributes = cls.create_query_attributes(cluster, database)
        cls.add_attributes(tracing_attributes=mgmt_attributes)

    @classmethod
    def set_ingest_attributes(cls, database: str, table: str) -> None:
        ingest_attributes = cls.create_ingest_attributes(database, table)
        cls.add_attributes(tracing_attributes=ingest_attributes)

    @classmethod
    def set_http_attributes(cls, url: str, method: str, headers: dict) -> None:
        http_tracing_attributes = cls.create_http_attributes(headers, method, url)
        cls.add_attributes(tracing_attributes=http_tracing_attributes)

    @classmethod
    def create_query_attributes(cls, cluster, database) -> dict:
        query_attributes: dict = {cls._KUSTO_CLUSTER: cluster, cls._DATABASE: database}
        return query_attributes

    @classmethod
    def create_ingest_attributes(cls, database, table) -> dict:
        ingest_attributes: dict = {cls._DATABASE: database, cls._TABLE: table}
        return ingest_attributes

    @classmethod
    def create_http_attributes(cls, headers, method, url) -> dict:
        http_tracing_attributes: dict = {cls._SPAN_COMPONENT: "http",
                                         cls._HTTP_METHOD: method,
                                         cls._HTTP_URL: url,
                                         }
        user_agent = headers.get("User-Agent")
        if user_agent:
            http_tracing_attributes[cls._HTTP_USER_AGENT] = user_agent
        return http_tracing_attributes







