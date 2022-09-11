from typing import Callable

from azure.core.settings import settings
from azure.core.tracing.decorator import distributed_trace
from azure.core.tracing import SpanKind


class KustoTracingAttributes:
    """
    Additional ADX attributes for telemetry spans
    """

    _KUSTO_CLUSTER = "Kusto Cluster"
    _DATABASE = "Database"
    _TABLE = "Table"

    _AUTH_METHOD = "Authentication Method"

    _SPAN_COMPONENT = "component"
    _HTTP = "http"
    _HTTP_USER_AGENT = "http.user_agent"
    _HTTP_METHOD = "http.method"
    _HTTP_URL = "http.url"
    _HTTP_STATUS_CODE = "http.status_code"

    @classmethod
    def add_attributes(cls, **kwargs) -> None:
        """
        Add ADX attributes to the current span
        :key dict tracing_attributes: key, val ADX attributes to include in span of trace
        """
        tracing_attributes: dict = kwargs.pop("tracing_attributes", {})
        span_impl_type = settings.tracing_implementation()
        if span_impl_type is not None:
            current_span = span_impl_type.get_current_span()
            span = span_impl_type(span=current_span)
            for key, val in tracing_attributes.items():
                span.add_attribute(key, val)

    @classmethod
    def set_query_attributes(cls, cluster: str, database: str) -> None:
        query_attributes: dict = cls.create_query_attributes(cluster, database)
        cls.add_attributes(tracing_attributes=query_attributes)

    @classmethod
    def set_mgmt_attributes(cls, cluster: str, database: str) -> None:
        mgmt_attributes: dict = cls.create_query_attributes(cluster, database)
        cls.add_attributes(tracing_attributes=mgmt_attributes)

    @classmethod
    def set_ingest_attributes(cls, database: str, table: str) -> None:
        ingest_attributes: dict = cls.create_ingest_attributes(database, table)
        cls.add_attributes(tracing_attributes=ingest_attributes)

    @classmethod
    def set_http_attributes(cls, url: str, method: str, headers: dict) -> None:
        http_tracing_attributes: dict = cls.create_http_attributes(method, url, headers)
        cls.add_attributes(tracing_attributes=http_tracing_attributes)

    @classmethod
    def set_get_token_attributes(cls, auth_method: str) -> None:
        get_token_attributes: dict = cls.create_get_token_attributes(auth_method)
        cls.add_attributes(tracing_attributes=get_token_attributes)

    @classmethod
    def create_query_attributes(cls, cluster: str, database: str) -> dict:
        query_attributes: dict = {cls._KUSTO_CLUSTER: cluster, cls._DATABASE: database}
        return query_attributes

    @classmethod
    def create_ingest_attributes(cls, database: str, table: str) -> dict:
        ingest_attributes: dict = {cls._DATABASE: database, cls._TABLE: table}
        return ingest_attributes

    @classmethod
    def create_http_attributes(cls, method: str, url: str, headers: dict = None) -> dict:
        if headers is None:
            headers = {}
        http_tracing_attributes: dict = {
            cls._SPAN_COMPONENT: cls._HTTP,
            cls._HTTP_METHOD: method,
            cls._HTTP_URL: url,
        }
        user_agent = headers.get("User-Agent")
        if user_agent:
            http_tracing_attributes[cls._HTTP_USER_AGENT] = user_agent
        return http_tracing_attributes

    @classmethod
    def create_get_token_attributes(cls, auth_method: str) -> dict:
        get_token_attributes: dict = {cls._AUTH_METHOD: auth_method}
        return get_token_attributes


class KustoTracing:
    @staticmethod
    def call_func_tracing(func: Callable, *args, **kwargs):
        """
        Prepares function for tracing and calls it
        :param func: function to trace
        :type func: Callable
        :key str name_of_span: name of the trace span
        :key dict tracing_attributes: key/value dictionary of attributes to include in span of trace
        :key str kind: the type of span
        :param kwargs: function arguments
        """
        name_of_span: str = kwargs.pop("name_of_span", None)
        tracing_attributes: dict = kwargs.pop("tracing_attributes", {})
        kind: str = kwargs.pop("kind", SpanKind.CLIENT)

        kusto_trace: Callable = distributed_trace(name_of_span=name_of_span, tracing_attributes=tracing_attributes,
                                                  kind=kind)
        kusto_func: Callable = kusto_trace(func)
        return kusto_func(*args, **kwargs)

    @staticmethod
    def prepare_func_tracing(func: Callable, **kwargs):
        """
        Prepares function for tracing
        :param func: function to trace
        :type func: Callable
        :key str name_of_span: name of the trace span
        :key dict tracing_attributes: key/value dictionary of attributes to include in span of trace
        :key str kind: the type of span
        """
        name_of_span: str = kwargs.pop("name_of_span", None)
        tracing_attributes: dict = kwargs.pop("tracing_attributes", {})
        kind: str = kwargs.pop("kind", SpanKind.CLIENT)

        kusto_trace: Callable = distributed_trace(name_of_span=name_of_span, tracing_attributes=tracing_attributes,
                                                  kind=kind)
        return kusto_trace(func)
