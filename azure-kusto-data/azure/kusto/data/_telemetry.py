from typing import Callable, Optional

from azure.core.settings import settings
from azure.core.tracing.decorator import distributed_trace
from azure.core.tracing.decorator_async import distributed_trace_async
from azure.core.tracing import SpanKind

from .client_request_properties import ClientRequestProperties


class SpanAttributes:
    """
    Additional ADX attributes for telemetry spans
    """

    _KUSTO_CLUSTER = "kusto_cluster"
    _DATABASE = "database"
    _TABLE = "table"

    _AUTH_METHOD = "authentication_method"
    _CLIENT_ACTIVITY_ID = "client_activity_id"

    _SPAN_COMPONENT = "component"
    _HTTP = "http"
    _HTTP_USER_AGENT = "http.user_agent"
    _HTTP_METHOD = "http.method"
    _HTTP_URL = "http.url"

    @classmethod
    def add_attributes(cls, **kwargs) -> None:
        """
        Add ADX attributes to the current span
        :key dict tracing_attributes: key, val ADX attributes to include in span of trace
        """
        tracing_attributes: dict = kwargs.pop("tracing_attributes", {})
        span_impl_type = settings.tracing_implementation()
        if span_impl_type is None:
            return
        current_span = span_impl_type.get_current_span()
        span = span_impl_type(span=current_span)
        for key, val in tracing_attributes.items():
            span.add_attribute(key, val)

    @classmethod
    def set_query_attributes(cls, cluster: str, database: str,
                             properties: Optional[ClientRequestProperties] = None) -> None:
        query_attributes: dict = cls.create_query_attributes(cluster, database, properties)
        cls.add_attributes(tracing_attributes=query_attributes)

    @classmethod
    def set_streaming_ingest_attributes(cls, cluster: str, database: str, table: str,
                                        properties: Optional[ClientRequestProperties] = None) -> None:
        ingest_attributes: dict = cls.create_streaming_ingest_attributes(cluster, database, table, properties)
        cls.add_attributes(tracing_attributes=ingest_attributes)

    @classmethod
    def set_cloud_info_attributes(cls, url: str) -> None:
        cloud_info_attributes: dict = cls.create_cloud_info_attributes(url)
        cls.add_attributes(tracing_attributes=cloud_info_attributes)

    @classmethod
    def create_query_attributes(cls, cluster: str, database: str,
                                properties: Optional[ClientRequestProperties] = None) -> dict:
        query_attributes: dict = {cls._KUSTO_CLUSTER: cluster, cls._DATABASE: database}
        if properties:
            query_attributes.update(properties.get_tracing_attributes())

        return query_attributes

    @classmethod
    def create_streaming_ingest_attributes(cls, cluster: str, database: str, table: str,
                                           properties: Optional[ClientRequestProperties] = None) -> dict:
        ingest_attributes: dict = {cls._KUSTO_CLUSTER: cluster, cls._DATABASE: database, cls._TABLE: table}
        if properties:
            ingest_attributes.update(properties.get_tracing_attributes())

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
    def create_cloud_info_attributes(cls, url: str) -> dict:
        ingest_attributes: dict = {cls._HTTP_URL: url}
        return ingest_attributes

    @classmethod
    def create_cluster_attributes(cls, cluster_uri: str) -> dict:
        cluster_attributes = {cls._KUSTO_CLUSTER: cluster_uri}
        return cluster_attributes


class Span:
    def __init__(self, name_of_span: str = None, tracing_attributes=None,
                 kind: str = SpanKind.INTERNAL):
        if tracing_attributes is None:
            tracing_attributes = {}
        self._name_of_span = name_of_span
        self._tracing_attributes = tracing_attributes
        self._kind = kind

    def run_span(self, invoker: Callable):
        """
        Runs the span on given function
        """
        span_shell: Callable = distributed_trace(name_of_span=self._name_of_span,
                                                 tracing_attributes=self._tracing_attributes,
                                                 kind=self._kind)
        span = span_shell(invoker)
        return span()

    def run_span_async(self, invoker: Callable):
        """
        Runs a span on given function
        """
        span_shell: Callable = distributed_trace_async(name_of_span=self._name_of_span,
                                                       tracing_attributes=self._tracing_attributes,
                                                       kind=self._kind)
        span = span_shell(invoker)
        return span()

    # def _call_func_tracing(self, name_of_span: str, tracing_attributes: dict, kind: str):
    #     """
    #     Prepares function for tracing and calls it
    #     :param func: function to trace
    #     :type func: Callable
    #     :param name_of_span: name of the trace span
    #     :param tracing_attributes: key/value dictionary of attributes to include in span of trace
    #     :param  kind: the type of span
    #     """
    #
    #     self._span_shell: Callable = distributed_trace(name_of_span=name_of_span, tracing_attributes=tracing_attributes,
    #                                                    kind=kind)
    #     return self
    #
    # async def _call_func_tracing_async(self, name_of_span: str, tracing_attributes: dict, kind: str):
    #     """
    #     Prepares function for tracing and calls it
    #     :param func: function to trace
    #     :type func: Callable
    #     :key str name_of_span: name of the trace span
    #     :key dict tracing_attributes: key/value dictionary of attributes to include in span of trace
    #     :key str kind: the type of span
    #     :param kwargs: function arguments
    #     """
    #
    #     self._span_shell: Callable = distributed_trace_async(name_of_span=name_of_span,
    #                                                          tracing_attributes=tracing_attributes, kind=kind)
    #     return self

    # @staticmethod
    # def prepare_func_tracing(func: Callable, **kwargs):
    #     """
    #     Prepares function for tracing
    #     :param func: function to trace
    #     :type func: Callable
    #     :key str name_of_span: name of the trace span
    #     :key dict tracing_attributes: key/value dictionary of attributes to include in span of trace
    #     :key str kind: the type of span
    #     """
    #     name_of_span: str = kwargs.pop("name_of_span", None)
    #     tracing_attributes: dict = kwargs.pop("tracing_attributes", {})
    #     kind: str = kwargs.pop("kind", SpanKind.CLIENT)
    #
    #     span_shell: Callable = distributed_trace(name_of_span=name_of_span, tracing_attributes=tracing_attributes,
    #                                          kind=kind)
    #     return span_shell(func)
