from azure.core.settings import settings


class KustoTracingAttributes:
    """
    Additional ADX attributes for telemetry spans
    """

    database = "Database"
    type = "Type"
    query = "Query"
    ingest = "Ingest"
    table = "Table"
    streaming_ingest = "Streaming Ingest"
    streaming_query = "Streaming Query"
    kql_cmd = "KQL Command"
    ctrl_cmd = "Control Command"

    @classmethod
    def _add_attributes(cls, **kwargs) -> None:
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
    def add_query_attributes(cls, database: str, query: str, streaming: bool = False):
        query_type = KustoTracingAttributes.query if not streaming else KustoTracingAttributes.streaming_query
        cls._add_attributes(
            tracing_attributes={KustoTracingAttributes.database: database, KustoTracingAttributes.type: query_type, KustoTracingAttributes.kql_cmd: query}
        )

    @classmethod
    def add_mgmt_attributes(cls, database, query):
        cls._add_attributes(
            tracing_attributes={
                KustoTracingAttributes.database: database,
                KustoTracingAttributes.type: KustoTracingAttributes.ctrl_cmd,
                KustoTracingAttributes.kql_cmd: query,
            }
        )

    @classmethod
    def add_ingest_attributes(cls, database, table, streaming: bool = False):
        ingestion_type = KustoTracingAttributes.ingest if not streaming else KustoTracingAttributes.streaming_ingest
        cls._add_attributes(
            tracing_attributes={KustoTracingAttributes.database: database, KustoTracingAttributes.type: ingestion_type, KustoTracingAttributes.table: table}
        )
