from .version import VERSION

__version__ = VERSION


class KustoIngestFactory(object):
    @staticmethod
    def create_ingest_client(kusto_connection_string_builder):
        from ._kusto_ingest_client import _KustoIngestClient

        return _KustoIngestClient(kusto_connection_string_builder)
