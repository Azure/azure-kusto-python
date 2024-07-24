from azure.kusto.data.data_format import DataFormat
from azure.kusto.ingest.V2.ingestion_source import IngestionSource
from azure.kusto.ingest.V2.local_source import LocalSource


class BlobSource(IngestionSource):
    def __init__(self, url: str, format: DataFormat, size=None):
        super().__init__(format)
        self.url = url
        self.size = size

    @staticmethod
    def from_local_source(local: LocalSource) -> None:
        BlobSource(local.format)
