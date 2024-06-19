from typing import Union

from azure.kusto.data.data_format import DataFormat
from azure.kusto.ingest.V2.ingestion_source import IngestionSource
from azure.kusto.ingest.V2.local_source import LocalSource


class BlobSource(IngestionSource):
    def __init__(self, url: str, format_or_local_source: Union[DataFormat, LocalSource], size=None):
        if isinstance(format_or_local_source, DataFormat):
            super().__init__(format_or_local_source)
        else:
            super().__init__(format_or_local_source.format)
        self.url = url
        self.size = size

    def __str__(self):
        return f"{self.url} SourceId: '{self.source_id}'"
