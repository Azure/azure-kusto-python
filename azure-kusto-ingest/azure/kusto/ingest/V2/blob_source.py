from typing import Union

from kusto.data.data_format import DataFormat
from kusto.ingest.V2.ingestion_source import IngestionSource
from kusto.ingest.V2.local_source import LocalSource


class BlobSource(IngestionSource):
    def __init__(self, url: str, format_or_local_source: Union[DataFormat, LocalSource], exact_size=None):
        if isinstance(format_or_local_source, DataFormat):
            super().__init__(format_or_local_source)
        else:
            super().__init__(format_or_local_source.format)
        self.url = url
        self.exact_size = exact_size

    def __str__(self):
        return f"{self.url} SourceId: '{self.source_id}'"
