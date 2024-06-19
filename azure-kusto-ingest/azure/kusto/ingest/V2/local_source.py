from azure.kusto.ingest import StreamDescriptor
from azure.kusto.ingest.V2.compression_type import CompressionType
from azure.kusto.ingest.V2.ingestion_source import IngestionSource
from azure.kusto.data.data_format import DataFormat
from abc import ABC, abstractmethod


class LocalSource(ABC, IngestionSource):
    def __init__(self, compression_type: CompressionType, format: DataFormat):
        super().__init__(format)
        self.compression_type = compression_type

    def should_compress(self):
        return (self.compression_type == CompressionType.Uncompressed) and self.format.compressible

    def __str__(self):
        return f"{self.__class__.__name__} SourceId: '{self.source_id}' CompressionType: '{self.compression_type}'"

    @abstractmethod
    def data(self):
        pass


class FileSource(LocalSource):
    def __init__(self, path: str, format: DataFormat, compression_type=CompressionType.Uncompressed):
        super().__init__(compression_type, format)
        self.cache_file_stream = None
        self.name = path
        if path.lower().endswith(".zip"):
            self.compression_type = CompressionType.Zip
        elif path.lower().endswith(".gz"):
            self.compression_type = CompressionType.GZip

    def data(self):
        if self.cache_file_stream is None:
            with open(self.name, "r") as file:
                self.cache_file_stream = file.read()
        return self.cache_file_stream


class StreamSource(LocalSource):
    def __init__(self, stream_descriptor: StreamDescriptor, format: DataFormat, name: str, compression_type: CompressionType):
        super().__init__(compression_type, format)
        assert stream_descriptor is not None
        self.stream_descriptor = stream_descriptor
        if name is None:
            self.name = "Stream_" + self.source_id

    def data(self):
        return self.stream_descriptor
