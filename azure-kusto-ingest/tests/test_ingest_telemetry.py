import unittest
from io import BytesIO

from azure.kusto.ingest import FileDescriptor, BlobDescriptor, StreamDescriptor
from azure.kusto.ingest.ingestion_properties import IngestionProperties


class TestIngestTelemetry(unittest.TestCase):
    def test_get_tracing_attributes(self):
        ingestion_properties = IngestionProperties("database_test", "table_test")
        assert {"database": "database_test", "table": "table_test"} == ingestion_properties.get_tracing_attributes()

        dummy_stream = BytesIO(b"dummy")
        stream = StreamDescriptor(dummy_stream)
        dummy_path = "dummy"
        blob = BlobDescriptor(dummy_path)
        file = FileDescriptor(dummy_path)

        descriptors = [stream, blob, file]
        for descriptor in descriptors:
            attributes = descriptor.get_tracing_attributes()
            assert isinstance(attributes, dict)
            for key, val in attributes.items():
                assert isinstance(key, str)
                assert isinstance(val, str)
                assert str.lower(key) == key
                assert " " not in key
