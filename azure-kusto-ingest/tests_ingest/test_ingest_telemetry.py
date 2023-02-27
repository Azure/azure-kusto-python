from io import BytesIO

from azure.kusto.ingest import FileDescriptor, BlobDescriptor, StreamDescriptor
from azure.kusto.ingest.ingestion_properties import IngestionProperties


def test_get_tracing_attributes():
    ingestion_properties = IngestionProperties("database_test", "table_test")
    assert {"database": "database_test", "table": "table_test"} == ingestion_properties.get_tracing_attributes()

    dummy_stream = BytesIO(b"dummy")
    stream = StreamDescriptor(dummy_stream)
    dummy_path = "dummy"
    blob = BlobDescriptor(dummy_path)
    file = FileDescriptor(dummy_path)

    descriptors = [stream, blob, file]
    keynames = [{"stream_name", "source_id"}, {"blob_uri", "source_id"}, {"file_path", "source_id"}]
    for i in range(len(descriptors)):
        attributes = descriptors[i].get_tracing_attributes()
        assert isinstance(attributes, dict)
        for key, val in attributes.items():
            assert key in keynames[i]
            assert isinstance(val, str)
        for key in keynames[i]:
            assert key in attributes.keys()
