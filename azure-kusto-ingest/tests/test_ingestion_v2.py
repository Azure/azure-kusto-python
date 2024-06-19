from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.data import DataFormat
from azure.kusto.ingest.V2.kusto_storage_uploader import KustoStorageUploader
from azure.kusto.ingest.V2.local_source import FileSource
from azure.kusto.tests.test_e2e_ingest import TestE2E


class TestIngestionV2:
    def __init__(self):
        self.dm_kcsb = KustoConnectionStringBuilder(TestE2E.dm_cs)
        self.uploader = KustoStorageUploader(self.dm_kcsb)

    def test_upload_source_is_regular_file(self):
        file_source = FileSource("azure-kusto-ingest/tests/input/dataset.csv", DataFormat.CSV)
        blob_source = self.uploader.upload_local_source(file_source)
        assert "dataset.csv" == blob_source.url

    def test_upload_source_is_zip_file(self):
        file_source = FileSource("azure-kusto-ingest/tests/input/dataset.csv.zip", DataFormat.CSV)
        blob_source = self.uploader.upload_local_source(file_source)
        assert "dataset.csv" == blob_source.url

    def test_upload_source_is_gzip_file(self):
        file_source = FileSource("azure-kusto-ingest/tests/input/dataset.csv.gz", DataFormat.CSV)
        blob_source = self.uploader.upload_local_source(file_source)
        assert "dataset.csv" == blob_source.url
