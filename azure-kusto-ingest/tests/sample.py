"""Sample how to use Kusto Ingest client"""

from azure.kusto.data.request import KustoConnectionStringBuilder
from azure.kusto.ingest import (
    KustoIngestClient,
    IngestionProperties,
    FileDescriptor,
    BlobDescriptor,
    DataFormat,
)

INGESTION_PROPERTIES = IngestionProperties(
    database="database name", table="table name", dataFormat=DataFormat.csv
)

INGEST_CLIENT = KustoIngestClient("https://ingest-<clustername>.kusto.windows.net")

KCSB = KustoConnectionStringBuilder.with_aad_application_key_authentication(
    "https://ingest-<clustername>.kusto.windows.net", "aad app id", "secret"
)
INGEST_CLIENT = KustoIngestClient(KCSB)

FILE_DESCRIPTOR = FileDescriptor(
    "E:\\filePath.csv", 3333
)  # 3333 is the raw size of the data in bytes.
INGEST_CLIENT.ingest_from_multiple_files(
    [FILE_DESCRIPTOR], delete_sources_on_success=True, ingestion_properties=INGESTION_PROPERTIES
)

INGEST_CLIENT.ingest_from_multiple_files(
    ["E:\\filePath.csv"], delete_sources_on_success=True, ingestion_properties=INGESTION_PROPERTIES
)

BLOB_DESCRIPTOR = BlobDescriptor(
    "https://path-to-blob.csv.gz?sas", 10
)  # 10 is the raw size of the data in bytes.
INGEST_CLIENT.ingest_from_multiple_blobs(
    [BLOB_DESCRIPTOR], delete_sources_on_success=True, ingestion_properties=INGESTION_PROPERTIES
)
