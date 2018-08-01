from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.ingest import KustoIngestClient
from azure.kusto.ingest.ingestion_properties import IngestionProperties, DataFormat
from azure.kusto.ingest.descriptors import FileDescriptor, BlobDescriptor

ingestion_properties = IngestionProperties(database="database name", table="table name", dataFormat=DataFormat.csv)

kcsb1 = KustoConnectionStringBuilder.with_aad_device_authentication("https://ingest-clustername.kusto.windows.net")
kcsb2 = KustoConnectionStringBuilder.with_aad_application_key_authentication("https://ingest-clustername.kusto.windows.net",
                                                                             application_client_id="aad app id", application_key="secret")

ingest_client = KustoIngestClient(kcsb1) # Authenticating interactively
ingest_client = KustoIngestClient(kcsb2) # Authenticating with app id

file_descriptor = FileDescriptor("E:\\filePath.csv", 3333) # 3333 is the raw size of the data in bytes.
ingest_client.ingest_from_multiple_files([file_descriptor],
                                          delete_sources_on_success=True,
                                          ingestion_properties=ingestion_properties)  

ingest_client.ingest_from_multiple_files(["E:\\filePath.csv"],
                                          delete_sources_on_success=True,
                                          ingestion_properties=ingestion_properties)

blob_descriptor = BlobDescriptor("https://path-to-blob.csv.gz?sas", 10) # 10 is the raw size of the data in bytes.
ingest_client.ingest_from_multiple_blobs([blob_descriptor],
                                          delete_sources_on_success=True,
                                          ingestion_properties=ingestion_properties)
