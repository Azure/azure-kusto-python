from azure.kusto.ingest import KustoIngestClient, IngestionProperties, FileDescriptor, BlobDescriptor, DataFormat

ingestion_properties = IngestionProperties(database="database name", table="table name", dataFormat=DataFormat.csv)

ingest_client = KustoIngestClient("https://ingest-clustername.kusto.windows.net", username="username@microsoft.com")
ingest_client = KustoIngestClient("https://ingest-clustername.kusto.windows.net", client_id="aad app id", client_secret="secret")

file_descriptor = FileDescriptor("E:\\filePath.csv", 3333) # 3333 is the raw size of the data in bytes.
ingest_client.ingest_from_multiple_files([file_descriptor],
                                          delete_sources_on_success=True,
                                          ingestion_properties=ingestion_properties)  

ingest_client.ingest_from_multiple_files(["E:\\filePath.csv"],
                                          delete_sources_on_success=True,
                                          ingestion_properties=ingestion_properties)

blob_descriptor = BlobDescriptor("https://path-to-blob.csv.gz?sas", 10) # 10 is the raw size of the data in bytes.
ingest_client.ingest_from_multiple_files([blob_descriptor],
                                          delete_sources_on_success=True,
                                          ingestion_properties=ingestion_properties)
