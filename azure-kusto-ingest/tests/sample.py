"""Samples on how to use Kusto Ingest client. Just Replace variables and run!"""

from azure.kusto.data.request import KustoConnectionStringBuilder
from azure.kusto.ingest import KustoIngestClient, IngestionProperties, FileDescriptor, BlobDescriptor, DataFormat

ingestion_props = IngestionProperties(database="{database_name}", table="{table_name}", dataFormat=DataFormat.csv)
client = KustoIngestClient("https://ingest-{cluster_name}.kusto.windows.net")

# there are more posbilities for authenticating
# KCSB = KustoConnectionStringBuilder.with_aad_application_key_authentication(
#     "https://ingest-<clustername>.kusto.windows.net", "aad app id", "secret"
# )
# INGEST_CLIENT = KustoIngestClient(KCSB)


file_descriptor = FileDescriptor("{filename}.csv", 3333)  # 3333 is the raw size of the data in bytes.
client.ingest_from_file(file_descriptor, ingestion_properties=ingestion_props)
client.ingest_from_file("{filename}.csv", ingestion_properties=ingestion_props)

blob_descriptor = BlobDescriptor("https://{path_to_blob}.csv.gz?sas", 10)  # 10 is the raw size of the data in bytes.
client.ingest_from_blob(blob_descriptor, ingestion_properties=ingestion_props)

# if status updates are required, something like this can be done
import pprint
import time
from azure.kusto.ingest import KustoIngestStatusQueues

qs = KustoIngestStatusQueues(client)

MAX_BACKOFF = 180

backoff = 1
while True:
    if qs.success.is_empty() and qs.failure.is_empty():
        time.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF)
        print("No new messages. backing off for {} seconds".format(backoff))
        continue

    backoff = 1

    success_messages = qs.success.pop(10)
    failure_messages = qs.failure.pop(10)


    pprint.pprint("SUCCESS : {}".format(success_messages))
    pprint.pprint("FAILURE : {}".format(failure_messages))

    # you can of course separate them and dump them into a file for follow up investigations
    with open('successes.log','w+') as sf:
        for sm in success_messages:
            sf.write(str(sm))

    with open('failures.log','w+') as ff:
        for fm in failure_messages:
            ff.write(str(fm))