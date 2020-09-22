# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import io

from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.ingest import (
    QueuedIngestClient,
    IngestionProperties,
    FileDescriptor,
    BlobDescriptor,
    StreamDescriptor,
    DataFormat,
    ReportLevel,
    IngestionMappingType,
    KustoStreamingIngestClient,
)

##################################################################
##                              AUTH                            ##
##################################################################
cluster = "https://ingest-{cluster_name}.kusto.windows.net"

# In case you want to authenticate with AAD application.
client_id = "<insert here your AAD application id>"
client_secret = "<insert here your AAD application key>"

# read more at https://docs.microsoft.com/en-us/onedrive/find-your-office-365-tenant-id
authority_id = "<insert here your tenant id>"

kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, client_id, client_secret, authority_id)

# In case you want to authenticate with AAD application certificate.
filename = "path to a PEM certificate"
with open(filename, "r") as pem_file:
    PEM = pem_file.read()

thumbprint = "certificate's thumbprint"
kcsb = KustoConnectionStringBuilder.with_aad_application_certificate_authentication(cluster, client_id, PEM, thumbprint, authority_id)

# In case you want to authenticate with AAD application certificate Subject Name & Issuer
filename = "path to a PEM certificate"
with open(filename, "r") as pem_file:
    PEM = pem_file.read()

filename = "path to a public certificate"
with open(filename, "r") as cert_file:
    public_certificate = cert_file.read()

thumbprint = "certificate's thumbprint"
kcsb = KustoConnectionStringBuilder.with_aad_application_certificate_sni_authentication(cluster, client_id, PEM, public_certificate, thumbprint, authority_id)

# In case you want to authenticate with a System Assigned Managed Service Identity (MSI)
kcsb = KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster)

# In case you want to authenticate with a User Assigned Managed Service Identity (MSI)
user_assigned_client_id = "the AAD identity client id"
kcsb = KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster, client_id=user_assigned_client_id)

# In case you want to authenticate with AAD username and password
username = "<username>"
password = "<password>"
kcsb = KustoConnectionStringBuilder.with_aad_user_password_authentication(cluster, username, password, authority_id)

# In case you want to authenticate with AAD device code.
# Please note that if you choose this option, you'll need to autenticate for every new instance that is initialized.
# It is highly recommended to create one instance and use it for all of your queries.
kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(cluster)

# The authentication method will be taken from the chosen KustoConnectionStringBuilder.
client = QueuedIngestClient(kcsb)

# there are more options for authenticating - see azure-kusto-data samples

##################################################################
##                        INGESTION                             ##
##################################################################

# there are a lot of useful properties, make sure to go over docs and check them out
ingestion_props = IngestionProperties(
    database="{database_name}",
    table="{table_name}",
    data_format=DataFormat.CSV,
    # in case status update for success are also required
    # report_level=ReportLevel.FailuresAndSuccesses,
    # in case a mapping is required
    # ingestion_mapping_reference="{json_mapping_that_already_exists_on_table}"
    # ingestion_mapping_type=IngestionMappingType.JSON
)

# ingest from file
file_descriptor = FileDescriptor("{filename}.csv", 3333)  # 3333 is the raw size of the data in bytes.
client.ingest_from_file(file_descriptor, ingestion_properties=ingestion_props)
client.ingest_from_file("{filename}.csv", ingestion_properties=ingestion_props)


# ingest from blob
blob_descriptor = BlobDescriptor(
    "https://{path_to_blob}.csv.gz?sp=rl&st=2020-05-20T13:38:37Z&se=2020-05-21T13:38:37Z&sv=2019-10-10&sr=c&sig=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    10,
)  # 10 is the raw size of the data in bytes.
client.ingest_from_blob(blob_descriptor, ingestion_properties=ingestion_props)

# ingest from dataframe
import pandas

fields = ["id", "name", "value"]
rows = [[1, "abc", 15.3], [2, "cde", 99.9]]

df = pandas.DataFrame(data=rows, columns=fields)

client.ingest_from_dataframe(df, ingestion_properties=ingestion_props)

# ingest a whole folder.
import os

path = "folder/path"
[client.ingest_from_file(f, ingestion_properties=ingestion_props) for f in os.listdir(path)]

##################################################################
##                        INGESTION STATUS                      ##
##################################################################

# if status updates are required, something like this can be done
import pprint
import time
from azure.kusto.ingest.status import KustoIngestStatusQueues

qs = KustoIngestStatusQueues(client)

MAX_BACKOFF = 180

backoff = 1
while True:
    ################### NOTICE ####################
    # in order to get success status updates,
    # make sure ingestion properties set the
    # reportLevel=ReportLevel.FailuresAndSuccesses.
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
    with open("successes.log", "w+") as sf:
        for sm in success_messages:
            sf.write(str(sm))

    with open("failures.log", "w+") as ff:
        for fm in failure_messages:
            ff.write(str(fm))

##################################################################
##                        STREAMING INGEST                      ##
##################################################################

# Authenticate against this cluster endpoint as shows in the Auth section
cluster = "https://{cluster_name}.kusto.windows.net"

client = KustoStreamingIngestClient(kcsb)

ingestion_properties = IngestionProperties(database="{database_name}", table="{table_name}", data_format=DataFormat.CSV)

# ingest from file
file_descriptor = FileDescriptor("{filename}.csv", 3333)  # 3333 is the raw size of the data in bytes.
client.ingest_from_file(file_descriptor, ingestion_properties=ingestion_properties)
client.ingest_from_file("{filename}.csv", ingestion_properties=ingestion_properties)

# ingest from dataframe
import pandas

fields = ["id", "name", "value"]
rows = [[1, "abc", 15.3], [2, "cde", 99.9]]

df = pandas.DataFrame(data=rows, columns=fields)

client.ingest_from_dataframe(df, ingestion_properties=ingestion_properties)

# ingest from stream
byte_sequence = b"56,56,56"
bytes_stream = io.BytesIO(byte_sequence)
client.ingest_from_stream(bytes_stream, ingestion_properties=ingestion_properties)

stream_descriptor = StreamDescriptor(bytes_stream)
client.ingest_from_stream(stream_descriptor, ingestion_properties=ingestion_properties)

str_sequence = u"57,57,57"
str_stream = io.StringIO(str_sequence)
client.ingest_from_stream(str_stream, ingestion_properties=ingestion_properties)
