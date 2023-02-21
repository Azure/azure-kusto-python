# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
"""A simple example how to use KustoClient."""

from datetime import timedelta

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table

######################################################
##                        AUTH                      ##
######################################################

# Note that the 'help' cluster only allows interactive
# access by AAD users (and *not* AAD applications)
cluster = "https://help.kusto.windows.net"

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


# No authentication - for rare cases where the cluster is defined to work without any need for auth. usually reserved for internal use.

kcsb = KustoConnectionStringBuilder(cluster)

# Managed Identity - automatically injected into your machine by azure when running on an azure service.
# It's the best way for any code that does such - it's automatic, and requires no saving of secrets.

# In case you want to authenticate with a System Assigned Managed Service Identity (MSI)
kcsb = KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster)

# In case you want to authenticate with a User Assigned Managed Service Identity (MSI)
user_assigned_client_id = "the AAD identity client id"
kcsb = KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster, client_id=user_assigned_client_id)

# In case you want to authenticate with Azure CLI.
# Users are required to be in a logged in state in az-cli, for this authentication method to succeed. Run `az login` to log in to azure cli.

kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster)

# In case you want to authenticate with AAD username and password
username = "<username>"
password = "<password>"
kcsb = KustoConnectionStringBuilder.with_aad_user_password_authentication(cluster, username, password, authority_id)

# In case you want to authenticate with AAD device code.
# Please note that if you choose this option, you'll need to authenticate for every new instance that is initialized.
# It is highly recommended to create one instance and use it for all of your queries.
kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(cluster)

# The authentication method will be taken from the chosen KustoConnectionStringBuilder.
client = KustoClient(kcsb)

# Make sure to close the client when you're done with it.
# Either by using a context manager:
with KustoClient(kcsb) as client2:
    pass  # will be closed automatically at the end of the block

# Or by calling the close method explicitly:
client3 = KustoClient(kcsb)
client3.close()

######################################################
##                       QUERY                      ##
######################################################

# once authenticated, usage is as following
db = "Samples"
query = "StormEvents | take 10"

response = client.execute(db, query)

# iterating over rows is possible
for row in response.primary_results[0]:
    # printing specific columns by index
    print("value at 0 {}".format(row[0]))
    print("\n")
    # printing specific columns by name
    print("EventType:{}".format(row["EventType"]))

# tables are serializeable, so:
with open("results.json", "w+") as f:
    f.write(str(response.primary_results[0]))

# we also support dataframes:
dataframe = dataframe_from_result_table(response.primary_results[0])

print(dataframe)

# Streaming Query - rather than reading everything ahead, iterate through results as they come
multiple_tables = 'StormEvents | where EventType == "Heavy Rain" | take 10; StormEvents | where EventType == "Tornado" | take 10'

results = client.execute_streaming_query("DB", multiple_tables)
tables_iter = results.iter_primary_results()

first_table = next(tables_iter)
# Will block until each row arrives
for row in first_table:
    # printing specific columns by index
    print("value at 0 {}".format(row[0]))
    print("\n")
    # printing specific columns by name
    print("EventType:{}".format(row["EventType"]))

    # next(tables_iter) - throws, we can't read the next table until we exhausted this one

# Read next table
second_table = next(tables_iter)
print(next(second_table))

# You can always access the table's properties, even after it's exhausted
print(first_table.columns, first_table.table_kind, first_table.table_name)
print(second_table.columns, second_table.table_kind, second_table.table_name)

# Will skip forward, but the previous table will be exhausted
results.set_skip_incomplete_tables(True)
next(results, None)  # Tables are finished

# When we finish all the tables we get None
print(results.tables)  # Access all tables, not just the primary results

##################
### EXCEPTIONS ###
##################


# Query is too big to be executed
query = "StormEvents"
try:
    response = client.execute(db, query)
except KustoServiceError as error:
    print("2. Error:", error)
    print("2. Is semantic error:", error.is_semantic_error())
    print("2. Has partial results:", error.has_partial_results())
    print("2. Result size:", len(error.get_partial_results()))

properties = ClientRequestProperties()
properties.set_option(properties.results_defer_partial_query_failures_option_name, True)
properties.set_option(properties.request_timeout_option_name, timedelta(seconds=8 * 60))
response = client.execute(db, query, properties=properties)
print("3. Response error count: ", response.errors_count)
print("3. Exceptions:", response.get_exceptions())
print("3. Result size:", len(response.primary_results))

# Query has semantic error
query = "StormEvents | where foo = bar"
try:
    response = client.execute(db, query)
except KustoServiceError as error:
    print("4. Error:", error)
    print("4. Is semantic error:", error.is_semantic_error())
    print("4. Has partial results:", error.has_partial_results())

client = KustoClient("https://kustolab.kusto.windows.net")
response = client.execute("ML", ".show version")

query = """
let max_t = datetime(2016-09-03);
service_traffic
| make-series num=count() on TimeStamp in range(max_t-5d, max_t, 1h) by OsVer
"""
response = client.execute_query("ML", query).primary_results[0]
