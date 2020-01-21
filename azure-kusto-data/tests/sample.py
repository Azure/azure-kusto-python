"""A simple example how to use KustoClient."""

from datetime import timedelta
from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties
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
