"""
A simple example how to use KustoClient
"""

from azure.kusto.data import KustoClient, KustoServiceError

# TODO: this should become functional test at some point.

KUSTO_CLUSTER = 'https://help.kusto.windows.net'

# In case you want to authenticate with AAD application.
CLIENT_ID = '<insert here your AAD application id>'
CLIENT_SECRET = '<insert here your AAD application key>'
KUSTO_CLIENT = KustoClient(kusto_cluster=KUSTO_CLUSTER,
                           client_id=CLIENT_ID,
                           client_secret=CLIENT_SECRET
                          )

# In case you want to authenticate with the logged in AAD user.
KUSTO_CLIENT = KustoClient(kusto_cluster=KUSTO_CLUSTER)

KUSTO_DATABASE = 'Samples'
KUSTO_QUERY = 'StormEvents | take 10'

RESPONSE = KUSTO_CLIENT.execute(KUSTO_DATABASE, KUSTO_QUERY)
for row in RESPONSE.iter_all():
    print(row[0], ' ', row["EventType"])

# Query is too big to be executed
KUSTO_QUERY = 'StormEvents'
try:
    RESPONSE = KUSTO_CLIENT.execute(KUSTO_DATABASE, KUSTO_QUERY)
except KustoServiceError as error:
    print('2. Error:', error)
    print('2. Is semantic error:', error.is_semantic_error())
    print('2. Has partial results:', error.has_partial_results())
    print('2. Result size:', len(list(error.get_partial_results().iter_all())))

RESPONSE = KUSTO_CLIENT.execute(KUSTO_DATABASE, KUSTO_QUERY, accept_partial_results=True)
print('3. Response has exception:', RESPONSE.has_exceptions())
print('3. Exceptions:', RESPONSE.get_exceptions())
print('3. Result size:', len(list(RESPONSE.iter_all())))

# Query has semantic error
KUSTO_QUERY = 'StormEvents | where foo = bar'
try:
    RESPONSE = KUSTO_CLIENT.execute(KUSTO_DATABASE, KUSTO_QUERY)
except KustoServiceError as error:
    print('4. Error:', error)
    print('4. Is semantic error:', error.is_semantic_error())
    print('4. Has partial results:', error.has_partial_results())

# Testing data frames
KUSTO_CLIENT = KustoClient('https://kustolab.kusto.windows.net')
RESPONSE = KUSTO_CLIENT.execute("ML", ".show version")
QUERY = '''
let max_t = datetime(2016-09-03);
service_traffic
| make-series num=count() on TimeStamp in range(max_t-5d, max_t, 1h) by OsVer
'''
DATA_FRAME = KUSTO_CLIENT.execute_query("ML", QUERY).to_dataframe()
