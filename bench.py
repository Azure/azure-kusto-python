from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

kcsb = KustoConnectionStringBuilder.with_az_cli_authentication('https://kuskus.kusto.windows.net')
client = KustoClient(kcsb)

result = client.execute('Kuskus', 'print "hello world"')

# print(result.primary_results[0])

result = client.execute(
    'Kuskus',
    """set truncationmaxsize=104857600;
set truncationmaxrecords=500000;
KustoLogs | where Timestamp > ago(10m) | top 500000 by Timestamp
"""
)
