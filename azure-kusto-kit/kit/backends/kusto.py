import logging
from urllib.parse import urlparse

from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder
from azure.kusto.ingest import IngestionProperties, KustoIngestClient, CsvColumnMapping, JsonColumnMapping, DataFormat, ReportMethod, ReportLevel

from kit.dtypes import dotnet_to_kusto_type
from kit.enums import SchemaConflictMode
from kit.exceptions import DatabaseDoesNotExist, DatabaseConflictError
from kit.models.database import Database, Table, Column

logger = logging.getLogger('kit')

LIST_COLUMNS_BY_TABLE = ".show database {database_name} schema | where TableName != '' and ColumnName != '' | summarize Columns=make_set(strcat(ColumnName,':',ColumnType)) by TableName"
CREATE_INGESTION_MAPPING = """.create table {table} ingestion {mapping_type} "{mapping_name}" '[{mappings}]'"""

_mapping_defs = {
    'csv': {
        'name': 'csv mapping',
        'mappingRefKey': 'csvMappingReference',
        'column': {
            'target_column_key': 'Name',
            'data_type_key': 'DataType',
            'path_key': 'Ordinal'
        },
        'formats': ['csv', 'tsv']
    },
    'json': {
        'name': 'json mapping',
        'mappingRefKey': 'jsonMappingReference',
        'column': {
            'target_column_key': 'Column',
            'data_type_key': 'DataType',
            'path_key': 'Path'
        },
        'format': ['json']
    }
}


class KustoClientProvider:
    def __init__(self, cluster, auth):
        self.target_engine = KustoClientProvider.resolve_engine_uri(cluster)
        self.target_dm = KustoClientProvider.resolve_ingest_uri(self.target_engine)
        self.auth = auth

    @staticmethod
    def resolve_engine_uri(cluster):
        uri = urlparse(cluster)

        scheme = uri.scheme if uri.scheme else 'https'
        # short style : 'cluster.region'
        if not uri.netloc and uri.path:
            return f'{scheme}://{uri.path}.kusto.windows.net'

        return uri.geturl()

    @staticmethod
    def resolve_ingest_uri(engine_uri):

        return '{0.scheme}://ingest-{0.netloc}'.format(urlparse(engine_uri))

    def resolve_kcsb(self, uri):
        if 'user_token' in self.auth:
            kcsb_f = KustoConnectionStringBuilder.with_aad_user_token_authentication
        if 'aad_app_id' in self.auth:
            kcsb_f = KustoConnectionStringBuilder.with_aad_application_key_authentication
        if 'user_id' in self.auth:
            kcsb_f = KustoConnectionStringBuilder.with_aad_user_password_authentication

        return kcsb_f(uri, **self.auth)

    def get_engine_client(self):
        return KustoClient(self.resolve_kcsb(self.target_engine))

    def get_dm_client(self):
        return KustoClient(self.resolve_kcsb(self.target_dm))

    def get_ingest_client(self):
        return KustoIngestClient(self.resolve_kcsb(self.target_dm))


class KustoBackend:
    def __init__(self, cluster: str, auth: dict, database: str = None):
        self.client_provider = KustoClientProvider(cluster, auth)

    def describe_database(self, database_name: str, **kwargs) -> Database:

        tables = []
        client = self.client_provider.get_engine_client()
        try:
            tables_result = client.execute('NetDefault', LIST_COLUMNS_BY_TABLE.format(database_name=database_name)).primary_results[0]

            for t in tables_result:
                columns = []
                for index, col in enumerate(t['Columns']):
                    name, dotnet_type = col.split(':')
                    columns.append(Column(name, index=index, data_type=dotnet_to_kusto_type(dotnet_type)))

                tables.append(Table(t['TableName'], columns))
        except KustoServiceError as e:
            if e.http_response.json()['error']['@type'] == 'Kusto.Data.Exceptions.EntityNotFoundException':
                raise DatabaseDoesNotExist(database_name)

        return Database(database_name, tables)

    def enforce_schema(self, database_schema: Database, schema_conflict: SchemaConflictMode = SchemaConflictMode.Append):
        client = self.client_provider.get_engine_client()

        try:
            actual = self.database_from_kusto(database=database_schema.name)

            for expected_table in database_schema.tables:
                if expected_table.name not in actual.tables_dict:
                    client.execute(actual.name, expected_table.kql_create_command())

        except DatabaseDoesNotExist:
            if schema_conflict == SchemaConflictMode.Safe:
                raise DatabaseConflictError(
                    f'SAFE MODE: Expected a Database named "{database_schema.name}" on {self.client_provider.target_engine} but none found')
            if schema_conflict == SchemaConflictMode.Append:
                # by default, if we can't find a database on the cluster, we will create it
                pass

    def create_table(self, table: Table, database_name: str):
        client = self.client_provider.get_engine_client()

        command_format = '.create table {} ({})'
        column_definitions = []
        for col in table.columns:
            column_definitions.append(col.column_definition)

        command = command_format.format(table.name, ','.join(column_definitions))

        client.execute(database_name, command)

    def ping(self):
        self.client_provider.get_engine_client().execute('NetDefault', '.show diagnostics')
        self.client_provider.get_dm_client().execute('NetDefault', '.show diagnostics')

    def ingest_from_source(self, source, mapping, target_database, target_table, **kwargs):
        files = source.files
        ingest_client = self.client_provider.get_ingest_client()
        kusto_ingest_mapping = []

        if source.data_format == 'csv':
            # TODO: need to add __str__ to columnMapping
            mapping_func = lambda source_col, target_col: CsvColumnMapping(target_col.name, target_col.data_type.value, source_col.index)
        if source.data_format == 'json':
            # TODO: need to add __str__ to columnMapping
            mapping_func = lambda source_col, target_col: JsonColumnMapping(target_col.name, f'$.{source_col.name}',
                                                                            cslDataType=target_col.data_type.value)

        for col in mapping.columns:
            kusto_ingest_mapping.append(mapping_func(col.source, col.target))
        # TODO: should maybe persist ingestion mappings
        ingestion_props = IngestionProperties(target_database,
                                              target_table,
                                              dataFormat=DataFormat(source.data_format),
                                              mapping=kusto_ingest_mapping,
                                              reportLevel=ReportLevel.FailuresOnly,
                                              reportMethod=ReportMethod.Queue)

        if 'batch_id' in kwargs and not kwargs.get('no_wait', False):
            # this helps with monitoring
            ingestion_props.ingest_by_tags = [kwargs['batch_id']]
        for file_path in files:
            if kwargs.get('dry', False):
                logger.info(f'DRY: {file_path} would have been ingested into {target_database}.{target_table} using: \n {kusto_ingest_mapping}')
            else:
                if kwargs.get('queued', True):
                    logger.info(f'Queueing "{file_path}" to ingest into "{ingestion_props.table}"')
                    ingest_client.ingest_from_file(file_path, ingestion_props)
                else:
                    # TODO: allow for inline ingestion (this is currently only relevant to files already in storage)
                    # client.execute(f'.ingest into table {operation.target} ({}) with ({mapping_ref_key}="{mapping_name}")')
                    pass

    def count_rows_with_tag(self, database, table, tag) -> int:
        client = self.client_provider.get_engine_client()

        result = client.execute(database, f"['{table}'] | where extent_tags() has 'ingest-by:{tag}' | count")

        return result.primary_results[0][0][0]

    def get_ingestion_errors(self, database, tag):
        client = self.client_provider.get_engine_client()

        result = client.execute(
            "NetDefault",
            f".show ingestion failures | where FailedOn > ago(1h) and Database == '{database}' and IngestionProperties contains ('{tag}')")

        return result.primary_results[0]
