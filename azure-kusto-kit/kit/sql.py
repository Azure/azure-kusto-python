import logging
from typing import List

import sqlparse

from kit.dtypes import KustoType
from kit.models.database import Table, Column

logger = logging.getLogger('kit')


# TODO: this should be dialect specific (currently assuming postgres)
# https://www.postgresql.org/docs/9.5/datatype.html
def sql_type_to_kusto_type(sql_type_str: str) -> KustoType:
    sql_type_normal = sql_type_str.strip().lower()
    # array dtypes are dynamic
    if '[' in sql_type_normal:
        return KustoType.DYNAMIC
    if sql_type_normal in ['character', 'varchar', 'text', 'char', 'tinytext', 'longtext', 'mediumtext']:
        return KustoType.STRING
    if sql_type_normal in ['smallint', 'int', 'bigint', 'integer', 'serial', 'bigserial']:
        return KustoType.INT
    if sql_type_normal in ['float', 'real', 'numeric']:
        return KustoType.DECIMAL
    if sql_type_normal in ['bool', 'boolean']:
        return KustoType.BOOL
    if sql_type_normal in ['date', 'datetime', 'timestamp', 'timestamptz']:
        return KustoType.DATETIME
    if sql_type_normal in ['interval']:
        return KustoType.TIMESPAN
    if sql_type_normal == 'uuid':
        return KustoType.GUID
    if sql_type_normal == 'json':
        return KustoType.DYNAMIC


def table_from_statement(sql_text) -> List[Table]:
    sql_commands = sqlparse.parse(sql_text)

    tables = []
    for sql_command in sql_commands:
        if sql_command.get_type().lower() == 'create':
            tables.append(table_from_create_command(sql_command))
        else:
            raise NotImplementedError('Currently only supports create commands')

    return tables


def table_from_create_command(sql_statement: sqlparse.sql.Statement) -> Table:
    columns_part = [a for a in sql_statement.get_sublists()][1].normalized
    columns = []
    for index, col in enumerate(columns_part[1:-1].split(',')):
        col_name, col_type, *col_modifires = col.strip().split(' ')
        kusto_type = sql_type_to_kusto_type(col_type)
        columns.append(Column(index=index, name=col_name, data_type=kusto_type))

    return Table(sql_statement.get_name(), columns)

#
# # TODO: should probably do this based on actual backends and not manifest, this is a temporary hack
# engine = KustoClient(KustoConnectionStringBuilder.with_aad_device_authentication(manifest.engine_connection_string))
# for db in db_tables.keys():
#     tables_with_columns = \
#         engine.execute(db,
#                        ".show database {} backends | where isnotempty(ColumnName) | summarize make_set(ColumnName) by TableName".format(db)).primary_results[
#             0]
#     for table_record in tables_with_columns:
#         table = table_record[0]
#         columns = table_record[1]
#         for col_index, col_name in enumerate(columns):
#             if col_name != expected_tables_columns[table][col_index]:
#                 rename_command = '.rename column ["{table}"].["{old}"] to ["{new}"]'.format(table=table, old=col_name,
#                                                                                             new=expected_tables_columns[table][col_index])
#                 engine.execute(db, rename_command)
#
# print(sqlparse.format('select * from foo', reindent=True))
