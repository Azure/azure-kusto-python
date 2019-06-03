import os
from urllib.parse import urlparse

from kit import sql
from kit.models.database import Database


def database_from_sql(sql_path, **kwargs) -> Database:
    uri = urlparse(sql_path)
    if uri.scheme.startswith('http'):
        raise NotImplementedError('Currently only supports Local FS. \nIt is planned to resolve the correct storage provider with credentials')
    else:
        with open(sql_path) as f:
            lines = f.readlines()

    suggested_name = os.path.basename(sql_path)

    sql_text = ''.join(lines).replace('\n', ' ')
    tables = sql.table_from_statement(sql_text)

    return Database(suggested_name, tables)
