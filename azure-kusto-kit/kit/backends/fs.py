import os
from pathlib import Path

from kit.dtypes import infer
from kit.enums import DataConflictMode
from kit.models.database import Database, Table


def table_from_path(path, name=None, conflict_mode=DataConflictMode.Safe, top=200) -> Table:
    name = name or Path(path).with_suffix('').stem
    if os.path.isdir(path):
        return table_from_folder(path, name, conflict_mode, top)

    return table_from_file(path, name, top)


def table_from_file(filepath, name=None, top=200) -> Table:
    with open(filepath, "r", encoding='utf8') as f:
        columns = infer.columns_from_stream(f, includes_headers=False, limit=top)

    return Table(name, columns)


def table_from_folder(path, name=None, conflict_mode=DataConflictMode.Safe, top=200) -> Table:
    inferred_table = Table(name, columns=[])
    for file in os.listdir(path):
        current_table = table_from_file(file, top)
        if conflict_mode == DataConflictMode.Safe:
            inferred_table.assert_eq(current_table)

    return inferred_table


def database_from_folder(path: str, conflict_mode: DataConflictMode = DataConflictMode.Safe) -> Database:
    db_name = os.path.basename(path)
    tables = os.listdir(path)

    inferred_tables = []
    for t in tables:
        table_path = os.path.join(path, t)
        inferred_tables.append(table_from_path(table_path, conflict_mode=conflict_mode))

    return Database(db_name, inferred_tables)
