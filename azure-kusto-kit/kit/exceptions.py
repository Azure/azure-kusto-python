class DataConflictError(Exception):
    pass


class SchemaConflictError(Exception):
    pass


class DatabaseMissingError(SchemaConflictError):
    pass


class TableMissingError(SchemaConflictError):
    pass


class ColumnMissingError(SchemaConflictError):
    pass


class DatabaseConflictError(SchemaConflictError):
    pass


class TableConflictError(SchemaConflictError):
    pass


class ColumnConflictError(SchemaConflictError):
    pass


class DatabaseDoesNotExist(Exception):
    pass
