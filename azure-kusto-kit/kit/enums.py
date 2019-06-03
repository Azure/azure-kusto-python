from enum import Enum

class DataConflictMode(Enum):
    Safe = 'safe'
    Extend = 'extend'
    Merge = 'merge'


class SchemaConflictMode(Enum):
    Safe = 'safe'
    Append = 'append'
    Merge = 'merge'
    Replace = 'replace'
