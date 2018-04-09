"""
This file has all classes to define ingestion properties
"""

from enum import Enum, IntEnum
from .kusto_ingest_client_exceptions import KustoDuplicateMappingError

class DataFormat(Enum):
    """ All data formats supported by Kusto """
    csv = "csv"
    tsv = "tsv"
    log = "log"
    storageanalyticslogformat = "storageanalyticslogformat"
    scsv = "scsv"
    sohsv = "sohsv"
    json = "json"
    psv = "psv"

class ValidationOptions(IntEnum):
    """ Validation options to ingest command """
    DoNotValidate = 0
    ValidateCsvInputConstantColumns = 1
    ValidateCsvInputColumnLevelOnly = 2

class ValidationImplications(IntEnum):
    """ Validation implications to ingest command """
    Fail = 0
    BestEffort = 1

class ValidationPolicy():
    """ Validation policy to ingest command """
    def __init__(self,
                 validationOptions=ValidationOptions.DoNotValidate,
                 validationImplications=ValidationImplications.BestEffort):
        self.ValidationOptions = validationOptions
        self.ValidationImplications = validationImplications

class ReportLevel(Enum):
    """ Report level to ingest command """
    FailuresOnly = 0
    DoNotReport = 1
    FailuresAndSuccesses = 2

class ReportMethod(Enum):
    """ Report method to ingest command """
    Queue = 0
    Table = 1
    QueueAndTable = 2

class ColumnMapping():
    """ abstract class to column mapping """
    pass

class CsvColumnMapping(ColumnMapping):
    """ Class to represent a csv column mapping """
    def __init__(self, columnName, cslDataType, ordinal):
        self.Name = columnName
        self.DataType = cslDataType
        self.Ordinal = ordinal

class JsonColumnMapping(ColumnMapping):
    """ Class to represent a json column mapping """
    def __init__(self, columnName, jsonPath, cslDataType=None):
        self.column = columnName
        self.path = jsonPath
        self.datatype = cslDataType

class IngestionProperties:
    """ Class to represent ingestion properties """
    def __init__(self, database, table,
                 dataFormat=DataFormat.csv,
                 mapping=None,
                 mapptingReference=None,
                 additionalTags=None,
                 ingestIfNotExists=None,
                 ingestByTags=None,
                 dropByTags=None,
                 flushImmediately=False,
                 reportLevel=ReportLevel.DoNotReport,
                 reportMethod=ReportMethod.Queue,
                 validationPolicy=None
                ):
        if mapping is not None and mapptingReference is not None:
            raise KustoDuplicateMappingError()
        self.database = database
        self.table = table
        self.format = dataFormat
        self.mapping = mapping
        self.mapping_reference = mapptingReference
        self.additional_tags = additionalTags
        self.ingest_if_not_exists = ingestIfNotExists
        self.ingest_by_tags = ingestByTags
        self.drop_by_tags = dropByTags
        self.flush_immediately = flushImmediately
        self.report_level = reportLevel
        self.report_method = reportMethod
        self.validation_policy = validationPolicy

    def get_mapping_format(self):
        """ Dictating the corresponding mapping to the format name """
        if self.format == DataFormat.json:
            return "json"
        else:
            return "csv"