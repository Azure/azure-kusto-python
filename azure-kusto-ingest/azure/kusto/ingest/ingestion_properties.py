"""
This file has all classes to define ingestion properties
"""

# review: your setup.py says you support Python 2.7, but the 'enum' module was
# not added to the stdlib until Python 3.4 and I don't see any backported version
# of it in your install_requires.
from enum import Enum, IntEnum
from .kusto_ingest_client_exceptions import KustoDuplicateMappingError

class DataFormat(Enum):
    """All data formats supported by Kusto."""

    csv = "csv"
    tsv = "tsv"
    log = "log"
    storageanalyticslogformat = "storageanalyticslogformat"
    scsv = "scsv"
    sohsv = "sohsv"
    json = "json"
    psv = "psv"
    avro = "avro"


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


# review: why the base class if there is no API commonality between, e.g.
# CSVColumnMapping and JSONColumnMapping?
class ColumnMapping(object):
    """ abstract class to column mapping """
    pass

class CSVColumnMapping(ColumnMapping):
    """ Class to represent a csv column mapping """
    def __init__(self, columnName, cslDataType, ordinal):
        self.Name = columnName
        self.DataType = cslDataType
        self.Ordinal = ordinal

class JSONColumnMapping(ColumnMapping):
    """ Class to represent a json column mapping """
    def __init__(self, column_name, json_path, csl_data_type=None):
        self.column = column_name
        self.path = json_path
        self.datatype = csl_data_type

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
                 validationPolicy=None):
        if mapping is not None and mapptingReference is not None:
            raise KustoDuplicateMappingError  # review: provide a message for the exception.
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
        """ Dictating the corresponding mapping to the format """
        if self.format == DataFormat.json or self.format == DataFormat.avro:
            return self.format.name
        else:
            return DataFormat.csv.name
