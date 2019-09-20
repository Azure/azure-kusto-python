"""This file has all classes to define ingestion properties."""

from enum import Enum, IntEnum
import warnings

from .exceptions import KustoDuplicateMappingError


class DataFormat(Enum):
    """All data formats supported by Kusto."""

    CSV = "csv"
    TSV = "tsv"
    SCSV = "scsv"
    SOHSV = "sohsv"
    PSV = "psv"
    TXT = "txt"
    JSON = "json"
    SINGLEJSON = "singlejson"
    AVRO = "avro"
    PARQUET = "parquet"
    MULTIJSON = "multijson"


class IngestionMappingType(Enum):
    CSV = "Csv"
    JSON = "Json"
    AVRO = "Avro"
    PARQUET = "Parquet"


class ValidationOptions(IntEnum):
    """Validation options to ingest command."""

    DoNotValidate = 0
    ValidateCsvInputConstantColumns = 1
    ValidateCsvInputColumnLevelOnly = 2


class ValidationImplications(IntEnum):
    """Validation implications to ingest command."""

    Fail = 0
    BestEffort = 1


class ValidationPolicy(object):
    """Validation policy to ingest command."""

    def __init__(
        self,
        validationOptions=ValidationOptions.DoNotValidate,
        validationImplications=ValidationImplications.BestEffort,
    ):
        self.ValidationOptions = validationOptions
        self.ValidationImplications = validationImplications


class ReportLevel(IntEnum):
    """Report level to ingest command."""

    FailuresOnly = 0
    DoNotReport = 1
    FailuresAndSuccesses = 2


class ReportMethod(IntEnum):
    """Report method to ingest command."""

    Queue = 0


class ColumnMapping:
    """Abstract class to column mapping."""

    pass


class CsvColumnMapping(ColumnMapping):
    """Class to represent a csv column mapping."""

    def __init__(self, columnName, cslDataType, ordinal):
        self.Name = columnName
        self.DataType = cslDataType
        self.Ordinal = ordinal

    def __str__(self):
        return "target: {0.Name} ,source: {0.Ordinal}, datatype: {0.DataType}".format(self)


class JsonColumnMapping(ColumnMapping):
    """ Class to represent a json column mapping """

    def __init__(self, columnName, jsonPath, cslDataType=None):
        self.column = columnName
        self.path = jsonPath
        self.datatype = cslDataType

    def __str__(self):
        return "target: {0.column} ,source: {0.path}, datatype: {0.datatype}".format(self)


class IngestionProperties:
    """Class to represent ingestion properties."""

    def __init__(
        self,
        database,
        table,
        dataFormat=DataFormat.CSV,
        mapping=None,
        mappingReference=None,
        ingestionMappingType=None,
        ingestionMappingReference=None,
        additionalTags=None,
        ingestIfNotExists=None,
        ingestByTags=None,
        dropByTags=None,
        flushImmediately=False,
        reportLevel=ReportLevel.DoNotReport,
        reportMethod=ReportMethod.Queue,
        validationPolicy=None,
        additionalProperties=None,
    ):
        # mapping_reference will be deprecated in the following versions
        if mappingReference is not None:
            warnings.warn(
                "mappingReference will be deprecated in the following versions."
                " Please use ingestionMappingReference instead",
                PendingDeprecationWarning,
            )
        if (mapping is not None and (mappingReference is not None or ingestionMappingReference is not None)) or (
            mappingReference is not None and ingestionMappingReference is not None
        ):
            raise KustoDuplicateMappingError()
        self.database = database
        self.table = table
        self.format = dataFormat
        self.mapping = mapping
        self.ingestion_mapping_type = ingestionMappingType
        self.ingestion_mapping_reference = (
            ingestionMappingReference if ingestionMappingReference is not None else mappingReference
        )
        self.additional_tags = additionalTags
        self.ingest_if_not_exists = ingestIfNotExists
        self.ingest_by_tags = ingestByTags
        self.drop_by_tags = dropByTags
        self.flush_immediately = flushImmediately
        self.report_level = reportLevel
        self.report_method = reportMethod
        self.validation_policy = validationPolicy
        self.additional_properties = additionalProperties

    def get_mapping_format(self):
        """Dictating the corresponding mapping to the format."""
        if self.format in [DataFormat.JSON, DataFormat.SINGLEJSON, DataFormat.MULTIJSON]:
            return DataFormat.JSON.name
        elif self.format == DataFormat.AVRO:
            return DataFormat.AVRO.name
        else:
            return DataFormat.CSV.name
