"""This file has all classes to define ingestion properties."""

import warnings
from enum import Enum, IntEnum

from .exceptions import KustoDuplicateMappingError, KustoDuplicateMappingReferenceError, KustoMappingAndMappingReferenceError


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
    ORC = "orc"
    TSVE = "tsve"


class IngestionMappingType(Enum):
    CSV = "Csv"
    JSON = "Json"
    AVRO = "Avro"
    PARQUET = "Parquet"
    ORC = "Orc"


class ValidationOptions(IntEnum):
    """Validation options to ingest command."""

    DoNotValidate = 0
    ValidateCsvInputConstantColumns = 1
    ValidateCsvInputColumnLevelOnly = 2


class ValidationImplications(IntEnum):
    """Validation implications to ingest command."""

    Fail = 0
    BestEffort = 1


class ValidationPolicy:
    """Validation policy to ingest command."""

    def __init__(self, validationOptions=ValidationOptions.DoNotValidate, validationImplications=ValidationImplications.BestEffort):
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


class TransformationMethod(Enum):
    """Transformations to configure over json column mapping
    To read more about mapping transformations look here: https://docs.microsoft.com/en-us/azure/kusto/management/mappings#mapping-transformations"""

    NONE = "None"
    PROPERTY_BAG_ARRAY_TO_DICTIONARY = ("PropertyBagArrayToDictionary",)
    SOURCE_LOCATION = "SourceLocation"
    SOURCE_LINE_NUMBER = "SourceLineNumber"
    GET_PATH_ELEMENT = "GetPathElement"
    UNKNOWN_ERROR = "UnknownMethod"
    DATE_TIME_FROM_UNIX_SECONDS = "DateTimeFromUnixSeconds"
    DATE_TIME_FROM_UNIX_MILLISECONDS = "DateTimeFromUnixMilliseconds"
    DATE_TIME_FROM_UNIX_MICROSECONDS = "DateTimeFromUnixMicroseconds"
    DATE_TIME_FROM_UNIX_NANOSECONDS = "DateTimeFromUnixNanoseconds"


class ColumnMapping:
    """Use this class to create mappings for IngestionProperties.ingestionMappings and utilize mappings that were not
    pre-created (it is recommended to create the mappings in advance and use ingestionMappingReference).
    To read more about mappings look here: https://docs.microsoft.com/en-us/azure/kusto/management/mappings"""

    # Json Mapping consts
    PATH = "Path"
    TRANSFORMATION_METHOD = "Transform"
    # csv Mapping consts
    ORDINAL = "Ordinal"
    CONST_VALUE = "ConstValue"
    # Avro Mapping consts
    FIELD_NAME = "Field"
    COLUMNS = "Columns"
    # General Mapping consts
    STORAGE_DATA_TYPE = "StorageDataType"

    def __init__(
        self,
        column_name,
        column_type,
        path=None,
        transform=TransformationMethod.NONE,
        ordinal=None,
        const_value=None,
        field=None,
        columns=None,
        storage_data_type=None,
    ):
        self.column = column_name
        self.datatype = column_type
        self.properties = {}
        if path:
            self.properties[self.PATH] = path
        if transform != TransformationMethod.NONE:
            self.properties[self.TRANSFORMATION_METHOD] = transform.value
        if ordinal is not None:
            self.properties[self.ORDINAL] = str(ordinal)
        if const_value:
            self.properties[self.CONST_VALUE] = const_value
        if field:
            self.properties[self.FIELD_NAME] = field
        if columns:
            self.properties[self.COLUMNS] = columns
        if storage_data_type:
            self.properties[self.STORAGE_DATA_TYPE] = storage_data_type


class ColumnMappingBase:
    """Deprecated abstract base mapping class"""

    pass


class CsvColumnMapping(ColumnMappingBase):
    """Class to represent a csv column mapping."""

    @DeprecationWarning
    def __init__(self, columnName, cslDataType, ordinal):
        self.Name = columnName
        self.DataType = cslDataType
        self.Ordinal = ordinal

    def __str__(self):
        return "target: {0.Name} ,source: {0.Ordinal}, datatype: {0.DataType}".format(self)


class JsonColumnMapping(ColumnMappingBase):
    """ Class to represent a json column mapping """

    @DeprecationWarning
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
        ingestionMapping=None,
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
        # mapping_reference will be deprecated in the next major version
        if mappingReference is not None:
            warnings.warn(
                """
                mappingReference will be deprecated in the next major version.
                Please use ingestionMappingReference instead
                """,
                PendingDeprecationWarning,
            )

        # mapping will be deprecated in the next major version
        if mapping is not None:
            warnings.warn(
                """
                mapping will be deprecated in the next major version.
                Please use ingestionMapping instead
                """,
                PendingDeprecationWarning,
            )

        if mapping is not None and ingestionMapping is not None:
            raise KustoDuplicateMappingError()

        mapping_exists = mapping is not None or ingestionMapping is not None
        if mapping_exists and (mappingReference is not None or ingestionMappingReference is not None):
            raise KustoMappingAndMappingReferenceError()

        if mappingReference is not None and ingestionMappingReference is not None:
            raise KustoDuplicateMappingReferenceError()

        self.database = database
        self.table = table
        self.format = dataFormat
        self.ingestion_mapping = ingestionMapping if ingestionMapping is not None else mapping
        self.ingestion_mapping_type = ingestionMappingType
        self.ingestion_mapping_reference = ingestionMappingReference if ingestionMappingReference is not None else mappingReference
        self.additional_tags = additionalTags
        self.ingest_if_not_exists = ingestIfNotExists
        self.ingest_by_tags = ingestByTags
        self.drop_by_tags = dropByTags
        self.flush_immediately = flushImmediately
        self.report_level = reportLevel
        self.report_method = reportMethod
        self.validation_policy = validationPolicy
        self.additional_properties = additionalProperties
