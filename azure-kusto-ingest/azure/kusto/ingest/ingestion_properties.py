# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
from enum import Enum, IntEnum
from typing import List

from .exceptions import KustoMappingAndMappingReferenceError


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
    RAW = "raw"
    W3CLOGFILE = "w3clogfile"


class IngestionMappingType(Enum):
    CSV = "Csv"
    JSON = "Json"
    AVRO = "Avro"
    PARQUET = "Parquet"
    ORC = "Orc"
    W3CLOGFILE = "W3CLogFile"


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

    def __init__(self, validation_options=ValidationOptions.DoNotValidate, validation_implications=ValidationImplications.BestEffort):
        self.ValidationOptions = validation_options
        self.ValidationImplications = validation_implications


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
        column_name: str,
        column_type,
        path: str = None,
        transform: TransformationMethod = TransformationMethod.NONE,
        ordinal: int = None,
        const_value: str = None,
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


class IngestionProperties:
    """
    Class to represent ingestion properties.
    For more information check out https://docs.microsoft.com/en-us/azure/data-explorer/ingestion-properties
    """

    def __init__(
        self,
        database: str,
        table: str,
        data_format: DataFormat = DataFormat.CSV,
        ingestion_mapping: List[ColumnMapping] = None,
        ingestion_mapping_type: IngestionMappingType = None,
        ingestion_mapping_reference: str = None,
        ingest_if_not_exists: List[str] = None,
        ingest_by_tags: List[str] = None,
        drop_by_tags: List[str] = None,
        additional_tags: List[str] = None,
        flush_immediately: bool = False,
        report_level: ReportLevel = ReportLevel.DoNotReport,
        report_method: ReportMethod = ReportMethod.Queue,
        validation_policy: ValidationPolicy = None,
        additional_properties: dict = None,
    ):

        if ingestion_mapping is not None and ingestion_mapping_reference is not None:
            raise KustoMappingAndMappingReferenceError()

        self.database = database
        self.table = table
        self.format = data_format
        self.ingestion_mapping = ingestion_mapping
        self.ingestion_mapping_type = ingestion_mapping_type
        self.ingestion_mapping_reference = ingestion_mapping_reference
        self.additional_tags = additional_tags
        self.ingest_if_not_exists = ingest_if_not_exists
        self.ingest_by_tags = ingest_by_tags
        self.drop_by_tags = drop_by_tags
        self.flush_immediately = flush_immediately
        self.report_level = report_level
        self.report_method = report_method
        self.validation_policy = validation_policy
        self.additional_properties = additional_properties
