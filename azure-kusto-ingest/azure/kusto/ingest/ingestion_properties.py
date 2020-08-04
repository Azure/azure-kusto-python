# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
from enum import Enum, IntEnum
from typing import List

from .exceptions import KustoDuplicateMappingReferenceError, KustoMappingAndMappingReferenceError


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
        **kwargs
    ):

        # backward compat - will be removed in future versions
        dataFormat = kwargs.get("dataFormat", None)
        ingestionMapping = kwargs.get("ingestionMapping", None)
        ingestionMappingType = kwargs.get("ingestionMappingType", None)
        ingestionMappingReference = kwargs.get("ingestionMappingReference", None)
        additionalTags = kwargs.get("additionalTags", None)
        ingestIfNotExists = kwargs.get("ingestIfNotExists", None)
        ingestByTags = kwargs.get("ingestByTags", None)
        dropByTags = kwargs.get("dropByTags", None)
        flushImmediately = kwargs.get("flushImmediately", None)
        reportLevel = kwargs.get("reportLevel", None)
        reportMethod = kwargs.get("reportMethod", None)
        validationPolicy = kwargs.get("validationPolicy", None)
        additionalProperties = kwargs.get("additionalProperties", None)

        mapping_exists = ingestionMapping is not None or ingestion_mapping is not None
        if mapping_exists and (ingestion_mapping_reference is not None or ingestionMappingReference is not None):
            raise KustoMappingAndMappingReferenceError()

        if ingestion_mapping_reference is not None and ingestionMappingReference is not None:
            raise KustoDuplicateMappingReferenceError()

        self.database = database
        self.table = table
        self.format = dataFormat or data_format
        self.ingestion_mapping = ingestionMapping or ingestion_mapping
        self.ingestion_mapping_type = ingestionMappingType or ingestion_mapping_type
        self.ingestion_mapping_reference = ingestionMappingReference or ingestion_mapping_reference
        self.additional_tags = additionalTags or additional_tags
        self.ingest_if_not_exists = ingestIfNotExists or ingest_if_not_exists
        self.ingest_by_tags = ingestByTags or ingest_by_tags
        self.drop_by_tags = dropByTags or drop_by_tags
        self.flush_immediately = flushImmediately or flush_immediately
        self.report_level = reportLevel or report_level
        self.report_method = reportMethod or report_method
        self.validation_policy = validationPolicy or validation_policy
        self.additional_properties = additionalProperties or additional_properties
