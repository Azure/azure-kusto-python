# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
from enum import Enum, IntEnum
from typing import List, Optional

from azure.kusto.data.data_format import DataFormat, IngestionMappingKind
from .exceptions import KustoDuplicateMappingError, KustoMissingMappingError, KustoMappingError


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
        column_type: str,
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

    def is_valid(self, kind: IngestionMappingKind) -> bool:
        if not self.column:
            return False

        if kind in (IngestionMappingKind.JSON, IngestionMappingKind.PARQUET, IngestionMappingKind.ORC, IngestionMappingKind.W3CLOGFILE):
            return (
                bool(self.properties.get(self.PATH))
                or self.properties.get(self.TRANSFORMATION_METHOD) == TransformationMethod.SOURCE_LINE_NUMBER.value
                or self.properties.get(self.TRANSFORMATION_METHOD) == TransformationMethod.SOURCE_LOCATION.value
            )

        if kind in (IngestionMappingKind.AVRO, IngestionMappingKind.APACHEAVRO):
            return bool(self.properties.get(self.COLUMNS))

        return True


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
        column_mappings: Optional[List[ColumnMapping]] = None,
        ingestion_mapping_kind: Optional[IngestionMappingKind] = None,
        ingestion_mapping_reference: Optional[str] = None,
        ingest_if_not_exists: Optional[List[str]] = None,
        ingest_by_tags: Optional[List[str]] = None,
        drop_by_tags: Optional[List[str]] = None,
        additional_tags: Optional[List[str]] = None,
        flush_immediately: bool = False,
        report_level: ReportLevel = ReportLevel.DoNotReport,
        report_method: ReportMethod = ReportMethod.Queue,
        validation_policy: Optional[ValidationPolicy] = None,
        additional_properties: Optional[dict] = None,
    ):

        if ingestion_mapping_reference is None and column_mappings is None:
            if data_format._mapping_required:
                raise KustoMissingMappingError(f"When stream format is '{data_format.kusto_value}', a mapping must be provided.")
            if ingestion_mapping_kind is not None:
                raise KustoMissingMappingError(f"When ingestion mapping kind is set ('{ingestion_mapping_kind.value}'), a mapping must be provided.")
        else:  # A mapping is provided
            if ingestion_mapping_kind is not None:
                if data_format.ingestion_mapping_kind != ingestion_mapping_kind:
                    raise KustoMappingError(
                        f"Wrong ingestion mapping for format '{data_format.kusto_value}'; mapping kind should be '{data_format.ingestion_mapping_kind.value}', "
                        f"but was '{ingestion_mapping_kind.value}'. "
                    )
            else:
                ingestion_mapping_kind = data_format.ingestion_mapping_kind

            if column_mappings is not None:
                if ingestion_mapping_reference is not None:
                    raise KustoDuplicateMappingError()

                validation_errors = [
                    f"Column mapping '{mapping.column}' is invalid." for mapping in column_mappings if not mapping.is_valid(ingestion_mapping_kind)
                ]

                if validation_errors:
                    errors = "\n".join(validation_errors)
                    raise KustoMappingError(f"Failed with validation errors:\n{errors}")

        self.database = database
        self.table = table
        self.format = data_format
        self.ingestion_mapping = column_mappings
        self.ingestion_mapping_type = ingestion_mapping_kind
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
