import pytest

from azure.kusto.data.data_format import IngestionMappingKind, DataFormat
from azure.kusto.ingest import IngestionProperties, ColumnMapping, TransformationMethod
from azure.kusto.ingest.exceptions import KustoDuplicateMappingError, KustoMissingMappingError, KustoMappingError


def test_duplicate_reference_and_column_mappings_raises():
    """Tests invalid ingestion properties."""
    with pytest.raises(KustoDuplicateMappingError):
        IngestionProperties(
            database="database", table="table", column_mappings=[ColumnMapping("test", "int")], ingestion_mapping_reference="ingestionMappingReference"
        )


def test_mapping_kind_without_mapping_raises():
    with pytest.raises(KustoMissingMappingError):
        IngestionProperties(database="database", table="table", ingestion_mapping_kind=IngestionMappingKind.CSV)


def test_mapping_kind_data_format_mismatch():
    with pytest.raises(KustoMappingError):
        IngestionProperties(
            database="database",
            table="table",
            ingestion_mapping_reference="ingestionMappingReference",
            data_format=DataFormat.JSON,
            ingestion_mapping_kind=IngestionMappingKind.CSV,
        )


def test_mapping_kind_data_format_invalid_no_name():
    with pytest.raises(KustoMappingError):
        IngestionProperties(
            database="database",
            table="table",
            column_mappings=[ColumnMapping("", "int")],
            data_format=DataFormat.JSON,
            ingestion_mapping_kind=IngestionMappingKind.JSON,
        )


def test_mapping_kind_data_format_invalid_no_path():
    with pytest.raises(KustoMappingError):
        IngestionProperties(
            database="database",
            table="table",
            column_mappings=[ColumnMapping("test", "int")],
            data_format=DataFormat.JSON,
            ingestion_mapping_kind=IngestionMappingKind.JSON,
        )


def test_mapping_kind_data_format_with_path():
    IngestionProperties(
        database="database",
        table="table",
        column_mappings=[ColumnMapping("test", "int", "path")],
        data_format=DataFormat.JSON,
        ingestion_mapping_kind=IngestionMappingKind.JSON,
    )


def test_mapping_kind_data_format_with_transform():
    IngestionProperties(
        database="database",
        table="table",
        column_mappings=[ColumnMapping("test", "int", transform=TransformationMethod.SOURCE_LINE_NUMBER)],
        data_format=DataFormat.JSON,
        ingestion_mapping_kind=IngestionMappingKind.JSON,
    )


def test_mapping_kind_data_format_with_no_properties():
    with pytest.raises(KustoMappingError):
        IngestionProperties(
            database="database",
            table="table",
            column_mappings=[ColumnMapping("test", "int")],
            data_format=DataFormat.AVRO,
            ingestion_mapping_kind=IngestionMappingKind.AVRO,
        )


def test_with_constant_value():
    IngestionProperties(
        database="database",
        table="table",
        column_mappings=[ColumnMapping("test", "int", const_value="1")],
        data_format=DataFormat.PARQUET,
        ingestion_mapping_kind=IngestionMappingKind.PARQUET,
    )
