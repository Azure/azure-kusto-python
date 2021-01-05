# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import unittest
import re
import json
from uuid import UUID

from azure.kusto.ingest._ingestion_blob_info import _IngestionBlobInfo
from azure.kusto.ingest.exceptions import KustoMappingAndMappingReferenceError
from azure.kusto.ingest import (
    BlobDescriptor,
    IngestionProperties,
    DataFormat,
    ColumnMapping,
    ReportLevel,
    ReportMethod,
    ValidationPolicy,
    ValidationOptions,
    ValidationImplications,
)

TIMESTAMP_REGEX = "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{6}"


class IngestionBlobInfoTest(unittest.TestCase):
    """Tests serialization of ingestion blob info. This serialization will be queued to the DM."""

    def test_blob_info_csv_mapping(self):
        """Tests serialization of csv ingestion blob info."""
        validation_policy = ValidationPolicy(ValidationOptions.ValidateCsvInputConstantColumns, ValidationImplications.BestEffort)
        column_mapping = ColumnMapping("ColumnName", "cslDataType", ordinal=1)

        properties = IngestionProperties(
            database="database",
            table="table",
            data_format=DataFormat.CSV,
            ingestion_mapping=[column_mapping],
            additional_tags=["tag"],
            ingest_if_not_exists=["ingestIfNotExistTags"],
            ingest_by_tags=["ingestByTags"],
            drop_by_tags=["dropByTags"],
            flush_immediately=True,
            report_level=ReportLevel.DoNotReport,
            report_method=ReportMethod.Queue,
            validation_policy=validation_policy,
        )
        blob = BlobDescriptor("somepath", 10)
        blob_info = _IngestionBlobInfo(blob, properties, auth_context="authorizationContextText")
        self._verify_ingestion_blob_info_result(blob_info.to_json())

    def test_blob_csv_mapping_reference(self):
        """Tests serialization of ingestion blob info with csv mapping reference."""
        validation_policy = ValidationPolicy(ValidationOptions.ValidateCsvInputConstantColumns, ValidationImplications.BestEffort)
        properties = IngestionProperties(
            database="database",
            table="table",
            data_format=DataFormat.CSV,
            ingestion_mapping_reference="csvMappingReference",
            additional_tags=["tag"],
            ingest_if_not_exists=["ingestIfNotExistTags"],
            ingest_by_tags=["ingestByTags"],
            drop_by_tags=["dropByTags"],
            flush_immediately=True,
            report_level=ReportLevel.DoNotReport,
            report_method=ReportMethod.Queue,
            validation_policy=validation_policy,
        )
        blob = BlobDescriptor("somepath", 10)
        blob_info = _IngestionBlobInfo(blob, properties, auth_context="authorizationContextText")
        self._verify_ingestion_blob_info_result(blob_info.to_json())

    def test_blob_info_json_mapping(self):
        """Tests serialization of json ingestion blob info."""
        validation_policy = ValidationPolicy(ValidationOptions.ValidateCsvInputConstantColumns, ValidationImplications.BestEffort)
        properties = IngestionProperties(
            database="database",
            table="table",
            data_format=DataFormat.JSON,
            ingestion_mapping=[ColumnMapping("ColumnName", "datatype", path="jsonpath")],
            additional_tags=["tag"],
            ingest_if_not_exists=["ingestIfNotExistTags"],
            ingest_by_tags=["ingestByTags"],
            drop_by_tags=["dropByTags"],
            flush_immediately=True,
            report_level=ReportLevel.DoNotReport,
            report_method=ReportMethod.Queue,
            validation_policy=validation_policy,
        )
        blob = BlobDescriptor("somepath", 10)
        blob_info = _IngestionBlobInfo(blob, properties, auth_context="authorizationContextText")
        self._verify_ingestion_blob_info_result(blob_info.to_json())

    def test_blob_json_mapping_reference(self):
        """Tests serialization of ingestion blob info with json mapping reference."""
        validation_policy = ValidationPolicy(ValidationOptions.ValidateCsvInputConstantColumns, ValidationImplications.BestEffort)
        properties = IngestionProperties(
            database="database",
            table="table",
            data_format=DataFormat.JSON,
            ingestion_mapping_reference="jsonMappingReference",
            additional_tags=["tag"],
            ingest_if_not_exists=["ingestIfNotExistTags"],
            ingest_by_tags=["ingestByTags"],
            drop_by_tags=["dropByTags"],
            flush_immediately=True,
            report_level=ReportLevel.DoNotReport,
            report_method=ReportMethod.Queue,
            validation_policy=validation_policy,
        )
        blob = BlobDescriptor("somepath", 10)
        blob_info = _IngestionBlobInfo(blob, properties, auth_context="authorizationContextText")
        self._verify_ingestion_blob_info_result(blob_info.to_json())

    def test_blob_info_csv_exceptions(self):
        """Tests invalid ingestion properties."""
        with self.assertRaises(KustoMappingAndMappingReferenceError):
            IngestionProperties(
                database="database", table="table", ingestion_mapping="ingestionMapping", ingestion_mapping_reference="ingestionMappingReference"
            )

    def _verify_ingestion_blob_info_result(self, ingestion_blob_info):
        result = json.loads(ingestion_blob_info)
        assert result is not None
        assert isinstance(result, dict)
        assert result["BlobPath"] == "somepath"
        assert result["DatabaseName"] == "database"
        assert result["TableName"] == "table"
        assert isinstance(result["RawDataSize"], int)
        assert isinstance(result["IgnoreSizeLimit"], bool)
        assert isinstance(result["FlushImmediately"], bool)
        assert isinstance(result["RetainBlobOnSuccess"], bool)
        assert isinstance(result["ReportMethod"], int)
        assert isinstance(result["ReportLevel"], int)
        assert isinstance(UUID(result["Id"]), UUID)
        assert re.match(TIMESTAMP_REGEX, result["SourceMessageCreationTime"])
        assert result["AdditionalProperties"]["authorizationContext"] == "authorizationContextText"
        assert result["AdditionalProperties"]["ingestIfNotExists"] == '["ingestIfNotExistTags"]'
        assert result["AdditionalProperties"]["ValidationPolicy"] in (
            '{"ValidationImplications":1,"ValidationOptions":1}',
            '{"ValidationImplications":ValidationImplications.BestEffort,"ValidationOptions":ValidationOptions.ValidateCsvInputConstantColumns}',
        )

        assert result["AdditionalProperties"]["tags"] == '["tag","drop-by:dropByTags","ingest-by:ingestByTags"]'
