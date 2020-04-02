import unittest
import re
import json
from uuid import UUID

from azure.kusto.ingest._ingestion_blob_info import _IngestionBlobInfo
from azure.kusto.ingest.exceptions import KustoDuplicateMappingError, KustoDuplicateMappingReferenceError, KustoMappingAndMappingReferenceError
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
        columnMapping = ColumnMapping("ColumnName", "cslDataType", ordinal=1)

        properties = IngestionProperties(
            database="database",
            table="table",
            dataFormat=DataFormat.CSV,
            ingestionMapping=[columnMapping],
            additionalTags=["tag"],
            ingestIfNotExists=["ingestIfNotExistTags"],
            ingestByTags=["ingestByTags"],
            dropByTags=["dropByTags"],
            flushImmediately=True,
            reportLevel=ReportLevel.DoNotReport,
            reportMethod=ReportMethod.Queue,
            validationPolicy=validation_policy,
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
            dataFormat=DataFormat.CSV,
            ingestionMappingReference="csvMappingReference",
            additionalTags=["tag"],
            ingestIfNotExists=["ingestIfNotExistTags"],
            ingestByTags=["ingestByTags"],
            dropByTags=["dropByTags"],
            flushImmediately=True,
            reportLevel=ReportLevel.DoNotReport,
            reportMethod=ReportMethod.Queue,
            validationPolicy=validation_policy,
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
            dataFormat=DataFormat.JSON,
            ingestionMapping=[ColumnMapping("ColumnName", "datatype", path="jsonpath")],
            additionalTags=["tag"],
            ingestIfNotExists=["ingestIfNotExistTags"],
            ingestByTags=["ingestByTags"],
            dropByTags=["dropByTags"],
            flushImmediately=True,
            reportLevel=ReportLevel.DoNotReport,
            reportMethod=ReportMethod.Queue,
            validationPolicy=validation_policy,
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
            dataFormat=DataFormat.JSON,
            mappingReference="jsonMappingReference",
            additionalTags=["tag"],
            ingestIfNotExists=["ingestIfNotExistTags"],
            ingestByTags=["ingestByTags"],
            dropByTags=["dropByTags"],
            flushImmediately=True,
            reportLevel=ReportLevel.DoNotReport,
            reportMethod=ReportMethod.Queue,
            validationPolicy=validation_policy,
        )
        blob = BlobDescriptor("somepath", 10)
        blob_info = _IngestionBlobInfo(blob, properties, auth_context="authorizationContextText")
        self._verify_ingestion_blob_info_result(blob_info.to_json())

    def test_blob_info_csv_exceptions(self):
        """Tests invalid ingestion properties."""
        with self.assertRaises(KustoDuplicateMappingError):
            IngestionProperties(database="database", table="table", mapping="mapping", ingestionMapping="ingestionMapping")
        with self.assertRaises(KustoMappingAndMappingReferenceError):
            IngestionProperties(database="database", table="table", mapping="mapping", ingestionMappingReference="ingestionMappingReference")

        with self.assertRaises(KustoMappingAndMappingReferenceError):
            IngestionProperties(database="database", table="table", ingestionMapping="ingestionMapping", ingestionMappingReference="ingestionMappingReference")
        with self.assertRaises(KustoMappingAndMappingReferenceError):
            IngestionProperties(database="database", table="table", mapping="mapping", mappingReference="mappingReference")
        with self.assertRaises(KustoMappingAndMappingReferenceError):
            IngestionProperties(database="database", table="table", ingestionMapping="ingestionMapping", mappingReference="mappingReference")
        with self.assertRaises(KustoDuplicateMappingReferenceError):
            IngestionProperties(database="database", table="table", mappingReference="mappingReference", ingestionMappingReference="ingestionMappingReference")

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
