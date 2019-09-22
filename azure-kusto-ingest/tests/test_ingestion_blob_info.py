import unittest
import re
import json
from uuid import UUID
from six import assertRegex
from azure.kusto.ingest._ingestion_blob_info import _IngestionBlobInfo
from azure.kusto.ingest.exceptions import (
    KustoDuplicateMappingError,
    KustoDuplicateMappingReferenceError,
    KustoMappingAndMappingReferenceError,
)
from azure.kusto.ingest import (
    BlobDescriptor,
    IngestionProperties,
    DataFormat,
    CsvColumnMapping,
    JsonColumnMapping,
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
        validation_policy = ValidationPolicy(
            ValidationOptions.ValidateCsvInputConstantColumns, ValidationImplications.BestEffort
        )
        properties = IngestionProperties(
            database="database",
            table="table",
            dataFormat=DataFormat.CSV,
            ingestionMapping=[CsvColumnMapping("ColumnName", "cslDataType", 1)],
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
        validation_policy = ValidationPolicy(
            ValidationOptions.ValidateCsvInputConstantColumns, ValidationImplications.BestEffort
        )
        properties = IngestionProperties(
            database="database",
            table="table",
            dataFormat=DataFormat.CSV,
            mappingReference="csvMappingReference",
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
        validation_policy = ValidationPolicy(
            ValidationOptions.ValidateCsvInputConstantColumns, ValidationImplications.BestEffort
        )
        properties = IngestionProperties(
            database="database",
            table="table",
            dataFormat=DataFormat.JSON,
            ingestionMapping=[JsonColumnMapping("ColumnName", "jsonpath", "datatype")],
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
        validation_policy = ValidationPolicy(
            ValidationOptions.ValidateCsvInputConstantColumns, ValidationImplications.BestEffort
        )
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
            IngestionProperties(
                database="database", table="table", mapping="mapping", ingestionMapping="ingestionMapping"
            )

        with self.assertRaises(KustoMappingAndMappingReferenceError):
            IngestionProperties(
                database="database",
                table="table",
                mapping="mapping",
                ingestionMappingReference="ingestionMappingReference",
            )

        with self.assertRaises(KustoMappingAndMappingReferenceError):
            IngestionProperties(
                database="database",
                table="table",
                ingestionMapping="ingestionMapping",
                ingestionMappingReference="ingestionMappingReference",
            )
        with self.assertRaises(KustoMappingAndMappingReferenceError):
            IngestionProperties(
                database="database", table="table", mapping="mapping", mappingReference="mappingReference"
            )
        with self.assertRaises(KustoMappingAndMappingReferenceError):
            IngestionProperties(
                database="database",
                table="table",
                ingestionMapping="ingestionMapping",
                mappingReference="mappingReference",
            )
        with self.assertRaises(KustoDuplicateMappingReferenceError):
            IngestionProperties(
                database="database",
                table="table",
                mappingReference="mappingReference",
                ingestionMappingReference="ingestionMappingReference",
            )

    def _verify_ingestion_blob_info_result(self, ingestion_blob_info):
        result = json.loads(ingestion_blob_info)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(result["BlobPath"], "somepath")
        self.assertEqual(result["DatabaseName"], "database")
        self.assertEqual(result["TableName"], "table")
        self.assertIsInstance(result["RawDataSize"], int)
        self.assertIsInstance(result["IgnoreSizeLimit"], bool)
        self.assertIsInstance(result["FlushImmediately"], bool)
        self.assertIsInstance(result["RetainBlobOnSuccess"], bool)
        self.assertIsInstance(result["ReportMethod"], int)
        self.assertIsInstance(result["ReportLevel"], int)
        self.assertIsInstance(UUID(result["Id"]), UUID)
        assertRegex(self, result["SourceMessageCreationTime"], TIMESTAMP_REGEX)
        self.assertEqual(result["AdditionalProperties"]["authorizationContext"], "authorizationContextText")
        self.assertEqual(result["AdditionalProperties"]["ingestIfNotExists"], '["ingestIfNotExistTags"]')
        self.assertIn(
            result["AdditionalProperties"]["ValidationPolicy"],
            (
                '{"ValidationImplications":1,"ValidationOptions":1}',
                '{"ValidationImplications":ValidationImplications.BestEffort,"ValidationOptions":ValidationOptions.ValidateCsvInputConstantColumns}',
            ),
        )
        self.assertEqual(
            result["AdditionalProperties"]["tags"], '["tag","drop-by:dropByTags","ingest-by:ingestByTags"]'
        )
