""" Tests serialization of ingestion blob info. This serialization will be queued to the DM
"""

import unittest
import re
from azure.kusto.ingest.ingestion_blob_info import _IngestionBlobInfo
from azure.kusto.ingest import (
    KustoDuplicateMappingError,
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

KUSTO_MESSAGE_REGEX = '{"BlobPath":"somepath","RawDataSize":[0-9]+,"DatabaseName":"database",\
"TableName":"table",\
"RetainBlobOnSuccess":(false|true),\
"FlushImmediately":(false|true),\
"IgnoreSizeLimit":(false|true),\
("ReportLevel":[0-2],)?\
("ReportMethod":[0-2],)?\
("IngestionStatusInTable":null,)?\
"SourceMessageCreationTime":"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{6}",\
"Id":"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",\
("BlobPathEncrypted":null,)?\
"AdditionalProperties":\
{"authorizationContext":"authorizationContextText",\
"tags":"\\[\\\\"tag\\\\",\\\\"drop-by:dropByTags\\\\",\\\\"ingest-by:ingestByTags\\\\"]",\
"ingestIfNotExists":"\\[\\\\"ingestIfNotExistTags\\\\"]",\
("csvMapping":"\\[{\\\\"DataType\\\\":\\\\"cslDataType\\\\",\\\\"Name\\\\":\\\\"ColumnName\\\\",\\\\"Ordinal\\\\":1}]",\
|"csvMappingReference":"csvMappingReference",\
|"jsonMapping":"\\[{\\\\"column\\\\":\\\\"ColumnName\\\\",\\\\"datatype\\\\":\\\\"datatype\\\\",\\\\"path\\\\":\\\\"jsonpath\\\\"}]",\
|"jsonMappingReference":"jsonMappingReference",)?\
"ValidationPolicy":"{\\\\"ValidationImplications\\\\":[0-1],\\\\"ValidationOptions\\\\":[0-2]}","format":"(csv|json)"}}'

class IngestionBlobInfoTest(unittest.TestCase):
    """ Tests serialization of ingestion blob info. This serialization will be queued to the DM """
    def test_blob_info_csv_mapping(self):
        """ Tests serialization of csv ingestion blob info. """
        validation_policy = ValidationPolicy(ValidationOptions.ValidateCsvInputConstantColumns,
                                             ValidationImplications.BestEffort)
        properties = IngestionProperties(database="database",
                                         table="table",
                                         dataFormat=DataFormat.csv,
                                         mapping=[CsvColumnMapping("ColumnName", "cslDataType", 1)],
                                         additionalTags=["tag"],
                                         ingestIfNotExists=["ingestIfNotExistTags"],
                                         ingestByTags=["ingestByTags"],
                                         dropByTags=["dropByTags"],
                                         flushImmediately=True,
                                         reportLevel=ReportLevel.DoNotReport,
                                         reportMethod=ReportMethod.QueueAndTable,
                                         validationPolicy=validation_policy,
                                        )
        blob = BlobDescriptor("somepath", 10)
        blob_info = _IngestionBlobInfo(blob, properties, deleteSourcesOnSuccess=True, authContext="authorizationContextText")
        blob_info_json = blob_info.to_json()
        match = re.match(KUSTO_MESSAGE_REGEX, blob_info_json)
        self.assertTrue(match is not None)
        self.assertTrue("csvMapping" in blob_info_json)

    def test_blob_csv_mapping_reference(self):
        """ Tests serialization of ingestion blob info with csv mapping reference. """
        validation_policy = ValidationPolicy(ValidationOptions.ValidateCsvInputConstantColumns,
                                             ValidationImplications.BestEffort)
        properties = IngestionProperties(database="database",
                                         table="table",
                                         dataFormat=DataFormat.csv,
                                         mapptingReference="csvMappingReference",
                                         additionalTags=["tag"],
                                         ingestIfNotExists=["ingestIfNotExistTags"],
                                         ingestByTags=["ingestByTags"],
                                         dropByTags=["dropByTags"],
                                         flushImmediately=True,
                                         reportLevel=ReportLevel.DoNotReport,
                                         reportMethod=ReportMethod.QueueAndTable,
                                         validationPolicy=validation_policy,
                                        )
        blob = BlobDescriptor("somepath", 10)
        blob_info = _IngestionBlobInfo(blob, properties, deleteSourcesOnSuccess=True, authContext="authorizationContextText")
        blob_info_json = blob_info.to_json()
        match = re.match(KUSTO_MESSAGE_REGEX, blob_info_json)
        self.assertTrue(match is not None)
        self.assertTrue("csvMappingReference" in blob_info_json)

    def test_blob_info_json_mapping(self):
        """ Tests serialization of json ingestion blob info. """
        validation_policy = ValidationPolicy(ValidationOptions.ValidateCsvInputConstantColumns,
                                             ValidationImplications.BestEffort)
        properties = IngestionProperties(database="database",
                                         table="table",
                                         dataFormat=DataFormat.json,
                                         mapping=[JsonColumnMapping("ColumnName", "jsonpath", "datatype")],
                                         additionalTags=["tag"],
                                         ingestIfNotExists=["ingestIfNotExistTags"],
                                         ingestByTags=["ingestByTags"],
                                         dropByTags=["dropByTags"],
                                         flushImmediately=True,
                                         reportLevel=ReportLevel.DoNotReport,
                                         reportMethod=ReportMethod.QueueAndTable,
                                         validationPolicy=validation_policy,
                                        )
        blob = BlobDescriptor("somepath", 10)
        blob_info = _IngestionBlobInfo(blob, properties, deleteSourcesOnSuccess=True, authContext="authorizationContextText")
        blob_info_json = blob_info.to_json()
        match = re.match(KUSTO_MESSAGE_REGEX, blob_info_json)
        self.assertTrue(match is not None)
        self.assertTrue("jsonMapping" in blob_info_json)

    def test_blob_json_mapping_reference(self):
        """ Tests serialization of ingestion blob info with json mapping reference. """
        validation_policy = ValidationPolicy(ValidationOptions.ValidateCsvInputConstantColumns,
                                             ValidationImplications.BestEffort)
        properties = IngestionProperties(database="database",
                                         table="table",
                                         dataFormat=DataFormat.json,
                                         mapptingReference="jsonMappingReference",
                                         additionalTags=["tag"],
                                         ingestIfNotExists=["ingestIfNotExistTags"],
                                         ingestByTags=["ingestByTags"],
                                         dropByTags=["dropByTags"],
                                         flushImmediately=True,
                                         reportLevel=ReportLevel.DoNotReport,
                                         reportMethod=ReportMethod.QueueAndTable,
                                         validationPolicy=validation_policy,
                                        )
        blob = BlobDescriptor("somepath", 10)
        blob_info = _IngestionBlobInfo(blob, properties, deleteSourcesOnSuccess=True, authContext="authorizationContextText")
        blob_info_json = blob_info.to_json()
        match = re.match(KUSTO_MESSAGE_REGEX, blob_info_json)
        self.assertTrue(match is not None)
        self.assertTrue("jsonMappingReference" in blob_info_json)

    def test_blob_info_csv_exceptions(self):
        """ Tests invalid ingestion properties. """
        with self.assertRaises(KustoDuplicateMappingError):
            IngestionProperties(database="database",
                                table="table",
                                mapping="mapping",
                                mapptingReference="mappingReference")
