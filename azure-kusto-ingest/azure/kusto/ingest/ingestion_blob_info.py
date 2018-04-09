""" This module represents the object to write to azure queue that the DM is listening to.
"""

import json
import uuid
from datetime import datetime
from .descriptors import BlobDescriptor

class _IngestionBlobInfo:
    def __init__(self, blob, ingestionProperties, deleteSourcesOnSuccess=True, authContext=None):
        if not isinstance(blob, BlobDescriptor):
            raise TypeError("blob must be of type BlobDescriptor")
        self.properties = dict()
        self.properties["BlobPath"] = blob.path
        self.properties["RawDataSize"] = blob.size
        self.properties["DatabaseName"] = ingestionProperties.database
        self.properties["TableName"] = ingestionProperties.table
        self.properties["RetainBlobOnSuccess"] = not deleteSourcesOnSuccess
        self.properties["FlushImmediately"] = ingestionProperties.flush_immediately
        self.properties["IgnoreSizeLimit"] = False
        self.properties["ReportLevel"] = ingestionProperties.report_level.value
        self.properties["ReportMethod"] = ingestionProperties.report_method.value
        self.properties["SourceMessageCreationTime"] = datetime.utcnow().isoformat()
        self.properties["Id"] = str(uuid.uuid4())
        # TODO: Add support for ingestion statuses
        #self.properties["IngestionStatusInTable"] = None
        #self.properties["BlobPathEncrypted"] = None
        additional_properties = dict()
        additional_properties["authorizationContext"] = authContext

        tags = list()
        if ingestionProperties.additional_tags:
            tags.extend(ingestionProperties.additional_tags)
        if ingestionProperties.drop_by_tags:
            tags.extend(["drop-by:" + drop for drop in ingestionProperties.drop_by_tags])
        if ingestionProperties.ingest_by_tags:
            tags.extend(["ingest-by:" + ingest for ingest in ingestionProperties.ingest_by_tags])
        if tags:
            additional_properties["tags"] = _IngestionBlobInfo._convert_list_to_json(tags)
        if ingestionProperties.ingest_if_not_exists:
            additional_properties["ingestIfNotExists"] = _IngestionBlobInfo._convert_list_to_json(ingestionProperties.ingest_if_not_exists)
        if ingestionProperties.mapping:
            json_string = _IngestionBlobInfo._convert_dict_to_json(ingestionProperties.mapping)
            additional_properties[ingestionProperties.get_mapping_format() + "Mapping"] = json_string
        if ingestionProperties.mapping_reference:
            key = ingestionProperties.get_mapping_format() + "MappingReference"
            additional_properties[key] = ingestionProperties.mapping_reference
        if ingestionProperties.validation_policy:
            additional_properties["ValidationPolicy"] = _IngestionBlobInfo._convert_dict_to_json(ingestionProperties.validation_policy)
        if ingestionProperties.format:
            additional_properties["format"] = ingestionProperties.format.name

        if additional_properties:
            self.properties["AdditionalProperties"] = additional_properties

    def to_json(self):
        """ Converts this object to a json string """
        return _IngestionBlobInfo._convert_list_to_json(self.properties)

    @staticmethod
    def _convert_list_to_json(array):
        """ Converts array to a json string """
        return json.dumps(array,
                          skipkeys=False,
                          allow_nan=False,
                          indent=None,
                          separators=(',', ':'))

    @staticmethod
    def _convert_dict_to_json(array):
        """ Converts array to a json string """
        return json.dumps(array,
                          skipkeys=False,
                          allow_nan=False,
                          indent=None,
                          separators=(',', ':'),
                          sort_keys=True,
                          default=lambda o: o.__dict__)
