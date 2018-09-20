"""This module represents the object to write to azure queue that the DM is listening to."""

import json
import uuid
from datetime import datetime
from six import text_type

from ._descriptors import BlobDescriptor


class _IngestionBlobInfo:
    def __init__(self, blob, ingestion_properties, auth_context=None):
        self.properties = dict()
        self.properties["BlobPath"] = blob.path
        self.properties["RawDataSize"] = blob.size
        self.properties["DatabaseName"] = ingestion_properties.database
        self.properties["TableName"] = ingestion_properties.table
        self.properties["RetainBlobOnSuccess"] = True
        self.properties["FlushImmediately"] = ingestion_properties.flush_immediately
        self.properties["IgnoreSizeLimit"] = False
        self.properties["ReportLevel"] = ingestion_properties.report_level.value
        self.properties["ReportMethod"] = ingestion_properties.report_method.value
        self.properties["SourceMessageCreationTime"] = datetime.utcnow().isoformat()
        self.properties["Id"] = text_type(uuid.uuid4())
        additional_properties = ingestion_properties.additional_properties or {}
        additional_properties["authorizationContext"] = auth_context

        tags = []
        if ingestion_properties.additional_tags:
            tags.extend(ingestion_properties.additional_tags)
        if ingestion_properties.drop_by_tags:
            tags.extend(["drop-by:" + drop for drop in ingestion_properties.drop_by_tags])
        if ingestion_properties.ingest_by_tags:
            tags.extend(["ingest-by:" + ingest for ingest in ingestion_properties.ingest_by_tags])
        if tags:
            additional_properties["tags"] = _convert_list_to_json(tags)
        if ingestion_properties.ingest_if_not_exists:
            additional_properties["ingestIfNotExists"] = _convert_list_to_json(
                ingestion_properties.ingest_if_not_exists
            )
        if ingestion_properties.mapping:
            json_string = _convert_dict_to_json(ingestion_properties.mapping)
            additional_properties[ingestion_properties.get_mapping_format() + "Mapping"] = json_string
        if ingestion_properties.mapping_reference:
            key = ingestion_properties.get_mapping_format() + "MappingReference"
            additional_properties[key] = ingestion_properties.mapping_reference
        if ingestion_properties.validation_policy:
            additional_properties["ValidationPolicy"] = _convert_dict_to_json(ingestion_properties.validation_policy)
        if ingestion_properties.format:
            additional_properties["format"] = ingestion_properties.format.name

        if additional_properties:
            self.properties["AdditionalProperties"] = additional_properties

    def to_json(self):
        """ Converts this object to a json string """
        return _convert_list_to_json(self.properties)


def _convert_list_to_json(array):
    """ Converts array to a json string """
    return json.dumps(array, skipkeys=False, allow_nan=False, indent=None, separators=(",", ":"))


def _convert_dict_to_json(array):
    """ Converts array to a json string """
    return json.dumps(
        array,
        skipkeys=False,
        allow_nan=False,
        indent=None,
        separators=(",", ":"),
        sort_keys=True,
        default=lambda o: o.__dict__,
    )
