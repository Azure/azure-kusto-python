# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import json
import uuid
from datetime import datetime


class _IngestionBlobInfo:
    def __init__(self, blob_descriptor: "BlobDescriptor", ingestion_properties: "IngestionProperties", auth_context=None):
        self.properties = dict()
        self.properties["BlobPath"] = blob_descriptor.path
        self.properties["RawDataSize"] = blob_descriptor.size
        self.properties["DatabaseName"] = ingestion_properties.database
        self.properties["TableName"] = ingestion_properties.table
        self.properties["RetainBlobOnSuccess"] = True
        self.properties["FlushImmediately"] = ingestion_properties.flush_immediately
        self.properties["IgnoreSizeLimit"] = False
        self.properties["ReportLevel"] = ingestion_properties.report_level.value
        self.properties["ReportMethod"] = ingestion_properties.report_method.value
        self.properties["SourceMessageCreationTime"] = datetime.utcnow().isoformat()
        self.properties["Id"] = (
            str(blob_descriptor.source_id) if hasattr(blob_descriptor, "source_id") and blob_descriptor.source_id is not None else str(uuid.uuid4())
        )

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
            additional_properties["ingestIfNotExists"] = _convert_list_to_json(ingestion_properties.ingest_if_not_exists)
        if ingestion_properties.ingestion_mapping:
            json_string = _convert_dict_to_json(ingestion_properties.ingestion_mapping)
            additional_properties["ingestionMapping"] = json_string

        if ingestion_properties.ingestion_mapping_reference:
            additional_properties["ingestionMappingReference"] = ingestion_properties.ingestion_mapping_reference
        if ingestion_properties.ingestion_mapping_type:
            additional_properties["ingestionMappingType"] = ingestion_properties.ingestion_mapping_type.value
        if ingestion_properties.validation_policy:
            additional_properties["ValidationPolicy"] = _convert_dict_to_json(ingestion_properties.validation_policy)
        if ingestion_properties.format:
            additional_properties["format"] = ingestion_properties.format.value

        if additional_properties:
            self.properties["AdditionalProperties"] = additional_properties

    def to_json(self):
        """Converts this object to a json string"""
        return _convert_list_to_json(self.properties)


def _convert_list_to_json(array):
    """Converts array to a json string"""
    return json.dumps(array, skipkeys=False, allow_nan=False, indent=None, separators=(",", ":"))


def _convert_dict_to_json(array):
    """Converts array to a json string"""
    return json.dumps(array, skipkeys=False, allow_nan=False, indent=None, separators=(",", ":"), sort_keys=True, default=lambda o: o.__dict__)
