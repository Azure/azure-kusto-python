from __future__ import annotations

import builtins
from dataclasses import dataclass
from datetime import datetime
from typing import List

from kit.models.serializable import SerializableModel, to_python_name

reserved_keywords = set(dir(builtins))


@dataclass
class FileFormatSettings(SerializableModel):
    _type: str

    column_headers: bool = None
    delimiter: str = None
    quote_style: str = None
    csv_style: str = None


@dataclass
class Partition(SerializableModel):
    name: str

    description: str = None
    refresh_time: datetime = None
    # url
    location: str = None
    file_format_settings: FileFormatSettings = None


@dataclass
class Annotation(SerializableModel):
    name: str
    value: str = None


@dataclass
class Attribute(SerializableModel):
    name: str
    data_type: str

    description: str = None
    annotations: List[Annotation] = None


@dataclass
class ReferenceAttribute(SerializableModel):
    entity_name: str
    attribute_name: str


#
@dataclass
class Entity(SerializableModel):
    @classmethod
    def fromdict(cls, d) -> SerializableModel:
        d = {to_python_name(k): v for k, v in d.items()}
        if d['_type'] == 'LocalEntity':
            return LocalEntity.fromdict(d)
        if d['_type'] == 'ReferenceEntity':
            return ReferencedEntity.fromdict(d)


@dataclass
class LocalEntity(SerializableModel):
    _type: str
    name: str
    attributes: List[Attribute]

    # url
    description: str = None
    annotations: List[Annotation] = None
    schemas: List[str] = None
    partitions: List[Partition] = None


@dataclass
class ReferencedEntity(SerializableModel):
    _type: str
    name: str
    source: str
    model_id: str

    description: str = None
    annotations: List[Annotation] = None


@dataclass
class ReferenceModel(SerializableModel):
    _id: str
    # url
    location: str


@dataclass
class Relationship(SerializableModel):
    _type: str
    from_attribute: ReferenceAttribute
    to_attribute: ReferenceAttribute


@dataclass
class Model(SerializableModel):
    name: str
    version: str
    entities: List[Entity]

    culture: str = None
    description: str = None
    application: str = None
    modified_time: datetime = None
    annotations: List[Annotation] = None
    reference_models: List[ReferenceModel] = None
    relationships: List[Relationship] = None
