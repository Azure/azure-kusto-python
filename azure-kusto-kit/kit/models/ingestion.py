from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Dict

from kit.models.data_source import DataSource
from kit.models.database import Database
from kit.models.mappings import IngestionMapping, ColumnMapping
from kit.models.serializable import SerializableModel


@dataclass
class IngestionSource(SerializableModel):
    files: List[str]
    mapping: str
    options: dict = field(default_factory=dict)
    data_format: str = 'csv'


@dataclass
class IngestionOp(SerializableModel):
    database: str
    sources: List[IngestionSource]
    target: str


@dataclass
class IngestionManifest:
    # This holds a snapshop of the database relevant for ingestion. should be matched with actual database per-ingestion.
    databases: List[Database]
    mappings: List[IngestionMapping]
    operations: List[IngestionOp]

    def __post_init__(self):
        self.load_database_lookup()
        self.load_mappings_lookup()

    @property
    def database_dict(self) -> Dict[str, Database]:
        return self._database_dict

    @property
    def mappings_dict(self) -> Dict[str, IngestionMapping]:
        return self._mappings_dict

    def load_database_lookup(self):
        self._database_dict = {}
        for database in self.databases:
            self._database_dict[database.name] = database

    def load_mappings_lookup(self):
        self._mappings_dict = {}
        for mapping in self.mappings:
            self._mappings_dict[mapping.name] = mapping

    @classmethod
    def from_source_and_database(cls, source: DataSource, target_database: Database) -> IngestionManifest:
        operations = []
        mappings: Dict[str, IngestionMapping] = {}

        for entity in source.entities:
            sources: Dict[str, List[str]] = defaultdict(list)
            target_table = target_database.tables_dict[entity.name]

            for file in entity.files:
                data_format = file.data_format
                mapping_name = entity.name + '_from_' + data_format
                if mapping_name not in mappings:
                    col_mappings = []
                    for index, source_col in enumerate(file.columns):
                        col_mappings.append(ColumnMapping(source_col, target_table.columns[index]))

                    mappings[mapping_name] = IngestionMapping(mapping_name, col_mappings)

                sources[mapping_name].append(file.path)

            ingestion_sources = [IngestionSource(s_files, s_mapping, data_format=s_mapping.split('_')[-1]) for s_mapping, s_files in sources.items()]
            operations.append(IngestionOp(target_database.name, ingestion_sources, target_table.name))

        return IngestionManifest([target_database], list(mappings.values()), operations)
