from dataclasses import dataclass
from typing import List

from kit.models.database import Column


@dataclass
class ColumnMapping:
    source: Column
    target: Column


@dataclass
class IngestionMapping:
    name: str
    columns: List[ColumnMapping]
