# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
from enum import Enum


class IngestionMappingKind(Enum):
    CSV = "Csv"
    JSON = "Json"
    AVRO = "Avro"
    APACHEAVRO = "ApacheAvro"
    PARQUET = "Parquet"
    SSTREAM = "SStream"
    ORC = "Orc"
    W3CLOGFILE = "W3CLogFile"
    UNKNOWN = "Unknown"


class DataFormat(Enum):
    """All data formats supported by Kusto."""

    CSV = ("csv", IngestionMappingKind.CSV, False, True)
    TSV = ("tsv", IngestionMappingKind.CSV, False, True)
    SCSV = ("scsv", IngestionMappingKind.CSV, False, True)
    SOHSV = ("sohsv", IngestionMappingKind.CSV, False, True)
    PSV = ("psv", IngestionMappingKind.CSV, False, True)
    TXT = ("txt", IngestionMappingKind.CSV, False, True)
    TSVE = ("tsve", IngestionMappingKind.CSV, False, True)
    JSON = ("json", IngestionMappingKind.JSON, True, True)
    SINGLEJSON = ("singlejson", IngestionMappingKind.JSON, True, True)
    MULTIJSON = ("multijson", IngestionMappingKind.JSON, True, True)
    AVRO = ("avro", IngestionMappingKind.AVRO, True, True)
    APACHEAVRO = ("apacheavro", IngestionMappingKind.APACHEAVRO, False, True)
    PARQUET = ("parquet", IngestionMappingKind.PARQUET, False, False)
    SSTREAM = ("sstream", IngestionMappingKind.SSTREAM, False, False)
    ORC = ("orc", IngestionMappingKind.ORC, False, False)
    RAW = ("raw", IngestionMappingKind.CSV, False, True)
    W3CLOGFILE = ("w3clogfile", IngestionMappingKind.W3CLOGFILE, False, True)

    def __init__(self, kusto_value: str, ingestion_mapping_kind: IngestionMappingKind, mapping_required: bool, compressible: bool):
        self.kusto_value = kusto_value  # Formatted how Kusto Service expects it
        self.ingestion_mapping_kind = ingestion_mapping_kind
        # Deprecated - will probably be removed soon
        self._mapping_required = mapping_required
        self.compressible = compressible  # Binary formats should not be compressed
