# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
from enum import Enum


class IngestionMappingType(Enum):
    CSV = "Csv"
    JSON = "Json"
    AVRO = "Avro"
    PARQUET = "Parquet"
    ORC = "Orc"
    W3CLOGFILE = "W3CLogFile"
    UNKNOWN = "Unknown"


class DataFormat(Enum):
    """All data formats supported by Kusto."""

    CSV = ("csv", IngestionMappingType.CSV, False, True)
    TSV = ("tsv", IngestionMappingType.CSV, False, True)
    SCSV = ("scsv", IngestionMappingType.CSV, False, True)
    SOHSV = ("sohsv", IngestionMappingType.CSV, False, True)
    PSV = ("psv", IngestionMappingType.CSV, False, True)
    TXT = ("txt", IngestionMappingType.UNKNOWN, False, True)
    TSVE = ("tsve", IngestionMappingType.CSV, False, True)
    JSON = ("json", IngestionMappingType.JSON, True, True)
    SINGLEJSON = ("singlejson", IngestionMappingType.JSON, True, True)
    MULTIJSON = ("multijson", IngestionMappingType.JSON, True, True)
    AVRO = ("avro", IngestionMappingType.AVRO, True, True)
    PARQUET = ("parquet", IngestionMappingType.PARQUET, False, False)
    ORC = ("orc", IngestionMappingType.ORC, False, False)
    RAW = ("raw", IngestionMappingType.UNKNOWN, False, True)
    W3CLOGFILE = ("w3clogfile", IngestionMappingType.W3CLOGFILE, False, True)

    def __init__(self, value_: str, ingestion_mapping_type: IngestionMappingType, mapping_required: bool, compressible: bool):
        self.value_ = value_  # Formatted how Kusto Service expects it
        self.ingestion_mapping_type = ingestion_mapping_type
        self.mapping_required = mapping_required
        self.compressible = compressible  # Binary formats should not be compressed
