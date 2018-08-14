"""E2E tests for kusto_ingest_client."""

import time
import os
from six import text_type

from azure.kusto.data.request import KustoClient
from azure.kusto.ingest import (
    KustoIngestClient,
    IngestionProperties,
    JsonColumnMapping,
    CsvColumnMapping,
    DataFormat,
    ValidationPolicy,
    ValidationOptions,
    ValidationImplications,
    ReportLevel,
    ReportMethod,
)


class Helpers:
    """A class to define mappings to deft table."""

    def __init__(self):
        pass

    @staticmethod
    def create_deft_table_csv_mappings():
        """A method to define csv mappings to deft table."""
        mappings = list()
        mappings.append(CsvColumnMapping(columnName="rownumber", cslDataType="int", ordinal=0))
        mappings.append(CsvColumnMapping(columnName="rowguid", cslDataType="string", ordinal=1))
        mappings.append(CsvColumnMapping(columnName="xdouble", cslDataType="real", ordinal=2))
        mappings.append(CsvColumnMapping(columnName="xfloat", cslDataType="real", ordinal=3))
        mappings.append(CsvColumnMapping(columnName="xbool", cslDataType="bool", ordinal=4))
        mappings.append(CsvColumnMapping(columnName="xint16", cslDataType="int", ordinal=5))
        mappings.append(CsvColumnMapping(columnName="xint32", cslDataType="int", ordinal=6))
        mappings.append(CsvColumnMapping(columnName="xint64", cslDataType="long", ordinal=7))
        mappings.append(CsvColumnMapping(columnName="xuint8", cslDataType="long", ordinal=8))
        mappings.append(CsvColumnMapping(columnName="xuint16", cslDataType="long", ordinal=9))
        mappings.append(CsvColumnMapping(columnName="xuint32", cslDataType="long", ordinal=10))
        mappings.append(CsvColumnMapping(columnName="xuint64", cslDataType="long", ordinal=11))
        mappings.append(CsvColumnMapping(columnName="xdate", cslDataType="datetime", ordinal=12))
        mappings.append(CsvColumnMapping(columnName="xsmalltext", cslDataType="string", ordinal=13))
        mappings.append(CsvColumnMapping(columnName="xtext", cslDataType="string", ordinal=14))
        mappings.append(
            CsvColumnMapping(columnName="xnumberAsText", cslDataType="string", ordinal=15)
        )
        mappings.append(CsvColumnMapping(columnName="xtime", cslDataType="timespan", ordinal=16))
        mappings.append(
            CsvColumnMapping(columnName="xtextWithNulls", cslDataType="string", ordinal=17)
        )
        mappings.append(
            CsvColumnMapping(columnName="xdynamicWithNulls", cslDataType="dynamic", ordinal=18)
        )
        return mappings

    @staticmethod
    def create_deft_table_json_mappings():
        """A method to define json mappings to deft table."""
        mappings = list()
        mappings.append(
            JsonColumnMapping(columnName="rownumber", jsonPath="$.rownumber", cslDataType="int")
        )
        mappings.append(
            JsonColumnMapping(columnName="rowguid", jsonPath="$.rowguid", cslDataType="string")
        )
        mappings.append(
            JsonColumnMapping(columnName="xdouble", jsonPath="$.xdouble", cslDataType="real")
        )
        mappings.append(
            JsonColumnMapping(columnName="xfloat", jsonPath="$.xfloat", cslDataType="real")
        )
        mappings.append(
            JsonColumnMapping(columnName="xbool", jsonPath="$.xbool", cslDataType="bool")
        )
        mappings.append(
            JsonColumnMapping(columnName="xint16", jsonPath="$.xint16", cslDataType="int")
        )
        mappings.append(
            JsonColumnMapping(columnName="xint32", jsonPath="$.xint32", cslDataType="int")
        )
        mappings.append(
            JsonColumnMapping(columnName="xint64", jsonPath="$.xint64", cslDataType="long")
        )
        mappings.append(
            JsonColumnMapping(columnName="xuint8", jsonPath="$.xuint8", cslDataType="long")
        )
        mappings.append(
            JsonColumnMapping(columnName="xuint16", jsonPath="$.xuint16", cslDataType="long")
        )
        mappings.append(
            JsonColumnMapping(columnName="xuint32", jsonPath="$.xuint32", cslDataType="long")
        )
        mappings.append(
            JsonColumnMapping(columnName="xuint64", jsonPath="$.xuint64", cslDataType="long")
        )
        mappings.append(
            JsonColumnMapping(columnName="xdate", jsonPath="$.xdate", cslDataType="datetime")
        )
        mappings.append(
            JsonColumnMapping(
                columnName="xsmalltext", jsonPath="$.xsmalltext", cslDataType="string"
            )
        )
        mappings.append(
            JsonColumnMapping(columnName="xtext", jsonPath="$.xtext", cslDataType="string")
        )
        mappings.append(
            JsonColumnMapping(
                columnName="xnumberAsText", jsonPath="$.xnumberAsText", cslDataType="string"
            )
        )
        mappings.append(
            JsonColumnMapping(columnName="xtime", jsonPath="$.xtime", cslDataType="timespan")
        )
        mappings.append(
            JsonColumnMapping(
                columnName="xtextWithNulls", jsonPath="$.xtextWithNulls", cslDataType="string"
            )
        )
        mappings.append(
            JsonColumnMapping(
                columnName="xdynamicWithNulls",
                jsonPath="$.xdynamicWithNulls",
                cslDataType="dynamic",
            )
        )
        return mappings


KUSTO_CLIENT = KustoClient("https://toshetah.kusto.windows.net")
KUSTO_INGEST_CLIENT = KustoIngestClient("https://ingest-toshetah.kusto.windows.net")

KUSTO_CLIENT.execute("PythonTest", ".drop table Deft ifexists")

# Sanity test - ingest from csv to a non-existing table
CSV_INGESTION_PROPERTIES = IngestionProperties(
    "PythonTest",
    "Deft",
    dataFormat=DataFormat.csv,
    mapping=Helpers.create_deft_table_csv_mappings(),
)
CSV_FILE_PATH = os.path.join(os.getcwd(), "azure-kusto-ingest", "tests", "input", "dataset.csv")
ZIPPED_CSV_FILE_PATH = os.path.join(
    os.getcwd(), "azure-kusto-ingest", "tests", "input", "dataset.csv.gz"
)
KUSTO_INGEST_CLIENT.ingest_from_multiple_files(
    [CSV_FILE_PATH, ZIPPED_CSV_FILE_PATH], False, CSV_INGESTION_PROPERTIES
)

time.sleep(60)
RESPONSE = KUSTO_CLIENT.execute("PythonTest", "Deft | count")
for row in RESPONSE.primary_results:
    if int(row["Count"]) == 20:
        print("Completed ingest from CSV mapping successfully.")
    else:
        print("Deft | count = " + text_type(row["Count"]))

# Sanity test - ingest from json to an existing table
JSON_INGESTION_PROPERTIES = IngestionProperties(
    "PythonTest",
    "Deft",
    dataFormat=DataFormat.json,
    mapping=Helpers.create_deft_table_json_mappings(),
)
JSON_FILE_PATH = os.path.join(os.getcwd(), "azure-kusto-ingest", "tests", "input", "dataset.json")
ZIPPED_JSON_FILE_PATH = os.path.join(
    os.getcwd(), "azure-kusto-ingest", "tests", "input", "dataset.jsonz.gz"
)
KUSTO_INGEST_CLIENT.ingest_from_multiple_files(
    [JSON_FILE_PATH, ZIPPED_JSON_FILE_PATH], False, JSON_INGESTION_PROPERTIES
)
time.sleep(60)
RESPONSE = KUSTO_CLIENT.execute("PythonTest", "Deft | count")
for row in RESPONSE.primary_results:
    if int(row["Count"]) == 24:
        print("Completed ingest from json mapping successfully.")
    else:
        print("Deft | count = " + text_type(row["Count"]))

# Test ingest with complicated ingestion properties
VALIDATION_POLICY = ValidationPolicy(
    validationOptions=ValidationOptions.ValidateCsvInputConstantColumns,
    validationImplications=ValidationImplications.Fail,
)
JSON_INGESTION_PROPERTIES = IngestionProperties(
    "PythonTest",
    "Deft",
    dataFormat=DataFormat.json,
    mapping=Helpers.create_deft_table_json_mappings(),
    additionalTags=["a", "b"],
    ingestIfNotExists=["aaaa", "bbbb"],
    ingestByTags=["ingestByTag"],
    dropByTags=["drop", "drop-by"],
    flushImmediately=False,
    reportLevel=ReportLevel.FailuresAndSuccesses,
    reportMethod=ReportMethod.QueueAndTable,
    validationPolicy=VALIDATION_POLICY,
)
KUSTO_INGEST_CLIENT.ingest_from_multiple_files(
    [JSON_FILE_PATH, ZIPPED_JSON_FILE_PATH], False, JSON_INGESTION_PROPERTIES
)
time.sleep(60)
RESPONSE = KUSTO_CLIENT.execute("PythonTest", "Deft | count")
for row in RESPONSE.primary_results:
    if int(row["Count"]) == 28:
        print("Completed ingest from json mapping with full ingestion properties successfully.")
    else:
        print("Deft | count = " + str(row["Count"]))

# Test ingest with existed ingest-by tag
JSON_INGESTION_PROPERTIES = IngestionProperties(
    "PythonTest",
    "Deft",
    dataFormat=DataFormat.json,
    mapping=Helpers.create_deft_table_json_mappings(),
    ingestIfNotExists=["ingestByTag"],
    dropByTags=["drop", "drop-by"],
)
KUSTO_INGEST_CLIENT.ingest_from_multiple_files(
    [JSON_FILE_PATH, ZIPPED_JSON_FILE_PATH], False, JSON_INGESTION_PROPERTIES
)
time.sleep(60)
RESPONSE = KUSTO_CLIENT.execute("PythonTest", "Deft | count")
for row in RESPONSE.primary_results:
    if int(row["Count"]) == 28:
        print("Completed ingest with existing ingest-by tag successfully.")
    else:
        print("Deft | count = " + text_type(row["Count"]))

# Test ingest with TSV format and csvMapping
TSV_INGESTION_PROPERTIES = IngestionProperties(
    "PythonTest",
    "Deft",
    dataFormat=DataFormat.tsv,
    mapping=Helpers.create_deft_table_csv_mappings(),
)
TSV_FILE_PATH = os.path.join(os.getcwd(), "azure-kusto-ingest", "tests", "input", "dataset.tsv")
KUSTO_INGEST_CLIENT.ingest_from_multiple_files([TSV_FILE_PATH], False, TSV_INGESTION_PROPERTIES)
time.sleep(60)
RESPONSE = KUSTO_CLIENT.execute("PythonTest", "Deft | count")
for row in RESPONSE.primary_results:
    if int(row["Count"]) == 38:
        print("Completed ingest TSV from CSV mapping successfully.")
    else:
        print("Deft | count = " + text_type(row["Count"]))
