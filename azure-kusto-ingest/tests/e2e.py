"""E2E tests for ingest_client. """
import pytest
import time
import os
import uuid
import io

from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.ingest.status import KustoIngestStatusQueues
from azure.kusto.ingest import (
    KustoIngestClient,
    KustoStreamingIngestClient,
    IngestionProperties,
    JsonColumnMapping,
    CsvColumnMapping,
    DataFormat,
    IngestionMappingType,
    ValidationPolicy,
    ValidationOptions,
    ValidationImplications,
    ReportLevel,
    ReportMethod,
    FileDescriptor,
    KustoMissingMappingReferenceError,
)


class Helpers:
    """A class to define mappings to deft table."""

    def __init__(self):
        pass

    @staticmethod
    def create_test_table_csv_mappings():
        """A method to define csv mappings to test table."""
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
        mappings.append(CsvColumnMapping(columnName="xnumberAsText", cslDataType="string", ordinal=15))
        mappings.append(CsvColumnMapping(columnName="xtime", cslDataType="timespan", ordinal=16))
        mappings.append(CsvColumnMapping(columnName="xtextWithNulls", cslDataType="string", ordinal=17))
        mappings.append(CsvColumnMapping(columnName="xdynamicWithNulls", cslDataType="dynamic", ordinal=18))
        return mappings

    @staticmethod
    def create_test_table_json_mappings():
        """A method to define json mappings to test table."""
        mappings = list()
        mappings.append(JsonColumnMapping(columnName="rownumber", jsonPath="$.rownumber", cslDataType="int"))
        mappings.append(JsonColumnMapping(columnName="rowguid", jsonPath="$.rowguid", cslDataType="string"))
        mappings.append(JsonColumnMapping(columnName="xdouble", jsonPath="$.xdouble", cslDataType="real"))
        mappings.append(JsonColumnMapping(columnName="xfloat", jsonPath="$.xfloat", cslDataType="real"))
        mappings.append(JsonColumnMapping(columnName="xbool", jsonPath="$.xbool", cslDataType="bool"))
        mappings.append(JsonColumnMapping(columnName="xint16", jsonPath="$.xint16", cslDataType="int"))
        mappings.append(JsonColumnMapping(columnName="xint32", jsonPath="$.xint32", cslDataType="int"))
        mappings.append(JsonColumnMapping(columnName="xint64", jsonPath="$.xint64", cslDataType="long"))
        mappings.append(JsonColumnMapping(columnName="xuint8", jsonPath="$.xuint8", cslDataType="long"))
        mappings.append(JsonColumnMapping(columnName="xuint16", jsonPath="$.xuint16", cslDataType="long"))
        mappings.append(JsonColumnMapping(columnName="xuint32", jsonPath="$.xuint32", cslDataType="long"))
        mappings.append(JsonColumnMapping(columnName="xuint64", jsonPath="$.xuint64", cslDataType="long"))
        mappings.append(JsonColumnMapping(columnName="xdate", jsonPath="$.xdate", cslDataType="datetime"))
        mappings.append(JsonColumnMapping(columnName="xsmalltext", jsonPath="$.xsmalltext", cslDataType="string"))
        mappings.append(JsonColumnMapping(columnName="xtext", jsonPath="$.xtext", cslDataType="string"))
        mappings.append(JsonColumnMapping(columnName="xnumberAsText", jsonPath="$.xnumberAsText", cslDataType="string"))
        mappings.append(JsonColumnMapping(columnName="xtime", jsonPath="$.xtime", cslDataType="timespan"))
        mappings.append(JsonColumnMapping(columnName="xtextWithNulls", jsonPath="$.xtextWithNulls", cslDataType="string"))
        mappings.append(JsonColumnMapping(columnName="xdynamicWithNulls", jsonPath="$.xdynamicWithNulls", cslDataType="dynamic"))
        return mappings

    @staticmethod
    def get_test_table_json_mapping_reference():
        """A method to get json mappings reference to test table."""
        return """'['
                    '    { "column" : "rownumber", "datatype" : "int", "Properties":{"Path":"$.rownumber"}},'
                    '    { "column" : "rowguid", "datatype" : "string", "Properties":{"Path":"$.rowguid"}},'
                    '    { "column" : "xdouble", "datatype" : "real", "Properties":{"Path":"$.xdouble"}},'
                    '    { "column" : "xfloat", "datatype" : "real", "Properties":{"Path":"$.xfloat"}},'
                    '    { "column" : "xbool", "datatype" : "bool", "Properties":{"Path":"$.xbool"}},'
                    '    { "column" : "xint16", "datatype" : "int", "Properties":{"Path":"$.xint16"}},'
                    '    { "column" : "xint32", "datatype" : "int", "Properties":{"Path":"$.xint32"}},'
                    '    { "column" : "xint64", "datatype" : "long", "Properties":{"Path":"$.xint64"}},'
                    '    { "column" : "xuint8", "datatype" : "long", "Properties":{"Path":"$.xuint8"}},'
                    '    { "column" : "xuint16", "datatype" : "long", "Properties":{"Path":"$.xuint16"}},'
                    '    { "column" : "xuint32", "datatype" : "long", "Properties":{"Path":"$.xuint32"}},'
                    '    { "column" : "xuint64", "datatype" : "long", "Properties":{"Path":"$.xuint64"}},'
                    '    { "column" : "xdate", "datatype" : "datetime", "Properties":{"Path":"$.xdate"}},'
                    '    { "column" : "xsmalltext", "datatype" : "string", "Properties":{"Path":"$.xsmalltext"}},'
                    '    { "column" : "xtext", "datatype" : "string", "Properties":{"Path":"$.xtext"}},'
                    '    { "column" : "xnumberAsText", "datatype" : "string", "Properties":{"Path":"$.rowguid"}},'
                    '    { "column" : "xtime", "datatype" : "timespan", "Properties":{"Path":"$.xtime"}},'
                    '    { "column" : "xtextWithNulls", "datatype" : "string", "Properties":{"Path":"$.xtextWithNulls"}},'
                    '    { "column" : "xdynamicWithNulls", "datatype" : "dynamic", "Properties":{"Path":"$.xdynamicWithNulls"}},'
                    ']'"""


# Get environment variables
engine_cs = os.environ.get("ENGINE_CONECTION_STRING")
dm_cs = os.environ.get("DM_CONECTION_STRING")
db_name = os.environ.get("TEST_DATABASE")  # Existed db with streaming ingestion enabled
app_id = os.environ.get("APP_ID")
app_key = os.environ.get("APP_KEY")
tenant_id = os.environ.get("TENANT_ID")

# Init clients
table_name = "python_test"
engine_kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(engine_cs, app_id, app_key, tenant_id)
dm_kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(dm_cs, app_id, app_key, tenant_id)
client = KustoClient(engine_kcsb)
ingest_client = KustoIngestClient(dm_kcsb)
ingest_status_q = KustoIngestStatusQueues(ingest_client)
streaming_ingest_client = KustoStreamingIngestClient(engine_kcsb)

# Clean previous test
client.execute(db_name, ".drop table {} ifexists".format(table_name))
while not ingest_status_q.success.is_empty():
    ingest_status_q.success.pop()

# Get files paths
current_dir = os.getcwd()
path_parts = ["azure-kusto-ingest", "tests", "input"]
missing_path_parts = []
for path_part in path_parts:
    if path_part not in current_dir:
        missing_path_parts.append(path_part)
input_folder_path = os.path.join(current_dir, *missing_path_parts)

csv_file_path = os.path.join(input_folder_path, "dataset.csv")
tsv_file_path = os.path.join(input_folder_path, "dataset.tsv")
zipped_csv_file_path = os.path.join(input_folder_path, "dataset.csv.gz")
json_file_path = os.path.join(input_folder_path, "dataset.json")
zipped_json_file_path = os.path.join(input_folder_path, "dataset.jsonz.gz")

current_count = 0


def assert_row_count(expected_row_count, timeout=300):
    global current_count
    row_count = 0

    while timeout > 0:
        time.sleep(10)
        timeout -= 10

        try:
            response = client.execute(db_name, "{} | count".format(table_name))
        except KustoServiceError:
            continue

        if response is not None:
            row = response.primary_results[0][0]
            row_count = int(row["Count"]) - current_count
            if row_count == expected_row_count:
                current_count += row_count
                return

    current_count += row_count
    assert False, "Row count expected = {}, while actual row count = {}".format(expected_row_count, row_count)


def assert_success_mesagges_count(expected_success_messages, timeout=60):
    successes = 0
    timeout = 60
    while successes != expected_success_messages and timeout > 0:
        while ingest_status_q.success.is_empty() and timeout > 0:
            time.sleep(1)
            timeout -= 1

        success_message = ingest_status_q.success.pop()

        assert success_message[0].Database == db_name
        assert success_message[0].Table == table_name

        successes += 1

    assert successes == expected_success_messages


def test_csv_ingest_non_existing_table():
    csv_ingest_props = IngestionProperties(
        db_name, table_name, dataFormat=DataFormat.CSV, ingestionMapping=Helpers.create_test_table_csv_mappings(), reportLevel=ReportLevel.FailuresAndSuccesses
    )

    for f in [csv_file_path, zipped_csv_file_path]:
        ingest_client.ingest_from_file(f, csv_ingest_props)

    assert_success_mesagges_count(2)
    assert_row_count(20)

    client.execute(db_name, ".create table {} ingestion json mapping 'JsonMapping' {}".format(table_name, Helpers.get_test_table_json_mapping_reference()))


def test_json_ingest_existing_table():
    json_ingestion_props = IngestionProperties(
        db_name,
        table_name,
        dataFormat=DataFormat.JSON,
        ingestionMapping=Helpers.create_test_table_json_mappings(),
        reportLevel=ReportLevel.FailuresAndSuccesses,
    )

    for f in [json_file_path, zipped_json_file_path]:
        ingest_client.ingest_from_file(f, json_ingestion_props)

    assert_success_mesagges_count(2)

    assert_row_count(4)


def test_ingest_complicated_props():
    validation_policy = ValidationPolicy(
        validationOptions=ValidationOptions.ValidateCsvInputConstantColumns, validationImplications=ValidationImplications.Fail
    )
    json_ingestion_props = IngestionProperties(
        db_name,
        table_name,
        dataFormat=DataFormat.JSON,
        ingestionMapping=Helpers.create_test_table_json_mappings(),
        additionalTags=["a", "b"],
        ingestIfNotExists=["aaaa", "bbbb"],
        ingestByTags=["ingestByTag"],
        dropByTags=["drop", "drop-by"],
        flushImmediately=False,
        reportLevel=ReportLevel.FailuresAndSuccesses,
        reportMethod=ReportMethod.Queue,
        validationPolicy=validation_policy,
    )

    file_paths = [json_file_path, zipped_json_file_path]
    fds = [FileDescriptor(fp, 0, uuid.uuid4()) for fp in file_paths]

    for fd in fds:
        ingest_client.ingest_from_file(fd, json_ingestion_props)

    assert_success_mesagges_count(2)
    assert_row_count(4)


def test_json_ingestion_ingest_by_tag():
    json_ingestion_props = IngestionProperties(
        db_name,
        table_name,
        dataFormat=DataFormat.JSON,
        ingestionMapping=Helpers.create_test_table_json_mappings(),
        ingestIfNotExists=["ingestByTag"],
        reportLevel=ReportLevel.FailuresAndSuccesses,
        dropByTags=["drop", "drop-by"],
    )

    for f in [json_file_path, zipped_json_file_path]:
        ingest_client.ingest_from_file(f, json_ingestion_props)

    assert_success_mesagges_count(2)
    assert_row_count(0)


def test_tsv_ingestion_csv_mapping():
    tsv_ingestion_props = IngestionProperties(
        db_name, table_name, dataFormat=DataFormat.TSV, ingestionMapping=Helpers.create_test_table_csv_mappings(), reportLevel=ReportLevel.FailuresAndSuccesses
    )

    ingest_client.ingest_from_file(tsv_file_path, tsv_ingestion_props)

    assert_success_mesagges_count(1)
    assert_row_count(10)


def test_streaming_ingest_from_opened_file():
    ingestion_properties = IngestionProperties(database=db_name, table=table_name, dataFormat=DataFormat.CSV)

    stream = open(csv_file_path, "r")
    streaming_ingest_client.ingest_from_stream(stream, ingestion_properties=ingestion_properties)

    assert_row_count(10, timeout=120)


def test_streaming_ingest_form_csv_file():
    ingestion_properties = IngestionProperties(database=db_name, table=table_name, dataFormat=DataFormat.CSV)

    for f in [csv_file_path, zipped_csv_file_path]:
        streaming_ingest_client.ingest_from_file(f, ingestion_properties=ingestion_properties)

    assert_row_count(20, timeout=120)


def test_streaming_ingest_from_json_file():
    ingestion_properties = IngestionProperties(
        database=db_name, table=table_name, dataFormat=DataFormat.JSON, ingestionMappingReference="JsonMapping", ingestionMappingType=IngestionMappingType.JSON
    )

    for f in [json_file_path, zipped_json_file_path]:
        streaming_ingest_client.ingest_from_file(f, ingestion_properties=ingestion_properties)

    assert_row_count(4, timeout=120)


def test_streaming_ingest_from_csv_io_streams():
    ingestion_properties = IngestionProperties(database=db_name, table=table_name, dataFormat=DataFormat.CSV)
    byte_sequence = b'0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,"Zero",0,00:00:00,,null'
    bytes_stream = io.BytesIO(byte_sequence)
    streaming_ingest_client.ingest_from_stream(bytes_stream, ingestion_properties=ingestion_properties)

    str_sequence = '0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,"Zero",0,00:00:00,,null'
    str_stream = io.StringIO(str_sequence)
    streaming_ingest_client.ingest_from_stream(str_stream, ingestion_properties=ingestion_properties)

    assert_row_count(2, timeout=120)


def test_streaming_ingest_from_json_io_streams():
    ingestion_properties = IngestionProperties(
        database=db_name, table=table_name, dataFormat=DataFormat.JSON, ingestionMappingReference="JsonMapping", ingestionMappingType=IngestionMappingType.JSON
    )

    byte_sequence = b'{"rownumber": 0, "rowguid": "00000000-0000-0000-0001-020304050607", "xdouble": 0.0, "xfloat": 0.0, "xbool": 0, "xint16": 0, "xint32": 0, "xint64": 0, "xunit8": 0, "xuint16": 0, "xunit32": 0, "xunit64": 0, "xdate": "2014-01-01T01:01:01Z", "xsmalltext": "Zero", "xtext": "Zero", "xnumberAsText": "0", "xtime": "00:00:00", "xtextWithNulls": null, "xdynamicWithNulls": ""}'
    bytes_stream = io.BytesIO(byte_sequence)
    streaming_ingest_client.ingest_from_stream(bytes_stream, ingestion_properties=ingestion_properties)

    str_sequence = u'{"rownumber": 0, "rowguid": "00000000-0000-0000-0001-020304050607", "xdouble": 0.0, "xfloat": 0.0, "xbool": 0, "xint16": 0, "xint32": 0, "xint64": 0, "xunit8": 0, "xuint16": 0, "xunit32": 0, "xunit64": 0, "xdate": "2014-01-01T01:01:01Z", "xsmalltext": "Zero", "xtext": "Zero", "xnumberAsText": "0", "xtime": "00:00:00", "xtextWithNulls": null, "xdynamicWithNulls": ""}'
    str_stream = io.StringIO(str_sequence)
    streaming_ingest_client.ingest_from_stream(str_stream, ingestion_properties=ingestion_properties)

    assert_row_count(2, timeout=120)


def test_streaming_ingest_from_dataframe():
    from pandas import DataFrame

    fields = [
        "rownumber",
        "rowguid",
        "xdouble",
        "xfloat",
        "xbool",
        "xint16",
        "xint32",
        "xint64",
        "xunit8",
        "xuint16",
        "xunit32",
        "xunit64",
        "xdate",
        "xsmalltext",
        "xtext",
        "xnumberAsText",
        "xtime",
        "xtextWithNulls",
        "xdynamicWithNulls",
    ]
    rows = [[0, "00000000-0000-0000-0001-020304050607", 0.0, 0.0, 0, 0, 0, 0, 0, 0, 0, 0, "2014-01-01T01:01:01Z", "Zero", "Zero", "0", "00:00:00", None, ""]]
    df = DataFrame(data=rows, columns=fields)
    ingestion_properties = IngestionProperties(database=db_name, table=table_name, dataFormat=DataFormat.CSV)
    ingest_client.ingest_from_dataframe(df, ingestion_properties)

    assert_row_count(1, timeout=120)
