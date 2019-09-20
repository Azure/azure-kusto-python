"""E2E tests for ingest_client."""
import pytest
import time
import os
import uuid
import sys
import io
from six import text_type

from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder
from azure.kusto.ingest.status import KustoIngestStatusQueues
from azure.kusto.ingest import (
    KustoIngestClient,
    KustoStreamingIngestClient,
    IngestionProperties,
    JsonColumnMapping,
    CsvColumnMapping,
    DataFormat,
    ValidationPolicy,
    ValidationOptions,
    ValidationImplications,
    ReportLevel,
    ReportMethod,
    FileDescriptor,
    KustoMissingMappingReferenceError,
    KustoStreamMaxSizeExceededError,
)

# TODO: change this file to use pytest as runner


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
        mappings.append(CsvColumnMapping(columnName="xnumberAsText", cslDataType="string", ordinal=15))
        mappings.append(CsvColumnMapping(columnName="xtime", cslDataType="timespan", ordinal=16))
        mappings.append(CsvColumnMapping(columnName="xtextWithNulls", cslDataType="string", ordinal=17))
        mappings.append(CsvColumnMapping(columnName="xdynamicWithNulls", cslDataType="dynamic", ordinal=18))
        return mappings

    @staticmethod
    def create_deft_table_json_mappings():
        """A method to define json mappings to deft table."""
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
        mappings.append(
            JsonColumnMapping(columnName="xtextWithNulls", jsonPath="$.xtextWithNulls", cslDataType="string")
        )
        mappings.append(
            JsonColumnMapping(columnName="xdynamicWithNulls", jsonPath="$.xdynamicWithNulls", cslDataType="dynamic")
        )
        return mappings


cluster = "Dadubovs1.westus"  # "toshetah"
db_name = "TestingDatabase"  # "PythonTest"
table_name = "Deft"


engine_kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(
    "https://{}.kusto.windows.net".format(cluster)
)
dm_kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(
    "https://ingest-{}.kusto.windows.net".format(cluster)
)
client = KustoClient(engine_kcsb)
ingest_client = KustoIngestClient(dm_kcsb)
ingest_status_q = KustoIngestStatusQueues(ingest_client)

streaming_ingest_client = KustoStreamingIngestClient(engine_kcsb)

client.execute(db_name, ".drop table {} ifexists".format(table_name))


@pytest.mark.run(order=1)
def test_csv_ingest_non_existing_table():
    csv_ingest_props = IngestionProperties(
        db_name,
        table_name,
        dataFormat=DataFormat.CSV,
        mapping=Helpers.create_deft_table_csv_mappings(),
        reportLevel=ReportLevel.FailuresAndSuccesses,
    )
    csv_file_path = os.path.join(os.getcwd(), "azure-kusto-ingest", "tests", "input", "dataset.csv")
    zipped_csv_file_path = os.path.join(os.getcwd(), "azure-kusto-ingest", "tests", "input", "dataset.csv.gz")

    for f in [csv_file_path, zipped_csv_file_path]:
        ingest_client.ingest_from_file(f, csv_ingest_props)

    successes = 0
    timeout = 60
    while successes != 2 and timeout > 0:
        while ingest_status_q.success.is_empty() and timeout > 0:
            time.sleep(1)
            timeout -= 1

        success_message = ingest_status_q.success.pop()

        assert success_message[0].Database == db_name
        assert success_message[0].Table == table_name

        successes += 1

    assert successes == 2
    # TODO: status queues only mark ingestion was successful, but takes time for data to become available
    time.sleep(20)
    response = client.execute(db_name, "{} | count".format(table_name))
    for row in response.primary_results[0]:
        assert int(row["Count"]) == 20, "{0} | count = {1}".format(table_name, text_type(row["Count"]))


json_file_path = os.path.join(os.getcwd(), "azure-kusto-ingest", "tests", "input", "dataset.json")
zipped_json_file_path = os.path.join(os.getcwd(), "azure-kusto-ingest", "tests", "input", "dataset.jsonz.gz")


@pytest.mark.run(order=2)
def test_json_ingest_existing_table():
    json_ingestion_props = IngestionProperties(
        db_name,
        table_name,
        dataFormat=DataFormat.JSON,
        mapping=Helpers.create_deft_table_json_mappings(),
        reportLevel=ReportLevel.FailuresAndSuccesses,
    )

    for f in [json_file_path, zipped_json_file_path]:
        ingest_client.ingest_from_file(f, json_ingestion_props)

    successes = 0
    timeout = 60

    while successes != 2 and timeout > 0:
        while ingest_status_q.success.is_empty() and timeout > 0:
            time.sleep(1)
            timeout -= 1

        success_message = ingest_status_q.success.pop()

        assert success_message[0].Database == db_name
        assert success_message[0].Table == table_name

        successes += 1

    assert successes == 2
    # TODO: status queues only mark ingestion was successful, but takes time for data to become available
    time.sleep(20)
    response = client.execute(db_name, "{} | count".format(table_name))
    for row in response.primary_results[0]:
        assert int(row["Count"]) == 24, "{0} | count = {1}".format(table_name, text_type(row["Count"]))


@pytest.mark.run(order=3)
def test_ingest_complicated_props():
    # Test ingest with complicated ingestion properties
    validation_policy = ValidationPolicy(
        validationOptions=ValidationOptions.ValidateCsvInputConstantColumns,
        validationImplications=ValidationImplications.Fail,
    )
    json_ingestion_props = IngestionProperties(
        db_name,
        table_name,
        dataFormat=DataFormat.JSON,
        mapping=Helpers.create_deft_table_json_mappings(),
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
    source_ids = ["{}".format(fd.source_id) for fd in fds]

    for fd in fds:
        ingest_client.ingest_from_file(fd, json_ingestion_props)

    successes = 0
    timeout = 60
    while successes != 2 and timeout > 0:
        while ingest_status_q.success.is_empty() and timeout > 0:
            time.sleep(1)
            timeout -= 1

        success_message = ingest_status_q.success.pop()
        if success_message[0].IngestionSourceId in source_ids:
            assert success_message[0].Database == db_name
            assert success_message[0].Table == table_name

            successes += 1

    assert successes == 2
    # TODO: status queues only mark ingestion was successful, but takes time for data to become available
    time.sleep(20)
    response = client.execute(db_name, "{} | count".format(table_name))
    for row in response.primary_results[0]:
        assert int(row["Count"]) == 28, "{0} | count = {1}".format(table_name, text_type(row["Count"]))


@pytest.mark.run(order=4)
def test_json_ingestion_ingest_by_tag():
    json_ingestion_props = IngestionProperties(
        db_name,
        table_name,
        dataFormat=DataFormat.JSON,
        mapping=Helpers.create_deft_table_json_mappings(),
        ingestIfNotExists=["ingestByTag"],
        reportLevel=ReportLevel.FailuresAndSuccesses,
        dropByTags=["drop", "drop-by"],
    )
    ops = []
    for f in [json_file_path, zipped_json_file_path]:
        ingest_client.ingest_from_file(f, json_ingestion_props)

    successes = 0
    timeout = 60
    while successes != 2 and timeout > 0:
        while ingest_status_q.success.is_empty() and timeout > 0:
            time.sleep(1)
            timeout -= 1

        success_message = ingest_status_q.success.pop()

        assert success_message[0].Database == db_name
        assert success_message[0].Table == table_name

        successes += 1

    assert successes == 2
    # TODO: status queues only mark ingestion was successful, but takes time for data to become available
    time.sleep(20)
    response = client.execute(db_name, "{} | count".format(table_name))
    for row in response.primary_results[0]:
        assert int(row["Count"]) == 28, "{0} | count = {1}".format(table_name, text_type(row["Count"]))


@pytest.mark.run(order=5)
def test_tsv_ingestion_csv_mapping():
    tsv_ingestion_props = IngestionProperties(
        db_name,
        table_name,
        dataFormat=DataFormat.TSV,
        mapping=Helpers.create_deft_table_csv_mappings(),
        reportLevel=ReportLevel.FailuresAndSuccesses,
    )
    tsv_file_path = os.path.join(os.getcwd(), "azure-kusto-ingest", "tests", "input", "dataset.tsv")

    ingest_client.ingest_from_file(tsv_file_path, tsv_ingestion_props)

    successes = 0
    timeout = 60
    while successes != 1 and timeout > 0:
        while ingest_status_q.success.is_empty() and timeout > 0:
            time.sleep(1)
            timeout -= 1

        success_message = ingest_status_q.success.pop()

        assert success_message[0].Table == table_name
        assert success_message[0].Database == db_name

        successes += 1

    assert successes == 1
    # TODO: status queues only mark ingestion was successful, but takes time for data to become available
    time.sleep(20)
    response = client.execute(db_name, "{} | count".format(table_name))
    for row in response.primary_results[0]:
        assert int(row["Count"]) == 38, "{0} | count = {1}".format(table_name, text_type(row["Count"]))


@pytest.mark.run(order=6)
def test_streaming_ingest_from_opened_file():
    current_dir = os.getcwd()
    path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.csv"]
    missing_path_parts = []
    for path_part in path_parts:
        if path_part not in current_dir:
            missing_path_parts.append(path_part)

    file_path = os.path.join(current_dir, *missing_path_parts)
    stream = open(file_path, "r")
    ingestion_properties = IngestionProperties(database=db_name, table=table_name, dataFormat=DataFormat.CSV)
    ingest_client.ingest_from_stream(stream, ingestion_properties=ingestion_properties)


@pytest.mark.run(order=7)
def test_streaming_ingest_form_csv_file():
    current_dir = os.getcwd()
    path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.csv"]
    missing_path_parts = []
    for path_part in path_parts:
        if path_part not in current_dir:
            missing_path_parts.append(path_part)

    file_path = os.path.join(current_dir, *missing_path_parts)

    ingestion_properties = IngestionProperties(database=db_name, table=table_name, dataFormat=DataFormat.CSV)
    ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)

    path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.csv.gz"]
    missing_path_parts = []
    for path_part in path_parts:
        if path_part not in current_dir:
            missing_path_parts.append(path_part)

    file_path = os.path.join(current_dir, *missing_path_parts)

    ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)


@pytest.mark.run(order=8)
def test_streaming_ingest_from_json_no_mapping():
    ingestion_properties = IngestionProperties(database=db_name, table=table_name, dataFormat=DataFormat.JSON)
    try:
        current_dir = os.getcwd()
        path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.json"]
        missing_path_parts = []
        for path_part in path_parts:
            if path_part not in current_dir:
                missing_path_parts.append(path_part)

        file_path = os.path.join(current_dir, *missing_path_parts)
        ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)
    except KustoMissingMappingReferenceError:
        pass

    try:
        byte_sequence = b'{"rownumber": 0, "rowguid": "00000000-0000-0000-0001-020304050607", "xdouble": 0.0, "xfloat": 0.0, "xbool": 0, "xint16": 0, "xint32": 0, "xint64": 0, "xunit8": 0, "xuint16": 0, "xunit32": 0, "xunit64": 0, "xdate": "2014-01-01T01:01:01Z", "xsmalltext": "Zero", "xtext": "Zero", "xnumberAsText": "0", "xtime": "00:00:00", "xtextWithNulls": null, "xdynamicWithNulls": ""}'
        bytes_stream = io.BytesIO(byte_sequence)
        ingest_client.ingest_from_stream(bytes_stream, ingestion_properties=ingestion_properties)
    except KustoMissingMappingReferenceError:
        pass


@pytest.mark.run(order=9)
def test_streaming_ingest_from_json_file():
    current_dir = os.getcwd()
    path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.json"]
    missing_path_parts = []
    for path_part in path_parts:
        if path_part not in current_dir:
            missing_path_parts.append(path_part)

    file_path = os.path.join(current_dir, *missing_path_parts)
    ingestion_properties = IngestionProperties(
        database=db_name, table=table_name, dataFormat=DataFormat.JSON, mappingReference="JsonMapping"
    )
    ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)

    path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.jsonz.gz"]
    missing_path_parts = []
    for path_part in path_parts:
        if path_part not in current_dir:
            missing_path_parts.append(path_part)

    file_path = os.path.join(current_dir, *missing_path_parts)

    ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)


@pytest.mark.run(order=10)
def test_streaming_ingest_from_io_streams():
    ingestion_properties = IngestionProperties(database=db_name, table=table_name, dataFormat=DataFormat.CSV)
    byte_sequence = b'0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,"Zero",0,00:00:00,,null'
    bytes_stream = io.BytesIO(byte_sequence)
    ingest_client.ingest_from_stream(bytes_stream, ingestion_properties=ingestion_properties)

    str_sequence = '0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,"Zero",0,00:00:00,,null'
    str_stream = io.StringIO(str_sequence)
    ingest_client.ingest_from_stream(str_stream, ingestion_properties=ingestion_properties)

    byte_sequence = b'{"rownumber": 0, "rowguid": "00000000-0000-0000-0001-020304050607", "xdouble": 0.0, "xfloat": 0.0, "xbool": 0, "xint16": 0, "xint32": 0, "xint64": 0, "xunit8": 0, "xuint16": 0, "xunit32": 0, "xunit64": 0, "xdate": "2014-01-01T01:01:01Z", "xsmalltext": "Zero", "xtext": "Zero", "xnumberAsText": "0", "xtime": "00:00:00", "xtextWithNulls": null, "xdynamicWithNulls": ""}'
    bytes_stream = io.BytesIO(byte_sequence)
    ingestion_properties.format = DataFormat.JSON

    ingestion_properties.mapping_reference = "JsonMapping"
    ingest_client.ingest_from_stream(bytes_stream, ingestion_properties=ingestion_properties)

    str_sequence = u'{"rownumber": 0, "rowguid": "00000000-0000-0000-0001-020304050607", "xdouble": 0.0, "xfloat": 0.0, "xbool": 0, "xint16": 0, "xint32": 0, "xint64": 0, "xunit8": 0, "xuint16": 0, "xunit32": 0, "xunit64": 0, "xdate": "2014-01-01T01:01:01Z", "xsmalltext": "Zero", "xtext": "Zero", "xnumberAsText": "0", "xtime": "00:00:00", "xtextWithNulls": null, "xdynamicWithNulls": ""}'
    str_stream = io.StringIO(str_sequence)
    ingest_client.ingest_from_stream(str_stream, ingestion_properties=ingestion_properties)

    byte_sequence = (
        b'0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,"Zero",0,00:00:00,,null'
        * 600000
    )
    bytes_stream = io.BytesIO(byte_sequence)

    try:
        ingest_client.ingest_from_stream(bytes_stream, ingestion_properties=ingestion_properties)
    except KustoStreamMaxSizeExceededError:
        pass


@pytest.mark.run(order=11)
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
    rows = [
        [
            0,
            "00000000-0000-0000-0001-020304050607",
            0.0,
            0.0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            "2014-01-01T01:01:01Z",
            "Zero",
            "Zero",
            "0",
            "00:00:00",
            None,
            "",
        ]
    ]
    df = DataFrame(data=rows, columns=fields)
    ingestion_properties = IngestionProperties(database=db_name, table=table_name, dataFormat=DataFormat.CSV)
    ingest_client.ingest_from_dataframe(df, ingestion_properties)
