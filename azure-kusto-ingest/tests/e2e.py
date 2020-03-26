"""E2E tests for ingest_client."""
import pytest
import time
import os
import uuid
import io

from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder
from azure.kusto.ingest.status import KustoIngestStatusQueues
from azure.kusto.ingest import (
    KustoIngestClient,
    KustoStreamingIngestClient,
    IngestionProperties,
    DataFormat,
    ValidationPolicy,
    ValidationOptions,
    ValidationImplications,
    ReportLevel,
    ReportMethod,
    FileDescriptor,
    KustoMissingMappingReferenceError,
    ColumnMapping,
    IngestionMappingType,
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
        mappings.append(ColumnMapping(columnName="rownumber", columnType="int", ordinal=0))
        mappings.append(ColumnMapping(columnName="rowguid", columnType="string", ordinal=1))
        mappings.append(ColumnMapping(columnName="xdouble", columnType="real", ordinal=2))
        mappings.append(ColumnMapping(columnName="xfloat", columnType="real", ordinal=3))
        mappings.append(ColumnMapping(columnName="xbool", columnType="bool", ordinal=4))
        mappings.append(ColumnMapping(columnName="xint16", columnType="int", ordinal=5))
        mappings.append(ColumnMapping(columnName="xint32", columnType="int", ordinal=6))
        mappings.append(ColumnMapping(columnName="xint64", columnType="long", ordinal=7))
        mappings.append(ColumnMapping(columnName="xuint8", columnType="long", ordinal=8))
        mappings.append(ColumnMapping(columnName="xuint16", columnType="long", ordinal=9))
        mappings.append(ColumnMapping(columnName="xuint32", columnType="long", ordinal=10))
        mappings.append(ColumnMapping(columnName="xuint64", columnType="long", ordinal=11))
        mappings.append(ColumnMapping(columnName="xdate", columnType="datetime", ordinal=12))
        mappings.append(ColumnMapping(columnName="xsmalltext", columnType="string", ordinal=13))
        mappings.append(ColumnMapping(columnName="xtext", columnType="string", ordinal=14))
        mappings.append(ColumnMapping(columnName="xnumberAsText", columnType="string", ordinal=15))
        mappings.append(ColumnMapping(columnName="xtime", columnType="timespan", ordinal=16))
        mappings.append(ColumnMapping(columnName="xtextWithNulls", columnType="string", ordinal=17))
        mappings.append(ColumnMapping(columnName="xdynamicWithNulls", columnType="dynamic", ordinal=18))
        return mappings

    @staticmethod
    def create_deft_table_json_mappings():
        """A method to define json mappings to deft table."""
        mappings = list()
        mappings.append(ColumnMapping(columnName="rownumber", path="$.rownumber", columnType="int"))
        mappings.append(ColumnMapping(columnName="rowguid", path="$.rowguid", columnType="string"))
        mappings.append(ColumnMapping(columnName="xdouble", path="$.xdouble", columnType="real"))
        mappings.append(ColumnMapping(columnName="xfloat", path="$.xfloat", columnType="real"))
        mappings.append(ColumnMapping(columnName="xbool", path="$.xbool", columnType="bool"))
        mappings.append(ColumnMapping(columnName="xint16", path="$.xint16", columnType="int"))
        mappings.append(ColumnMapping(columnName="xint32", path="$.xint32", columnType="int"))
        mappings.append(ColumnMapping(columnName="xint64", path="$.xint64", columnType="long"))
        mappings.append(ColumnMapping(columnName="xuint8", path="$.xuint8", columnType="long"))
        mappings.append(ColumnMapping(columnName="xuint16", path="$.xuint16", columnType="long"))
        mappings.append(ColumnMapping(columnName="xuint32", path="$.xuint32", columnType="long"))
        mappings.append(ColumnMapping(columnName="xuint64", path="$.xuint64", columnType="long"))
        mappings.append(ColumnMapping(columnName="xdate", path="$.xdate", columnType="datetime"))
        mappings.append(ColumnMapping(columnName="xsmalltext", path="$.xsmalltext", columnType="string"))
        mappings.append(ColumnMapping(columnName="xtext", path="$.xtext", columnType="string"))
        mappings.append(ColumnMapping(columnName="xnumberAsText", path="$.xnumberAsText", columnType="string"))
        mappings.append(ColumnMapping(columnName="xtime", path="$.xtime", columnType="timespan"))
        mappings.append(ColumnMapping(columnName="xtextWithNulls", path="$.xtextWithNulls", columnType="string"))
        mappings.append(ColumnMapping(columnName="xdynamicWithNulls", path="$.xdynamicWithNulls", columnType="dynamic"))
        return mappings


cluster = "Dadubovs1.westus"  # "toshetah"
db_name = "TestingDatabase"  # "PythonTest"
table_name = "Deft"


engine_kcsb = KustoConnectionStringBuilder.with_aad_device_authentication("https://{}.kusto.windows.net".format(cluster))
dm_kcsb = KustoConnectionStringBuilder.with_aad_device_authentication("https://ingest-{}.kusto.windows.net".format(cluster))
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
        ingestionMapping=Helpers.create_deft_table_csv_mappings(),
        ingestionMappingType=IngestionMappingType.CSV,
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
        assert int(row["Count"]) == 20, "{0} | count = {1}".format(table_name, str(row["Count"]))


json_file_path = os.path.join(os.getcwd(), "azure-kusto-ingest", "tests", "input", "dataset.json")
zipped_json_file_path = os.path.join(os.getcwd(), "azure-kusto-ingest", "tests", "input", "dataset.jsonz.gz")


@pytest.mark.run(order=2)
def test_json_ingest_existing_table():
    json_ingestion_props = IngestionProperties(
        db_name, table_name, dataFormat=DataFormat.JSON, mapping=Helpers.create_deft_table_json_mappings(), reportLevel=ReportLevel.FailuresAndSuccesses
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
        assert int(row["Count"]) == 24, "{0} | count = {1}".format(table_name, str(row["Count"]))


@pytest.mark.run(order=3)
def test_ingest_complicated_props():
    # Test ingest with complicated ingestion properties
    validation_policy = ValidationPolicy(
        validationOptions=ValidationOptions.ValidateCsvInputConstantColumns, validationImplications=ValidationImplications.Fail
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
        assert int(row["Count"]) == 28, "{0} | count = {1}".format(table_name, str(row["Count"]))


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
        assert int(row["Count"]) == 28, "{0} | count = {1}".format(table_name, str(row["Count"]))


@pytest.mark.run(order=5)
def test_tsv_ingestion_csv_mapping():
    tsv_ingestion_props = IngestionProperties(
        db_name, table_name, dataFormat=DataFormat.TSV, mapping=Helpers.create_deft_table_csv_mappings(), reportLevel=ReportLevel.FailuresAndSuccesses
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
        assert int(row["Count"]) == 38, "{0} | count = {1}".format(table_name, str(row["Count"]))


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
    client.execute(db_name, ".alter table {} policy streamingingestion enable".format(table_name))
    streaming_ingest_client.ingest_from_stream(stream, ingestion_properties=ingestion_properties)


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
    streaming_ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)

    path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.csv.gz"]
    missing_path_parts = []
    for path_part in path_parts:
        if path_part not in current_dir:
            missing_path_parts.append(path_part)

    file_path = os.path.join(current_dir, *missing_path_parts)

    streaming_ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)


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
        streaming_ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)
    except KustoMissingMappingReferenceError:
        pass

    try:
        byte_sequence = b'{"rownumber": 0, "rowguid": "00000000-0000-0000-0001-020304050607", "xdouble": 0.0, "xfloat": 0.0, "xbool": 0, "xint16": 0, "xint32": 0, "xint64": 0, "xunit8": 0, "xuint16": 0, "xunit32": 0, "xunit64": 0, "xdate": "2014-01-01T01:01:01Z", "xsmalltext": "Zero", "xtext": "Zero", "xnumberAsText": "0", "xtime": "00:00:00", "xtextWithNulls": null, "xdynamicWithNulls": ""}'
        bytes_stream = io.BytesIO(byte_sequence)
        streaming_ingest_client.ingest_from_stream(bytes_stream, ingestion_properties=ingestion_properties)
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
    ingestion_properties = IngestionProperties(database=db_name, table=table_name, dataFormat=DataFormat.JSON, mappingReference="JsonMapping")
    streaming_ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)

    path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.jsonz.gz"]
    missing_path_parts = []
    for path_part in path_parts:
        if path_part not in current_dir:
            missing_path_parts.append(path_part)

    file_path = os.path.join(current_dir, *missing_path_parts)

    streaming_ingest_client.ingest_from_file(file_path, ingestion_properties=ingestion_properties)


@pytest.mark.run(order=10)
def test_streaming_ingest_from_io_streams():
    ingestion_properties = IngestionProperties(database=db_name, table=table_name, dataFormat=DataFormat.CSV)
    byte_sequence = b'0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,"Zero",0,00:00:00,,null'
    bytes_stream = io.BytesIO(byte_sequence)
    streaming_ingest_client.ingest_from_stream(bytes_stream, ingestion_properties=ingestion_properties)

    str_sequence = '0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,"Zero",0,00:00:00,,null'
    str_stream = io.StringIO(str_sequence)
    streaming_ingest_client.ingest_from_stream(str_stream, ingestion_properties=ingestion_properties)

    byte_sequence = b'{"rownumber": 0, "rowguid": "00000000-0000-0000-0001-020304050607", "xdouble": 0.0, "xfloat": 0.0, "xbool": 0, "xint16": 0, "xint32": 0, "xint64": 0, "xunit8": 0, "xuint16": 0, "xunit32": 0, "xunit64": 0, "xdate": "2014-01-01T01:01:01Z", "xsmalltext": "Zero", "xtext": "Zero", "xnumberAsText": "0", "xtime": "00:00:00", "xtextWithNulls": null, "xdynamicWithNulls": ""}'
    bytes_stream = io.BytesIO(byte_sequence)
    ingestion_properties.format = DataFormat.JSON

    ingestion_properties.ingestion_mapping_reference = "JsonMapping"
    streaming_ingest_client.ingest_from_stream(bytes_stream, ingestion_properties=ingestion_properties)

    str_sequence = u'{"rownumber": 0, "rowguid": "00000000-0000-0000-0001-020304050607", "xdouble": 0.0, "xfloat": 0.0, "xbool": 0, "xint16": 0, "xint32": 0, "xint64": 0, "xunit8": 0, "xuint16": 0, "xunit32": 0, "xunit64": 0, "xdate": "2014-01-01T01:01:01Z", "xsmalltext": "Zero", "xtext": "Zero", "xnumberAsText": "0", "xtime": "00:00:00", "xtextWithNulls": null, "xdynamicWithNulls": ""}'
    str_stream = io.StringIO(str_sequence)
    streaming_ingest_client.ingest_from_stream(str_stream, ingestion_properties=ingestion_properties)

    byte_sequence = b'0,00000000-0000-0000-0001-020304050607,0,0,0,0,0,0,0,0,0,0,2014-01-01T01:01:01.0000000Z,Zero,"Zero",0,00:00:00,,null' * 600000
    bytes_stream = io.BytesIO(byte_sequence)

    try:
        streaming_ingest_client.ingest_from_stream(bytes_stream, ingestion_properties=ingestion_properties)
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
    rows = [[0, "00000000-0000-0000-0001-020304050607", 0.0, 0.0, 0, 0, 0, 0, 0, 0, 0, 0, "2014-01-01T01:01:01Z", "Zero", "Zero", "0", "00:00:00", None, ""]]
    df = DataFrame(data=rows, columns=fields)
    ingestion_properties = IngestionProperties(database=db_name, table=table_name, dataFormat=DataFormat.CSV)
    streaming_ingest_client.ingest_from_dataframe(df, ingestion_properties)
