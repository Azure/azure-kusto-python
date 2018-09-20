"""E2E tests for ingest_client."""
import pytest
import time
import os
from six import text_type

from azure.kusto.data.request import KustoClient
from azure.kusto.ingest.status import KustoIngestStatusQueues
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

# TODO: change this file to use pytes as runner


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


client = KustoClient("https://toshetah.kusto.windows.net")
ingest_client = KustoIngestClient("https://ingest-toshetah.kusto.windows.net")
ingest_status_q = KustoIngestStatusQueues(ingest_client)
client.execute("PythonTest", ".drop table Deft ifexists")


@pytest.mark.run(order=1)
def test_csv_ingest_non_existing_table():
    csv_ingest_props = IngestionProperties(
        "PythonTest",
        "Deft",
        dataFormat=DataFormat.csv,
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

        assert success_message[0].Database == "PythonTest"
        assert success_message[0].Table == "Deft"

        successes += 1

    assert successes == 2
    # TODO: status queues only mark ingestion was successful, but takes time for data to become available
    time.sleep(20)
    response = client.execute("PythonTest", "Deft | count")
    for row in response.primary_results[0]:
        assert int(row["Count"]) == 20, "Deft | count = " + text_type(row["Count"])


json_file_path = os.path.join(os.getcwd(), "azure-kusto-ingest", "tests", "input", "dataset.json")
zipped_json_file_path = os.path.join(os.getcwd(), "azure-kusto-ingest", "tests", "input", "dataset.jsonz.gz")


@pytest.mark.run(order=2)
def test_json_ingest_exisiting_table():
    json_ingestion_props = IngestionProperties(
        "PythonTest",
        "Deft",
        dataFormat=DataFormat.json,
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

        assert success_message[0].Database == "PythonTest"
        assert success_message[0].Table == "Deft"

        successes += 1

    assert successes == 2
    # TODO: status queues only mark ingestion was successful, but takes time for data to become available
    time.sleep(20)
    response = client.execute("PythonTest", "Deft | count")
    for row in response.primary_results[0]:
        assert int(row["Count"]) == 24, "Deft | count = " + text_type(row["Count"])


@pytest.mark.run(order=3)
def test_ingest_complicated_props():
    # Test ingest with complicated ingestion properties
    validation_policy = ValidationPolicy(
        validationOptions=ValidationOptions.ValidateCsvInputConstantColumns,
        validationImplications=ValidationImplications.Fail,
    )
    json_ingestion_props = IngestionProperties(
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
        reportMethod=ReportMethod.Queue,
        validationPolicy=validation_policy,
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

        assert success_message[0].Database == "PythonTest"
        assert success_message[0].Table == "Deft"

        successes += 1

    assert successes == 2
    # TODO: status queues only mark ingestion was successful, but takes time for data to become available
    time.sleep(20)
    response = client.execute("PythonTest", "Deft | count")
    for row in response.primary_results[0]:
        assert int(row["Count"]) == 28, "Deft | count = " + str(row["Count"])


@pytest.mark.run(order=4)
def test_json_ingestion_ingest_by_tag():
    json_ingestion_props = IngestionProperties(
        "PythonTest",
        "Deft",
        dataFormat=DataFormat.json,
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

        assert success_message[0].Database == "PythonTest"
        assert success_message[0].Table == "Deft"

        successes += 1

    assert successes == 2
    # TODO: status queues only mark ingestion was successful, but takes time for data to become available
    time.sleep(20)
    response = client.execute("PythonTest", "Deft | count")
    for row in response.primary_results[0]:
        assert int(row["Count"]) == 28, "Deft | count = " + text_type(row["Count"])


@pytest.mark.run(order=5)
def test_tsv_ingestion_csv_mapping():
    tsv_ingestion_props = IngestionProperties(
        "PythonTest",
        "Deft",
        dataFormat=DataFormat.tsv,
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

        assert success_message[0].Table == "Deft"
        assert success_message[0].Database == "PythonTest"

        successes += 1

    assert successes == 1
    # TODO: status queues only mark ingestion was successful, but takes time for data to become available
    time.sleep(20)
    response = client.execute("PythonTest", "Deft | count")
    for row in response.primary_results[0]:
        assert int(row["Count"]) == 38, print("Deft | count = " + text_type(row["Count"]))
