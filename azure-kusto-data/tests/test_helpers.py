# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import unittest
import json
import pytest

from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data.response import KustoResponseDataSetV2


PANDAS = False
try:
    import pandas
    import numpy

    PANDAS = True
except:
    pass


# Sample response containing conversion edge cases.
RESPONSE_TEXT = """
[{
    "FrameType": "DataSetHeader",
    "IsProgressive": false,
    "Version": "v2.0"
},
{
    "FrameType": "DataTable",
    "TableId": 0,
    "TableName": "@ExtendedProperties",
    "TableKind": "QueryProperties",
    "Columns": [{
        "ColumnName": "TableId",
        "ColumnType": "int"
    },
    {
        "ColumnName": "Key",
        "ColumnType": "string"
    },
    {
        "ColumnName": "Value",
        "ColumnType": "dynamic"
    }],
    "Rows": [[1,
    "Visualization",
    "{\\"Visualization\\":null,\\"Title\\":null,\\"XColumn\\":null,\\"Series\\":null,\\"YColumns\\":null,\\"XTitle\\":null,\\"YTitle\\":null,\\"XAxis\\":null,\\"YAxis\\":null,\\"Legend\\":null,\\"YSplit\\":null,\\"Accumulate\\":false,\\"IsQuerySorted\\":false,\\"Kind\\":null}"]]
},
{
    "FrameType": "DataTable",
    "TableId": 1,
    "TableName": "temp",
    "TableKind": "PrimaryResult",
    "Columns": [{
        "ColumnName": "RecordName",
        "ColumnType": "string"
    },
    {
        "ColumnName": "RecordTime",
        "ColumnType": "datetime"
    },
    {
        "ColumnName": "RecordOffset",
        "ColumnType": "timespan"
    },
    {
        "ColumnName": "RecordBool",
        "ColumnType": "bool"
    },
    {
        "ColumnName": "RecordInt",
        "ColumnType": "int"
    },
    {
        "ColumnName": "RecordReal",
        "ColumnType": "real"
    }],
    "Rows": [["now",
    "2021-12-22T11:43:00Z",
    0,
    true,
    5678,
    3.14159],
    ["earliest datetime",
    "0000-01-01T00:00:00Z",
    0,
    true,
    5678,
    NaN],
    ["latest datetime",
    "9999-12-31T23:59:59Z",
    0,
    true,
    5678,
    Infinity],
    ["earliest pandas datetime",
    "1677-09-21T00:12:43.145224193Z",
    0,
    true,
    5678,
    -Infinity],
    ["latest pandas datetime",
    "2262-04-11T23:47:16.854775807Z",
    0,
    true,
    5678,
    3.14159],
    ["timedelta ticks",
    "2021-12-22T11:43:00Z",
    600000000,
    true,
    5678,
    3.14159],
    ["timedelta string",
    "2021-12-22T11:43:00Z",
    "1.01:01:01.0",
    true,
    5678,
    3.14159],
    [null,
    "",
    0,
    false,
    0,
    0]]
},
{
    "FrameType": "DataTable",
    "TableId": 2,
    "TableName": "QueryCompletionInformation",
    "TableKind": "QueryCompletionInformation",
    "Columns": [{
        "ColumnName": "Timestamp",
        "ColumnType": "datetime"
    },
    {
        "ColumnName": "ClientRequestId",
        "ColumnType": "string"
    },
    {
        "ColumnName": "ActivityId",
        "ColumnType": "guid"
    },
    {
        "ColumnName": "SubActivityId",
        "ColumnType": "guid"
    },
    {
        "ColumnName": "ParentActivityId",
        "ColumnType": "guid"
    },
    {
        "ColumnName": "Level",
        "ColumnType": "int"
    },
    {
        "ColumnName": "LevelName",
        "ColumnType": "string"
    },
    {
        "ColumnName": "StatusCode",
        "ColumnType": "int"
    },
    {
        "ColumnName": "StatusCodeName",
        "ColumnType": "string"
    },
    {
        "ColumnName": "EventType",
        "ColumnType": "int"
    },
    {
        "ColumnName": "EventTypeName",
        "ColumnType": "string"
    },
    {
        "ColumnName": "Payload",
        "ColumnType": "string"
    }],
    "Rows": [["2018-05-01T09:32:38.916566Z",
    "unspecified;e8e72755-786b-4bdc-835d-ea49d63d09fd",
    "5935a050-e466-48a0-991d-0ec26bd61c7e",
    "8182b177-7a80-4158-aca8-ff4fd8e7d3f8",
    "6f3c1072-2739-461c-8aa7-3cfc8ff528a8",
    4,
    "Info",
    0,
    "S_OK (0)",
    4,
    "QueryInfo",
    "{\\"Count\\":1,\\"Text\\":\\"Querycompletedsuccessfully\\"}"],
    ["2018-05-01T09:32:38.916566Z",
    "unspecified;e8e72755-786b-4bdc-835d-ea49d63d09fd",
    "5935a050-e466-48a0-991d-0ec26bd61c7e",
    "8182b177-7a80-4158-aca8-ff4fd8e7d3f8",
    "6f3c1072-2739-461c-8aa7-3cfc8ff528a8",
    6,
    "Stats",
    0,
    "S_OK (0)",
    0,
    "QueryResourceConsumption",
    "{\\"ExecutionTime\\":0.0156222,\\"resource_usage\\":{\\"cache\\":{\\"memory\\":{\\"hits\\":13,\\"misses\\":0,\\"total\\":13},\\"disk\\":{\\"hits\\":0,\\"misses\\":0,\\"total\\":0}},\\"cpu\\":{\\"user\\":\\"00: 00: 00\\",\\"kernel\\":\\"00: 00: 00\\",\\"totalcpu\\":\\"00: 00: 00\\"},\\"memory\\":{\\"peak_per_node\\":16777312}},\\"dataset_statistics\\":[{\\"table_row_count\\":3,\\"table_size\\":191}]}"]]
},
{
    "FrameType": "DataSetCompletion",
    "HasErrors": false,
    "Cancelled": false
}]
"""


class TestDataFrameFromResultsTable(unittest.TestCase):
    """Tests the dataframe_from_result_table helper function"""

    @pytest.mark.skipif(not PANDAS, reason="requires pandas")
    def test_dataframe_from_result_table(self):
        """Test conversion of KustoResultTable to pandas.DataFrame, including fixes for certain column types"""
        response = KustoResponseDataSetV2(json.loads(RESPONSE_TEXT))
        df = dataframe_from_result_table(response.primary_results[0])

        assert df.iloc[0].RecordName == "now"
        assert type(df.iloc[0].RecordTime) is pandas._libs.tslibs.timestamps.Timestamp
        assert all(getattr(df.iloc[0].RecordTime, k) == v for k, v in {"year": 2021, "month": 12, "day": 22, "hour": 11, "minute": 43, "second": 00}.items())
        assert type(df.iloc[0].RecordBool) is numpy.bool_
        assert df.iloc[0].RecordBool == True
        assert type(df.iloc[0].RecordInt) is numpy.int64
        assert df.iloc[0].RecordInt == 5678
        assert type(df.iloc[0].RecordReal) is numpy.float64
        assert df.iloc[0].RecordReal == 3.14159

        # Kusto datetime(0000-01-01T00:00:00Z), which Pandas can't represent.
        assert df.iloc[1].RecordName == "earliest datetime"
        assert type(df.iloc[1].RecordTime) is pandas._libs.tslibs.nattype.NaTType
        assert type(df.iloc[1].RecordReal) is pandas._libs.missing.NAType

        # Kusto datetime(9999-12-31T23:59:59Z), which Pandas can't represent.
        assert df.iloc[2].RecordName == "latest datetime"
        assert type(df.iloc[2].RecordTime) is pandas._libs.tslibs.nattype.NaTType
        assert type(df.iloc[2].RecordReal) is numpy.float64
        assert df.iloc[2].RecordReal == numpy.inf

        # Pandas earliest datetime
        assert df.iloc[3].RecordName == "earliest pandas datetime"
        assert type(df.iloc[3].RecordTime) is pandas._libs.tslibs.timestamps.Timestamp
        assert df.iloc[3].RecordTime.tz_localize(None) == pandas._libs.tslibs.timestamps.Timestamp.min
        assert type(df.iloc[3].RecordReal) is numpy.float64
        assert df.iloc[3].RecordReal == -numpy.inf

        # Pandas latest datetime
        assert df.iloc[4].RecordName == "latest pandas datetime"
        assert type(df.iloc[4].RecordTime) is pandas._libs.tslibs.timestamps.Timestamp
        assert df.iloc[4].RecordTime.tz_localize(None) == pandas._libs.tslibs.timestamps.Timestamp.max

        # Kusto 600000000 ticks timedelta
        assert df.iloc[5].RecordName == "timedelta ticks"
        assert type(df.iloc[5].RecordTime) is pandas._libs.tslibs.timestamps.Timestamp
        assert type(df.iloc[5].RecordOffset) is pandas._libs.tslibs.timestamps.Timedelta
        assert df.iloc[5].RecordOffset == pandas.to_timedelta("00:01:00")

        # Kusto timedelta(1.01:01:01.0) == 
        assert df.iloc[6].RecordName == "timedelta string"
        assert type(df.iloc[6].RecordTime) is pandas._libs.tslibs.timestamps.Timestamp
        assert type(df.iloc[6].RecordOffset) is pandas._libs.tslibs.timestamps.Timedelta
        assert df.iloc[6].RecordOffset == pandas.to_timedelta("1 days 01:01:01")
