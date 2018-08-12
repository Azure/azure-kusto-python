"""Functional tests of the client."""

import json
import unittest
from six import text_type
from datetime import datetime, timedelta
from dateutil.tz.tz import tzutc

from azure.kusto.data._kusto_client import _KustoResponseDataSetV2

# Sample response against all tests should be run
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
		"ColumnName": "Timestamp",
		"ColumnType": "datetime"
	},
	{
		"ColumnName": "Name",
		"ColumnType": "string"
	},
	{
		"ColumnName": "Altitude",
		"ColumnType": "long"
	},
	{
		"ColumnName": "Temperature",
		"ColumnType": "real"
	},
	{
		"ColumnName": "IsFlying",
		"ColumnType": "bool"
	},
	{
		"ColumnName": "TimeFlying",
		"ColumnType": "timespan"
	}],
	"Rows": [["2016-06-06T15:35:00Z",
	"foo",
	101,
	3.14,
	false,
	3493235670000],
	["2016-06-07T16:00:00Z",
	"bar",
	555,
	2.71,
	true,
	0],
	[null,
	"",
	null,
	null,
	null,
	null]]
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


class FunctionalTests(unittest.TestCase):
    """E2E tests, from part where response is received from Kusto. This does not include access to actual Kusto cluster."""

    def test_valid_response(self):
        """Tests on happy path, validating response and iterations over it."""
        response = _KustoResponseDataSetV2(json.loads(RESPONSE_TEXT))
        # Test that basic iteration works
        self.assertEqual(len(response), 3)
        self.assertEqual(len(list(response.primary_results)), 3)
        table = list(response.tables[0])
        self.assertEqual(1, len(table))

        expected_table = [
            [
                datetime(2016, 6, 6, 15, 35, tzinfo=tzutc()),
                "foo",
                101,
                3.14,
                False,
                timedelta(days=4, hours=1, minutes=2, seconds=3, milliseconds=567),
            ],
            [datetime(2016, 6, 7, 16, tzinfo=tzutc()), "bar", 555, 2.71, True, timedelta()],
            [None, text_type(""), None, None, None, None],
        ]

        # Test access by index and by column name
        for row in response.primary_results:
            self.assertEqual(row[0], row["Timestamp"])
            self.assertEqual(row[1], row["Name"])
            self.assertEqual(row[2], row["Altitude"])
            self.assertEqual(row[3], row["Temperature"])
            self.assertEqual(row[4], row["IsFlying"])
            self.assertEqual(row[5], row["TimeFlying"])

            # Test all types
            self.assertEqual(type(row[0]), datetime if row[0] else type(None))
            self.assertEqual(type(row[1]), text_type)
            self.assertEqual(type(row[2]), int if row[2] else type(None))
            self.assertEqual(type(row[3]), float if row[3] else type(None))
            self.assertEqual(type(row[4]), bool if row[4] is not None else type(None))
            self.assertEqual(type(row[5]), timedelta if row[5] is not None else type(None))

        for i in range(0, len(response.primary_results)):
            row = response.primary_results[i]
            expected_row = expected_table[i]
            for j in range(0, len(row)):
                self.assertEqual(row[j], expected_row[j])

    def test_invalid_table(self):
        """Tests calling of table with index that doesn't exists."""
        response = _KustoResponseDataSetV2(json.loads(RESPONSE_TEXT))
        self.assertRaises(IndexError, response.__getitem__, 7)
        self.assertRaises(IndexError, response.__getitem__, -6)

    def test_column_dont_exist(self):
        """Tests accessing column that doesn't exists."""
        response = _KustoResponseDataSetV2(json.loads(RESPONSE_TEXT))
        row = response.primary_results[0]
        self.assertRaises(IndexError, row.__getitem__, 10)
        self.assertRaises(LookupError, row.__getitem__, "NonexistentColumn")

    def test_iterating_after_end(self):
        """Tests StopIteration is raised when the response ends."""
        response = _KustoResponseDataSetV2(json.loads(RESPONSE_TEXT))
        self.assertEqual(sum(1 for _ in response.primary_results), 3)
