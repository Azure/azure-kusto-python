"""
Functional tests of the client.
"""

import json
import unittest
from dateutil.tz.tz import tzutc
from datetime import datetime, timedelta

from azure.kusto.data import KustoResponse

# Sample response against all tests should be run
RESPONSE_TEXT = """
{
   "Tables":[
      {
         "Columns":[
            {
               "ColumnName":"Timestamp",
               "DataType":"DateTime"
            },
            {
               "ColumnName":"Name",
               "DataType":"String"
            },
            {
               "ColumnName":"Altitude",
               "DataType":"Int64"
            },
            {
               "ColumnName":"Temperature",
               "DataType":"Double"
            },
            {
               "ColumnName":"IsFlying",
               "DataType":"SByte"
            },
            {
               "ColumnName":"TimeFlying",
               "DataType":"TimeSpan"
            }
         ],
         "TableName":"Table_0",
         "Rows":[
            [
               "2016-06-06T15:35:00Z",
               "foo",
               101,
               3.14,
               false,
               "4.01:02:03.567"
            ],
            [
               "2016-06-07T16:00:00Z",
               "bar",
               555,
               2.71,
               true,
               "00:00:00"
            ],
            [
               null,
               null,
               null,
               null,
               null,
               null
            ]
         ]
      },
      {
         "Columns":[
            {
               "ColumnName":"Value",
               "DataType":"String"
            }
         ],
         "TableName":"Table_1",
         "Rows":[
            [
               {"Visualization":"table","Title":"","Accumulate":false,"IsQuerySorted":false,"Annotation":""}
            ]
         ]
      }
   ]
}
"""


class FunctionalTests(unittest.TestCase):
    """
    E2E tests, from part where response is received from Kusto. This does not include access to actual Kusto cluster.
    """

    def test_valid_response(self):
        """ Tests on happy path, validating response and iterations over it """
        response = KustoResponse(json.loads(RESPONSE_TEXT))
        # Test that basic iteration works
        row_count = 0
        for _ in response.iter_all():
            row_count = row_count + 1
        self.assertEqual(row_count, 3)

        self.assertEqual(2, response.get_table_count())
        # Test access by index and by column name
        for row in response.iter_all():
            self.assertEqual(row[0], row['Timestamp'])
            self.assertEqual(row[1], row['Name'])
            self.assertEqual(row[2], row['Altitude'])
            self.assertEqual(row[3], row['Temperature'])
            self.assertEqual(row[4], row['IsFlying'])
            self.assertEqual(row[5], row['TimeFlying'])

        # Test all types
        for row in response.iter_all():
            if row[0] is not None:
                self.assertEqual(type(row[0]), datetime)
                try:
                    self.assertEqual(type(row[1]), str)
                except AssertionError:
                    self.assertEqual(type(row[1]), unicode)
                self.assertEqual(type(row[2]), int)
                self.assertEqual(type(row[3]), float)
                self.assertEqual(type(row[4]), bool)
                self.assertEqual(type(row[5]), timedelta)
        # Test actual values
        rows = list(response.iter_all())
        self.assertEqual(datetime(2016, 6, 6, 15, 35, tzinfo=tzutc()), rows[0]['Timestamp'])
        self.assertEqual('foo', rows[0]['Name'])
        self.assertEqual(101, rows[0]['Altitude'])
        self.assertAlmostEqual(3.14, rows[0]['Temperature'], 2)
        self.assertEqual(False, rows[0]['IsFlying'])
        self.assertEqual(timedelta(days=4, hours=1, minutes=2, seconds=3, milliseconds=567), rows[0]['TimeFlying'])

        self.assertEqual(datetime(2016, 6, 7, 16, tzinfo=tzutc()), rows[1]['Timestamp'])
        self.assertEqual('bar', rows[1]['Name'])
        self.assertEqual(555, rows[1]['Altitude'])
        self.assertAlmostEqual(2.71, rows[1]['Temperature'], 2)
        self.assertEqual(True, rows[1]['IsFlying'])
        self.assertEqual(timedelta(), rows[1]['TimeFlying'])

        self.assertIsNone(rows[2]['Timestamp'])
        self.assertIsNone(rows[2]['Name'])
        self.assertIsNone(rows[2]['Altitude'])
        self.assertIsNone(rows[2]['Temperature'], 2)
        self.assertIsNone(rows[2]['IsFlying'])
        self.assertIsNone(rows[2]['TimeFlying'])
        # Test second table
        rows = list(response.iter_all(1))
        self.assertEqual(1, len(rows))

    def test_invalid_table(self):
        """ Tests calling of table with index that doesn't exists """
        response = KustoResponse(json.loads(RESPONSE_TEXT))
        self.assertRaises(IndexError, response.iter_all, 4)
        self.assertRaises(IndexError, response.iter_all, -3)

    def test_columndont_exist(self):
        """ Tests accessing column that doesn't exists """
        response = KustoResponse(json.loads(RESPONSE_TEXT))
        row = list(response.iter_all())[0]
        self.assertRaises(IndexError, row.__getitem__, 10)
        self.assertRaises(KeyError, row.__getitem__, 'NonexistentColumn')

    def test_interating_after_end(self):
        """ Tests StopIteration is raised when the response ends. """
        response = KustoResponse(json.loads(RESPONSE_TEXT))
        iterator = response.iter_all()
        iterator.__next__()
        iterator.__next__()
        iterator.__next__()
        self.assertRaises(StopIteration, iterator.__next__)
