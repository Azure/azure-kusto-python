"""Tests for KustoClient."""

import json
import os
import unittest
from datetime import datetime, timedelta

import pytest
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data.request import KustoClient, ClientRequestProperties
from azure.kusto.data.response import WellKnownDataSet
from dateutil.tz import UTC
from mock import patch
from aioresponses import aioresponses, CallbackResult
from .async_unittest import AsyncUnittest

PANDAS = False
try:
    import pandas

    PANDAS = True
except:
    pass

async_installed = False
try:
    import asgiref

    async_installed = True
except:
    pass


def mocked_requests_post(*args, **kwargs):
    """Mock to replace requests.Session.post"""

    class MockResponse:
        """Mock class for KustoResponse."""

        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.text = str(json_data)
            self.status_code = status_code
            self.headers = None

        def json(self):
            """Get json data from response."""
            return self.json_data

    if args[0] == "https://somecluster.kusto.windows.net/v2/rest/query":
        if "truncationmaxrecords" in kwargs["json"]["csl"]:
            if json.loads(kwargs["json"]["properties"])["Options"]["deferpartialqueryfailures"]:
                file_name = "query_partial_results_defer_is_true.json"
            else:
                file_name = "query_partial_results_defer_is_false.json"
        elif "Deft" in kwargs["json"]["csl"]:
            file_name = "deft.json"
        elif "print dynamic" in kwargs["json"]["csl"]:
            file_name = "dynamic.json"
        elif "take 0" in kwargs["json"]["csl"]:
            file_name = "zero_results.json"
        elif "PrimaryResultName" in kwargs["json"]["csl"]:
            file_name = "null_values.json"

        with open(os.path.join(os.path.dirname(__file__), "input", file_name), "r") as response_file:
            data = response_file.read()
        return MockResponse(json.loads(data), 200)

    elif args[0] == "https://somecluster.kusto.windows.net/v1/rest/mgmt":
        if kwargs["json"]["csl"] == ".show version":
            file_name = "versionshowcommandresult.json"
        else:
            file_name = "adminthenquery.json"
        with open(os.path.join(os.path.dirname(__file__), "input", file_name), "r") as response_file:
            data = response_file.read()
        return MockResponse(json.loads(data), 200)

    return MockResponse(None, 404)


DIGIT_WORDS = [str("Zero"), str("One"), str("Two"), str("Three"), str("Four"), str("Five"), str("Six"), str("Seven"), str("Eight"), str("Nine"), str("ten")]


class KustoClientMixin:
    HOST = "https://somecluster.kusto.windows.net"

    def _create_kusto_client(self):
        return KustoClient(self.HOST)

    @staticmethod
    def _assert_sanity_query_response(response):
        expected = {
            "rownumber": None,
            "rowguid": str(""),
            "xdouble": None,
            "xfloat": None,
            "xbool": None,
            "xint16": None,
            "xint32": None,
            "xint64": None,
            "xuint8": None,
            "xuint16": None,
            "xuint32": None,
            "xuint64": None,
            "xdate": None,
            "xsmalltext": str(""),
            "xtext": str(""),
            "xnumberAsText": str(""),
            "xtime": None,
            "xtextWithNulls": str(""),
            "xdynamicWithNulls": str(""),
        }
        for row in response.primary_results[0]:
            assert row["rownumber"] == expected["rownumber"]
            assert row["rowguid"] == expected["rowguid"]
            assert row["xdouble"] == expected["xdouble"]
            assert row["xfloat"] == expected["xfloat"]
            assert row["xbool"] == expected["xbool"]
            assert row["xint16"] == expected["xint16"]
            assert row["xint32"] == expected["xint32"]
            assert row["xint64"] == expected["xint64"]
            assert row["xuint8"] == expected["xuint8"]
            assert row["xuint16"] == expected["xuint16"]
            assert row["xuint32"] == expected["xuint32"]
            assert row["xuint64"] == expected["xuint64"]
            assert row["xdate"] == expected["xdate"]
            assert row["xsmalltext"] == expected["xsmalltext"]
            assert row["xtext"] == expected["xtext"]
            assert row["xnumberAsText"] == expected["xnumberAsText"]
            assert row["xtime"] == expected["xtime"]
            assert row["xtextWithNulls"] == expected["xtextWithNulls"]
            assert row["xdynamicWithNulls"] == expected["xdynamicWithNulls"]

            assert isinstance(row["rownumber"], type(expected["rownumber"]))
            assert isinstance(row["rowguid"], type(expected["rowguid"]))
            assert isinstance(row["xdouble"], type(expected["xdouble"]))
            assert isinstance(row["xfloat"], type(expected["xfloat"]))
            assert isinstance(row["xbool"], type(expected["xbool"]))
            assert isinstance(row["xint16"], type(expected["xint16"]))
            assert isinstance(row["xint32"], type(expected["xint32"]))
            assert isinstance(row["xint64"], type(expected["xint64"]))
            assert isinstance(row["xuint8"], type(expected["xuint8"]))
            assert isinstance(row["xuint16"], type(expected["xuint16"]))
            assert isinstance(row["xuint32"], type(expected["xuint32"]))
            assert isinstance(row["xuint64"], type(expected["xuint64"]))
            assert isinstance(row["xdate"], type(expected["xdate"]))
            assert isinstance(row["xsmalltext"], type(expected["xsmalltext"]))
            assert isinstance(row["xtext"], type(expected["xtext"]))
            assert isinstance(row["xnumberAsText"], type(expected["xnumberAsText"]))
            assert isinstance(row["xtime"], type(expected["xtime"]))
            assert isinstance(row["xtextWithNulls"], type(expected["xtextWithNulls"]))
            assert isinstance(row["xdynamicWithNulls"], type(expected["xdynamicWithNulls"]))

            expected["rownumber"] = 0 if expected["rownumber"] is None else expected["rownumber"] + 1
            expected["rowguid"] = str("0000000{0}-0000-0000-0001-020304050607".format(expected["rownumber"]))
            expected["xdouble"] = round(float(0) if expected["xdouble"] is None else expected["xdouble"] + 1.0001, 4)
            expected["xfloat"] = round(float(0) if expected["xfloat"] is None else expected["xfloat"] + 1.01, 2)
            expected["xbool"] = False if expected["xbool"] is None else not expected["xbool"]
            expected["xint16"] = 0 if expected["xint16"] is None else expected["xint16"] + 1
            expected["xint32"] = 0 if expected["xint32"] is None else expected["xint32"] + 1
            expected["xint64"] = 0 if expected["xint64"] is None else expected["xint64"] + 1
            expected["xuint8"] = 0 if expected["xuint8"] is None else expected["xuint8"] + 1
            expected["xuint16"] = 0 if expected["xuint16"] is None else expected["xuint16"] + 1
            expected["xuint32"] = 0 if expected["xuint32"] is None else expected["xuint32"] + 1
            expected["xuint64"] = 0 if expected["xuint64"] is None else expected["xuint64"] + 1
            expected["xdate"] = expected["xdate"] or datetime(2013, 1, 1, 1, 1, 1, 0, tzinfo=UTC)
            expected["xdate"] = expected["xdate"].replace(year=expected["xdate"].year + 1)
            expected["xsmalltext"] = DIGIT_WORDS[int(expected["xint16"])]
            expected["xtext"] = DIGIT_WORDS[int(expected["xint16"])]
            expected["xnumberAsText"] = str(expected["xint16"])

            next_time = (
                timedelta()
                if expected["xtime"] is None
                else (abs(expected["xtime"]) + timedelta(days=1, seconds=1, microseconds=1000)) * (-1) ** (expected["rownumber"] + 1)
            )

            # hacky tests - because time here is relative to previous row, after we pass a time where we have > 500 nanoseconds,
            # another microseconds digit is needed
            if expected["rownumber"] + 1 == 6:
                next_time += timedelta(microseconds=1)
            expected["xtime"] = next_time
            if expected["xint16"] > 0:
                expected["xdynamicWithNulls"] = {"rowId": expected["xint16"], "arr": [0, expected["xint16"]]}

    @staticmethod
    def _assert_sanity_control_command_response(response):
        assert len(response) == 1
        primary_table = response.primary_results[0]
        row_count = 0
        for _ in primary_table:
            row_count += 1
        assert row_count == 1
        result = primary_table[0]
        assert result["BuildVersion"] == "1.0.6693.14577"
        assert result["BuildTime"] == datetime(year=2018, month=4, day=29, hour=8, minute=5, second=54, tzinfo=UTC)
        assert result["ServiceType"] == "Engine"
        assert result["ProductVersion"] == "KustoMain_2018.04.29.5"

    def _assert_sanity_data_frame_response(self, data_frame):
        from pandas import DataFrame, Series
        from pandas.util.testing import assert_frame_equal

        self.assertEqual(len(data_frame.columns), 19)
        expected_dict = {
            "rownumber": Series([None, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]),
            "rowguid": Series(
                [
                    "",
                    "00000000-0000-0000-0001-020304050607",
                    "00000001-0000-0000-0001-020304050607",
                    "00000002-0000-0000-0001-020304050607",
                    "00000003-0000-0000-0001-020304050607",
                    "00000004-0000-0000-0001-020304050607",
                    "00000005-0000-0000-0001-020304050607",
                    "00000006-0000-0000-0001-020304050607",
                    "00000007-0000-0000-0001-020304050607",
                    "00000008-0000-0000-0001-020304050607",
                    "00000009-0000-0000-0001-020304050607",
                ],
                dtype=object,
            ),
            "xdouble": Series([None, 0.0, 1.0001, 2.0002, 3.0003, 4.0004, 5.0005, 6.0006, 7.0007, 8.0008, 9.0009]),
            "xfloat": Series([None, 0.0, 1.01, 2.02, 3.03, 4.04, 5.05, 6.06, 7.07, 8.08, 9.09]),
            "xbool": Series([None, False, True, False, True, False, True, False, True, False, True], dtype=bool),
            "xint16": Series([None, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]),
            "xint32": Series([None, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]),
            "xint64": Series([None, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]),
            "xuint8": Series([None, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]),
            "xuint16": Series([None, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]),
            "xuint32": Series([None, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]),
            "xuint64": Series([None, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]),
            "xdate": Series(
                [
                    pandas.to_datetime(None),
                    pandas.to_datetime("2014-01-01T01:01:01.0000000Z"),
                    pandas.to_datetime("2015-01-01T01:01:01.0000001Z"),
                    pandas.to_datetime("2016-01-01T01:01:01.0000002Z"),
                    pandas.to_datetime("2017-01-01T01:01:01.0000003Z"),
                    pandas.to_datetime("2018-01-01T01:01:01.0000004Z"),
                    pandas.to_datetime("2019-01-01T01:01:01.0000005Z"),
                    pandas.to_datetime("2020-01-01T01:01:01.0000006Z"),
                    pandas.to_datetime("2021-01-01T01:01:01.0000007Z"),
                    pandas.to_datetime("2022-01-01T01:01:01.0000008Z"),
                    pandas.to_datetime("2023-01-01T01:01:01.0000009Z"),
                ]
            ),
            "xsmalltext": Series(["", "Zero", "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine"], dtype=object),
            "xtext": Series(["", "Zero", "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine"], dtype=object),
            "xnumberAsText": Series(["", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"], dtype=object),
            "xtime": Series(
                [
                    "NaT",
                    0,
                    "1 days 00:00:01.0010001",
                    "-3 days 23:59:57.9979998",
                    "3 days 00:00:03.0030003",
                    "-5 days 23:59:55.9959996",
                    "5 days 00:00:05.0050005",
                    "-7 days 23:59:53.9939994",
                    "7 days 00:00:07.0070007",
                    "-9 days 23:59:51.9919992",
                    "9 days 00:00:09.0090009",
                ],
                dtype="timedelta64[ns]",
            ),
            "xtextWithNulls": Series(["", "", "", "", "", "", "", "", "", "", ""], dtype=object),
            "xdynamicWithNulls": Series(
                [
                    str(""),
                    str(""),
                    {"rowId": 1, "arr": [0, 1]},
                    {"rowId": 2, "arr": [0, 2]},
                    {"rowId": 3, "arr": [0, 3]},
                    {"rowId": 4, "arr": [0, 4]},
                    {"rowId": 5, "arr": [0, 5]},
                    {"rowId": 6, "arr": [0, 6]},
                    {"rowId": 7, "arr": [0, 7]},
                    {"rowId": 8, "arr": [0, 8]},
                    {"rowId": 9, "arr": [0, 9]},
                ],
                dtype=object,
            ),
        }
        columns = [
            "rownumber",
            "rowguid",
            "xdouble",
            "xfloat",
            "xbool",
            "xint16",
            "xint32",
            "xint64",
            "xuint8",
            "xuint16",
            "xuint32",
            "xuint64",
            "xdate",
            "xsmalltext",
            "xtext",
            "xnumberAsText",
            "xtime",
            "xtextWithNulls",
            "xdynamicWithNulls",
        ]
        expected_data_frame = DataFrame(expected_dict, columns=columns, copy=True)
        assert_frame_equal(data_frame, expected_data_frame)

    @staticmethod
    def _assert_partial_results_response(response):
        assert response.errors_count == 1
        assert "E_QUERY_RESULT_SET_TOO_LARGE" in response.get_exceptions()[0]
        assert len(response) == 3
        results = list(response.primary_results[0])
        assert len(results) == 5
        assert results[0]["x"] == 1

    @staticmethod
    def _assert_admin_then_query_response(response):
        assert response.errors_count == 0
        assert len(response) == 4
        results = list(response.primary_results[0])
        assert len(results) == 2
        assert response[0].table_kind == WellKnownDataSet.PrimaryResult
        assert response[1].table_kind == WellKnownDataSet.QueryProperties
        assert response[2].table_kind == WellKnownDataSet.QueryCompletionInformation
        assert response[3].table_kind == WellKnownDataSet.TableOfContents

    @staticmethod
    def _assert_dynamic_response(row):
        assert isinstance(row[0], int)
        assert row[0] == 123
        assert isinstance(row[1], str)
        assert row[1] == "123"
        assert isinstance(row[2], str)
        assert row[2] == "test bad json"
        assert row[3] is None
        assert isinstance(row[4], str)
        assert row[4] == '{"rowId":2,"arr":[0,2]}'
        assert isinstance(row[5], dict)
        assert row[5] == {"rowId": 2, "arr": [0, 2]}


class KustoClientTestsSync(unittest.TestCase, KustoClientMixin):
    """Tests class for KustoClient Sync API"""
    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_sanity_query(self, mock_post):
        """Test query V2."""
        client = self._create_kusto_client()
        response = client.execute_query("PythonTest", "Deft")
        self._assert_sanity_query_response(response)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_sanity_control_command(self, mock_post):
        """Tests contol command."""
        client = self._create_kusto_client()
        response = client.execute_mgmt("NetDefaultDB", ".show version")
        self._assert_sanity_control_command_response(response)

    @pytest.mark.skipif(not PANDAS, reason="requires pandas")
    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_sanity_data_frame(self, mock_post):
        """Tests KustoResponse to pandas.DataFrame."""
        client = self._create_kusto_client()
        response = client.execute_query("PythonTest", "Deft")
        data_frame = dataframe_from_result_table(response.primary_results[0])
        self._assert_sanity_data_frame_response(data_frame)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_partial_results(self, mock_post):
        """Tests partial results."""
        client = self._create_kusto_client()
        query = """set truncationmaxrecords = 5;
range x from 1 to 10 step 1"""
        properties = ClientRequestProperties()
        properties.set_option(ClientRequestProperties.results_defer_partial_query_failures_option_name, False)
        self.assertRaises(KustoServiceError, client.execute_query, "PythonTest", query, properties)
        properties.set_option(ClientRequestProperties.results_defer_partial_query_failures_option_name, True)
        response = client.execute_query("PythonTest", query, properties)
        self._assert_partial_results_response(response)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_admin_then_query(self, mock_post):
        """Tests admin then query."""
        client = self._create_kusto_client()
        query = ".show tables | project DatabaseName, TableName"
        response = client.execute_mgmt("PythonTest", query)
        self._assert_admin_then_query_response(response)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_dynamic(self, mock_post):
        """Tests dynamic responses."""
        client = self._create_kusto_client()
        query = """print dynamic(123), dynamic("123"), dynamic("test bad json"),"""
        """ dynamic(null), dynamic('{"rowId":2,"arr":[0,2]}'), dynamic({"rowId":2,"arr":[0,2]})"""
        row = client.execute_query("PythonTest", query).primary_results[0].rows[0]
        self._assert_dynamic_response(row)

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_empty_result(self, mock_post):
        """Tests dynamic responses."""
        client = self._create_kusto_client()
        query = """print 'a' | take 0"""
        response = client.execute_query("PythonTest", query)
        assert response.primary_results[0]

    @patch("requests.Session.post", side_effect=mocked_requests_post)
    def test_null_values_in_data(self, mock_post):
        """Tests response with null values in non nullable column types"""
        client = self._create_kusto_client()
        query = "PrimaryResultName"
        response = client.execute_query("PythonTest", query)

        assert response is not None


@pytest.mark.skipif(not async_installed, reason="requires async")
class KustoClientTestsAsync(AsyncUnittest, KustoClientMixin):
    """Tests class for KustoClient ASync API"""
    @staticmethod
    def _mock_callback(url, **kwargs):
        body = json.dumps(mocked_requests_post(str(url), **kwargs).json())
        return CallbackResult(status=200, body=body)

    def _mock_query(self, mocked):
        url = "{host}/v2/rest/query".format(host=self.HOST)
        mocked.post(url, callback=self._mock_callback)

    def _mock_mgmt(self, mocked):
        url = "{host}/v1/rest/mgmt".format(host=self.HOST)
        mocked.post(url, callback=self._mock_callback)

    @aioresponses()
    def test_sanity_query_async(self, mocked):
        """Async version of self.test_sanity_query"""
        self._mock_query(mocked)
        client = self._create_kusto_client()
        response = self.loop.run_until_complete(client.execute_query_async("PythonTest", "Deft"))
        self._assert_sanity_query_response(response)

    @aioresponses()
    def test_sanity_control_command_async(self, mocked):
        """Async version of self.test_sanity_control_command"""
        self._mock_mgmt(mocked)
        client = self._create_kusto_client()
        response = self.loop.run_until_complete(client.execute_mgmt_async("NetDefaultDB", ".show version"))
        self._assert_sanity_control_command_response(response)

    @pytest.mark.skipif(not PANDAS, reason="requires pandas")
    @aioresponses()
    def test_sanity_data_frame_async(self, mocked):
        """Async version of self.test_sanity_data_frame"""
        self._mock_query(mocked)
        client = self._create_kusto_client()
        response = self.loop.run_until_complete(client.execute_query_async("PythonTest", "Deft"))
        data_frame = dataframe_from_result_table(response.primary_results[0])
        self._assert_sanity_data_frame_response(data_frame)

    @aioresponses()
    def test_partial_results_async(self, mocked):
        """Async version of self.test_partial_results"""
        client = self._create_kusto_client()
        query = """set truncationmaxrecords = 5;
range x from 1 to 10 step 1"""
        properties = ClientRequestProperties()
        properties.set_option(ClientRequestProperties.results_defer_partial_query_failures_option_name, False)
        self._mock_query(mocked)
        with self.assertRaises(KustoServiceError):
            self.loop.run_until_complete(client.execute_query_async("PythonTest", query, properties))
        properties.set_option(ClientRequestProperties.results_defer_partial_query_failures_option_name, True)
        self._mock_query(mocked)
        response = self.loop.run_until_complete(client.execute_query_async("PythonTest", query, properties))
        self._assert_partial_results_response(response)

    @aioresponses()
    def test_admin_then_query_async(self, mocked):
        """Async version of self.test_admin_then_query"""
        self._mock_mgmt(mocked)
        client = self._create_kusto_client()
        query = ".show tables | project DatabaseName, TableName"
        response = self.loop.run_until_complete(client.execute_mgmt_async("PythonTest", query))
        self._assert_admin_then_query_response(response)

    @aioresponses()
    def test_dynamic_async(self, mocked):
        """Async version of self.test_dynamic"""
        self._mock_query(mocked)
        client = self._create_kusto_client()
        query = """print dynamic(123), dynamic("123"), dynamic("test bad json"),"""
        """ dynamic(null), dynamic('{"rowId":2,"arr":[0,2]}'), dynamic({"rowId":2,"arr":[0,2]})"""
        response = self.loop.run_until_complete(client.execute_query_async("PythonTest", query))
        row = response.primary_results[0].rows[0]
        self._assert_dynamic_response(row)

    @aioresponses()
    def test_empty_result_async(self, mocked):
        """Async version of self.test_empty_result"""
        self._mock_query(mocked)
        client = self._create_kusto_client()
        query = """print 'a' | take 0"""
        response = self.loop.run_until_complete(client.execute_query_async("PythonTest", query))
        assert response.primary_results[0]

    @aioresponses()
    def test_null_values_in_data_async(self, mocked):
        """Async version of self.test_null_values_in_data"""
        self._mock_query(mocked)
        client = self._create_kusto_client()
        query = "PrimaryResultName"
        response = self.loop.run_until_complete(client.execute_query_async("PythonTest", query))
        assert response is not None
