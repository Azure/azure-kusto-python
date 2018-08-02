"""E2E class for KustoClient."""

import os
import json
import unittest
from six import text_type
from datetime import datetime, timedelta
from mock import patch
from dateutil.tz.tz import tzutc
from pandas import DataFrame, Series
from pandas.util.testing import assert_frame_equal

from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.data.net import KustoClient
from azure.kusto.data.exceptions import KustoServiceError

# This method will be used by the mock to replace KustoClient._acquire_token
def mocked_aad_helper(*args, **kwargs):
    """A class to mock _aad_halper."""
    return None


# This method will be used by the mock to replace requests.post
def mocked_requests_post(*args, **kwargs):
    """A class to mock requests package."""

    class MockResponse:
        """A class to mock KustoResponse."""

        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.text = text_type(json_data)
            self.status_code = status_code
            self.headers = None

        def json(self):
            """Get json data from response."""
            return self.json_data

    if args[0] == "https://somecluster.kusto.windows.net/v2/rest/query":
        if "truncationmaxrecords" in kwargs["json"]["csl"]:
            file_name = "querypartialresults.json"
        elif "Deft" in kwargs["json"]["csl"]:
            file_name = "deft.json"
        return MockResponse(
            json.loads(
                open(os.path.join(os.path.dirname(__file__), "input", file_name), "r").read()
            ),
            200,
        )
    elif args[0] == text_type("https://somecluster.kusto.windows.net/v1/rest/mgmt"):
        return MockResponse(
            json.loads(
                open(
                    os.path.join(
                        os.path.dirname(__file__), "input", "versionshowcommandresult.json"
                    ),
                    "r",
                ).read()
            ),
            200,
        )

    return MockResponse(None, 404)


DIGIT_WORDS = [
    text_type("Zero"),
    text_type("One"),
    text_type("Two"),
    text_type("Three"),
    text_type("Four"),
    text_type("Five"),
    text_type("Six"),
    text_type("Seven"),
    text_type("Eight"),
    text_type("Nine"),
    text_type("ten"),
]


class KustoClientTests(unittest.TestCase):
    """A class to test KustoClient."""

    @patch("requests.post", side_effect=mocked_requests_post)
    @patch("azure.kusto.data.security._AadHelper.acquire_token", side_effect=mocked_aad_helper)
    def test_sanity_query(self, mock_post, mock_aad):
        """Test query V2."""
        kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(
            text_type("https://somecluster.kusto.windows.net")
        )
        client = KustoClient(kcsb)
        response = client.execute_query("PythonTest", "Deft")
        expected = {
            "rownumber": None,
            "rowguid": text_type(""),
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
            "xsmalltext": text_type(""),
            "xtext": text_type(""),
            "xnumberAsText": text_type(""),
            "xtime": None,
            "xtextWithNulls": text_type(""),
            "xdynamicWithNulls": text_type(""),
        }

        for row in response.iter_all():
            self.assertEqual(row["rownumber"], expected["rownumber"])
            self.assertEqual(row["rowguid"], expected["rowguid"])
            self.assertEqual(row["xdouble"], expected["xdouble"])
            self.assertEqual(row["xfloat"], expected["xfloat"])
            self.assertEqual(row["xbool"], expected["xbool"])
            self.assertEqual(row["xint16"], expected["xint16"])
            self.assertEqual(row["xint32"], expected["xint32"])
            self.assertEqual(row["xint64"], expected["xint64"])
            self.assertEqual(row["xuint8"], expected["xuint8"])
            self.assertEqual(row["xuint16"], expected["xuint16"])
            self.assertEqual(row["xuint32"], expected["xuint32"])
            self.assertEqual(row["xuint64"], expected["xuint64"])
            self.assertEqual(row["xdate"], expected["xdate"])
            self.assertEqual(row["xsmalltext"], expected["xsmalltext"])
            self.assertEqual(row["xtext"], expected["xtext"])
            self.assertEqual(row["xnumberAsText"], expected["xnumberAsText"])
            self.assertEqual(row["xtime"], expected["xtime"])
            self.assertEqual(row["xtextWithNulls"], expected["xtextWithNulls"])
            self.assertEqual(row["xdynamicWithNulls"], expected["xdynamicWithNulls"])

            self.assertEqual(type(row["rownumber"]), type(expected["rownumber"]))
            self.assertEqual(type(row["rowguid"]), type(expected["rowguid"]))
            self.assertEqual(type(row["xdouble"]), type(expected["xdouble"]))
            self.assertEqual(type(row["xfloat"]), type(expected["xfloat"]))
            self.assertEqual(type(row["xbool"]), type(expected["xbool"]))
            self.assertEqual(type(row["xint16"]), type(expected["xint16"]))
            self.assertEqual(type(row["xint32"]), type(expected["xint32"]))
            self.assertEqual(type(row["xint64"]), type(expected["xint64"]))
            self.assertEqual(type(row["xuint8"]), type(expected["xuint8"]))
            self.assertEqual(type(row["xuint16"]), type(expected["xuint16"]))
            self.assertEqual(type(row["xuint32"]), type(expected["xuint32"]))
            self.assertEqual(type(row["xuint64"]), type(expected["xuint64"]))
            self.assertEqual(type(row["xdate"]), type(expected["xdate"]))
            self.assertEqual(type(row["xsmalltext"]), type(expected["xsmalltext"]))
            self.assertEqual(type(row["xtext"]), type(expected["xtext"]))
            self.assertEqual(type(row["xnumberAsText"]), type(expected["xnumberAsText"]))
            self.assertEqual(type(row["xtime"]), type(expected["xtime"]))
            self.assertEqual(type(row["xtextWithNulls"]), type(expected["xtextWithNulls"]))
            self.assertEqual(type(row["xdynamicWithNulls"]), type(expected["xdynamicWithNulls"]))

            expected["rownumber"] = (
                0 if expected["rownumber"] is None else expected["rownumber"] + 1
            )
            expected["rowguid"] = text_type(
                "0000000{0}-0000-0000-0001-020304050607".format(expected["rownumber"])
            )
            expected["xdouble"] = round(
                float(0) if expected["xdouble"] is None else expected["xdouble"] + 1.0001, 4
            )
            expected["xfloat"] = round(
                float(0) if expected["xfloat"] is None else expected["xfloat"] + 1.01, 2
            )
            expected["xbool"] = False if expected["xbool"] is None else not expected["xbool"]
            expected["xint16"] = 0 if expected["xint16"] is None else expected["xint16"] + 1
            expected["xint32"] = 0 if expected["xint32"] is None else expected["xint32"] + 1
            expected["xint64"] = 0 if expected["xint64"] is None else expected["xint64"] + 1
            expected["xuint8"] = 0 if expected["xuint8"] is None else expected["xuint8"] + 1
            expected["xuint16"] = 0 if expected["xuint16"] is None else expected["xuint16"] + 1
            expected["xuint32"] = 0 if expected["xuint32"] is None else expected["xuint32"] + 1
            expected["xuint64"] = 0 if expected["xuint64"] is None else expected["xuint64"] + 1
            expected["xdate"] = expected["xdate"] or datetime(
                2013, 1, 1, 1, 1, 1, 0, tzinfo=tzutc()
            )
            expected["xdate"] = expected["xdate"].replace(year=expected["xdate"].year + 1)
            expected["xsmalltext"] = DIGIT_WORDS[int(expected["xint16"])]
            expected["xtext"] = DIGIT_WORDS[int(expected["xint16"])]
            expected["xnumberAsText"] = text_type(expected["xint16"])
            microseconds = 1001 if expected["rownumber"] == 5 else 1000
            expected["xtime"] = (
                timedelta()
                if expected["xtime"] is None
                else (abs(expected["xtime"]) + timedelta(seconds=1, microseconds=microseconds))
                * (-1) ** (expected["rownumber"] + 1)
            )
            if expected["xint16"] > 0:
                expected["xdynamicWithNulls"] = text_type(
                    '{{"rowId":{0},"arr":[0,{0}]}}'.format(expected["xint16"])
                )

    @patch("requests.post", side_effect=mocked_requests_post)
    @patch("azure.kusto.data.security._AadHelper.acquire_token", side_effect=mocked_aad_helper)
    def test_sanity_control_command(self, mock_post, mock_aad):
        """Tests contol command."""
        kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(
            text_type("https://somecluster.kusto.windows.net")
        )
        client = KustoClient(kcsb)
        response = client.execute_mgmt("NetDefaultDB", ".show version")
        self.assertEqual(response.get_table_count(), 1)
        row_count = 0
        for _ in response.iter_all():
            row_count += 1
        self.assertEqual(row_count, 1)
        result = list(response.iter_all())[0]
        self.assertEqual(result["BuildVersion"], "1.0.6693.14577")
        self.assertEqual(
            result["BuildTime"],
            datetime(year=2018, month=4, day=29, hour=8, minute=5, second=54, tzinfo=tzutc()),
        )
        self.assertEqual(result["ServiceType"], "Engine")
        self.assertEqual(result["ProductVersion"], "KustoMain_2018.04.29.5")

    @patch("requests.post", side_effect=mocked_requests_post)
    @patch("azure.kusto.data.security._AadHelper.acquire_token", side_effect=mocked_aad_helper)
    def test_sanity_data_frame(self, mock_post, mock_aad):
        """Tests KustoResponse to pandas.DataFrame."""
        kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(
            text_type("https://somecluster.kusto.windows.net")
        )
        client = KustoClient(kcsb)
        data_frame = client.execute_query("PythonTest", "Deft").to_dataframe(errors="ignore")
        self.assertEqual(len(data_frame.columns), 19)
        expected_dict = {
            "rownumber": Series([None, 0., 1., 2., 3., 4., 5., 6., 7., 8., 9.]),
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
            "xdouble": Series(
                [None, 0., 1.0001, 2.0002, 3.0003, 4.0004, 5.0005, 6.0006, 7.0007, 8.0008, 9.0009]
            ),
            "xfloat": Series([None, 0., 1.01, 2.02, 3.03, 4.04, 5.05, 6.06, 7.07, 8.08, 9.09]),
            "xbool": Series(
                [None, False, True, False, True, False, True, False, True, False, True], dtype=bool
            ),
            "xint16": Series([None, 0., 1., 2., 3., 4., 5., 6., 7., 8., 9.]),
            "xint32": Series([None, 0., 1., 2., 3., 4., 5., 6., 7., 8., 9.]),
            "xint64": Series([None, 0., 1., 2., 3., 4., 5., 6., 7., 8., 9.]),
            "xuint8": Series([None, 0., 1., 2., 3., 4., 5., 6., 7., 8., 9.]),
            "xuint16": Series([None, 0., 1., 2., 3., 4., 5., 6., 7., 8., 9.]),
            "xuint32": Series([None, 0., 1., 2., 3., 4., 5., 6., 7., 8., 9.]),
            "xuint64": Series([None, 0., 1., 2., 3., 4., 5., 6., 7., 8., 9.]),
            "xdate": Series(
                [
                    "NaT",
                    "2014-01-01T01:01:01.000000000",
                    "2015-01-01T01:01:01.000000000",
                    "2016-01-01T01:01:01.000000000",
                    "2017-01-01T01:01:01.000000000",
                    "2018-01-01T01:01:01.000000000",
                    "2019-01-01T01:01:01.000000000",
                    "2020-01-01T01:01:01.000000000",
                    "2021-01-01T01:01:01.000000000",
                    "2022-01-01T01:01:01.000000000",
                    "2023-01-01T01:01:01.000000000",
                ],
                dtype="datetime64[ns]",
            ),
            "xsmalltext": Series(
                [
                    "",
                    "Zero",
                    "One",
                    "Two",
                    "Three",
                    "Four",
                    "Five",
                    "Six",
                    "Seven",
                    "Eight",
                    "Nine",
                ],
                dtype=object,
            ),
            "xtext": Series(
                [
                    "",
                    "Zero",
                    "One",
                    "Two",
                    "Three",
                    "Four",
                    "Five",
                    "Six",
                    "Seven",
                    "Eight",
                    "Nine",
                ],
                dtype=object,
            ),
            "xnumberAsText": Series(
                ["", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"], dtype=object
            ),
            "xtime": Series(
                [
                    "NaT",
                    0,
                    "00:00:01.0010001",
                    "-00:00:02.0020002",
                    "00:00:03.0030003",
                    "-00:00:04.0040004",
                    "00:00:05.0050005",
                    "-00:00:06.0060006",
                    "00:00:07.0070007",
                    "-00:00:08.0080008",
                    "00:00:09.0090009",
                ],
                dtype="timedelta64[ns]",
            ),
            "xtextWithNulls": Series(["", "", "", "", "", "", "", "", "", "", ""], dtype=object),
            "xdynamicWithNulls": Series(
                [
                    None,
                    None,
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

    @patch("requests.post", side_effect=mocked_requests_post)
    @patch("azure.kusto.data.security._AadHelper.acquire_token", side_effect=mocked_aad_helper)
    def test_partial_results(self, mock_post, mock_aad):
        """Tests partial results."""
        kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(
            text_type("https://somecluster.kusto.windows.net")
        )
        client = KustoClient(kcsb)
        query = """\
set truncationmaxrecords = 1;
range x from 1 to 2 step 1"""
        self.assertRaises(KustoServiceError, client.execute_query, "PythonTest", query)
        response = client.execute_query("PythonTest", query, accept_partial_results=True)
        self.assertTrue(response.has_exceptions())
        self.assertEqual(response.get_exceptions()[0]["error"]["code"], "LimitsExceeded")
        self.assertEqual(response.get_table_count(), 5)
        results = list(response.iter_all())
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["x"], 1)
