# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import json
import os
import pathlib
import uuid
from datetime import datetime, timedelta
from io import BytesIO
from typing import Optional, Any, Dict, Union, Iterator, Tuple

import pytest
from dateutil.tz import UTC
from requests import HTTPError

from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.data._models import KustoResultRow, KustoResultTable, KustoStreamingResultTable
from azure.kusto.data.response import WellKnownDataSet, KustoStreamingResponseDataSet, KustoResponseDataSet

PANDAS = False
try:
    import pandas

    PANDAS = True
except:
    pass


def get_input_folder_path(file: Optional[str] = None) -> pathlib.Path:
    path = pathlib.Path(__file__).parent.joinpath("input")
    if file:
        path = path.joinpath(file)
    return path


def mocked_requests_post(*args, **kwargs):
    """Mock to replace requests.Session.post"""

    class Raw(BytesIO):
        def __init__(self, initial_value: Optional[bytes]) -> None:
            super().__init__(initial_value)
            self.decode_content = False

    class MockResponse:
        """Mock class for KustoResponse."""

        def __init__(self, json_data: Optional[Dict[str, Any]], status_code: int, url: str):
            self.json_data = json_data
            self.text = str(json_data)
            self.status_code = status_code
            self.headers = None
            self.reason = ""
            self.url = url
            self.raw = Raw(json.dumps(json_data).encode())

        def json(self) -> Optional[Dict[str, Any]]:
            """Get json data from response."""
            return self.json_data

        def raise_for_status(self):
            """Raises stored :class:`HTTPError`, if one occurred."""
            http_error_msg = ""
            if isinstance(self.reason, bytes):
                # We attempt to decode utf-8 first because some servers
                # choose to localize their reason strings. If the string
                # isn't utf-8, we fall back to iso-8859-1 for all other
                # encodings. (See PR #3538)
                try:
                    reason = self.reason.decode("utf-8")
                except UnicodeDecodeError:
                    reason = self.reason.decode("iso-8859-1")
            else:
                reason = self.reason

            if 400 <= self.status_code < 500:
                http_error_msg = "%s Client Error: %s for url: %s" % (self.status_code, reason, self.url)

            elif 500 <= self.status_code < 600:
                http_error_msg = "%s Server Error: %s for url: %s" % (self.status_code, reason, self.url)

            if http_error_msg:
                raise HTTPError(http_error_msg, response=self)

    url = args[0]
    if url == "https://somecluster.kusto.windows.net/v2/rest/query":
        if "truncationmaxrecords" in kwargs["json"]["csl"]:
            if json.loads(kwargs["json"]["properties"])["Options"]["deferpartialqueryfailures"]:
                file_name = "query_partial_results_defer_is_true.json"
            else:
                file_name = "query_partial_results_defer_is_false.json"
        elif "Deft" in kwargs["json"]["csl"]:
            file_name = "deft.json"
        elif "pandas_bool" in kwargs["json"]["csl"]:
            file_name = "pandas_bool.json"
        elif "print dynamic" in kwargs["json"]["csl"]:
            file_name = "dynamic.json"
        elif "take 0" in kwargs["json"]["csl"]:
            file_name = "zero_results.json"
        elif "PrimaryResultName" in kwargs["json"]["csl"]:
            file_name = "null_values.json"
        else:
            raise Exception("Invalid file name")

        with open(os.path.join(os.path.dirname(__file__), "input", file_name), "r") as response_file:
            data = response_file.read()
        return MockResponse(json.loads(data), 200, url)

    elif url == "https://somecluster.kusto.windows.net/v1/rest/mgmt":
        if kwargs["json"]["csl"] == ".show version":
            file_name = "versionshowcommandresult.json"
        else:
            file_name = "adminthenquery.json"
        with open(os.path.join(os.path.dirname(__file__), "input", file_name), "r") as response_file:
            data = response_file.read()
        return MockResponse(json.loads(data), 200, url)

    elif url == "https://somecluster.kusto.windows.net/v1/rest/auth/metadata":
        return MockResponse(
            {
                "AzureAD": {
                    "LoginEndpoint": "https://login.microsoftonline.com",
                    "LoginMfaRequired": False,
                    "KustoClientAppId": "db662dc1-0cfe-4e1c-a843-19a68e65be58",
                    "KustoClientRedirectUri": "https://microsoft/kustoclient",
                    "KustoServiceResourceId": "https://kusto.dev.kusto.windows.net",
                    "FirstPartyAuthorityUrl": "https://login.microsoftonline.com/f8cdef31-a31e-4b4a-93e4-5f571e91255a",
                },
                "dSTS": {
                    "CloudEndpointSuffix": "windows.net",
                    "DstsRealm": "realm://dsts.core.windows.net",
                    "DstsInstance": "prod-dsts.dsts.core.windows.net",
                    "KustoDnsHostName": "kusto.windows.net",
                    "ServiceName": "kusto",
                },
            },
            200,
            url,
        )

    return MockResponse(None, 404, url)


@pytest.fixture(
    params=[
        "user_password",
        "application_key",
        "application_token",
        "device",
        "user_token",
        "managed_identity",
        "token_provider",
        "async_token_provider",
        "az_cli",
        "interactive_login",
    ]
)
def proxy_kcsb(request) -> Tuple[KustoConnectionStringBuilder, bool]:
    cluster = KustoClientTestsMixin.HOST
    user = "test2"
    password = "Pa$$w0rd2"
    authority_id = "13456"
    uuid = "11111111-1111-1111-1111-111111111111"
    key = "key of application"
    token = "The app hardest token ever"

    return {
        "user_password": (KustoConnectionStringBuilder.with_aad_user_password_authentication(cluster, user, password, authority_id), True),
        "application_key": (KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, uuid, key, "microsoft.com"), True),
        "application_token": (KustoConnectionStringBuilder.with_aad_application_token_authentication(cluster, application_token=token), False),
        "device": (KustoConnectionStringBuilder.with_aad_device_authentication(cluster), True),
        "user_token": (KustoConnectionStringBuilder.with_aad_user_token_authentication(cluster, user_token=token), False),
        "managed_identity": (KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster), False),
        "token_provider": (KustoConnectionStringBuilder.with_token_provider(cluster, lambda x: x), False),
        "async_token_provider": (KustoConnectionStringBuilder.with_async_token_provider(cluster, lambda x: x), False),
        "az_cli": (KustoConnectionStringBuilder.with_az_cli_authentication(cluster), True),
        "interactive_login": (KustoConnectionStringBuilder.with_interactive_login(cluster), True),
    }[request.param]


DIGIT_WORDS = [str("Zero"), str("One"), str("Two"), str("Three"), str("Four"), str("Five"), str("Six"), str("Seven"), str("Eight"), str("Nine"), str("ten")]

SyncResponseSet = Union[KustoStreamingResponseDataSet, KustoResponseDataSet]
SyncResultTable = Union[KustoResultTable, KustoStreamingResultTable]


def get_response_first_primary_result(response: SyncResponseSet) -> SyncResultTable:
    return next(x for x in response if x.table_kind == WellKnownDataSet.PrimaryResult)


def get_table_first_row(table: SyncResultTable) -> KustoResultRow:
    return next(iter(table))


class KustoClientTestsMixin:
    HOST = "https://somecluster.kusto.windows.net"

    @staticmethod
    def _assert_client_request_id(response_args: dict, value: Optional[str] = None) -> None:
        header = response_args["headers"]["x-ms-client-request-id"]
        if value:
            assert header == value
            return

        [header_prefix, header_uuid] = header.split(";")
        assert header_prefix == "KPC.execute"
        uuid.UUID(header_uuid)

    @staticmethod
    def _assert_sanity_query_primary_results(results: Iterator[KustoResultRow]):
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
        for row in results:
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
    def _assert_sanity_query_response(response: SyncResponseSet):
        KustoClientTestsMixin._assert_sanity_query_primary_results(get_response_first_primary_result(response))

    @staticmethod
    def _assert_sanity_control_command_response(response: SyncResponseSet):
        assert len(response) == 1
        primary_table = get_response_first_primary_result(response)
        row_count = 0
        for _ in primary_table:
            row_count += 1
        assert row_count == 1
        result = primary_table[0]
        assert result["BuildVersion"] == "1.0.6693.14577"
        assert result["BuildTime"] == datetime(year=2018, month=4, day=29, hour=8, minute=5, second=54, tzinfo=UTC)
        assert result["ServiceType"] == "Engine"
        assert result["ProductVersion"] == "KustoMain_2018.04.29.5"

    def _assert_sanity_data_frame_response(self, data_frame: "pandas.DataFrame"):
        from pandas import DataFrame, Series
        from pandas.testing import assert_frame_equal

        assert len(data_frame.columns) == 19
        expected_dict = {
            "rownumber": Series([None, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9], dtype="Int32"),
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
            "xdouble": Series([None, 0.0, 1.0001, 2.0002, 3.0003, 4.0004, 5.0005, 6.0006, 7.0007, 8.0008, 9.0009], dtype="Float64"),
            "xfloat": Series([None, 0.0, 1.01, 2.02, 3.03, 4.04, 5.05, 6.06, 7.07, 8.08, 9.09], dtype="Float64"),
            "xbool": Series([None, False, True, False, True, False, True, False, True, False, True], dtype=bool),
            "xint16": Series([None, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9], dtype="Int32"),
            "xint32": Series([None, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9], dtype="Int32"),
            "xint64": Series([None, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9], dtype="Int64"),
            "xuint8": Series([None, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9], dtype="Int64"),
            "xuint16": Series([None, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9], dtype="Int64"),
            "xuint32": Series([None, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9], dtype="Int64"),
            "xuint64": Series([None, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9], dtype="Int64"),
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
                    "-2 days 00:00:02.0020002",
                    "3 days 00:00:03.0030003",
                    "-4 days 00:00:04.0040004",
                    "5 days 00:00:05.0050005",
                    "-6 days 00:00:06.0060006",
                    "7 days 00:00:07.0070007",
                    "-8 days 00:00:08.0080008",
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
    def _assert_partial_results_response(response: SyncResponseSet):
        results = list(get_response_first_primary_result(response))
        assert len(results) == 5
        assert results[0]["x"] == 1

        if type(response) == KustoStreamingResponseDataSet:
            _ = [t for t in response]  # Read rest of tables
        assert response.errors_count == 1
        assert "E_QUERY_RESULT_SET_TOO_LARGE" in response.get_exceptions()[0]
        assert len(response) == 3

    @staticmethod
    def _assert_admin_then_query_response(response: SyncResponseSet):
        assert response.errors_count == 0
        assert len(response) == 4
        results = list(get_response_first_primary_result(response))
        assert len(results) == 2
        assert response[0].table_kind == WellKnownDataSet.PrimaryResult
        assert response[1].table_kind == WellKnownDataSet.QueryProperties
        assert response[2].table_kind == WellKnownDataSet.QueryCompletionInformation
        assert response[3].table_kind == WellKnownDataSet.TableOfContents

    @staticmethod
    def _assert_dynamic_response(row: KustoResultRow):
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
