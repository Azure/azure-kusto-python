# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import json
import os

from azure.kusto.data._models import KustoResultTable


def test_str_and_dates_smoke():
    with open(os.path.join(os.path.dirname(__file__), "input", "deft.json"), "r") as f:
        data = f.read()
    json_table = json.loads(data)[2]

    result_table = KustoResultTable(json_table)
    assert len(str(result_table)) == 4537


def test_to_dict_json():
    with open(os.path.join(os.path.dirname(__file__), "input", "deft.json"), "r") as f:
        data = f.read()
    json_table = json.loads(data)[2]

    result_table = KustoResultTable(json_table)
    assert (
        json.dumps(result_table.to_dict(), default=str)
        == """{"name": "Deft", "kind": "PrimaryResult", "data": [{"rownumber": null, "rowguid": "", "xdouble": null, "xfloat": null, "xbool": null, "xint16": null, "xint32": null, "xint64": null, "xuint8": null, "xuint16": null, "xuint32": null, "xuint64": null, "xdate": null, "xsmalltext": "", "xtext": "", "xnumberAsText": "", "xtime": null, "xtextWithNulls": "", "xdynamicWithNulls": ""}, {"rownumber": 0, "rowguid": "00000000-0000-0000-0001-020304050607", "xdouble": 0.0, "xfloat": 0.0, "xbool": false, "xint16": 0, "xint32": 0, "xint64": 0, "xuint8": 0, "xuint16": 0, "xuint32": 0, "xuint64": 0, "xdate": "2014-01-01 01:01:01+00:00", "xsmalltext": "Zero", "xtext": "Zero", "xnumberAsText": "0", "xtime": "0:00:00", "xtextWithNulls": "", "xdynamicWithNulls": ""}, {"rownumber": 1, "rowguid": "00000001-0000-0000-0001-020304050607", "xdouble": 1.0001, "xfloat": 1.01, "xbool": true, "xint16": 1, "xint32": 1, "xint64": 1, "xuint8": 1, "xuint16": 1, "xuint32": 1, "xuint64": 1, "xdate": "2015-01-01 01:01:01+00:00", "xsmalltext": "One", "xtext": "One", "xnumberAsText": "1", "xtime": "1 day, 0:00:01.001000", "xtextWithNulls": "", "xdynamicWithNulls": {"rowId": 1, "arr": [0, 1]}}, {"rownumber": 2, "rowguid": "00000002-0000-0000-0001-020304050607", "xdouble": 2.0002, "xfloat": 2.02, "xbool": false, "xint16": 2, "xint32": 2, "xint64": 2, "xuint8": 2, "xuint16": 2, "xuint32": 2, "xuint64": 2, "xdate": "2016-01-01 01:01:01+00:00", "xsmalltext": "Two", "xtext": "Two", "xnumberAsText": "2", "xtime": "-3 days, 23:59:57.998000", "xtextWithNulls": "", "xdynamicWithNulls": {"rowId": 2, "arr": [0, 2]}}, {"rownumber": 3, "rowguid": "00000003-0000-0000-0001-020304050607", "xdouble": 3.0003, "xfloat": 3.03, "xbool": true, "xint16": 3, "xint32": 3, "xint64": 3, "xuint8": 3, "xuint16": 3, "xuint32": 3, "xuint64": 3, "xdate": "2017-01-01 01:01:01+00:00", "xsmalltext": "Three", "xtext": "Three", "xnumberAsText": "3", "xtime": "3 days, 0:00:03.003000", "xtextWithNulls": "", "xdynamicWithNulls": {"rowId": 3, "arr": [0, 3]}}, {"rownumber": 4, "rowguid": "00000004-0000-0000-0001-020304050607", "xdouble": 4.0004, "xfloat": 4.04, "xbool": false, "xint16": 4, "xint32": 4, "xint64": 4, "xuint8": 4, "xuint16": 4, "xuint32": 4, "xuint64": 4, "xdate": "2018-01-01 01:01:01+00:00", "xsmalltext": "Four", "xtext": "Four", "xnumberAsText": "4", "xtime": "-5 days, 23:59:55.996000", "xtextWithNulls": "", "xdynamicWithNulls": {"rowId": 4, "arr": [0, 4]}}, {"rownumber": 5, "rowguid": "00000005-0000-0000-0001-020304050607", "xdouble": 5.0005, "xfloat": 5.05, "xbool": true, "xint16": 5, "xint32": 5, "xint64": 5, "xuint8": 5, "xuint16": 5, "xuint32": 5, "xuint64": 5, "xdate": "2019-01-01 01:01:01+00:00", "xsmalltext": "Five", "xtext": "Five", "xnumberAsText": "5", "xtime": "5 days, 0:00:05.005001", "xtextWithNulls": "", "xdynamicWithNulls": {"rowId": 5, "arr": [0, 5]}}, {"rownumber": 6, "rowguid": "00000006-0000-0000-0001-020304050607", "xdouble": 6.0006, "xfloat": 6.06, "xbool": false, "xint16": 6, "xint32": 6, "xint64": 6, "xuint8": 6, "xuint16": 6, "xuint32": 6, "xuint64": 6, "xdate": "2020-01-01 01:01:01+00:00", "xsmalltext": "Six", "xtext": "Six", "xnumberAsText": "6", "xtime": "-7 days, 23:59:53.993999", "xtextWithNulls": "", "xdynamicWithNulls": {"rowId": 6, "arr": [0, 6]}}, {"rownumber": 7, "rowguid": "00000007-0000-0000-0001-020304050607", "xdouble": 7.0007, "xfloat": 7.07, "xbool": true, "xint16": 7, "xint32": 7, "xint64": 7, "xuint8": 7, "xuint16": 7, "xuint32": 7, "xuint64": 7, "xdate": "2021-01-01 01:01:01+00:00", "xsmalltext": "Seven", "xtext": "Seven", "xnumberAsText": "7", "xtime": "7 days, 0:00:07.007001", "xtextWithNulls": "", "xdynamicWithNulls": {"rowId": 7, "arr": [0, 7]}}, {"rownumber": 8, "rowguid": "00000008-0000-0000-0001-020304050607", "xdouble": 8.0008, "xfloat": 8.08, "xbool": false, "xint16": 8, "xint32": 8, "xint64": 8, "xuint8": 8, "xuint16": 8, "xuint32": 8, "xuint64": 8, "xdate": "2022-01-01 01:01:01+00:00", "xsmalltext": "Eight", "xtext": "Eight", "xnumberAsText": "8", "xtime": "-9 days, 23:59:51.991999", "xtextWithNulls": "", "xdynamicWithNulls": {"rowId": 8, "arr": [0, 8]}}, {"rownumber": 9, "rowguid": "00000009-0000-0000-0001-020304050607", "xdouble": 9.0009, "xfloat": 9.09, "xbool": true, "xint16": 9, "xint32": 9, "xint64": 9, "xuint8": 9, "xuint16": 9, "xuint32": 9, "xuint64": 9, "xdate": "2023-01-01 01:01:01+00:00", "xsmalltext": "Nine", "xtext": "Nine", "xnumberAsText": "9", "xtime": "9 days, 0:00:09.009001", "xtextWithNulls": "", "xdynamicWithNulls": {"rowId": 9, "arr": [0, 9]}}]}"""
    )
