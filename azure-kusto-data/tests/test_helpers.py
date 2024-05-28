# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import datetime
import json
import os

from azure.kusto.data._models import KustoResultTable
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data.response import KustoResponseDataSetV2
import pandas
import numpy


def test_dataframe_from_result_table():
    """Test conversion of KustoResultTable to pandas.DataFrame, including fixes for certain column types"""

    with open(os.path.join(os.path.dirname(__file__), "input", "dataframe.json"), "r") as response_file:
        data = response_file.read()

    response = KustoResponseDataSetV2(json.loads(data))
    test_dict1 = {'RecordName': lambda col, frame: frame[col].astype('str'), 'RecordInt64': lambda col, frame: frame[col].astype('int64')}
    test_dict2 = {'int': lambda col, frame: frame[col].astype('int32')}
    df = dataframe_from_result_table(response.primary_results[0], column_name_totype_dict= test_dict1, type_totype_dict=test_dict2)

    if hasattr(pandas, "StringDType"):
        assert df["RecordName"].dtype == pandas.StringDtype()
        assert str(df.iloc[0].RecordName) == "now"
    else:
        assert df.iloc[0].RecordName == "now"

    if hasattr(pandas, "StringDType"):
        assert df["RecordGUID"].dtype == pandas.StringDtype()
        assert str(df.iloc[0].RecordGUID) == "6f3c1072-2739-461c-8aa7-3cfc8ff528a8"
    else:
        assert df.iloc[0].RecordGUID == "6f3c1072-2739-461c-8aa7-3cfc8ff528a8"

    assert type(df.iloc[0].RecordTime) is pandas._libs.tslibs.timestamps.Timestamp
    assert all(getattr(df.iloc[0].RecordTime, k) == v for k, v in {"year": 2021, "month": 12, "day": 22, "hour": 11, "minute": 43, "second": 00}.items())
    assert type(df.iloc[0].RecordBool) is numpy.bool_
    assert df.iloc[0].RecordBool == True
    assert type(df.iloc[0].RecordInt) is numpy.int32
    assert df.iloc[0].RecordInt == 5678
    assert type(df.iloc[0].RecordInt64) is numpy.int64
    assert df.iloc[0].RecordInt64 == 222
    assert type(df.iloc[0].RecordLong) is numpy.int64
    assert df.iloc[0].RecordLong == 92233720368
    assert type(df.iloc[0].RecordReal) is numpy.float64
    assert df.iloc[0].RecordReal == 3.14159

    # Kusto datetime(0000-01-01T00:00:00Z), which Pandas can't represent.
    assert df.iloc[1].RecordName == "earliest datetime"
    assert type(df.iloc[1].RecordTime) is pandas._libs.tslibs.nattype.NaTType
    assert pandas.isnull(df.iloc[1].RecordReal)

    # Kusto datetime(9999-12-31T23:59:59Z), which Pandas can't represent.
    assert df.iloc[2].RecordName == "latest datetime"
    assert type(df.iloc[2].RecordTime) is pandas._libs.tslibs.nattype.NaTType
    assert type(df.iloc[2].RecordReal) is numpy.float64
    assert df.iloc[2].RecordReal == numpy.inf

    # Pandas earliest datetime
    assert df.iloc[3].RecordName == "earliest pandas datetime"
    assert type(df.iloc[3].RecordTime) is pandas._libs.tslibs.timestamps.Timestamp
    assert type(df.iloc[3].RecordReal) is numpy.float64
    assert df.iloc[3].RecordReal == -numpy.inf

    # Pandas latest datetime
    assert df.iloc[4].RecordName == "latest pandas datetime"
    assert type(df.iloc[4].RecordTime) is pandas._libs.tslibs.timestamps.Timestamp

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


def test_pandas_mixed_date():
    df = dataframe_from_result_table(
        KustoResultTable(
            {
                "TableName": "Table_0",
                "Columns": [
                    {"ColumnName": "Date", "ColumnType": "datetime"},
                ],
                "Rows": [
                    ["2023-12-12T01:59:59.352Z"],
                    ["2023-12-12T01:54:44Z"],
                ],
            }
        )
    )

    assert df["Date"][0] == pandas.Timestamp(year=2023, month=12, day=12, hour=1, minute=59, second=59, microsecond=352000, tzinfo=datetime.timezone.utc)
    assert df["Date"][1] == pandas.Timestamp(year=2023, month=12, day=12, hour=1, minute=54, second=44, tzinfo=datetime.timezone.utc)
