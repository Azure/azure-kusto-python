# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import datetime
import json
import os

import pytest

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
# Test when given both types of dictionary parameters that type conversion doesn't override column name conversion
test_dict_by_name = {
    "RecordName": lambda col, frame: frame[col].astype("str"),
    "RecordInt64": lambda col, frame: frame[col].astype("int64"),
    "MissingType": lambda col, frame: frame[col].astype("str"),
}
test_dict_by_type = {"int": lambda col, frame: frame[col].astype("int32")}
df = dataframe_from_result_table(response.primary_results[0], converters_by_type=test_dict_by_type, converters_by_column_name=test_dict_by_name)

if hasattr(pandas, "StringDType"):
    assert df["RecordName"].dtype == pandas.StringDtype()
    assert str(df.iloc[0].RecordName) == "now"
    assert df["RecordGUID"].dtype == pandas.StringDtype()
    assert str(df.iloc[0].RecordGUID) == "6f3c1072-2739-461c-8aa7-3cfc8ff528a8"
    assert df["RecordDynamic"].dtype == pandas.StringDtype()
    assert (
        str(df.iloc[0].RecordDynamic)
        == '{"Visualization":null,"Title":null,"XColumn":null,"Series":null,"YColumns":null,"XTitle":null,"YTitle":null,"XAxis":null,"YAxis":null,"Legend":null,"YSplit":null,"Accumulate":false,"IsQuerySorted":false,"Kind":null}'
    )
else:
    assert df.iloc[0].RecordName == "now"
    assert df.iloc[0].RecordGUID == "6f3c1072-2739-461c-8aa7-3cfc8ff528a8"

assert type(df.iloc[0].RecordTime) is pandas._libs.tslibs.timestamps.Timestamp

for k, v in {"year": 2021, "month": 12, "day": 22, "hour": 11, "minute": 43, "second": 00}.items():
    assert getattr(df.iloc[0].RecordTime, k) == v
assert type(df.iloc[0].RecordBool) is numpy.bool_
assert df.iloc[0].RecordBool
assert type(df.iloc[0].RecordInt) is numpy.int32
assert df.iloc[0].RecordInt == 5678
assert type(df.iloc[0].RecordInt64) is numpy.int64
assert df.iloc[0].RecordInt64 == 222
assert type(df.iloc[0].RecordLong) is numpy.int64
assert df.iloc[0].RecordLong == 92233720368
assert type(df.iloc[0].RecordReal) is numpy.float64
assert df.iloc[0].RecordReal == 3.14159
assert type(df.iloc[0].RecordDouble) is numpy.float64
assert df.iloc[0].RecordDouble == 7.89
assert type(df.iloc[0].RecordDecimal) is numpy.float64
assert df.iloc[0].RecordDecimal == 1.2

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

# Testing int to float conversion
test_int_to_float = {"int": "float64"}
ignore_missing_type = {
    "MissingType": lambda col, frame: frame[col].astype("str"),
}
df_int_to_float = dataframe_from_result_table(response.primary_results[0], converters_by_type=test_int_to_float, converters_by_column_name=ignore_missing_type)
assert type(df_int_to_float.iloc[0].RecordInt) is numpy.float64
assert df.iloc[0].RecordInt == 5678

# Testing missing type conversion
with pytest.raises(Exception):
    df_missing_type = dataframe_from_result_table(response.primary_results[0])


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


@pytest.mark.xdist_group("outside")
def test_parse_datetime():
    """Test parse_datetime function with different pandas versions and datetime formats"""
    from azure.kusto.data.helpers import parse_datetime

    # Test with pandas v2 behavior (force version 2)
    df_v2 = pandas.DataFrame(
        {
            "mixed": ["2023-12-12T01:59:59.352Z", "2023-12-12T01:54:44Z"],
        }
    )

    # Force pandas v2 behavior
    result_v2 = parse_datetime(df_v2, "mixed", force_version="2.0.0")
    assert str(result_v2[0]) == "2023-12-12 01:59:59.352000+00:00"
    assert str(result_v2[1]) == "2023-12-12 01:54:44+00:00"
    # Test with pandas v1 behavior (force version 1)

    df_v1 = pandas.DataFrame(
        {
            "mixed": ["2023-12-12T01:59:59.352Z", "2023-12-12T01:54:44Z"],
        }
    )

    # Force pandas v1 behavior - it should add .000 to dates without milliseconds
    result_v1 = parse_datetime(df_v1, "mixed", force_version="1.5.3")
    assert str(result_v1[0]) == "2023-12-12 01:59:59.352000+00:00"
    assert str(result_v1[1]) == "2023-12-12 01:54:44+00:00"
