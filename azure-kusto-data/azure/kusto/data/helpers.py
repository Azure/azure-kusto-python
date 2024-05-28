import sys
from typing import TYPE_CHECKING, Union
import numpy as np
import pandas as pd

if TYPE_CHECKING:
    import pandas
    from azure.kusto.data._models import KustoResultTable, KustoStreamingResultTable


# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
def to_pandas_timedelta(raw_value: Union[int, float, str]) -> "pandas.Timedelta":
    """
    Transform a raw python value to a pandas timedelta.
    """
    import pandas as pd

    if isinstance(raw_value, (int, float)):
        # https://docs.microsoft.com/en-us/dotnet/api/system.datetime.ticks
        # Kusto saves up to ticks, 1 tick == 100 nanoseconds
        return pd.to_timedelta(raw_value * 100, unit="ns")
    if isinstance(raw_value, str):
        # The timespan format Kusto returns is 'd.hh:mm:ss.ssssss' or 'hh:mm:ss.ssssss' or 'hh:mm:ss'
        # Pandas expects 'd days hh:mm:ss.ssssss' or 'hh:mm:ss.ssssss' or 'hh:mm:ss'
        parts = raw_value.split(":")
        if "." not in parts[0]:
            return pd.to_timedelta(raw_value)
        else:
            formatted_value = raw_value.replace(".", " days ", 1)
            return pd.to_timedelta(formatted_value)


def dataframe_from_result_table(table: "Union[KustoResultTable, KustoStreamingResultTable]",
                                nullable_bools: bool = False, column_name_totype_dict: dict = {},
                                type_totype_dict: dict = {}) -> "pandas.DataFrame":
    """Converts Kusto tables into pandas DataFrame.
    :param azure.kusto.data._models.KustoResultTable table: Table received from the response.
    :param nullable_bools: When True, converts bools that are 'null' from kusto or 'None' from python to pandas.NA. This will be the default in the future.
    :param column_name_totype_dict: When received converts specified columns to corresponding types, else uses type_totype_dict
    :param type_totype_dict: When received converts specified types to corresponding types, else uses default_dict
    :return: pandas DataFrame.
    """
    import pandas as pd

    if not table:
        raise ValueError()

    from azure.kusto.data._models import KustoResultTable, KustoStreamingResultTable

    if not isinstance(table, KustoResultTable) and not isinstance(table, KustoStreamingResultTable):
        raise TypeError("Expected KustoResultTable or KustoStreamingResultTable got {}".format(type(table).__name__))

    columns = [col.column_name for col in table.columns]
    frame = pd.DataFrame(table.raw_rows, columns=columns)

    default_dict = {
        "string": lambda col, df: df[col].astype(pd.StringDtype()) if hasattr(pd, "StringDType") else df[col],
        "guid": lambda col, df: df[col],
        "dynamic": lambda col, df: df[col],
        "bool": lambda col, df: df[col].astype(pd.BooleanDtype() if nullable_bools else bool),
        "int": lambda col, df: df[col].astype(pd.Int32Dtype()),
        "long": lambda col, df: df[col].astype(pd.Int64Dtype()),
        "real": lambda col, df: real_or_decimal(df, col),
        "decimal": lambda col, df: real_or_decimal(df, col),
        "datetime": lambda col, df: datetime_func(df, col),
        "timespan": lambda col, df: df[col].apply(to_pandas_timedelta)
    }

    by_name_bool = True if column_name_totype_dict else False
    by_type_bool = True if type_totype_dict else False

    for col in table.columns:
        if by_name_bool and col.column_name in column_name_totype_dict.keys():
            frame[col.column_name] = column_name_totype_dict[col.column_name](col.column_name, frame)
        elif by_type_bool and col.column_type in type_totype_dict.keys():
            frame[col.column_name] = type_totype_dict[col.column_type](col.column_name, frame)
        else:
            frame[col.column_name] = default_dict[col.column_type](col.column_name, frame)

    return frame


def get_string_tail_lower_case(val, length):
    if length <= 0:
        return ""

    if length >= len(val):
        return val.lower()

    return val[len(val) - length:].lower()


def real_or_decimal(frame, col):
    frame[col] = frame[col].replace("NaN", np.NaN).replace("Infinity", np.PINF).replace("-Infinity", np.NINF)
    frame[col] = pd.to_numeric(frame[col], errors="coerce").astype(pd.Float64Dtype())
    return frame[col]


def datetime_func(frame, col):
    # Pandas before version 2 doesn't support the "format" arg
    args = {}
    if pd.__version__.startswith("2."):
        args = {"format": "ISO8601", "utc": True}
    else:
        # if frame contains ".", replace "Z" with ".000Z"
        # == False is not a mistake - that's the pandas way to do it
        contains_dot = frame[col].str.contains(".")
        frame.loc[contains_dot == False, col] = frame.loc[contains_dot == False, col].str.replace("Z", ".000Z")
    frame[col] = pd.to_datetime(frame[col], errors="coerce", **args)
    return frame[col]
