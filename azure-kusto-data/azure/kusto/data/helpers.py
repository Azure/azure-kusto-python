"""Kusto helper functions"""

import pandas
from pandas import Series

def _timespan_column_parser(timespan):
    """Converts kusto timespan into pandas timedelta"""
    factor = 1
    if timespan and timespan.startswith("-"):
        factor = -1
        timespan = timespan[1:]
    return pandas.Timedelta(timespan.replace(".", " days ", 1) if timespan and "." in timespan.split(":")[0] else timespan) * factor

def dataframe_from_result_table(table, raise_errors=True):
    """Converts Kusto tables into pandas DataFrame.
    :param azure.kusto.data._models.KustoResultTable table: Table recieved from the response.
    :param bool raise_errors: Will raise errors if set to true, and swallow otherwise.
    :return: pandas DataFrame.
    :rtype: pandas.DataFrame
    """
    import json
    from six import text_type

    kusto_to_dataframe_data_types = {
        "bool": "bool",
        "uint8": "int64",
        "int16": "int64",
        "uint16": "int64",
        "int": "int64",
        "uint": "int64",
        "long": "int64",
        "ulong": "int64",
        "float": "float64",
        "real": "float64",
        "decimal": "float64",
        "string": "object",
        "datetime": "datetime64[ns]",
        "guid": "object",
        "timespan": "timedelta64[ns]",
        "dynamic": "object",
        # Support V1
        "DateTime": "datetime64[ns]",
        "Int32": "int32",
        "Int64": "int64",
        "Double": "float64",
        "String": "object",
        "SByte": "object",
        "Guid": "object",
        "TimeSpan": "object",
    }

    """Returns Pandas data frame."""
    if not table.columns or not table.rows:
        return pandas.DataFrame()

    frame = pandas.DataFrame(table.rows, columns=[column.column_name for column in table.columns])

    for column in table.columns:
        col_name = column.column_name
        col_type = column.column_type
        if col_type.lower() == "timespan":
            frame[col_name] = [pandas.to_timedelta(t) if t is not None else "nan" for t in frame[col_name]]
        elif col_type.lower() == "dynamic":
            frame[col_name] = frame[col_name].apply(
                lambda x: json.loads(x) if x and isinstance(x, text_type) else x if x else None
            )
        elif col_type in kusto_to_dataframe_data_types:
            pandas_type = kusto_to_dataframe_data_types[col_type]
            frame[col_name] = frame[col_name].astype(pandas_type, errors="raise" if raise_errors else "ignore")

    return frame
