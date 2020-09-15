# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
def to_pandas_datetime(raw_value, *args):
    """
    Transform a raw python value to a pandas datetime (datetime64)
    """
    import pandas as pd

    return pd.to_datetime(raw_value)


def to_pandas_timedelta(raw_value, timedelta_value) -> "pandas.Timedelta":
    """
    Transform a raw python value to a pandas timedelta.
    """
    import pandas as pd

    if isinstance(raw_value, (int, float)):
        # https://docs.microsoft.com/en-us/dotnet/api/system.datetime.ticks
        # kusto saves up to ticks, 1 tick == 100 nanoseconds
        return pd.Timedelta(raw_value * 100, unit="ns")
    if isinstance(raw_value, str):
        fraction = raw_value.split(".")[-1]
        if fraction.isdigit():
            whole_part = int(timedelta_value.total_seconds())
            time_with_exact_fraction = str(whole_part) + "." + fraction
            total_seconds = float(time_with_exact_fraction)

            return pd.Timedelta(total_seconds, unit="s")

        return pd.Timedelta(timedelta_value)

    return pd.Timedelta(timedelta_value.total_seconds(), unit="ns")


def to_decimal(raw_value, *args):
    from decimal import Decimal

    return Decimal(raw_value.strip(' "'))


def dataframe_from_result_table(table: "KustoResultTable"):
    """Converts Kusto tables into pandas DataFrame.
    :param azure.kusto.data._models.KustoResultTable table: Table received from the response.
    :return: pandas DataFrame.
    """
    import pandas as pd

    if not table:
        raise ValueError()

    from azure.kusto.data._models import KustoResultTable

    if not isinstance(table, KustoResultTable):
        raise TypeError("Expected KustoResultTable got {}".format(type(table).__name__))

    columns = [col.column_name for col in table.columns]
    frame = pd.DataFrame(table._rows, columns=columns)

    # fix types
    for col in table.columns:
        if col.column_type == "bool":
            frame[col.column_name] = frame[col.column_name].astype(bool)

    return frame
