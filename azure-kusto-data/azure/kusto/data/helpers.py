"""Kusto helper functions"""
import six


def to_pandas_datetime(raw_value):
    import pandas as pd

    return pd.to_datetime(raw_value)


def to_pandas_timedelta(raw_value, timedelta_value):
    import pandas as pd

    if isinstance(raw_value, (six.integer_types, float)):
        # https://docs.microsoft.com/en-us/dotnet/api/system.datetime.ticks
        # kusto saves up to ticks, 1 tick == 100 nanoseconds
        return pd.Timedelta(raw_value * 100, unit="ns")
    if isinstance(raw_value, six.string_types):
        fraction = raw_value.split(".")[-1]
        if fraction.isdigit():
            whole_part = int(timedelta_value.total_seconds())
            time_with_exact_fraction = str(whole_part) + "." + fraction
            total_seconds = float(time_with_exact_fraction)

            return pd.Timedelta(total_seconds, unit="s")
        else:
            return pd.Timedelta(timedelta_value)

    return pd.Timedelta(timedelta_value.total_seconds(), unit="ns")


def dataframe_from_result_table(table):
    import pandas as pd
    from ._models import KustoResultTable
    from dateutil.tz import UTC

    """Converts Kusto tables into pandas DataFrame.
    :param azure.kusto.data._models.KustoResultTable table: Table received from the response.
    :return: pandas DataFrame.
    :rtype: pandas.DataFrame
    """
    if not table:
        raise ValueError()

    if not isinstance(table, KustoResultTable):
        raise TypeError("Expected KustoResultTable got {}".format(type(table).__name__))

    columns = [col.column_name for col in table.columns]
    frame = pd.DataFrame(table._rows, columns=columns)

    # fix types
    for col in table.columns:
        if col.column_type == "bool":
            frame[col.column_name] = frame[col.column_name].astype(bool)

    return frame
