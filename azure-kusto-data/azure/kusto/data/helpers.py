"""Kusto helper functions"""

import pandas
from ._models import KustoResultTable
from dateutil.tz import UTC


def timespan_column_parser(data):
    factor = 1
    if data and data.startswith("-"):
        factor = -1
        data = data[1:]

    return pandas.Timedelta(data.replace(".", " days ", 1) if data and "." in data.split(":")[0] else data) * factor


def dataframe_from_result_table(table):
    """Converts Kusto tables into pandas DataFrame.
    :param azure.kusto.data._models.KustoResultTable table: Table recieved from the response.
    :return: pandas DataFrame.
    :rtype: pandas.DataFrame
    """
    if not table:
        raise ValueError()

    if not isinstance(table, KustoResultTable):
        raise TypeError("Expected KustoResultTable got {}".format(type(table).__name__))

    frame = pandas.DataFrame(table._rows, columns=[col.column_name for col in table.columns])

    for col in table.columns:
        if col.column_type == "bool":
            # TODO: this may be wrong
            frame[col.column_name] = frame[col.column_name].astype(bool)
        if col.column_type == "datetime":
            frame[col.column_name] = pandas.to_datetime(frame[col.column_name], utc=True).dt.tz_convert(UTC)
        if col.column_type == "timespan":
            frame[col.column_name] = frame[col.column_name].apply(timespan_column_parser)

    return frame
