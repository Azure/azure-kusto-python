"""Kusto helper functions"""

import pandas
from ._models import KustoResultTable


def dataframe_from_result_table(table):
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
    frame = pandas.DataFrame(table._rows, columns=columns)

    # fix types
    for col in table.columns:
        if col.column_type == "bool":
            frame[col.column_name] = frame[col.column_name].astype(bool)
        elif col.column_type == "datetime":
            # as string first because can be None due to previous conversions
            frame[col.column_name] = pandas.to_datetime(frame[col.column_name], utc=True)
        elif col.column_type == "timespan":
            # as string first because can be None due to previous conversions
            frame[col.column_name] = pandas.to_timedelta(frame[col.column_name], unit='s')

    return frame
