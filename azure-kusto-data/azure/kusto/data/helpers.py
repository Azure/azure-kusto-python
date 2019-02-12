"""Kusto helper functions"""

import pandas
from ._models import KustoResultTable


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

    frame = pandas.DataFrame.from_records(
        [row.to_list() for row in table.rows], columns=[col.column_name for col in table.columns]
    )
    bool_columns = [col.column_name for col in table.columns if col.column_type == "bool"]
    for col in bool_columns:
        frame[col] = frame[col].astype(bool)

    for i in range(len(table.rows)):
        seventh = table.rows[i]._seventh_digit
        for name in seventh.keys():
            frame.loc[i, name] += pandas.Timedelta(seventh[name] * 100, unit="ns")

    return frame
