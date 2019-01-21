"""Kusto helper functions"""

import pandas


def dataframe_from_result_table(table):
    """Converts Kusto tables into pandas DataFrame.
    :param azure.kusto.data._models.KustoResultTable table: Table recieved from the response.
    :param bool raise_errors: Will raise errors if set to true, and swallow otherwise.
    :return: pandas DataFrame.
    :rtype: pandas.DataFrame
    """

    if not table.columns or not table.rows:
        return pandas.DataFrame()

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
