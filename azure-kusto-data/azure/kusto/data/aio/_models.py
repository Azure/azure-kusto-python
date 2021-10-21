from azure.kusto.data._models import WellKnownDataSet, KustoResultColumn, KustoResultRow
from azure.kusto.data.exceptions import KustoStreamingQueryError


class KustoStreamingResultTable:
    """Iterator over a Kusto result table."""

    def __init__(self, json_table: dict):
        self.table_name = json_table.get("TableName")
        self.table_id = json_table.get("TableId")
        self.table_kind = WellKnownDataSet[json_table["TableKind"]] if "TableKind" in json_table else None
        self.columns = [KustoResultColumn(column, index) for index, column in enumerate(json_table["Columns"])]

        self.raw_columns = json_table["Columns"]
        self.raw_rows = json_table["Rows"]
        self.kusto_result_rows = None

        self.finished = False
        self.row_count = 0

    @property
    def rows(self):
        if self.finished:
            raise KustoStreamingQueryError("Can't retrieve rows after iteration is finished")
        return self.__aiter__()

    @property
    def rows_count(self) -> int:
        if not self.finished:
            raise KustoStreamingQueryError("Can't retrieve rows count before the iteration is finished")
        return self.row_count

    @property
    def columns_count(self) -> int:
        return len(self.columns)

    def __len__(self):
        if not self.finished:
            return None
        return self.rows_count

    async def __aiter__(self):
        while not self.finished:
            try:
                row = await self.raw_rows.__anext__()
            except StopAsyncIteration:
                self.finished = True
                break
            self.row_count += 1
            yield KustoResultRow(self.columns, row)

    def __bool__(self):
        return any(self.columns)

    __nonzero__ = __bool__
