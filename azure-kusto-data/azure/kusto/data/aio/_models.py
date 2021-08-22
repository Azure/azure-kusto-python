from typing import Optional, List

from azure.kusto.data._models import WellKnownDataSet, KustoResultColumn, KustoResultRow, KustoResultTable
from azure.kusto.data.aio.streaming_response import ProgressiveDataSetEnumerator
from azure.kusto.data.exceptions import KustoStreamingError
from azure.kusto.data.response import BaseKustoResponseDataSet
from azure.kusto.data.streaming_response import FrameType


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
            raise KustoStreamingError("Can't retrieve rows after iteration is finished")
        return self.__aiter__()

    @property
    def rows_count(self) -> int:
        if not self.finished:
            raise KustoStreamingError("Can't retrieve rows count before the iteration is finished")
        return self.row_count

    @property
    def columns_count(self) -> int:
        return len(self.columns)

    def to_dict(self):
        """Converts the table to a dict."""
        return {"name": self.table_name, "kind": self.table_kind}

    def __len__(self):
        if not self.finished:
            return None
        return self.rows_count

    async def __aiter__(self):
        while True:
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


class KustoStreamingResponseDataSet(BaseKustoResponseDataSet):
    _status_column = "Payload"
    _error_column = "Level"
    _crid_column = "ClientRequestId"

    current_primary_results_table: KustoStreamingResultTable

    async def extract_tables_until_primary_result(self):
        while True:
            table = await self.streamed_data.__anext__()
            if table["FrameType"] != FrameType.DataTable:
                continue
            if self.streamed_data.started_primary_results:
                self.current_primary_results_table = KustoStreamingResultTable(table)
                self.tables.append(self.current_primary_results_table)
                break
            else:
                self.tables.append(KustoResultTable(table))

    @staticmethod
    async def create(streamed_data: ProgressiveDataSetEnumerator) -> "KustoStreamingResponseDataSet":
        data_set = KustoStreamingResponseDataSet(streamed_data)
        await data_set.extract_tables_until_primary_result()
        return data_set

    def __init__(self, streamed_data: ProgressiveDataSetEnumerator):
        self.tables = []
        self.streamed_data = streamed_data
        self.have_read_rest_of_tables = False

    async def next_primary_results_table(self, ensure_current_finished=True) -> Optional[KustoStreamingResultTable]:
        if self.have_read_rest_of_tables:
            return None
        if ensure_current_finished and not self.current_primary_results_table.finished:
            raise KustoStreamingError(
                "Tried retrieving a new primary_result table before the old one was finished. To override pass `ensure_current_finished=False`"
            )

        table = await self.streamed_data.__anext__()
        if self.streamed_data.finished_primary_results:
            # If we're finished with primary results, we want to retrieve the rest of the tables
            if table["FrameType"] == FrameType.DataTable:
                self.tables.append(KustoResultTable(table))
            await self.read_rest_of_tables()
        else:
            self.current_primary_results_table = KustoStreamingResultTable(table)
            return self.current_primary_results_table

    async def read_rest_of_tables(self, ensure_primary_tables_finished=True):
        if self.have_read_rest_of_tables:
            return

        if ensure_primary_tables_finished and not self.streamed_data.finished_primary_results:
            raise KustoStreamingError(
                "Tried retrieving all of the tables before the primary_results are finished. To override pass `ensure_primary_tables_finished=False`"
            )

        table = [KustoResultTable(t) async for t in self.streamed_data if t["FrameType"] == FrameType.DataTable]
        self.tables.extend(table)
        self.have_read_rest_of_tables = True

    @property
    def errors_count(self) -> int:
        if not self.have_read_rest_of_tables:
            raise KustoStreamingError(
                "Unable to get errors count before reading all of the tables. Advance `next_primary_results_table` to the end, or use `read_rest_of_tables`"
            )
        return super().errors_count

    def get_exceptions(self) -> List[str]:
        if not self.have_read_rest_of_tables:
            raise KustoStreamingError(
                "Unable to get errors count before reading all of the tables. Advance `next_primary_results_table` to the end, or use `read_rest_of_tables`"
            )
        return super().get_exceptions()

    def __getitem__(self, key) -> KustoResultTable:
        if isinstance(key, int):
            return self.tables[key]
        try:
            return next(t for t in self.tables if t.table_name == key)
        except StopIteration:
            raise LookupError(key)

    def __len__(self) -> int:
        return len(self.tables)
