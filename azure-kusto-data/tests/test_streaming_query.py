import os

import pytest

from azure.kusto.data._models import WellKnownDataSet, KustoResultRow, KustoResultColumn
from azure.kusto.data.aio._models import KustoStreamingResponseDataSet as AsyncKustoStreamingResponseDataSet
from azure.kusto.data.aio.streaming_response import JsonTokenReader as AsyncJsonTokenReader, ProgressiveDataSetEnumerator as AsyncProgressiveDataSetEnumerator
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.response import KustoStreamingResponseDataSet
from azure.kusto.data.streaming_response import JsonTokenReader, ProgressiveDataSetEnumerator, FrameType
from tests.kusto_client_common import KustoClientTestsMixin


class MockAioFile:
    def __init__(self, filename):
        self.filename = filename

    def __enter__(self):
        self.file = open(self.filename, "rb")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()

    async def read(self, n=-1):
        return self.file.read(n)


class TestStreamingQuery(KustoClientTestsMixin):
    """Tests class for KustoClient API"""

    @staticmethod
    def open_json_file(file_name: str):
        return open(os.path.join(os.path.dirname(__file__), "input", file_name), "rb")

    @staticmethod
    def open_async_json_file(file_name: str):
        return MockAioFile(os.path.join(os.path.dirname(__file__), "input", file_name))

    def test_sanity(self):
        with self.open_json_file("deft.json") as f:
            reader = ProgressiveDataSetEnumerator(JsonTokenReader(f))

            for i in reader:
                if i["FrameType"] == FrameType.DataTable and i["TableKind"] == WellKnownDataSet.PrimaryResult.value:
                    columns = [KustoResultColumn(column, index) for index, column in enumerate(i["Columns"])]
                    self._assert_sanity_query_primary_results(KustoResultRow(columns, r) for r in i["Rows"])

    def test_dynamic(self):
        with self.open_json_file("dynamic.json") as f:
            reader = ProgressiveDataSetEnumerator(JsonTokenReader(f))

            for i in reader:
                if i["FrameType"] == FrameType.DataTable and i["TableKind"] == WellKnownDataSet.PrimaryResult.value:
                    row = next(i["Rows"])
                    self._assert_dynamic_response(row)

    def test_sanity_kusto_streaming_response_dataset(self):
        with self.open_json_file("deft.json") as f:
            reader = ProgressiveDataSetEnumerator(JsonTokenReader(f))

            response = KustoStreamingResponseDataSet(reader)
            self._assert_sanity_query_primary_results(response.get_current_primary_results_table())

    def test_exception_in_row(self):
        with self.open_json_file("query_partial_results_defer_is_false.json") as f:
            reader = ProgressiveDataSetEnumerator(JsonTokenReader(f))

            response = KustoStreamingResponseDataSet(reader)
            table = response.get_current_primary_results_table()
            with pytest.raises(KustoServiceError):
                rows = [r for r in table.rows]

    @pytest.mark.asyncio
    async def test_sanity_async(self):
        with self.open_async_json_file("deft.json") as f:
            reader = AsyncProgressiveDataSetEnumerator(AsyncJsonTokenReader(f))

            async for i in reader:
                if i["FrameType"] == FrameType.DataTable and i["TableKind"] == WellKnownDataSet.PrimaryResult.value:
                    columns = [KustoResultColumn(column, index) for index, column in enumerate(i["Columns"])]
                    rows = [KustoResultRow(columns, r) async for r in i["Rows"]]
                    self._assert_sanity_query_primary_results(rows)

    @pytest.mark.asyncio
    async def test_dynamic_async(self):
        with self.open_async_json_file("dynamic.json") as f:
            reader = AsyncProgressiveDataSetEnumerator(AsyncJsonTokenReader(f))
            async for i in reader:
                if i["FrameType"] == FrameType.DataTable and i["TableKind"] == WellKnownDataSet.PrimaryResult.value:
                    row = await i["Rows"].__anext__()
                    self._assert_dynamic_response(row)

    @pytest.mark.asyncio
    async def test_sanity_kusto_streaming_response_dataset_async(self):
        with self.open_async_json_file("deft.json") as f:
            reader = AsyncProgressiveDataSetEnumerator(AsyncJsonTokenReader(f))

            response = await AsyncKustoStreamingResponseDataSet.create(reader)
            self._assert_sanity_query_primary_results([x async for x in response.get_current_primary_results_table()])

    @pytest.mark.asyncio
    async def test_exception_in_row_async(self):
        with self.open_async_json_file("query_partial_results_defer_is_false.json") as f:
            reader = AsyncProgressiveDataSetEnumerator(AsyncJsonTokenReader(f))

            response = await AsyncKustoStreamingResponseDataSet.create(reader)
            table = response.get_current_primary_results_table()
            with pytest.raises(KustoServiceError):
                rows = [r async for r in table.rows]
