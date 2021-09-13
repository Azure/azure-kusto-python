import os
from io import StringIO

import pytest

from azure.kusto.data._models import WellKnownDataSet, KustoResultRow, KustoResultColumn
from azure.kusto.data.aio._models import KustoStreamingResponseDataSet as AsyncKustoStreamingResponseDataSet
from azure.kusto.data.aio.streaming_response import JsonTokenReader as AsyncJsonTokenReader, ProgressiveDataSetEnumerator as AsyncProgressiveDataSetEnumerator
from azure.kusto.data.exceptions import KustoServiceError, KustoStreamingQueryError, KustoTokenParsingError
from azure.kusto.data.response import KustoStreamingResponseDataSet
from azure.kusto.data.streaming_response import JsonTokenReader, ProgressiveDataSetEnumerator, FrameType, JsonTokenType
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


class AsyncStringIO:
    def __init__(self, string):
        self.string_io = StringIO(string)

    async def read(self, n=-1):
        return self.string_io.read(n)


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

            # Before reading all of the tables these results won't be available
            with pytest.raises(KustoStreamingQueryError):
                errors_count = response.errors_count
            with pytest.raises(KustoStreamingQueryError):
                exceptions = response.get_exceptions()
            # Can't advance by default until current table is finished
            with pytest.raises(KustoStreamingQueryError):
                response.next_primary_results_table()

            self._assert_sanity_query_primary_results(response.current_primary_results_table)

            assert response.next_primary_results_table() is None
            assert response.errors_count == 0
            assert response.get_exceptions() == []

            # After we finish the tables we're left with None
            assert response.next_primary_results_table() is None

    def test_exception_in_row(self):
        with self.open_json_file("query_partial_results_defer_is_false.json") as f:
            reader = ProgressiveDataSetEnumerator(JsonTokenReader(f))

            response = KustoStreamingResponseDataSet(reader)
            table = response.current_primary_results_table
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

            # Before reading all of the tables these results won't be available
            with pytest.raises(KustoStreamingQueryError):
                errors_count = response.errors_count
            with pytest.raises(KustoStreamingQueryError):
                exceptions = response.get_exceptions()
            # Can't advance by default until current table is finished
            with pytest.raises(KustoStreamingQueryError):
                await response.next_primary_results_table()

            self._assert_sanity_query_primary_results([x async for x in response.current_primary_results_table])

            assert (await response.next_primary_results_table()) is None
            assert response.errors_count == 0
            assert response.get_exceptions() == []

            # After we finish the tables we're left with None
            assert (await response.next_primary_results_table()) is None

    @pytest.mark.asyncio
    async def test_exception_in_row_async(self):
        with self.open_async_json_file("query_partial_results_defer_is_false.json") as f:
            reader = AsyncProgressiveDataSetEnumerator(AsyncJsonTokenReader(f))

            response = await AsyncKustoStreamingResponseDataSet.create(reader)
            table = response.current_primary_results_table
            with pytest.raises(KustoServiceError):
                rows = [r async for r in table.rows]


class TestJsonTokenReader:
    def get_reader(self, data) -> JsonTokenReader:
        return JsonTokenReader(StringIO(data))

    def get_async_reader(self, data) -> AsyncJsonTokenReader:
        return AsyncJsonTokenReader(AsyncStringIO(data))

    def test_reading_token(self):
        reader = self.get_reader("{")
        assert reader.read_next_token_or_throw().token_type == JsonTokenType.START_MAP
        with pytest.raises(KustoTokenParsingError):
            reader.read_next_token_or_throw()

    def test_types(self):
        reader = self.get_reader('{"junk": [[[]]], "key": ["a",false,3.63]}')
        assert reader.read_start_object().token_type == JsonTokenType.START_MAP
        assert reader.skip_until_property_name("key").token_type == JsonTokenType.MAP_KEY
        assert reader.read_start_array().token_type == JsonTokenType.START_ARRAY
        assert reader.read_string() == "a"
        assert reader.read_boolean() is False
        assert reader.read_number() == 3.63

    @pytest.mark.asyncio
    async def test_reading_token_async(self):
        reader = self.get_async_reader("{")
        assert (await reader.read_next_token_or_throw()).token_type == JsonTokenType.START_MAP
        with pytest.raises(KustoTokenParsingError):
            await reader.read_next_token_or_throw()

    @pytest.mark.asyncio
    async def test_types_async(self):
        reader = self.get_async_reader('{"junk": [[[]]], "key": ["a",false,3.63]}')
        assert (await reader.read_start_object()).token_type == JsonTokenType.START_MAP
        assert (await reader.skip_until_property_name("key")).token_type == JsonTokenType.MAP_KEY
        assert (await reader.read_start_array()).token_type == JsonTokenType.START_ARRAY
        assert (await reader.read_string()) == "a"
        assert (await reader.read_boolean()) is False
        assert (await reader.read_number()) == 3.63
