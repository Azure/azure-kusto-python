import os
from io import StringIO

import pytest

from azure.kusto.data._models import WellKnownDataSet, KustoResultRow, KustoResultColumn
from azure.kusto.data.aio.response import KustoStreamingResponseDataSet as AsyncKustoStreamingResponseDataSet
from azure.kusto.data.aio.streaming_response import JsonTokenReader as AsyncJsonTokenReader, StreamingDataSetEnumerator as AsyncProgressiveDataSetEnumerator
from azure.kusto.data.exceptions import KustoServiceError, KustoStreamingQueryError, KustoTokenParsingError, KustoUnsupportedApiError, KustoMultiApiError
from azure.kusto.data.response import KustoStreamingResponseDataSet
from azure.kusto.data.streaming_response import JsonTokenReader, StreamingDataSetEnumerator, FrameType, JsonTokenType
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
            reader = StreamingDataSetEnumerator(JsonTokenReader(f))

            for i in reader:
                if i["FrameType"] == FrameType.DataTable and i["TableKind"] == WellKnownDataSet.PrimaryResult.value:
                    columns = [KustoResultColumn(column, index) for index, column in enumerate(i["Columns"])]
                    self._assert_sanity_query_primary_results(KustoResultRow(columns, r) for r in i["Rows"])

    def test_alternative_order(self):
        with self.open_json_file("alternative_order.json") as f:
            reader = StreamingDataSetEnumerator(JsonTokenReader(f))

            for i in reader:
                if i["FrameType"] == FrameType.DataTable and i["TableKind"] == WellKnownDataSet.PrimaryResult.value:
                    columns = [KustoResultColumn(column, index) for index, column in enumerate(i["Columns"])]
                    self._assert_sanity_query_primary_results(KustoResultRow(columns, r) for r in i["Rows"])

    def test_progressive_unsupported(self):
        with self.open_json_file("progressive_result.json") as f:
            reader = StreamingDataSetEnumerator(JsonTokenReader(f))

            with pytest.raises(KustoUnsupportedApiError):
                for _ in reader:
                    pass

        with self.open_json_file("deft_with_progressive_result.json") as f:
            reader = StreamingDataSetEnumerator(JsonTokenReader(f))

            with pytest.raises(KustoUnsupportedApiError):
                for _ in reader:
                    pass

    def test_dynamic(self):
        with self.open_json_file("dynamic.json") as f:
            reader = StreamingDataSetEnumerator(JsonTokenReader(f))

            for i in reader:
                if i["FrameType"] == FrameType.DataTable and i["TableKind"] == WellKnownDataSet.PrimaryResult.value:
                    row = next(i["Rows"])
                    self._assert_dynamic_response(row)

    def test_sanity_kusto_streaming_response_dataset(self):
        with self.open_json_file("deft.json") as f:
            reader = StreamingDataSetEnumerator(JsonTokenReader(f))

            response = KustoStreamingResponseDataSet(reader)
            primary_tables = response.iter_primary_results()

            # Before reading all of the tables these results won't be available
            with pytest.raises(KustoStreamingQueryError):
                errors_count = response.errors_count
            with pytest.raises(KustoStreamingQueryError):
                exceptions = response.get_exceptions()

            first_table = next(primary_tables)

            # Can't advance by default until current table is finished
            with pytest.raises(KustoStreamingQueryError):
                next(primary_tables)

            self._assert_sanity_query_primary_results(first_table)

            assert next(primary_tables, None) is None
            assert response.errors_count == 0
            assert response.get_exceptions() == []

            assert response.finished

    def test_exception_in_row(self):
        with self.open_json_file("query_partial_results_defer_is_false.json") as f:
            reader = StreamingDataSetEnumerator(JsonTokenReader(f))

            response = KustoStreamingResponseDataSet(reader)
            table = next(response.iter_primary_results())
            with pytest.raises(KustoServiceError):
                rows = [r for r in table]

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
    async def test_alternative_order_async(self):
        with self.open_async_json_file("alternative_order.json") as f:
            reader = AsyncProgressiveDataSetEnumerator(AsyncJsonTokenReader(f))

            async for i in reader:
                if i["FrameType"] == FrameType.DataTable and i["TableKind"] == WellKnownDataSet.PrimaryResult.value:
                    columns = [KustoResultColumn(column, index) for index, column in enumerate(i["Columns"])]
                    rows = [KustoResultRow(columns, r) async for r in i["Rows"]]
                    self._assert_sanity_query_primary_results(rows)

    @pytest.mark.asyncio
    async def test_progressive_unsupported_async(self):
        with self.open_async_json_file("progressive_result.json") as f:
            reader = AsyncProgressiveDataSetEnumerator(AsyncJsonTokenReader(f))

            with pytest.raises(KustoUnsupportedApiError):
                async for _ in reader:
                    pass

        with self.open_async_json_file("deft_with_progressive_result.json") as f:
            reader = AsyncProgressiveDataSetEnumerator(AsyncJsonTokenReader(f))

            with pytest.raises(KustoUnsupportedApiError):
                async for _ in reader:
                    pass

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

            response = AsyncKustoStreamingResponseDataSet(reader)
            primary_tables = response.iter_primary_results()

            # Before reading all of the tables these results won't be available
            with pytest.raises(KustoStreamingQueryError):
                errors_count = response.errors_count
            with pytest.raises(KustoStreamingQueryError):
                exceptions = response.get_exceptions()

            first_table = await primary_tables.__anext__()

            # Can't advance by default until current table is finished
            with pytest.raises(KustoStreamingQueryError):
                await primary_tables.__anext__()

            self._assert_sanity_query_primary_results([x async for x in first_table])

            with pytest.raises(StopAsyncIteration):
                await primary_tables.__anext__()
            assert response.errors_count == 0
            assert response.get_exceptions() == []

            assert response.finished

    @pytest.mark.asyncio
    async def test_exception_in_row_async(self):
        with self.open_async_json_file("query_partial_results_defer_is_false.json") as f:
            reader = AsyncProgressiveDataSetEnumerator(AsyncJsonTokenReader(f))

            response = AsyncKustoStreamingResponseDataSet(reader)
            table = await response.iter_primary_results().__anext__()
            with pytest.raises(KustoMultiApiError):
                rows = [r async for r in table]


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

    def test_read_token_of_type(self):
        reader = self.get_reader("{}")
        assert reader.read_token_of_type(JsonTokenType.START_MAP).token_type == JsonTokenType.START_MAP
        with pytest.raises(KustoTokenParsingError):
            reader.read_token_of_type(JsonTokenType.START_MAP)

    def test_types(self):
        reader = self.get_reader('{"junk": [[[]]], "key": ["a",false,3.63]}')
        assert reader.read_start_object().token_type == JsonTokenType.START_MAP
        assert reader.skip_until_property_name("key").token_type == JsonTokenType.MAP_KEY
        assert reader.read_start_array().token_type == JsonTokenType.START_ARRAY
        assert reader.read_string() == "a"
        assert reader.read_boolean() is False
        assert reader.read_number() == 3.63

    def test_skip(self):
        reader = self.get_reader('{"junk": [[[]]], "key": "a", "junk2": {"key2": "aaaaa"}, "key2": "www"}')
        assert reader.read_start_object().token_type == JsonTokenType.START_MAP
        key = reader.skip_until_any_property_name("key", "junk2")
        assert key.token_type == JsonTokenType.MAP_KEY
        assert reader.read_string() == "a"
        key2 = reader.skip_until_property_name_or_end_object("key2")
        assert key2.token_type == JsonTokenType.MAP_KEY
        assert key2.token_path == ""
        assert reader.read_string() == "www"
        assert reader.skip_until_property_name_or_end_object().token_type == JsonTokenType.END_MAP

    @pytest.mark.asyncio
    async def test_reading_token_async(self):
        reader = self.get_async_reader("{")
        assert (await reader.read_next_token_or_throw()).token_type == JsonTokenType.START_MAP
        with pytest.raises(KustoTokenParsingError):
            await reader.read_next_token_or_throw()

    @pytest.mark.asyncio
    async def test_read_token_of_type_async(self):
        reader = self.get_async_reader("{}")
        assert (await reader.read_token_of_type(JsonTokenType.START_MAP)).token_type == JsonTokenType.START_MAP
        with pytest.raises(KustoTokenParsingError):
            await reader.read_token_of_type(JsonTokenType.START_MAP)

    @pytest.mark.asyncio
    async def test_types_async(self):
        reader = self.get_async_reader('{"junk": [[[]]], "key": ["a",false,3.63]}')
        assert (await reader.read_start_object()).token_type == JsonTokenType.START_MAP
        assert (await reader.skip_until_property_name("key")).token_type == JsonTokenType.MAP_KEY
        assert (await reader.read_start_array()).token_type == JsonTokenType.START_ARRAY
        assert (await reader.read_string()) == "a"
        assert (await reader.read_boolean()) is False
        assert (await reader.read_number()) == 3.63

    @pytest.mark.asyncio
    async def test_skip_async(self):
        reader = self.get_async_reader('{"junk": [[[]]], "key": "a", "junk2": {"key2": "aaaaa"}, "key2": "www" }')
        assert (await reader.read_start_object()).token_type == JsonTokenType.START_MAP
        key = await reader.skip_until_any_property_name("key", "junk2")
        assert key.token_type == JsonTokenType.MAP_KEY
        assert (await reader.read_string()) == "a"
        key2 = await reader.skip_until_property_name_or_end_object("key2")
        assert key2.token_type == JsonTokenType.MAP_KEY
        assert key2.token_path == ""
        assert (await reader.read_string()) == "www"
        assert (await reader.skip_until_property_name_or_end_object()).token_type == JsonTokenType.END_MAP
