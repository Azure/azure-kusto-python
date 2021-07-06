from typing import Any, Tuple, Dict

import aiohttp
import ijson

from azure.kusto.data._models import WellKnownDataSet
from azure.kusto.data.streaming_response import JsonTokenType, FrameType, JsonToken


class JsonTokenReader:
    def __init__(self, stream: aiohttp.StreamReader):
        self.json_iter = ijson.parse_async(stream)

    async def read_next_token_or_throw(self) -> JsonToken:
        next_item = await self.json_iter.__anext__()
        if next_item is None:
            raise Exception("Unexpected end of stream")  # todo - better exception
        (token_path, token_type, token_value) = next_item

        return JsonToken(token_path, JsonTokenType[token_type.upper()], token_value)

    async def read_token_of_type(self, *token_types: JsonTokenType) -> JsonToken:
        token = await self.read_next_token_or_throw()
        if token.token_type not in token_types:
            raise Exception(
                f"Expected one the following types: '{','.join(t.name for t in token_types)}' , got type {token.token_type}"
            )  # todo - better exception
        return token

    async def read_start_object(self) -> JsonToken:
        return await self.read_token_of_type(JsonTokenType.START_MAP)

    async def read_start_array(self) -> JsonToken:
        return await self.read_token_of_type(JsonTokenType.START_ARRAY)

    async def read_string(self) -> str:
        return (await self.read_token_of_type(JsonTokenType.STRING)).token_value

    async def read_boolean(self) -> bool:
        return (await self.read_token_of_type(JsonTokenType.BOOLEAN)).token_value

    async def read_number(self) -> float:
        return (await self.read_token_of_type(JsonTokenType.NUMBER)).token_value

    async def skip_to_end(self):
        async for _ in self.json_iter:
            pass

    async def skip_children(self, prev_token: JsonToken):
        if prev_token.token_type == JsonTokenType.MAP_KEY:
            await self.read_next_token_or_throw()
        elif prev_token.token_type in JsonTokenType.start_tokens():
            while True:
                potential_end_token = await self.read_next_token_or_throw()
                if potential_end_token.token_path == prev_token.token_path and potential_end_token.token_type in JsonTokenType.end_tokens():
                    break

    async def skip_until_property_name(self, name: str):
        while True:
            token = await self.read_token_of_type(JsonTokenType.MAP_KEY)
            if token.token_value == name:
                return token

            await self.skip_children(token)

    async def skip_until_property_name_or_end_object(self, *names: str) -> JsonToken:
        while True:
            token = await self.read_next_token_or_throw()
            if token.token_type == JsonTokenType.END_MAP:
                return token

            if token.token_type == JsonTokenType.MAP_KEY:
                if token.token_value in names:
                    return token

                await self.skip_children(token)

            raise Exception(f"Unexpected token {token}")

    async def skip_until_token(self, *tokens: JsonTokenType):
        while True:
            token = await self.read_next_token_or_throw()
            if token.token_type in tokens:
                return token
            await self.skip_children(token)

    async def skip_until_token_with_paths(self, *tokens: (JsonTokenType, str)):
        while True:
            token = await self.read_next_token_or_throw()
            if any((token.token_type == t_type and token.token_path == t_path) for (t_type, t_path) in tokens):
                return token
            await self.skip_children(token)

    async def skip_until_end_object(self):
        await self.skip_until_token(JsonTokenType.END_MAP)


class ProgressiveDataSetEnumerator:
    def __init__(self, reader: JsonTokenReader):
        self.reader = reader

    async def __aiter__(self):
        self.done = False
        await self.reader.read_start_array()
        return self

    async def __anext__(self):
        if self.done:
            raise StopIteration()

        token = await self.reader.skip_until_token_with_paths((JsonTokenType.START_MAP, "item"), (JsonTokenType.END_ARRAY, ""))
        if token == JsonTokenType.END_ARRAY:
            self.done = True
            raise StopIteration()

        frame_type = await self.read_frame_type()

        if frame_type == FrameType.DataSetHeader:
            return await self.extract_props(frame_type, ("IsProgressive", JsonTokenType.BOOLEAN), ("Version", JsonTokenType.STRING))
        elif frame_type == FrameType.TableHeader:
            return await self.extract_props(
                frame_type,
                ("TableId", JsonTokenType.NUMBER),
                ("TableKind", JsonTokenType.STRING),
                ("TableName", JsonTokenType.STRING),
                ("Columns", JsonTokenType.START_ARRAY),
            )
        elif frame_type == FrameType.TableFragment:
            return await self.extract_props(
                frame_type, ("TableFragmentType", JsonTokenType.STRING), ("TableId", JsonTokenType.NUMBER), ("Rows", JsonTokenType.START_ARRAY)
            )
        elif frame_type == FrameType.TableCompletion:
            return await self.extract_props(frame_type, ("TableId", JsonTokenType.NUMBER), ("RowCount", JsonTokenType.NUMBER))
        elif frame_type == FrameType.TableProgress:
            return await self.extract_props(frame_type, ("TableId", JsonTokenType.NUMBER), ("TableProgress", JsonTokenType.NUMBER))
        elif frame_type == FrameType.DataTable:
            props = await self.extract_props(
                frame_type,
                ("TableId", JsonTokenType.NUMBER),
                ("TableKind", JsonTokenType.STRING),
                ("TableName", JsonTokenType.STRING),
                ("Columns", JsonTokenType.START_ARRAY),
            )
            await self.reader.skip_until_property_name("Rows")
            props["Rows"] = self.row_iterator(props["Columns"])
            if props["TableKind"] != WellKnownDataSet.PrimaryResult.value:
                props["Rows"] = [r async for r in props["Rows"]]
            return props
        elif frame_type == FrameType.DataSetCompletion:
            res = await self.extract_props(frame_type, ("HasErrors", JsonTokenType.BOOLEAN), ("Cancelled", JsonTokenType.BOOLEAN))
            token = await self.reader.skip_until_property_name_or_end_object("OneApiErrors")
            if token.token_type != JsonTokenType.END_MAP:
                res["OneApiErrors"] = self.parse_array(skip_start=False)
            return res

    async def row_iterator(self, columns):
        await self.reader.read_token_of_type(JsonTokenType.START_ARRAY)
        while True:
            token = await self.reader.read_token_of_type(JsonTokenType.START_ARRAY, JsonTokenType.END_ARRAY)
            if token.token_type == JsonTokenType.END_ARRAY:
                return
            row = {}
            for i in range(len(columns)):
                token = await self.reader.read_next_token_or_throw()
                if token.token_type == JsonTokenType.START_MAP:
                    row[columns[i]["ColumnName"]] = await self.parse_object(skip_start=True)
                elif token.token_type == JsonTokenType.START_ARRAY:
                    row[columns[i]["ColumnName"]] = await self.parse_array(skip_start=True)
                else:
                    row[columns[i]["ColumnName"]] = token.token_value
            await self.reader.read_token_of_type(JsonTokenType.END_ARRAY)
            yield row

    async def parse_array(self, skip_start):
        if not skip_start:
            await self.reader.read_start_array()
        arr = []

        while True:
            token = await self.reader.read_token_of_type(
                JsonTokenType.NULL,
                JsonTokenType.BOOLEAN,
                JsonTokenType.NUMBER,
                JsonTokenType.STRING,
                JsonTokenType.START_MAP,
                JsonTokenType.START_ARRAY,
                JsonTokenType.END_ARRAY,
            )

            if token.token_type == JsonTokenType.END_ARRAY:
                return arr

            if token.token_type == JsonTokenType.START_MAP:
                arr.append(await self.parse_object(skip_start=True))
            elif token.token_type == JsonTokenType.START_ARRAY:
                arr.append(await self.parse_array(skip_start=True))
            else:
                arr.append(token.token_value)

    async def parse_object(self, skip_start):
        if not skip_start:
            await self.reader.read_start_object()

        obj = {}
        while True:
            token_prop_name = await self.reader.read_token_of_type(JsonTokenType.MAP_KEY, JsonTokenType.END_MAP)
            if token_prop_name.token_type == JsonTokenType.END_MAP:
                return obj
            prop_name = token_prop_name.token_value

            token = await self.reader.read_token_of_type(
                JsonTokenType.NULL, JsonTokenType.BOOLEAN, JsonTokenType.NUMBER, JsonTokenType.STRING, JsonTokenType.START_MAP, JsonTokenType.START_ARRAY
            )

            if token.token_type == JsonTokenType.START_MAP:
                obj[prop_name] = await self.parse_object(skip_start=True)
            elif token.token_type == JsonTokenType.START_ARRAY:
                obj[prop_name] = await self.parse_array(skip_start=True)
            else:
                obj[prop_name] = token.token_value

    async def extract_props(self, frame_type, *props: Tuple[str, JsonTokenType]) -> Dict[str, Any]:
        result = {"frame_type": frame_type}
        for (name, type) in props:
            await self.reader.skip_until_property_name(name)
            if type == JsonTokenType.START_ARRAY:
                result[name] = await self.parse_array(skip_start=False)
            else:
                result[name] = (await self.reader.read_token_of_type(type)).token_value
        return result

    async def read_frame_type(self) -> FrameType:
        await self.reader.skip_until_property_name("FrameType")
        return FrameType[await self.reader.read_string()]


class KustoJsonDataStreamReader:
    def __init__(self, reader):
        self.reader: JsonTokenReader = reader

    async def read_preamble(self) -> bool:
        # {
        await self.reader.read_start_object()

        # "Version" : "X.Y -- Optional
        token = await self.reader.skip_until_property_name_or_end_object("Version", "Tables", "error")
        if token.token_type == JsonTokenType.END_MAP:
            raise Exception("There is no table in the stream")

        if token.token_value == "Version":
            version = await self.reader.read_string()
            if not version.startswith("1."):
                raise Exception("Unexpected version")
            await self.reader.skip_until_property_name_or_end_object("Tables")
        elif token.token_value == "error":
            await self.reader.read_start_object()
            return False

        await self.reader.read_start_array()
        return True
