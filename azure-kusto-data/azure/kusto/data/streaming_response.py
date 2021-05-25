import io
from enum import Enum
from typing import Optional, Any, Tuple, Dict

import dataclasses
import ijson


@dataclasses.dataclass
class JsonToken:
    token_path: str
    token_type: str
    token_value: Optional[Any]


class JsonTokenReader:
    def __init__(self, stream: io.RawIOBase):
        self.reader = io.BufferedReader(stream)
        self.json_iter = ijson.parse(self.reader)

    def read_next_token_or_throw(self) -> JsonToken:
        next_item = next(self.json_iter)
        if next_item is None:
            raise Exception("Unexpected end of stream")  # todo - better exception

        return JsonToken(*next_item)

    def read_token_of_type(self, *token_types: str) -> JsonToken:
        token = self.read_next_token_or_throw()
        if token.token_type not in token_types:
            raise Exception(f"Expected one the following types: '{','.join(token_types)}' , got type {token.token_type}")  # todo - better exception
        return token

    def read_start_object(self) -> JsonToken:
        return self.read_token_of_type("start_map")

    def read_start_array(self) -> JsonToken:
        return self.read_token_of_type("start_array")

    def read_string(self) -> str:
        return self.read_token_of_type("string").token_value

    def read_boolean(self) -> bool:
        return self.read_token_of_type("boolean").token_value

    def read_number(self) -> float:
        return self.read_token_of_type("number").token_value

    def skip_to_end(self):
        for i in self.json_iter:
            pass

    def skip_children(self, prev_token):
        if prev_token.token_type == "map_key":
            self.read_next_token_or_throw()
        elif prev_token.token_type.startswith("start"):
            while True:
                potential_end_token = self.read_next_token_or_throw()
                if potential_end_token.token_path == prev_token and potential_end_token.token_type.startswith("end"):
                    break

    def skip_until_property_name(self, name: str):
        while True:
            token = self.read_token_of_type("map_key")
            if token.token_value == name:
                return token

            self.skip_children(token)

    def skip_until_property_name_or_end_object(self, *names: str) -> JsonToken:
        while True:
            token = self.read_next_token_or_throw()
            if token.token_type == "end_map":
                return token

            if token.token_type == "map_key":
                if token.token_value in names:
                    return token

                self.skip_children(token)

            raise Exception(f"Unexpected token {token}")

    def skip_until_token(self, *tokens: str):
        while True:
            token = self.read_next_token_or_throw()
            if token.token_type in tokens:
                return token
            self.skip_children(token)

    def skip_until_end_object(self):
        self.skip_until_token("end_map")


class FrameType(Enum):
    DataSetHeader = 0
    TableHeader = 1
    TableFragment = 2
    TableCompletion = 3
    TableProgress = 4
    DataTable = 5
    DataSetCompletion = 6


class ProgressiveDataSetEnumerator:
    def __init__(self, reader: JsonTokenReader):
        self.reader = reader

    def __iter__(self):
        self.done = False
        self.reader.read_start_array()
        return self

    def __next__(self):
        if self.done:
            raise StopIteration()

        token = self.reader.skip_until_token("start_map", "end_array")
        if token == "end_array":
            self.done = True
            raise StopIteration()

        frame_type = self.read_frame_type()

        if frame_type == FrameType.DataSetHeader:
            return self.extract_props(frame_type, ("IsProgressive", "boolean"), ("Version", "string"))
        elif frame_type == FrameType.TableHeader:
            return self.extract_props(frame_type, ("TableId", "number"), ("TableKind", "string"), ("TableName", "string"), ("Columns", "array"))
        elif frame_type == FrameType.TableFragment:
            return self.extract_props(frame_type, ("TableFragmentType", "string"), ("TableId", "number"), ("Rows", "array"))
        elif frame_type == FrameType.TableCompletion:
            return self.extract_props(frame_type, ("TableId", "number"), ("RowCount", "number"))
        elif frame_type == FrameType.TableProgress:
            return self.extract_props(frame_type, ("TableId", "number"), ("TableProgress", "number"))
        elif frame_type == FrameType.DataTable:
            props = self.extract_props(frame_type, ("TableId", "number"), ("TableKind", "string"), ("TableName", "string"), ("Columns", "array"))
            self.reader.skip_until_property_name("Rows")
            props["Rows"] = self.row_iterator(props["Columns"])
            if props["TableKind"] != "PrimaryResult" or True:
                props["Rows"] = list(props["Rows"])
            return props
        elif frame_type == FrameType.DataSetCompletion:
            res = self.extract_props(frame_type, ("HasErrors", "boolean"), ("Cancelled", "boolean"))
            token = self.reader.skip_until_property_name_or_end_object("OneApiErrors")
            if token.token_type != "end_map":
                res["OneApiErrors"] = self.parse_array(skip_start=True)
            return res

    def row_iterator(self, columns):
        self.reader.read_token_of_type("start_array")
        while True:
            token = self.reader.read_token_of_type("start_array", "end_array")
            if token.token_type == "end_array":
                return
            row = {}
            for i in range(len(columns)):
                token = self.reader.read_next_token_or_throw()
                if token.token_type == "start_map":
                    row[columns[i]["ColumnName"]] = self.parse_object(skip_start=True)
                elif token.token_type == "start_array":
                    row[columns[i]["ColumnName"]] = self.parse_array(skip_start=True)
                else:
                    row[columns[i]["ColumnName"]] = token.token_value
            self.reader.read_token_of_type("end_array")
            yield row

    def parse_array(self, skip_start):
        if not skip_start:
            self.reader.read_start_array()
        arr = []

        while True:
            token = self.reader.read_token_of_type("null", "boolean", "number", "string", "start_map", "start_array", "end_array")

            if token.token_type == "end_array":
                return arr

            if token.token_type == "start_map":
                arr.append(self.parse_object(skip_start=True))
            elif token.token_type == "start_array":
                arr.append(self.parse_array(skip_start=True))
            else:
                arr.append(token.token_value)

    def parse_object(self, skip_start):
        if not skip_start:
            self.reader.read_start_object()

        obj = {}
        while True:
            token_prop_name = self.reader.read_token_of_type("map_key", "end_map")
            if token_prop_name.token_type == "end_map":
                return obj
            prop_name = token_prop_name.token_value

            token = self.reader.read_token_of_type("null", "boolean", "number", "string", "start_map", "start_array")

            if token.token_type == "start_map":
                obj[prop_name] = self.parse_object(skip_start=True)
            elif token.token_type == "start_array":
                obj[prop_name] = self.parse_array(skip_start=True)
            else:
                obj[prop_name] = token.token_value


    def extract_props(self, frame_type, *props: Tuple[str, str]) -> Dict[str, Any]:
        result = {"frame_type": frame_type}
        for (name, type) in props:
            token = self.reader.skip_until_property_name(name)
            if type == "object":
                result[name] = self.parse_object(skip_start=False)
            elif type == "array":
                result[name] = self.parse_array(skip_start=False)
            else:
                result[name] = self.reader.read_token_of_type(type).token_value
        return result

    def read_frame_type(self) -> FrameType:
        self.reader.skip_until_property_name("FrameType")
        return FrameType[self.reader.read_string()]


class KustoJsonDataStreamReader:
    def __init__(self, reader):
        self.reader: JsonTokenReader = reader

    def read_preamble(self) -> bool:
        # {
        self.reader.read_start_object()

        # "Version" : "X.Y -- Optional
        token = self.reader.skip_until_property_name_or_end_object("Version", "Tables", "error")
        if token.token_type == "end_map":
            raise Exception("There is no table in the stream")

        if token.token_value == "Version":
            version = self.reader.read_string()
            if not version.startswith("1."):
                raise Exception("Unexpected version")
            self.reader.skip_until_property_name_or_end_object("Tables")
        elif token.token_value == "error":
            self.reader.read_start_object()
            return False

        self.reader.read_start_array()
        return True
