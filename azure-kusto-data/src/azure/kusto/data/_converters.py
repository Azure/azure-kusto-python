# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import io
import re
from datetime import timedelta, datetime
from decimal import Decimal
from pprint import pprint
from typing import List, Any, Union, Type, TypeVar, Generic, IO, Dict, Generator
from uuid import UUID

import msgspec
from dateutil import parser
from msgspec.structs import force_setattr

# Regex for TimeSpan
_TIMESPAN_PATTERN = re.compile(r"(-?)((?P<d>[0-9]*).)?(?P<h>[0-9]{2}):(?P<m>[0-9]{2}):(?P<s>[0-9]{2}(\.[0-9]+)?$)")

T = TypeVar("T")


class KustoJsonReader:
    def __init__(self, data: Union[io.RawIOBase, io.BufferedReader]):
        if isinstance(data, io.RawIOBase):
            data = io.BufferedReader(data)
        self.data: io.BufferedReader = data
        self.after_start = False

    def lines(self) -> Generator[memoryview, None, None]:
        self.handle_start()
        while True:
            res = self.data.readline()
            if res == b"]\n":
                break
            yield memoryview(res)[1:-1]  # strip the comma and newline

    def handle_start(self):
        if not self.after_start:
            res = self.data.peek(1)
            if not res or res[0] != b"["[0]:
                raise ValueError("Invalid JSON format")
            self.after_start = True


def kusto_raw(name: str, items: Dict[str, Type], **kwargs):
    tuples = []
    timedeltas = []
    for f, t in items.items():
        if t is timedelta or t is Union[timedelta, None]:
            name = "__timedelta_" + f + "__"
            tuples.append((name, Union[str, None], msgspec.field(name=f)))
            timedeltas.append((f, name))
        else:
            tuples.append((f, t))

    new = msgspec.defstruct(name, tuples, frozen=True, array_like=True, dict=True, **kwargs)

    if timedeltas:

        class Final(new, frozen=True, array_like=True, dict=True):
            def __post_init__(self):
                for f, name in timedeltas:
                    force_setattr(self, f, to_timedelta(getattr(self, name)))

        return Final

    return new


def kusto(**kwargs):
    def f(cls):
        return kusto_raw(cls.__name__, cls.__annotations__, **kwargs)

    return f


@kusto()
class Data:
    vnum: int
    vdec: Decimal
    vdate: datetime
    vspan: timedelta
    vspan2: timedelta
    vspan3: timedelta
    vobj: Any
    vb: bool
    vreal: float
    vstr: str
    vlong: int
    vguid: UUID


@kusto(rename="pascal")
class QueryProperties:
    table_id: int
    key: str
    value: Any


@kusto(rename="pascal")
class QueryCompletionInformation:
    timestamp: datetime
    client_request_id: str
    activity_id: UUID
    sub_activity_id: UUID
    parent_activity_id: UUID
    level: int
    level_name: str
    status_code: int
    status_code_name: str
    event_type: int
    event_type_name: str
    payload: str


class Frame(msgspec.Struct, frozen=True, tag_field="FrameType", rename="pascal"):
    pass


class DataSetHeader(Frame, frozen=True):
    is_progressive: bool
    version: str
    is_fragmented: bool
    error_reporting_placement: str


class Column(msgspec.Struct, frozen=True, rename="pascal"):
    column_name: str
    column_type: str


class DataTable(Frame, Generic[T], frozen=True):
    table_id: int
    table_kind: str
    table_name: str
    columns: List[Column]
    rows: List[T]


class TableHeader(Frame, frozen=True):
    table_id: int
    table_kind: str
    table_name: str
    columns: List[Column]


class TableFragment(Frame, Generic[T], frozen=True):
    table_fragment_type: str
    table_id: int
    rows: List[T]


class TableCompletion(Frame, frozen=True):
    table_id: int
    row_count: int


class DataSetCompletion(Frame, frozen=True):
    has_errors: bool
    cancelled: bool


def decode_frame(data: memoryview, t: Type[T]) -> T:
    frame = msgspec.json.decode(data.tobytes(), type=t)
    print(frame)
    return frame


kusto_to_python = {
    "int": Union[int, None],
    "decimal": Union[Decimal, None],
    "datetime": Union[datetime, None],
    "timespan": Union[timedelta, None],
    "dynamic": Any,
    "bool": Union[bool, None],
    "real": Union[float, None],
    "string": str,
    "long": Union[int, None],
    "guid": Union[UUID, None],
}


def decode_dataset(data: KustoJsonReader) -> Generator[DataTable, None, None]:
    lines = data.lines()
    header = decode_frame(lines.__next__(), DataSetHeader)

    props = decode_frame(lines.__next__(), DataTable[QueryProperties])
    yield props

    while True:
        frame = decode_frame(lines.__next__(), Union[DataTable[QueryCompletionInformation], TableHeader, DataSetCompletion])
        if isinstance(frame, TableHeader):
            header = frame
            mapper = kusto_raw(frame.table_name, {c.column_name: kusto_to_python[c.column_type] for c in frame.columns})
            rows = []
            while True:
                frame = decode_frame(lines.__next__(), Union[TableFragment[mapper], TableCompletion])
                if isinstance(frame, TableFragment):
                    rows.extend(frame.rows)
                if isinstance(frame, TableCompletion):
                    yield DataTable(table_id=frame.table_id, table_kind=header.table_kind, table_name=header.table_name, columns=header.columns, rows=rows)
                    break
        if isinstance(frame, DataSetCompletion):
            break
        if isinstance(frame, DataTable):
            yield frame


def to_datetime(value):
    """Converts a string to a datetime."""
    if isinstance(value, int):
        return datetime.fromtimestamp(value)
    return parser.isoparse(value)


def to_timedelta(value):
    """Converts a string to a timedelta."""
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return timedelta(microseconds=(float(value) / 10))
    match = _TIMESPAN_PATTERN.match(value)
    if match:
        if match.group(1) == "-":
            factor = -1
        else:
            factor = 1
        return factor * timedelta(days=int(match.group("d") or 0), hours=int(match.group("h")), minutes=int(match.group("m")), seconds=float(match.group("s")))
    else:
        raise ValueError("Timespan value '{}' cannot be decoded".format(value))


strip = open(r"D:\azure-kusto-go\azkustodata\query\v2\testData\validFrames.json", "rb")

values = list(decode_dataset(KustoJsonReader(strip)))
pprint(list(values))
