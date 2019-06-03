from __future__ import annotations

from dataclasses import dataclass

from kit.dtypes import KustoType
from kit.models.serializable import SerializableModel


@dataclass
class Column(SerializableModel):
    name: str
    dtype: str = KustoType.STRING.value
    index: int = None

    def __init__(self, name: str, dtype: str = None, index: int = None, data_type: KustoType = None):
        self.name = name
        if data_type:
            self.dtype = data_type.value
        elif dtype:
            self.dtype = dtype
        else:
            raise ValueError('Missing data type property')
        self.index = index

    @property
    def data_type(self):
        return KustoType(self.dtype)

    @data_type.setter
    def data_type(self, v: KustoType):
        self.dtype = v.value

    @property
    def column_definition(self):
        return '{0.name}:{0.dtype}'.format(self)

