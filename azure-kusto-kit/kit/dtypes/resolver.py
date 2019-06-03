import json

import maya
import pyarrow

from kit.dtypes import KustoType


class KustoTypeResolver:
    strict_bool = ['true', 'false']
    loose_bool = ['T', 'True', 't', 'F', 'False', 'f', '0', '1'] + strict_bool
    valid_year = (1, 2100)

    @classmethod
    def from_arrow_type(cls, arrow_type: pyarrow.lib.DataType, strict: bool = True):
        if pyarrow.types.is_binary(arrow_type):
            return KustoType.BOOL
        elif pyarrow.types.is_int8(arrow_type) or pyarrow.types.is_int16(arrow_type) or pyarrow.types.is_int32(arrow_type):
            return KustoType.INT
        elif pyarrow.types.is_int64(arrow_type):
            return KustoType.LONG
        elif pyarrow.types.is_float16(arrow_type) or pyarrow.types.is_float32(arrow_type):
            return KustoType.REAL
        elif pyarrow.types.is_float64(arrow_type):
            return KustoType.DECIMAL
        elif pyarrow.types.is_date(arrow_type):
            return KustoType.DATETIME
        # elif pyarrow.dtypes.is_time(arrow_type):
        #     return KustoType.TIMESPAN
        elif pyarrow.types.is_map(arrow_type) or pyarrow.types.is_nested(arrow_type) or pyarrow.types.is_list(arrow_type):
            return KustoType.DYNAMIC
        else:
            return KustoType.STRING

    @classmethod
    def from_string(cls, string: str, strict: bool = True):
        # TODO: missing timespan / GUID handling as well
        if type(string) is not str:
            raise ValueError('Expected a string')

        try:
            """Simple dtypes fall here: string, int, float"""
            evaluated = eval(string)
            evaluated_type = type(evaluated)

            if evaluated_type is int:
                if not strict:
                    try:
                        if cls.valid_year[0] < maya.MayaDT(evaluated).year < cls.valid_year[1]:
                            return KustoType.DATETIME
                    except:
                        pass
                if evaluated > 2 ** 31 or evaluated < -1 * (2 ** 31):
                    return KustoType.LONG
                return KustoType.INT
            if evaluated_type is float:
                # TODO: need to handle decimal properly
                return KustoType.REAL
            if not strict and evaluated_type in [dict, list, tuple, set]:
                return KustoType.DYNAMIC
            if evaluated_type is str:
                try:
                    if cls.valid_year[0] < maya.parse(evaluated).year < cls.valid_year[1]:
                        return KustoType.DATETIME
                except:
                    pass

                try:
                    json.loads(string)
                    return KustoType.DYNAMIC
                except:
                    pass
        except:
            """Special dtypes fall here: bool, datetime, timespan"""
            bool_values = cls.strict_bool if strict else cls.loose_bool

            if string in bool_values:
                return KustoType.BOOL

            if not strict:
                try:
                    if cls.valid_year[0] < maya.parse(evaluated).year < cls.valid_year[1]:
                        return KustoType.DATETIME
                except:
                    pass

        return KustoType.STRING
