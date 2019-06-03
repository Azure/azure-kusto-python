from __future__ import annotations

import builtins
import json
from dataclasses import dataclass, asdict
from datetime import datetime

import maya
from typing_inspect import is_generic_type, get_args

from kit.helpers import to_snake_case

reserved_keywords = set(dir(builtins))


def to_python_name(prop_name):
    prop_name = to_snake_case(prop_name)

    prop_name = prop_name.replace('$', '_')

    if prop_name in reserved_keywords:
        prop_name = '_' + prop_name

    return prop_name


@dataclass
class SerializableModel:
    @classmethod
    def fromdict(cls, d) -> SerializableModel:
        # translate to PEP
        d = {to_python_name(k): v for k, v in d.items()}

        ctor_params = {}
        for field in cls.__dataclass_fields__.values():
            if d.get(field.name) is not None:
                T = eval(field.type)
                if T in [int, str, float, bool, float]:
                    # validate type
                    ctor_params[field.name] = T(d[field.name])
                elif T is datetime:
                    ctor_params[field.name] = maya.when(d[field.name]).datetime()
                elif is_generic_type(T):
                    # go deep
                    T_of = get_args(T)[0]
                    if issubclass(T_of, SerializableModel):
                        ctor_params[field.name] = [T_of.fromdict(nested_dict) for nested_dict in d[field.name]]
                    else:
                        ctor_params[field.name] = d[field.name]
                else:
                    raise ValueError(f'Unexpected field type {T}')

        return cls(**ctor_params)

    def to_json(self):
        return json.dumps(asdict(self))
