from typing import TypeVar, Dict, List

T = TypeVar("T")
U = TypeVar("U")


def invert_dict_of_lists(d: Dict[T, List[U]]) -> Dict[U, List[T]]:
    """
    Invert a dictionary of lists.
    EG {'a': [1, 2], 'b': [2, 3]} -> {1: ['a'], 2: ['a', 'b'], 3: ['b']}
    """
    new_dict = {}
    for k, v in d.items():
        for x in v:
            new_dict.setdefault(x, []).append(k)

    return new_dict
