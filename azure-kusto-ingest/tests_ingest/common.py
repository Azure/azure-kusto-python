import pathlib
from typing import Optional


def get_input_folder_path(file: Optional[str] = None) -> pathlib.Path:
    path = pathlib.Path(__file__).parent.joinpath("input")
    if file:
        path = path.joinpath(file)
    return path
