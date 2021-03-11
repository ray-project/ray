import pathlib
from typing import List


def dispatch(request: str, args: List[str]):
    if request == "DEL_FILE" and len(args) == 1:
        pathlib.Path(args[0]).unlink(missing_ok=True)
