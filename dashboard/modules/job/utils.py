import logging
import os
from typing import Iterator, Optional

logger = logging.getLogger(__name__)


def file_tail_iterator(path: str) -> Iterator[Optional[str]]:
    """Yield lines from a file as it's written.

    Returns lines in batches opportunistically.

    Returns None until the file exists or if no new line has been written.
    """
    if not isinstance(path, str):
        raise TypeError(f"path must be a string, got {type(path)}.")

    while not os.path.exists(path):
        logger.debug(f"Path {path} doesn't exist yet.")
        yield None

    with open(path, "r") as f:
        lines = ""
        while True:
            curr_line = f.readline()
            # readline() returns empty string when there's no new line.
            if curr_line:
                lines += curr_line
            else:
                yield lines or None
                lines = ""
