from typing import Any, List


def split(items: List[Any], chunk_size: int):
    """Splits provided list into chunks of given size"""

    assert chunk_size > 0, "Chunk size has to be > 0"

    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]
