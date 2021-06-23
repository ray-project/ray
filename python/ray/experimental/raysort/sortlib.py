from typing import Iterable, List

import numpy as np

from ray.experimental.raysort.types import BlockInfo


def get_boundaries(num_parts: int) -> List[int]:
    return [0] * num_parts


def sort_and_partition(part: np.ndarray,
                       boundaries: List[int]) -> List[BlockInfo]:
    N = len(boundaries)
    offset = 0
    size = int(np.ceil(part.size / N))
    blocks = []
    for _ in range(N):
        blocks.append((offset, size))
        offset += size
    return blocks


def merge_partitions(blocks: List[np.ndarray],
                     _n: int) -> Iterable[memoryview]:
    for block in blocks:
        yield block
