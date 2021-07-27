from typing import Callable, List

import ray
from ray.types import ObjectRef
from ray.experimental.data.block import Block, BlockMetadata, T
from ray.experimental.data.impl.block_list import BlockList


class LazyBlockList(BlockList[T]):
    def __init__(self, calls: Callable[[], ObjectRef[Block]],
                 metadata: List[BlockMetadata], max_concurrency: int):
        assert len(calls) == len(metadata), (calls, metadata)
        self._metadata = metadata
        self._calls = calls
        self._max_concurrency = max_concurrency

        self._materialized = []

    def __len__(self):
        return len(self._calls)

    def __iter__(self):
        outer = self

        def gen():
            nonlocal outer
            for i in range(len(self._calls)):
                outer._ensure_concurrency()
                to_return = outer._materialized[i]
                ray.wait([to_return], fetch_local=False)
                yield outer._materialized[i]

        return gen()

    def _ensure_concurrency(self):
        start = len(self._materialized)
        end_of_buffer = len(self._materialized) + self._max_concurrency
        end = min(len(self._calls), end_of_buffer)
        for i in range(start, end):
            self._materialized.append(self._calls[i]())
