import builtins
from typing import List, Any, Callable, Iterable, Generic, TypeVar
import sys

import ray

# TODO(ekl) how do we express ObjectRef[Block?
BlockRef = List
T = TypeVar("T")
U = TypeVar("U")


class Block:
    def __init__(self, items: List[Any]):
        raise NotImplementedError

    def __iter__(self):
        raise NotImplementedError

    def __len__(self):
        raise NotImplementedError

    def size_bytes(self):
        raise NotImplementedError


class ListBlock(Block):
    def __init__(self, items: List[Any]):
        self._items = items

    def __iter__(self):
        return self._items.__iter__()

    def __len__(self):
        return len(self._items)

    def size_bytes(self):
        # TODO
        return sys.getsizeof(self._items)


class Dataset(Generic[T]):
    def __init__(self, blocks: List[BlockRef], block_cls: Any):
        self._blocks: List[BlockRef] = blocks
        self._block_cls = block_cls

    def map(self, fn: Callable[[T], U]) -> "Dataset[U]":
        @ray.remote
        def transform(block):
            return self._block_cls([fn(row) for row in block])

        return Dataset([transform.remote(b) for b in self._blocks],
                       self._block_cls)

    def flat_map(self, fn: Callable[[T], Iterable[U]]) -> "Dataset[U]":
        @ray.remote
        def transform(block):
            output = []
            for row in block:
                for r2 in fn(row):
                    output.append(r2)
            return self._block_cls(output)

        return Dataset([transform.remote(b) for b in self._blocks],
                       self._block_cls)

    def filter(self, fn: Callable[[T], bool]) -> "Dataset[T]":
        @ray.remote
        def transform(block):
            return self._block_cls([row for row in block if fn(row)])

        return Dataset([transform.remote(b) for b in self._blocks],
                       self._block_cls)

    def take(self, limit: int = 20) -> List[T]:
        output = []
        for b in self._blocks:
            for row in ray.get(b):
                output.append(row)
            if len(output) >= limit:
                break
        return output


def range(n: int, num_blocks: int = 200) -> Dataset[int]:
    block_size = max(1, n // num_blocks)
    blocks: List[BlockRef] = []
    i = 0

    @ray.remote
    def gen_block(start: int, count: int) -> Block:
        return ListBlock(list(builtins.range(start, start + count)))

    while i < n:
        blocks.append(gen_block.remote(block_size * i, min(block_size, n - i)))
        i += block_size

    return Dataset(blocks, ListBlock)


def read_parquet(directory: str) -> Dataset[tuple]:
    pass


def read_files(directory: str) -> Dataset[bytes]:
    pass
