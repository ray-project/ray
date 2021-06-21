from typing import List, Any, Callable, Iterable

import ray

# TODO(ekl) how do we express ObjectRef[Block?
BlockRef = List


class Block:
    def __init__(self, items: List[Any]):
        raise NotImplementedError

    def __iter__(self):
        raise NotImplementedError

    def __len__(self):
        raise NotImplementedError

    def size_bytes(self):
        raise NotImplementedError


class Dataset:
    def __init__(self, blocks: List[BlockRef]):
        self._blocks: List[BlockRef] = blocks

    def map(self, fn: Callable[[Any], Any]) -> "Dataset":
        @ray.remote
        def transform(block):
            return Block([fn(row) for row in block])

        return Dataset([transform.remote(b) for b in self._blocks])

    def flat_map(self, fn: Callable[[Any], Iterable[Any]]) -> "Dataset":
        @ray.remote
        def transform(block):
            output = []
            for row in block:
                for r2 in fn(row):
                    output.append(r2)
            return Block(output)

        return Dataset([transform.remote(b) for b in self._blocks])

    def filter(self, fn: Callable[[Any], bool]) -> "Dataset":
        @ray.remote
        def transform(block):
            return Block([row for row in block if fn(row)])

        return Dataset([transform.remote(b) for b in self._blocks])

    def take(self, limit: int = 20) -> List[Any]:
        output = []
        for b in self._blocks:
            for row in ray.get(b):
                output.append(row)
            if len(output) >= limit:
                break
        return output


def range(n: int, num_blocks: int = 200) -> Dataset:
    block_size = max(1, n // num_blocks)
    blocks: List[BlockRef] = []
    i = 0

    @ray.remote
    def gen_block(start: int, count: int) -> Block:
        return Block(list(range(start, start + count)))

    while i < num_blocks:
        blocks.append(gen_block.remote(block_size * i, block_size))

    return Dataset(blocks)


def read_parquet(directory: str) -> Dataset:
    pass


def read_files(directory: str) -> Dataset:
    pass
