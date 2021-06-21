import builtins
from typing import List, Dict, Any, Callable, Iterable, Generic, TypeVar
import sys
import pyarrow as pa
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


class ArrowTableAccessor:
    def __init__(self, table):
        self._table = table

    def __getitem__(self, key, index=0):
        return self._table.to_pydict()[key][index]

    def to_pydict(self):
        return {key: self[key] for key in self._table.column_names}


class ArrowBlock(Block):
    def __init__(self, table: pa.Table):
        self._table = table
        self._cur = -1

    def __iter__(self):
        return self

    def __next__(self):
        self._cur += 1
        if self._cur < len(self):
            return self._table.slice(self._cur, 1)
        raise StopIteration

    def __len__(self):
        return self._table.num_rows

    def size_bytes(self):
        return self._table.nbytes

    def serialize(self):
        return self._table.to_pydict()

    @staticmethod
    def deserialize(dict: Dict):
        return ArrowBlock(pa.Table.from_pydict(dict))


class ArrowDataset(Dataset[ArrowBlock]):
    def __init__(self, blocks: List[Any], block_cls: ArrowBlock):
        self._blocks: List[Any] = blocks
        self._block_cls = block_cls

    def map(self, fn: Callable[[], Dict]) -> Dict:
        @ray.remote
        def transform(block):
            columns_by_names = {}
            arrow_block = ArrowBlock.deserialize(block)
            for row in arrow_block:
                result = fn(ArrowTableAccessor(row))
                for key in result:
                    if not key in columns_by_names:
                        columns_by_names[key] = []
                    columns_by_names[key].append(result[key])
            return columns_by_names

        return ArrowDataset([transform.remote(b) for b in self._blocks],
                            self._block_cls)

    def flat_map(self, fn: Callable[[], Iterable]) -> "ArrowDataset":
        @ray.remote
        def transform(block):
            columns_by_names = {}
            arrow_block = ArrowBlock.deserialize(block)
            for row in arrow_block:
                results = fn(ArrowTableAccessor(row))
                for result in results:
                    for key in result:
                        if not key in columns_by_names:
                            columns_by_names[key] = []
                        columns_by_names[key].append(result[key])
            return columns_by_names

        return ArrowDataset([transform.remote(b) for b in self._blocks],
                            self._block_cls)

    def filter(self, fn: Callable[[], bool]) -> "ArrowDataset":
        @ray.remote
        def transform(block):
            arrow_block = ArrowBlock.deserialize(block)
            tables = [
                row for row in arrow_block if fn(ArrowTableAccessor(row))
            ]
            tables.append(arrow_block._table.schema.empty_table())
            return ArrowBlock(pa.concat_tables(tables)).serialize()

        return ArrowDataset([transform.remote(b) for b in self._blocks],
                            self._block_cls)

    def take(self, limit: int = 20) -> List[Dict]:
        output = []
        for b in self._blocks:
            for row in ArrowBlock.deserialize(ray.get(b)):
                output.append(ArrowTableAccessor(row).to_pydict())
            if len(output) >= limit:
                break
        return output
