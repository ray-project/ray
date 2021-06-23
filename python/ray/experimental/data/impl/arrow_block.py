from typing import List, Any, Callable, Iterable, Dict

import pyarrow as pa

import ray
from ray.experimental.data.dataset import Dataset
from ray.experimental.data.impl.block import Block


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
                    if key not in columns_by_names:
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
                        if key not in columns_by_names:
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
