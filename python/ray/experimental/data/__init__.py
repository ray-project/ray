import builtins
import sys
from typing import List, Any, Callable, Iterable, Generic, TypeVar, Union, \
    Optional

import pyarrow.parquet as pq

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

    def show(self, limit: int = 20) -> None:
        for row in self.take(limit):
            print(row)


def range(n: int, parallelism: int = 200) -> Dataset[int]:
    block_size = max(1, n // parallelism)
    blocks: List[BlockRef] = []
    i = 0

    @ray.remote
    def gen_block(start: int, count: int) -> Block:
        return ListBlock(list(builtins.range(start, start + count)))

    while i < n:
        blocks.append(gen_block.remote(block_size * i, min(block_size, n - i)))
        i += block_size

    return Dataset(blocks, ListBlock)


def read_parquet(paths: Union[str, List[str]],
                 parallelism: int = 200,
                 columns: Optional[List[str]] = None,
                 **kwargs) -> Dataset[Any]:
    """Read parquet format data from hdfs like filesystem into a Dataset.

    .. code-block:: python

        # create dummy data
        spark.range(...).write.parquet(...)
        # create Dataset
        data = ray.util.data.read_parquet(...)

    Args:
        paths (Union[str, List[str]): a single file path or a list of file path
        columns (Optional[List[str]]): a list of column names to read
        kwargs: the other parquet read options

    Returns:
        A Dataset
    """
    pq_ds = pq.ParquetDataset(paths, **kwargs)
    pieces = pq_ds.pieces
    data_pieces = []

    for piece in pieces:
        num_row_groups = piece.get_metadata().to_dict()["num_row_groups"]
        for i in builtins.range(num_row_groups):
            data_pieces.append(
                pq.ParquetDatasetPiece(piece.path, piece.open_file_func,
                                       piece.file_options, i,
                                       piece.partition_keys))

    # TODO(ekl) also enforce max size limit of blocks
    read_tasks = [[] for _ in builtins.range(parallelism)]
    for i, piece in enumerate(pieces):
        read_tasks[i].append(piece)
    nonempty_tasks = [r for r in read_tasks if r]
    partitions = pq_ds.partitions

    @ray.remote
    def gen_read(pieces: List[pq.ParquetDatasetPiece]):
        print("Reading {} parquet pieces".format(len(pieces)))
        table = piece.read(
            columns=columns, use_threads=False, partitions=partitions)
        rows = []
        for rb in table.to_batches():
            for row in zip(
                    *[rb.column(i) for i in builtins.range(rb.num_columns)]):
                rows.append([v.as_py() for v in row])
        return ListBlock(rows)

    return Dataset([gen_read.remote(ps) for ps in nonempty_tasks], ListBlock)


def read_files(directory: str) -> Dataset[bytes]:
    pass


if __name__ == "__main__":
    import os
    import pandas as pd
    import pyarrow as pa
    tmp_path = "/tmp/f"
    ray.init()

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    table = pa.Table.from_pandas(df1)
    pq.write_table(table, os.path.join(tmp_path, "test1.parquet"))
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    table = pa.Table.from_pandas(df2)
    pq.write_table(table, os.path.join(tmp_path, "test2.parquet"))

    ds = ray.experimental.data.read_parquet(tmp_path)
    import IPython
    IPython.embed()
    ds.show()
