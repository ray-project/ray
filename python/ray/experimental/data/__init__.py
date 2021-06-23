import builtins
import sys
from typing import List, Any, Callable, Iterable, Generic, TypeVar, Union, \
    Optional

import pyarrow.parquet as pq
import pyarrow as pa

import ray

# TODO(ekl) how do we express ObjectRef[Block?
BlockRef = List
T = TypeVar("T")
U = TypeVar("U")


class Block(Generic[T]):
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


class ComputePool:
    def apply(self, fn: Any, blocks: List[Block[T]]) -> List[BlockRef]:
        raise NotImplementedError


class TaskPool(ComputePool):
    def apply(self, fn: Any, remote_args: dict,
              blocks: List[Block[T]]) -> List[BlockRef]:
        fn = ray.remote(**remote_args)(fn)
        return [fn.remote(b) for b in blocks]


class ActorPool(ComputePool):
    def apply(self, fn: Any, remote_args: dict,
              blocks: List[Block[T]]) -> List[BlockRef]:
        @ray.remote(**remote_args)
        class Worker:
            def ready(self):
                print("Worker created")
                return "ok"

            def process_block(self, block: Block[T]) -> Block[U]:
                return fn(block)

        workers = [Worker.remote()]
        tasks = {w.ready.remote(): w for w in workers}
        ready_workers = set()
        blocks_in = [b for b in blocks]
        blocks_out = []

        while len(blocks_out) < len(blocks):
            ready, _ = ray.wait(list(tasks), timeout=0.01, num_returns=1)
            if not ready:
                if len(ready_workers) / len(workers) > 0.75:
                    w = Worker.remote()
                    workers.append(w)
                    tasks[w.ready.remote()] = w
                    print("Creating new worker, {} pending, {} total".format(
                        len(workers) - len(ready_workers), len(workers)))
                continue

            [obj_id] = ready
            worker = tasks[obj_id]
            del tasks[obj_id]

            # Process task result.
            if worker in ready_workers:
                blocks_out.append(obj_id)
            else:
                ready_workers.add(worker)

            # Schedule a new task.
            if blocks_in:
                tasks[worker.process_block.remote(blocks_in.pop())] = worker

        return blocks_out


def get_compute(compute_spec: str) -> ComputePool:
    if compute_spec == "tasks":
        return TaskPool()
    elif compute_spec == "actors":
        return ActorPool()
    else:
        raise ValueError("compute must be one of [`tasks`, `actors`]")


class Dataset(Generic[T]):
    def __init__(self, blocks: List[BlockRef], block_cls: Any):
        self._blocks: List[BlockRef] = blocks
        self._block_cls = block_cls

    def map(self, fn: Callable[[T], U], compute="tasks",
            **remote_args) -> "Dataset[U]":
        def transform(block):
            return self._block_cls([fn(row) for row in block])

        compute = get_compute(compute)

        return Dataset(
            compute.apply(transform, remote_args, self._blocks),
            self._block_cls)

    def flat_map(self,
                 fn: Callable[[T], Iterable[U]],
                 compute="tasks",
                 **remote_args) -> "Dataset[U]":
        def transform(block):
            output = []
            for row in block:
                for r2 in fn(row):
                    output.append(r2)
            return self._block_cls(output)

        return Dataset(
            compute.apply(transform, remote_args, self._blocks),
            self._block_cls)

    def filter(self, fn: Callable[[T], bool], compute="tasks",
               **remote_args) -> "Dataset[T]":
        def transform(block):
            return self._block_cls([row for row in block if fn(row)])

        return Dataset(
            compute.apply(transform, remote_args, self._blocks),
            self._block_cls)

    def take(self, limit: int = 20) -> List[T]:
        output = []
        for b in self._blocks:
            for row in ray.get(b):
                output.append(row)
                if len(output) >= limit:
                    break
            if len(output) >= limit:
                break
        return output

    def show(self, limit: int = 20) -> None:
        for row in self.take(limit):
            print(row)

    def count(self) -> int:
        @ray.remote
        def count(block):
            return len(block)

        return sum(ray.get([count.remote(block) for block in self._blocks]))

    def sum(self) -> int:
        @ray.remote
        def _sum(block):
            return sum(block)

        return sum(ray.get([_sum.remote(block) for block in self._blocks]))


def range(n: int, parallelism: int = 200) -> Dataset[int]:
    block_size = max(1, n // parallelism)
    blocks: List[BlockRef] = []

    @ray.remote
    def gen_block(start: int, count: int) -> Block:
        return ListBlock(list(builtins.range(start, start + count)))

    i = 0
    while i < n:
        blocks.append(gen_block.remote(i, min(block_size, n - i)))
        i += block_size

    return Dataset(blocks, ListBlock)


def from_items(items: List[Any], parallelism: int = 200) -> Dataset[Any]:
    block_size = max(1, len(items) // parallelism)

    blocks: List[BlockRef] = []
    i = 0
    while i < len(items):
        blocks.append(ray.put(ListBlock(items[i:i + block_size])))
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
