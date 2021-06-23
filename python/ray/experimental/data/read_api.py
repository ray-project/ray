import builtins
from typing import List, Any, Union, Optional

import pyarrow.parquet as pq
import pyarrow as pa

import ray
from ray.experimental.data.dataset import Dataset
from ray.experimental.data.impl.block import BlockRef, ListBlock
from ray.experimental.data.impl.arrow_block import ArrowBlock


def range(n: int, parallelism: int = 200) -> Dataset[int]:
    block_size = max(1, n // parallelism)
    blocks: List[BlockRef] = []

    @ray.remote
    def gen_block(start: int, count: int) -> ListBlock:
        builder = ListBlock.builder()
        for value in builtins.range(start, start + count):
            builder.add(value)
        return builder.build().serialize()

    i = 0
    while i < n:
        blocks.append(gen_block.remote(i, min(block_size, n - i)))
        i += block_size

    return Dataset(blocks, ListBlock)


def range_arrow(n: int, num_blocks: int = 200) -> Dataset[dict]:
    block_size = max(1, n // num_blocks)
    blocks = []
    i = 0

    @ray.remote
    def gen_block(start: int, count: int) -> "ArrowBlock":
        return ArrowBlock(
            pa.Table.from_pydict({
                "value": list(builtins.range(start, start + count))
            })).serialize()

    while i < n:
        blocks.append(gen_block.remote(block_size * i, min(block_size, n - i)))
        i += block_size

    return Dataset(blocks, ArrowBlock)


def from_items(items: List[Any], parallelism: int = 200) -> Dataset[Any]:
    block_size = max(1, len(items) // parallelism)

    blocks: List[BlockRef] = []
    i = 0
    while i < len(items):
        builder = ListBlock.builder()
        for item in items[i:i + block_size]:
            builder.add(item)
        blocks.append(ray.put(builder.build().serialize()))
        i += block_size

    return Dataset(blocks, ListBlock)


def read_parquet(paths: Union[str, List[str]],
                 parallelism: int = 200,
                 columns: Optional[List[str]] = None,
                 **kwargs) -> Dataset[dict]:
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
        return ArrowBlock(table).serialize()

    return Dataset([gen_read.remote(ps) for ps in nonempty_tasks], ArrowBlock)


def read_binary_files(directory: str) -> Dataset[bytes]:
    pass
