import logging
import builtins
import inspect
from typing import List, Any, Union, Optional, Tuple, Callable, TypeVar, \
    TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow
    import pandas
    import dask
    import mars
    import modin
    import pyspark

import ray
from ray.experimental.data.dataset import Dataset
from ray.experimental.data.datasource import Datasource, RangeDatasource, \
    ReadTask
from ray.experimental.data.impl.block import ObjectRef, ListBlock, Block
from ray.experimental.data.impl.arrow_block import ArrowBlock, ArrowRow
from ray.experimental.data.impl.block_list import BlockList, BlockMetadata
from ray.experimental.data.impl.lazy_block_list import LazyBlockList

T = TypeVar("T")

logger = logging.getLogger(__name__)


def autoinit_ray(f: Callable) -> Callable:
    def wrapped(*a, **kw):
        if not ray.is_initialized():
            ray.client().connect()
        return f(*a, **kw)

    wrapped.__name__ = f.__name__
    wrapped.__doc__ = f.__doc__
    setattr(wrapped, "__signature__", inspect.signature(f))
    return wrapped


@autoinit_ray
def from_items(items: List[Any], parallelism: int = 200) -> Dataset[Any]:
    """Create a dataset from a list of local Python objects.

    Examples:
        >>> ds.from_items([1, 2, 3, 4, 5])

    Args:
        items: List of local Python objects.
        parallelism: The amount of parallelism to use for the dataset.

    Returns:
        Dataset holding the items.
    """
    block_size = max(1, len(items) // parallelism)

    blocks: List[ObjectRef[Block]] = []
    metadata: List[BlockMetadata] = []
    i = 0
    while i < len(items):
        builder = ListBlock.builder()
        for item in items[i:i + block_size]:
            builder.add(item)
        block = builder.build()
        blocks.append(ray.put(block))
        metadata.append(
            BlockMetadata(
                num_rows=block.num_rows(),
                size_bytes=block.size_bytes(),
                schema=type(items[0]),
                input_files=None))
        i += block_size

    return Dataset(BlockList(blocks, metadata))


@autoinit_ray
def range(n: int, parallelism: int = 200) -> Dataset[int]:
    """Create a dataset from a range of integers [0..n).

    Examples:
        >>> ds.range(10000).map(lambda x: x * 2).show()

    Args:
        n: The upper bound of the range of integers.
        parallelism: The amount of parallelism to use for the dataset.

    Returns:
        Dataset holding the integers.
    """
    return read_datasource(
        RangeDatasource(), parallelism=parallelism, n=n, use_arrow=False)


@autoinit_ray
def range_arrow(n: int, parallelism: int = 200) -> Dataset[ArrowRow]:
    """Create an Arrow dataset from a range of integers [0..n).

    Examples:
        >>> ds.range_arrow(1000).map(lambda r: {"v2": r["value"] * 2}).show()

    This is similar to range(), but uses Arrow tables to hold the integers
    in Arrow records. The dataset elements take the form {"value": N}.

    Args:
        n: The upper bound of the range of integer records.
        parallelism: The amount of parallelism to use for the dataset.

    Returns:
        Dataset holding the integers as Arrow records.
    """
    return read_datasource(
        RangeDatasource(), parallelism=parallelism, n=n, use_arrow=True)


@autoinit_ray
def read_datasource(datasource: Datasource[T],
                    parallelism: int = 200,
                    **read_args) -> Dataset[T]:
    """Read a dataset from a custom data source.

    Args:
        datasource: The datasource to read data from.
        parallelism: The requested parallelism of the read.
        read_args: Additional kwargs to pass to the datasource impl.

    Returns:
        Dataset holding the data read from the datasource.
    """

    read_tasks = datasource.prepare_read(parallelism, **read_args)

    @ray.remote
    def remote_read(task: ReadTask) -> Block[T]:
        return task()

    calls: List[Callable[[], ObjectRef[Block[T]]]] = []
    metadata: List[BlockMetadata] = []

    for task in read_tasks:
        calls.append(lambda task=task: remote_read.remote(task))
        metadata.append(task.get_metadata())

    return Dataset(LazyBlockList(calls, metadata))


@autoinit_ray
def read_parquet(paths: Union[str, List[str]],
                 filesystem: Optional["pyarrow.fs.FileSystem"] = None,
                 columns: Optional[List[str]] = None,
                 parallelism: int = 200,
                 **arrow_parquet_args) -> Dataset[ArrowRow]:
    """Create an Arrow dataset from parquet files.

    Examples:
        # Read a directory of files in remote storage.
        >>> ds.read_parquet("s3://bucket/path")

        # Read multiple local files.
        >>> ds.read_parquet(["/path/to/file1", "/path/to/file2"])

    Args:
        paths: A single file path or a list of file paths (or directories).
        filesystem: The filesystem implementation to read from.
        columns: A list of column names to read.
        parallelism: The amount of parallelism to use for the dataset.
        arrow_parquet_args: Other parquet read options to pass to pyarrow.

    Returns:
        Dataset holding Arrow records read from the specified paths.
    """
    import pyarrow.parquet as pq

    pq_ds = pq.ParquetDataset(paths, **arrow_parquet_args)

    read_tasks = [[] for _ in builtins.range(parallelism)]
    # TODO(ekl) support reading row groups (maybe as an option)
    for i, piece in enumerate(pq_ds.pieces):
        read_tasks[i % len(read_tasks)].append(piece)
    nonempty_tasks = [r for r in read_tasks if r]
    partitions = pq_ds.partitions

    @ray.remote
    def gen_read(pieces: List[pq.ParquetDatasetPiece]):
        import pyarrow
        logger.debug("Reading {} parquet pieces".format(len(pieces)))
        tables = [
            piece.read(
                columns=columns, use_threads=False, partitions=partitions)
            for piece in pieces
        ]
        if len(tables) > 1:
            table = pyarrow.concat_tables(tables)
        else:
            table = tables[0]
        return ArrowBlock(table)

    calls: List[Callable[[], ObjectRef[Block]]] = []
    metadata: List[BlockMetadata] = []
    for pieces in nonempty_tasks:
        calls.append(lambda pieces=pieces: gen_read.remote(pieces))
        piece_metadata = [p.get_metadata() for p in pieces]
        metadata.append(
            BlockMetadata(
                num_rows=sum(m.num_rows for m in piece_metadata),
                size_bytes=sum(
                    sum(
                        m.row_group(i).total_byte_size
                        for i in builtins.range(m.num_row_groups))
                    for m in piece_metadata),
                schema=piece_metadata[0].schema.to_arrow_schema(),
                input_files=[p.path for p in pieces]))

    return Dataset(LazyBlockList(calls, metadata))


@autoinit_ray
def read_json(paths: Union[str, List[str]],
              filesystem: Optional["pyarrow.fs.FileSystem"] = None,
              parallelism: int = 200,
              **arrow_json_args) -> Dataset[ArrowRow]:
    """Create an Arrow dataset from json files.

    Examples:
        # Read a directory of files in remote storage.
        >>> ds.read_json("s3://bucket/path")

        # Read multiple local files.
        >>> ds.read_json(["/path/to/file1", "/path/to/file2"])

    Args:
        paths: A single file path or a list of file paths (or directories).
        filesystem: The filesystem implementation to read from.
        parallelism: The amount of parallelism to use for the dataset.
        arrow_json_args: Other json read options to pass to pyarrow.

    Returns:
        Dataset holding Arrow records read from the specified paths.
    """
    raise NotImplementedError  # P0


@autoinit_ray
def read_csv(paths: Union[str, List[str]],
             filesystem: Optional["pyarrow.fs.FileSystem"] = None,
             parallelism: int = 200,
             **arrow_csv_args) -> Dataset[ArrowRow]:
    """Create an Arrow dataset from csv files.

    Examples:
        # Read a directory of files in remote storage.
        >>> ds.read_csv("s3://bucket/path")

        # Read multiple local files.
        >>> ds.read_csv(["/path/to/file1", "/path/to/file2"])

    Args:
        paths: A single file path or a list of file paths (or directories).
        filesystem: The filesystem implementation to read from.
        parallelism: The amount of parallelism to use for the dataset.
        arrow_csv_args: Other csv read options to pass to pyarrow.

    Returns:
        Dataset holding Arrow records read from the specified paths.
    """
    raise NotImplementedError  # P0


@autoinit_ray
def read_binary_files(
        paths: Union[str, List[str]],
        include_paths: bool = False,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        parallelism: int = 200) -> Dataset[Union[Tuple[str, bytes], bytes]]:
    """Create a dataset from binary files of arbitrary contents.

    Examples:
        # Read a directory of files in remote storage.
        >>> ds.read_binary_files("s3://bucket/path")

        # Read multiple local files.
        >>> ds.read_binary_files(["/path/to/file1", "/path/to/file2"])

    Args:
        paths: A single file path or a list of file paths (or directories).
        include_paths: Whether to include the full path of the file in the
            dataset records. When specified, the dataset records will be a
            tuple of the file path and the file contents.
        filesystem: The filesystem implementation to read from.
        parallelism: The amount of parallelism to use for the dataset.

    Returns:
        Dataset holding Arrow records read from the specified paths.
    """
    raise NotImplementedError  # P0


@autoinit_ray
def from_dask(df: "dask.DataFrame",
              parallelism: int = 200) -> Dataset[ArrowRow]:
    """Create a dataset from a Dask dataframe.

    Args:
        df: A Dask dataframe, which must be executed by Dask-on-Ray.

    Returns:
        Dataset holding Arrow records read from the dataframe.
    """
    raise NotImplementedError  # P1


@autoinit_ray
def from_mars(df: "mars.DataFrame",
              parallelism: int = 200) -> Dataset[ArrowRow]:
    """Create a dataset from a MARS dataframe.

    Args:
        df: A MARS dataframe, which must be executed by MARS-on-Ray.

    Returns:
        Dataset holding Arrow records read from the dataframe.
    """
    raise NotImplementedError  # P1


@autoinit_ray
def from_modin(df: "modin.DataFrame",
               parallelism: int = 200) -> Dataset[ArrowRow]:
    """Create a dataset from a Modin dataframe.

    Args:
        df: A Modin dataframe, which must be using the Ray backend.
        parallelism: The amount of parallelism to use for the dataset.

    Returns:
        Dataset holding Arrow records read from the dataframe.
    """
    raise NotImplementedError  # P1


@autoinit_ray
def from_pandas(dfs: List[ObjectRef["pandas.DataFrame"]],
                parallelism: int = 200) -> Dataset[ArrowRow]:
    """Create a dataset from a set of Pandas dataframes.

    Args:
        dfs: A list of Ray object references to pandas dataframes.
        parallelism: The amount of parallelism to use for the dataset.

    Returns:
        Dataset holding Arrow records read from the dataframes.
    """
    raise NotImplementedError  # P1


@autoinit_ray
def from_spark(df: "pyspark.sql.DataFrame",
               parallelism: int = 200) -> Dataset[ArrowRow]:
    """Create a dataset from a Spark dataframe.

    Args:
        df: A Spark dataframe, which must be created by RayDP (Spark-on-Ray).
        parallelism: The amount of parallelism to use for the dataset.

    Returns:
        Dataset holding Arrow records read from the dataframe.
    """
    raise NotImplementedError  # P2
