import logging
from pathlib import Path
import builtins
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
    JSONDatasource, CSVDatasource, ReadTask
from ray.experimental.data.impl import reader as _reader
from ray.experimental.data.impl.arrow_block import ArrowBlock, ArrowRow
from ray.experimental.data.impl.block import ObjectRef, SimpleBlock, Block, \
    BlockMetadata
from ray.experimental.data.impl.block_list import BlockList
from ray.experimental.data.impl.lazy_block_list import LazyBlockList

T = TypeVar("T")

logger = logging.getLogger(__name__)


def _parse_paths(paths: Union[str, List[str]]
                 ) -> Tuple["pyarrow.fs.FileSystem", Union[str, List[str]]]:
    from pyarrow import fs

    def parse_single_path(path: str):
        if Path(path).exists():
            return fs.LocalFileSystem(), path
        else:
            return fs.FileSystem.from_uri(path)

    if isinstance(paths, str):
        return parse_single_path(paths)

    if not isinstance(paths, list) or any(not isinstance(p, str)
                                          for p in paths):
        raise ValueError(
            "paths must be a path string or a list of path strings.")
    else:
        if len(paths) == 0:
            raise ValueError("No data provided")

        parsed_results = [parse_single_path(path) for path in paths]
        fses, paths = zip(*parsed_results)
        unique_fses = set(map(type, fses))
        if len(unique_fses) > 1:
            raise ValueError(
                f"When specifying multiple paths, each path must have the "
                f"same filesystem, but found: {unique_fses}")
        return fses[0], list(paths)


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
        builder = SimpleBlock.builder()
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

    if filesystem is None:
        filesystem, paths = _parse_paths(paths)
    pq_ds = pq.ParquetDataset(
        paths, **arrow_parquet_args, filesystem=filesystem)
    pieces = pq_ds.pieces

    read_tasks = [[] for _ in builtins.range(parallelism)]
    # TODO(ekl) support reading row groups (maybe as an option)
    for i, piece in enumerate(pq_ds.pieces):
        read_tasks[i % len(read_tasks)].append(piece)
    nonempty_tasks = [r for r in read_tasks if r]

    @ray.remote
    def gen_read(pieces: List["pyarrow._dataset.ParquetFileFragment"]):
        import pyarrow
        logger.debug("Reading {} parquet pieces".format(len(pieces)))
        tables = [piece.to_table() for piece in pieces]
        if len(tables) > 1:
            table = pyarrow.concat_tables(tables)
        else:
            table = tables[0]
        return ArrowBlock(table)

    calls: List[Callable[[], ObjectRef[Block]]] = []
    metadata: List[BlockMetadata] = []
    for pieces in nonempty_tasks:
        calls.append(lambda pieces=pieces: gen_read.remote(pieces))
        piece_metadata = [p.metadata for p in pieces]
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

        # Read multiple directories.
        >>> ds.read_json(["s3://bucket/path1", "s3://bucket/path2"])

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation to read from.
        parallelism: The amount of parallelism to use for the dataset.
        arrow_json_args: Other json read options to pass to pyarrow.

    Returns:
        Dataset holding Arrow records read from the specified paths.
    """
    return read_datasource(
        JSONDatasource(),
        parallelism=parallelism,
        paths=paths,
        filesystem=filesystem,
        **arrow_json_args)


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

        # Read multiple directories.
        >>> ds.read_csv(["s3://bucket/path1", "s3://bucket/path2"])

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation to read from.
        parallelism: The amount of parallelism to use for the dataset.
        arrow_csv_args: Other csv read options to pass to pyarrow.

    Returns:
        Dataset holding Arrow records read from the specified paths.
    """
    return read_datasource(
        CSVDatasource(),
        parallelism=parallelism,
        paths=paths,
        filesystem=filesystem,
        **arrow_csv_args)


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
    dataset = from_items(paths, parallelism=parallelism)
    return dataset.map(
        lambda path: _reader.read_file(
            path,
            include_paths=include_paths,
            filesystem=filesystem))


def from_dask(df: "dask.DataFrame",
              parallelism: int = 200) -> Dataset[ArrowRow]:
    """Create a dataset from a Dask DataFrame.

    Args:
        df: A Dask DataFrame.

    Returns:
        Dataset holding Arrow records read from the DataFrame.
    """
    import dask
    from ray.util.dask import ray_dask_get

    partitions = df.to_delayed()
    persisted_partitions = dask.persist(*partitions, scheduler=ray_dask_get)
    return from_pandas(
        [next(iter(part.dask.values())) for part in persisted_partitions])


def from_mars(df: "mars.DataFrame",
              parallelism: int = 200) -> Dataset[ArrowRow]:
    """Create a dataset from a MARS dataframe.

    Args:
        df: A MARS dataframe, which must be executed by MARS-on-Ray.

    Returns:
        Dataset holding Arrow records read from the dataframe.
    """
    raise NotImplementedError  # P1


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


def from_pandas(dfs: List[ObjectRef["pandas.DataFrame"]],
                parallelism: int = 200) -> Dataset[ArrowRow]:
    """Create a dataset from a set of Pandas dataframes.

    Args:
        dfs: A list of Ray object references to pandas dataframes.
        parallelism: The amount of parallelism to use for the dataset.

    Returns:
        Dataset holding Arrow records read from the dataframes.
    """
    import pyarrow as pa

    @ray.remote(num_returns=2)
    def df_to_block(df: "pandas.DataFrame"):
        block = ArrowBlock(pa.table(df))
        return block, block.get_metadata(input_files=None)

    res = [df_to_block.remote(df) for df in dfs]
    blocks, metadata = zip(*res)
    return Dataset(BlockList(blocks, ray.get(list(metadata))))


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


def from_source(source: Any) -> Dataset[Any]:
    """Create a dataset from a generic data source."""
    raise NotImplementedError  # P0
