import builtins
import logging
from typing import List, Any, Union, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow
    import pandas
    import dask
    import modin
    import pyspark

import ray
from ray.experimental.data.dataset import Dataset
from ray.experimental.data.impl.block import ObjectRef, ListBlock, Block
from ray.experimental.data.impl.arrow_block import ArrowBlock, ArrowRow

logger = logging.getLogger(__name__)


def autoinit_ray(f):
    def wrapped(*a, **kw):
        if not ray.is_initialized():
            ray.client().connect()
        return f(*a, **kw)

    return wrapped


def _expand_directory(path: str,
                      filesystem: "pyarrow.fs.FileSystem",
                      exclude_prefixes: List[str] = [".", "_"]) -> List[str]:
    """
    Expand the provided directory path to a list of file paths.

    Args:
        path: The directory path to expand.
        filesystem: The filesystem implementation that should be used for
            reading these files.
        exclude_prefixes: The file relative path prefixes that should be
            excluded from the returned file set. Default excluded prefixes are
            "." and "_".

    Returns:
        A list of file paths contained in the provided directory.
    """
    from pyarrow.fs import FileSelector
    selector = FileSelector(path, recursive=True)
    files = filesystem.get_file_info(selector)
    base_path = selector.base_dir
    filtered_paths = []
    for file_ in files:
        if not file_.is_file:
            continue
        file_path = file_.path
        if not file_path.startswith(base_path):
            continue
        relative = file_path[len(base_path):]
        if any(relative.startswith(prefix) for prefix in [".", "_"]):
            continue
        filtered_paths.append(file_path)
    # We sort the paths to guarantee a stable order.
    return sorted(filtered_paths)


def _resolve_paths_and_filesystem(
        paths: Union[str, List[str]],
        filesystem: "pyarrow.fs.FileSystem" = None
) -> Tuple[List[str], "pyarrow.fs.FileSystem"]:
    """
    Resolves and normalizes all provided paths, infers a filesystem from the
    paths and ensures that all paths use the same filesystem, and expands all
    directory paths to the underlying file paths.

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation that should be used for
            reading these files. If None, a filesystem will be inferred. If not
            None, the provided filesystem will still be validated against all
            filesystems inferred from the provided paths to ensure
            compatibility.
    """
    from pyarrow.fs import FileType, _resolve_filesystem_and_path

    if isinstance(paths, str):
        paths = [paths]
    elif (not isinstance(paths, list)
          or any(not isinstance(p, str) for p in paths)):
        raise ValueError(
            "paths must be a path string or a list of path strings.")
    elif len(paths) == 0:
        raise ValueError("Must provide at least one path.")

    resolved_paths = []
    for path in paths:
        resolved_filesystem, resolved_path = _resolve_filesystem_and_path(
            path, filesystem)
        if filesystem is None:
            filesystem = resolved_filesystem
        elif type(resolved_filesystem) != type(filesystem):
            raise ValueError("All paths must use same filesystem.")
        resolved_path = filesystem.normalize_path(resolved_path)
        resolved_paths.append(resolved_path)

    expanded_paths = []
    for path in resolved_paths:
        file_info = filesystem.get_file_info(path)
        if file_info.type == FileType.Directory:
            expanded_paths.extend(_expand_directory(path, filesystem))
        elif file_info.type == FileType.File:
            expanded_paths.append(path)
        else:
            raise FileNotFoundError(path)
    return expanded_paths, filesystem


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
    i = 0
    while i < len(items):
        builder = ListBlock.builder()
        for item in items[i:i + block_size]:
            builder.add(item)
        blocks.append(ray.put(builder.build()))
        i += block_size

    return Dataset(blocks)


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
    block_size = max(1, n // parallelism)
    blocks: List[ObjectRef[Block]] = []

    @ray.remote
    def gen_block(start: int, count: int) -> ListBlock:
        builder = ListBlock.builder()
        for value in builtins.range(start, start + count):
            builder.add(value)
        return builder.build()

    i = 0
    while i < n:
        blocks.append(gen_block.remote(i, min(block_size, n - i)))
        i += block_size

    return Dataset(blocks)


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
    block_size = max(1, n // parallelism)
    blocks = []
    i = 0

    @ray.remote
    def gen_block(start: int, count: int) -> "ArrowBlock":
        import pyarrow

        return ArrowBlock(
            pyarrow.Table.from_pydict({
                "value": list(builtins.range(start, start + count))
            }))

    while i < n:
        blocks.append(gen_block.remote(block_size * i, min(block_size, n - i)))
        i += block_size

    return Dataset(blocks)


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
    import numpy as np

    pq_ds = pq.ParquetDataset(paths, **arrow_parquet_args)
    pieces = pq_ds.pieces
    data_pieces = []

    for piece in pieces:
        num_row_groups = piece.get_metadata().to_dict()["num_row_groups"]
        for i in builtins.range(num_row_groups):
            data_pieces.append(
                pq.ParquetDatasetPiece(piece.path, piece.open_file_func,
                                       piece.file_options, i,
                                       piece.partition_keys))

    partitions = pq_ds.partitions

    @ray.remote
    def gen_read(pieces: List[pq.ParquetDatasetPiece]):
        logger.debug("Reading {} parquet pieces".format(len(pieces)))
        table = piece.read(
            columns=columns, use_threads=False, partitions=partitions)
        return ArrowBlock(table)

    return Dataset([
        gen_read.remote(ps) for ps in np.array_split(pieces, parallelism)
        if len(ps) > 0
    ])


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
    import pyarrow as pa
    from pyarrow import json
    import numpy as np

    paths, filesystem = _resolve_paths_and_filesystem(paths, filesystem)

    @ray.remote
    def json_read(read_paths: List[str]):
        logger.debug(f"Reading {len(read_paths)} files.")
        tables = []
        for read_path in read_paths:
            with filesystem.open_input_file(read_path) as f:
                tables.append(
                    json.read_json(
                        f,
                        read_options=json.ReadOptions(use_threads=False),
                        **arrow_json_args))
        return ArrowBlock(pa.concat_tables(tables))

    return Dataset([
        json_read.remote(read_paths)
        for read_paths in np.array_split(paths, parallelism)
        if len(read_paths) > 0
    ])


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
    import pyarrow as pa
    from pyarrow import csv
    import numpy as np

    paths, filesystem = _resolve_paths_and_filesystem(paths, filesystem)

    @ray.remote
    def csv_read(read_paths: List[str]):
        logger.debug(f"Reading {len(read_paths)} files.")
        tables = []
        for read_path in read_paths:
            with filesystem.open_input_file(read_path) as f:
                tables.append(
                    csv.read_csv(
                        f,
                        read_options=csv.ReadOptions(use_threads=False),
                        **arrow_csv_args))
        return ArrowBlock(pa.concat_tables(tables))

    return Dataset([
        csv_read.remote(read_paths)
        for read_paths in np.array_split(paths, parallelism)
        if len(read_paths) > 0
    ])


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


def from_dask(df: "dask.DataFrame") -> Dataset[ArrowRow]:
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

    @ray.remote
    def df_to_block(df: "pandas.DataFrame"):
        return ArrowBlock(pa.table(df))

    return Dataset([df_to_block.remote(df) for df in dfs])


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
