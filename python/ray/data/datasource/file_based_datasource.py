import io
import logging
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Union,
)

import numpy as np

import ray
from ray.data._internal.util import (
    RetryingContextManager,
    RetryingPyFileSystem,
    _check_pyarrow_version,
    _is_local_scheme,
    iterate_with_retry,
    make_async_gen,
)
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.data.datasource.file_meta_provider import (
    BaseFileMetadataProvider,
    DefaultFileMetadataProvider,
)
from ray.data.datasource.partitioning import (
    Partitioning,
    PathPartitionFilter,
    PathPartitionParser,
)
from ray.data.datasource.path_util import (
    _has_file_extension,
    _resolve_paths_and_filesystem,
)
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow


logger = logging.getLogger(__name__)


# We should parallelize file size fetch operations beyond this threshold.
FILE_SIZE_FETCH_PARALLELIZATION_THRESHOLD = 16

# 16 file size fetches from S3 takes ~1.5 seconds with Arrow's S3FileSystem.
PATHS_PER_FILE_SIZE_FETCH_TASK = 16


@DeveloperAPI
@dataclass
class FileShuffleConfig:
    """Configuration for file shuffling.

    This configuration object controls how files are shuffled while reading file-based
    datasets.

    .. note::
        Even if you provided a seed, you might still observe a non-deterministic row
        order. This is because tasks are executed in parallel and their completion
        order might vary. If you need to preserve the order of rows, set
        `DataContext.get_current().execution_options.preserve_order`.

    Args:
        seed: An optional integer seed for the file shuffler. If provided, Ray Data
            shuffles files deterministically based on this seed.

    Example:
        >>> import ray
        >>> from ray.data import FileShuffleConfig
        >>> shuffle = FileShuffleConfig(seed=42)
        >>> ds = ray.data.read_images("s3://anonymous@ray-example-data/batoidea", shuffle=shuffle)
    """  # noqa: E501

    seed: Optional[int] = None

    def __post_init__(self):
        """Ensure that the seed is either None or an integer."""
        if self.seed is not None and not isinstance(self.seed, int):
            raise ValueError("Seed must be an integer or None.")


@DeveloperAPI
class FileBasedDatasource(Datasource):
    """File-based datasource for reading files.

    Don't use this class directly. Instead, subclass it and implement `_read_stream()`.
    """

    # If `_WRITE_FILE_PER_ROW` is `True`, this datasource calls `_write_row` and writes
    # each row to a file. Otherwise, this datasource calls `_write_block` and writes
    # each block to a file.
    _WRITE_FILE_PER_ROW = False
    _FILE_EXTENSIONS: Optional[Union[str, List[str]]] = None
    # Number of threads for concurrent reading within each read task.
    # If zero or negative, reading will be performed in the main thread.
    _NUM_THREADS_PER_TASK = 0

    def __init__(
        self,
        paths: Union[str, List[str]],
        *,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        schema: Optional[Union[type, "pyarrow.lib.Schema"]] = None,
        open_stream_args: Optional[Dict[str, Any]] = None,
        meta_provider: BaseFileMetadataProvider = DefaultFileMetadataProvider(),
        partition_filter: PathPartitionFilter = None,
        partitioning: Partitioning = None,
        ignore_missing_paths: bool = False,
        shuffle: Optional[Union[Literal["files"], FileShuffleConfig]] = None,
        include_paths: bool = False,
        file_extensions: Optional[List[str]] = None,
    ):
        _check_pyarrow_version()

        self._supports_distributed_reads = not _is_local_scheme(paths)
        if not self._supports_distributed_reads and ray.util.client.ray.is_connected():
            raise ValueError(
                "Because you're using Ray Client, read tasks scheduled on the Ray "
                "cluster can't access your local files. To fix this issue, store "
                "files in cloud storage or a distributed filesystem like NFS."
            )

        self._schema = schema
        self._data_context = DataContext.get_current()
        self._open_stream_args = open_stream_args
        self._meta_provider = meta_provider
        self._partition_filter = partition_filter
        self._partitioning = partitioning
        self._ignore_missing_paths = ignore_missing_paths
        self._include_paths = include_paths
        paths, self._filesystem = _resolve_paths_and_filesystem(paths, filesystem)
        self._filesystem = RetryingPyFileSystem.wrap(
            self._filesystem, context=self._data_context
        )
        paths, file_sizes = map(
            list,
            zip(
                *meta_provider.expand_paths(
                    paths,
                    self._filesystem,
                    partitioning,
                    ignore_missing_paths=ignore_missing_paths,
                )
            ),
        )

        if ignore_missing_paths and len(paths) == 0:
            raise ValueError(
                "None of the provided paths exist. "
                "The 'ignore_missing_paths' field is set to True."
            )

        if self._partition_filter is not None:
            # Use partition filter to skip files which are not needed.
            path_to_size = dict(zip(paths, file_sizes))
            paths = self._partition_filter(paths)
            file_sizes = [path_to_size[p] for p in paths]
            if len(paths) == 0:
                raise ValueError(
                    "No input files found to read. Please double check that "
                    "'partition_filter' field is set properly."
                )

        if file_extensions is not None:
            path_to_size = dict(zip(paths, file_sizes))
            paths = [p for p in paths if _has_file_extension(p, file_extensions)]
            file_sizes = [path_to_size[p] for p in paths]
            if len(paths) == 0:
                raise ValueError(
                    "No input files found to read with the following file extensions: "
                    f"{file_extensions}. Please double check that "
                    "'file_extensions' field is set properly."
                )

        _validate_shuffle_arg(shuffle)
        self._file_metadata_shuffler = None
        if shuffle == "files":
            self._file_metadata_shuffler = np.random.default_rng()
        elif isinstance(shuffle, FileShuffleConfig):
            # Create a NumPy random generator with a fixed seed if provided
            self._file_metadata_shuffler = np.random.default_rng(shuffle.seed)

        # Read tasks serialize `FileBasedDatasource` instances, and the list of paths
        # can be large. To avoid slow serialization speeds, we store a reference to
        # the paths rather than the paths themselves.
        self._paths_ref = ray.put(paths)
        self._file_sizes_ref = ray.put(file_sizes)

    def _paths(self) -> List[str]:
        return ray.get(self._paths_ref)

    def _file_sizes(self) -> List[float]:
        return ray.get(self._file_sizes_ref)

    def estimate_inmemory_data_size(self) -> Optional[int]:
        total_size = 0
        for sz in self._file_sizes():
            if sz is not None:
                total_size += sz
        return total_size

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        import numpy as np

        open_stream_args = self._open_stream_args
        partitioning = self._partitioning

        paths = self._paths()
        file_sizes = self._file_sizes()

        if self._file_metadata_shuffler is not None:
            files_metadata = list(zip(paths, file_sizes))
            shuffled_files_metadata = [
                files_metadata[i]
                for i in self._file_metadata_shuffler.permutation(len(files_metadata))
            ]
            paths, file_sizes = list(map(list, zip(*shuffled_files_metadata)))

        filesystem = _wrap_s3_serialization_workaround(self._filesystem)

        if open_stream_args is None:
            open_stream_args = {}

        def read_files(
            read_paths: Iterable[str],
        ) -> Iterable[Block]:
            nonlocal filesystem, open_stream_args, partitioning

            fs = _unwrap_s3_serialization_workaround(filesystem)

            for read_path in read_paths:
                partitions: Dict[str, str] = {}
                if partitioning is not None:
                    parse = PathPartitionParser(partitioning)
                    partitions = parse(read_path)

                with RetryingContextManager(
                    self._open_input_source(fs, read_path, **open_stream_args),
                    context=self._data_context,
                ) as f:
                    for block in iterate_with_retry(
                        lambda: self._read_stream(f, read_path),
                        description="read stream iteratively",
                        match=self._data_context.retried_io_errors,
                    ):
                        if partitions:
                            block = _add_partitions(block, partitions)
                        if self._include_paths:
                            block_accessor = BlockAccessor.for_block(block)
                            block = block_accessor.append_column(
                                "path", [read_path] * block_accessor.num_rows()
                            )
                        yield block

        def create_read_task_fn(read_paths, num_threads):
            def read_task_fn():
                nonlocal num_threads, read_paths

                # TODO: We should refactor the code so that we can get the results in
                # order even when using multiple threads.
                if self._data_context.execution_options.preserve_order:
                    num_threads = 0

                if num_threads > 0:
                    if len(read_paths) < num_threads:
                        num_threads = len(read_paths)

                    logger.debug(
                        f"Reading {len(read_paths)} files with {num_threads} threads."
                    )

                    yield from make_async_gen(
                        iter(read_paths),
                        read_files,
                        num_workers=num_threads,
                    )
                else:
                    logger.debug(f"Reading {len(read_paths)} files.")
                    yield from read_files(read_paths)

            return read_task_fn

        # fix https://github.com/ray-project/ray/issues/24296
        parallelism = min(parallelism, len(paths))

        read_tasks = []
        split_paths = np.array_split(paths, parallelism)
        split_file_sizes = np.array_split(file_sizes, parallelism)

        for read_paths, file_sizes in zip(split_paths, split_file_sizes):
            if len(read_paths) <= 0:
                continue

            meta = self._meta_provider(
                read_paths,
                self._schema,
                rows_per_file=self._rows_per_file(),
                file_sizes=file_sizes,
            )

            read_task_fn = create_read_task_fn(read_paths, self._NUM_THREADS_PER_TASK)

            read_task = ReadTask(read_task_fn, meta)

            read_tasks.append(read_task)

        return read_tasks

    def _open_input_source(
        self,
        filesystem: "RetryingPyFileSystem",
        path: str,
        **open_args,
    ) -> "pyarrow.NativeFile":
        """Opens a source path for reading and returns the associated Arrow NativeFile.

        The default implementation opens the source path as a sequential input stream,
        using self._data_context.streaming_read_buffer_size as the buffer size if none
        is given by the caller.

        Implementations that do not support streaming reads (e.g. that require random
        access) should override this method.
        """
        import pyarrow as pa
        from pyarrow.fs import HadoopFileSystem

        compression = open_args.get("compression", None)
        if compression is None:
            try:
                # If no compression manually given, try to detect
                # compression codec from path.
                compression = pa.Codec.detect(path).name
            except (ValueError, TypeError):
                # Arrow's compression inference on the file path
                # doesn't work for Snappy, so we double-check ourselves.
                import pathlib

                suffix = pathlib.Path(path).suffix
                if suffix and suffix[1:] == "snappy":
                    compression = "snappy"
                else:
                    compression = None

        buffer_size = open_args.pop("buffer_size", None)
        if buffer_size is None:
            buffer_size = self._data_context.streaming_read_buffer_size

        if compression == "snappy":
            # Arrow doesn't support streaming Snappy decompression since the canonical
            # C++ Snappy library doesn't natively support streaming decompression. We
            # works around this by manually decompressing the file with python-snappy.
            open_args["compression"] = None
        else:
            open_args["compression"] = compression

        file = filesystem.open_input_stream(path, buffer_size=buffer_size, **open_args)

        if compression == "snappy":
            import snappy

            stream = io.BytesIO()
            if isinstance(filesystem.unwrap(), HadoopFileSystem):
                snappy.hadoop_snappy.stream_decompress(src=file, dst=stream)
            else:
                snappy.stream_decompress(src=file, dst=stream)
            stream.seek(0)

            file = pa.PythonFile(stream, mode="r")

        return file

    def _rows_per_file(self):
        """Returns the number of rows per file, or None if unknown."""
        return None

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        """Streaming read a single file.

        This method should be implemented by subclasses.
        """
        raise NotImplementedError(
            "Subclasses of FileBasedDatasource must implement _read_stream()."
        )

    @property
    def supports_distributed_reads(self) -> bool:
        return self._supports_distributed_reads


def _add_partitions(
    data: Union["pyarrow.Table", "pd.DataFrame"], partitions: Dict[str, Any]
) -> Union["pyarrow.Table", "pd.DataFrame"]:
    import pandas as pd
    import pyarrow as pa

    assert isinstance(data, (pa.Table, pd.DataFrame))
    if isinstance(data, pa.Table):
        return _add_partitions_to_table(data, partitions)
    if isinstance(data, pd.DataFrame):
        return _add_partitions_to_dataframe(data, partitions)


def _add_partitions_to_table(
    table: "pyarrow.Table", partitions: Dict[str, Any]
) -> "pyarrow.Table":
    import pyarrow as pa
    import pyarrow.compute as pc

    column_names = set(table.column_names)
    for field, value in partitions.items():
        column = pa.array([value] * len(table))
        if field in column_names:
            # TODO: Handle cast error.
            column_type = table.schema.field(field).type
            column = column.cast(column_type)

            values_are_equal = pc.all(pc.equal(column, table[field]))
            values_are_equal = values_are_equal.as_py()

            if not values_are_equal:
                raise ValueError(
                    f"Partition column {field} exists in table data, but partition "
                    f"value '{value}' is different from in-data values: "
                    f"{table[field].unique().to_pylist()}."
                )

            i = table.schema.get_field_index(field)
            table = table.set_column(i, field, column)
        else:
            table = table.append_column(field, column)

    return table


def _add_partitions_to_dataframe(
    df: "pd.DataFrame", partitions: Dict[str, Any]
) -> "pd.DataFrame":
    import pandas as pd

    for field, value in partitions.items():
        column = pd.Series(data=[value] * len(df), name=field)

        if field in df:
            column = column.astype(df[field].dtype)
            mask = df[field].notna()
            if not df[field][mask].equals(column[mask]):
                raise ValueError(
                    f"Partition column {field} exists in table data, but partition "
                    f"value '{value}' is different from in-data values: "
                    f"{list(df[field].unique())}."
                )

        df[field] = column

    return df


def _wrap_s3_serialization_workaround(filesystem: "pyarrow.fs.FileSystem"):
    # This is needed because pa.fs.S3FileSystem assumes pa.fs is already
    # imported before deserialization. See #17085.
    import pyarrow as pa
    import pyarrow.fs

    wrap_retries = False
    fs_to_be_wrapped = filesystem  # Only unwrap for S3FileSystemWrapper
    context = None
    if isinstance(fs_to_be_wrapped, RetryingPyFileSystem):
        wrap_retries = True
        context = fs_to_be_wrapped.data_context
        fs_to_be_wrapped = fs_to_be_wrapped.unwrap()
    if isinstance(fs_to_be_wrapped, pa.fs.S3FileSystem):
        return _S3FileSystemWrapper(
            fs_to_be_wrapped, wrap_retries=wrap_retries, context=context
        )
    return filesystem


def _unwrap_s3_serialization_workaround(
    filesystem: Union["pyarrow.fs.FileSystem", "_S3FileSystemWrapper"],
    context: Optional[DataContext] = None,
):
    if isinstance(filesystem, _S3FileSystemWrapper):
        wrap_retries = filesystem._wrap_retries
        context = filesystem._context
        filesystem = filesystem.unwrap()
        if wrap_retries:
            filesystem = RetryingPyFileSystem.wrap(filesystem, context=context)
    return filesystem


class _S3FileSystemWrapper:
    def __init__(
        self,
        fs: "pyarrow.fs.S3FileSystem",
        wrap_retries: bool = False,
        context: Optional[DataContext] = None,
    ):
        self._fs = fs
        self._wrap_retries = wrap_retries
        self._context = context

    def unwrap(self):
        return self._fs

    @classmethod
    def _reconstruct(cls, fs_reconstruct, fs_args):
        # Implicitly trigger S3 subsystem initialization by importing
        # pyarrow.fs.
        import pyarrow.fs  # noqa: F401

        return cls(fs_reconstruct(*fs_args))

    def __reduce__(self):
        return _S3FileSystemWrapper._reconstruct, self._fs.__reduce__()


def _wrap_arrow_serialization_workaround(kwargs: dict) -> dict:
    if "filesystem" in kwargs:
        kwargs["filesystem"] = _wrap_s3_serialization_workaround(kwargs["filesystem"])

    return kwargs


def _unwrap_arrow_serialization_workaround(kwargs: dict) -> dict:
    if isinstance(kwargs.get("filesystem"), _S3FileSystemWrapper):
        kwargs["filesystem"] = kwargs["filesystem"].unwrap()
    return kwargs


def _resolve_kwargs(
    kwargs_fn: Callable[[], Dict[str, Any]], **kwargs
) -> Dict[str, Any]:
    if kwargs_fn:
        kwarg_overrides = kwargs_fn()
        kwargs.update(kwarg_overrides)
    return kwargs


def _validate_shuffle_arg(shuffle: Optional[str]) -> None:
    if not (
        shuffle is None or shuffle == "files" or isinstance(shuffle, FileShuffleConfig)
    ):
        raise ValueError(
            f"Invalid value for 'shuffle': {shuffle}. "
            "Valid values are None, 'files', `FileShuffleConfig`."
        )
