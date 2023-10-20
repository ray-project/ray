import os
import pathlib
import sys
import urllib.parse
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
    Tuple,
    Union,
)

import numpy as np

from ray._private.utils import _add_creatable_buckets_param_if_s3_uri
from ray.air._internal.remote_storage import _is_local_windows_path
from ray.data._internal.dataset_logger import DatasetLogger
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import (
    _check_pyarrow_version,
    _resolve_custom_scheme,
    make_async_gen,
)
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.block_path_provider import (
    BlockWritePathProvider,
    DefaultBlockWritePathProvider,
)
from ray.data.datasource.datasource import Datasource, Reader, ReadTask, WriteResult
from ray.data.datasource.file_meta_provider import (
    BaseFileMetadataProvider,
    DefaultFileMetadataProvider,
)
from ray.data.datasource.partitioning import (
    Partitioning,
    PathPartitionFilter,
    PathPartitionParser,
)
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow


logger = DatasetLogger(__name__)


# We should parallelize file size fetch operations beyond this threshold.
FILE_SIZE_FETCH_PARALLELIZATION_THRESHOLD = 16

# 16 file size fetches from S3 takes ~1.5 seconds with Arrow's S3FileSystem.
PATHS_PER_FILE_SIZE_FETCH_TASK = 16

# The errors to retry for opening file.
OPEN_FILE_RETRY_ON_ERRORS = ["AWS Error SLOW_DOWN"]

# The max retry backoff in seconds for opening file.
OPEN_FILE_RETRY_MAX_BACKOFF_SECONDS = 32

# The max number of attempts for opening file.
OPEN_FILE_MAX_ATTEMPTS = 10


@PublicAPI(stability="beta")
class FileExtensionFilter(PathPartitionFilter):
    """A file-extension-based path filter that filters files that don't end
    with the provided extension(s).

    Attributes:
        file_extensions: File extension(s) of files to be included in reading.
        allow_if_no_extension: If this is True, files without any extensions
            will be included in reading.

    """

    def __init__(
        self,
        file_extensions: Union[str, List[str]],
        allow_if_no_extension: bool = False,
    ):
        if isinstance(file_extensions, str):
            file_extensions = [file_extensions]

        self.extensions = [f".{ext.lower()}" for ext in file_extensions]
        self.allow_if_no_extension = allow_if_no_extension

    def _file_has_extension(self, path: str):
        suffixes = [suffix.lower() for suffix in pathlib.Path(path).suffixes]
        if not suffixes:
            return self.allow_if_no_extension
        return any(ext in suffixes for ext in self.extensions)

    def __call__(self, paths: List[str]) -> List[str]:
        return [path for path in paths if self._file_has_extension(path)]

    def __str__(self):
        return (
            f"{type(self).__name__}(extensions={self.extensions}, "
            f"allow_if_no_extensions={self.allow_if_no_extension})"
        )

    def __repr__(self):
        return str(self)


@DeveloperAPI
class FileBasedDatasource(Datasource):
    """File-based datasource, for reading and writing files.

    This class should not be used directly, and should instead be subclassed
    and tailored to particular file formats. Classes deriving from this class
    must implement _read_file().

    If the _FILE_EXTENSION is defined, per default only files with this extension
    will be read. If None, no default filter is used.

    Current subclasses:
        JSONDatasource, CSVDatasource, NumpyDatasource, BinaryDatasource
    """

    # If `_WRITE_FILE_PER_ROW` is `True`, this datasource calls `_write_row` and writes
    # each row to a file. Otherwise, this datasource calls `_write_block` and writes
    # each block to a file.
    _WRITE_FILE_PER_ROW = False
    _FILE_EXTENSION: Optional[Union[str, List[str]]] = None
    # Number of threads for concurrent reading within each read task.
    # If zero or negative, reading will be performed in the main thread.
    _NUM_THREADS_PER_TASK = 0

    def _open_input_source(
        self,
        filesystem: "pyarrow.fs.FileSystem",
        path: str,
        **open_args,
    ) -> "pyarrow.NativeFile":
        """Opens a source path for reading and returns the associated Arrow NativeFile.

        The default implementation opens the source path as a sequential input stream,
        using ctx.streaming_read_buffer_size as the buffer size if none is given by the
        caller.

        Implementations that do not support streaming reads (e.g. that require random
        access) should override this method.
        """
        buffer_size = open_args.pop("buffer_size", None)
        if buffer_size is None:
            ctx = DataContext.get_current()
            buffer_size = ctx.streaming_read_buffer_size
        return filesystem.open_input_stream(path, buffer_size=buffer_size, **open_args)

    def create_reader(self, **kwargs):
        return _FileBasedDatasourceReader(self, **kwargs)

    def _rows_per_file(self):
        """Returns the number of rows per file, or None if unknown."""
        return None

    def _read_stream(
        self, f: "pyarrow.NativeFile", path: str, **reader_args
    ) -> Iterator[Block]:
        """Streaming read a single file, passing all kwargs to the reader.

        By default, delegates to self._read_file().
        """
        yield self._read_file(f, path, **reader_args)

    def _read_file(self, f: "pyarrow.NativeFile", path: str, **reader_args) -> Block:
        """Reads a single file, passing all kwargs to the reader.

        This method should be implemented by subclasses.
        """
        raise NotImplementedError(
            "Subclasses of FileBasedDatasource must implement _read_file()."
        )

    def on_write_start(
        self,
        path: str,
        try_create_dir: bool = True,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        **write_args,
    ) -> None:
        """Create a directory to write files to.

        If ``try_create_dir`` is ``False``, this method is a no-op.
        """
        from pyarrow.fs import FileType

        self.has_created_dir = False
        if try_create_dir:
            paths, filesystem = _resolve_paths_and_filesystem(path, filesystem)
            assert len(paths) == 1, len(paths)
            path = paths[0]

            if filesystem.get_file_info(path).type is FileType.NotFound:
                # Arrow's S3FileSystem doesn't allow creating buckets by default, so we
                # add a query arg enabling bucket creation if an S3 URI is provided.
                tmp = _add_creatable_buckets_param_if_s3_uri(path)
                filesystem.create_dir(tmp, recursive=True)
                self.has_created_dir = True

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
        path: str,
        dataset_uuid: str,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        open_stream_args: Optional[Dict[str, Any]] = None,
        block_path_provider: Optional[BlockWritePathProvider] = None,
        write_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        file_format: Optional[str] = None,
        _block_udf: Optional[Callable[[Block], Block]] = None,
        **write_args,
    ) -> WriteResult:
        """Write blocks for a file-based datasource."""
        # `FileBasedDatasource` subclasses expose a `_FILE_EXTENSION` attribute. It
        # represents a list of supported file extensions. If the user doesn't specify
        # a file format, we default to the first extension in the list.
        if file_format is None:
            file_format = self._FILE_EXTENSION
            if isinstance(file_format, list):
                file_format = file_format[0]

        path, filesystem = _resolve_paths_and_filesystem(path, filesystem)
        path = path[0]
        _write_block_to_file = self._write_block
        _write_row_to_file = self._write_row

        if open_stream_args is None:
            open_stream_args = {}

        if not block_path_provider:
            block_path_provider = DefaultBlockWritePathProvider()

        num_rows_written = 0
        block_idx = 0
        for block in blocks:
            if _block_udf is not None:
                block = _block_udf(block)

            block = BlockAccessor.for_block(block)
            if block.num_rows() == 0:
                continue

            fs = _unwrap_s3_serialization_workaround(filesystem)

            if self._WRITE_FILE_PER_ROW:
                for row_index, row in enumerate(
                    block.iter_rows(public_row_format=False)
                ):
                    # TODO: Refactor `BlockWritePathProvider` to support rows.
                    filename = (
                        f"{dataset_uuid}_{ctx.task_idx:06}_{block_idx:06}_"
                        f"{row_index:06}.{file_format}"
                    )
                    write_path = os.path.join(path, filename)
                    logger.get_logger().debug(f"Writing {write_path} file.")
                    with _open_file_with_retry(
                        write_path,
                        lambda: fs.open_output_stream(write_path, **open_stream_args),
                    ) as f:
                        _write_row_to_file(
                            f,
                            row,
                            writer_args_fn=write_args_fn,
                            file_format=file_format,
                            **write_args,
                        )
            else:
                write_path = block_path_provider(
                    path,
                    filesystem=filesystem,
                    dataset_uuid=dataset_uuid,
                    task_index=ctx.task_idx,
                    block_index=block_idx,
                    file_format=file_format,
                )
                logger.get_logger().debug(f"Writing {write_path} file.")
                with _open_file_with_retry(
                    write_path,
                    lambda: fs.open_output_stream(write_path, **open_stream_args),
                ) as f:
                    _write_block_to_file(
                        f,
                        block,
                        writer_args_fn=write_args_fn,
                        **write_args,
                    )

            num_rows_written += block.num_rows()
            block_idx += 1

        if num_rows_written == 0:
            logger.get_logger().warning(
                f"Skipping writing empty dataset with UUID {dataset_uuid} at {path}",
            )
            return "skip"

        # TODO: decide if we want to return richer object when the task
        # succeeds.
        return "ok"

    def on_write_complete(
        self,
        write_results: List[WriteResult],
        path: Optional[str] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        **kwargs,
    ) -> None:
        if not self.has_created_dir:
            return

        paths, filesystem = _resolve_paths_and_filesystem(path, filesystem)
        assert len(paths) == 1, len(paths)
        path = paths[0]

        if all(write_results == "skip" for write_results in write_results):
            filesystem.delete_dir(path)

    def _write_block(
        self,
        f: "pyarrow.NativeFile",
        block: BlockAccessor,
        writer_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        **writer_args,
    ):
        """Writes a block to a single file, passing all kwargs to the writer.

        This method should be implemented by subclasses.
        """
        raise NotImplementedError(
            "Subclasses of FileBasedDatasource must implement _write_files()."
        )

    def _write_row(
        self,
        f: "pyarrow.NativeFile",
        row,
        writer_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        **writer_args,
    ):
        """Writes a row to a single file, passing all kwargs to the writer.

        If `_WRITE_FILE_PER_ROW` is set to `True`, this method will be called instead
        of `_write_block()`.
        """
        raise NotImplementedError

    @classmethod
    def file_extension_filter(cls) -> Optional[PathPartitionFilter]:
        if cls._FILE_EXTENSION is None:
            return None
        return FileExtensionFilter(cls._FILE_EXTENSION)


class _FileBasedDatasourceReader(Reader):
    def __init__(
        self,
        delegate: FileBasedDatasource,
        paths: Union[str, List[str]],
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        schema: Optional[Union[type, "pyarrow.lib.Schema"]] = None,
        open_stream_args: Optional[Dict[str, Any]] = None,
        meta_provider: BaseFileMetadataProvider = DefaultFileMetadataProvider(),
        partition_filter: PathPartitionFilter = None,
        partitioning: Partitioning = None,
        ignore_missing_paths: bool = False,
        shuffle: Union[Literal["files"], None] = None,
        **reader_args,
    ):
        _check_pyarrow_version()
        self._delegate = delegate
        self._schema = schema
        self._open_stream_args = open_stream_args
        self._meta_provider = meta_provider
        self._partition_filter = partition_filter
        self._partitioning = partitioning
        self._ignore_missing_paths = ignore_missing_paths
        self._reader_args = reader_args
        paths, self._filesystem = _resolve_paths_and_filesystem(paths, filesystem)
        self._paths, self._file_sizes = map(
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

        if ignore_missing_paths and len(self._paths) == 0:
            raise ValueError(
                "None of the provided paths exist. "
                "The 'ignore_missing_paths' field is set to True."
            )

        if self._partition_filter is not None:
            # Use partition filter to skip files which are not needed.
            path_to_size = dict(zip(self._paths, self._file_sizes))
            self._paths = self._partition_filter(self._paths)
            self._file_sizes = [path_to_size[p] for p in self._paths]
            if len(self._paths) == 0:
                raise ValueError(
                    "No input files found to read. Please double check that "
                    "'partition_filter' field is set properly."
                )
        self._file_metadata_shuffler = None
        if shuffle == "files":
            self._file_metadata_shuffler = np.random.default_rng()

    def estimate_inmemory_data_size(self) -> Optional[int]:
        total_size = 0
        for sz in self._file_sizes:
            if sz is not None:
                total_size += sz
        return total_size

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        import numpy as np

        ctx = DataContext.get_current()
        open_stream_args = self._open_stream_args
        reader_args = self._reader_args
        partitioning = self._partitioning

        if self._file_metadata_shuffler is not None:
            files_metadata = list(zip(self._paths, self._file_sizes))
            shuffled_files_metadata = [
                files_metadata[i]
                for i in self._file_metadata_shuffler.permutation(len(files_metadata))
            ]
            paths, file_sizes = list(map(list, zip(*shuffled_files_metadata)))
        else:
            paths, file_sizes = self._paths, self._file_sizes

        read_stream = self._delegate._read_stream
        filesystem = _wrap_s3_serialization_workaround(self._filesystem)

        if open_stream_args is None:
            open_stream_args = {}

        open_input_source = self._delegate._open_input_source

        def read_files(
            read_paths: Iterable[str],
        ) -> Iterable[Block]:
            nonlocal filesystem, open_stream_args, reader_args, partitioning

            DataContext._set_current(ctx)
            fs = _unwrap_s3_serialization_workaround(filesystem)
            for read_path in read_paths:
                compression = open_stream_args.pop("compression", None)
                if compression is None:
                    import pyarrow as pa

                    try:
                        # If no compression manually given, try to detect
                        # compression codec from path.
                        compression = pa.Codec.detect(read_path).name
                    except (ValueError, TypeError):
                        # Arrow's compression inference on the file path
                        # doesn't work for Snappy, so we double-check ourselves.
                        import pathlib

                        suffix = pathlib.Path(read_path).suffix
                        if suffix and suffix[1:] == "snappy":
                            compression = "snappy"
                        else:
                            compression = None
                if compression == "snappy":
                    # Pass Snappy compression as a reader arg, so datasource subclasses
                    # can manually handle streaming decompression in
                    # self._delegate._read_stream().
                    reader_args["compression"] = compression
                    reader_args["filesystem"] = fs
                elif compression is not None:
                    # Non-Snappy compression, pass as open_input_stream() arg so Arrow
                    # can take care of streaming decompression for us.
                    open_stream_args["compression"] = compression

                partitions: Dict[str, str] = {}
                if partitioning is not None:
                    parse = PathPartitionParser(partitioning)
                    partitions = parse(read_path)

                with _open_file_with_retry(
                    read_path,
                    lambda: open_input_source(fs, read_path, **open_stream_args),
                ) as f:
                    for data in read_stream(f, read_path, **reader_args):
                        if partitions:
                            data = _add_partitions(data, partitions)
                        yield data

        def create_read_task_fn(read_paths, num_threads):
            def read_task_fn():
                nonlocal num_threads, read_paths

                if num_threads > 0:
                    if len(read_paths) < num_threads:
                        num_threads = len(read_paths)

                    logger.get_logger().debug(
                        f"Reading {len(read_paths)} files with {num_threads} threads."
                    )

                    yield from make_async_gen(
                        iter(read_paths),
                        read_files,
                        num_workers=num_threads,
                    )
                else:
                    logger.get_logger().debug(f"Reading {len(read_paths)} files.")
                    yield from read_files(read_paths)

            return read_task_fn

        # fix https://github.com/ray-project/ray/issues/24296
        parallelism = min(parallelism, len(paths))

        read_tasks = []
        for read_paths, file_sizes in zip(
            np.array_split(paths, parallelism), np.array_split(file_sizes, parallelism)
        ):
            if len(read_paths) <= 0:
                continue

            meta = self._meta_provider(
                read_paths,
                self._schema,
                rows_per_file=self._delegate._rows_per_file(),
                file_sizes=file_sizes,
            )

            read_task_fn = create_read_task_fn(
                read_paths, self._delegate._NUM_THREADS_PER_TASK
            )

            read_task = ReadTask(read_task_fn, meta)

            read_tasks.append(read_task)

        return read_tasks


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


# TODO(Clark): Add unit test coverage of _resolve_paths_and_filesystem and
# _expand_paths.


def _resolve_paths_and_filesystem(
    paths: Union[str, List[str]],
    filesystem: "pyarrow.fs.FileSystem" = None,
) -> Tuple[List[str], "pyarrow.fs.FileSystem"]:
    """
    Resolves and normalizes all provided paths, infers a filesystem from the
    paths and ensures that all paths use the same filesystem.

    Args:
        paths: A single file/directory path or a list of file/directory paths.
            A list of paths can contain both files and directories.
        filesystem: The filesystem implementation that should be used for
            reading these files. If None, a filesystem will be inferred. If not
            None, the provided filesystem will still be validated against all
            filesystems inferred from the provided paths to ensure
            compatibility.
    """
    import pyarrow as pa
    from pyarrow.fs import (
        FileSystem,
        FSSpecHandler,
        PyFileSystem,
        _resolve_filesystem_and_path,
    )

    if isinstance(paths, str):
        paths = [paths]
    if isinstance(paths, pathlib.Path):
        paths = [str(paths)]
    elif not isinstance(paths, list) or any(not isinstance(p, str) for p in paths):
        raise ValueError(
            "Expected `paths` to be a `str`, `pathlib.Path`, or `list[str]`, but got "
            f"`{paths}`."
        )
    elif len(paths) == 0:
        raise ValueError("Must provide at least one path.")

    need_unwrap_path_protocol = True
    if filesystem and not isinstance(filesystem, FileSystem):
        err_msg = (
            f"The filesystem passed must either conform to "
            f"pyarrow.fs.FileSystem, or "
            f"fsspec.spec.AbstractFileSystem. The provided "
            f"filesystem was: {filesystem}"
        )
        try:
            import fsspec
            from fsspec.implementations.http import HTTPFileSystem
        except ModuleNotFoundError:
            # If filesystem is not a pyarrow filesystem and fsspec isn't
            # installed, then filesystem is neither a pyarrow filesystem nor
            # an fsspec filesystem, so we raise a TypeError.
            raise TypeError(err_msg) from None
        if not isinstance(filesystem, fsspec.spec.AbstractFileSystem):
            raise TypeError(err_msg) from None
        if isinstance(filesystem, HTTPFileSystem):
            # If filesystem is fsspec HTTPFileSystem, the protocol/scheme of paths
            # should not be unwrapped/removed, because HTTPFileSystem expects full file
            # paths including protocol/scheme. This is different behavior compared to
            # file systems implementation in pyarrow.fs.FileSystem.
            need_unwrap_path_protocol = False

        filesystem = PyFileSystem(FSSpecHandler(filesystem))

    resolved_paths = []
    for path in paths:
        path = _resolve_custom_scheme(path)
        try:
            resolved_filesystem, resolved_path = _resolve_filesystem_and_path(
                path, filesystem
            )
        except pa.lib.ArrowInvalid as e:
            if "Cannot parse URI" in str(e):
                resolved_filesystem, resolved_path = _resolve_filesystem_and_path(
                    _encode_url(path), filesystem
                )
                resolved_path = _decode_url(resolved_path)
            elif "Unrecognized filesystem type in URI" in str(e):
                scheme = urllib.parse.urlparse(path, allow_fragments=False).scheme
                if scheme in ["http", "https"]:
                    # If scheme of path is HTTP and filesystem is not resolved,
                    # try to use fsspec HTTPFileSystem. This expects fsspec is
                    # installed.
                    try:
                        from fsspec.implementations.http import HTTPFileSystem
                    except ModuleNotFoundError:
                        raise ImportError(
                            "To read files from HTTP, run `pip install fsspec[http]`."
                        ) from None

                    resolved_filesystem = PyFileSystem(FSSpecHandler(HTTPFileSystem()))
                    resolved_path = path
                    need_unwrap_path_protocol = False
                else:
                    raise
            else:
                raise
        if filesystem is None:
            filesystem = resolved_filesystem
        elif need_unwrap_path_protocol:
            resolved_path = _unwrap_protocol(resolved_path)
        resolved_path = filesystem.normalize_path(resolved_path)
        resolved_paths.append(resolved_path)

    return resolved_paths, filesystem


def _is_url(path) -> bool:
    return urllib.parse.urlparse(path).scheme != ""


def _encode_url(path):
    return urllib.parse.quote(path, safe="/:")


def _decode_url(path):
    return urllib.parse.unquote(path)


def _unwrap_protocol(path):
    """
    Slice off any protocol prefixes on path.
    """
    if sys.platform == "win32" and _is_local_windows_path(path):
        # Represent as posix path such that downstream functions properly handle it.
        # This is executed when 'file://' is NOT included in the path.
        return pathlib.Path(path).as_posix()

    parsed = urllib.parse.urlparse(path, allow_fragments=False)  # support '#' in path
    query = "?" + parsed.query if parsed.query else ""  # support '?' in path
    netloc = parsed.netloc
    if parsed.scheme == "s3" and "@" in parsed.netloc:
        # If the path contains an @, it is assumed to be an anonymous
        # credentialed path, and we need to strip off the credentials.
        netloc = parsed.netloc.split("@")[-1]

    parsed_path = parsed.path
    # urlparse prepends the path with a '/'. This does not work on Windows
    # so if this is the case strip the leading slash.
    if (
        sys.platform == "win32"
        and not netloc
        and len(parsed_path) >= 3
        and parsed_path[0] == "/"  # The problematic leading slash
        and parsed_path[1].isalpha()  # Ensure it is a drive letter.
        and parsed_path[2:4] in (":", ":/")
    ):
        parsed_path = parsed_path[1:]

    return netloc + parsed_path + query


def _wrap_s3_serialization_workaround(filesystem: "pyarrow.fs.FileSystem"):
    # This is needed because pa.fs.S3FileSystem assumes pa.fs is already
    # imported before deserialization. See #17085.
    import pyarrow as pa
    import pyarrow.fs

    if isinstance(filesystem, pa.fs.S3FileSystem):
        return _S3FileSystemWrapper(filesystem)
    return filesystem


def _unwrap_s3_serialization_workaround(
    filesystem: Union["pyarrow.fs.FileSystem", "_S3FileSystemWrapper"]
):
    if isinstance(filesystem, _S3FileSystemWrapper):
        return filesystem.unwrap()
    else:
        return filesystem


class _S3FileSystemWrapper:
    def __init__(self, fs: "pyarrow.fs.S3FileSystem"):
        self._fs = fs

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


def _open_file_with_retry(
    file_path: str,
    open_file: Callable[[], "pyarrow.NativeFile"],
) -> "pyarrow.NativeFile":
    """Open file with an exponential backoff retry strategy.

    This is to avoid transient task failure with remote storage (such as S3),
    when the remote storage throttles the requests.
    """
    import random
    import time

    if OPEN_FILE_MAX_ATTEMPTS < 1:
        raise ValueError(
            "OPEN_FILE_MAX_ATTEMPTS cannot be negative or 0. Get: "
            f"{OPEN_FILE_MAX_ATTEMPTS}"
        )

    for i in range(OPEN_FILE_MAX_ATTEMPTS):
        try:
            return open_file()
        except Exception as e:
            error_message = str(e)
            is_retryable = any(
                [error in error_message for error in OPEN_FILE_RETRY_ON_ERRORS]
            )
            if is_retryable and i + 1 < OPEN_FILE_MAX_ATTEMPTS:
                # Retry with binary expoential backoff with random jitter.
                backoff = min(
                    (2 ** (i + 1)) * random.random(),
                    OPEN_FILE_RETRY_MAX_BACKOFF_SECONDS,
                )
                logger.get_logger().debug(
                    f"Retrying {i+1} attempts to open file {file_path} after "
                    f"{backoff} seconds."
                )
                time.sleep(backoff)
            else:
                raise e from None
