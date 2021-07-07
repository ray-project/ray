import builtins
import logging
from typing import Any, Generic, Optional, List, Tuple, Callable, Union, \
    TypeVar, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow

import numpy as np

import ray
from ray.experimental.data.impl.arrow_block import ArrowRow, ArrowBlock
from ray.experimental.data.impl.block import Block, SimpleBlock
from ray.experimental.data.impl.block_list import BlockList, BlockMetadata

T = TypeVar("T")
WriteResult = Any

logger = logging.getLogger(__name__)


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
        filtered_paths.append((file_path, file_))
    # We sort the paths to guarantee a stable order.
    return zip(*sorted(filtered_paths, key=lambda x: x[0]))


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
    file_infos = []
    for path in resolved_paths:
        file_info = filesystem.get_file_info(path)
        if file_info.type == FileType.Directory:
            paths, file_infos_ = _expand_directory(path, filesystem)
            expanded_paths.extend(paths)
            file_infos.extend(file_infos_)
        elif file_info.type == FileType.File:
            expanded_paths.append(path)
            file_infos.append(file_info)
        else:
            raise FileNotFoundError(path)
    return expanded_paths, file_infos, filesystem


class Datasource(Generic[T]):
    """Interface for defining a custom ``ray.data.Dataset`` datasource.

    To read a datasource into a dataset, use ``ray.data.read_datasource()``.
    To write to a writable datasource, use ``Dataset.write_datasource()``.

    See ``RangeDatasource`` and ``DummyOutputDatasource`` below for examples
    of how to implement readable and writable datasources.
    """

    def prepare_read(self, parallelism: int,
                     **read_args) -> List["ReadTask[T]"]:
        """Return the list of tasks needed to perform a read.

        Args:
            parallelism: The requested read parallelism. The number of read
                tasks should be as close to this value as possible.
            read_args: Additional kwargs to pass to the datasource impl.

        Returns:
            A list of read tasks that can be executed to read blocks from the
            datasource in parallel.
        """
        raise NotImplementedError

    def prepare_write(self, blocks: BlockList,
                      **write_args) -> List["WriteTask[T]"]:
        """Return the list of tasks needed to perform a write.

        Args:
            blocks: List of data block references and block metadata. It is
                recommended that one write task be generated per block.
            write_args: Additional kwargs to pass to the datasource impl.

        Returns:
            A list of write tasks that can be executed to write blocks to the
            datasource in parallel.
        """
        raise NotImplementedError

    def on_write_complete(self, write_tasks: List["WriteTask[T]"],
                          write_task_outputs: List[WriteResult],
                          **kwargs) -> None:
        """Callback for when a write job completes.

        This can be used to "commit" a write output. This method must
        succeed prior to ``write_datasource()`` returning to the user. If this
        method fails, then ``on_write_failed()`` will be called.

        Args:
            write_tasks: The list of the original write tasks.
            write_task_outputs: The list of write task outputs.
            kwargs: Forward-compatibility placeholder.
        """
        pass

    def on_write_failed(self, write_tasks: List["WriteTask[T]"],
                        error: Exception, **kwargs) -> None:
        """Callback for when a write job fails.

        This is called on a best-effort basis on write failures.

        Args:
            write_tasks: The list of the original write tasks.
            error: The first error encountered.
            kwargs: Forward-compatibility placeholder.
        """
        pass


class ReadTask(Callable[[], Block[T]]):
    """A function used to read a block of a dataset.

    Read tasks are generated by ``datasource.prepare_read()``, and return
    a ``ray.data.Block`` when called. Metadata about the read operation can
    be retrieved via ``get_metadata()`` prior to executing the read.

    Ray will execute read tasks in remote functions to parallelize execution.
    """

    def __init__(self, read_fn: Callable[[], Block[T]],
                 metadata: BlockMetadata):
        self._metadata = metadata
        self._read_fn = read_fn

    def get_metadata(self) -> BlockMetadata:
        return self._metadata

    def __call__(self) -> Block[T]:
        return self._read_fn()


class WriteTask(Callable[[], WriteResult]):
    """A function used to write a chunk of a dataset.

    Write tasks are generated by ``datasource.prepare_write()``, and return
    a datasource-specific output that is passed to ``on_write_complete()``
    on write completion.

    Ray will execute write tasks in remote functions to parallelize execution.
    """

    def __init__(self, write_fn: Callable[[], WriteResult]):
        self._write_fn = write_fn

    def __call__(self) -> WriteResult:
        return self._write_fn()


class RangeDatasource(Datasource[Union[ArrowRow, int]]):
    """An example datasource that generates ranges of numbers from [0..n).

    Examples:
        >>> source = RangeDatasource()
        >>> ray.data.read_datasource(source, n=10).take()
        ... [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    """

    def prepare_read(self, parallelism: int, n: int,
                     use_arrow: bool) -> List[ReadTask]:
        read_tasks: List[ReadTask] = []
        block_size = max(1, n // parallelism)

        # Example of a read task. In a real datasource, this would pull data
        # from an external system instead of generating dummy data.
        def make_block(start: int, count: int) -> Block[Union[ArrowRow, int]]:
            if use_arrow:
                return ArrowBlock(
                    pyarrow.Table.from_arrays(
                        [np.arange(start, start + count)], names=["value"]))
            else:
                return SimpleBlock(list(builtins.range(start, start + count)))

        i = 0
        while i < n:
            count = min(block_size, n - i)
            if use_arrow:
                import pyarrow
                schema = pyarrow.Table.from_pydict({"value": [0]}).schema
            else:
                schema = int
            read_tasks.append(
                ReadTask(
                    lambda i=i, count=count: make_block(i, count),
                    BlockMetadata(
                        num_rows=count,
                        size_bytes=8 * count,
                        schema=schema,
                        input_files=None)))
            i += block_size

        return read_tasks


class JSONDatasource(Datasource[Union[ArrowRow, int]]):
    """JSON datasource, for reading JSON files.

    Examples:
        >>> source = JSONDatasource()
        >>> ray.data.read_datasource(source, paths="/path/to/dir").take()
        ... {"a": 1, "b": "foo"}
    """

    def prepare_read(
            self, parallelism: int, paths: Union[str, List[str]],
            filesystem: Optional["pyarrow.fs.FileSystem"] = None,
            **arrow_json_args) -> List[ReadTask]:
        import pyarrow as pa
        from pyarrow import json
        import numpy as np

        paths, file_infos, filesystem = _resolve_paths_and_filesystem(
            paths, filesystem)
        file_sizes = [file_info.size for file_info in file_infos]

        def read_json(read_paths: List[str]):
            logger.debug(f"Reading {len(read_paths)} files.")
            tables = []
            for read_path in read_paths:
                with filesystem.open_input_file(read_path) as f:
                    tables.append(
                        json.read_json(
                            f,
                            read_options=json.ReadOptions(use_threads=False),
                            **arrow_json_args))
            block = ArrowBlock(pa.concat_tables(tables))
            return block

        read_tasks = [
            ReadTask(
                lambda read_paths=read_paths: read_json(read_paths),
                BlockMetadata(
                    num_rows=None,
                    size_bytes=sum(file_sizes),
                    schema=None,
                    input_files=read_paths))
            for read_paths, file_sizes in zip(
                    np.array_split(paths, parallelism),
                    np.array_split(file_sizes, parallelism))
            if len(read_paths) > 0]

        return read_tasks


class CSVDatasource(Datasource[Union[ArrowRow, int]]):
    """CSV datasource, for reading CSV files.

    Examples:
        >>> source = CSVDatasource()
        >>> ray.data.read_datasource(source, paths="/path/to/dir").take()
        ... {"a": 1, "b": "foo"}
    """

    def prepare_read(
            self, parallelism: int, paths: Union[str, List[str]],
            filesystem: Optional["pyarrow.fs.FileSystem"] = None,
            **arrow_csv_args) -> List[ReadTask]:
        import pyarrow as pa
        from pyarrow import csv
        import numpy as np

        paths, file_infos, filesystem = _resolve_paths_and_filesystem(
            paths, filesystem)
        file_sizes = [file_info.size for file_info in file_infos]

        def read_csv(read_paths: List[str]):
            logger.debug(f"Reading {len(read_paths)} files.")
            tables = []
            for read_path in read_paths:
                with filesystem.open_input_file(read_path) as f:
                    tables.append(
                        csv.read_csv(
                            f,
                            read_options=csv.ReadOptions(use_threads=False),
                            **arrow_csv_args))
            block = ArrowBlock(pa.concat_tables(tables))
            return block

        read_tasks = [
            ReadTask(
                lambda read_paths=read_paths: read_csv(read_paths),
                BlockMetadata(
                    num_rows=None,
                    size_bytes=sum(file_sizes),
                    schema=None,
                    input_files=read_paths))
            for read_paths, file_sizes in zip(
                    np.array_split(paths, parallelism),
                    np.array_split(file_sizes, parallelism))
            if len(read_paths) > 0]

        return read_tasks


class DummyOutputDatasource(Datasource[Union[ArrowRow, int]]):
    """An example implementation of a writable datasource for testing.

    Examples:
        >>> output = DummyOutputDatasource()
        >>> ray.data.range(10).write_datasource(output)
        >>> assert output.num_ok == 1
    """

    def __init__(self):
        # Setup a dummy actor to send the data. In a real datasource, write
        # tasks would send data to an external system instead of a Ray actor.
        @ray.remote
        class DataSink:
            def __init__(self):
                self.rows_written = 0
                self.enabled = True

            def write(self, block: Block[T]) -> str:
                if not self.enabled:
                    raise ValueError("disabled")
                self.rows_written += block.num_rows()
                return "ok"

            def get_rows_written(self):
                return self.rows_written

            def set_enabled(self, enabled):
                self.enabled = enabled

        self.data_sink = DataSink.remote()
        self.num_ok = 0
        self.num_failed = 0

    def prepare_write(self, blocks: BlockList,
                      **write_args) -> List["WriteTask[T]"]:
        tasks = []
        for b in blocks:
            tasks.append(
                WriteTask(lambda b=b: ray.get(self.data_sink.write.remote(b))))
        return tasks

    def on_write_complete(self, write_tasks: List["WriteTask[T]"],
                          write_task_outputs: List[WriteResult]) -> None:
        assert len(write_task_outputs) == len(write_tasks)
        assert all(w == "ok" for w in write_task_outputs), write_task_outputs
        self.num_ok += 1

    def on_write_failed(self, write_tasks: List["WriteTask[T]"],
                        error: Exception) -> None:
        self.num_failed += 1
