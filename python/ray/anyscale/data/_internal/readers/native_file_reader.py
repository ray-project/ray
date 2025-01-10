import abc
import io
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import pyarrow

from .file_reader import FileReader
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.util import call_with_retry, make_async_gen
from ray.data.block import BlockAccessor, DataBatch
from ray.data.context import DataContext
from ray.data.datasource import Partitioning, PathPartitionParser
from ray.data.datasource.file_based_datasource import (
    OPEN_FILE_MAX_ATTEMPTS,
    OPEN_FILE_RETRY_MAX_BACKOFF_SECONDS,
)


class NativeFileReader(FileReader):
    """Base class for reading a stream of bytes.

    Implementations of this interface should implement the `read_stream` method to read
    data from a stream of bytes and return data batches.
    """

    _NUM_THREADS_PER_TASK = 0

    def __init__(
        self,
        *,
        include_paths: bool,
        partitioning: Optional[Partitioning],
        open_args: Optional[Dict[str, Any]],
    ):
        if open_args is None:
            open_args = {}

        self._include_paths = include_paths
        self._partitioning = partitioning
        self._open_args = open_args
        self._data_context = DataContext.get_current()

    @property
    def data_context(self) -> DataContext:
        return self._data_context

    @abc.abstractmethod
    def read_stream(self, file: "pyarrow.NativeFile", path: str) -> Iterable[DataBatch]:
        ...

    def read_paths(
        self,
        paths: List[str],
        *,
        columns: Optional[List[str]] = None,
        columns_rename: Optional[Dict[str, str]] = None,
        filter_expr: Optional["pyarrow.dataset.Expression"] = None,
        filesystem,
    ) -> Iterable[DataBatch]:
        num_threads = self._NUM_THREADS_PER_TASK
        if len(paths) < num_threads:
            num_threads = len(paths)

        # TODO: We should refactor the code so that we can get the results in order even
        # when using multiple threads.
        if self._data_context.execution_options.preserve_order:
            num_threads = 0

        if columns and columns_rename:
            assert set(columns_rename.keys()).issubset(columns), (
                f"All column rename keys must be a subset of the columns list. "
                f"Invalid keys: {set(columns_rename.keys()) - set(columns)}"
            )

        def _read_paths(paths: List[str]):
            for path in paths:
                partitions = {}
                if self._partitioning is not None:
                    parse = PathPartitionParser(self._partitioning)
                    partitions = parse(path)

                file = call_with_retry(
                    lambda: self._open_input_source(path, filesystem=filesystem),
                    description=f"open file {path}",
                    match=self._data_context.retried_io_errors,
                    max_attempts=OPEN_FILE_MAX_ATTEMPTS,
                    max_backoff_s=OPEN_FILE_RETRY_MAX_BACKOFF_SECONDS,
                )
                for batch in self.read_stream(file, path):
                    if self._include_paths:
                        batch = _add_column_to_batch(batch, "path", path)
                    for partition, value in partitions.items():
                        if not columns or partition in columns:
                            batch = _add_column_to_batch(batch, partition, value)
                    if columns:
                        batch = _filter_columns(batch, columns)
                    if columns_rename:
                        batch = _rename_columns(batch, columns_rename)
                    yield batch

        if num_threads > 0:
            yield from make_async_gen(
                iter(paths),
                _read_paths,
                num_workers=num_threads,
            )
        else:
            yield from _read_paths(paths)

    def _open_input_source(
        self,
        path: str,
        *,
        filesystem: "pyarrow.fs.FileSystem",
    ) -> "pyarrow.NativeFile":
        """Opens a source path and returns a file-like object that can be read from.

        This implementation opens the source path as a sequential input stream, using
        `DataContext.streaming_read_buffer_size` as the buffer size if none is given by
        the caller.
        """
        import pyarrow as pa
        from pyarrow.fs import HadoopFileSystem

        open_args = self._open_args.copy()
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

        file = call_with_retry(
            lambda: filesystem.open_input_stream(
                path,
                buffer_size=buffer_size,
                **open_args,
            ),
            description=f"open file {path}",
            match=self._data_context.retried_io_errors,
        )

        if compression == "snappy":
            import snappy

            stream = io.BytesIO()
            if isinstance(filesystem, HadoopFileSystem):
                snappy.hadoop_snappy.stream_decompress(src=file, dst=stream)
            else:
                snappy.stream_decompress(src=file, dst=stream)
            stream.seek(0)

            file = pa.PythonFile(stream, mode="r")

        return file

    def estimate_in_memory_size(self, path: str, file_size: int, *, filesystem) -> int:
        batches = self.read_paths([path], filesystem=filesystem)

        try:
            first_batch = next(batches)
        except StopIteration:
            # If there's no data, return the on-disk file size.
            return file_size

        try:
            # Try to read a second batch. If it succeeds, it means the file contains
            # multiple batches.
            next(batches)
        except StopIteration:
            # Each file contains exactly one batch.
            builder = DelegatingBlockBuilder()
            builder.add_batch(first_batch)
            block = builder.build()

            in_memory_size = BlockAccessor.for_block(block).size_bytes()
        else:
            # Each file contains multiple batches. To avoid reading the entire file to
            # estimate the encoding ratio, default to the on-disk size.
            in_memory_size = file_size

        return in_memory_size


def _rename_columns(batch: DataBatch, columns_rename: Dict[str, str]) -> DataBatch:
    assert isinstance(batch, (pd.DataFrame, dict, pyarrow.Table)), batch

    if isinstance(batch, pd.DataFrame):
        batch = batch.rename(columns=columns_rename)
    elif isinstance(batch, dict):
        batch = {columns_rename.get(k, k): v for k, v in batch.items()}
    elif isinstance(batch, pyarrow.Table):
        batch = batch.rename_columns(
            [columns_rename.get(col, col) for col in batch.schema.names]
        )

    return batch


def _filter_columns(batch: DataBatch, columns: List[str]) -> DataBatch:
    assert isinstance(batch, (pd.DataFrame, dict, pyarrow.Table)), batch

    if isinstance(batch, pd.DataFrame):
        batch = batch[columns]
    elif isinstance(batch, dict):
        batch = {column: batch[column] for column in columns}
    elif isinstance(batch, pyarrow.Table):
        batch = batch.select(columns)

    return batch


def _add_column_to_batch(batch: DataBatch, column: str, value: Any) -> DataBatch:
    assert isinstance(batch, (pd.DataFrame, dict, pyarrow.Table)), batch

    if isinstance(batch, pd.DataFrame) and column not in batch.columns:
        batch[column] = value
    elif isinstance(batch, dict) and column not in batch:
        batch_size = len(batch[next(iter(batch.keys()))])
        batch[column] = [value] * batch_size
    elif isinstance(batch, pyarrow.Table) and column not in batch.column_names:
        batch = batch.append_column(column, pyarrow.array([value] * len(batch)))

    return batch
