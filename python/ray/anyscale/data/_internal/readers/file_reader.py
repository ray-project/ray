import abc
import io
from typing import Any, Dict, Iterable, List, Optional

import pyarrow

from ray.data._internal.util import call_with_retry, make_async_gen
from ray.data.block import DataBatch
from ray.data.context import DataContext
from ray.data.datasource import Partitioning, PathPartitionParser
from ray.data.datasource.file_based_datasource import (
    OPEN_FILE_MAX_ATTEMPTS,
    OPEN_FILE_RETRY_MAX_BACKOFF_SECONDS,
)


# TODO(@bveeramani): Consolidate this with `FileBasedDatasource` so that there aren't
# two divergent code paths.
class FileReader(abc.ABC):
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

    def open_input_source(
        self,
        path: str,
        *,
        filesystem: "pyarrow.fs.FileSystem",
    ) -> "pyarrow.io.InputStream":
        """Opens a source path for reading and returns the associated Arrow NativeFile.

        The default implementation opens the source path as a sequential input stream,
        using ctx.streaming_read_buffer_size as the buffer size if none is given by the
        caller.

        Implementations that do not support streaming reads (e.g. that require random
        access) should override this method.
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
            ctx = DataContext.get_current()
            buffer_size = ctx.streaming_read_buffer_size

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
            match=ctx.retried_io_errors,
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

    @abc.abstractmethod
    def read_stream(self, file: "pyarrow.NativeFile", path: str) -> Iterable[DataBatch]:
        ...

    def read_paths(
        self,
        paths: List[str],
        *,
        filesystem,
    ) -> Iterable[DataBatch]:
        num_threads = self._NUM_THREADS_PER_TASK
        if len(paths) < num_threads:
            num_threads = len(paths)

        # TODO: We should refactor the code so that we can get the results in order even
        # when using multiple threads.
        ctx = DataContext.get_current()
        if ctx.execution_options.preserve_order:
            num_threads = 0

        def _read_paths(paths: List[str]):
            for path in paths:
                partitions = {}
                if self._partitioning is not None:
                    parse = PathPartitionParser(self._partitioning)
                    partitions = parse(path)

                file = call_with_retry(
                    lambda: self.open_input_source(path, filesystem=filesystem),
                    description=f"open file {path}",
                    match=ctx.retried_io_errors,
                    max_attempts=OPEN_FILE_MAX_ATTEMPTS,
                    max_backoff_s=OPEN_FILE_RETRY_MAX_BACKOFF_SECONDS,
                )
                for batch in self.read_stream(file, path):
                    if self._include_paths:
                        batch = _add_column_to_batch(batch, "path", path)
                    for partition, value in partitions.items():
                        batch = _add_column_to_batch(batch, partition, value)
                    yield batch

        if num_threads > 0:
            yield from make_async_gen(
                iter(paths),
                _read_paths,
                num_workers=num_threads,
            )
        else:
            yield from _read_paths(paths)


def _add_column_to_batch(batch: DataBatch, column: str, value: Any) -> DataBatch:
    import pandas as pd
    import pyarrow as pa

    assert isinstance(batch, (pd.DataFrame, dict, pa.Table)), batch

    if isinstance(batch, pd.DataFrame) and column not in batch.columns:
        batch[column] = value
    elif isinstance(batch, dict) and column not in batch:
        batch_size = len(batch[next(iter(batch.keys()))])
        batch[column] = [value] * batch_size
    elif isinstance(batch, pa.Table) and column not in batch.column_names:
        batch = batch.append_column(column, pa.array([value] * len(batch)))

    return batch
