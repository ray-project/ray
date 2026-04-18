from enum import Enum
from typing import Iterator, List, Optional

import pyarrow as pa
import pyarrow.dataset as pds
from pyarrow import compute as pc
from pyarrow.fs import FileSystem, LocalFileSystem

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.readers.base_reader import Reader
from ray.data._internal.util import iterate_with_retry
from ray.data.context import DataContext
from ray.util.annotations import DeveloperAPI

# https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html#pyarrow.dataset.Scanner.from_batches
# Default is specified by PyArrow.
_ARROW_DEFAULT_BATCH_SIZE = 131_072


class FileFormat(str, Enum):
    PARQUET = "parquet"
    CSV = "csv"
    FEATHER = "feather"
    JSON = "json"
    ARROW = "arrow"
    IPC = "ipc"


@DeveloperAPI
class FileReader(Reader[FileManifest]):
    """Reader for file-based sources.

    This reader uses PyArrow's Dataset API which automatically handles:
    - Column pruning
    - Filter pushdown (row group pruning)
    - Batch-level filtering
    """

    def __init__(
        self,
        format: FileFormat,
        batch_size: int = _ARROW_DEFAULT_BATCH_SIZE,
        columns: Optional[List[str]] = None,
        predicate: Optional[pc.Expression] = None,
        limit: Optional[int] = None,
        filesystem: Optional[FileSystem] = None,
        partitioning: Optional[pds.Partitioning] = None,
        ignore_prefixes: Optional[List[str]] = None,
        include_paths: bool = False,
    ):
        """Initialize the reader.
        Refer to https://arrow.apache.org/docs/python/generated/pyarrow.dataset.dataset.html for more details.

        Args:
            format: Format of the files to read.
            batch_size: Number of rows per batch.
            columns: Columns to read. None means all columns.
            predicate: PyArrow compute expression for filtering.
            limit: Maximum number of rows to read.
            filesystem: Filesystem for reading files.
            partitioning: Partitioning to use for reading files.
            ignore_prefixes: Prefixes to ignore when reading files. Default is ['.', '_'] set by PyArrow.
            include_paths: If True, include the source file path in a
                ``'path'`` column for each row.

        """
        self._format = format
        self._columns = columns
        self._predicate = predicate
        self._batch_size = batch_size
        self._limit = limit
        self._filesystem = filesystem
        self._partitioning = partitioning
        self._ignore_prefixes = ignore_prefixes
        self._include_paths = include_paths

    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        """Read data from the input bucket and yield Arrow tables.

        This method is called on workers to perform the actual read operation.
        It should respect all pushdowns configured on this reader.

        Args:
            input_split: Work unit describing what data to read.

        Yields:
            pa.Table: PyArrow Tables containing the read data.
        """
        if len(input_split) > 0:
            paths = list(input_split.paths)
            filesystem = self._filesystem or LocalFileSystem()

            dataset = pds.dataset(
                source=paths,
                format=self._format.value,
                filesystem=filesystem,
                partitioning=self._partitioning,
                ignore_prefixes=self._ignore_prefixes,
            )

            batch_size = self._resolve_batch_size(dataset)

            scanner = dataset.scanner(
                columns=self._columns,
                filter=self._predicate,
                batch_size=batch_size,
                batch_readahead=1,
                **self._scanner_kwargs(),
            )

            ctx = DataContext.get_current()
            rows_read = 0
            for table, fragment_path in iterate_with_retry(
                lambda: self._read_batches(scanner),
                "read batches",
                match=ctx.retried_io_errors,
            ):
                if self._limit is not None:
                    remaining = self._limit - rows_read
                    if remaining <= 0:
                        break
                    if len(table) > remaining:
                        table = table.slice(0, remaining)

                if self._include_paths:
                    table = table.append_column(
                        "path",
                        pa.array([fragment_path] * table.num_rows, type=pa.string()),
                    )

                self._on_batch_read(table)
                rows_read += len(table)
                yield table

    def _resolve_batch_size(self, dataset: pds.Dataset) -> int:
        """Return the batch size to use for scanning.

        Subclasses can override this to implement adaptive batch sizing.
        """
        return self._batch_size

    def _on_batch_read(self, table: pa.Table) -> None:
        """Hook called after each batch is read.

        Subclasses can override this to update internal state (e.g., refine
        batch size estimates from actual data).
        """
        pass

    def _scanner_kwargs(self) -> dict:
        """Additional keyword arguments passed to ``pds.Dataset.scanner()``.

        Subclasses override this to inject format-specific options.
        """
        return {}

    @staticmethod
    def _read_batches(
        scanner: pds.Scanner,
    ) -> Iterator[tuple[pa.Table, str]]:
        """Yield non-empty (table, fragment_path) pairs from scanner batches."""
        for tagged in scanner.scan_batches():
            table = pa.Table.from_batches([tagged.record_batch])
            if table.num_rows > 0:
                yield table, tagged.fragment.path
