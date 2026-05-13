from enum import Enum
from functools import cached_property
from typing import Any, Iterator, List, Optional, Set, Tuple

import pyarrow as pa
import pyarrow.dataset as pds
from pyarrow import compute as pc
from pyarrow.fs import FileSystem, LocalFileSystem

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource.parquet_datasource import _compute_row_hashes
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.readers.base_reader import Reader
from ray.data._internal.util import iterate_with_retry
from ray.data.context import DataContext
from ray.data.datasource.partitioning import Partitioning, PathPartitionParser
from ray.util.annotations import DeveloperAPI

# Synthetic column name produced when ``include_paths=True``. Shared with
# the V2 datasource and scanner layers so all references are spelled the
# same way.
INCLUDE_PATHS_COLUMN_NAME = "path"

# https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html#pyarrow.dataset.Scanner.from_batches
# Default is specified by PyArrow.
_ARROW_DEFAULT_BATCH_SIZE = 131_072

# Small fixed readahead keeps driver memory bounded when scanning
# uncompressed batches (jumbo tensor columns can run to multi-GB per
# batch, and pyarrow's default 16-batch readahead would retain all of
# them).
_ARROW_SCANNER_BATCH_READAHEAD = 1

ROW_HASH_COLUMN_NAME = "row_hash"


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
        partitioning: Optional[Partitioning] = None,
        ignore_prefixes: Optional[List[str]] = None,
        include_paths: bool = False,
        include_row_hash: bool = False,
        schema: Optional[pa.Schema] = None,
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
            partitioning: Ray ``Partitioning`` object. Partition columns are
                synthesized per-path via ``PathPartitionParser`` after each
                batch is read, producing string-typed columns (V1 parity).
            ignore_prefixes: Prefixes to ignore when reading files. Default is ['.', '_'] set by PyArrow.
            include_paths: If True, include the source file path in a
                ``'path'`` column for each row.
            include_row_hash: If True, include a deterministic uint64 hash
                per row in a ``'row_hash'`` column. The hash is derived from
                the source file path and the row's post-filter output
                position within the fragment, matching V1 semantics. If a
                ``'row_hash'`` column already exists in the file, it is
                overwritten.
            schema: Caller-supplied unified schema used both to override
                pyarrow's per-fragment inference (so a file whose column
                is all-null doesn't pin the type to ``null``) and to cast
                path-derived partition values to their target types when
                ``Partitioning(field_types=...)`` is set.

        """
        self._format = format
        self._columns = columns
        self._predicate = predicate
        self._batch_size = batch_size
        self._limit = limit
        self._filesystem = filesystem
        self._partition_parser: Optional[PathPartitionParser] = (
            PathPartitionParser(partitioning) if partitioning is not None else None
        )
        self._ignore_prefixes = ignore_prefixes
        self._include_paths = include_paths
        self._include_row_hash = include_row_hash
        self._schema = schema

    @cached_property
    def _file_dataset_schema(self) -> Optional[pa.Schema]:
        """Schema passed to ``pds.dataset`` — partition keys and ``path``
        stripped out since those are synthesized post-read.

        Pinning the caller-supplied schema at the pyarrow layer is how
        we cover the "first file has an all-null column, later files
        have the real type" case (e.g.
        ``test_read_null_data_in_first_file``): without the pin,
        pyarrow locks column X to ``null`` across the fragment group
        and the later string-typed file fails the cast.

        But pyarrow refuses extension-to-extension casts (e.g.
        ``ArrowTensorTypeV2(shape=X)`` → ``ArrowVariableShapedTensor``),
        and files with different per-file tensor shapes only unify
        through ``ArrowVariableShapedTensor``. When the caller schema
        contains *any* extension column we skip the pin entirely and
        let pyarrow infer per-file — downstream concat handles the
        heterogeneous blocks. Losing the all-null promotion in this
        narrow case is acceptable; the combination of an all-null
        first file *and* an extension column is uncommon, whereas
        reading multiple files with variable-shape tensors is a
        supported V1 feature.
        """
        if self._schema is None:
            return None
        if any(isinstance(f.type, pa.ExtensionType) for f in self._schema):
            return None
        partition_keys = (
            set(self._partition_parser._scheme.field_names or [])
            if self._partition_parser is not None
            else set()
        )
        synthesized = {INCLUDE_PATHS_COLUMN_NAME}
        if self._include_row_hash:
            # ``row_hash`` is synthesized post-read, and the schema's type
            # (``uint64``) may not match the on-disk column's type when a
            # file already carries a ``row_hash`` column. Strip it from the
            # dataset schema so pyarrow doesn't try to cast.
            synthesized.add(ROW_HASH_COLUMN_NAME)
        fields = [
            f
            for f in self._schema
            if f.name not in partition_keys and f.name not in synthesized
        ]
        return pa.schema(fields) if fields else None

    def _broadcast_partition_value(
        self, name: str, value: Any, num_rows: int
    ) -> pa.Array:
        """Broadcast a single path-derived partition value to ``num_rows``,
        casting to the caller-supplied schema's field type if set.

        Values are stringified first (``PathPartitionParser`` in
        ``explicit`` mode can return arrow-scalar-like non-strings) and
        then cast to the target type, so ``Partitioning(field_types=
        {"year": int})`` still promotes them correctly.
        """
        str_val = None if value is None else str(value)
        arr = pa.repeat(pa.scalar(str_val, type=pa.string()), num_rows)
        if self._schema is not None:
            idx = self._schema.get_field_index(name)
            if idx != -1 and self._schema.field(idx).type != pa.string():
                arr = arr.cast(self._schema.field(idx).type)
        return arr

    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        """Read data from the input bucket and yield Arrow tables.

        This method is called on workers to perform the actual read operation.
        It should respect all pushdowns configured on this reader.

        Args:
            input_split: Work unit describing what data to read.

        Yields:
            pa.Table: PyArrow Tables containing the read data.
        """
        if len(input_split) == 0:
            return

        paths = list(input_split.paths)
        filesystem = self._filesystem or LocalFileSystem()
        # Build a ``pds.Dataset`` over *all* manifest paths so pyarrow's
        # listing + column metadata is shared, but then iterate its
        # fragments one at a time. ``dataset.scanner(fragments=...)``
        # at the aggregate level would force a cross-fragment cast —
        # which breaks variable-shape tensor extensions where each
        # file has its own ``ArrowTensorTypeV2(shape=...)``. Per-
        # fragment scanners let pyarrow use the native per-file type,
        # and downstream concat handles unification.
        dataset = pds.dataset(
            source=paths,
            format=self._format.value,
            filesystem=filesystem,
            schema=self._file_dataset_schema,
            ignore_prefixes=self._ignore_prefixes,
        )

        # Split the requested columns into ones the on-disk file has
        # (pyarrow reads these) and ones we need to synthesize post-read
        # (hive partition keys, "path"). ``self._columns is None`` means
        # "no projection" — read every file column and synthesize every
        # available partition/path column.
        on_disk_column_names = set(dataset.schema.names)
        if self._columns is None:
            columns_to_read_from_file: Optional[List[str]] = None
            columns_to_synthesize: Optional[Set[str]] = None
        else:
            columns_to_read_from_file = [
                c for c in self._columns if c in on_disk_column_names
            ]
            columns_to_synthesize = set(self._columns) - on_disk_column_names

        scanner_kwargs = {
            "columns": columns_to_read_from_file,
            "filter": self._predicate,
            "batch_size": self._resolve_batch_size(dataset),
            "batch_readahead": _ARROW_SCANNER_BATCH_READAHEAD,
        }
        scanner_kwargs.update(self._arrow_scanner_kwargs())

        ctx = DataContext.get_current()
        rows_read = 0
        for table, fragment_path, fragment_row_offset in iterate_with_retry(
            lambda: self._read_fragment_batches(dataset, scanner_kwargs),
            "read batches",
            match=ctx.retried_io_errors,
        ):
            if self._limit is not None:
                if rows_read >= self._limit:
                    break
                if len(table) > self._limit - rows_read:
                    table = table.slice(0, self._limit - rows_read)

            # Build the list of (name, value) pairs to synthesize from
            # the fragment path: hive partitions + optional ``path``.
            derived_items: List[Tuple[str, Any]] = []
            if self._partition_parser is not None:
                derived_items.extend(self._partition_parser(fragment_path).items())
            if self._include_paths:
                derived_items.append((INCLUDE_PATHS_COLUMN_NAME, fragment_path))

            for name, value in derived_items:
                if (
                    columns_to_synthesize is not None
                    and name not in columns_to_synthesize
                ):
                    continue
                if name in table.column_names:
                    # When the caller schema names a partition key, pyarrow
                    # expects it in every file and fills it with nulls when
                    # absent (the hive-typical case). Drop that placeholder
                    # so the path-derived value below replaces it.
                    table = table.drop([name])
                table = table.append_column(
                    name,
                    self._broadcast_partition_value(name, value, table.num_rows),
                )

            # Skip when projection pushdown has narrowed ``columns`` to
            # exclude ``row_hash`` — the projection below would just drop it.
            if self._include_row_hash and (
                columns_to_synthesize is None
                or ROW_HASH_COLUMN_NAME in columns_to_synthesize
            ):
                hashes = _compute_row_hashes(
                    fragment_path, fragment_row_offset, table.num_rows
                )
                if ROW_HASH_COLUMN_NAME in table.column_names:
                    table = table.drop([ROW_HASH_COLUMN_NAME])
                table = table.append_column(
                    ROW_HASH_COLUMN_NAME, pa.array(hashes, type=pa.uint64())
                )

            if self._columns is not None:
                # Project/reorder to the caller's requested column order;
                # drop any that weren't produced (matches V1's lenient
                # behavior). Always select — an empty projection must
                # narrow the table to zero columns so the stub-column
                # guard below handles row preservation.
                produced = set(table.column_names)
                projected = [c for c in self._columns if c in produced]
                table = table.select(projected)

            if table.num_columns == 0 and table.num_rows > 0:
                # Guards against ``pa.concat_tables`` collapsing rows
                # when a batch has zero columns (e.g., empty projection
                # for a count query). The stub column is dropped by
                # downstream projections.
                table = table.append_column(
                    _BATCH_SIZE_PRESERVING_STUB_COL_NAME,
                    pa.nulls(table.num_rows),
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

    def _arrow_scanner_kwargs(self) -> dict:
        """Additional keyword arguments passed to ``pds.Dataset.scanner()``.

        Subclasses override this to inject format-specific options.
        """
        return {}

    def _read_fragment_batches(
        self,
        dataset: pds.Dataset,
        scanner_kwargs: dict,
    ) -> Iterator[Tuple[pa.Table, str, int]]:
        """Yield non-empty (table, fragment_path, fragment_row_offset) triples
        one fragment at a time.

        ``fragment_row_offset`` is the post-filter row position of the first
        row of ``table`` within the current fragment. Tracking it inside the
        generator means it resets correctly whenever ``iterate_with_retry``
        recreates the generator on a retry — outer-loop state would otherwise
        carry stale values from the failed attempt and corrupt row hashes.

        Each fragment gets its own scanner so pyarrow uses the native
        per-file schema. A cross-fragment scanner would force a unified
        schema cast, which refuses extension-to-extension conversion
        (e.g. variable-shape tensors). V1 ``ParquetDatasource`` follows
        the same per-fragment pattern via ``fragment.to_batches``.

        When a non-extension caller schema is available we pin it at the
        scanner so pyarrow null-fills any column the unified schema names
        but the fragment lacks (V1 parity). Falling back to the
        per-fragment ``physical_schema`` preserves the variable-shape
        tensor escape hatch already encoded in ``_file_dataset_schema``.
        """
        for fragment in dataset.get_fragments():
            fragment_schema = (
                self._file_dataset_schema
                if self._file_dataset_schema is not None
                else fragment.physical_schema
            )
            scanner = fragment.scanner(**scanner_kwargs, schema=fragment_schema)
            offset = 0
            for tagged in scanner.scan_batches():
                table = pa.Table.from_batches(batches=[tagged.record_batch])
                if table.num_rows > 0:
                    yield table, fragment.path, offset
                    offset += table.num_rows
