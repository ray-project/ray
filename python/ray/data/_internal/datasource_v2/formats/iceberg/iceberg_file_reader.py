"""``Reader`` that decodes an Iceberg read unit through PyIceberg.

A read unit is a ``FileManifest`` of whole data files. Each row's pickled
``FileScanTask`` goes to PyIceberg's ``ArrowScan`` unchanged, one file at a
time, so deletes, schema evolution and partition backfill stay PyIceberg's job:

    manifest row -> FileScanTask -> ArrowScan (one file) -> record batches
                 -> pickle check -> cast to the projected schema -> block

The reader yields a block per record batch and stops once the pushed limit is
reached. Filter parts PyArrow's statistics could get wrong (see
``iceberg_filter_pushdown``) are applied after ``ArrowScan``, with Ray's own
evaluator, and the columns read only for them are then dropped.

One file per call bounds memory; a delete file several of the unit's data
files share is fetched once and kept for the unit
(``iceberg_delete_file_cache``). ``IcebergConfig.read_file_tasks_sequentially
= False`` passes the whole unit in one call instead, as V1 does.

Row-group runs the listing sent through ``iceberg_parquet_route`` are read
with Ray's Parquet reader, in the same pipeline; their position deletes come
through the same delete-file cache.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Iterator, List, Optional, Set, Tuple

import pyarrow as pa
from typing_extensions import override

from ray.data._internal.arrow_block import (
    _BATCH_SIZE_PRESERVING_STUB_COL_NAME,
    ArrowBlockAccessor,
)
from ray.data._internal.datasource_v2.formats.iceberg.iceberg_manifest import (
    scan_tasks_by_row,
    scan_tasks_from_manifest,
)
from ray.data._internal.datasource_v2.formats.iceberg.iceberg_parquet_route import (
    is_routed_row,
    read_routed_rows,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)
from ray.data._internal.datasource_v2.interfaces.reader import Reader
from ray.data._internal.datasource_v2.interfaces.supports_metadata import (
    MetadataType,
    SupportsMetadata,
)
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
from ray.data.block import BlockMetadata
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.io import FileIO
    from pyiceberg.io.pyarrow import ArrowScan
    from pyiceberg.schema import Schema
    from pyiceberg.table import FileScanTask
    from pyiceberg.table.metadata import TableMetadata

    from ray.data.expressions import Expr


@DeveloperAPI
class IcebergFileReader(Reader[FileManifest], SupportsMetadata):
    """Decodes a manifest's files with PyIceberg's ``ArrowScan``, one at a time.

    ``ArrowScan`` reads Parquet and ORC data files; Avro ones raise
    ``ValueError``, as in V1. It holds every batch of the files it is given, so
    one file per call keeps memory to one file. With
    ``DataContext.iceberg_config.read_file_tasks_sequentially = False`` the
    whole unit goes in one call, as in V1.
    """

    def __init__(
        self,
        *,
        table_metadata: "TableMetadata",
        io: "FileIO",
        projected_schema: "Schema",
        row_filter: "BooleanExpression",
        case_sensitive: bool = True,
        limit: Optional[int] = None,
        drop_all_columns: bool = False,
        after_read_filter: Optional["Expr"] = None,
        output_columns: Optional[List[str]] = None,
        scan_predicate: Optional["Expr"] = None,
    ):
        self._table_metadata = table_metadata
        self._io = io
        self._projected_schema = projected_schema
        self._row_filter = row_filter
        self._case_sensitive = case_sensitive
        self._limit = limit
        # Zero columns were asked for; see ``IcebergScanner.create_reader``.
        self._drop_all_columns = drop_all_columns
        # Applied by Ray after ``ArrowScan``; see ``iceberg_filter_pushdown``.
        self._after_read_filter = after_read_filter
        # The columns to keep after that filter, ``None`` for all of them.
        self._output_columns = output_columns
        # The pushed parts ``row_filter`` holds in Iceberg form, which Ray's
        # Parquet reader filters routed rows with.
        self._scan_predicate = scan_predicate

    def _scan(self, limit: Optional[int], io: Optional["FileIO"] = None) -> "ArrowScan":
        from pyiceberg.io.pyarrow import ArrowScan

        return ArrowScan(
            table_metadata=self._table_metadata,
            io=self._io if io is None else io,
            projected_schema=self._projected_schema,
            row_filter=self._row_filter,
            case_sensitive=self._case_sensitive,
            limit=limit,
        )

    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        # Files can disagree on ``string`` vs ``large_string``, so every batch is
        # cast to the declared schema, as V1 does.
        target_schema = schema_to_pyarrow(
            self._projected_schema, include_field_ids=False
        )

        rows_yielded = 0
        for record_batch in self._record_batches(input_split):
            if self._limit is not None and rows_yielded >= self._limit:
                return
            table = pa.Table.from_batches([record_batch])
            # Before the cast, which fails opaquely on a pickled column.
            raise_on_pickle_object_columns(table)
            table = table.cast(target_schema)
            table = self._apply_after_read_filter(table)
            if self._limit is not None:
                table = table.slice(0, self._limit - rows_yielded)
            rows_yielded += table.num_rows
            if self._drop_all_columns:
                # Zero-column tables lose their length on concat, so use the
                # shared ``FileReader``'s null stub column.
                table = table.select([]).append_column(
                    _BATCH_SIZE_PRESERVING_STUB_COL_NAME,
                    pa.nulls(table.num_rows),
                )
            yield table

    def _apply_after_read_filter(self, table: pa.Table) -> pa.Table:
        if self._after_read_filter is None:
            return table
        table = ArrowBlockAccessor(table).filter(self._after_read_filter)
        if self._output_columns is not None:
            table = table.select(self._output_columns)
        return table

    def _record_batches(self, input_split: FileManifest) -> Iterator[pa.RecordBatch]:
        # Neither reader counts the after-read filter, so neither gets a limit
        # then; ``read`` stops instead.
        limit = self._limit if self._after_read_filter is None else None
        delete_io = self._delete_io(input_split)
        rows_read = 0
        for rows, routed in _split_by_route(input_split):
            remaining = None if limit is None else limit - rows_read
            if remaining is not None and remaining <= 0:
                return
            if routed:
                record_batches = self._read_routed(
                    rows, limit=remaining, delete_io=delete_io
                )
            else:
                record_batches = self._arrow_scan(rows, limit=remaining, io=delete_io)
            for record_batch in record_batches:
                rows_read += record_batch.num_rows
                yield record_batch

    def _delete_io(self, manifest: FileManifest) -> "FileIO":
        """The ``FileIO`` that fetches a delete file the unit's files share once."""
        from ray.data._internal.datasource_v2.formats.iceberg.iceberg_delete_file_cache import (  # noqa: E501
            with_shared_delete_files_cached,
        )

        tasks = [task for task in scan_tasks_by_row(manifest) if task is not None]
        return with_shared_delete_files_cached(self._io, tasks)

    def _read_routed(
        self,
        rows: FileManifest,
        *,
        limit: Optional[int],
        delete_io: Optional["FileIO"] = None,
    ) -> Iterator[pa.RecordBatch]:
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        # The listing routes only reads of at least one column.
        assert not self._drop_all_columns
        columns = [field.name for field in self._projected_schema.columns]
        names = set(columns)
        if self._scan_predicate is not None:
            names.update(get_column_references(self._scan_predicate))
        schema = self._table_metadata.schema().select(*sorted(names))
        return read_routed_rows(
            rows,
            io=self._io,
            delete_io=self._io if delete_io is None else delete_io,
            table_location=self._table_metadata.location,
            columns=columns,
            schema=schema_to_pyarrow(schema, include_field_ids=False),
            predicate=self._scan_predicate,
            limit=limit,
        )

    def _arrow_scan(
        self, rows: FileManifest, *, limit: Optional[int], io: "FileIO"
    ) -> Iterator[pa.RecordBatch]:
        from ray.data.context import DataContext

        tasks = list(scan_tasks_from_manifest(manifest=rows))
        if not DataContext.get_current().iceberg_config.read_file_tasks_sequentially:
            # One call reads a delete file shared by several files once, but
            # holds every file's batches. A list, since PyIceberg iterates twice.
            yield from self._scan(limit=limit).to_record_batches(tasks)
            return
        rows_read = 0
        for task in tasks:
            remaining = None if limit is None else limit - rows_read
            if remaining is not None and remaining <= 0:
                return
            scan = self._scan(limit=remaining, io=io)
            for record_batch in scan.to_record_batches([task]):
                rows_read += record_batch.num_rows
                yield record_batch

    @override
    def read_metadata(self, file_manifest: FileManifest) -> Iterator[BlockMetadata]:
        """Yield one ``BlockMetadata`` per file, with ``num_rows`` from Iceberg.

        A file's ``record_count`` is written into Iceberg's own manifests, so
        for most files this touches no data at all -- not even a Parquet
        footer, unlike the equivalent path for a plain Parquet read.

        ``record_count`` is exact only for a file the listing marked
        ``fully_matched``: no delete files, and every row passes the filter
        (``iceberg_row_match``). Any other file is counted by decoding it,
        which is what PyIceberg's own ``count()`` does.

        A row-group run the listing routed to Ray's Parquet reader carries its
        row count in its chunk, exact under the same ``fully_matched`` rule,
        and is otherwise counted by reading it with that reader.

        Args:
            file_manifest: The read unit's manifest, as the listing wrote it.

        Yields:
            BlockMetadata: One per row in ``file_manifest``, in order.
        """
        for rows, routed in _split_by_route(file_manifest):
            chunks = [FileChunk.from_metadata(c) for c in rows.file_chunk_metadatas]
            if routed:
                counts = (
                    chunk.num_rows
                    if chunk.fully_matched
                    else self._count_rows(self._read_routed(row, limit=None))
                    for chunk, row in zip(chunks, _rows(rows))
                )
            else:
                counts = (
                    task.file.record_count
                    if chunk.fully_matched
                    else self._count_by_reading(task)
                    for chunk, task in zip(chunks, scan_tasks_from_manifest(rows))
                )
            for num_rows in counts:
                yield BlockMetadata(
                    num_rows=num_rows,
                    size_bytes=None,
                    exec_stats=None,
                    input_files=None,
                )

    def _count_by_reading(self, task: "FileScanTask") -> int:
        """Rows ``task`` actually yields, by decoding it.

        ``self._projected_schema`` is whatever the scanner asked for, which on
        the count path is a stub field no file has (see
        ``IcebergScanner.create_reader``), so the only columns decoded are the
        ones the row filter names. PyIceberg reads those alongside the
        projection, so the filter still binds. ``limit`` is deliberately not
        passed: a partial read would undercount, and ``available_metadata``
        already declines when a limit is set.

        No ``raise_on_pickle_object_columns`` here, unlike ``read``: the batches
        are counted and dropped, never handed to anyone and never unpickled, so
        there is nothing to protect. A caller who then reads the data gets the
        check from ``read``.

        Args:
            task: One Iceberg scan task, naming a single data file.

        Returns:
            The number of rows the task yields after filters and deletes.
        """
        return self._count_rows(self._scan(None).to_record_batches([task]))

    def _count_rows(self, record_batches: Iterator[pa.RecordBatch]) -> int:
        return sum(
            self._apply_after_read_filter(pa.Table.from_batches([batch])).num_rows
            for batch in record_batches
        )

    @override
    def available_metadata(self) -> Set[MetadataType]:
        # A limit would make the answer a partial count, and unlike a row
        # filter there is nothing in the metadata that could account for it.
        # Only ``NUM_ROWS``: Iceberg records a file's *compressed* size, which
        # is not what a byte-metadata consumer means.
        if self._limit is not None:
            return set()
        return {MetadataType.NUM_ROWS}

    @override
    def get_target_metadata_batch_size(self) -> Optional[int]:
        # ``None`` means one task per manifest, and a manifest is already one
        # bin-packed read unit. The Parquet reader batches to bound how many
        # footers one task fetches; here the counts are in the manifest, so
        # there is no per-file IO to spread out.
        return None


def _split_by_route(manifest: FileManifest) -> Iterator[Tuple[FileManifest, bool]]:
    """Consecutive rows that share a reader, and whether they were routed."""
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    routed = [is_routed_row(chunk) for chunk in manifest.file_chunk_metadatas]
    start = 0
    for end in range(1, len(routed) + 1):
        if end == len(routed) or routed[end] != routed[start]:
            yield FileManifest(block.slice(start, end - start)), routed[start]
            start = end


def _rows(manifest: FileManifest) -> Iterator[FileManifest]:
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    for index in range(block.num_rows):
        yield FileManifest(block.slice(index, 1))
