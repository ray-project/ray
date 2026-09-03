"""``Reader`` that decodes an Iceberg read unit through PyIceberg."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Iterator, Optional, Set

import pyarrow as pa
from typing_extensions import override

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.listing.iceberg_manifest import (
    RECORD_COUNT_COLUMN_NAME,
    RESIDUAL_IS_TRUE_COLUMN_NAME,
    scan_tasks_from_manifest,
)
from ray.data._internal.datasource_v2.readers.base_reader import Reader
from ray.data._internal.datasource_v2.readers.supports_metadata import (
    MetadataType,
    SupportsMetadata,
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

logger = logging.getLogger(__name__)


@DeveloperAPI
class IcebergFileReader(Reader[FileManifest], SupportsMetadata):
    """Decodes a manifest's files with PyIceberg's own reader, one file at a time.

    Going through ``ArrowScan`` rather than reading the Parquet directly is
    what makes positional deletes, field-ID schema resolution, type promotion
    and partition-value backfill correct without reimplementing any of them.

    The per-file loop is the memory bound: ``ArrowScan`` materializes every
    batch of every file it is handed before returning, so handing it the whole
    read unit at once would hold the whole unit. One file per call holds one
    file, whatever the unit's size.
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
    ):
        self._table_metadata = table_metadata
        self._io = io
        self._projected_schema = projected_schema
        self._row_filter = row_filter
        self._case_sensitive = case_sensitive
        self._limit = limit
        # Set when the scanner asked for zero columns: ``projected_schema``
        # above is then a one-column stand-in, decoded only so the batches
        # carry a row count, and replaced here by the stub column.
        self._drop_all_columns = drop_all_columns

    def _scan(self, limit: Optional[int]) -> "ArrowScan":
        from pyiceberg.io.pyarrow import ArrowScan

        return ArrowScan(
            table_metadata=self._table_metadata,
            io=self._io,
            projected_schema=self._projected_schema,
            row_filter=self._row_filter,
            case_sensitive=self._case_sensitive,
            limit=limit,
        )

    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        # ``to_record_batches`` types each batch from the file it came out of,
        # so two files backing the same table can disagree on ``string`` vs
        # ``large_string`` (PyIceberg says as much in ``ArrowScan.to_table``).
        # Casting to the projected schema is what makes every block match the
        # schema ``infer_schema`` declared, and it is what the V1 datasource
        # does for the same reason.
        target_schema = schema_to_pyarrow(
            self._projected_schema, include_field_ids=False
        )

        rows_read = 0
        for task in scan_tasks_from_manifest(input_split, self._table_metadata):
            remaining = None if self._limit is None else self._limit - rows_read
            if remaining is not None and remaining <= 0:
                return
            for batch in self._scan(remaining).to_record_batches([task]):
                rows_read += batch.num_rows
                table = pa.Table.from_batches([batch.cast(target_schema)])
                if self._drop_all_columns:
                    # Same convention as the Parquet V2 reader: a zero-column
                    # table carries its length, but ``pa.concat_tables``
                    # collapses it to zero rows, so hand downstream an
                    # all-null stub column instead. It is filtered out of the
                    # user-visible schema.
                    table = table.select([]).append_column(
                        _BATCH_SIZE_PRESERVING_STUB_COL_NAME,
                        pa.nulls(table.num_rows),
                    )
                yield table

    @override
    def read_metadata(self, file_manifest: FileManifest) -> Iterator[BlockMetadata]:
        """Yield one ``BlockMetadata`` per file, with ``num_rows`` from Iceberg.

        A file's ``record_count`` is written into Iceberg's own manifests, so
        for most files this touches no data at all -- not even a Parquet
        footer, unlike the equivalent path for a plain Parquet read.

        It is an *upper* bound on two kinds of file, and each is handled rather
        than assumed away. A file with delete files has rows that are still
        counted but must not be returned, and a file whose per-file residual is
        not ``AlwaysTrue`` has rows the filter rejects. Either way the count
        comes from decoding the file, which is what PyIceberg's own ``count()``
        does.

        The residual is what makes a filtered ``count()`` worth attempting at
        all: a filter on a partition column leaves every listed file with an
        ``AlwaysTrue`` residual, so the whole count stays free. A filter on a
        data column leaves none of them, and every file is decoded -- the same
        work the ordinary read path would have done.
        """
        block = file_manifest.as_block()
        record_counts = block[RECORD_COUNT_COLUMN_NAME].to_pylist()
        residual_is_true = block[RESIDUAL_IS_TRUE_COLUMN_NAME].to_pylist()

        tasks = scan_tasks_from_manifest(file_manifest, self._table_metadata)
        for index, task in enumerate(tasks):
            if residual_is_true[index] and not task.delete_files:
                num_rows = record_counts[index]
            else:
                num_rows = self._count_by_reading(task)
            yield BlockMetadata(
                num_rows=num_rows,
                size_bytes=None,
                exec_stats=None,
                input_files=None,
            )

    def _count_by_reading(self, task: "FileScanTask") -> int:
        """Rows ``task`` actually yields, by decoding it.

        ``self._projected_schema`` is whatever the scanner asked for, which on
        the count path is a single cheap column (see
        ``IcebergScanner.create_reader``). The row filter may name columns that
        projection does not; PyIceberg reads the union, so the filter still
        binds. ``limit`` is deliberately not passed: a partial read would
        undercount, and ``available_metadata`` already declines when a limit is
        set.
        """
        return sum(
            batch.num_rows for batch in self._scan(None).to_record_batches([task])
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
