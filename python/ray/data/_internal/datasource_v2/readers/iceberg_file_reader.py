"""``Reader`` that decodes an Iceberg read unit through PyIceberg."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Iterator, Optional

import pyarrow as pa

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.listing.iceberg_manifest import (
    scan_tasks_from_manifest,
)
from ray.data._internal.datasource_v2.readers.base_reader import Reader
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.io import FileIO
    from pyiceberg.schema import Schema
    from pyiceberg.table.metadata import TableMetadata

logger = logging.getLogger(__name__)


@DeveloperAPI
class IcebergFileReader(Reader[FileManifest]):
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

    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        from pyiceberg.io.pyarrow import ArrowScan, schema_to_pyarrow

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
            scan = ArrowScan(
                table_metadata=self._table_metadata,
                io=self._io,
                projected_schema=self._projected_schema,
                row_filter=self._row_filter,
                case_sensitive=self._case_sensitive,
                limit=remaining,
            )
            for batch in scan.to_record_batches([task]):
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
