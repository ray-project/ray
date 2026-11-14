"""``Reader`` that decodes an Iceberg read unit through PyIceberg."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Iterator, Optional

import pyarrow as pa

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.formats.iceberg.iceberg_manifest import (
    scan_tasks_from_manifest,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.reader import Reader
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.io import FileIO
    from pyiceberg.io.pyarrow import ArrowScan
    from pyiceberg.schema import Schema
    from pyiceberg.table.metadata import TableMetadata

logger = logging.getLogger(__name__)


@DeveloperAPI
class IcebergFileReader(Reader[FileManifest]):
    """Decodes a manifest's files with PyIceberg's ``ArrowScan``, one at a time.

    ``ArrowScan`` applies deletes, schema evolution and partition backfill. It
    reads Parquet and ORC data files; Avro ones raise ``ValueError``, as in V1.
    It holds every batch of the files it is given, so one file per call keeps
    memory to one file.
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
        # Zero columns were asked for; see ``IcebergScanner.create_reader``.
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

        # Files can disagree on ``string`` vs ``large_string``, so every batch is
        # cast to the declared schema, as V1 does.
        target_schema = schema_to_pyarrow(
            self._projected_schema, include_field_ids=False
        )

        rows_read = 0
        tasks = scan_tasks_from_manifest(
            manifest=input_split, table_metadata=self._table_metadata
        )
        for task in tasks:
            remaining = None if self._limit is None else self._limit - rows_read
            if remaining is not None and remaining <= 0:
                return
            for record_batch in self._scan(limit=remaining).to_record_batches([task]):
                rows_read += record_batch.num_rows
                table = pa.Table.from_batches([record_batch])
                # Before the cast, which fails opaquely on a pickled column.
                raise_on_pickle_object_columns(table)
                table = table.cast(target_schema)
                if self._drop_all_columns:
                    # Zero-column tables lose their length on concat, so use the
                    # Parquet V2 reader's null stub column.
                    table = table.select([]).append_column(
                        _BATCH_SIZE_PRESERVING_STUB_COL_NAME,
                        pa.nulls(table.num_rows),
                    )
                yield table
