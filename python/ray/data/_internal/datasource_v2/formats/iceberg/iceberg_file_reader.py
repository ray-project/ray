"""``Reader`` that decodes an Iceberg read unit through PyIceberg.

A read unit is a ``FileManifest`` of whole data files. Each row's pickled
``FileScanTask`` goes to PyIceberg's ``ArrowScan`` unchanged, one file at a
time, so deletes, schema evolution and partition backfill stay PyIceberg's job:

    manifest row -> FileScanTask -> ArrowScan (one file) -> record batches
                 -> pickle check -> cast to the projected schema -> block

The reader yields a block per record batch and stops once the pushed limit is
reached. One file per call bounds memory but re-reads a delete file shared by
several data files once per file; ``IcebergConfig.read_file_tasks_sequentially
= False`` trades the other way, passing the whole unit in one call as V1 does.
"""

from __future__ import annotations

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


@DeveloperAPI
class IcebergFileReader(Reader[FileManifest]):
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

        for record_batch in self._record_batches(input_split):
            table = pa.Table.from_batches([record_batch])
            # Before the cast, which fails opaquely on a pickled column.
            raise_on_pickle_object_columns(table)
            table = table.cast(target_schema)
            if self._drop_all_columns:
                # Zero-column tables lose their length on concat, so use the
                # shared ``FileReader``'s null stub column.
                table = table.select([]).append_column(
                    _BATCH_SIZE_PRESERVING_STUB_COL_NAME,
                    pa.nulls(table.num_rows),
                )
            yield table

    def _record_batches(self, input_split: FileManifest) -> Iterator[pa.RecordBatch]:
        from ray.data.context import DataContext

        tasks = scan_tasks_from_manifest(manifest=input_split)
        if not DataContext.get_current().iceberg_config.read_file_tasks_sequentially:
            # One call reads a delete file shared by several files once, but
            # holds every file's batches. A list, since PyIceberg iterates twice.
            yield from self._scan(limit=self._limit).to_record_batches(list(tasks))
            return
        rows_read = 0
        for task in tasks:
            remaining = None if self._limit is None else self._limit - rows_read
            if remaining is not None and remaining <= 0:
                return
            for record_batch in self._scan(limit=remaining).to_record_batches([task]):
                rows_read += record_batch.num_rows
                yield record_batch
