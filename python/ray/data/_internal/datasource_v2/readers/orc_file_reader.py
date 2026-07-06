from typing import Any, Iterator, List, Optional, Sequence, Set, Tuple

import pyarrow as pa
import pyarrow.orc as orc
from pyarrow.fs import FileSystem, LocalFileSystem

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    OrcFileChunkMetadata,
)
from ray.data._internal.datasource_v2.chunkers.orc_file_chunking_utils import (
    stripe_range_from_chunk_metadata,
)
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.readers.base_reader import Reader
from ray.data._internal.datasource_v2.readers.file_reader import (
    _ARROW_DEFAULT_BATCH_SIZE,
    INCLUDE_PATHS_COLUMN_NAME,
)
from ray.data.datasource.partitioning import Partitioning, PathPartitionParser
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class OrcFileReader(Reader[FileManifest]):
    """ORC reader that maps manifest chunks onto ORC stripes."""

    def __init__(
        self,
        batch_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        predicate: Optional[Expr] = None,
        limit: Optional[int] = None,
        filesystem: Optional[FileSystem] = None,
        partitioning: Optional[Partitioning] = None,
        include_paths: bool = False,
        schema: Optional[pa.Schema] = None,
    ):
        self._batch_size = batch_size or _ARROW_DEFAULT_BATCH_SIZE
        self._columns = columns
        self._predicate = predicate
        self._limit = limit
        self._filesystem = filesystem
        self._partition_parser: Optional[PathPartitionParser] = (
            PathPartitionParser(partitioning) if partitioning is not None else None
        )
        self._include_paths = include_paths
        self._schema = schema

    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        if len(input_split) == 0:
            return

        rows_read = 0
        for path, chunk_metadatas in self._iter_contiguous_path_groups(input_split):
            if self._limit is not None and rows_read >= self._limit:
                return
            for table in self._read_path(path, chunk_metadatas):
                if self._limit is not None and table.num_rows > self._limit - rows_read:
                    table = table.slice(0, self._limit - rows_read)

                for batch in self._slice_table(table):
                    rows_read += batch.num_rows
                    yield batch

    def _iter_contiguous_path_groups(
        self, manifest: FileManifest
    ) -> Iterator[Tuple[str, List[Optional[OrcFileChunkMetadata]]]]:
        current_path: Optional[str] = None
        current_metadatas: List[Optional[OrcFileChunkMetadata]] = []
        for path, chunk_metadata in zip(manifest.paths, manifest.file_chunk_metadatas):
            if current_path is None:
                current_path = path
            if path != current_path:
                yield current_path, current_metadatas
                current_path = path
                current_metadatas = []
            current_metadatas.append(chunk_metadata)

        if current_path is not None:
            yield current_path, current_metadatas

    def _read_path(
        self,
        path: str,
        chunk_metadatas: Sequence[Optional[OrcFileChunkMetadata]],
    ) -> Iterator[pa.Table]:
        filesystem = self._filesystem or LocalFileSystem()
        with filesystem.open_input_file(path) as handle:
            orc_file = orc.ORCFile(handle)
            physical_schema = orc_file.schema
            physical_names = physical_schema.names
            if not physical_names:
                return

            for stripe_idx in self._iter_stripe_indices(orc_file, chunk_metadatas):
                table = self._read_stripe(path, orc_file, stripe_idx, physical_names)
                if table.num_rows > 0:
                    yield table

    def _iter_stripe_indices(
        self,
        orc_file: orc.ORCFile,
        chunk_metadatas: Sequence[Optional[OrcFileChunkMetadata]],
    ) -> Iterator[int]:
        for chunk_metadata in chunk_metadatas:
            if chunk_metadata is None:
                stripe_range = (0, orc_file.nstripes)
            else:
                stripe_range = stripe_range_from_chunk_metadata(
                    chunk_metadata, orc_file.nstripes
                )
            if stripe_range is None:
                continue
            start, end = stripe_range
            yield from range(start, end)

    def _read_stripe(
        self,
        path: str,
        orc_file: orc.ORCFile,
        stripe_idx: int,
        physical_names: List[str],
    ) -> pa.Table:
        output_file_columns = self._output_file_columns(physical_names)
        filter_columns = self._filter_columns()
        read_columns = self._columns_to_read(
            physical_names,
            output_file_columns,
            filter_columns,
        )
        stripe = orc_file.read_stripe(stripe_idx, columns=read_columns)
        table = (
            pa.Table.from_batches([stripe])
            if isinstance(stripe, pa.RecordBatch)
            else stripe
        )

        table = self._append_missing_columns(
            table,
            [c for c in output_file_columns if c not in table.column_names],
        )
        table = self._append_missing_columns(
            table,
            [c for c in filter_columns if c not in table.column_names],
        )

        if self._predicate is not None:
            table = table.filter(self._predicate.to_pyarrow())

        if output_file_columns:
            produced = set(table.column_names)
            table = table.select([c for c in output_file_columns if c in produced])
            table = self._cast_to_schema(table)
        else:
            table = table.select([])

        table = self._append_synthetic_columns(path, stripe_idx, table)

        if self._columns is not None:
            produced = set(table.column_names)
            table = table.select([c for c in self._columns if c in produced])

        if table.num_columns == 0 and table.num_rows > 0:
            table = table.append_column(
                _BATCH_SIZE_PRESERVING_STUB_COL_NAME,
                pa.nulls(table.num_rows),
            )

        return table

    def _output_file_columns(self, physical_names: List[str]) -> List[str]:
        if self._columns is not None:
            return [
                c
                for c in self._columns
                if c not in self._synthetic_column_names()
                and c not in self._partition_column_names()
            ]

        if self._schema is None:
            return list(physical_names)

        excluded = self._synthetic_column_names() | self._partition_column_names()
        return [field.name for field in self._schema if field.name not in excluded]

    def _filter_columns(self) -> Set[str]:
        if self._predicate is None:
            return set()

        from ray.data._internal.planner.plan_expression.expression_visitors import (
            get_column_references,
        )

        return set(get_column_references(self._predicate))

    def _columns_to_read(
        self,
        physical_names: List[str],
        output_file_columns: List[str],
        filter_columns: Set[str],
    ) -> Optional[List[str]]:
        physical = set(physical_names)
        if self._columns is None and self._schema is None and not filter_columns:
            return None

        columns = [c for c in output_file_columns if c in physical]
        for column in filter_columns:
            if column in physical and column not in columns:
                columns.append(column)

        if not columns:
            # ORC returns zero rows for ``columns=[]``. Read one physical
            # column as a row-count carrier, then drop it after filtering.
            columns.append(physical_names[0])
        return columns

    def _append_missing_columns(
        self, table: pa.Table, column_names: List[str]
    ) -> pa.Table:
        for column_name in column_names:
            if column_name in table.column_names:
                continue
            table = table.append_column(
                column_name,
                pa.nulls(table.num_rows, type=self._schema_type(column_name)),
            )
        return table

    def _append_synthetic_columns(
        self, path: str, stripe_idx: int, table: pa.Table
    ) -> pa.Table:
        derived_items: List[Tuple[str, Any]] = []
        if self._partition_parser is not None:
            derived_items.extend(self._partition_parser(path).items())
        if self._include_paths:
            derived_items.append((INCLUDE_PATHS_COLUMN_NAME, path))

        columns_to_synthesize = (
            None
            if self._columns is None
            else set(self._columns) - set(table.column_names)
        )
        for name, value in derived_items:
            if columns_to_synthesize is not None and name not in columns_to_synthesize:
                continue
            if name in table.column_names:
                table = table.drop([name])
            table = table.append_column(
                name,
                self._broadcast_partition_value(name, value, table.num_rows),
            )

        return table

    def _broadcast_partition_value(
        self, name: str, value: Any, num_rows: int
    ) -> pa.Array:
        str_val = None if value is None else str(value)
        arr = pa.repeat(pa.scalar(str_val, type=pa.string()), num_rows)
        target_type = self._schema_type(name)
        if target_type != pa.null() and target_type != pa.string():
            arr = arr.cast(target_type)
        return arr

    def _cast_to_schema(self, table: pa.Table) -> pa.Table:
        if self._schema is None or table.num_columns == 0:
            return table

        fields = []
        changed = False
        for field in table.schema:
            target_idx = self._schema.get_field_index(field.name)
            if target_idx == -1:
                fields.append(field)
                continue
            target = self._schema.field(target_idx)
            fields.append(target)
            changed = changed or target.type != field.type
        if not changed:
            return table
        return table.cast(pa.schema(fields))

    def _schema_type(self, column_name: str) -> pa.DataType:
        if self._schema is None:
            return pa.null()
        idx = self._schema.get_field_index(column_name)
        if idx == -1:
            return pa.null()
        return self._schema.field(idx).type

    def _synthetic_column_names(self) -> Set[str]:
        return {INCLUDE_PATHS_COLUMN_NAME}

    def _partition_column_names(self) -> Set[str]:
        if self._partition_parser is None:
            return set()
        return set(self._partition_parser._scheme.field_names or [])

    def _slice_table(self, table: pa.Table) -> Iterator[pa.Table]:
        if self._batch_size <= 0 or table.num_rows <= self._batch_size:
            yield table
            return

        offset = 0
        while offset < table.num_rows:
            yield table.slice(offset, self._batch_size)
            offset += self._batch_size
