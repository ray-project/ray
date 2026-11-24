from dataclasses import dataclass
from functools import partial
from typing import ClassVar, Dict, Iterator, List, Optional, Tuple, Type

import numpy as np
import pyarrow as pa
import pyarrow.dataset as pds
from typing_extensions import override

from ray.data._internal.datasource_v2.formats.delta.delta_column_mapping import (
    DeltaColumnMapping,
)
from ray.data._internal.datasource_v2.formats.delta.delta_file_indexer import (
    DELETED_ROWS_COLUMN,
    NUM_RECORDS_COLUMN,
    PARTITION_VALUES_COLUMN,
    DeltaPartitionPruner,
    PartitionValues,
)
from ray.data._internal.datasource_v2.formats.delta.delta_filesystem import (
    add_known_file_sizes,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
    ParquetScanner,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
from ray.data._internal.datasource_v2.interfaces.read_units import ReadUnitFragment
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    ReadUnitPosition,
)
from ray.data._internal.util import iterate_with_retry
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI


def _deleted_rows_by_path(manifest: FileManifest) -> Dict[str, np.ndarray]:
    """The deleted row positions the listing put in ``manifest``, by file."""
    extra_columns = manifest.extra_columns
    if extra_columns is None or DELETED_ROWS_COLUMN not in extra_columns.column_names:
        return {}
    return {
        path: np.asarray(deleted, dtype=np.int64)
        for path, deleted in zip(
            manifest.paths.tolist(), extra_columns[DELETED_ROWS_COLUMN].to_pylist()
        )
        if deleted
    }


def _file_row_positions(fragment: pds.Fragment) -> np.ndarray:
    """Each row's position in its file, in the order a scan of ``fragment``
    without a filter yields them.

    ``fragment`` is a whole Parquet file or a subset of its row groups.
    """
    metadata = fragment.metadata
    starts = np.zeros(metadata.num_row_groups + 1, dtype=np.int64)
    for i in range(metadata.num_row_groups):
        starts[i + 1] = starts[i] + metadata.row_group(i).num_rows
    row_groups = fragment.row_groups
    ids = (
        range(metadata.num_row_groups)
        if row_groups is None
        else [row_group.id for row_group in row_groups]
    )
    if not ids:
        return np.zeros(0, dtype=np.int64)
    return np.concatenate([np.arange(starts[i], starts[i + 1]) for i in ids])


def _predicate_mask(table: pa.Table, predicate: Expr) -> np.ndarray:
    """Which rows of ``table`` match ``predicate`` (a null result doesn't)."""
    row_index = "__ray_delta_row_index"
    indexed = table.append_column(row_index, pa.array(np.arange(table.num_rows)))
    matching = indexed.filter(predicate.to_pyarrow())[row_index].to_numpy()
    mask = np.zeros(table.num_rows, dtype=bool)
    mask[matching] = True
    return mask


class DeltaFileReader(ParquetFileReader):
    """Parquet reader that takes partition values and deletion vectors from the
    listing.

    A Delta file's path needn't hold its partition values, so the listing puts
    the ones the log records in each manifest (``PARTITION_VALUES_COLUMN``).
    The reader casts them to the columns' types as it does a value parsed from
    a path. A file with a deletion vector also carries the positions of its
    deleted rows (``DELETED_ROWS_COLUMN``), which the reader drops. It also
    hands the files' sizes from the log to the table's filesystem, which then
    opens them without asking the storage for them.
    """

    # Replaced, never changed in place, by each ``read``.
    _partition_values_by_path: Dict[str, PartitionValues] = {}
    _deleted_rows_by_path: Dict[str, np.ndarray] = {}
    # Set by ``DeltaScanner.create_reader``.
    _column_mapping: Optional[DeltaColumnMapping] = None

    def set_column_mapping(self, column_mapping: Optional[DeltaColumnMapping]) -> None:
        """Read a column-mapped table's data files by the names they use."""
        self._column_mapping = column_mapping

    @override
    def _iter_fragment_tables_without_pickle_check(
        self,
        fragment: pds.Fragment,
        scanner_kwargs: dict,
        file_dataset_schema: pa.Schema,
        predicate: Optional[Expr],
    ) -> Iterator[pa.Table]:
        """Scan the file by the column names it uses, then rename to logical ones.

        Everything above this works in the table's logical column names. The
        file is scanned with its own names: the physical names in the ``name``
        mode, and in the ``id`` mode the names of the file's columns whose
        Parquet field ids match the log's column ids. That covers the projected
        columns, the pinned schema (nested fields included) and the filter, so
        Parquet statistics still prune row groups. Columns the file holds but the
        table doesn't, such as materialized row-tracking columns, are never
        read. A filter on a column with nested fields may name those fields
        by their logical names, so it is applied after renaming instead.
        """
        mapping = self._column_mapping
        if mapping is None:
            yield from super()._iter_fragment_tables_without_pickle_check(
                fragment, scanner_kwargs, file_dataset_schema, predicate
            )
            return
        from ray.data._internal.planner.plan_expression.expression_visitors import (
            get_column_references,
        )

        # In the ``id`` mode, columns are matched by the Parquet field ids in
        # the file's own schema, which the scan reads anyway.
        file_schema = fragment.physical_schema if mapping.mode == "id" else None
        kwargs = dict(scanner_kwargs)
        columns: Optional[List[str]] = kwargs.get("columns")
        after_rename: Optional[Expr] = None
        physical_predicate: Optional[Expr] = None
        if predicate is not None:
            references = list(get_column_references(predicate))
            if any(mapping.is_nested(name) for name in references):
                after_rename = predicate
                if columns is not None:
                    columns = columns + [c for c in references if c not in columns]
            else:
                physical_predicate = mapping.physical_predicate(predicate, file_schema)
        kwargs["filter"] = (
            physical_predicate.to_pyarrow() if physical_predicate is not None else None
        )
        if columns is not None:
            kwargs["columns"] = mapping.physical_names(columns, file_schema)
        for table in super()._iter_fragment_tables_without_pickle_check(
            fragment,
            kwargs,
            mapping.physical_schema(file_dataset_schema, file_schema),
            physical_predicate,
        ):
            table = mapping.to_logical(table, file_schema)
            if after_rename is not None:
                table = table.filter(after_rename.to_pyarrow())
                if scanner_kwargs.get("columns") is not None:
                    table = table.select(scanner_kwargs["columns"])
            yield table

    @override
    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        # A read task reads its manifests one after another.
        self._add_known_file_sizes(input_split)
        extra_columns = input_split.extra_columns
        self._partition_values_by_path = (
            dict(
                zip(
                    input_split.paths.tolist(),
                    extra_columns[PARTITION_VALUES_COLUMN].to_pylist(),
                )
            )
            if extra_columns is not None
            and PARTITION_VALUES_COLUMN in extra_columns.column_names
            else {}
        )
        self._deleted_rows_by_path = _deleted_rows_by_path(input_split)
        yield from super().read(input_split)

    @override
    def _read_fragments_sequential(
        self,
        fragments_with_offsets: Iterator[ReadUnitFragment],
        scanner_kwargs: dict,
    ) -> Iterator[Tuple[pa.Table, ReadUnitPosition]]:
        """Read each unit, by row position where a deletion vector or a column
        needs it.

        A unit of a file with a deletion vector, and every unit when a
        synthesized column depends on the row's position (the generated ID
        column), is read by ``_iter_positioned_unit``. Other units are read as
        Parquet units are.
        """
        ctx = DataContext.get_current()
        for fragment_to_read in fragments_with_offsets:
            if (
                fragment_to_read.unit.source not in self._deleted_rows_by_path
                and not self._per_row_group_offsets
            ):
                yield from super()._read_fragments_sequential(
                    iter([fragment_to_read]), scanner_kwargs
                )
                continue
            yield from iterate_with_retry(
                partial(
                    self._iter_positioned_unit,
                    fragment_to_read=fragment_to_read,
                    scanner_kwargs=scanner_kwargs,
                ),
                f"read fragment {fragment_to_read.fragment.path}",
                match=ctx.retried_io_errors,
            )

    def _iter_positioned_unit(
        self, fragment_to_read: ReadUnitFragment, scanner_kwargs: dict
    ) -> Iterator[Tuple[pa.Table, ReadUnitPosition]]:
        """Read a unit's rows knowing each one's position in its file.

        A deletion vector names deleted rows by their position in the file, and
        the generated ID column needs the position too: one counted after a
        filter, or after a deletion vector a later commit added, would change
        between runs. So the unit is scanned without the row filter (row groups
        the statistics rule out are already left out by the listing), then the
        deleted rows are dropped, then the filter is applied.

        Each table yielded is a run of consecutive rows in the file, whose
        ``ReadUnitPosition.rows_before`` is the first row's position in the
        unit, when a synthesized column depends on row positions. Otherwise
        ``rows_before`` counts the rows yielded, as for Parquet.
        """
        from ray.data._internal.planner.plan_expression.expression_visitors import (
            get_column_references,
        )

        fragment = fragment_to_read.fragment
        unit = fragment_to_read.unit
        positions = _file_row_positions(fragment)
        deleted = self._deleted_rows_by_path.get(unit.source)
        predicate = self._predicate
        predicate_columns = (
            frozenset(get_column_references(predicate))
            if predicate is not None
            else frozenset()
        )
        cursor = 0
        yielded = 0
        for table in self._scan_fragment(
            fragment, scanner_kwargs, None, extra_columns=predicate_columns
        ):
            rows = positions[cursor : cursor + table.num_rows]
            cursor += table.num_rows
            keep = np.ones(table.num_rows, dtype=bool)
            if deleted is not None:
                keep &= ~np.isin(rows, deleted)
            if predicate is not None and keep.any():
                keep &= _predicate_mask(table, predicate)
            kept = np.flatnonzero(keep)
            if len(kept) == 0:
                continue
            if len(kept) < table.num_rows:
                table = table.take(pa.array(kept))
                rows = rows[kept]
            if not self._per_row_group_offsets:
                yield table, ReadUnitPosition(
                    unit=unit,
                    rows_before=yielded,
                    unit_start_row=fragment_to_read.unit_start_row,
                )
                yielded += table.num_rows
                continue
            in_unit = rows - fragment_to_read.unit_start_row
            run_starts = np.flatnonzero(np.diff(in_unit) != 1) + 1
            bounds = [0, *run_starts.tolist(), len(in_unit)]
            for start, end in zip(bounds, bounds[1:]):
                yield table.slice(start, end - start), ReadUnitPosition(
                    unit=unit,
                    rows_before=int(in_unit[start]),
                    unit_start_row=fragment_to_read.unit_start_row,
                )

    @override
    def read_metadata(self, file_manifest: FileManifest) -> Iterator[BlockMetadata]:
        """Yield each file's row count, from the log where it has one.

        The whole-file listing puts the log's counts in ``NUM_RECORDS_COLUMN``,
        already less the rows a deletion vector deletes. Only the files without
        one have their Parquet footer read. A file with a deletion vector
        always has a count: the Delta protocol requires it, and ``deltalake``
        refuses to load the deletion vector without it.
        """
        extra_columns = file_manifest.extra_columns
        if (
            extra_columns is not None
            and NUM_RECORDS_COLUMN in extra_columns.column_names
        ):
            num_records = extra_columns[NUM_RECORDS_COLUMN].to_pylist()
            for count in num_records:
                if count is not None:
                    yield BlockMetadata(
                        num_rows=count,
                        size_bytes=None,
                        exec_stats=None,
                        input_files=None,
                    )
            unknown = [count is None for count in num_records]
            if not any(unknown):
                return
            block = file_manifest.as_block()
            # Listing manifests are always built as Arrow tables.
            assert isinstance(block, pa.Table)
            file_manifest = FileManifest(block.filter(pa.array(unknown)))
        self._add_known_file_sizes(file_manifest)
        yield from super().read_metadata(file_manifest)

    def _add_known_file_sizes(self, manifest: FileManifest) -> None:
        add_known_file_sizes(
            self._filesystem,
            dict(zip(manifest.paths.tolist(), manifest.known_file_sizes)),
        )

    @override
    def _get_partition_values(self, path: str) -> Dict[str, Optional[str]]:
        # ``ParquetFileReader`` keeps its partitioning here, not in the base
        # class's parser; ``None`` for an unpartitioned table.
        if self._partition_value_parser is None:
            return {}
        values = self._partition_values_by_path.get(path)
        if values is None:
            raise ValueError(
                f"The listing gave no partition values for the Delta data file {path!r}."
            )
        return values


@DeveloperAPI
@dataclass(frozen=True)
class DeltaScanner(ParquetScanner):
    """Scanner for a Delta Lake table.

    A Delta table's data files are Parquet files, so filter, column and limit
    pushdown and the generated ID column are inherited from
    :class:`ParquetScanner`. Partition values come from the log: the listing
    prunes files by them and carries them to :class:`DeltaFileReader`.
    ``partitioning`` only names the partition columns.
    """

    #: The table URI, for messages.
    table_uri: str = ""
    #: The table version the read lists, for messages.
    version: Optional[int] = None
    #: Whether the caller passed the version to read. Otherwise the read is
    #: of the version that was latest when the dataset was created, which a
    #: resumed job reads anew.
    version_pinned_by_caller: bool = False
    #: How the data files name the columns, or ``None`` if they use the
    #: table's column names.
    column_mapping: Optional[DeltaColumnMapping] = None

    reader_class: ClassVar[Type[ParquetFileReader]] = DeltaFileReader

    @override
    def create_reader(self) -> ParquetFileReader:
        reader = super().create_reader()
        assert isinstance(reader, DeltaFileReader), type(reader)
        reader.set_column_mapping(self.column_mapping)
        return reader

    @override
    def _create_partition_pruner(self) -> Optional[FilePruner]:
        if self.partitioning is None or self.partition_predicate is None:
            return None
        return DeltaPartitionPruner(self.partition_predicate, self.schema)
