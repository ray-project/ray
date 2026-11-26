from dataclasses import dataclass
from typing import ClassVar, Dict, Iterator, List, Optional, Type

import pyarrow as pa
import pyarrow.dataset as pds
from typing_extensions import override

from ray.data._internal.datasource_v2.formats.delta.delta_column_mapping import (
    DeltaColumnMapping,
)
from ray.data._internal.datasource_v2.formats.delta.delta_file_indexer import (
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
from ray.data.block import BlockMetadata
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI


class DeltaFileReader(ParquetFileReader):
    """Parquet reader that takes partition values from the listing.

    A Delta file's path needn't hold its partition values, so the listing puts
    the ones the log records in each manifest (``PARTITION_VALUES_COLUMN``).
    The reader casts them to the columns' types as it does a value parsed from
    a path. It also hands the files' sizes from the log to the table's
    filesystem, which then opens them without asking the storage for them.
    """

    # Replaced, never changed in place, by each ``read``.
    _partition_values_by_path: Dict[str, PartitionValues] = {}
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
        yield from super().read(input_split)

    @override
    def read_metadata(self, file_manifest: FileManifest) -> Iterator[BlockMetadata]:
        """Yield each file's row count, from the log where it has one.

        The whole-file listing puts the log's counts in ``NUM_RECORDS_COLUMN``.
        Only the files without one have their Parquet footer read. Tables with
        deletion vectors, whose log counts include deleted rows, aren't read
        with this reader.
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
