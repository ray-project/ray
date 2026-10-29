import math
from functools import partial
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

import pyarrow as pa
import pyarrow.dataset as pds
from pyarrow.fs import FileSystem, LocalFileSystem
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_reader import (
    _ARROW_DEFAULT_BATCH_SIZE,
    FileFormat,
    FileReader,
)
from ray.data._internal.datasource_v2.common.pushdown_utils import (
    _split_predicate_by_columns,
)
from ray.data._internal.datasource_v2.formats.orc.orc_batch_size_estimator import (
    estimate_batch_size,
)
from ray.data._internal.datasource_v2.formats.orc.orc_metadata import orc_read_unit_id
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    ChunkMetadata,
    FileChunk,
    FileManifest,
)
from ray.data._internal.datasource_v2.interfaces.read_units import (
    ReadUnit,
    ReadUnitFragment,
)
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    ReadUnitPosition,
    SynthesizedColumn,
)
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
from ray.data._internal.util import call_with_retry, iterate_with_retry
from ray.data.context import DataContext
from ray.data.datasource.partitioning import Partitioning
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class OrcFileReader(FileReader):
    """Read ORC files in batches using PyArrow Dataset fragments.

    Manifest chunks select physical stripes. Whole files retain Arrow batch
    streaming; read_stripe materializes a stripe before output batching.
    Path-derived partition values take precedence over same-named file columns
    and are synthesized before filtering and projection.
    """

    def __init__(
        self,
        format: FileFormat,
        batch_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        predicate: Optional[Expr] = None,
        limit: Optional[int] = None,
        filesystem: Optional[FileSystem] = None,
        partitioning: Optional[Partitioning] = None,
        ignore_prefixes: Optional[List[str]] = None,
        synthesized_columns: Sequence[SynthesizedColumn] = (),
        schema: Optional[pa.Schema] = None,
        target_block_size: Optional[int] = None,
    ):
        """Initialize the ORC reader.

        Args:
            format: File format passed to the base reader.
            batch_size: Explicit batch row count, which takes priority over estimates.
            columns: Columns to read, or None for all columns.
            predicate: Row filter applied during reading.
            limit: Maximum number of output rows.
            filesystem: Filesystem used to open files.
            partitioning: Path-derived partition columns.
            ignore_prefixes: File name prefixes ignored by PyArrow.
            synthesized_columns: Columns appended after reading.
            schema: Unified schema for file reads and partition values.
            target_block_size: Target output batch size in bytes when batch_size
                is unset. Observed batches refine subsequent read() calls.
        """
        super().__init__(
            format=format,
            batch_size=(
                batch_size if batch_size is not None else _ARROW_DEFAULT_BATCH_SIZE
            ),
            columns=columns,
            predicate=predicate,
            limit=limit,
            filesystem=filesystem,
            partitioning=partitioning,
            ignore_prefixes=ignore_prefixes,
            synthesized_columns=synthesized_columns,
            schema=schema,
        )
        self._explicit_batch_size = batch_size
        self._target_block_size = target_block_size
        self._sampled_batch_size: Optional[int] = None

    @override
    def _resolve_batch_size(self, dataset: pds.Dataset, manifest: FileManifest) -> int:
        """Use explicit rows, cached feedback, footer statistics, or the default."""
        if self._explicit_batch_size is not None:
            return self._explicit_batch_size
        if self._sampled_batch_size is not None:
            return self._sampled_batch_size
        if self._target_block_size is None:
            return _ARROW_DEFAULT_BATCH_SIZE

        batch_size = _ARROW_DEFAULT_BATCH_SIZE
        fragment = next(dataset.get_fragments(), None)
        if fragment is not None:
            excluded = {column.name for column in self._synthesized_columns}
            if self._partition_parser is not None:
                excluded.update(self._partition_parser(fragment.path))
            estimated = estimate_batch_size(
                fragment,
                self._columns,
                self._target_block_size,
                schema=self._file_dataset_schema,
                excluded_columns=tuple(excluded),
            )
            if estimated is not None:
                batch_size = estimated
        self._sampled_batch_size = batch_size
        return batch_size

    @override
    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        """Keep the declared order after per-file partition synthesis."""
        schema = self._schema
        for table in super().read(input_split):
            if self._columns is None and schema is not None:
                produced = set(table.column_names)
                schema_names = schema.names
                schema_name_set = set(schema_names)
                column_names = [name for name in schema_names if name in produced]
                column_names.extend(
                    name for name in table.column_names if name not in schema_name_set
                )
                table = table.select(column_names)
            yield table

    @override
    def _get_fragments_to_read(
        self, dataset: pds.Dataset, manifest: FileManifest
    ) -> List[ReadUnitFragment]:
        by_path = {fragment.path: fragment for fragment in dataset.get_fragments()}
        chunks: Dict[str, List[Optional[ChunkMetadata]]] = {}
        for path, metadata in zip(manifest.paths, manifest.file_chunk_metadatas):
            chunks.setdefault(path, []).append(metadata)
        result = []
        for path, metadatas in chunks.items():
            fragment = by_path[path]
            if all(metadata is None for metadata in metadatas):
                num_rows = None
                if any(
                    column.requires_read_unit_boundaries
                    for column in self._synthesized_columns
                ):
                    import pyarrow.orc as orc

                    def file_rows(path=path):
                        filesystem = self._filesystem or LocalFileSystem()
                        with filesystem.open_input_file(path) as source:
                            return orc.ORCFile(source).nrows

                    # Whole-file manifests intentionally have no chunk payload.
                    # Only a boundary consumer needs this extra Arrow metadata
                    # session; ordinary streamed scans keep their existing IO.
                    num_rows = call_with_retry(
                        file_rows,
                        description=f"read ORC row count for {path}",
                        match=DataContext.get_current().retried_io_errors,
                    )
                result.append(
                    ReadUnitFragment(
                        fragment,
                        ReadUnit(id=path, source=path, count=1, num_rows=num_rows),
                    )
                )
                continue
            if any(metadata is None for metadata in metadatas):
                raise ValueError(f"Mixed whole-file and stripe assignments for {path}")
            runs = [
                FileChunk.from_metadata(metadata)
                for metadata in metadatas
                if metadata is not None
            ]
            counts = {run.file_num_units for run in runs}
            if len(counts) != 1 or None in counts:
                raise ValueError(f"Missing/inconsistent ORC file_num_units for {path}")
            count = runs[0].file_num_units
            if count is None or count <= 0:
                raise ValueError(f"Invalid ORC file_num_units for {path}")
            units = []
            for run in runs:
                if not run.unit_rows or not run.unit_start_rows:
                    raise ValueError(f"Missing ORC stripe positions for {path}")
                for index, rows, start in zip(
                    run.unit_ids, run.unit_rows, run.unit_start_rows
                ):
                    if not 0 <= index < count or rows < 0 or start < 0:
                        raise ValueError(f"Invalid ORC stripe {index} for {path}")
                    units.append((index, rows, start))
            if len({index for index, _, _ in units}) != len(units):
                raise ValueError(f"Duplicate ORC stripe assignments for {path}")
            if [index for index, _, _ in units] == list(range(count)) and not any(
                column.requires_read_unit_boundaries
                for column in self._synthesized_columns
            ):
                result.append(
                    ReadUnitFragment(
                        fragment,
                        ReadUnit(
                            id=path,
                            source=path,
                            count=1,
                            num_rows=sum(rows for _, rows, _ in units),
                        ),
                    )
                )
                continue
            for index, rows, start in units:
                result.append(
                    ReadUnitFragment(
                        fragment,
                        ReadUnit(
                            id=orc_read_unit_id(path, index),
                            source=path,
                            index=index,
                            count=count,
                            num_rows=rows,
                        ),
                        unit_start_row=start,
                    )
                )
        return result

    @override
    def _read_fragment_batches(
        self,
        dataset: pds.Dataset,
        scanner_kwargs: Dict[str, Any],
        manifest: FileManifest,
    ) -> Iterator[Tuple[pa.Table, ReadUnitPosition]]:
        if all(metadata is None for metadata in manifest.file_chunk_metadatas):
            # Preserve the existing concurrent whole-file scanner, including
            # optional-backend fallback and directories of single-stripe files.
            yield from super()._read_fragment_batches(dataset, scanner_kwargs, manifest)
            return
        # A task decodes at most one stripe at a time. Output batching and
        # inter-task scheduling remain independent of this materialization cap.
        yield from self._read_fragments_sequential(
            iter(self._get_fragments_to_read(dataset, manifest)), scanner_kwargs
        )

    @override
    def _read_fragments_sequential(
        self,
        fragments_with_offsets: Iterator[ReadUnitFragment],
        scanner_kwargs: Dict[str, Any],
    ) -> Iterator[Tuple[pa.Table, ReadUnitPosition]]:
        sessions = {}
        rows_read = 0
        try:
            for item in fragments_with_offsets:
                if self._limit is not None and rows_read >= self._limit:
                    return
                # Keep just one file session alive, reusing it for adjacent units.
                for path in list(sessions):
                    if path != item.unit.source:
                        sessions.pop(path)[0].close()
                if item.unit.id == item.unit.source:
                    for table, position in super()._read_fragments_sequential(
                        iter([item]), scanner_kwargs
                    ):
                        yield table, position
                        rows_read += table.num_rows
                    continue
                rows_before = 0
                for table in iterate_with_retry(
                    partial(self._iter_stripe_tables, item, scanner_kwargs, sessions),
                    f"read ORC stripe {item.unit.index} from {item.unit.source}",
                    match=DataContext.get_current().retried_io_errors,
                ):
                    if table.num_rows:
                        yield table, ReadUnitPosition(
                            unit=item.unit,
                            rows_before=rows_before,
                            unit_start_row=item.unit_start_row,
                        )
                        rows_before += table.num_rows
                        rows_read += table.num_rows
                        if self._limit is not None and rows_read >= self._limit:
                            return
        finally:
            for source, _ in sessions.values():
                source.close()

    def _iter_stripe_tables(
        self, item: ReadUnitFragment, scanner_kwargs: Dict[str, Any], sessions: dict
    ) -> Iterator[pa.Table]:
        import pyarrow.orc as orc

        path = item.unit.source
        if path not in sessions:
            source = (self._filesystem or LocalFileSystem()).open_input_file(path)
            try:
                sessions[path] = (source, orc.ORCFile(source))
            except BaseException:
                source.close()
                raise
        source, reader = sessions[path]
        try:
            if item.unit.num_rows is None:
                raise ValueError(f"Missing ORC stripe row count for {path}")
            if (
                reader.nstripes != item.unit.count
                or item.unit_start_row + item.unit.num_rows > reader.nrows
            ):
                raise ValueError(f"ORC layout changed for {path}")
            yield from self._scan_stripe(
                reader, path, item.unit.index, scanner_kwargs, item.unit.num_rows
            )
        except BaseException:
            sessions.pop(path)[0].close()
            raise

    def _scan_stripe(self, reader, path, stripe_index, scanner_kwargs, num_rows):
        def scan(schema, kwargs):
            names = schema.names if kwargs.get("columns") is None else kwargs["columns"]
            columns = [
                name
                for name in reader.schema.names
                if any(
                    requested == name or requested.startswith(name + ".")
                    for requested in names
                )
            ]
            if columns:
                table = pa.Table.from_batches(
                    [reader.read_stripe(stripe_index, columns=columns)]
                )
            else:
                # Cardinality carrier after opening/validating the real ORC file.
                # No arbitrary physical column is decoded for an empty projection.
                table = pa.table({"__orc_cardinality": pa.nulls(num_rows)})
            raise_on_pickle_object_columns(table)
            fragment = next(pds.dataset(table).get_fragments())
            yield from fragment.scanner(schema=schema, **kwargs).scan_batches()

        yield from self._scan_tables(path, reader.schema, scanner_kwargs, scan)

    @override
    def _iter_fragment_tables(
        self, fragment: pds.Fragment, scanner_kwargs: dict
    ) -> Iterator[pa.Table]:
        """Read one ORC fragment and apply its path-derived partition values.

        PyArrow reads physical columns and evaluates data predicates. Partition
        values are synthesized from the path, then any residual predicate is
        evaluated on the resulting table.
        """
        yield from self._scan_tables(
            fragment.path,
            fragment.physical_schema,
            scanner_kwargs,
            lambda schema, kwargs: fragment.scanner(
                schema=schema, **kwargs
            ).scan_batches(),
        )

    def _scan_tables(
        self,
        path: str,
        physical_schema: pa.Schema,
        scanner_kwargs: Dict[str, Any],
        scan,
    ) -> Iterator[pa.Table]:
        partitions = self._partition_parser(path) if self._partition_parser else {}
        schema = self._schema
        if schema is None:
            schema = physical_schema
        synthesized = {column.name for column in self._synthesized_columns}
        schema = pa.schema([field for field in schema if field.name not in synthesized])

        for name, value in partitions.items():
            if schema.get_field_index(name) == -1:
                schema = schema.append(
                    pa.field(name, self._broadcast_partition_value(name, value, 0).type)
                )

        schema_names = set(schema.names)
        output_columns = (
            schema.names
            if self._columns is None
            else [name for name in self._columns if name in schema_names]
        )
        filter_columns = (
            get_column_references(self._predicate)
            if self._predicate is not None
            else []
        )
        required_columns = list(dict.fromkeys(output_columns + filter_columns))

        data_predicate = self._predicate
        residual_predicate = None
        partition_matches = True
        if self._predicate is not None:
            split = _split_predicate_by_columns(self._predicate, set(partitions))
            data_predicate = split.data_predicate
            residual_predicate = split.residual_predicate
            if split.partition_predicate is not None:
                # Compare the same typed values that will appear in the output.
                # A missing directory key stays a data predicate, so root-file
                # values and logical nulls cannot be pruned by a path guess.
                partition_columns = set(
                    get_column_references(split.partition_predicate)
                )
                partition_table = pa.table(
                    {
                        name: self._broadcast_partition_value(name, value, 1)
                        for name, value in partitions.items()
                        if name in partition_columns
                    }
                )
                partition_matches = (
                    partition_table.filter(
                        split.partition_predicate.to_pyarrow()
                    ).num_rows
                    > 0
                )

        # The shared kwargs are also used by concurrent fragment reads.
        scan_kwargs: Dict[str, Any] = dict(scanner_kwargs)

        if not partition_matches:
            return

        required_names = set(required_columns)
        # Partition fields come from the path, so only read physical columns.
        scan_kwargs["columns"] = [
            name for name in required_columns if name not in partitions
        ]
        # Start from the full predicate in scanner_kwargs. After splitting,
        # only the data part belongs in PyArrow; other parts are handled separately.
        if data_predicate is not self._predicate:
            scan_kwargs["filter"] = (
                data_predicate.to_pyarrow() if data_predicate is not None else None
            )
        file_schema = pa.schema(
            [field for field in schema if field.name not in partitions]
        )
        # Filtering must not hide an unsafe column that this scan will decode.
        decoded_names = set(scan_kwargs["columns"])
        decoded_schema = pa.schema(
            [field for field in physical_schema if field.name in decoded_names]
        )
        raise_on_pickle_object_columns(pa.Table.from_batches([], schema=decoded_schema))
        residual_filter = (
            residual_predicate.to_pyarrow() if residual_predicate is not None else None
        )
        for tagged in scan(file_schema, scan_kwargs):
            table = pa.Table.from_batches([tagged.record_batch])
            for name, value in partitions.items():
                if name in required_names:
                    table = table.append_column(
                        name,
                        self._broadcast_partition_value(name, value, table.num_rows),
                    )
            if residual_filter is not None:
                table = table.filter(residual_filter)
            yield table

    @override
    def _on_batch_read(self, table: pa.Table) -> None:
        super()._on_batch_read(table)
        raise_on_pickle_object_columns(table)
        if self._target_block_size is None or table.nbytes == 0 or table.num_rows == 0:
            return
        row_size = table.nbytes / table.num_rows
        self._sampled_batch_size = max(math.ceil(self._target_block_size / row_size), 1)
