import math
from contextlib import ExitStack, closing
from functools import partial
from typing import Any, Dict, Generator, Iterator, List, Optional, Sequence, Tuple, cast

import pyarrow as pa
import pyarrow.dataset as pds
from pyarrow.fs import FileSystem, LocalFileSystem
from typing_extensions import override

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
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
from ray.data._internal.datasource_v2.formats.orc.orc_metadata import (
    OrcFileMetadata,
    OrcStripeMetadata,
    orc_read_unit_id,
    read_orc_file,
    read_orc_layout,
    require_stripe_reads,
)
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
    streaming; selected stripes use Arrow's existing stripe decoder.
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
        # Stripe assignments are expanded below with their decoder already open.
        return [
            self._whole_file_fragment(fragment) for fragment in dataset.get_fragments()
        ]

    def _whole_file_fragment(self, fragment):
        path = fragment.path
        rows = None
        if any(c.requires_read_unit_boundaries for c in self._synthesized_columns):
            rows = read_orc_file(
                path, self._filesystem, lambda reader, size: reader.nrows
            )
        return ReadUnitFragment(
            fragment, ReadUnit(id=path, source=path, count=1, num_rows=rows)
        )

    @override
    def _read_fragment_batches(self, dataset, scanner_kwargs, manifest):
        if self._limit is not None and self._limit <= 0:
            return
        if all(m is None for m in manifest.file_chunk_metadatas):
            yield from super()._read_fragment_batches(dataset, scanner_kwargs, manifest)
            return
        fragments = {f.path: f for f in dataset.get_fragments()}
        assignments: Dict[str, List[Optional[ChunkMetadata]]] = {}
        for path, metadata in zip(manifest.paths, manifest.file_chunk_metadatas):
            assignments.setdefault(path, []).append(metadata)
        rows = 0
        for path, metadata in assignments.items():
            if all(m is None for m in metadata):
                batches = super()._read_fragments_sequential(
                    iter([self._whole_file_fragment(fragments[path])]), scanner_kwargs
                )
            else:
                if any(m is None for m in metadata):
                    raise ValueError(
                        f"Mixed whole-file and stripe assignments for {path}"
                    )
                batches = self._read_stripes(path, metadata, scanner_kwargs)
            with closing(
                cast(Generator[Tuple[pa.Table, ReadUnitPosition], None, None], batches)
            ):
                for table, position in batches:
                    yield table, position
                    rows += table.num_rows
                    if self._limit is not None and rows >= self._limit:
                        return

    def _read_stripes(self, path, metadata, scanner_kwargs):
        import pyarrow.orc as orc

        require_stripe_reads(path)
        reader = layout = None
        retry_errors = DataContext.get_current().retried_io_errors
        with ExitStack() as inputs:

            def open_reader():
                nonlocal reader, layout
                if reader is None:
                    try:
                        source = inputs.enter_context(
                            (self._filesystem or LocalFileSystem()).open_input_file(
                                path
                            )
                        )
                        owner = orc.ORCFile(source)
                        actual = read_orc_layout(owner, path, source.size())
                        if layout is not None and actual != layout:
                            raise ValueError(f"ORC layout changed for {path}")
                        reader, layout = owner, actual
                    except BaseException:
                        inputs.close()
                        raise
                return reader

            def read(stripe):
                nonlocal reader
                try:
                    yield from self._iter_stripe_tables(
                        open_reader(), path, stripe, scanner_kwargs
                    )
                except BaseException:
                    inputs.close()
                    reader = None
                    raise

            call_with_retry(
                open_reader, description=f"open ORC {path}", match=retry_errors
            )
            layout = cast(OrcFileMetadata, layout)
            stripes = layout.stripes
            assert stripes is not None
            indices = []
            for value in metadata:
                run = FileChunk.from_metadata(value)
                if any(not 0 <= i < len(stripes) for i in run.unit_ids):
                    raise ValueError(f"Invalid ORC stripe assignment for {path}")
                if sum(stripes[i].num_rows for i in run.unit_ids) != run.num_rows:
                    raise ValueError(f"ORC layout changed for {path}")
                indices.extend(run.unit_ids)
            if len(set(indices)) != len(indices):
                raise ValueError(f"Duplicate ORC stripe assignments for {path}")
            for index in indices:
                stripe = stripes[index]
                unit = ReadUnit(
                    id=orc_read_unit_id(path, index),
                    source=path,
                    index=index,
                    count=len(stripes),
                    num_rows=stripe.num_rows,
                )
                cursor = 0
                with closing(
                    iterate_with_retry(
                        partial(read, stripe),
                        f"read ORC unit {unit.id}",
                        match=retry_errors,
                    )
                ) as batches:
                    for table in batches:
                        if table.num_rows:
                            yield table, ReadUnitPosition(
                                unit, cursor, stripe.row_offset
                            )
                            cursor += table.num_rows

    def _iter_stripe_tables(
        self, reader, path, stripe: OrcStripeMetadata, scanner_kwargs
    ):
        def scan(schema, kwargs):
            names = schema.names if kwargs.get("columns") is None else kwargs["columns"]
            physical_names = reader.schema.names
            columns = [
                name
                for name in physical_names
                if any(
                    requested == name
                    or (
                        requested not in physical_names
                        and requested.startswith(name + ".")
                    )
                    for requested in names
                )
            ]
            # Empty read_stripe projections lose row counts on released Arrow.
            # Use the validated footer count without decoding any file column.
            table = (
                pa.Table.from_batches(
                    [reader.read_stripe(stripe.index, columns=columns)]
                )
                if columns
                else pa.table(
                    {_BATCH_SIZE_PRESERVING_STUB_COL_NAME: pa.nulls(stripe.num_rows)}
                )
            )
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
        synthesized = {column.name for column in self._synthesized_columns}
        schema = self._schema
        if schema is None or not set(schema.names).difference(synthesized, partitions):
            schema = physical_schema
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
