import logging
import math
from contextlib import closing
from typing import Generator, Iterator, List, Optional, Sequence, cast

import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.orc as orc
from pyarrow.fs import FileSystem
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
from ray.data._internal.datasource_v2.formats.orc.orc_metadata import (
    MetadataUnavailable,
    read_metadata,
    read_stripe_statistics,
)
from ray.data._internal.datasource_v2.formats.orc.orc_predicate import (
    OrcPredicate,
    safe_scan_types,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
from ray.data.datasource.partitioning import Partitioning
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


def validate_statistics_pruning(mode):
    if mode not in ("off", "file", "stripe"):
        raise ValueError("orc_statistics_pruning must be 'off', 'file', or 'stripe'")
    return mode


@DeveloperAPI
class OrcFileReader(FileReader):
    """Read ORC files in batches using PyArrow Dataset fragments.

    Each fragment covers a whole file. PyArrow applies row filters to the
    scanned batches. File statistics pruning is enabled by default; explicitly
    selecting ``stripe`` also skips stripes inside the existing file read unit.
    Surviving stripes are fully materialized by PyArrow before batching.
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
        statistics_pruning: str = "file",
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
            statistics_pruning: File or stripe statistics pruning, or off.
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
        self._statistics_pruning = validate_statistics_pruning(statistics_pruning)

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
        # FileReader.read is a generator. Close it when the ORC limit is met
        # before requesting another fragment; the shared loop checks its limit
        # only after fetching the next table.
        batches = cast(Generator[pa.Table, None, None], super().read(input_split))
        rows = 0
        with closing(batches):
            for table in batches:
                if self._columns is None and schema is not None:
                    produced = set(table.column_names)
                    schema_names = schema.names
                    schema_name_set = set(schema_names)
                    column_names = [name for name in schema_names if name in produced]
                    column_names.extend(
                        name
                        for name in table.column_names
                        if name not in schema_name_set
                    )
                    table = table.select(column_names)
                rows += table.num_rows
                yield table
                if self._limit is not None and rows >= self._limit:
                    return

    @override
    def _iter_fragment_tables(
        self, fragment: pds.Fragment, scanner_kwargs: dict
    ) -> Iterator[pa.Table]:
        """Read one ORC fragment and apply its path-derived partition values.

        PyArrow reads physical columns and evaluates data predicates. Partition
        values are synthesized from the path, then any residual predicate is
        evaluated on the resulting table.
        """
        if self._partition_parser is None:
            schema = self._file_dataset_schema
            if schema is None:
                schema = fragment.physical_schema
            yield from self._scan_with_statistics(
                fragment, scanner_kwargs, schema, self._predicate, limit=self._limit
            )
            return

        partitions = self._partition_parser(fragment.path)
        physical_schema = fragment.physical_schema
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
        scan_kwargs = dict(scanner_kwargs)

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
        yielded = 0
        for table in self._scan_with_statistics(
            fragment,
            scan_kwargs,
            file_schema,
            data_predicate,
            limit=self._limit if residual_filter is None else None,
        ):
            for name, value in partitions.items():
                if name in required_names:
                    table = table.append_column(
                        name,
                        self._broadcast_partition_value(name, value, table.num_rows),
                    )
            if residual_filter is not None:
                table = table.filter(residual_filter)
            yielded += table.num_rows
            yield table
            if self._limit is not None and yielded >= self._limit:
                return

    def _scan_with_statistics(
        self, fragment, scan_kwargs, schema, predicate, limit=None
    ):
        # Bind the original Arrow scanner first: pruning must not suppress
        # missing-field, invalid-expression, or schema binding errors.
        scanner = fragment.scanner(**scan_kwargs, schema=schema)
        physical = fragment.physical_schema
        output = scan_kwargs.get("columns")
        required = set(schema.names if output is None else output)
        if predicate is not None:
            required.update(get_column_references(predicate))
        decoded = pa.schema([field for field in physical if field.name in required])
        raise_on_pickle_object_columns(pa.Table.from_batches([], schema=decoded))
        bound = OrcPredicate(predicate, physical, schema)
        selected = None
        if (
            self._statistics_pruning != "off"
            and bound.eligible
            and safe_scan_types(physical, schema, required)
        ):
            with fragment.filesystem.open_input_file(fragment.path) as source:
                try:
                    metadata = read_metadata(source)
                    if not bound.can_match(metadata.statistics, metadata.rows):
                        return
                    if self._statistics_pruning == "stripe":
                        stats = read_stripe_statistics(source, metadata)
                        selected = [
                            index
                            for index, (stat, stripe) in enumerate(
                                zip(stats, metadata.stripes)
                            )
                            if bound.can_match(stat, stripe[2])
                        ]
                        if len(selected) == len(metadata.stripes):
                            selected = None
                except MetadataUnavailable as exc:
                    logger.debug(
                        "ORC statistics unavailable for %s: %s", fragment.path, exc
                    )
                    selected = None
                if selected is not None:
                    yield from self._scan_stripes(
                        source,
                        selected,
                        scan_kwargs,
                        schema,
                        physical,
                        predicate,
                        limit,
                    )
                    return
        for tagged in scanner.scan_batches():
            yield pa.Table.from_batches([tagged.record_batch])

    def _scan_stripes(
        self, source, selected, scan_kwargs, schema, physical, predicate, limit
    ):
        if not selected:
            return
        output = scan_kwargs.get("columns")
        required = set(schema.names if output is None else output)
        if predicate is not None:
            required.update(get_column_references(predicate))
        columns = [name for name in physical.names if name in required]
        reader = orc.ORCFile(source)
        yielded = 0
        for index in selected:
            if limit is not None and yielded >= limit:
                break
            # read_stripe() materializes the whole stripe. Python batch iteration over
            # selected stripes is tracked in:
            # https://github.com/apache/arrow/issues/54921
            batch = reader.read_stripe(index, columns=columns)
            table = pa.Table.from_batches([batch])
            # Check the decoder's actual result before a cast or projection can
            # remove an unsafe extension marker, including filter-only columns.
            raise_on_pickle_object_columns(table)
            # Dataset applies the same logical schema, missing-column NULLs,
            # exact residual data filter, projection and batch_size as the
            # streaming path. ORC decode itself still materializes one stripe.
            read_schema = pa.schema(
                [field for field in schema if field.name in required]
            )
            arrays = [
                table[field.name].cast(field.type)
                if field.name in table.column_names
                else pa.nulls(table.num_rows, type=field.type)
                for field in read_schema
            ]
            table = pa.Table.from_arrays(arrays, schema=read_schema)
            dataset = pds.dataset(table)
            for tagged in dataset.scanner(**scan_kwargs).scan_batches():
                result = pa.Table.from_batches([tagged.record_batch])
                yielded += result.num_rows
                yield result
                if limit is not None and yielded >= limit:
                    return

    @override
    def _on_batch_read(self, table: pa.Table) -> None:
        super()._on_batch_read(table)
        raise_on_pickle_object_columns(table)
        if self._target_block_size is None or table.nbytes == 0 or table.num_rows == 0:
            return
        row_size = table.nbytes / table.num_rows
        self._sampled_batch_size = max(math.ceil(self._target_block_size / row_size), 1)
