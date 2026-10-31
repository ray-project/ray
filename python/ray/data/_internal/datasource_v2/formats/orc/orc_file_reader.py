import math
from concurrent.futures import ThreadPoolExecutor
from typing import Iterator, List, Optional, Sequence, Set

import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.orc as orc
from pyarrow.fs import FileSystem, LocalFileSystem
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_metadata import (
    get_cached_file_metadata,
)
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
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.supports_metadata import (
    MetadataType,
    SupportsMetadata,
)
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
from ray.data._internal.util import call_with_retry
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource.partitioning import Partitioning
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class OrcFileReader(FileReader, SupportsMetadata):
    """Read ORC files in batches using PyArrow Dataset fragments.

    Each fragment covers a whole file. PyArrow applies row filters to the
    scanned batches; this reader does not provide ORC-native stripe pruning.
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
        reuse_file_metadata: bool = False,
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
            reuse_file_metadata: Enable metadata counts for full-schema reads.
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
        self._reuse_file_metadata = reuse_file_metadata

    @override
    def read_metadata(self, file_manifest: FileManifest) -> Iterator[BlockMetadata]:
        """Read exact file row counts, preferring full-schema planning metadata."""
        filesystem = self._filesystem or LocalFileSystem()
        retried_io_errors = DataContext.get_current().retried_io_errors
        metadatas = file_manifest.file_metadatas or [None] * len(file_manifest)
        files = [
            FileInfo(str(path), size, metadata=metadata)
            for path, size, metadata in zip(
                file_manifest.paths, file_manifest.file_sizes, metadatas
            )
        ]

        def num_rows(file: FileInfo) -> int:
            cached = get_cached_file_metadata(file, filesystem, retried_io_errors)
            if cached is not None and cached.num_rows is not None:
                return cached.num_rows

            def read_footer() -> int:
                try:
                    with filesystem.open_input_file(file.path) as source:
                        return orc.ORCFile(source).nrows
                except (OSError, pa.ArrowInvalid) as error:
                    raise type(error)(
                        f"Failed to read ORC footer for {file.path!r}: {error}"
                    ) from error

            return call_with_retry(
                read_footer,
                description=f"read ORC footer for {file.path}",
                match=retried_io_errors,
            )

        with ThreadPoolExecutor(max_workers=16) as executor:
            for count in executor.map(num_rows, files):
                yield BlockMetadata(
                    num_rows=count, size_bytes=None, exec_stats=None, input_files=None
                )

    @override
    def available_metadata(self) -> Set[MetadataType]:
        if self._reuse_file_metadata and self._predicate is None:
            return {MetadataType.NUM_ROWS}
        return set()

    @override
    def get_target_metadata_batch_size(self) -> Optional[int]:
        return 16

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
    def _iter_fragment_tables(
        self, fragment: pds.Fragment, scanner_kwargs: dict
    ) -> Iterator[pa.Table]:
        """Read one ORC fragment and apply its path-derived partition values.

        PyArrow reads physical columns and evaluates data predicates. Partition
        values are synthesized from the path, then any residual predicate is
        evaluated on the resulting table.
        """
        if self._partition_parser is None:
            yield from super()._iter_fragment_tables(fragment, scanner_kwargs)
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
        scanner = fragment.scanner(**scan_kwargs, schema=file_schema)
        residual_filter = (
            residual_predicate.to_pyarrow() if residual_predicate is not None else None
        )
        for tagged in scanner.scan_batches():
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
