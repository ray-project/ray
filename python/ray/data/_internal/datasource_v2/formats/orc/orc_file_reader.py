from typing import Iterator, Tuple

import pyarrow as pa
import pyarrow.dataset as pds
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_reader import FileReader
from ray.data._internal.datasource_v2.common.pushdown_utils import (
    _split_predicate_by_columns,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
from ray.util.annotations import DeveloperAPI


def _struct_scan_schema(
    physical_schema: pa.Schema, target_schema: pa.Schema
) -> Tuple[pa.Schema, bool]:
    """Return the schema to scan with when struct children differ from the target.

    PyArrow older than 21 cannot reconcile struct columns whose children differ
    from the file's physical type, and raises ``ArrowTypeError`` while scanning.
    For such columns, scan with the physical struct type so the read succeeds;
    the caller then aligns and casts to ``target_schema``. Other fields keep the
    target type, so columns missing from the file are still null-filled.

    Args:
        physical_schema: The schema stored in the ORC file.
        target_schema: The unified schema the reader should produce.

    Returns:
        ``(scan_schema, needs_alignment)``.
    """
    fields = []
    needs_alignment = False
    for field in target_schema:
        index = physical_schema.get_field_index(field.name)
        if index != -1:
            physical_type = physical_schema.field(index).type
            if (
                pa.types.is_struct(physical_type)
                and pa.types.is_struct(field.type)
                and physical_type != field.type
            ):
                field = field.with_type(physical_type)
                needs_alignment = True
        fields.append(field)
    return pa.schema(fields), needs_alignment


def _align_to_schema(table: pa.Table, target_schema: pa.Schema) -> pa.Table:
    """Backfill missing struct children with nulls and cast to ``target_schema``."""
    from ray.data._internal.arrow_ops.transform_pyarrow import _align_struct_fields

    names = [name for name in table.column_names if name in target_schema.names]
    align_schema = pa.schema([target_schema.field(name) for name in names])
    return _align_struct_fields([table.select(names)], align_schema)[0].cast(
        align_schema
    )


@DeveloperAPI
class OrcFileReader(FileReader):
    """Read ORC files in batches using PyArrow Dataset fragments.

    Each fragment covers a whole file. PyArrow applies row filters to the
    scanned batches; this reader does not provide ORC-native stripe pruning.
    Path-derived partition values take precedence over same-named file columns
    and are synthesized before filtering and projection.
    """

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
            target_schema = self._file_dataset_schema
            if target_schema is not None:
                scan_schema, needs_alignment = _struct_scan_schema(
                    fragment.physical_schema, target_schema
                )
                if needs_alignment:
                    scanner = fragment.scanner(**scanner_kwargs, schema=scan_schema)
                    for tagged in scanner.scan_batches():
                        table = pa.Table.from_batches([tagged.record_batch])
                        yield _align_to_schema(table, target_schema)
                    return
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
                partition_table = pa.table(
                    {
                        name: self._broadcast_partition_value(name, value, 1)
                        for name, value in partitions.items()
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
        # Older PyArrow cannot read files whose nested struct children differ
        # from the target schema, so read those columns as stored and align.
        scan_schema, needs_alignment = _struct_scan_schema(physical_schema, file_schema)
        scanner = fragment.scanner(**scan_kwargs, schema=scan_schema)
        residual_filter = (
            residual_predicate.to_pyarrow() if residual_predicate is not None else None
        )
        for tagged in scanner.scan_batches():
            table = pa.Table.from_batches([tagged.record_batch])
            if needs_alignment:
                table = _align_to_schema(table, file_schema)
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
