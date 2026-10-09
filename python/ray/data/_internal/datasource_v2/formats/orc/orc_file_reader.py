from typing import Iterator

import pyarrow as pa
import pyarrow.dataset as pds
from pyarrow.fs import LocalFileSystem
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
from ray.data.datasource.file_based_datasource import _add_partitions_to_table
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class OrcFileReader(FileReader):
    """Read ORC files in batches using PyArrow Dataset fragments.

    Each fragment covers a whole file. PyArrow applies row filters to the
    scanned batches; this reader does not provide ORC-native stripe pruning.
    Partitioned reads validate and synthesize partition values before projection.
    """

    @override
    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        """Keep the declared order after per-file validation and synthesis."""
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
        validation_columns = [
            name for name in partitions if physical_schema.get_field_index(name) != -1
        ]

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
        scan_kwargs["columns"] = required_columns
        required_names = set(required_columns)

        if validation_columns:
            import pyarrow.orc as orc

            # Validate a whole stripe before any row-reducing operation.
            # Batch-level checks reject a null-only batch in a valid stripe.
            # Even a rejected directory must validate its real partition fields.
            filesystem = self._filesystem or LocalFileSystem()
            columns = [
                name
                for name in dict.fromkeys(
                    (required_columns if partition_matches else []) + validation_columns
                )
                if physical_schema.get_field_index(name) != -1
            ]
            with filesystem.open_input_file(fragment.path) as source:
                orc_file = orc.ORCFile(source)
                for stripe_index in range(orc_file.nstripes):
                    table = pa.Table.from_batches(
                        [orc_file.read_stripe(stripe_index, columns=columns)]
                    )
                    if table.num_rows == 0:
                        continue
                    raise_on_pickle_object_columns(table)
                    table = _add_partitions_to_table(table, partitions)
                    if not partition_matches:
                        continue
                    # Validation uses physical types. Filtering must use the
                    # same logical path values as the final output, without
                    # carrying over rounding or formatting from those types.
                    for name, value in partitions.items():
                        if name in required_names:
                            column = self._broadcast_partition_value(
                                name, value, table.num_rows
                            )
                            table = table.set_column(
                                table.schema.get_field_index(name), name, column
                            )
                            index = schema.get_field_index(name)
                            schema = schema.set(
                                index, schema.field(index).with_type(column.type)
                            )
                    # Reuse Arrow's schema alignment and batch sizing after
                    # validation, including null-fill for missing data fields.
                    for stripe in pds.dataset(table).get_fragments():
                        scanner = stripe.scanner(**scan_kwargs, schema=schema)
                        for tagged in scanner.scan_batches():
                            yield pa.Table.from_batches([tagged.record_batch])
            return

        if not partition_matches:
            return

        # Directory-only fields are produced here, before the base reader's
        # projection and limit. They must not be read as null placeholders.
        scan_kwargs["columns"] = [
            name for name in required_columns if name not in partitions
        ]
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
        for tagged in scanner.scan_batches():
            table = pa.Table.from_batches([tagged.record_batch])
            for name, value in partitions.items():
                if name in required_names:
                    table = table.append_column(
                        name,
                        self._broadcast_partition_value(name, value, table.num_rows),
                    )
            if residual_predicate is not None:
                table = table.filter(residual_predicate.to_pyarrow())
            yield table

    @override
    def _on_batch_read(self, table: pa.Table) -> None:
        super()._on_batch_read(table)
        raise_on_pickle_object_columns(table)
