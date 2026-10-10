from dataclasses import dataclass, replace
from typing import Optional, Tuple

import pyarrow as pa
from typing_extensions import override

from ray.data._internal.datasource_v2.common.arrow_file_scanner import (
    ArrowFileScanner,
)
from ray.data._internal.datasource_v2.common.file_pruners import (
    PartitionPredicatePruner,
)
from ray.data._internal.datasource_v2.common.file_reader import (
    _ARROW_DEFAULT_BATCH_SIZE,
    FileFormat,
)
from ray.data._internal.datasource_v2.common.pushdown_utils import (
    _split_predicate_by_columns,
    combine_predicates,
)
from ray.data._internal.datasource_v2.formats.orc.orc_file_reader import OrcFileReader
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
from ray.data.datasource.partitioning import (
    Partitioning,
    _partition_field_types_to_pa_schema,
)
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI


class _OrcPartitionPredicatePruner(PartitionPredicatePruner):
    """Prune known partition conditions and defer unknown values to the reader."""

    def __init__(self, partitioning: Partitioning, predicate: Expr, schema: pa.Schema):
        super().__init__(partitioning, predicate)
        self._predicate_columns = set(get_column_references(predicate))
        path_schema = _partition_field_types_to_pa_schema(
            field_names=list(self._predicate_columns),
            field_types=partitioning.field_types or {},
        )
        self._type_safe_columns = {
            field.name
            for field in path_schema
            if schema.get_field_index(field.name) != -1
            and schema.field(field.name).type == field.type
        }

    @override
    def should_include(self, path: str) -> bool:
        partitions = self._parser(path)
        if self._predicate_columns.issubset(partitions):
            return super().should_include(path)

        # A known false AND conjunct rules out the file even if another key is
        # missing. Only use values with the reader's logical type: narrowing or
        # casting here could otherwise disagree with the synthesized output.
        known_values = {
            name: [value]
            for name, value in partitions.items()
            if name in self._type_safe_columns
        }
        split = _split_predicate_by_columns(self._predicate, set(known_values))
        if split.partition_predicate is None:
            return True
        known_table = pa.table(known_values)
        return known_table.filter(split.partition_predicate.to_pyarrow()).num_rows > 0


@DeveloperAPI
@dataclass(frozen=True)
class OrcScanner(ArrowFileScanner):
    """Configure a file-level ORC scan through PyArrow Dataset."""

    synthesized_columns: Tuple[SynthesizedColumn, ...] = ()

    @override
    def prune_partitions(self, predicate: Expr) -> "OrcScanner":
        """Prune known path values and retain the filter for unknown paths."""
        # A root file may store the column or receive a logical null. Path
        # pruning cannot decide its rows, so the reader must also apply the
        # predicate, including when projection removes the filtered column.
        return replace(
            self,
            partition_predicate=combine_predicates(self.partition_predicate, predicate),
            predicate=combine_predicates(self.predicate, predicate),
        )

    @override
    def pushed_partition_pruner(self) -> Optional[FilePruner]:
        if self.partitioning is None or self.partition_predicate is None:
            return None
        return _OrcPartitionPredicatePruner(
            self.partitioning, self.partition_predicate, self.schema
        )

    @override
    def prune_input_split(self, input_split: FileManifest) -> FileManifest:
        """Use the same conservative path pruning as upstream file listing."""
        pruner = self.pushed_partition_pruner()
        if pruner is None:
            return input_split
        keep = [pruner.should_include(path) for path in input_split.paths]
        if all(keep):
            return input_split
        return FileManifest(
            input_split.as_block().filter(pa.array(keep, type=pa.bool_()))
        )

    def read_schema(self) -> pa.Schema:
        """Return the projected schema including synthesized columns."""
        synthesized_by_name = {
            column.name: column for column in self.synthesized_columns
        }

        if self.columns is None:
            schema = self.schema
            for column in self.synthesized_columns:
                field = pa.field(column.name, column.type)
                index = schema.get_field_index(column.name)
                if index == -1:
                    schema = schema.append(field)
                elif schema.field(index).type != column.type:
                    schema = schema.set(index, field)
            return schema

        fields = []
        for name in self.columns:
            column = synthesized_by_name.get(name)
            if column is not None:
                # The reader replaces any same-named on-disk field, so the
                # logical schema must advertise the synthesized type too.
                fields.append(pa.field(name, column.type))
                continue

            index = self.schema.get_field_index(name)
            assert index >= 0, f"Column {name} not found in schema"
            fields.append(self.schema.field(index))
        return pa.schema(fields)

    @override
    def push_filters(
        self, predicate: Expr
    ) -> Tuple["ArrowFileScanner", Optional[Expr]]:
        """Keep predicates on post-read synthesized columns above the read."""
        if self.partitioning is not None:
            # These columns are generated by the base reader after the fragment
            # hook. Keep their predicates above the read, including mixed ORs.
            synthesized = {column.name for column in self.synthesized_columns}
            split = _split_predicate_by_columns(predicate, synthesized)
            residual = combine_predicates(
                split.partition_predicate, split.residual_predicate
            )
            if split.data_predicate is None:
                return self, residual
            scanner, _ = super().push_filters(split.data_predicate)
            return scanner, residual
        return super().push_filters(predicate)

    def create_reader(self) -> OrcFileReader:
        """Create a reader with this scanner's projection and filters."""
        batch_size = (
            self.batch_size
            if self.batch_size is not None
            else _ARROW_DEFAULT_BATCH_SIZE
        )
        return OrcFileReader(
            format=FileFormat.ORC,
            batch_size=batch_size,
            columns=list(self.columns) if self.columns is not None else None,
            predicate=self.predicate,
            limit=self.limit,
            filesystem=self.filesystem,
            partitioning=self.partitioning,
            ignore_prefixes=self.ignore_prefixes,
            synthesized_columns=self.synthesized_columns,
            schema=self.schema,
        )
