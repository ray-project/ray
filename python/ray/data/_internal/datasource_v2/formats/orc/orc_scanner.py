from dataclasses import dataclass, field, replace
from typing import Optional, Tuple

import pyarrow as pa
from typing_extensions import override

from ray.data._internal.datasource_v2.common.arrow_file_scanner import (
    ArrowFileScanner,
)
from ray.data._internal.datasource_v2.common.file_pruners import (
    PartitionPredicatePruner,
)
from ray.data._internal.datasource_v2.common.file_reader import FileFormat
from ray.data._internal.datasource_v2.common.pushdown_utils import (
    _split_predicate_by_columns,
    combine_predicates,
)
from ray.data._internal.datasource_v2.formats.orc.orc_file_reader import (
    OrcFileReader,
    validate_statistics_pruning,
)
from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
from ray.data.context import DataContext
from ray.data.datasource.partitioning import Partitioning
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI


class _OrcPartitionPredicatePruner(PartitionPredicatePruner):
    """Keep paths whose missing keys require evaluation by the ORC reader."""

    def __init__(
        self,
        partitioning: Partitioning,
        predicate: Expr,
        *,
        schema: Optional[pa.Schema] = None,
    ):
        super().__init__(partitioning, predicate, schema=schema)
        self._predicate_columns = set(get_column_references(predicate))

    @override
    def should_include(self, path: str) -> bool:
        partitions = self._parser(path)
        if not self._predicate_columns.issubset(partitions):
            return True
        return super().should_include(path)


@DeveloperAPI
@dataclass(frozen=True)
class OrcScanner(ArrowFileScanner):
    """Configure ORC reads through PyArrow Dataset and :class:`OrcFileReader`.

    Adds ORC-specific schema, partition-pruning, and synthesized-column
    handling while reusing the shared ``ArrowFileScanner`` pushdowns.
    """

    synthesized_columns: Tuple[SynthesizedColumn, ...] = ()
    target_block_size: Optional[int] = None
    statistics_pruning: str = field(
        default_factory=lambda: DataContext.get_current().orc_statistics_pruning
    )

    def __post_init__(self):
        validate_statistics_pruning(self.statistics_pruning)

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
    def _create_partition_pruner(self) -> Optional[FilePruner]:
        if self.partitioning is None or self.partition_predicate is None:
            return None
        return _OrcPartitionPredicatePruner(
            self.partitioning, self.partition_predicate, schema=self.schema
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
            # Synthesized columns are added after scanning, so their filters
            # stay above the read.
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
        return OrcFileReader(
            format=FileFormat.ORC,
            batch_size=self.batch_size,
            columns=list(self.columns) if self.columns is not None else None,
            predicate=self.predicate,
            limit=self.limit,
            filesystem=self.filesystem,
            partitioning=self.partitioning,
            ignore_prefixes=self.ignore_prefixes,
            synthesized_columns=self.synthesized_columns,
            schema=self.schema,
            target_block_size=self.target_block_size,
            statistics_pruning=self.statistics_pruning,
        )
