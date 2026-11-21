"""``Scanner`` for Iceberg tables: the read state every read task gets.

``IcebergScanner`` is a frozen dataclass that travels to the read tasks. It
holds what PyIceberg's ``ArrowScan`` needs (table metadata, ``FileIO``,
projected schema, row filter) and creates one ``IcebergFileReader`` per task.

It takes column pruning, the limit and filters: pruned columns are reprojected
in the order asked for, the limit is handed to the reader, and a filter is
split at its ``&`` (``iceberg_filter_pushdown.split_predicate``). Parts Iceberg
can evaluate go into ``ArrowScan``'s row filter; float parts whose NaN rows
PyArrow's statistics could drop are applied by the reader after the read, so
their columns are read too; the rest stay in a ``Filter`` above the read.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, List, Optional, Tuple

import pyarrow as pa
from typing_extensions import override

from ray.data._internal.datasource_v2.common.pushdown_utils import (
    combine_predicate_chain,
    combine_predicates,
)
from ray.data._internal.datasource_v2.formats.iceberg.iceberg_file_reader import (
    IcebergFileReader,
)
from ray.data._internal.datasource_v2.formats.iceberg.iceberg_filter_pushdown import (
    split_predicate,
    to_iceberg_filter,
)
from ray.data._internal.datasource_v2.formats.iceberg.iceberg_utils import (
    _get_empty_projection_schema,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.pushdown import (
    SupportsColumnPruning,
    SupportsFilterPushdown,
    SupportsLimitPushdown,
)
from ray.data._internal.datasource_v2.interfaces.scanner import Scanner
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
from ray.data.expressions import Expr, Operation
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.io import FileIO
    from pyiceberg.schema import Schema
    from pyiceberg.table.metadata import TableMetadata


@DeveloperAPI
@dataclass(frozen=True)
class IcebergScanner(
    Scanner[FileManifest],
    SupportsFilterPushdown,
    SupportsColumnPruning,
    SupportsLimitPushdown,
):
    """What every read task needs to decode an Iceberg table's files.

    ``columns`` and ``limit`` come from the pushdown mixins.
    """

    # Arrow form of ``projected_schema``, before any column pruning.
    schema: pa.Schema
    projected_schema: "Schema"
    table_metadata: "TableMetadata"
    io: "FileIO"
    # ``read_iceberg``'s parsed ``row_filter`` argument.
    row_filter: "BooleanExpression"
    case_sensitive: bool = True
    # The accepted Ray predicate: ``scan`` and ``after_read`` parts only.
    predicate: Optional[Expr] = None

    def _pruned_schema(self) -> "Schema":
        from pyiceberg.schema import Schema

        if self.columns is None:
            return self.projected_schema
        pruned = self.projected_schema.select(
            *self.columns, case_sensitive=self.case_sensitive
        )
        # Keep the requested order; ``Schema.select`` uses schema order.
        return Schema(
            *(
                pruned.find_field(name, case_sensitive=self.case_sensitive)
                for name in self.columns
            ),
            schema_id=pruned.schema_id,
        )

    def read_schema(self) -> pa.Schema:
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        if self.columns is None:
            return self.schema
        # The schema the reader projects to, so the column order matches.
        return schema_to_pyarrow(self._pruned_schema(), include_field_ids=False)

    @override
    def metadata_row_count_is_exact(self) -> bool:
        """A row filter is fine here; a limit is not.

        Iceberg plans a *residual* per file: the part of the filter that the
        file's partition values do not already satisfy. Where the residual is
        ``AlwaysTrue`` every row of the file passes, so the file's recorded
        ``record_count`` is the post-filter count, and a filter on a partition
        column makes that true of every file the scan lists.
        ``IcebergFileReader.read_metadata`` checks the residual per file and
        decodes the ones that need it, so the answer is exact either way.

        A limit is different: it is applied while reading, and nothing in the
        metadata says which files it will stop at.

        Column pruning changes a block's width, never its height. Delete files
        are not visible here at all -- they are a property of a file, not of
        the scan -- and are likewise handled per file in ``read_metadata``.

        Returns:
            ``True`` unless a limit has been pushed into this scanner.
        """
        return self.limit is None

    @override
    def push_filters(self, predicate: Expr) -> Tuple["IcebergScanner", Optional[Expr]]:
        parts = split_predicate(
            predicate,
            schema=self.table_metadata.schema(),
            case_sensitive=self.case_sensitive,
        )
        accepted = combine_predicate_chain(parts.scan + parts.after_read, Operation.AND)
        residual = combine_predicate_chain(parts.residual, Operation.AND)
        if accepted is None:
            return self, residual
        pushed = combine_predicates(self.predicate, accepted)
        return replace(self, predicate=pushed), residual

    @override
    def pushed_predicate(self) -> Optional[Expr]:
        return self.predicate

    def create_reader(self) -> IcebergFileReader:
        pruned = self._pruned_schema()
        output_columns = [field.name for field in pruned.fields]
        row_filter = self.row_filter
        after_read = None
        if self.predicate is not None:
            schema = self.table_metadata.schema()
            parts = split_predicate(
                self.predicate, schema=schema, case_sensitive=self.case_sensitive
            )
            row_filter = to_iceberg_filter(
                self.row_filter,
                parts.scan,
                schema=schema,
                case_sensitive=self.case_sensitive,
            )
            after_read = combine_predicate_chain(parts.after_read, Operation.AND)
        read_schema = pruned
        if after_read is not None:
            # Read the columns the after-read filter needs too.
            read_schema = self._with_columns(pruned, get_column_references(after_read))
        # The reader drops columns read only for the after-read filter.
        drop_filter_columns = read_schema is not pruned
        # PyIceberg returns zero rows for an empty projection (``count()``), so
        # project V1's stub field: no file has it, so it reads as one null per row.
        row_count_only = not output_columns
        if len(read_schema.fields) == 0:
            read_schema = _get_empty_projection_schema()
        return IcebergFileReader(
            table_metadata=self.table_metadata,
            io=self.io,
            projected_schema=read_schema,
            row_filter=row_filter,
            case_sensitive=self.case_sensitive,
            limit=self.limit,
            drop_all_columns=row_count_only,
            after_read_filter=after_read,
            output_columns=output_columns if drop_filter_columns else None,
        )

    def _with_columns(self, pruned: "Schema", names: List[str]) -> "Schema":
        from pyiceberg.schema import Schema

        have = {field.name for field in pruned.fields}
        extra = [
            self.projected_schema.find_field(name, case_sensitive=self.case_sensitive)
            for name in dict.fromkeys(names)
            if name not in have
        ]
        if not extra:
            return pruned
        return Schema(*pruned.fields, *extra, schema_id=pruned.schema_id)
