"""``Scanner`` for Iceberg tables, plus the Ray-to-Iceberg filter translation."""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Optional, Tuple

import pyarrow as pa
from typing_extensions import override

from ray.data._internal.datasource_v2.formats.iceberg.iceberg_file_reader import (
    IcebergFileReader,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.pushdown import (
    SupportsColumnPruning,
    SupportsFilterPushdown,
    SupportsLimitPushdown,
)
from ray.data._internal.datasource_v2.interfaces.scanner import Scanner
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.io import FileIO
    from pyiceberg.schema import Schema
    from pyiceberg.table.metadata import TableMetadata

logger = logging.getLogger(__name__)


def combine_row_filter(
    row_filter: "BooleanExpression", predicate: Optional[Expr]
) -> Tuple["BooleanExpression", Optional[Expr]]:
    """AND a Ray expression into an Iceberg filter.

    Args:
        row_filter: The scan's current Iceberg filter.
        predicate: The Ray expression to AND into it, or ``None``.

    Returns:
        ``(filter, residual)``. ``residual`` is the whole ``predicate`` if it
        has no Iceberg equivalent, e.g. ``col("x") * 2 > 10``, else ``None``.
    """
    from pyiceberg.expressions import AlwaysTrue, And, BooleanExpression

    from ray.data._internal.datasource.iceberg_datasource import (
        _IcebergExpressionVisitor,
    )

    if predicate is None:
        return row_filter, None
    try:
        translated = _IcebergExpressionVisitor().visit(predicate)
    except (ValueError, TypeError) as e:
        logger.debug("Not pushing %s into the Iceberg scan: %s", predicate, e)
        return row_filter, predicate
    if not isinstance(translated, BooleanExpression):
        # A bare column like ``col("is_valid")`` is a term, not a filter.
        return row_filter, predicate
    if isinstance(row_filter, AlwaysTrue):
        return translated, None
    return And(row_filter, translated), None


@DeveloperAPI
@dataclass(frozen=True)
class IcebergScanner(
    Scanner[FileManifest],
    SupportsFilterPushdown,
    SupportsColumnPruning,
    SupportsLimitPushdown,
):
    """What every read task needs to decode an Iceberg table's files.

    No ``SupportsPartitionPruning``: that reads partition values from paths,
    and ``plan_files()`` already prunes on Iceberg's own. ``columns`` and
    ``limit`` come from the pushdown mixins.
    """

    # Arrow form of ``projected_schema``, before any column pruning.
    schema: pa.Schema
    projected_schema: "Schema"
    table_metadata: "TableMetadata"
    io: "FileIO"
    # ``read_iceberg``'s parsed row filter, ANDed with any pushed predicate.
    row_filter: "BooleanExpression"
    case_sensitive: bool = True
    # The pushed predicate in Ray form, only for ``pushed_predicate``.
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

    def create_reader(self) -> IcebergFileReader:
        from ray.data._internal.datasource.iceberg_datasource import (
            _get_empty_projection_schema,
        )

        pruned = self._pruned_schema()
        # PyIceberg returns zero rows for an empty projection (``count()``), so
        # project V1's stub field: no file has it, so it reads as one null per row.
        row_count_only = len(pruned.fields) == 0
        if row_count_only:
            pruned = _get_empty_projection_schema()
        return IcebergFileReader(
            table_metadata=self.table_metadata,
            io=self.io,
            projected_schema=pruned,
            row_filter=self.row_filter,
            case_sensitive=self.case_sensitive,
            limit=self.limit,
            drop_all_columns=row_count_only,
        )

    @override
    def push_filters(self, predicate: Expr) -> Tuple["IcebergScanner", Optional[Expr]]:
        row_filter, residual = combine_row_filter(
            row_filter=self.row_filter, predicate=predicate
        )
        if residual is not None:
            return self, residual
        pushed = predicate if self.predicate is None else self.predicate & predicate
        return replace(self, row_filter=row_filter, predicate=pushed), None

    @override
    def pushed_predicate(self) -> Optional[Expr]:
        return self.predicate
