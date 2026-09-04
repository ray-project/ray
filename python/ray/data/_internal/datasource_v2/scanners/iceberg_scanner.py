"""``Scanner`` for Iceberg tables, plus the Ray-to-Iceberg filter translation."""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, List, Optional, Tuple

import pyarrow as pa
from typing_extensions import override

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.logical_optimizers import (
    SupportsColumnPruning,
    SupportsFilterPushdown,
    SupportsLimitPushdown,
)
from ray.data._internal.datasource_v2.readers.iceberg_file_reader import (
    IcebergFileReader,
)
from ray.data._internal.datasource_v2.scanners.scanner import Scanner
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

    Returns ``(filter, residual)``. ``residual`` is the whole ``predicate``
    when it has no Iceberg equivalent -- Iceberg's expression language has no
    arithmetic or UDFs, so ``col("x") * 2 > 10`` cannot be expressed at all --
    and ``None`` when the filter carries all of it. Translation is
    all-or-nothing rather than per-conjunct; a partly-translated filter would
    be the more precise answer, but the caller must apply the residual either
    way and this keeps the two sides trivially consistent.
    """
    from pyiceberg.expressions import AlwaysTrue, And

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
    """Everything a read task needs to decode an Iceberg table's files.

    Pickled to every read task, so it holds the table's metadata and IO handle
    rather than a catalog connection or a ``Table``. Partition pruning is
    deliberately not implemented (no ``SupportsPartitionPruning``): that mixin
    reads partition values out of file *paths*, whereas Iceberg's live in
    metadata and may be transformed (``day(ts)``, ``bucket(id, 16)``), and
    ``plan_files()`` has already pruned on them properly.
    """

    # Arrow form of ``projected_schema``, before any column pruning.
    schema: pa.Schema
    projected_schema: "Schema"
    table_metadata: "TableMetadata"
    io: "FileIO"
    # The scan's row filter: what ``read_iceberg(row_filter=...)`` was given,
    # ANDed with whatever the optimizer pushed. Always a real expression --
    # ``read_iceberg`` accepts a string, but ``ArrowScan`` requires a parsed
    # one, so parsing happens once on the driver.
    row_filter: "BooleanExpression"
    case_sensitive: bool = True
    limit: Optional[int] = None
    columns: Optional[Tuple[str, ...]] = None
    # The pushed predicate in Ray form, kept only so ``pushed_predicate`` can
    # report it. ``row_filter`` above is what the reader applies.
    predicate: Optional[Expr] = None

    def _pruned_schema(self) -> "Schema":
        if self.columns is None:
            return self.projected_schema
        return self.projected_schema.select(
            *self.columns, case_sensitive=self.case_sensitive
        )

    def read_schema(self) -> pa.Schema:
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        if self.columns is None:
            return self.schema
        # Derived from the same pruned Iceberg schema the reader projects to,
        # so declared and actual column order agree -- ``Schema.select`` keeps
        # schema order, not the order the columns were requested in.
        return schema_to_pyarrow(self._pruned_schema(), include_field_ids=False)

    def create_reader(self) -> IcebergFileReader:
        pruned = self._pruned_schema()
        # ``select_columns([])`` -- which is how ``count()`` asks for row counts
        # without data -- prunes to zero columns, but PyIceberg derives a
        # batch's length from the columns it decoded, so an empty projection
        # yields *zero-row* batches rather than zero-width ones. Decode one
        # cheap column instead and drop it after, which keeps the row count.
        row_count_only = (
            len(pruned.fields) == 0 and len(self.projected_schema.fields) > 0
        )
        if row_count_only:
            pruned = self.projected_schema.select(
                self._cheapest_field_name(), case_sensitive=self.case_sensitive
            )
        return IcebergFileReader(
            table_metadata=self.table_metadata,
            io=self.io,
            projected_schema=pruned,
            row_filter=self.row_filter,
            case_sensitive=self.case_sensitive,
            limit=self.limit,
            drop_all_columns=row_count_only,
        )

    def _cheapest_field_name(self) -> str:
        """Name of the column to decode when only the row count is wanted."""
        fields = self.projected_schema.fields
        for field in fields:
            if field.field_type.is_primitive:
                return field.name
        return fields[0].name

    @override
    def push_filters(self, predicate: Expr) -> Tuple["IcebergScanner", Optional[Expr]]:
        row_filter, residual = combine_row_filter(self.row_filter, predicate)
        if residual is not None:
            return self, residual
        pushed = predicate if self.predicate is None else self.predicate & predicate
        return replace(self, row_filter=row_filter, predicate=pushed), None

    @override
    def pushed_predicate(self) -> Optional[Expr]:
        return self.predicate

    @override
    def prune_columns(self, columns: List[str]) -> "IcebergScanner":
        return replace(self, columns=tuple(columns))

    @override
    def pruned_column_names(self) -> Optional[Tuple[str, ...]]:
        return self.columns

    @override
    def push_limit(self, limit: int) -> "IcebergScanner":
        # Applied per read unit, not globally -- the ``Limit`` operator above
        # the read is what enforces the total, so reading too many rows here is
        # only wasted work, never a wrong answer.
        combined = limit if self.limit is None else min(self.limit, limit)
        return replace(self, limit=combined)

    @override
    def pushed_limit(self) -> Optional[int]:
        return self.limit
