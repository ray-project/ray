"""Iceberg helpers shared by the V2 Iceberg scanner and by V1.

Three pieces, all stateless:

- Filter translation. ``_IcebergExpressionVisitor`` turns a Ray ``Expr`` into a
  PyIceberg ``BooleanExpression`` by mapping each Ray operation to its Iceberg
  class (``RAY_DATA_OPERATION_TO_ICEBERG``), and raises ``ValueError`` or
  ``TypeError`` for anything Iceberg can't express. V1's datasource and datasink
  use it; V2 doesn't push filters yet.
- ``_get_empty_projection_schema``, the stub field a read projects when it needs
  no columns, because PyIceberg returns zero rows for an empty projection.
- ``_estimate_inmemory_file_size``, a data file's decoded size for the columns
  read, from the per-column sizes Iceberg's manifests record.

The visitor, the stub schema and the size estimate used to live in the legacy
``ray.data._internal.datasource.iceberg_datasource`` module. They moved here so
that ``datasource_v2`` doesn't depend on the legacy package; the legacy
datasource and datasink import them from here instead.
"""

from typing import TYPE_CHECKING, Callable, Dict, Optional, Set

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.formats.parquet.parquet_size_estimator import (
    PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
)
from ray.data._internal.planner.plan_expression.expression_visitors import _ExprVisitor
from ray.data.expressions import (
    AliasExpr,
    BinaryExpr,
    ColumnExpr,
    DownloadExpr,
    LiteralExpr,
    MonotonicallyIncreasingIdExpr,
    Operation,
    RandomExpr,
    StarExpr,
    UDFExpr,
    UnaryExpr,
    UUIDExpr,
)
from ray.util import log_once

try:
    from pyiceberg.expressions import (
        And,
        EqualTo,
        GreaterThan,
        GreaterThanOrEqual,
        In,
        IsNull,
        LessThan,
        LessThanOrEqual,
        Literal,
        Not,
        NotEqualTo,
        NotIn,
        NotNull,
        Or,
        Reference,
        UnboundTerm,
        literal,
    )

    # Typed as a callable: the classes take different arguments.
    RAY_DATA_OPERATION_TO_ICEBERG: Dict[
        Operation, Callable[..., "BooleanExpression"]
    ] = {
        Operation.EQ: EqualTo,
        Operation.NE: NotEqualTo,
        Operation.GT: GreaterThan,
        Operation.GE: GreaterThanOrEqual,
        Operation.LT: LessThan,
        Operation.LE: LessThanOrEqual,
        Operation.AND: And,
        Operation.OR: Or,
        Operation.IN: In,
        Operation.NOT_IN: NotIn,
        Operation.IS_NULL: IsNull,
        Operation.IS_NOT_NULL: NotNull,
        Operation.NOT: Not,
    }
except ImportError:
    log_once("pyiceberg.expressions not found. Please install pyiceberg >= 0.9.0")

if TYPE_CHECKING:
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.manifest import DataFile
    from pyiceberg.schema import Schema


# Field ID for the fabricated stub field, see ``_get_empty_projection_schema``.
# Iceberg matches columns by ID, so this must not be a real column's: colliding
# with a non-boolean column raises ``ResolveError``, and with a boolean column
# reads that column's data instead (the row count is still right, but the read
# is no longer free). Catalogs assign IDs sequentially from 1, so a value this
# large will not collide; Iceberg reserves 2147483546 and above for metadata
# columns, so stay below that.
_STUB_FIELD_ID = 2147483000


def _get_empty_projection_schema() -> "Schema":
    """Return a projected schema that reads no data but keeps the row count.

    PyIceberg rebuilds its output to match the projected schema, and a table
    rebuilt from zero fields reports zero rows however many the scan matched --
    so an empty projection, which ``Dataset.count()`` legitimately requests,
    would silently count zero. Project a single optional field that no data file
    has instead: Iceberg fills a missing optional field with nulls, the same way
    it reads a file written before a column was added, so the row count arrives
    in a column that costs nothing to read.

    The field is named after Ray's stub column so the resulting block matches
    what every other reader produces for an empty projection. ``boolean`` is
    used because Iceberg gained a null-valued type only after 0.9.0; the V1 and
    V2 readers retype the column to ``null`` on the way out. V1 calls this too.
    """
    from pyiceberg.schema import Schema
    from pyiceberg.types import BooleanType, NestedField

    return Schema(
        NestedField(
            field_id=_STUB_FIELD_ID,
            name=_BATCH_SIZE_PRESERVING_STUB_COL_NAME,
            # pyrefly: ignore[missing-argument]  # pydantic fills ``root``.
            field_type=BooleanType(),
            required=False,
        )
    )


def _estimate_inmemory_file_size(
    data_file: "DataFile", projected_field_ids: Optional[Set[int]]
) -> int:
    """Estimate a data file's decoded size for the projected columns.

    Iceberg's column sizes are compressed on-disk bytes. For Parquet, use them
    only when they cover every projected top-level field, then apply Ray's
    established Parquet decoding-ratio default. For other formats, retain the
    existing whole-file estimate until Ray has a format-specific decode ratio.
    """
    if projected_field_ids == set():
        return 0

    file_format = getattr(data_file.file_format, "name", data_file.file_format)
    if file_format != "PARQUET":
        return data_file.file_size_in_bytes

    column_sizes = data_file.column_sizes
    if (
        projected_field_ids is not None
        and column_sizes is not None
        and projected_field_ids.issubset(column_sizes)
    ):
        on_disk_size = sum(column_sizes[field_id] for field_id in projected_field_ids)
    else:
        on_disk_size = data_file.file_size_in_bytes

    return int(on_disk_size * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT)


class _IcebergExpressionVisitor(
    _ExprVisitor["BooleanExpression | UnboundTerm | Literal"]
):
    """Visitor that converts Ray Data expressions to PyIceberg expressions.

    This enables Ray Data users to write filters using the familiar col() syntax
    while leveraging Iceberg's native filtering capabilities. V1's datasource
    and datasink use it too.

    Example:
        >>> from ray.data.expressions import col
        >>> ray_expr = (col("date") >= "2024-01-01") & (col("status") == "active")
        >>> iceberg_expr = _IcebergExpressionVisitor().visit(ray_expr)
        >>> # iceberg_expr can now be used with PyIceberg's filter APIs
    """

    def visit_column(self, expr: "ColumnExpr") -> "UnboundTerm":
        """Convert a column reference to an Iceberg reference."""
        return Reference(expr.name)

    def visit_literal(self, expr: "LiteralExpr") -> "Literal":
        """Convert a literal value to an Iceberg literal."""
        return literal(expr.value)

    def visit_binary(self, expr: "BinaryExpr") -> "BooleanExpression":
        """Convert a binary operation to an Iceberg expression."""
        # Handle IN/NOT_IN specially since they don't visit the right operand
        # (the right operand is a list literal that can't be converted)
        if expr.op in (Operation.IN, Operation.NOT_IN):
            left = self.visit(expr.left)
            if not isinstance(expr.right, LiteralExpr):
                raise ValueError(
                    f"{expr.op.name} operation requires right operand to be a literal list, "
                    f"got {type(expr.right).__name__}"
                )
            return RAY_DATA_OPERATION_TO_ICEBERG[expr.op](left, expr.right.value)

        # For all other operations, visit both operands
        left = self.visit(expr.left)
        right = self.visit(expr.right)

        if expr.op in RAY_DATA_OPERATION_TO_ICEBERG:
            return RAY_DATA_OPERATION_TO_ICEBERG[expr.op](left, right)
        else:
            # Arithmetic operations are not supported in filter expressions
            raise ValueError(
                f"Unsupported binary operation for Iceberg filters: {expr.op}. "
                f"Iceberg filters support: {RAY_DATA_OPERATION_TO_ICEBERG.keys()}. "
                f"Arithmetic operations (ADD, SUB, MUL, DIV) cannot be used in filters."
            )

    def visit_unary(self, expr: "UnaryExpr") -> "BooleanExpression":
        """Convert a unary operation to an Iceberg expression."""
        operand = self.visit(expr.operand)

        if expr.op in RAY_DATA_OPERATION_TO_ICEBERG:
            return RAY_DATA_OPERATION_TO_ICEBERG[expr.op](operand)
        else:
            raise ValueError(
                f"Unsupported unary operation for Iceberg: {expr.op}. "
                f"Supported operations: {RAY_DATA_OPERATION_TO_ICEBERG.keys()}"
            )

    def visit_alias(
        self, expr: "AliasExpr"
    ) -> "BooleanExpression | UnboundTerm | Literal":
        """Convert an aliased expression (just unwrap the alias)."""
        return self.visit(expr.expr)

    def visit_udf(self, expr: "UDFExpr") -> "BooleanExpression | UnboundTerm | Literal":
        """UDF expressions cannot be converted to Iceberg expressions."""
        raise TypeError(
            "UDF expressions cannot be converted to Iceberg expressions. "
            "Iceberg filters must use simple column comparisons and boolean operations."
        )

    def visit_download(
        self, expr: "DownloadExpr"
    ) -> "BooleanExpression | UnboundTerm | Literal":
        """Download expressions cannot be converted to Iceberg expressions."""
        raise TypeError(
            "Download expressions cannot be converted to Iceberg expressions."
        )

    def visit_star(
        self, expr: "StarExpr"
    ) -> "BooleanExpression | UnboundTerm | Literal":
        """Star expressions cannot be converted to Iceberg expressions."""
        raise TypeError(
            "Star expressions cannot be converted to Iceberg filter expressions."
        )

    def visit_monotonically_increasing_id(
        self, expr: "MonotonicallyIncreasingIdExpr"
    ) -> "BooleanExpression | UnboundTerm | Literal":
        """Monotonically increasing ID expressions cannot be converted to Iceberg expressions."""
        raise TypeError(
            "monotonically_increasing_id expressions cannot be converted to Iceberg filter expressions."
        )

    def visit_random(
        self, expr: "RandomExpr"
    ) -> "BooleanExpression | UnboundTerm | Literal":
        """Random expressions cannot be converted to Iceberg expressions."""
        raise TypeError(
            "Random expressions cannot be converted to Iceberg filter expressions."
        )

    def visit_uuid(
        self, expr: "UUIDExpr"
    ) -> "BooleanExpression | UnboundTerm | Literal":
        """UUID expressions cannot be converted to Iceberg expressions."""
        raise TypeError(
            "UUID expressions cannot be converted to Iceberg filter expressions."
        )
