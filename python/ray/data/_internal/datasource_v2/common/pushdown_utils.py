import functools
from dataclasses import dataclass
from typing import TYPE_CHECKING, AbstractSet, List, Optional, Set

import pyarrow as pa
import pyarrow.compute as pc

from ray.data._internal.datasource_v2.interfaces.pushdown import (
    ListFilesPushdown,
    SupportsColumnPruning,
    SupportsFilterPushdown,
    SupportsLimitPushdown,
    SupportsPartitionPruning,
)
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
from ray.data.expressions import (
    BinaryExpr,
    ColumnExpr,
    Expr,
    LiteralExpr,
    Operation,
    PyArrowComputeUDFExpr,
    UnaryExpr,
)

if TYPE_CHECKING:
    from ray.data._internal.datasource_v2.interfaces.scanner import Scanner


def derive_list_files_pushdown(
    scanner: Optional["Scanner"],
) -> ListFilesPushdown:
    """Read the pushed-down state a scanner accepted, for upstream listing.

    The returned :class:`ListFilesPushdown` holds the constraints a
    ``ListFiles`` feeding this scanner's ``ReadFiles`` may apply while listing
    (see :class:`~ray.data._internal.logical.rules.
    derive_list_files_pushdown.DeriveListFilesPushdown`).

    ``scanner`` may be ``None`` (no downstream reader), which yields an
    all-``None`` result: nothing downstream applies these constraints, so
    listing must not either.
    """
    predicate = (
        scanner.pushed_predicate()
        if isinstance(scanner, SupportsFilterPushdown)
        else None
    )
    if isinstance(scanner, SupportsColumnPruning):
        pruned = scanner.pruned_column_names()
        projected_columns = list(pruned) if pruned is not None else None
    else:
        projected_columns = None
    limit = (
        scanner.pushed_limit() if isinstance(scanner, SupportsLimitPushdown) else None
    )
    partition_pruner = None
    if (
        isinstance(scanner, SupportsPartitionPruning)
        and scanner.pushed_partition_predicate() is not None
    ):
        # The reader drops whole files on a partition predicate, but listing
        # prunes on file statistics and partition columns live in the path.
        # Give listing the same path-based pruning, so every row it counts
        # towards the limit is one the reader keeps.
        partition_pruner = scanner.pushed_partition_pruner()
        if partition_pruner is None:
            # Defensive: unreachable today. A scanner only accepts a partition
            # predicate if it reported partition columns, and
            # ``FileScanner`` reports none without a partitioning spec --
            # the only case where it cannot build a pruner. Kept for other
            # implementations (e.g. partition values from a catalog, not the
            # path), where listing still must not stop early: failing safe
            # costs a footer sweep, failing open loses rows.
            limit = None
    return ListFilesPushdown(
        predicate=predicate,
        projected_columns=projected_columns,
        limit=limit,
        partition_pruner=partition_pruner,
    )


# Predicate splitting ahead of pushdown.
#
# A predicate pushed into a file read has up to three destinations: conjuncts
# over data columns go to the file scanner, conjuncts over partition columns are
# evaluated from file paths, and anything that straddles both kinds has to stay
# in a ``Filter`` above the read. ``_split_predicate_by_columns`` performs that
# classification for the ``ReadFiles`` logical operator and for the legacy
# Parquet datasource.
def combine_predicates(left: Optional[Expr], right: Optional[Expr]) -> Optional[Expr]:
    """``AND`` two optional predicates; ``None`` means "nothing to apply".

    ``&`` builds a logical-``AND`` node (Python can't overload ``and``), and an
    ``AND`` chain is order-independent, so conjuncts of one chain can be
    recombined in any order.
    """
    if left is None and right is None:
        return None
    if left is None:
        return right
    if right is None:
        return left
    return left & right


def flatten_predicate_chain(predicate: Expr, op: Operation) -> List[Expr]:
    """Operands of ``predicate``'s top level ``op`` chain, left to right (iterative)."""
    operands: List[Expr] = []
    stack = [predicate]
    while stack:
        expr = stack.pop()
        if isinstance(expr, BinaryExpr) and expr.op == op:
            stack.append(expr.right)
            stack.append(expr.left)
        else:
            operands.append(expr)
    return operands


def combine_predicate_chain(predicates: List[Expr], op: Operation) -> Optional[Expr]:
    """Fold ``predicates`` into one ``op`` chain, left to right; ``None`` if empty."""
    if not predicates:
        return None
    return functools.reduce(lambda left, right: BinaryExpr(op, left, right), predicates)


def append_unique_predicate(predicates: List[Expr], predicate: Expr) -> None:
    """Append ``predicate`` unless a structurally equal one is already listed."""
    if not any(predicate.structurally_equals(existing) for existing in predicates):
        predicates.append(predicate)


# Comparisons that Parquet min/max statistics answer correctly for floats.
_STATISTICS_SAFE_FLOAT_COMPARISONS = frozenset(
    {Operation.LT, Operation.LE, Operation.GT, Operation.GE, Operation.EQ}
)


def float_column_names(schema: pa.Schema) -> Set[str]:
    """Names of the float columns in ``schema``, for :func:`is_statistics_safe`."""
    return {field.name for field in schema if pa.types.is_floating(field.type)}


def is_statistics_safe(predicate: Expr, float_columns: AbstractSet[str]) -> bool:
    """Whether Parquet row-group pruning on ``predicate`` keeps every matching row.

    Parquet min/max statistics skip NaN and do not tell -0.0 from 0.0, so PyArrow
    can drop matching rows when it prunes on a float column. Only ``<``, ``<=``,
    ``>``, ``>=``, ``==`` and null checks between a float column and a literal or
    column are safe, plus ``is_in`` with no NaN or zero in its list.

    TODO: Remove once PyArrow fixes NaN (apache/arrow#50807) and signed zero
    (apache/arrow#54915) in statistics.
    """
    if isinstance(predicate, BinaryExpr) and predicate.op in (
        Operation.AND,
        Operation.OR,
    ):
        return is_statistics_safe(
            predicate=predicate.left, float_columns=float_columns
        ) and is_statistics_safe(predicate=predicate.right, float_columns=float_columns)
    if float_columns.isdisjoint(get_column_references(predicate)):
        return True
    if isinstance(predicate, UnaryExpr):
        return _is_safe_float_unary(expr=predicate)
    if isinstance(predicate, BinaryExpr):
        return _is_safe_float_binary(expr=predicate)
    return False


def _is_safe_float_unary(expr: UnaryExpr) -> bool:
    """Only null checks are safe on a float column; ``~`` can match NaN."""
    return expr.op in (Operation.IS_NULL, Operation.IS_NOT_NULL) and isinstance(
        expr.operand, ColumnExpr
    )


def _is_safe_float_binary(expr: BinaryExpr) -> bool:
    """Comparisons between columns and literals, or ``is_in`` without NaN or zero."""
    if (
        expr.op == Operation.IN
        and isinstance(expr.left, ColumnExpr)
        and isinstance(expr.right, LiteralExpr)
    ):
        values = expr.right.value
        values = values if isinstance(values, list) else [values]
        # ``value == value`` is false only for NaN; ``value != 0`` also rejects -0.0.
        return all(value == value and value != 0 for value in values)
    return (
        expr.op in _STATISTICS_SAFE_FLOAT_COMPARISONS
        and isinstance(expr.left, (ColumnExpr, LiteralExpr))
        and isinstance(expr.right, (ColumnExpr, LiteralExpr))
    )


# Arithmetic operators whose result type PyArrow takes from the column.
_ARITHMETIC_OPERATIONS = frozenset(
    {
        Operation.ADD,
        Operation.SUB,
        Operation.MUL,
        Operation.DIV,
        Operation.MOD,
        Operation.FLOORDIV,
    }
)


def has_column_arithmetic(expr: Expr) -> bool:
    """Whether ``expr`` does arithmetic on a column.

    PyArrow evaluates a pushed filter's arithmetic in the column's type, so
    int8 ``x + 100`` wraps to -56, while Ray's row filter widens it first.
    ``power`` is the only compute method that takes a second operand.

    TODO: Remove once ``Expr.to_pyarrow()`` widens like Ray's row filter.
    """
    expr = expr._unalias()
    if isinstance(expr, BinaryExpr):
        if expr.op in _ARITHMETIC_OPERATIONS:
            return bool(get_column_references(expr))
        return has_column_arithmetic(expr=expr.left) or has_column_arithmetic(
            expr=expr.right
        )
    if isinstance(expr, UnaryExpr):
        return has_column_arithmetic(expr=expr.operand)
    if isinstance(expr, PyArrowComputeUDFExpr):
        # pyrefly: ignore[missing-attribute]  # PyArrow generates it at import time
        if expr.pc_func is pc.power:
            return bool(get_column_references(expr))
        return any(has_column_arithmetic(expr=arg) for arg in expr.args)
    return False


@dataclass
class _SplitPredicateResult:
    """Result of splitting a predicate by column type.

    Attributes:
        data_predicate: Conjuncts referencing only data columns (for PyArrow
            pushdown), or None if none could be extracted.
        partition_predicate: Conjuncts referencing only partition columns
            (for partition pruning), or None if none could be extracted.
        residual_predicate: Conjuncts that mix partition and data columns
            and can't be split safely (e.g. an ``OR`` straddling both
            kinds). The caller must keep these as a ``Filter`` above the
            read; dropping them would over-include rows.
    """

    data_predicate: Optional[Expr]
    partition_predicate: Optional[Expr]
    residual_predicate: Optional[Expr]


def _split_predicate_by_columns(
    predicate: Expr,
    partition_columns: set,
) -> _SplitPredicateResult:
    """Split a predicate into data, partition, and residual parts.

    This function walks the top-level ``AND`` chain and classifies each
    conjunct by the columns it references:

    - References only data columns (or none) → data bucket; pyarrow can
      evaluate it at scan time.
    - References only partition columns → partition bucket; the partition
      parser can evaluate it from file paths.
    - References both kinds (i.e. a non-``AND`` whose column set spans
      both) → residual bucket; semantics-preserving splitting is
      impossible (e.g. ``data > 5 OR partition == "US"``), so the caller
      must keep these as a ``Filter`` above the read.

    Args:
        predicate: The predicate expression to analyze.
        partition_columns: Set of partition column names.

    Returns:
        :class:`_SplitPredicateResult` with the three buckets. Combining
        ``data_predicate``, ``partition_predicate``, and
        ``residual_predicate`` with ``AND`` reproduces the original
        predicate exactly.

    Examples:
        >>> from ray.data.expressions import col
        >>> # Pure data predicate:
        >>> result = _split_predicate_by_columns(col("data1") > 5, {"partition_col"})
        >>> result.data_predicate is not None
        True
        >>> result.partition_predicate is None and result.residual_predicate is None
        True

        >>> # Pure partition predicate:
        >>> result = _split_predicate_by_columns(col("partition_col") == "US", {"partition_col"})
        >>> result.partition_predicate is not None
        True
        >>> result.data_predicate is None and result.residual_predicate is None
        True

        >>> # Mixed AND - can split into data and partition parts:
        >>> result = _split_predicate_by_columns(
        ...     (col("data1") > 5) & (col("partition_col") == "US"),
        ...     {"partition_col"}
        ... )
        >>> result.data_predicate is not None and result.partition_predicate is not None
        True
        >>> result.residual_predicate is None
        True

        >>> # Mixed OR - kept as residual; caller wraps it in a Filter above:
        >>> result = _split_predicate_by_columns(
        ...     (col("data1") > 5) | (col("partition_col") == "US"),
        ...     {"partition_col"}
        ... )
        >>> result.data_predicate is None and result.partition_predicate is None
        True
        >>> result.residual_predicate is not None
        True
    """
    data: List[Expr] = []
    partition: List[Expr] = []
    residual: List[Expr] = []
    for conjunct in flatten_predicate_chain(predicate, Operation.AND):
        referenced_cols = set(get_column_references(conjunct))
        if not (referenced_cols & partition_columns):
            # Pure data conjunct (or no column refs).
            data.append(conjunct)
        elif referenced_cols <= partition_columns:
            partition.append(conjunct)
        else:
            # Straddles both kinds (e.g. an ``OR``): must stay a Filter above.
            residual.append(conjunct)
    return _SplitPredicateResult(
        data_predicate=combine_predicate_chain(data, Operation.AND),
        partition_predicate=combine_predicate_chain(partition, Operation.AND),
        residual_predicate=combine_predicate_chain(residual, Operation.AND),
    )
