from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

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
from ray.data.expressions import BinaryExpr, Expr, Operation

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
    referenced_cols = set(get_column_references(predicate))
    data_cols = referenced_cols - partition_columns
    partition_cols_in_predicate = referenced_cols & partition_columns

    if not partition_cols_in_predicate:
        # Pure data predicate (or no column refs).
        return _SplitPredicateResult(
            data_predicate=predicate,
            partition_predicate=None,
            residual_predicate=None,
        )

    if not data_cols:
        # Pure partition predicate.
        return _SplitPredicateResult(
            data_predicate=None,
            partition_predicate=predicate,
            residual_predicate=None,
        )

    # Mixed predicate - keep splitting if it's an AND chain.
    if isinstance(predicate, BinaryExpr) and predicate.op == Operation.AND:
        left_result = _split_predicate_by_columns(predicate.left, partition_columns)
        right_result = _split_predicate_by_columns(predicate.right, partition_columns)

        return _SplitPredicateResult(
            data_predicate=combine_predicates(
                left_result.data_predicate, right_result.data_predicate
            ),
            partition_predicate=combine_predicates(
                left_result.partition_predicate, right_result.partition_predicate
            ),
            residual_predicate=combine_predicates(
                left_result.residual_predicate, right_result.residual_predicate
            ),
        )

    # ``OR``/``NOT``/etc. straddling both column kinds — not safely
    # splittable. Surface as residual so the caller doesn't silently drop
    # it (the prior version returned ``(None, None)`` here, which let the
    # surrounding ``AND`` chain push partial conjuncts and over-include
    # rows that should have been filtered by this one).
    return _SplitPredicateResult(
        data_predicate=None,
        partition_predicate=None,
        residual_predicate=predicate,
    )
