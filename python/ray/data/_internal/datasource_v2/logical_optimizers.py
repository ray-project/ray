from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional, Set, Tuple

from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
from ray.data.expressions import BinaryExpr, Expr, Operation
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.data._internal.datasource_v2.listing.file_pruners import FilePruner
    from ray.data._internal.datasource_v2.scanners.scanner import Scanner


@DeveloperAPI
class SupportsFilterPushdown(ABC):
    """Mixin for scanners that support filter/predicate pushdown.

    Filter pushdown allows predicates to be evaluated at the data source level,
    reducing the amount of data that needs to be read and transferred.
    """

    @abstractmethod
    def push_filters(self, predicate: "Expr") -> Tuple["Scanner", Optional["Expr"]]:
        """Push a filter predicate down to the scanner.

        Args:
            predicate: Expression representing the filter condition.

        Returns:
            Tuple of (new_scanner, residual_predicate) where:
            - new_scanner: New Scanner instance with the filter applied
            - residual_predicate: Any part of the predicate that couldn't be
              pushed down and must be applied post-scan. None if fully pushed.
        """
        ...

    def pushed_predicate(self) -> Optional["Expr"]:
        """The predicate this scanner will apply at read time, if any.

        This is the accepted result of :meth:`push_filters`, not the predicate
        that was offered to it. Planning derives upstream listing-time pruning
        from this value, so it must never be stronger than what the scanner
        actually evaluates -- returning ``None`` is always safe, returning a
        predicate the reader does not apply drops rows.

        Deliberately concrete rather than abstract: the safe answer is ``None``,
        and defaulting to it means an existing scanner keeps working (just
        without listing-time pruning) instead of failing to instantiate.
        """
        return None


@DeveloperAPI
class SupportsColumnPruning(ABC):
    """Mixin for scanners that support column pruning/projection pushdown.

    Column pruning allows reading only the columns needed by the query,
    which is especially beneficial for columnar formats like Parquet.
    """

    @abstractmethod
    def prune_columns(self, columns: List[str]) -> "Scanner":
        """Prune the scanner to only read the specified columns.

        Args:
            columns: List of column names to read.

        Returns:
            New Scanner instance configured to read only the specified columns.
        """
        ...

    @abstractmethod
    def pruned_column_names(self) -> Optional[Tuple[str, ...]]:
        """Physical column names selected after pruning, if any.

        Returns:
            ``None`` when no pruning has been applied (read all columns).
            A tuple (possibly empty) after :meth:`prune_columns` has been
            applied, listing on-disk / reader column names in read order.
        """
        ...


@DeveloperAPI
class SupportsLimitPushdown(ABC):
    """Mixin for scanners that support limit pushdown.

    Limit pushdown allows the scanner to stop early once the required number
    of rows has been read.
    """

    @abstractmethod
    def push_limit(self, limit: int) -> "Scanner":
        """Push a row limit down to the scanner.

        Args:
            limit: Maximum number of rows to read.

        Returns:
            New Scanner instance with the limit applied.
        """
        ...

    def pushed_limit(self) -> Optional[int]:
        """The row limit this scanner will stop at, if any.

        This is the accepted result of :meth:`push_limit`. Planning derives
        early-stop listing from it, so it must never be smaller than the limit
        the scanner actually honors.

        Concrete rather than abstract, for the same reason as
        :meth:`SupportsFilterPushdown.pushed_predicate`.
        """
        return None


@DeveloperAPI
class SupportsPartitionPruning(ABC):
    """Mixin for scanners that support partition pruning.

    Partition pruning allows skipping entire files/partitions based on
    predicates that reference partition columns.
    """

    @property
    @abstractmethod
    def partition_columns(self) -> Set[str]:
        """Names of columns that are partition keys.

        Callers (e.g. the predicate-pushdown rule) use this to decide
        whether a predicate should be routed through :meth:`push_filters`
        (data columns) or :meth:`prune_partitions` (partition columns).
        Must be fully populated by schema inference at planning time.
        """
        ...

    @abstractmethod
    def prune_partitions(self, predicate: "Expr") -> "Scanner":
        """Prune partitions based on a predicate.

        The scanner determines its partition columns from its
        ``Partitioning`` configuration, which is fully populated
        by schema inference at planning time.

        Args:
            predicate: Expression to evaluate against partition values.

        Returns:
            New Scanner instance with partition pruning applied.
        """
        ...

    def pushed_partition_predicate(self) -> Optional["Expr"]:
        """The partition predicate this scanner will apply at read time, if any.

        Concrete rather than abstract, like
        :meth:`SupportsFilterPushdown.pushed_predicate`.
        """
        return None

    def pushed_partition_pruner(self) -> Optional["FilePruner"]:
        """A listing-time pruner equivalent to this scanner's partition predicate.

        ``None`` means listing cannot reproduce the pruning, and planning
        drops the limit instead.
        """
        return None


@dataclass(frozen=True, eq=False)
class ListFilesPushdown:
    """Constraints a ``ListFiles`` may safely apply while listing.

    Each field is ``None`` unless the scanner consuming the listing both
    implements the corresponding ``Supports*`` mixin and reports state it
    actually accepted, so a datasource that ignores a pushdown can never cause
    listing-time pruning.

    ``eq=False`` on purpose: a generated ``__eq__`` would compare ``predicate``
    with ``==``, and ``Expr.__eq__`` builds an expression rather than answering
    a bool -- two instances holding different predicates would compare equal.
    Callers compare fields themselves, identity for the expression ones.
    """

    predicate: Optional["Expr"] = None
    projected_columns: Optional[List[str]] = None
    limit: Optional[int] = None
    partition_pruner: Optional["FilePruner"] = None


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
            # ``ArrowFileScanner`` reports none without a partitioning spec --
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
