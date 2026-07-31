from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Optional, Set, Tuple

from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
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

    @property
    def enforces_partition_predicate(self) -> bool:
        """Whether :meth:`prune_partitions` guarantees the predicate is applied.

        Scanners that evaluate partition predicates by parsing file paths
        return ``True``: every row they emit has already been checked.

        A scanner returns ``False`` to accept the predicate as a *pruning
        hint only* -- it may use it to skip work, but does not promise that
        every surviving row satisfies it. The optimizer then keeps a
        ``Filter`` above the read, so correctness never rests on the hint.
        """
        return True

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


def derive_list_files_pushdown(
    scanner: Optional["Scanner"],
) -> Tuple[Optional["Expr"], Optional[List[str]], Optional[int]]:
    """Read the pushed-down state a scanner accepted, for upstream listing.

    Returns ``(predicate, projected_columns, limit)`` -- the constraints a
    ``ListFiles`` feeding this scanner's ``ReadFiles`` may safely apply while
    listing (see :class:`~ray.data._internal.logical.rules.
    derive_list_files_pushdown.DeriveListFilesPushdown`). Each element is
    ``None`` unless the scanner both implements the corresponding ``Supports*``
    mixin and reports state it actually accepted, so a datasource that ignores
    a pushdown can never cause listing-time pruning.

    ``scanner`` may be ``None`` (no downstream reader), which yields all-``None``:
    nothing downstream applies these constraints, so listing must not either.
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
    return predicate, projected_columns, limit
