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
