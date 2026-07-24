from abc import ABC, abstractmethod
from dataclasses import replace
from typing import TYPE_CHECKING, List, Optional, Set, Tuple

from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.data._internal.datasource_v2.scanners.scanner import Scanner
    from ray.data._internal.logical.interfaces import LogicalOperator


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


def sync_list_files_pushdown(read_files: "LogicalOperator") -> "LogicalOperator":
    """Mirror a ``ReadFiles`` scanner's pushed-down state onto its ``ListFiles``.

    Called by the predicate / projection / limit pushdown rules after they push
    onto the ``ReadFiles`` scanner. A metadata-aware indexer (e.g. the
    footer-based Parquet indexer) reads ``predicate`` / ``projected_columns`` /
    ``limit`` off ``ListFiles`` at planning time to prune row groups, size only
    projected columns, and stop listing early.

    Reads the pushed state straight from the scanner, so it is inherently gated
    on the datasource supporting each pushdown -- the scanner only carries state
    it actually accepted (via the ``Supports*`` mixins here). No-op unless
    ``read_files`` is a ``ReadFiles`` whose immediate input is a ``ListFiles``.
    """
    from ray.data._internal.logical.operators.read_operator import (
        ListFiles,
        ReadFiles,
    )

    if not isinstance(read_files, ReadFiles):
        return read_files
    if not read_files.input_dependencies:
        return read_files
    upstream = read_files.input_dependencies[0]
    if not isinstance(upstream, ListFiles):
        return read_files

    scanner = read_files.scanner
    predicate = (
        getattr(scanner, "predicate", None)
        if isinstance(scanner, SupportsFilterPushdown)
        else None
    )
    if isinstance(scanner, SupportsColumnPruning):
        pruned = scanner.pruned_column_names()
        projected_columns = list(pruned) if pruned is not None else None
    else:
        projected_columns = None
    limit = (
        getattr(scanner, "limit", None)
        if isinstance(scanner, SupportsLimitPushdown)
        else None
    )

    new_list_files = replace(
        upstream,
        predicate=predicate,
        projected_columns=projected_columns,
        limit=limit,
    )
    return replace(read_files, input_dependencies=[new_list_files])
