"""Native Delta Lake datasource with partition-level predicate pushdown.

Resolves the surviving parquet URIs from a Delta table by handing supported
partition predicates to ``DeltaTable.file_uris(partition_filters=...)``, then
delegates the actual read to ``ParquetDatasource``. Mirrors Spark's
``TahoeFileIndex.matchingFiles(partitionFilters, dataFilters)`` shape using
delta-rs Python primitives.

Stats-based / non-partition file skipping (delta-io/delta-rs#3014) is out of
scope for this iteration -- non-partition predicates fall through to PyArrow
row-group/scanner-level pushdown via the inner ParquetDatasource.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

from ray.data._internal.datasource.parquet_datasource import (
    ParquetDatasource,
    _split_predicate_by_columns,
)
from ray.data._internal.util import _check_import
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.data.expressions import (
    BinaryExpr,
    ColumnExpr,
    Expr,
    LiteralExpr,
    Operation,
)

if TYPE_CHECKING:
    import pyarrow

    from deltalake import DeltaTable
    from ray.data.context import DataContext

logger = logging.getLogger(__name__)

# delta-rs DNF ops we can translate. Anything else -> NotImplementedError, and
# the caller (DeltaDatasource._split) drops the partition_filters and lets the
# Filter operator stay in the plan.
_DELTA_OP: Dict[Operation, str] = {
    Operation.EQ: "=",
    Operation.NE: "!=",
    Operation.IN: "in",
    Operation.NOT_IN: "not in",
}


class _DeltaPartitionFilterTranslator:
    """Convert a partition-only Ray Expr into delta-rs DNF.

    delta-rs ``DeltaTable.file_uris`` accepts a list of ``(col, op, value)``
    tuples interpreted as a conjunction (AND of all clauses). We therefore
    flatten AND-chains and translate each leaf comparison.

    The caller MUST have already verified the expression references only
    partition columns (via ``_split_predicate_by_columns``). Anything we can't
    translate (OR, NOT, GT/LT, UDF, non-literal RHS, ...) raises
    ``NotImplementedError`` so the caller can fall back to no pushdown.
    """

    def translate(self, expr: Expr) -> List[Tuple[str, str, Any]]:
        # Flatten AND chains -- everything else is a leaf to translate atomically.
        if isinstance(expr, BinaryExpr) and expr.op == Operation.AND:
            return self.translate(expr.left) + self.translate(expr.right)
        return [self._translate_atom(expr)]

    @staticmethod
    def _translate_atom(expr: Expr) -> Tuple[str, str, Any]:
        if not isinstance(expr, BinaryExpr) or expr.op not in _DELTA_OP:
            raise NotImplementedError(
                f"delta-rs DNF can't represent "
                f"{type(expr).__name__}({getattr(expr, 'op', None)})"
            )
        if not isinstance(expr.left, ColumnExpr):
            raise NotImplementedError(
                "Left-hand side of a Delta partition predicate must be a column"
            )
        if not isinstance(expr.right, LiteralExpr):
            raise NotImplementedError(
                "Right-hand side of a Delta partition predicate must be a literal"
            )

        op = _DELTA_OP[expr.op]
        value = expr.right.value
        # delta-rs requires string-typed partition values today (delta-rs#3597).
        if op in ("in", "not in"):
            value = [str(v) for v in value]
        else:
            value = str(value)
        return (expr.left.name, op, value)


class DeltaDatasource(Datasource):
    """Native Delta Lake datasource with partition-level predicate pushdown.

    See module docstring for design notes and references to Spark's
    DataSkippingReader / TahoeFileIndex equivalents.
    """

    def __init__(
        self,
        path: str,
        *,
        version: Optional[int] = None,
        storage_options: Optional[Dict[str, Any]] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        columns: Optional[List[str]] = None,
        shuffle: Union[str, None] = None,
        include_paths: bool = False,
        arrow_parquet_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__()
        _check_import(self, module="deltalake", package="deltalake")

        if not isinstance(path, str):
            raise ValueError("Only a single Delta Lake table path is supported.")

        self._path = path
        self._version = version
        self._storage_options = storage_options
        self._filesystem = filesystem
        self._columns = columns
        self._shuffle = shuffle
        self._include_paths = include_paths
        self._arrow_parquet_args = dict(arrow_parquet_args or {})
        # Lazy: opened on first property access. Resetting on clone keeps the
        # PyO3 handle from being shared across apply_predicate copies.
        self._delta_table: Optional["DeltaTable"] = None

    # ------------------------------------------------------------------
    # Delta-table accessors
    # ------------------------------------------------------------------

    @property
    def delta_table(self) -> "DeltaTable":
        if self._delta_table is None:
            from deltalake import DeltaTable

            self._delta_table = DeltaTable(
                self._path,
                version=self._version,
                storage_options=self._storage_options,
            )
        return self._delta_table

    @property
    def partition_columns(self) -> List[str]:
        return list(self.delta_table.metadata().partition_columns)

    # ------------------------------------------------------------------
    # Pushdown contract
    # ------------------------------------------------------------------

    def supports_predicate_pushdown(self) -> bool:
        return True

    # apply_predicate is inherited from _DatasourcePredicatePushdownMixin
    # (clones self, AND-combines into self._predicate_expr). Tests A1/A2 in
    # test_delta_pushdown.py exercise this contract.

    def __copy__(self) -> "DeltaDatasource":
        # The default copy.copy would share the lazily-opened DeltaTable PyO3
        # handle across clones. Reset it on the clone so each copy re-opens its
        # own table the first time it's needed.
        new = self.__class__.__new__(self.__class__)
        new.__dict__.update(self.__dict__)
        new._delta_table = None
        return new

    # ------------------------------------------------------------------
    # Internal: split + resolve
    # ------------------------------------------------------------------

    def _split(
        self,
    ) -> Tuple[Optional[List[Tuple[str, str, Any]]], Optional[Expr]]:
        """Split ``self._predicate_expr`` into ``(partition_dnf, data_predicate)``.

        Returns ``(None, None)`` when no predicate has been applied. On any
        translation failure we degrade gracefully: the partition DNF goes back
        to ``None`` and the full predicate is left in ``data_predicate`` so the
        Filter operator (still in the plan) can handle it.
        """
        if self._predicate_expr is None:
            return None, None

        partition_cols = set(self.partition_columns)
        if not partition_cols:
            # Nothing to push at the Delta level; let downstream PyArrow scanner
            # handle the predicate via row-group pushdown.
            return None, self._predicate_expr

        split = _split_predicate_by_columns(self._predicate_expr, partition_cols)
        partition_dnf: Optional[List[Tuple[str, str, Any]]] = None
        data_predicate = split.data_predicate

        if split.partition_predicate is not None:
            try:
                partition_dnf = _DeltaPartitionFilterTranslator().translate(
                    split.partition_predicate
                )
            except NotImplementedError as exc:
                logger.debug(
                    "Delta partition pushdown fallback for %s: %s",
                    self._path,
                    exc,
                )
                partition_dnf = None
                # Leave the original (full) predicate to flow downstream so the
                # outer Filter operator handles correctness.
                data_predicate = self._predicate_expr

        return partition_dnf, data_predicate

    def _build_inner(self) -> ParquetDatasource:
        """Resolve files via Delta and construct a fresh inner ParquetDatasource.

        Called once per ``get_read_tasks`` / ``estimate_inmemory_data_size``
        invocation -- keeps the lifetime trivial and avoids stale-cache bugs
        when ``apply_predicate`` is called multiple times.
        """
        partition_dnf, data_predicate = self._split()

        # ---- The call that actually solves #61547 ----
        # Asks Delta which files survive the partition predicate, instead of
        # eagerly fanning out to every parquet file and post-filtering.
        uris = self.delta_table.file_uris(partition_filters=partition_dnf)

        inner = ParquetDatasource(
            paths=list(uris),
            # Use Delta's schema so PyArrow doesn't inspect parquet footers
            # (issue #61547 explicitly calls out "even just for tail metadata").
            schema=self.delta_table.schema().to_arrow(),
            columns=self._columns,
            filesystem=self._filesystem,
            # We've already pruned at the Delta level. Disable ParquetDatasource's
            # own Hive partition parsing to avoid a no-op second pass and to
            # keep partition columns resolved from the parquet files themselves
            # (Delta writes them into both the path AND the file).
            partitioning=None,
            shuffle=self._shuffle,
            include_paths=self._include_paths,
            **self._arrow_parquet_args,
        )

        if data_predicate is not None:
            inner = inner.apply_predicate(data_predicate)
        return inner

    # ------------------------------------------------------------------
    # Datasource API
    # ------------------------------------------------------------------

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List[ReadTask]:
        return self._build_inner().get_read_tasks(
            parallelism, per_task_row_limit, data_context
        )

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return self._build_inner().estimate_inmemory_data_size()
