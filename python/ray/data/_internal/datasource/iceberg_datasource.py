"""
Module to read an iceberg table into a Ray Dataset, by using the Ray Datasource API.
"""

import heapq
import itertools
import logging
from functools import partial
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import pyarrow as pa
from packaging import version

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.planner.plan_expression.expression_visitors import _ExprVisitor
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.data.expressions import (
    AliasExpr,
    BinaryExpr,
    ColumnExpr,
    DownloadExpr,
    Expr,
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
from ray.util.annotations import DeveloperAPI

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

    RAY_DATA_OPERATION_TO_ICEBERG = {
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
    from pyiceberg.catalog import Catalog
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.io import FileIO
    from pyiceberg.manifest import DataFile
    from pyiceberg.schema import Schema
    from pyiceberg.table import DataScan, FileScanTask, Table
    from pyiceberg.table.metadata import TableMetadata

    from ray.data.context import DataContext

logger = logging.getLogger(__name__)

# Approximate width, in bytes, of the Iceberg primitive types that are cheap to
# read. Used to choose a stand-in column for an empty projection, see
# ``IcebergDatasource._get_sentinel_field``. Keys are Iceberg type names as
# returned by ``str(field_type)``. Types missing from this map -- strings,
# binary, decimals and nested types -- have unbounded width, and are only used
# as a sentinel if the table has nothing cheaper to offer.
_SENTINEL_FIELD_WIDTHS = {
    "boolean": 1,
    "int": 4,
    "float": 4,
    "date": 4,
    "long": 8,
    "double": 8,
    "time": 8,
    "timestamp": 8,
    "timestamptz": 8,
    "uuid": 16,
}
_UNKNOWN_FIELD_WIDTH = 1 << 20


class _IcebergExpressionVisitor(
    _ExprVisitor["BooleanExpression | UnboundTerm | Literal"]
):
    """
    Visitor that converts Ray Data expressions to PyIceberg expressions.

    This enables Ray Data users to write filters using the familiar col() syntax
    while leveraging Iceberg's native filtering capabilities.

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
    ) -> "BooleanExpression | UnboundTerm[Any] | Literal[Any]":
        """Random expressions cannot be converted to Iceberg expressions."""
        raise TypeError(
            "Random expressions cannot be converted to Iceberg filter expressions."
        )

    def visit_uuid(
        self, expr: "UUIDExpr"
    ) -> "BooleanExpression | UnboundTerm[Any] | Literal[Any]":
        """UUID expressions cannot be converted to Iceberg expressions."""
        raise TypeError(
            "UUID expressions cannot be converted to Iceberg filter expressions."
        )


def _get_read_task(
    tasks: Iterable["FileScanTask"],
    table_io: "FileIO",
    table_metadata: "TableMetadata",
    row_filter: "BooleanExpression",
    case_sensitive: bool,
    limit: Optional[int],
    schema: "Schema",
    drop_all_columns: bool = False,
) -> Iterable[Block]:
    # Determine the PyIceberg version to handle backward compatibility
    import pyiceberg

    def _generate_tables() -> Iterable[pa.Table]:
        if version.parse(pyiceberg.__version__) >= version.parse("0.9.0"):
            # Modern implementation using ArrowScan (PyIceberg 0.9.0+)
            from pyiceberg.io.pyarrow import ArrowScan

            # Initialize scanner with Iceberg metadata and query parameters
            scanner = ArrowScan(
                table_metadata=table_metadata,
                io=table_io,
                row_filter=row_filter,
                projected_schema=schema,
                case_sensitive=case_sensitive,
                limit=limit,
            )

            # Convert scanned data to Arrow Table format
            result_table = scanner.to_table(tasks=tasks)

            # Stream results as RecordBatches for memory efficiency
            for batch in result_table.to_batches():
                yield pa.Table.from_batches([batch])

        else:
            # Legacy implementation using project_table (PyIceberg <0.9.0)
            from pyiceberg.io import pyarrow as pyi_pa_io

            # Use the PyIceberg API to read only a single task (specifically, a
            # FileScanTask) - note that this is not as simple as reading a single
            # parquet file, as there might be delete files, etc. associated, so we
            # must use the PyIceberg API for the projection.
            table = pyi_pa_io.project_table(
                tasks=tasks,
                table_metadata=table_metadata,
                io=table_io,
                row_filter=row_filter,
                projected_schema=schema,
                case_sensitive=case_sensitive,
                limit=limit,
            )
            yield table

    for table in _generate_tables():
        if drop_all_columns:
            # ``schema`` holds a sentinel column the caller never asked for, see
            # ``IcebergDatasource._get_sentinel_field``. Hand it to Ray's
            # zero-column projection helper, which slices every column away
            # (preserving the row count, unlike rebuilding a table from zero
            # columns) and appends the ``__bsp_stub`` placeholder that keeps the
            # count alive through downstream ``pa.concat_tables`` calls.
            table = BlockAccessor.for_block(table).select([])
        yield table


@DeveloperAPI
class IcebergDatasource(Datasource):
    """
    Iceberg datasource to read Iceberg tables into a Ray Dataset. This module heavily
    uses PyIceberg to read iceberg tables. All the routines in this class override
    `ray.data.Datasource`.
    """

    def __init__(
        self,
        table_identifier: str,
        row_filter: Union[str, "BooleanExpression"] = None,
        selected_fields: Tuple[str, ...] = ("*",),
        snapshot_id: Optional[int] = None,
        scan_kwargs: Optional[Dict[str, Any]] = None,
        catalog_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize an IcebergDatasource.

        Args:
            table_identifier: Fully qualified table identifier (i.e.,
                "db_name.table_name")
            row_filter: A PyIceberg BooleanExpression to use to filter the data *prior*
                 to reading
            selected_fields: Which columns from the data to read, passed directly to
                PyIceberg's load functions
            snapshot_id: Optional snapshot ID for the Iceberg table
            scan_kwargs: Optional arguments to pass to PyIceberg's Table.scan()
                function
            catalog_kwargs: Optional arguments to use when setting up the Iceberg
                catalog
        """
        # Initialize parent class to set up predicate pushdown mixin
        super().__init__()

        _check_import(self, module="pyiceberg", package="pyiceberg")
        from pyiceberg.expressions import AlwaysTrue

        # Copy both dicts: below we pop ``name`` out of the catalog kwargs and
        # write ``snapshot_id`` into the scan kwargs. Doing that in place would
        # mutate dicts the caller still holds, so a second read reusing the same
        # dict would fail to find its catalog or silently inherit a snapshot ID.
        self._scan_kwargs = dict(scan_kwargs) if scan_kwargs is not None else {}
        self._catalog_kwargs = (
            dict(catalog_kwargs) if catalog_kwargs is not None else {}
        )

        if "name" in self._catalog_kwargs:
            self._catalog_name = self._catalog_kwargs.pop("name")
        else:
            self._catalog_name = "default"

        self.table_identifier = table_identifier

        self._row_filter = row_filter if row_filter is not None else AlwaysTrue()
        # Convert selected_fields to projection_map (identity mapping if specified)
        # Note: Empty tuple () means no columns, None/"*" means all columns
        if selected_fields is None or selected_fields == ("*",):
            self._projection_map = None
        else:
            self._projection_map = {col: col for col in selected_fields}

        if snapshot_id:
            self._scan_kwargs["snapshot_id"] = snapshot_id

        self._plan_files = None
        self._table = None

    def _get_catalog(self) -> "Catalog":
        from pyiceberg import catalog

        return catalog.load_catalog(self._catalog_name, **self._catalog_kwargs)

    @property
    def table(self) -> "Table":
        """
        Return the table reference from the catalog
        """
        if self._table is None:
            catalog = self._get_catalog()
            self._table = catalog.load_table(self.table_identifier)
        return self._table

    @property
    def plan_files(self) -> List["FileScanTask"]:
        """
        Return the plan files specified by this query
        """
        # Calculate and cache the plan_files if they don't already exist
        if self._plan_files is None:
            data_scan = self._get_data_scan()
            # ``DataScan.plan_files`` is annotated ``Iterable[FileScanTask]``.
            # PyIceberg returns a list today, but callers here iterate the cache
            # more than once, so materialize it rather than depend on that.
            self._plan_files = list(data_scan.plan_files())

        return self._plan_files

    def apply_predicate(self, predicate_expr: Expr) -> "IcebergDatasource":
        """Push a predicate down, discarding plan files planned without it.

        The base implementation shallow-copies, which would leave the clone
        sharing a plan-file cache computed before this predicate existed. Those
        tasks carry ``AlwaysTrue`` residuals -- nothing was filtered at plan
        time -- which ``get_read_tasks`` would read as "the filter is fully
        resolved" and report an unfiltered row count for a filtered read.
        """
        return self._invalidate_plan_files(super().apply_predicate(predicate_expr))

    def apply_projection(
        self, projection_map: Optional[Dict[str, str]]
    ) -> "IcebergDatasource":
        """Push a projection down, discarding plan files planned without it.

        The projected columns are part of the scan, so a cache built for a
        different projection does not belong to the clone. See
        ``apply_predicate``.
        """
        return self._invalidate_plan_files(super().apply_projection(projection_map))

    def _invalidate_plan_files(self, clone: "Datasource") -> "Datasource":
        """Drop ``clone``'s inherited plan-file cache, so it re-plans its scan."""
        # A datasource returns ``self`` to signal "no pushdown applied", in which
        # case there is no clone and the cache is still valid for this scan.
        if clone is not self:
            clone._plan_files = None
        return clone

    def _get_combined_filter(self) -> "BooleanExpression":
        """Get the combined filter including both row_filter and pushed-down predicates."""
        combined_filter = self._row_filter

        if self._predicate_expr is not None:
            # Convert Ray Data expression to PyIceberg expression using internal visitor
            visitor = _IcebergExpressionVisitor()
            iceberg_filter = visitor.visit(self._predicate_expr)

            # Combine with existing row_filter using AND
            from pyiceberg.expressions import AlwaysTrue, And

            if not isinstance(combined_filter, AlwaysTrue):
                combined_filter = And(combined_filter, iceberg_filter)
            else:
                combined_filter = iceberg_filter

        return combined_filter

    def _get_sentinel_field(self) -> str:
        """Return the cheapest column to read in place of an empty projection.

        PyIceberg builds its output by reconstructing a table against the
        projected schema, and a table reconstructed from zero columns reports
        zero rows however many rows the scan actually matched. So an empty
        projection -- which ``Dataset.count()`` legitimately requests, since it
        needs a row count but no values -- would silently count zero rows. We
        read a single column instead and slice it away in ``_get_read_task``,
        which preserves the row count. Picking the narrowest column keeps the
        wasted I/O small: reading a string column here instead of a boolean one
        can cost several orders of magnitude more bytes.

        The column is chosen from the schema the scan will actually read, not
        from ``self.table.schema()``. Those differ when ``snapshot_id`` pins a
        historical snapshot: after a column is added, the current schema holds
        names that do not exist in the pinned snapshot, and selecting one raises
        ``ValueError: Could not find column``.
        """
        fields = self._get_scan_schema().fields
        if not fields:
            raise ValueError(
                f"Cannot read table '{self.table_identifier}': it has no columns."
            )
        # ``min`` is stable, so an all-equal-width schema deterministically
        # yields the first field.
        cheapest_field = min(
            fields,
            key=lambda field: _SENTINEL_FIELD_WIDTHS.get(
                str(field.field_type), _UNKNOWN_FIELD_WIDTH
            ),
        )
        return cheapest_field.name

    def _get_scan_schema(self) -> "Schema":
        """Return the full schema this scan reads against.

        Delegates snapshot resolution to PyIceberg -- ``DataScan.projection()``
        returns the pinned snapshot's schema rather than the table's current one,
        and returns it whole when every field is selected -- so that we agree
        with the scan built in ``_get_data_scan``. Builds a scan but does not
        plan it, which touches no data files.
        """
        return self.table.scan(
            row_filter=self._get_combined_filter(),
            selected_fields=("*",),
            **self._scan_kwargs,
        ).projection()

    def _is_empty_projection(self) -> bool:
        """Whether the pushed-down projection selects no columns at all."""
        return self._get_data_columns() == []

    def _get_data_scan(self) -> "DataScan":
        # Get the combined filter
        combined_filter = self._get_combined_filter()

        # Convert back to tuple for PyIceberg API (None -> ("*",))
        data_columns = self._get_data_columns()
        if data_columns is None:
            selected_fields = ("*",)
        elif not data_columns:
            selected_fields = (self._get_sentinel_field(),)
        else:
            selected_fields = tuple(data_columns)

        data_scan = self.table.scan(
            row_filter=combined_filter,
            selected_fields=selected_fields,
            **self._scan_kwargs,
        )

        return data_scan

    def estimate_inmemory_data_size(self) -> Optional[int]:
        # Approximate the size by using the plan files - this will not
        # incorporate the deletes, but that's a reasonable approximation
        # task
        return sum(task.file.file_size_in_bytes for task in self.plan_files)

    def supports_predicate_pushdown(self) -> bool:
        """Returns True to indicate this datasource supports predicate pushdown."""
        return True

    def supports_projection_pushdown(self) -> bool:
        """Returns True to indicate this datasource supports projection pushdown."""
        return True

    @staticmethod
    def _distribute_tasks_into_equal_chunks(
        plan_files: Iterable["FileScanTask"], n_chunks: int
    ) -> List[List["FileScanTask"]]:
        """
        Implement a greedy knapsack algorithm to distribute the files in the scan
        across tasks, based on their file size, as evenly as possible
        """
        chunks = [list() for _ in range(n_chunks)]

        chunk_sizes = [(0, chunk_id) for chunk_id in range(n_chunks)]
        heapq.heapify(chunk_sizes)

        # From largest to smallest, add the plan files to the smallest chunk one at a
        # time
        for plan_file in sorted(
            plan_files, key=lambda f: f.file.file_size_in_bytes, reverse=True
        ):
            smallest_chunk = heapq.heappop(chunk_sizes)
            chunks[smallest_chunk[1]].append(plan_file)
            heapq.heappush(
                chunk_sizes,
                (
                    smallest_chunk[0] + plan_file.file.file_size_in_bytes,
                    smallest_chunk[1],
                ),
            )

        return chunks

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List[ReadTask]:
        from pyiceberg.expressions import AlwaysTrue
        from pyiceberg.io import pyarrow as pyi_pa_io
        from pyiceberg.manifest import DataFileContent

        # Get the PyIceberg scan
        data_scan = self._get_data_scan()
        # Get the plan files in this query
        plan_files = self.plan_files

        # Get the projected schema for this scan, given all the row filters,
        # snapshot ID, etc.
        projected_schema = data_scan.projection()
        # Get the arrow schema, to set in the metadata
        pya_schema = pyi_pa_io.schema_to_pyarrow(projected_schema)

        # An empty projection is read as a single sentinel column, see
        # ``_get_sentinel_field``, which the read tasks then replace with Ray's
        # ``__bsp_stub`` placeholder. Declare that placeholder so the reported
        # schema matches the blocks; it is hidden from the user-visible schema.
        drop_all_columns = self._is_empty_projection()
        if drop_all_columns:
            pya_schema = pa.schema(
                [pa.field(_BATCH_SIZE_PRESERVING_STUB_COL_NAME, pa.null())]
            )

        # Set the n_chunks to the min of the number of plan files and the actual
        # requested n_chunks, so that there are no empty tasks
        if parallelism > len(plan_files):
            parallelism = len(plan_files)
            logger.warning(
                f"Reducing the parallelism to {parallelism}, as that is the number of files"
            )

        # Get required properties for reading tasks - table IO, table metadata,
        # row filter, case sensitivity,limit and projected schema to pass
        # them directly to `_get_read_task` to avoid capture of `self` reference
        # within the closure carrying substantial overhead invoking these tasks
        #
        # See https://github.com/ray-project/ray/issues/49107 for more context
        table_io = self.table.io
        table_metadata = self.table.metadata
        row_filter = self._get_combined_filter()
        case_sensitive = self._scan_kwargs.get("case_sensitive", True)
        limit = self._scan_kwargs.get("limit")

        # Whether the manifests' ``record_count`` is still an exact row count.
        # It is written when the data is written, so it only holds if every row
        # of every surviving file matches the filter. PyIceberg reports whatever
        # is left of the filter after partition pruning as
        # ``FileScanTask.residual``: ``AlwaysTrue`` means pruning resolved the
        # filter completely (e.g. a filter on a partition column dropped whole
        # files), so the counts still hold. Anything else has to be evaluated row
        # by row, and the counts would over-report. Old PyIceberg versions do not
        # populate ``residual``, in which case we cannot prove anything and fall
        # back to reporting an unknown row count.
        #
        # This is deliberately the same test PyIceberg's own ``DataScan.count()``
        # applies, so the two agree on when a manifest count still holds.
        #
        # A ``limit`` makes the read stop early, so no manifest arithmetic
        # describes what comes back and the count cannot be exact. (It is handed
        # to each read task, so the rows returned depend on how files were
        # chunked -- another reason not to try to predict it.)
        counts_are_exact = limit is None and (
            isinstance(row_filter, AlwaysTrue)
            or all(
                isinstance(getattr(task, "residual", None), AlwaysTrue)
                for task in plan_files
            )
        )

        get_read_task = partial(
            _get_read_task,
            table_io=table_io,
            table_metadata=table_metadata,
            row_filter=row_filter,
            case_sensitive=case_sensitive,
            limit=limit,
            schema=projected_schema,
            drop_all_columns=drop_all_columns,
        )

        read_tasks = []
        # Chunk the plan files based on the requested parallelism
        for chunk_tasks in IcebergDatasource._distribute_tasks_into_equal_chunks(
            plan_files, parallelism
        ):
            unique_deletes: Set[DataFile] = set(
                itertools.chain.from_iterable(
                    [task.delete_files for task in chunk_tasks]
                )
            )
            # Get a rough estimate of the number of deletes by just looking at
            # position deletes. Equality deletes are harder to estimate, as they
            # can delete multiple rows.
            position_delete_count = sum(
                delete.record_count
                for delete in unique_deletes
                if delete.content == DataFileContent.POSITION_DELETES
            )
            metadata = BlockMetadata(
                num_rows=(
                    sum(task.file.record_count for task in chunk_tasks)
                    - position_delete_count
                    if counts_are_exact
                    else None
                ),
                size_bytes=sum(task.file.file_size_in_bytes for task in chunk_tasks),
                input_files=[task.file.file_path for task in chunk_tasks],
                exec_stats=None,
            )
            read_tasks.append(
                ReadTask(
                    read_fn=lambda tasks=chunk_tasks: get_read_task(tasks),
                    metadata=metadata,
                    schema=pya_schema,
                    per_task_row_limit=per_task_row_limit,
                )
            )

        return read_tasks
