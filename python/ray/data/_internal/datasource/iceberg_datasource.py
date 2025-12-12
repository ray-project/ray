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

from ray.data._internal.planner.plan_expression.expression_visitors import _ExprVisitor
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.data.expressions import (
    AliasExpr,
    BinaryExpr,
    ColumnExpr,
    DownloadExpr,
    LiteralExpr,
    Operation,
    StarExpr,
    UDFExpr,
    UnaryExpr,
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

logger = logging.getLogger(__name__)


class _IcebergExpressionVisitor(
    _ExprVisitor["BooleanExpression | UnboundTerm[Any] | Literal[Any]"]
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

    def visit_column(self, expr: "ColumnExpr") -> "UnboundTerm[Any]":
        """Convert a column reference to an Iceberg reference."""
        return Reference(expr.name)

    def visit_literal(self, expr: "LiteralExpr") -> "Literal[Any]":
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
    ) -> "BooleanExpression | UnboundTerm[Any] | Literal[Any]":
        """Convert an aliased expression (just unwrap the alias)."""
        return self.visit(expr.expr)

    def visit_udf(
        self, expr: "UDFExpr"
    ) -> "BooleanExpression | UnboundTerm[Any] | Literal[Any]":
        """UDF expressions cannot be converted to Iceberg expressions."""
        raise TypeError(
            "UDF expressions cannot be converted to Iceberg expressions. "
            "Iceberg filters must use simple column comparisons and boolean operations."
        )

    def visit_download(
        self, expr: "DownloadExpr"
    ) -> "BooleanExpression | UnboundTerm[Any] | Literal[Any]":
        """Download expressions cannot be converted to Iceberg expressions."""
        raise TypeError(
            "Download expressions cannot be converted to Iceberg expressions."
        )

    def visit_star(
        self, expr: "StarExpr"
    ) -> "BooleanExpression | UnboundTerm[Any] | Literal[Any]":
        """Star expressions cannot be converted to Iceberg expressions."""
        raise TypeError(
            "Star expressions cannot be converted to Iceberg filter expressions."
        )


def _get_read_task(
    tasks: Iterable["FileScanTask"],
    table_io: "FileIO",
    table_metadata: "TableMetadata",
    row_filter: "BooleanExpression",
    case_sensitive: bool,
    limit: Optional[int],
    schema: "Schema",
    column_rename_map: Optional[Dict[str, str]],
) -> Iterable[Block]:
    # Determine the PyIceberg version to handle backward compatibility
    import pyiceberg

    from ray.data.datasource.datasource import _DatasourceProjectionPushdownMixin

    def _generate_tables() -> Iterable[pa.Table]:
        """Inner generator that yields tables without renaming."""
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

    # Apply renames to all tables from the generator
    yield from _DatasourceProjectionPushdownMixin._apply_rename_to_tables(
        _generate_tables(), column_rename_map
    )


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

        self._scan_kwargs = scan_kwargs if scan_kwargs is not None else {}
        self._catalog_kwargs = catalog_kwargs if catalog_kwargs is not None else {}

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
            self._plan_files = data_scan.plan_files()

        return self._plan_files

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

    def _get_data_scan(self) -> "DataScan":
        # Get the combined filter
        combined_filter = self._get_combined_filter()

        # Convert back to tuple for PyIceberg API (None -> ("*",))
        data_columns = self._get_data_columns()
        selected_fields = ("*",) if data_columns is None else tuple(data_columns)

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
        self, parallelism: int, per_task_row_limit: Optional[int] = None
    ) -> List[ReadTask]:
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

        # Set the n_chunks to the min of the number of plan files and the actual
        # requested n_chunks, so that there are no empty tasks
        if parallelism > len(list(plan_files)):
            parallelism = len(list(plan_files))
            logger.warning(
                f"Reducing the parallelism to {parallelism}, as that is the"
                "number of files"
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

        get_read_task = partial(
            _get_read_task,
            table_io=table_io,
            table_metadata=table_metadata,
            row_filter=row_filter,
            case_sensitive=case_sensitive,
            limit=limit,
            schema=projected_schema,
            column_rename_map=self.get_column_renames(),
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
                num_rows=sum(task.file.record_count for task in chunk_tasks)
                - position_delete_count,
                size_bytes=sum(task.length for task in chunk_tasks),
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
