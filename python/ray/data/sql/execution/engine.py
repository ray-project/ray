"""SQL execution engine for Ray Data SQL API.

This module provides the core execution engine that converts SQL queries
into Ray Dataset operations and executes them efficiently.
"""

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, Set
from dataclasses import dataclass

import ray.data
from ray.data import Dataset
import sqlglot
from sqlglot import exp

from ..exceptions import (
    SQLExecutionError,
    TableNotFoundError,
    ColumnNotFoundError,
)
from ..compiler.expressions import ExpressionCompiler
from ..registry.base import TableRegistry


@dataclass
class JoinInfo:
    """Information about a JOIN operation following Ray Data Join API.

    This dataclass stores join information that maps to the Ray Data Join API
    parameters: join_type, on, right_on, left_suffix, right_suffix, num_partitions.

    Args:
        left_table: Name of the left table.
        right_table: Name of the right table.
        left_columns: Join columns in the left table (as tuple for API compatibility).
        right_columns: Join columns in the right table (as tuple for API compatibility).
        join_type: Type of join (inner, left_outer, right_outer, full_outer).
        left_dataset: The left Ray Dataset.
        right_dataset: The right Ray Dataset.
        left_suffix: Suffix for left operand columns (default: "").
        right_suffix: Suffix for right operand columns (default: "_r").
        num_partitions: Number of partitions for join operation.
    """

    left_table: str
    right_table: str
    left_columns: Tuple[str, ...]  # Changed to tuple for API compatibility
    right_columns: Tuple[str, ...]  # Changed to tuple for API compatibility
    join_type: str
    left_dataset: Optional[Dataset] = None
    right_dataset: Optional[Dataset] = None
    left_suffix: str = ""
    right_suffix: str = "_r"
    num_partitions: int = 10


class JoinHandler:
    """Handles JOIN operations between datasets.

    The JoinHandler processes JOIN clauses in SQL queries and executes
    the corresponding Ray Data join operations. All operations follow Ray
    Dataset API patterns for lazy evaluation and proper return types.

    Examples:
        .. testcode::

            config = SQLConfig()
            handler = JoinHandler(config)
            result = handler.apply_joins(dataset, ast, registry)
    """

    def __init__(self, config):
        self.config = config
        self._logger = logging.getLogger(__name__)

    def apply_joins(
        self, dataset: Dataset, ast: exp.Select, registry: TableRegistry
    ) -> Dataset:
        """Apply all JOIN clauses in the SELECT AST to the dataset.

        This method follows Ray Dataset API patterns for join operations
        and maintains lazy evaluation by chaining join operations.

        Args:
            dataset: Left dataset for joins.
            ast: SELECT AST containing JOIN clauses.
            registry: Dataset registry for table lookups.

        Returns:
            Dataset: Dataset with joins applied following Ray Dataset API patterns.
        """
        for join_node in ast.find_all(exp.Join):
            dataset = self.apply_single_join(dataset, join_node, registry)
        return dataset

    def apply_single_join(
        self, left_dataset: Dataset, join_ast: exp.Join, registry: TableRegistry
    ) -> Dataset:
        """Apply a single JOIN to the left_dataset.

        This method follows Ray Dataset API patterns for join operations
        and maintains lazy evaluation.

        Args:
            left_dataset: Left dataset for the join.
            join_ast: JOIN AST node.
            registry: Dataset registry for table lookups.

        Returns:
            Dataset: Dataset with join applied following Ray Dataset API patterns.
        """
        join_info = self._extract_join_info(join_ast, registry, left_dataset)
        return self._execute_join(left_dataset, join_info)

    def _extract_join_info(
        self, join_ast: exp.Join, registry: TableRegistry, left_dataset: Dataset
    ) -> JoinInfo:
        """Extract join information from the JOIN AST.

        This method extracts join information and maps it to the Ray Data Join API
        parameters. Currently supports single-column equi-joins, with extensibility
        for multi-column joins in the future.

        Args:
            join_ast: JOIN AST node.
            registry: Dataset registry for table lookups.
            left_dataset: Left dataset for the join.

        Returns:
            JoinInfo: Object with join details following Ray Data Join API.

        Raises:
            NotImplementedError: If only equi-joins are supported.
            ValueError: If join condition is invalid.
        """
        right_table_name = str(join_ast.this.name)
        right_dataset = registry.get(right_table_name)
        join_kind = str(join_ast.args.get("side", "inner")).lower()

        # Map SQL join types to Ray Data join types
        join_type_mapping = {
            "inner": "inner",
            "left": "left_outer",
            "right": "right_outer",
            "full": "full_outer",
            "outer": "full_outer",
        }
        ray_join_type = join_type_mapping.get(join_kind, "inner")

        on_condition = join_ast.args.get("on")
        if not isinstance(on_condition, exp.EQ):
            raise NotImplementedError(
                "Only equi-joins (ON left.col = right.col) are supported"
            )

        # Extract column names from join condition
        left_column = self._extract_column_name(on_condition.left)
        right_column = self._extract_column_name(on_condition.right)

        # Add debug logging
        self._logger.debug(f"Join condition: {on_condition}")
        self._logger.debug(f"Left side: {on_condition.left} -> {left_column}")
        self._logger.debug(f"Right side: {on_condition.right} -> {right_column}")

        # Check if the left and right sides are swapped
        # If the left column is not found in the left table but the right column is,
        # then swap them
        left_table_columns = (
            left_dataset.columns() if hasattr(left_dataset, "columns") else []
        )
        right_table_columns = (
            right_dataset.columns() if hasattr(right_dataset, "columns") else []
        )

        self._logger.debug(f"Left table columns: {left_table_columns}")
        self._logger.debug(f"Right table columns: {right_table_columns}")

        # Check if we need to swap the columns
        left_found_in_left = left_column in left_table_columns
        right_found_in_right = right_column in right_table_columns
        left_found_in_right = left_column in right_table_columns
        right_found_in_left = right_column in left_table_columns

        if not left_found_in_left and not right_found_in_right:
            # Try swapping the columns
            if left_found_in_right and right_found_in_left:
                self._logger.debug(
                    f"Swapping join columns: {left_column} <-> {right_column}"
                )
                left_column, right_column = right_column, left_column
            else:
                raise ValueError(
                    f"Invalid join condition: columns not found in expected tables"
                )

        if not left_column or not right_column:
            raise ValueError(
                "Invalid join condition: both left and right columns must be specified"
            )

        # Create JoinInfo with tuple format for API compatibility
        return JoinInfo(
            left_table="left",
            right_table=right_table_name,
            left_columns=(left_column,),  # Single column as tuple for API compatibility
            right_columns=(
                right_column,
            ),  # Single column as tuple for API compatibility
            join_type=ray_join_type,  # Use Ray Data join type
            left_dataset=None,  # Will be set later
            right_dataset=right_dataset,
            left_suffix="",  # Default suffix for left columns
            right_suffix="_r",  # Default suffix for right columns
            num_partitions=self.config.max_join_partitions
            if hasattr(self.config, "max_join_partitions")
            else 10,
        )

    def _extract_column_name(self, expr) -> str:
        """Extract column name from an expression, handling table qualifiers.

        Args:
            expr: Expression to extract column name from.

        Returns:
            str: Column name as string.
        """
        if isinstance(expr, exp.Column):
            # For table-qualified columns like "e.dept", extract just "dept"
            if hasattr(expr, "table") and expr.table:
                return str(expr.name)
            else:
                return str(expr.name)
        elif hasattr(expr, "name"):
            return str(expr.name)
        else:
            return str(expr)

    def _execute_join(self, left_dataset: Dataset, join_info: JoinInfo) -> Dataset:
        """Execute the join operation.

        This method follows Ray Dataset API patterns for join operations
        and maintains lazy evaluation by using the built-in join() method.
        The implementation follows the exact Ray Data Join API specification.

        Args:
            left_dataset: Left dataset for the join.
            join_info: Join information following Ray Data Join API.

        Returns:
            Dataset: Joined dataset following Ray Dataset API patterns.

        Raises:
            ValueError: If join keys are not found.
        """
        join_info.left_dataset = left_dataset

        # Resolve column names with case sensitivity for all join columns
        left_columns = self._create_column_mapping(
            left_dataset.columns(), getattr(self.config, "case_sensitive", False)
        )
        right_columns = self._create_column_mapping(
            join_info.right_dataset.columns(),
            getattr(self.config, "case_sensitive", False),
        )

        self._logger.debug(f"Left dataset columns: {left_dataset.columns()}")
        self._logger.debug(
            f"Right dataset columns: {join_info.right_dataset.columns()}"
        )
        self._logger.debug(f"Left column mapping: {left_columns}")
        self._logger.debug(f"Right column mapping: {right_columns}")

        # Resolve all left join columns
        resolved_left_columns = []
        for col in join_info.left_columns:
            normalized = self._normalize_identifier(
                col, getattr(self.config, "case_sensitive", False)
            )
            resolved_col = left_columns.get(normalized)
            if not resolved_col:
                available_left = list(left_columns.values())
                raise ValueError(
                    f"Join key '{col}' not found in left table. Available columns: {available_left}"
                )
            resolved_left_columns.append(resolved_col)

        # Resolve all right join columns
        resolved_right_columns = []
        for col in join_info.right_columns:
            normalized = self._normalize_identifier(
                col, getattr(self.config, "case_sensitive", False)
            )
            resolved_col = right_columns.get(normalized)
            if not resolved_col:
                available_right = list(right_columns.values())
                raise ValueError(
                    f"Join key '{col}' not found in right table. Available columns: {available_right}"
                )
            resolved_right_columns.append(resolved_col)

        self._logger.debug(
            f"Executing {join_info.join_type.upper()} JOIN: {resolved_left_columns} = {resolved_right_columns}"
        )
        self._logger.debug(f"Left dataset count: {left_dataset.count()}")
        self._logger.debug(f"Right dataset count: {join_info.right_dataset.count()}")

        # Use Ray Dataset API join method for ALL join types (Ray's native join works correctly)
        # join(ds, join_type, num_partitions, on, right_on, left_suffix, right_suffix, ...)
        result = left_dataset.join(
            ds=join_info.right_dataset,  # Other dataset to join against
            join_type=join_info.join_type,  # The kind of join to perform
            num_partitions=join_info.num_partitions,  # Total number of partitions
            on=tuple(resolved_left_columns),  # Columns from left operand as tuple
            right_on=tuple(
                resolved_right_columns
            ),  # Columns from right operand as tuple
            left_suffix=join_info.left_suffix,  # Suffix for left operand columns
            right_suffix=join_info.right_suffix,  # Suffix for right operand columns
        )

        self._logger.debug(f"Join result: {result.count()} rows")
        return result

    def _create_column_mapping(
        self, columns: List[str], case_sensitive: bool = False
    ) -> Dict[str, str]:
        """Create a mapping from normalized column names to their actual names.

        Args:
            columns: List of column names.
            case_sensitive: Whether to preserve case in normalization.

        Returns:
            Dictionary mapping normalized names to actual names.
        """
        mapping = {}
        if columns:
            for col in columns:
                normalized = self._normalize_identifier(col, case_sensitive)
                if normalized not in mapping:
                    mapping[normalized] = col
        return mapping

    def _normalize_identifier(self, name: str, case_sensitive: bool = False) -> str:
        """Normalize SQL identifiers for case-insensitive matching.

        Args:
            name: Identifier to normalize.
            case_sensitive: Whether to preserve case.

        Returns:
            Normalized identifier.
        """
        if name is None:
            return None
        name = str(name).strip()
        return name if case_sensitive else name.lower()


class AggregateAnalyzer:
    """Analyzes aggregate functions in SELECT queries and maps them to data aggregates.

    The AggregateAnalyzer processes aggregate functions in SELECT clauses and
    converts them into data aggregate operations.

    Examples:
        .. testcode::

            config = SQLConfig()
            analyzer = AggregateAnalyzer(config)
            aggregates = analyzer.extract_aggregates(select_ast)
            aggs, renames = analyzer.build_aggregates(aggregates, dataset)
    """

    SUPPORTED_AGGREGATES = {
        "sum",
        "min",
        "max",
        "count",
        "avg",
        "mean",
        "std",
        "stddev",
    }
    AGGREGATE_MAPPING = {
        "sum": "Sum",
        "min": "Min",
        "max": "Max",
        "count": "Count",
        "avg": "Mean",
        "mean": "Mean",
        "std": "Std",
        "stddev": "Std",
    }

    def __init__(self, config):
        self.config = config
        self._logger = logging.getLogger(__name__)

    def extract_group_by_keys(self, select_ast: exp.Select) -> Optional[List[str]]:
        """Extract GROUP BY column names from the SELECT AST.

        Args:
            select_ast: SELECT AST to analyze.

        Returns:
            List of GROUP BY column names, or None if no GROUP BY.
        """
        group_by = select_ast.args.get("group")
        if not group_by:
            return None
        return [str(expr.name) for expr in group_by.expressions]

    def extract_aggregates(
        self, select_ast: exp.Select
    ) -> List[Tuple[str, exp.Expression]]:
        """Extract aggregate expressions and their output names from the SELECT AST.

        Args:
            select_ast: SELECT AST to analyze.

        Returns:
            List of (output_name, aggregate_expression) tuples.
        """
        aggregates = []
        for expr in select_ast.args["expressions"]:
            if isinstance(expr, exp.Alias) and self._is_aggregate(expr.this):
                aggregates.append((str(expr.alias), expr.this))
            elif self._is_aggregate(expr):
                output_name = self._generate_aggregate_name(expr)
                aggregates.append((output_name, expr))
        return aggregates

    def build_aggregates(
        self, aggs: List[Tuple[str, exp.Expression]], dataset: Dataset
    ) -> Tuple[List[Any], Dict[str, str]]:
        """Build aggregate objects and a column rename mapping.

        Args:
            aggs: List of (output_name, aggregate_expression) tuples.
            dataset: Dataset for column information.

        Returns:
            Tuple of (aggregates, renames).

        Raises:
            NotImplementedError: If aggregate function is not supported.
            ValueError: If aggregate specification is invalid.
        """
        import ray.data.aggregate as agg_module

        aggregates = []
        renames = {}
        cols = list(dataset.columns()) if dataset else []
        column_mapping = self._create_column_mapping(
            cols, getattr(self.config, "case_sensitive", False)
        )

        for output_name, agg_expr in aggs:
            agg, col_name = self._build_single_aggregate(
                agg_expr, column_mapping, agg_module
            )
            aggregates.append(agg)
            if output_name != col_name:
                renames[col_name] = output_name

        return aggregates, renames

    def _build_single_aggregate(
        self, agg_expr: exp.Expression, column_mapping: Dict[str, str], agg_module
    ) -> Tuple[Any, str]:
        """Build a single aggregate object.

        Args:
            agg_expr: Aggregate expression to build.
            column_mapping: Mapping from normalized to actual column names.
            agg_module: Data aggregate module.

        Returns:
            Tuple of (aggregate, column_name).

        Raises:
            NotImplementedError: If aggregate function is not supported.
            ValueError: If aggregate specification is invalid.
        """
        func_name = self._get_function_name(agg_expr).lower()
        if func_name not in self.SUPPORTED_AGGREGATES:
            raise NotImplementedError(
                f"Aggregate function '{func_name.upper()}' not supported"
            )

        target_column = self._extract_target_column(agg_expr, column_mapping)

        if target_column and func_name not in ("count",):
            self._validate_aggregate_column(target_column, func_name)

        agg_class = getattr(agg_module, self.AGGREGATE_MAPPING[func_name])

        # Handle COUNT(*) vs COUNT(column) properly
        if func_name == "count":
            if target_column is None:
                # COUNT(*) - count all rows
                aggregate = agg_class()
                column_name = "count()"
            else:
                # COUNT(column) - count non-null values in column
                aggregate = agg_class(target_column)
                column_name = f"count({target_column})"
        elif target_column:
            aggregate = agg_class(target_column)
            func_output = self.AGGREGATE_MAPPING[func_name].lower()
            column_name = f"{func_output}({target_column})"
        else:
            raise ValueError(f"Invalid aggregate specification: {agg_expr}")

        return aggregate, column_name

    def _is_aggregate(self, expr: exp.Expression) -> bool:
        """Return True if the expression is an aggregate function.

        Args:
            expr: Expression to check.

        Returns:
            True if it's an aggregate function.
        """
        return isinstance(expr, (exp.AggFunc, exp.Anonymous))

    def _get_function_name(self, agg_expr: exp.Expression) -> str:
        """Get the function name (e.g., SUM, COUNT) from an aggregate expression.

        Args:
            agg_expr: Aggregate expression.

        Returns:
            Function name as string.
        """
        if hasattr(agg_expr, "sql_name") and agg_expr.sql_name().lower() != "anonymous":
            return agg_expr.sql_name()
        elif isinstance(agg_expr, exp.Anonymous):
            return agg_expr.name
        else:
            return type(agg_expr).__name__

    def _extract_target_column(
        self, agg_expr: exp.Expression, column_mapping: Dict[str, str]
    ) -> Optional[str]:
        """Extract the target column name for an aggregate function, or None for COUNT(*).

        Args:
            agg_expr: Aggregate expression.
            column_mapping: Mapping from normalized to actual column names.

        Returns:
            Target column name, or None for COUNT(*).

        Raises:
            NotImplementedError: If multi-argument aggregates are not supported.
            ValueError: If column is not found.
        """
        # Handle specific aggregate function classes (Sum, Count, etc.)
        if hasattr(agg_expr, "this") and agg_expr.this:
            if isinstance(agg_expr.this, exp.Column):
                col_name = str(agg_expr.this.name)
                # Handle qualified column names (table.column) by extracting just the column part
                if "." in col_name:
                    col_name = col_name.split(".")[-1]
                normalized = self._normalize_identifier(
                    col_name, getattr(self.config, "case_sensitive", False)
                )
                resolved_col = column_mapping.get(normalized)
                if not resolved_col:
                    available_cols = list(column_mapping.values())
                    raise ValueError(
                        f"Column '{col_name}' not found. Available columns: {available_cols}"
                    )
                return resolved_col
            elif isinstance(agg_expr.this, exp.Star):
                # COUNT(*) - no column specified
                return None
        elif isinstance(agg_expr, exp.Anonymous):
            expressions = agg_expr.args.get("expressions", [])
            if len(expressions) == 0:
                # COUNT(*) - no column specified
                return None
            elif len(expressions) == 1:
                # Single argument aggregate like COUNT(column)
                arg = expressions[0]
                if isinstance(arg, exp.Column):
                    col_name = str(arg.name)
                    # Handle qualified column names (table.column) by extracting just the column part
                    if "." in col_name:
                        col_name = col_name.split(".")[-1]
                    normalized = self._normalize_identifier(
                        col_name, getattr(self.config, "case_sensitive", False)
                    )
                    resolved_col = column_mapping.get(normalized)
                    if not resolved_col:
                        available_cols = list(column_mapping.values())
                        raise ValueError(
                            f"Column '{col_name}' not found. Available columns: {available_cols}"
                        )
                    return resolved_col
                else:
                    raise NotImplementedError(
                        f"Complex aggregate arguments not supported: {arg}"
                    )
            else:
                raise NotImplementedError(
                    f"Multi-argument aggregates not supported: {agg_expr}"
                )
        elif isinstance(agg_expr, exp.AggFunc):
            # Handle function-style aggregates like COUNT(column)
            expressions = agg_expr.args.get("expressions", [])
            if len(expressions) == 0:
                # COUNT(*) - no column specified
                return None
            elif len(expressions) == 1:
                # Single argument aggregate like COUNT(column)
                arg = expressions[0]
                if isinstance(arg, exp.Column):
                    col_name = str(arg.name)
                    # Handle qualified column names (table.column) by extracting just the column part
                    if "." in col_name:
                        col_name = col_name.split(".")[-1]
                    normalized = self._normalize_identifier(
                        col_name, getattr(self.config, "case_sensitive", False)
                    )
                    resolved_col = column_mapping.get(normalized)
                    if not resolved_col:
                        available_cols = list(column_mapping.values())
                        raise ValueError(
                            f"Column '{col_name}' not found. Available columns: {available_cols}"
                        )
                    return resolved_col
                else:
                    raise NotImplementedError(
                        f"Complex aggregate arguments not supported: {arg}"
                    )
            else:
                raise NotImplementedError(
                    f"Multi-argument aggregates not supported: {agg_expr}"
                )
        else:
            raise ValueError(f"Unexpected aggregate expression type: {type(agg_expr)}")

    def _generate_aggregate_name(self, agg_expr: exp.Expression) -> str:
        """Generate a default output column name for an aggregate expression.

        Args:
            agg_expr: Aggregate expression.

        Returns:
            Generated column name.
        """
        func_name = self._get_function_name(agg_expr).upper()
        target = agg_expr.args.get("this")
        if isinstance(target, exp.Star):
            return f"{func_name}(*)"
        elif isinstance(target, exp.Column):
            return f"{func_name}({str(target.name)})"
        elif target is None:
            return f"{func_name}()"
        else:
            return f"{func_name}({target})"

    def _validate_aggregate_column(self, column: str, func_name: str) -> None:
        """Validate that the aggregate function can be applied to the column.

        Args:
            column: Column name to validate.
            func_name: Aggregate function name.
        """
        # This is a simplified validation - in practice, you'd check the actual data
        if func_name not in ("count",):
            self._logger.debug(
                f"Validating column '{column}' for aggregate '{func_name.upper()}'"
            )

    def _create_column_mapping(
        self, columns: List[str], case_sensitive: bool = False
    ) -> Dict[str, str]:
        """Create a mapping from normalized column names to their actual names."""
        mapping = {}
        if columns:
            for col in columns:
                normalized = self._normalize_identifier(col, case_sensitive)
                if normalized not in mapping:
                    mapping[normalized] = col
        return mapping

    def _normalize_identifier(self, name: str, case_sensitive: bool = False) -> str:
        """Normalize SQL identifiers for case-insensitive matching."""
        if name is None:
            return None
        name = str(name).strip()
        return name if case_sensitive else name.lower()


class SQLExecutionEngine:
    """Executes parsed SQL queries against registered Ray Datasets.

    This engine converts SQLGlot ASTs into Ray Dataset operations and executes them.
    It supports SELECT queries with WHERE, JOIN, GROUP BY, ORDER BY, and LIMIT clauses.
    """

    def __init__(self, registry: TableRegistry, config=None):
        self.registry = registry
        self.config = (
            config
            or type(
                "Config", (), {"case_sensitive": False, "max_join_partitions": 10}
            )()
        )
        self.join_handler = JoinHandler(self.config)
        self.aggregate_analyzer = AggregateAnalyzer(self.config)
        self._logger = logging.getLogger(__name__)

    def execute(self, ast: exp.Expression) -> Dataset:
        """Execute a parsed SQLGlot AST.

        Args:
            ast: SQLGlot AST to execute.

        Returns:
            Ray Dataset with the query result.

        Raises:
            NotImplementedError: If the AST type is not supported.
        """
        if isinstance(ast, exp.Select):
            return self._execute_select(ast)
        else:
            raise NotImplementedError(
                f"Unsupported statement type: {type(ast).__name__}"
            )

    def _execute_select(self, ast: exp.Select) -> Dataset:
        """Execute a SELECT statement.

        Args:
            ast: SELECT AST to execute.

        Returns:
            Ray Dataset with the query result.
        """
        # Resolve FROM clause and get base dataset
        dataset, table_name = self._resolve_from(ast)

        # Apply JOINs if present
        dataset = self.join_handler.apply_joins(dataset, ast, self.registry)

        # Apply WHERE clause if present
        dataset = self._apply_where_clause(dataset, ast)

        # Check if this is a GROUP BY query
        group_keys = self.aggregate_analyzer.extract_group_by_keys(ast)
        if group_keys:
            return self._execute_group_by_query(ast, group_keys, dataset)

        # Check if this is an aggregate-only query (no GROUP BY)
        aggregates = self.aggregate_analyzer.extract_aggregates(ast)
        if aggregates and self._is_aggregate_only_query(ast):
            return self._execute_aggregate_query(dataset, aggregates)

        # Apply projection
        dataset = self._apply_projection(dataset, ast, table_name)

        # Apply ORDER BY
        dataset = self._apply_order_by(dataset, ast)

        # Apply LIMIT
        dataset = self._apply_limit(dataset, ast)

        return dataset

    def _resolve_from(self, ast: exp.Select) -> Tuple[Dataset, str]:
        """Resolve the FROM clause and return the dataset and table name.

        Returns:
            Tuple of (dataset, table_name).
        """
        from_clause = ast.args.get("from")
        if from_clause:
            if hasattr(from_clause, "expressions") and from_clause.expressions:
                table_expr = from_clause.expressions[0]
                return self._resolve_table_expression(table_expr)
            elif hasattr(from_clause, "this") and from_clause.this:
                table_expr = from_clause.this
                return self._resolve_table_expression(table_expr)
            else:
                raise ValueError("Invalid FROM clause")

        # Try to get default table if no FROM clause
        tables = self.registry.list_tables()
        if len(tables) == 1:
            table_name = tables[0]
            dataset = self.registry.get(table_name)
            return dataset, table_name

        raise ValueError("No FROM clause specified and no default table available")

    def _resolve_table_expression(self, table_expr) -> Tuple[Dataset, str]:
        """Resolve a table expression, which could be a table name or subquery.

        Args:
            table_expr: Table expression to resolve.

        Returns:
            Tuple of (dataset, table_name).
        """
        if isinstance(table_expr, exp.Table):
            # Simple table reference
            table_name = str(table_expr.this)
            dataset = self.registry.get(table_name)
            return dataset, table_name
        elif isinstance(table_expr, exp.Subquery):
            # Handle subquery in FROM clause
            return self._execute_subquery(table_expr)
        elif isinstance(table_expr, exp.Alias):
            # Handle aliased table or subquery
            if isinstance(table_expr.this, exp.Subquery):
                dataset, _ = self._execute_subquery(table_expr.this)
                return dataset, str(table_expr.alias)
            elif isinstance(table_expr.this, exp.Table):
                table_name = str(table_expr.this.this)
                dataset = self.registry.get(table_name)
                return dataset, str(table_expr.alias)
            else:
                raise ValueError(
                    f"Unsupported aliased expression: {type(table_expr.this)}"
                )
        else:
            raise ValueError(f"Unsupported table expression: {type(table_expr)}")

    def _execute_subquery(self, subquery_expr: exp.Subquery) -> Tuple[Dataset, str]:
        """Execute a subquery and return the result dataset.

        Args:
            subquery_expr: Subquery expression to execute.

        Returns:
            Tuple of (dataset, generated_table_name).
        """
        # Execute the subquery
        subquery_ast = subquery_expr.this
        if isinstance(subquery_ast, exp.Select):
            result_dataset = self._execute_select(subquery_ast)
        else:
            raise ValueError(f"Unsupported subquery type: {type(subquery_ast)}")

        # Generate a unique table name for the subquery result
        table_name = f"subquery_{id(subquery_expr)}"

        # Register the subquery result temporarily
        self.registry.register(table_name, result_dataset)

        return result_dataset, table_name

    def _apply_where_clause(self, dataset: Dataset, ast: exp.Select) -> Dataset:
        """Apply WHERE clause filtering to the dataset.

        Args:
            dataset: Input dataset.
            ast: SELECT AST containing WHERE clause.

        Returns:
            Filtered dataset.
        """
        where_clause = ast.args.get("where")
        if not where_clause:
            return dataset

        try:
            # Check if WHERE clause contains subqueries
            if self._contains_subquery(where_clause.this):
                return self._apply_where_with_subquery(dataset, where_clause.this)
            else:
                filter_func = ExpressionCompiler.compile(where_clause.this)
                return dataset.filter(filter_func)
        except Exception as e:
            self._logger.error(f"WHERE clause evaluation failed: {e}")
            raise ValueError(f"Invalid WHERE clause: {e}")

    def _contains_subquery(self, expr: exp.Expression) -> bool:
        """Check if an expression contains subqueries.

        Args:
            expr: Expression to check.

        Returns:
            True if the expression contains subqueries.
        """
        for node in expr.walk():
            if isinstance(node, exp.Subquery):
                return True
        return False

    def _apply_where_with_subquery(
        self, dataset: Dataset, where_expr: exp.Expression
    ) -> Dataset:
        """Apply WHERE clause that contains subqueries.

        Args:
            dataset: Input dataset.
            where_expr: WHERE clause expression.

        Returns:
            Filtered dataset.
        """
        # For now, we'll handle basic subquery patterns
        # This can be expanded to handle more complex subquery scenarios

        if isinstance(where_expr, exp.In):
            # Handle IN subquery: WHERE col IN (SELECT ...)
            return self._handle_in_subquery(dataset, where_expr)
        elif isinstance(where_expr, exp.Exists):
            # Handle EXISTS subquery: WHERE EXISTS (SELECT ...)
            return self._handle_exists_subquery(dataset, where_expr)
        elif isinstance(where_expr, exp.EQ):
            # Handle comparison with subquery: WHERE col = (SELECT ...)
            return self._handle_comparison_subquery(dataset, where_expr)
        else:
            # For other subquery patterns, compile and apply
            try:
                filter_func = ExpressionCompiler.compile(where_expr)
                return dataset.filter(filter_func)
            except Exception as e:
                self._logger.warning(
                    f"Could not compile WHERE clause with subquery: {e}"
                )
                # Return original dataset if compilation fails
                return dataset

    def _handle_in_subquery(self, dataset: Dataset, in_expr: exp.In) -> Dataset:
        """Handle IN subquery: WHERE col IN (SELECT ...)

        Args:
            dataset: Input dataset.
            in_expr: IN expression with subquery.

        Returns:
            Filtered dataset.
        """
        # Extract the column and subquery
        column_expr = in_expr.this
        subquery_expr = in_expr.expressions[0]

        if not isinstance(subquery_expr, exp.Subquery):
            # Not a subquery, use regular IN handling
            try:
                filter_func = ExpressionCompiler.compile(in_expr)
                return dataset.filter(filter_func)
            except Exception:
                return dataset

        # Execute the subquery to get the values
        subquery_values, _ = self._execute_subquery(subquery_expr)

        # Extract the column name
        if isinstance(column_expr, exp.Column):
            col_name = str(column_expr.name)
            # Handle table-qualified columns
            if "." in col_name:
                col_name = col_name.split(".")[-1]
        else:
            # For complex expressions, we can't handle this easily
            self._logger.warning(
                "Complex column expressions in IN subquery not supported"
            )
            return dataset

        # Get the values from the subquery result
        try:
            subquery_rows = subquery_values.take_all()
            # Extract the first column from each row
            values = [row[list(row.keys())[0]] for row in subquery_rows if row]
        except Exception as e:
            self._logger.warning(f"Failed to extract subquery values: {e}")
            return dataset

        # Filter the dataset
        def in_filter(row):
            col_value = row.get(col_name)
            return col_value in values

        return dataset.filter(in_filter)

    def _handle_exists_subquery(
        self, dataset: Dataset, exists_expr: exp.Exists
    ) -> Dataset:
        """Handle EXISTS subquery: WHERE EXISTS (SELECT ...)

        Args:
            dataset: Input dataset.
            exists_expr: EXISTS expression with subquery.

        Returns:
            Filtered dataset.
        """
        # For EXISTS subqueries, we need to correlate with the outer query
        # This is complex and not fully implemented yet
        self._logger.warning("EXISTS subqueries are not fully supported yet")

        # For now, return the original dataset
        return dataset

    def _handle_comparison_subquery(self, dataset: Dataset, eq_expr: exp.EQ) -> Dataset:
        """Handle comparison with subquery: WHERE col = (SELECT ...)

        Args:
            dataset: Input dataset.
            eq_expr: Equality expression with subquery.

        Returns:
            Filtered dataset.
        """
        # Check if one side is a subquery
        left_is_subquery = isinstance(eq_expr.left, exp.Subquery)
        right_is_subquery = isinstance(eq_expr.right, exp.Subquery)

        if not (left_is_subquery or right_is_subquery):
            # Not a subquery comparison
            try:
                filter_func = ExpressionCompiler.compile(eq_expr)
                return dataset.filter(filter_func)
            except Exception:
                return dataset

        # Determine which side is the subquery and which is the column
        if left_is_subquery:
            column_expr = eq_expr.right
            subquery_expr = eq_expr.left
        else:
            column_expr = eq_expr.left
            subquery_expr = eq_expr.right

        # Extract the column name
        if isinstance(column_expr, exp.Column):
            col_name = str(column_expr.name)
            # Handle table-qualified columns
            if "." in col_name:
                col_name = col_name.split(".")[-1]
        else:
            # For complex expressions, we can't handle this easily
            self._logger.warning(
                "Complex column expressions in comparison subquery not supported"
            )
            return dataset

        # Execute the subquery to get the value
        subquery_values, _ = self._execute_subquery(subquery_expr)

        # Get the value from the subquery result
        try:
            subquery_rows = subquery_values.take_all()
            if not subquery_rows:
                # Empty subquery result - no matches
                return ray.data.from_items([])

            # Extract the first value from the first row
            first_row = subquery_rows[0]
            subquery_value = first_row[list(first_row.keys())[0]]

            # Check if subquery returns multiple rows (should be scalar)
            if len(subquery_rows) > 1:
                self._logger.warning("Subquery returned multiple rows, using first row")

        except Exception as e:
            self._logger.warning(f"Failed to extract subquery value: {e}")
            return dataset

        # Filter the dataset
        def eq_filter(row):
            col_value = row.get(col_name)
            return col_value == subquery_value

        return dataset.filter(eq_filter)

    def _apply_projection(
        self, dataset: Dataset, ast: exp.Select, table_name: str
    ) -> Dataset:
        """Apply SELECT projection to the dataset.

        Args:
            dataset: Input dataset.
            ast: SELECT AST containing projection expressions.
            table_name: Name of the base table.

        Returns:
            Dataset with projection applied.
        """
        select_exprs = ast.args["expressions"]

        # Handle SELECT *
        if len(select_exprs) == 1 and isinstance(select_exprs[0], exp.Star):
            return dataset

        # Handle specific columns
        column_names = []
        for expr in select_exprs:
            if isinstance(expr, exp.Alias):
                alias_name = str(expr.alias)
                if isinstance(expr.this, exp.Column):
                    col_name = str(expr.this.name)
                    # Handle table-qualified columns
                    if "." in col_name:
                        col_name = col_name.split(".")[-1]
                    column_names.append(alias_name)
                else:
                    # Complex expression with alias
                    column_names.append(alias_name)
            elif isinstance(expr, exp.Column):
                col_name = str(expr.name)
                # Handle table-qualified columns
                if "." in col_name:
                    col_name = col_name.split(".")[-1]
                column_names.append(col_name)
            else:
                # Complex expression without alias
                column_names.append(f"col_{len(column_names)}")

        # For now, just select the columns - in a full implementation,
        # you'd compile the expressions and apply them
        if column_names:
            try:
                return dataset.select_columns(column_names)
            except Exception:
                # Fallback to map if select_columns fails
                return dataset.map(
                    lambda row: {col: row.get(col, None) for col in column_names}
                )

        return dataset

    def _apply_order_by(self, dataset: Dataset, ast: exp.Select) -> Dataset:
        """Apply ORDER BY to the dataset.

        Args:
            dataset: Input dataset.
            ast: SELECT AST containing ORDER BY clause.

        Returns:
            Sorted dataset.
        """
        order_clause = ast.args.get("order")
        if not order_clause:
            return dataset

        sort_keys = []
        descending = []

        for ordering in order_clause.expressions:
            if not isinstance(ordering, exp.Ordered):
                continue

            sort_expr = ordering.this
            is_descending = bool(ordering.args.get("desc", False))

            if isinstance(sort_expr, exp.Column):
                col_name = str(sort_expr.name)
                # Handle table-qualified columns
                if "." in col_name:
                    col_name = col_name.split(".")[-1]
                sort_keys.append(col_name)
                descending.append(is_descending)

        if sort_keys:
            try:
                if len(sort_keys) == 1:
                    return dataset.sort(sort_keys[0], descending=descending[0])
                else:
                    return dataset.sort(sort_keys, descending=descending)
            except Exception as e:
                self._logger.warning(f"ORDER BY failed: {e}")

        return dataset

    def _apply_limit(self, dataset: Dataset, ast: exp.Select) -> Dataset:
        """Apply LIMIT to the dataset.

        Args:
            dataset: Input dataset.
            ast: SELECT AST containing LIMIT clause.

        Returns:
            Limited dataset.
        """
        limit_clause = ast.args.get("limit")
        if not limit_clause:
            return dataset

        try:
            limit_value = int(str(limit_clause.expression))
            if limit_value > 0:
                return dataset.limit(limit_value)
        except (ValueError, TypeError) as e:
            self._logger.warning(f"Invalid LIMIT value: {e}")

        return dataset

    def _execute_group_by_query(
        self, ast: exp.Select, group_keys: List[str], dataset: Dataset
    ) -> Dataset:
        """Execute a GROUP BY query.

        Args:
            ast: SELECT AST containing GROUP BY.
            group_keys: List of GROUP BY column names.
            dataset: Input dataset.

        Returns:
            Grouped dataset with aggregates applied.
        """
        # Resolve group keys to actual column names
        resolved_keys = self._resolve_group_keys(group_keys, dataset)

        # Extract aggregates
        aggregates = self.aggregate_analyzer.extract_aggregates(ast)
        if not aggregates:
            raise ValueError("GROUP BY queries must include aggregate functions")

        # Build aggregate objects
        agg_objects, renames = self.aggregate_analyzer.build_aggregates(
            aggregates, dataset
        )

        # Execute group by
        if len(resolved_keys) == 1:
            grouped = dataset.groupby(resolved_keys[0])
        else:
            grouped = dataset.groupby(resolved_keys)

        result = grouped.aggregate(*agg_objects)

        # Apply column renames
        if renames and result is not None:
            try:
                result = result.rename_columns(renames)
            except Exception as e:
                self._logger.warning(f"Failed to rename columns: {e}")

        # Apply ORDER BY and LIMIT
        result = self._apply_order_by(result, ast)
        result = self._apply_limit(result, ast)

        return result

    def _execute_aggregate_query(
        self, dataset: Dataset, aggregates: List[Tuple[str, exp.Expression]]
    ) -> Dataset:
        """Execute an aggregate-only query (no GROUP BY).

        Args:
            dataset: Input dataset.
            aggregates: List of aggregate expressions.

        Returns:
            Dataset with aggregate results.
        """
        # Build aggregate objects
        agg_objects, renames = self.aggregate_analyzer.build_aggregates(
            aggregates, dataset
        )

        # Execute aggregates
        result = dataset.aggregate(*agg_objects)

        # Convert to dataset if needed
        if isinstance(result, dict):
            result = ray.data.from_items([result])
        elif result is None:
            # Create empty result
            result = ray.data.from_items([{}])

        # Apply column renames
        if renames and result is not None:
            try:
                result = result.rename_columns(renames)
            except Exception as e:
                self._logger.warning(f"Failed to rename columns: {e}")

        return result

    def _resolve_group_keys(self, group_keys: List[str], dataset: Dataset) -> List[str]:
        """Resolve GROUP BY column names to actual column names.

        Args:
            group_keys: List of GROUP BY column names.
            dataset: Input dataset.

        Returns:
            List of resolved column names.
        """
        cols = list(dataset.columns())
        column_mapping = self._create_column_mapping(
            cols, getattr(self.config, "case_sensitive", False)
        )
        keys = []

        for key in group_keys:
            normalized = self._normalize_identifier(
                key, getattr(self.config, "case_sensitive", False)
            )
            actual_column = column_mapping.get(normalized)
            if not actual_column:
                raise ValueError(
                    f"GROUP BY column '{key}' not found. Available columns: {cols}"
                )
            keys.append(actual_column)

        return keys

    def _is_aggregate_only_query(self, ast: exp.Select) -> bool:
        """Check if this is an aggregate-only query (no GROUP BY).

        Args:
            ast: SELECT AST to check.

        Returns:
            True if all SELECT expressions are aggregates.
        """
        select_exprs = ast.args["expressions"]
        for expr in select_exprs:
            if isinstance(expr, exp.Alias):
                if not self.aggregate_analyzer._is_aggregate(expr.this):
                    return False
            elif not self.aggregate_analyzer._is_aggregate(expr):
                return False
        return len(select_exprs) > 0

    def _create_column_mapping(
        self, columns: List[str], case_sensitive: bool = False
    ) -> Dict[str, str]:
        """Create a mapping from normalized column names to their actual names."""
        mapping = {}
        if columns:
            for col in columns:
                normalized = self._normalize_identifier(col, case_sensitive)
                if normalized not in mapping:
                    mapping[normalized] = col
        return mapping

    def _normalize_identifier(self, name: str, case_sensitive: bool = False) -> str:
        """Normalize SQL identifiers for case-insensitive matching."""
        if name is None:
            return None
        name = str(name).strip()
        return name if case_sensitive else name.lower()
