"""
Main query executor for Ray Data SQL API.

This module provides the QueryExecutor class which coordinates the execution
of parsed SQL ASTs by applying the appropriate operations to Ray Datasets.
"""

from typing import Any, Callable, Dict, List, Tuple

from sqlglot import exp

import ray
from ray.data import Dataset
from ray.data.sql.config import SQLConfig
from ray.data.sql.execution.analyzers import AggregateAnalyzer, ProjectionAnalyzer
from ray.data.sql.execution.handlers import (
    FilterHandler,
    JoinHandler,
    LimitHandler,
    OrderHandler,
)
from ray.data.sql.schema import DatasetRegistry
from ray.data.sql.utils import (
    create_column_mapping,
    is_aggregate_function,
    normalize_identifier,
    setup_logger,
)


class QueryExecutor:
    """Executes parsed SQL ASTs against registered Ray Datasets.

    The QueryExecutor is the core execution engine that transforms SQL operations
    into Ray Dataset operations. It orchestrates the execution of SELECT queries
    by coordinating various handlers and analyzers for different SQL constructs.

    The executor follows a systematic approach:
    1. Analyze the query structure (simple vs. GROUP BY)
    2. Resolve table references from the registry
    3. Apply operations in the correct order (FROM -> WHERE -> GROUP BY -> HAVING -> ORDER BY -> LIMIT)
    4. Return a Ray Dataset that can be further chained or materialized

    All operations maintain Ray Dataset's lazy evaluation semantics and follow
    the Ray Dataset API patterns for consistency and performance.

    Examples:
        .. testcode::

            executor = QueryExecutor(registry, config)
            result = executor.execute(ast)
    """

    def __init__(self, registry: DatasetRegistry, config: SQLConfig):
        """Initialize the query executor with required components.

        Args:
            registry: Dataset registry containing table name to dataset mappings.
            config: SQL configuration controlling execution behavior.
        """
        # Core dependencies for execution
        self.registry = registry  # Table name -> Dataset mapping
        self.config = config  # Execution configuration and options

        # Specialized analyzers for different aspects of query processing
        self.projection_analyzer = ProjectionAnalyzer(
            config
        )  # Handles SELECT clause analysis
        self.aggregate_analyzer = AggregateAnalyzer(
            config
        )  # Handles GROUP BY and aggregates

        # Operation handlers for different SQL constructs
        self.join_handler = JoinHandler(config)  # Handles JOIN operations
        self.filter_handler = FilterHandler(config)  # Handles WHERE conditions
        self.order_handler = OrderHandler(config)  # Handles ORDER BY clauses
        self.limit_handler = LimitHandler(config)  # Handles LIMIT clauses

        # Logger for debugging and monitoring execution
        self._logger = setup_logger("QueryExecutor")

    def execute(self, ast: exp.Expression) -> Dataset:
        """Execute a parsed SQLGlot AST (must be a SELECT statement).

        This is the main entry point for query execution. It analyzes the query
        structure and dispatches to appropriate execution methods based on
        whether the query uses GROUP BY aggregation or not.

        Args:
            ast: Parsed SQLGlot AST representing a SQL SELECT statement.

        Returns:
            Ray Dataset containing the query results.

        Raises:
            NotImplementedError: If the AST is not a SELECT statement.
            ValueError: If query execution fails due to invalid operations.
        """
        # Validate that we have a SELECT statement (other statement types not supported)
        if not isinstance(ast, exp.Select):
            raise NotImplementedError("Only SELECT statements are supported")

        try:
            # Analyze the query to determine if it uses GROUP BY aggregation
            group_keys = self.aggregate_analyzer.extract_group_by_keys(ast)

            if group_keys:
                # Execute as a GROUP BY query with aggregation
                return self._execute_group_by_query(ast, group_keys)
            else:
                # Execute as a simple query without aggregation
                return self._execute_simple_query(ast)

        except Exception as e:
            # Log the error and re-raise with more context
            self._logger.error(f"Query execution failed: {e}")
            raise ValueError(f"Query execution failed: {e}")

    def _execute_simple_query(self, ast: exp.Select) -> Dataset:
        """Execute a SELECT query without GROUP BY."""
        dataset, table_name = self._resolve_from_clause(ast)
        select_exprs = ast.args["expressions"]

        # Handle SELECT of only literals
        if self._has_only_literals(select_exprs):
            return self._execute_literal_query(ast, select_exprs)

        # Handle empty dataset
        if dataset.count() == 0:
            return self._handle_empty_dataset(ast)

        # Apply operations in sequence following Ray Dataset API patterns
        dataset = self._apply_query_operations(dataset, ast, table_name, select_exprs)
        return dataset

    def _execute_literal_query(
        self, ast: exp.Select, select_exprs: List[exp.Expression]
    ) -> Dataset:
        """Execute a query that selects only literal values."""
        limit = self._extract_limit_value(ast)
        if limit <= 0:
            return ray.data.from_items([])

        result_row = self._build_literal_row(select_exprs)
        return ray.data.from_items([result_row for _ in range(limit)])

    def _extract_limit_value(self, ast: exp.Select) -> int:
        """Extract the LIMIT value from the AST."""
        limit_clause = ast.args.get("limit")
        if limit_clause and getattr(limit_clause, "this", None):
            try:
                limit_expr = getattr(limit_clause, "this", None)
                if isinstance(limit_expr, exp.Literal):
                    return int(str(limit_expr.name))
                elif hasattr(limit_expr, "name"):
                    return int(str(limit_expr.name))
                else:
                    return int(limit_expr)
            except Exception:
                return 1
        return 1

    def _build_literal_row(self, select_exprs: List[exp.Expression]) -> Dict[str, Any]:
        """Build a row from literal expressions."""
        from ray.data.sql.compiler import ExpressionCompiler

        compiler = ExpressionCompiler(self.config)

        result_row = {}
        for idx, expr in enumerate(select_exprs):
            if isinstance(expr, exp.Alias):
                key = str(expr.alias)
                val_expr = expr.this
            else:
                key = f"col_{idx}"
                val_expr = expr

            if isinstance(val_expr, exp.Literal):
                value = compiler._parse_literal(val_expr)
            elif isinstance(val_expr, exp.Boolean):
                value = str(val_expr.name).lower() == "true"
            else:
                value = None

            result_row[key] = value
        return result_row

    def _apply_query_operations(
        self,
        dataset: Dataset,
        ast: exp.Select,
        table_name: str,
        select_exprs: List[exp.Expression],
    ) -> Dataset:
        """Apply all query operations in the correct order."""
        # Apply joins following Ray Dataset API patterns
        dataset = self.join_handler.apply_joins(dataset, ast, self.registry)

        # Apply WHERE clause following Ray Dataset API patterns
        dataset = self.filter_handler.apply_where_clause(dataset, ast)

        # Check if this is an aggregate-only query
        if self._is_aggregate_only_query(select_exprs):
            aggregates = self.aggregate_analyzer.extract_aggregates(ast)
            dataset = self._execute_aggregate_query(dataset, aggregates)
            dataset = self.limit_handler.apply_limit(dataset, ast)
            return dataset

        # Analyze projections
        column_names, funcs = self.projection_analyzer.analyze_projections(
            select_exprs, dataset, table_name
        )

        # Apply operations in the correct order following Ray Dataset API patterns
        dataset = self._apply_ordered_operations(dataset, ast, column_names, funcs)
        return dataset

    def _apply_ordered_operations(
        self,
        dataset: Dataset,
        ast: exp.Select,
        column_names: List[str],
        funcs: List[Callable],
    ) -> Dataset:
        """Apply operations in the correct order: order -> projection -> limit."""
        # Apply ORDER BY first (before projection to ensure ordering columns are available)
        order_result = self.order_handler.apply_order_by(dataset, ast, column_names)
        if order_result != "DEFER":
            dataset = order_result

        # Apply projection after ordering
        dataset = self._apply_projection(dataset, column_names, funcs)

        # Apply LIMIT last
        dataset = self.limit_handler.apply_limit(dataset, ast)

        return dataset

    def _apply_projection(
        self, dataset: Dataset, column_names: List[str], exprs: List[Callable]
    ) -> Dataset:
        """Apply the SELECT projection to the dataset."""

        def project_row(row: Dict[str, Any]) -> Dict[str, Any]:
            try:
                return {name: func(row) for name, func in zip(column_names, exprs)}
            except Exception as e:
                self._logger.error(f"Projection failed for row {row}: {e}")
                raise

        # Add debug logging to see what's happening
        self._logger.debug(f"Applying projection with column names: {column_names}")

        result = dataset.map(project_row)
        self._logger.debug("Projection applied successfully")
        return result

    def _has_only_literals(self, select_exprs: List[exp.Expression]) -> bool:
        """Return True if all SELECT expressions are literals or booleans."""
        for expr in select_exprs:
            inner_expr = expr.this if isinstance(expr, exp.Alias) else expr
            if not isinstance(inner_expr, (exp.Literal, exp.Boolean)):
                return False
        return True

    def _handle_empty_dataset(self, ast: exp.Select) -> Dataset:
        """Handle SELECT queries on empty datasets."""
        select_exprs = ast.args["expressions"]

        # Aggregate-only queries with empty input
        if self._is_aggregate_only_query(select_exprs):
            aggregates = self.aggregate_analyzer.extract_aggregates(ast)
            return self._create_empty_aggregate_result(aggregates)

        # All literal/simple expressions
        if self._has_only_literals(select_exprs):
            return self._create_literal_result(ast, select_exprs)

        # Otherwise, no output rows
        return ray.data.from_items([])

    def _create_literal_result(
        self, ast: exp.Select, select_exprs: List[exp.Expression]
    ) -> Dataset:
        """Create result for literal-only queries on empty datasets."""
        limit_value = self._extract_limit_value(ast)
        if limit_value == 0:
            return ray.data.from_items([])

        row = self._build_literal_row(select_exprs)
        return ray.data.from_items([row.copy() for _ in range(limit_value)])

    def _is_aggregate_only_query(self, exprs: List[exp.Expression]) -> bool:
        """Return True if all SELECT expressions are aggregate functions."""
        for expr in exprs:
            if isinstance(expr, exp.Alias):
                if not is_aggregate_function(expr.this):
                    return False
            elif not is_aggregate_function(expr):
                return False
        return len(exprs) > 0

    def _execute_group_by_query(
        self, ast: exp.Select, group_keys: List[str]
    ) -> Dataset:
        """Execute a SELECT ... GROUP BY ... query."""
        dataset, _ = self._resolve_from_clause(ast)
        dataset = self.join_handler.apply_joins(dataset, ast, self.registry)
        dataset = self.filter_handler.apply_where_clause(dataset, ast)

        if dataset.count() == 0:
            return ray.data.from_items([])

        aggregates = self.aggregate_analyzer.extract_aggregates(ast)
        if not aggregates:
            raise ValueError("GROUP BY queries must include aggregate functions")

        keys = self._resolve_group_keys(group_keys, dataset)
        aggregates, renames = self.aggregate_analyzer.build_aggregates(
            aggregates, dataset
        )

        # Execute group by following Ray Dataset API patterns
        if len(keys) == 1:
            grouped = dataset.groupby(keys[0])
        else:
            grouped = dataset.groupby(keys)

        result = grouped.aggregate(*aggregates)

        # Apply column renames following Ray Dataset API patterns
        if renames and result is not None:
            try:
                result = result.rename_columns(renames)
            except Exception as e:
                self._logger.warning(f"Failed to rename columns: {e}")

        # Apply ORDER BY and LIMIT following Ray Dataset API patterns
        result = self.order_handler.apply_order_by(result, ast)
        result = self.limit_handler.apply_limit(result, ast)
        return result

    def _resolve_group_keys(self, group_keys: List[str], dataset: Dataset) -> List[str]:
        """Resolve group by column names to actual column names."""
        cols = list(dataset.columns())
        column_mapping = create_column_mapping(cols, self.config.case_sensitive)
        keys = []

        for key in group_keys:
            normalized = normalize_identifier(key, self.config.case_sensitive)
            actual_column = column_mapping.get(normalized)
            if not actual_column:
                raise ValueError(
                    f"GROUP BY column '{key}' not found. Available columns: {cols}"
                )
            keys.append(actual_column)

        return keys

    def _execute_aggregate_query(
        self, dataset: Dataset, aggregates: List[Tuple[str, exp.Expression]]
    ) -> Dataset:
        """Execute a SELECT query with only aggregate functions (no GROUP BY)."""
        if not aggregates:
            raise ValueError("No aggregates found in aggregate-only query")

        if dataset.count() == 0:
            return self._create_empty_aggregate_result(aggregates)

        # Check if we have COUNT(*) - handle it specially
        if self._has_count_star(aggregates):
            return self._execute_count_star_query(dataset, aggregates)
        else:
            return self._execute_standard_aggregate_query(dataset, aggregates)

    def _has_count_star(self, aggregates: List[Tuple[str, exp.Expression]]) -> bool:
        """Check if any aggregate is COUNT(*)."""
        from ray.data.sql.utils import get_function_name_from_expression

        for output_name, agg_expr in aggregates:
            func_name = get_function_name_from_expression(agg_expr).lower()
            if func_name == "count":
                # Check if it's COUNT(*) by looking for a star
                if hasattr(agg_expr, "this") and isinstance(agg_expr.this, exp.Star):
                    return True
                elif isinstance(agg_expr, exp.Anonymous):
                    expressions = agg_expr.args.get("expressions", [])
                    if len(expressions) == 0:
                        return True
                elif isinstance(agg_expr, exp.AggFunc):
                    expressions = agg_expr.args.get("expressions", [])
                    if len(expressions) == 0:
                        return True
        return False

    def _execute_count_star_query(
        self, dataset: Dataset, aggregates: List[Tuple[str, exp.Expression]]
    ) -> Dataset:
        """Execute aggregate query with COUNT(*) using manual counting."""
        from ray.data.sql.utils import get_function_name_from_expression

        self._logger.debug("Handling COUNT(*) with manual row counting")
        result_row = {}
        total_rows = dataset.count()

        # Create proper column mapping for the dataset
        cols = list(dataset.columns()) if dataset else []
        column_mapping = create_column_mapping(cols, self.config.case_sensitive)

        for output_name, agg_expr in aggregates:
            func_name = get_function_name_from_expression(agg_expr).lower()
            target_column = self.aggregate_analyzer._extract_target_column(
                agg_expr, column_mapping
            )

            if func_name == "count":
                if target_column is None:
                    # COUNT(*) - count all rows
                    result_row[output_name] = total_rows
                else:
                    # COUNT(column) - count non-null values
                    non_null_count = dataset.filter(
                        lambda row: row.get(target_column) is not None
                    ).count()
                    result_row[output_name] = non_null_count
            else:
                # For other aggregates, use built-in
                aggregates, renames = self.aggregate_analyzer.build_aggregates(
                    [(output_name, agg_expr)], dataset
                )
                agg_result = dataset.aggregate(*aggregates)
                if isinstance(agg_result, dict):
                    result_row[output_name] = agg_result.get(list(agg_result.keys())[0])
                else:
                    result_row[output_name] = None

        return ray.data.from_items([result_row])

    def _execute_standard_aggregate_query(
        self, dataset: Dataset, aggregates: List[Tuple[str, exp.Expression]]
    ) -> Dataset:
        """Execute standard aggregate query using built-in aggregates."""
        aggregates, renames = self.aggregate_analyzer.build_aggregates(
            aggregates, dataset
        )

        self._logger.debug(f"Built {len(aggregates)} aggregates: {aggregates}")
        self._logger.debug(f"Column renames: {renames}")

        result = dataset.aggregate(*aggregates)

        self._logger.debug(f"Aggregate result type: {type(result)}")
        self._logger.debug(f"Aggregate result: {result}")

        if isinstance(result, dict):
            result = ray.data.from_items([result])
        elif result is None:
            result = self._create_empty_aggregate_result(aggregates)

        if renames and result is not None:
            try:
                self._logger.debug(f"Renaming columns: {renames}")
                result = result.rename_columns(renames)
            except Exception as e:
                self._logger.warning(f"Failed to rename columns: {e}")

        self._logger.debug(f"Final aggregate result: {result}")
        return result

    def _resolve_from_clause(self, ast: exp.Select) -> Tuple[Dataset, str]:
        """Resolve the FROM clause and return the dataset and table name."""
        from_clause = ast.args.get("from")
        if from_clause:
            if hasattr(from_clause, "expressions") and from_clause.expressions:
                table_expr = from_clause.expressions[0]
                table_name = str(table_expr.name)
            elif hasattr(from_clause, "this") and from_clause.this:
                table_expr = from_clause.this
                table_name = str(table_expr.name)
            else:
                raise ValueError("Invalid FROM clause")
            dataset = self.registry.get(table_name)
            return dataset, table_name

        default_dataset = self.registry.get_default_table()
        if not default_dataset:
            raise ValueError("No FROM clause specified and no default table available")
        table_name = getattr(default_dataset, "_sql_name", "default")
        return default_dataset, table_name

    def _create_empty_aggregate_result(
        self, aggregates: List[Tuple[str, exp.Expression]]
    ) -> Dataset:
        """Create a single-row result for aggregate queries on empty datasets."""
        from ray.data.sql.utils import get_function_name_from_expression

        result_row = {}
        for output_name, agg_expr in aggregates:
            func_name = get_function_name_from_expression(agg_expr).upper()
            if func_name == "COUNT":
                result_row[output_name] = 0
            elif func_name in ("SUM",):
                result_row[output_name] = 0
            elif func_name in ("MIN", "MAX", "AVG", "MEAN", "STD", "STDDEV"):
                result_row[output_name] = None
            else:
                result_row[output_name] = None
        return ray.data.from_items([result_row])
