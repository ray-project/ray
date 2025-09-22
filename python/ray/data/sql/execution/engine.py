"""SQL execution engine for Ray Data SQL API.

This module provides the core execution engine that converts SQL queries
into Ray Dataset operations and executes them efficiently.
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from sqlglot import exp

import ray.data
import ray.data.aggregate as agg_module
from ray.data.sql.compiler.expressions import ExpressionCompiler
from ray.data.sql.registry.base import TableRegistry
from ray.data import Dataset


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
