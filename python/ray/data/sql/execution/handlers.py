"""
Query operation handlers for Ray Data SQL API.

This module provides handlers for various SQL operations including JOINs,
WHERE clauses, ORDER BY, and LIMIT operations.
"""

from typing import Any, Dict, List, Optional, Tuple, Union

import ray
from ray.data import Dataset
from sqlglot import exp
from ray.data.sql.compiler import ExpressionCompiler
from ray.data.sql.config import JoinInfo, SQLConfig
from ray.data.sql.schema import DatasetRegistry
from ray.data.sql.utils import (
    create_column_mapping,
    extract_column_from_expression,
    normalize_identifier,
    normalize_join_type,
    setup_logger,
)


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

    def __init__(self, config: SQLConfig):
        self.config = config
        self._logger = setup_logger("JoinHandler")

    def apply_joins(
        self, dataset: Dataset, ast: exp.Select, registry: DatasetRegistry
    ) -> Dataset:
        """Apply all JOIN clauses in the SELECT AST to the dataset."""
        for join_node in ast.find_all(exp.Join):
            dataset = self.apply_single_join(dataset, join_node, registry)
        return dataset

    def apply_single_join(
        self, left_dataset: Dataset, join_ast: exp.Join, registry: DatasetRegistry
    ) -> Dataset:
        """Apply a single JOIN to the left_dataset."""
        join_info = self._extract_join_info(join_ast, registry, left_dataset)
        return self._execute_join(left_dataset, join_info)

    def _extract_join_info(
        self, join_ast: exp.Join, registry: DatasetRegistry, left_dataset: Dataset
    ) -> JoinInfo:
        """Extract join information from the JOIN AST."""
        right_table_name = str(join_ast.this.name)
        right_dataset = registry.get(right_table_name)
        join_kind = str(join_ast.args.get("side", "inner")).lower()
        ray_join_type = normalize_join_type(join_kind)

        on_condition = join_ast.args.get("on")
        if not isinstance(on_condition, exp.EQ):
            raise NotImplementedError(
                "Only equi-joins (ON left.col = right.col) are supported"
            )

        # Extract column names from join condition
        left_column = extract_column_from_expression(on_condition.left)
        right_column = extract_column_from_expression(on_condition.right)

        # Add debug logging
        self._logger.debug(f"Join condition: {on_condition}")
        self._logger.debug(f"Left side: {on_condition.left} -> {left_column}")
        self._logger.debug(f"Right side: {on_condition.right} -> {right_column}")

        # Check if the left and right sides are swapped
        left_table_columns = (
            left_dataset.columns() if hasattr(left_dataset, "columns") else []
        )
        right_table_columns = (
            right_dataset.columns() if hasattr(right_dataset, "columns") else []
        )

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
            num_partitions=self.config.max_join_partitions,
        )

    def _execute_join(self, left_dataset: Dataset, join_info: JoinInfo) -> Dataset:
        """Execute the join operation."""
        join_info.left_dataset = left_dataset

        # Resolve column names with case sensitivity for all join columns
        left_columns = create_column_mapping(
            left_dataset.columns(), self.config.case_sensitive
        )
        right_columns = create_column_mapping(
            join_info.right_dataset.columns(), self.config.case_sensitive
        )

        # Resolve all left join columns
        resolved_left_columns = []
        for col in join_info.left_columns:
            normalized = normalize_identifier(col, self.config.case_sensitive)
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
            normalized = normalize_identifier(col, self.config.case_sensitive)
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

        # Use Ray Dataset API join method for ALL join types
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


class FilterHandler:
    """Handles WHERE clause filtering operations.

    The FilterHandler processes WHERE clauses in SQL queries and applies
    the corresponding filters to Ray Datasets. All operations follow Ray
    Dataset API patterns for lazy evaluation and proper return types.

    Examples:
        .. testcode::

            config = SQLConfig()
            handler = FilterHandler(config)
            filtered = handler.apply_where_clause(dataset, ast)
    """

    def __init__(self, config: SQLConfig):
        self.config = config
        self.compiler = ExpressionCompiler(config)
        self._logger = setup_logger("FilterHandler")

    def apply_where_clause(self, dataset: Dataset, ast: exp.Select) -> Dataset:
        """Apply the WHERE clause filter to the dataset, if present."""
        where_clause = ast.args.get("where")
        if not where_clause:
            return dataset
        try:
            filter_expr = self.compiler.compile(where_clause.this)
            return dataset.filter(filter_expr)
        except Exception as e:
            self._logger.error(f"WHERE clause evaluation failed: {e}")
            raise ValueError(f"Invalid WHERE clause: {e}")


class OrderHandler:
    """Handles ORDER BY operations.

    The OrderHandler processes ORDER BY clauses in SQL queries and applies
    the corresponding sorting to Ray Datasets. All operations follow Ray
    Dataset API patterns for lazy evaluation and proper return types.

    Examples:
        .. testcode::

            config = SQLConfig()
            handler = OrderHandler(config)
            sorted_dataset = handler.apply_order_by(dataset, ast)
    """

    def __init__(self, config: SQLConfig):
        self.config = config
        self._logger = setup_logger("OrderHandler")

    def apply_order_by(
        self,
        dataset: Dataset,
        ast: exp.Select,
        select_names: Optional[List[str]] = None,
    ) -> Union[Dataset, str]:
        """Apply ORDER BY to the dataset."""
        order_clause = ast.args.get("order")
        if not order_clause:
            return dataset

        sort_info = self._extract_sort_info(order_clause, dataset, select_names)
        if not sort_info:
            return dataset

        return self._execute_sort(dataset, sort_info)

    def _extract_sort_info(
        self, order_clause, dataset: Dataset, select_names: Optional[List[str]] = None
    ) -> Optional[Tuple[List[str], List[bool]]]:
        """Extract sorting information from ORDER BY clause."""
        keys = []
        desc = []

        for ordering in order_clause.expressions:
            if not isinstance(ordering, exp.Ordered):
                continue

            sort_expr = ordering.this
            is_descending = bool(ordering.args.get("desc", False))

            column_name = self._extract_column_name(sort_expr, select_names)
            if column_name is None:
                return None

            cols = list(dataset.columns()) if hasattr(dataset, "columns") else []
            column_mapping = create_column_mapping(cols, self.config.case_sensitive)
            normalized_name = normalize_identifier(
                column_name, self.config.case_sensitive
            )
            actual_column = column_mapping.get(normalized_name)

            if not actual_column and select_names:
                return None
            elif not actual_column:
                raise ValueError(f"ORDER BY column '{column_name}' not found")

            keys.append(actual_column)
            desc.append(is_descending)

        return (keys, desc) if keys else None

    def _extract_column_name(
        self, sort_expr, select_names: Optional[List[str]] = None
    ) -> Optional[str]:
        """Extract column name from sort expression."""
        if isinstance(sort_expr, exp.Column):
            col_name = str(sort_expr.name)
            # Handle qualified column names (table.column) by extracting just the column part
            if "." in col_name:
                col_name = col_name.split(".")[-1]
            return col_name
        elif isinstance(sort_expr, exp.Identifier):
            col_name = str(sort_expr.this)
            # Handle qualified column names (table.column) by extracting just the column part
            if "." in col_name:
                col_name = col_name.split(".")[-1]
            return col_name
        elif isinstance(sort_expr, exp.Literal):
            # Handle ORDER BY position (e.g., ORDER BY 1, 2)
            try:
                position = int(sort_expr.name) - 1  # Convert to 0-based index
                if select_names and 0 <= position < len(select_names):
                    return select_names[position]
            except (ValueError, TypeError):
                pass
        return None

    def _execute_sort(
        self, dataset: Dataset, sort_info: Tuple[List[str], List[bool]]
    ) -> Dataset:
        """Execute the sort operation."""
        keys, desc = sort_info

        try:
            if len(keys) == 1:
                return dataset.sort(keys[0], descending=desc[0])
            else:
                return dataset.sort(keys, descending=desc)
        except Exception as e:
            self._logger.error(f"ORDER BY failed: {e}")
            raise


class LimitHandler:
    """Handles LIMIT operations.

    The LimitHandler processes LIMIT clauses in SQL queries and applies
    the corresponding row limiting to Ray Datasets. All operations follow
    Ray Dataset API patterns for lazy evaluation and proper return types.

    Examples:
        .. testcode::

            config = SQLConfig()
            handler = LimitHandler(config)
            limited = handler.apply_limit(dataset, ast)
    """

    def __init__(self, config: SQLConfig):
        self.config = config
        self._logger = setup_logger("LimitHandler")

    def apply_limit(self, dataset: Dataset, ast: exp.Select) -> Dataset:
        """Apply LIMIT clause to the dataset, if present."""
        limit_clause = ast.args.get("limit")
        if not limit_clause:
            return dataset

        limit_expr = getattr(limit_clause, "this", None)
        if limit_expr is None:
            return dataset

        limit_value = self._extract_limit_value(limit_expr)
        if limit_value <= 0:
            return ray.data.from_items([])

        self._logger.debug(f"Applying LIMIT {limit_value}")

        # Use Ray's built-in limit method
        try:
            result = dataset.limit(limit_value)
            self._logger.debug(f"LIMIT applied successfully")
            return result
        except Exception as e:
            self._logger.warning(f"Failed to apply LIMIT using limit(): {e}")
            # Fallback to take() and from_items() if limit() fails
            try:
                limited_rows = dataset.take(limit_value)
                result = ray.data.from_items(limited_rows)
                self._logger.debug(f"LIMIT applied using take()")
                return result
            except Exception as e2:
                self._logger.warning(f"Failed to apply LIMIT using take(): {e2}")
                # Fallback to original dataset if both methods fail
                return dataset

    def _extract_limit_value(self, limit_expr) -> int:
        """Extract the limit value from the limit expression."""
        try:
            if isinstance(limit_expr, exp.Literal):
                return int(str(limit_expr.name))
            elif isinstance(limit_expr, exp.Identifier):
                return int(str(limit_expr.this))
            elif hasattr(limit_expr, "name"):
                return int(str(limit_expr.name))
            else:
                return int(limit_expr)
        except (ValueError, TypeError) as e:
            self._logger.warning(f"Invalid LIMIT value: {limit_expr} - {e}")
            return 0
