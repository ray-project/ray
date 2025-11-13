"""Main query executor for Ray Data SQL API.

This module provides the QueryExecutor class that executes parsed SQL ASTs
against registered Ray Datasets, translating SQL operations into Ray Data
operations.

SQLGlot: SQL parser and AST manipulation
https://github.com/tobymao/sqlglot
"""

from typing import Any, Callable, Dict, List, Optional, Tuple

from sqlglot import exp

import ray
import ray.data.aggregate as agg_module
from ray.data import Dataset
from ray.data.experimental.sql.compiler import ExpressionConverter
from ray.data.experimental.sql.compiler.expressions import ExpressionCompiler
from ray.data.experimental.sql.config import SQLConfig
from ray.data.experimental.sql.exceptions import UnsupportedOperationError
from ray.data.experimental.sql.execution.types import JoinInfo
from ray.data.experimental.sql.registry.base import TableRegistry as DatasetRegistry
from ray.data.experimental.sql.utils import (
    AGGREGATE_MAPPING,
    SUPPORTED_AGGREGATES,
    create_column_mapping,
    extract_column_from_expression,
    get_function_name_from_expression,
    is_aggregate_function,
    normalize_identifier,
    normalize_join_type,
    safe_get_column,
    setup_logger,
)


class QueryExecutor:
    """Executes parsed SQL ASTs against registered Ray Datasets."""

    def __init__(self, registry: DatasetRegistry, config: SQLConfig):
        """Initialize the query executor."""
        self.registry = registry
        self.config = config
        self.converter = ExpressionConverter(config)
        self._logger = setup_logger("QueryExecutor")
        self._queries_executed = 0

    def execute(self, ast: exp.Expression) -> Dataset:
        """Execute a parsed SQLGlot AST (must be a SELECT statement)."""
        if not isinstance(ast, exp.Select):
            raise NotImplementedError(f"Unsupported SQL statement type: {type(ast).__name__ if ast else 'unknown'}. Ray Data SQL supports only SELECT queries.")
        self._queries_executed += 1
        group_keys = self._extract_group_by_keys(ast)
        result = self._execute_group_by_query(ast, group_keys) if group_keys else self._execute_simple_query(ast)
        self._logger.debug(f"Query executed successfully (total: {self._queries_executed})")
        return result

    def _execute_simple_query(self, ast: exp.Select) -> Dataset:
        """Execute a SELECT query without GROUP BY."""
        dataset, table_name = self._resolve_from_clause(ast)
        select_exprs = ast.args["expressions"]

        if self._has_only_literals(select_exprs):
            return self._execute_literal_query(ast, select_exprs)

        if ast.args.get("distinct"):
            raise UnsupportedOperationError("SELECT DISTINCT", suggestion="Ray Dataset API doesn't yet support deduplication.")

        return self._apply_query_operations(dataset, ast, table_name, select_exprs)

    def _execute_literal_query(self, ast: exp.Select, select_exprs: List[exp.Expression]) -> Dataset:
        """Execute a query that selects only literal values."""
        limit = self._extract_limit_value(ast)
        if limit <= 0:
            return ray.data.from_items([])
        if limit > 1_000_000:
            self._logger.warning(f"Large LIMIT value ({limit}) may cause memory issues")
        literal_row = self._build_literal_row(select_exprs)
        return ray.data.from_items([literal_row] * limit)

    def _extract_limit_value(self, ast: exp.Select) -> int:
        """Extract the LIMIT value from the AST."""
        limit_clause = ast.args.get("limit")
        return self._extract_limit_value_from_expr(getattr(limit_clause, "this", None)) if limit_clause else 1

    def _build_literal_row(self, select_exprs: List[exp.Expression]) -> Dict[str, Any]:
        """Build a row from literal expressions."""
        result_row = {}
        for idx, expr in enumerate(select_exprs):
            key = str(expr.alias) if isinstance(expr, exp.Alias) else f"col_{idx}"
            val_expr = expr.this if isinstance(expr, exp.Alias) else expr
            if isinstance(val_expr, exp.Literal):
                value = self.converter._parse_literal(val_expr)
            elif isinstance(val_expr, exp.Boolean):
                value = str(val_expr.name).lower() == "true"
            else:
                value = None
            result_row[key] = value
        return result_row

    def _apply_query_operations(self, dataset: Dataset, ast: exp.Select, table_name: str, select_exprs: List[exp.Expression]) -> Dataset:
        """Apply all query operations in the correct order."""
        dataset = self._apply_where_clause(self._apply_joins(dataset, ast), ast)
        if self._is_aggregate_only_query(select_exprs):
            return self._apply_limit(self._execute_aggregate_query(dataset, self._extract_aggregates(ast)), ast)
        column_names, funcs = self._analyze_projections(select_exprs, dataset, table_name)
        return self._apply_limit(self._apply_order_by(self._apply_projection(dataset, column_names, funcs), ast), ast)

    def _apply_joins(self, dataset: Dataset, ast: exp.Select) -> Dataset:
        """Apply all JOIN clauses in the SELECT AST."""
        for join_node in ast.find_all(exp.Join):
            dataset = self._execute_join(dataset, self._extract_join_info(join_node, dataset))
        return dataset

    def _extract_join_info(self, join_ast: exp.Join, left_dataset: Dataset) -> JoinInfo:
        """Extract join information from the JOIN AST."""
        if left_dataset is None:
            raise ValueError("Left dataset cannot be None for JOIN")
        if not hasattr(join_ast, "this") or not hasattr(join_ast.this, "name"):
            raise ValueError("JOIN right table name is missing")
        right_table_name = str(join_ast.this.name)
        right_dataset = self.registry.get(right_table_name)
        if right_dataset is None:
            raise ValueError(f"Right table '{right_table_name}' has None dataset")
        on_condition = join_ast.args.get("on")
        if not isinstance(on_condition, exp.EQ):
            raise NotImplementedError(f"Unsupported JOIN condition: {on_condition}. Ray Data SQL supports only equi-joins with = operator.")
        left_column = extract_column_from_expression(on_condition.left)
        right_column = extract_column_from_expression(on_condition.right)
        if not left_column or not right_column:
            raise ValueError(f"JOIN condition incomplete: left='{left_column}', right='{right_column}'.")
        left_cols = list(left_dataset.columns()) if hasattr(left_dataset, "columns") else []
        right_cols = list(right_dataset.columns()) if hasattr(right_dataset, "columns") else []
        if left_column not in left_cols and right_column not in right_cols:
            if left_column in right_cols and right_column in left_cols:
                left_column, right_column = right_column, left_column
            else:
                raise ValueError(f"JOIN condition uses invalid columns: '{left_column}' and '{right_column}'. Left table columns: {left_cols}. Right table ('{right_table_name}') columns: {right_cols}.")
        join_type_str = str(join_ast.args.get("side", "inner")).lower()
        join_type = normalize_join_type(join_type_str)
        num_partitions = max(1, min(self.config.max_join_partitions, 1000))
        return JoinInfo(left_table="left", right_table=right_table_name, left_columns=(left_column,), right_columns=(right_column,),
                       join_type=join_type, left_dataset=None, right_dataset=right_dataset, left_suffix="", right_suffix="_r",
                       num_partitions=num_partitions)

    def _execute_join(self, left_dataset: Dataset, join_info: JoinInfo) -> Dataset:
        """Execute the join operation.

        Args:
            left_dataset: Left dataset for the join.
            join_info: Join information including right dataset and join keys.

        Returns:
            Joined dataset.

        Raises:
            ValueError: If join columns are not found.
        """
        join_info.left_dataset = left_dataset
        left_columns = create_column_mapping(
            left_dataset.columns(), self.config.case_sensitive
        )
        right_columns = create_column_mapping(
            join_info.right_dataset.columns(), self.config.case_sensitive
        )

        left_keys = self._resolve_join_keys(
            join_info.left_columns, left_columns, "left"
        )
        right_keys = self._resolve_join_keys(
            join_info.right_columns, right_columns, "right"
        )

        return left_dataset.join(
            ds=join_info.right_dataset,
            join_type=join_info.join_type,
            num_partitions=join_info.num_partitions,
            on=tuple(left_keys),
            right_on=tuple(right_keys),
            left_suffix=join_info.left_suffix,
            right_suffix=join_info.right_suffix,
        )

    def _resolve_join_keys(
        self, columns: Tuple[str, ...], column_mapping: Dict[str, str], table_name: str
    ) -> List[str]:
        """Resolve join column names using column mapping.

        Args:
            columns: Join column names to resolve.
            column_mapping: Mapping from normalized to actual column names.
            table_name: Name of the table (for error messages).

        Returns:
            List of resolved column names.

        Raises:
            ValueError: If any join column is not found.
        """
        resolved = []
        for col in columns:
            try:
                resolved.append(self._resolve_column_name(col, column_mapping))
            except ValueError as e:
                raise ValueError(
                    f"Join key '{col}' not found in {table_name} table. "
                    f"Available columns: {list(column_mapping.values())}"
                ) from e
        return resolved

    def _apply_where_clause(self, dataset: Dataset, ast: exp.Select) -> Dataset:
        """Apply the WHERE clause filter to the dataset, if present."""
        where_clause = ast.args.get("where")
        return self._apply_having_clause(dataset, ast, where_clause) if where_clause else dataset

    def _apply_projection(
        self, dataset: Dataset, column_names: List[str], exprs: List[Any]
    ) -> Dataset:
        """Apply the SELECT projection to the dataset.

        Args:
            dataset: Input dataset.
            column_names: List of output column names.
            exprs: List of expressions or functions for each column.

        Returns:
            Dataset with projected columns.

        Raises:
            ValueError: If dataset is None or column/expression count mismatch.
        """
        if dataset is None:
            raise ValueError("Cannot apply projection to None dataset")
        if not column_names or not exprs:
            raise ValueError("Projection requires column names and expressions")
        if len(column_names) != len(exprs):
            raise ValueError(
                f"Column names ({len(column_names)}) and expressions "
                f"({len(exprs)}) count mismatch"
            )
        from ray.data.expressions import Expr as RayExpr

        ray_exprs = [e for e in exprs if isinstance(e, RayExpr)]
        if len(ray_exprs) == len(column_names) and ray_exprs:
            return self._apply_ray_exprs_projection(dataset, column_names, ray_exprs)
        if self._is_simple_column_projection(column_names, exprs):
            return dataset.select_columns(column_names)
        return self._apply_map_projection(dataset, column_names, exprs)

    def _apply_ray_exprs_projection(
        self, dataset: Dataset, column_names: List[str], ray_exprs: List[Any]
    ) -> Dataset:
        """Apply projection using Ray Data expressions."""
        for name, expr in zip(column_names, ray_exprs):
            dataset = dataset.with_column(name, expr)
        return dataset

    def _apply_map_projection(
        self, dataset: Dataset, column_names: List[str], exprs: List[Any]
    ) -> Dataset:
        """Apply projection using map operation."""
        from ray.data.expressions import Expr as RayExpr

        def project_row(row: Dict[str, Any]) -> Dict[str, Any]:
            result = {}
            for name, func in zip(column_names, exprs):
                if isinstance(func, RayExpr):
                    result[name] = func
                else:
                    result[name] = func(row) if callable(func) else func
            return result

        return dataset.map(project_row)

    def _is_simple_column_projection(self, column_names: List[str], exprs: List[Callable]) -> bool:
        """Check if this is a simple column selection without expressions."""
        return all(hasattr(expr, "__name__") and expr.__name__.startswith("get_") for expr in exprs)

    def _analyze_projections(self, select_exprs: List[exp.Expression], dataset: Dataset, table_name: Optional[str] = None) -> Tuple[List[str], List[Any]]:
        """Analyze the SELECT clause expressions and return column names and expressions."""
        column_names, exprs, cols = [], [], list(dataset.columns()) if dataset else []
        column_mapping = create_column_mapping(cols, self.config.case_sensitive)
        for idx, expr in enumerate(select_exprs):
            names, eval_exprs = self._analyze_single_expression(expr, idx, cols, column_mapping, table_name)
            column_names.extend(names)
            exprs.extend(eval_exprs)
        return column_names, exprs

    def _analyze_single_expression(self, expr: exp.Expression, index: int, cols: List[str], column_mapping: Dict[str, str], table_name: Optional[str]) -> Tuple[List[str], List[Any]]:
        """Analyze a single SELECT expression."""
        if isinstance(expr, exp.Star):
            return self._handle_star_expression(cols)
        if isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star):
            return self._handle_star_expression(cols, table_name)
        if isinstance(expr, exp.Alias):
            alias_name, expr_to_convert = str(expr.alias), expr.this
        elif isinstance(expr, exp.Column):
            return self._handle_column_expression(expr, column_mapping, cols)
        else:
            alias_name, expr_to_convert = f"col_{index}", expr
        try:
            return [alias_name], [self.converter.convert(expr_to_convert)]
        except UnsupportedOperationError:
            return [alias_name], [ExpressionCompiler(self.config).compile(expr_to_convert)]

    def _create_column_accessor(self, col_name: str) -> Callable:
        """Create a column accessor function."""
        return lambda row, col=col_name: safe_get_column(row, col, self.config.case_sensitive)

    def _handle_star_expression(self, cols: List[str], table_name: Optional[str] = None) -> Tuple[List[str], List[Callable]]:
        """Handle SELECT * or table.* expressions."""
        if not cols:
            if table_name:
                raise ValueError(f"Cannot expand {table_name}.* - no columns available")
            return [], []
        return cols[:], [self._create_column_accessor(col) for col in cols]

    def _handle_column_expression(self, expr: exp.Column, column_mapping: Dict[str, str], cols: List[str]) -> Tuple[List[str], List[Callable]]:
        """Handle SELECT column expressions."""
        col_name = str(expr.name).split(".")[-1] if "." in str(expr.name) else str(expr.name)
        actual_column = self._resolve_column_name(col_name, column_mapping)
        return [actual_column], [self._create_column_accessor(actual_column)]

    def _apply_order_by(self, dataset: Dataset, ast: exp.Select, select_names: Optional[List[str]] = None) -> Dataset:
        """Apply ORDER BY to the dataset."""
        order_clause = ast.args.get("order")
        if not order_clause:
            return dataset
        sort_info = self._extract_sort_info(order_clause, dataset, select_names)
        return self._execute_sort(dataset, sort_info) if sort_info else dataset

    def _resolve_column_name(self, col_name: str, column_mapping: Dict[str, str], select_names: Optional[List[str]] = None) -> Optional[str]:
        """Resolve a column name using column mapping."""
        if not col_name:
            raise ValueError("Column name cannot be empty")
        if not isinstance(col_name, str):
            raise ValueError(f"Column name must be a string, got {type(col_name)}")
        normalized = normalize_identifier(col_name, self.config.case_sensitive)
        actual_column = column_mapping.get(normalized)
        if not actual_column and select_names:
            return None
        if not actual_column:
            available = list(column_mapping.values()) if column_mapping else []
            raise ValueError(f"Column '{col_name}' not found. Available columns: {available}")
        return actual_column

    def _extract_sort_info(self, order_clause, dataset: Dataset, select_names: Optional[List[str]] = None) -> Optional[Tuple[List[str], List[bool]]]:
        """Extract sorting information from ORDER BY clause."""
        keys, desc, column_mapping = [], [], create_column_mapping(list(dataset.columns()) if hasattr(dataset, "columns") else [], self.config.case_sensitive)
        for ordering in order_clause.expressions:
            if not isinstance(ordering, exp.Ordered):
                continue
            column_name = self._extract_column_name(ordering.this, select_names)
            if column_name is None or (actual_column := self._resolve_column_name(column_name, column_mapping, select_names)) is None:
                return None
            keys.append(actual_column)
            desc.append(bool(ordering.args.get("desc", False)))
        return (keys, desc) if keys else None

    def _extract_column_name(self, sort_expr, select_names: Optional[List[str]] = None) -> Optional[str]:
        """Extract column name from sort expression."""
        def _get_col_name(obj):
            name = str(obj.name if isinstance(obj, exp.Column) else obj.this)
            return name.split(".")[-1] if "." in name else name
        if isinstance(sort_expr, (exp.Column, exp.Identifier)):
            return _get_col_name(sort_expr)
        if isinstance(sort_expr, exp.Literal):
            try:
                position = int(sort_expr.name) - 1
                if select_names and 0 <= position < len(select_names):
                    return select_names[position]
            except (ValueError, TypeError):
                pass
        return None

    def _execute_sort(self, dataset: Dataset, sort_info: Tuple[List[str], List[bool]]) -> Dataset:
        """Execute the sort operation."""
        keys, desc = sort_info
        return dataset.sort(keys[0] if len(keys) == 1 else keys, descending=desc[0] if len(keys) == 1 else desc)

    def _apply_limit(self, dataset: Dataset, ast: exp.Select) -> Dataset:
        """Apply LIMIT and OFFSET clauses to the dataset."""
        if dataset is None:
            raise ValueError("Cannot apply LIMIT to None dataset")
        limit_clause, offset_clause = ast.args.get("limit"), ast.args.get("offset")
        if not limit_clause and not offset_clause:
            return dataset

        limit_value = self._extract_limit_value_from_expr(getattr(limit_clause, "this", None)) if limit_clause else None
        if offset_clause:
            offset_value = self._extract_limit_value_from_expr(getattr(offset_clause, "this", None))
            if offset_value > 0:
                raise UnsupportedOperationError(
                    "OFFSET clause",
                    suggestion="OFFSET requires materialization. Use LIMIT only, or apply OFFSET after materialization with take()",
                )

        if limit_value is not None and limit_value >= 0:
            return dataset.limit(limit_value)
        return dataset

    def _extract_limit_value_from_expr(self, limit_expr) -> int:
        """Extract the limit value from the limit expression."""
        if limit_expr is None:
            return 0
        try:
            if isinstance(limit_expr, exp.Literal):
                value = int(str(limit_expr.name))
            elif isinstance(limit_expr, exp.Identifier):
                value = int(str(limit_expr.this))
            elif hasattr(limit_expr, "name"):
                value = int(str(limit_expr.name))
            else:
                value = int(limit_expr)
            if value < 0:
                raise ValueError(f"LIMIT value cannot be negative: {value}")
            if value > 2**31 - 1:
                raise ValueError(f"LIMIT value too large: {value}")
            return value
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid LIMIT expression: {limit_expr}") from e

    def _has_only_literals(self, select_exprs: List[exp.Expression]) -> bool:
        """Return True if all SELECT expressions are literals or booleans."""
        return all(isinstance(expr.this if isinstance(expr, exp.Alias) else expr, (exp.Literal, exp.Boolean)) for expr in select_exprs)

    def _is_aggregate_only_query(self, exprs: List[exp.Expression]) -> bool:
        """Return True if all SELECT expressions are aggregate functions."""
        return len(exprs) > 0 and all(is_aggregate_function(expr.this if isinstance(expr, exp.Alias) else expr) for expr in exprs)

    def _apply_renames(self, dataset: Dataset, renames: Dict[str, str]) -> Dataset:
        """Apply column renames if needed."""
        if not renames or not dataset:
            return dataset
        try:
            return dataset.rename_columns(renames)
        except Exception as e:
            self._logger.warning(f"Failed to rename columns: {e}")
            return dataset

    def _execute_group_by_query(self, ast: exp.Select, group_keys: List[str]) -> Dataset:
        """Execute a SELECT ... GROUP BY ... query."""
        dataset = self._apply_where_clause(self._apply_joins(self._resolve_from_clause(ast)[0], ast), ast)
        aggregates = self._extract_aggregates(ast)
        if not aggregates:
            raise ValueError("GROUP BY queries must include aggregate functions")
        keys, (aggs, renames) = self._resolve_group_keys(group_keys, dataset), self._build_aggregates(aggregates, dataset)
        result = self._apply_renames(dataset.groupby(keys[0] if len(keys) == 1 else keys).aggregate(*aggs), renames)
        return self._apply_limit(self._apply_order_by(self._apply_having_clause(result, ast), ast), ast)

    def _extract_group_by_keys(self, select_ast: exp.Select) -> List[str]:
        """Extract GROUP BY column names from the SELECT AST."""
        group_by = select_ast.args.get("group")
        if not group_by:
            return []
        keys, select_exprs = [], select_ast.args.get("expressions", [])
        for idx, expr in enumerate(group_by.expressions):
            if isinstance(expr, (exp.Column, exp.Identifier)):
                keys.append(str(expr.name if isinstance(expr, exp.Column) else expr.this))
            elif isinstance(expr, exp.Literal):
                try:
                    pos = int(str(expr.name)) - 1
                    if 0 <= pos < len(select_exprs):
                        sel_expr = select_exprs[pos]
                        keys.append(str(sel_expr.alias if isinstance(sel_expr, exp.Alias) else sel_expr.name if isinstance(sel_expr, exp.Column) else f"col_{pos}"))
                    else:
                        raise ValueError(f"GROUP BY position {pos + 1} out of range (SELECT has {len(select_exprs)} expressions)")
                except (ValueError, TypeError) as e:
                    raise ValueError(f"Invalid GROUP BY literal expression: {expr}") from e
            else:
                expr_str = str(expr)
                expr_hash = abs(hash(expr_str)) % 100000
                keys.append(f"expr_{expr_hash}")
        return keys

    def _resolve_group_keys(self, group_keys: List[str], dataset: Dataset) -> List[str]:
        """Resolve group by column names to actual column names."""
        column_mapping = create_column_mapping(list(dataset.columns()), self.config.case_sensitive)
        return [col for key in group_keys if (col := self._resolve_column_name(key, column_mapping))]

    def _execute_aggregate_query(
        self, dataset: Dataset, aggregates: List[Tuple[str, exp.Expression]]
    ) -> Dataset:
        """Execute a SELECT query with only aggregate functions (no GROUP BY)."""
        if not aggregates:
            raise ValueError("No aggregates found in aggregate-only query")
        return self._execute_count_star_query(dataset, aggregates) if self._has_count_star(aggregates) else self._execute_standard_aggregate_query(dataset, aggregates)

    def _has_count_star(self, aggregates: List[Tuple[str, exp.Expression]]) -> bool:
        """Check if any aggregate is COUNT(*)."""
        for _, agg_expr in aggregates:
            if get_function_name_from_expression(agg_expr).lower() == "count":
                if hasattr(agg_expr, "this") and isinstance(agg_expr.this, exp.Star):
                    return True
                expressions = agg_expr.args.get("expressions", []) if isinstance(agg_expr, (exp.Anonymous, exp.AggFunc)) else None
                if expressions == []:
                    return True
        return False

    def _execute_count_star_query(self, dataset: Dataset, aggregates: List[Tuple[str, exp.Expression]]) -> Dataset:
        """Execute aggregate query with COUNT(*) using manual counting."""
        if dataset is None:
            raise ValueError("Cannot execute aggregate query on None dataset")
        result_row = {}
        try:
            total_rows = dataset.count()
        except Exception as e:
            self._logger.warning(f"Failed to count dataset rows: {e}, using 0")
            total_rows = 0
        column_mapping = create_column_mapping(list(dataset.columns()) if hasattr(dataset, "columns") else [], self.config.case_sensitive)
        for output_name, agg_expr in aggregates:
            func_name = get_function_name_from_expression(agg_expr).lower()
            try:
                target_column = self._extract_target_column(agg_expr, column_mapping)
            except Exception as e:
                self._logger.warning(f"Failed to extract target column for {func_name}: {e}")
                target_column = None
            if func_name == "count" and target_column is None:
                result_row[output_name] = total_rows
            elif func_name == "count" and target_column:
                try:
                    agg_result = dataset.aggregate(agg_module.Count(target_column))
                    result_row[output_name] = agg_result.get(f"count({target_column})", 0) if isinstance(agg_result, dict) else 0
                except Exception as e:
                    self._logger.warning(f"Failed to aggregate COUNT({target_column}): {e}")
                    result_row[output_name] = 0
            else:
                try:
                    aggs, _ = self._build_aggregates([(output_name, agg_expr)], dataset)
                    agg_result = dataset.aggregate(*aggs)
                    result_row[output_name] = (agg_result.get(list(agg_result.keys())[0]) if isinstance(agg_result, dict) and agg_result else None)
                except Exception as e:
                    self._logger.warning(f"Failed to aggregate {func_name}: {e}")
                    result_row[output_name] = None
        return ray.data.from_items([result_row])

    def _execute_standard_aggregate_query(self, dataset: Dataset, aggregates: List[Tuple[str, exp.Expression]]) -> Dataset:
        """Execute standard aggregate query using built-in aggregates."""
        aggs, renames = self._build_aggregates(aggregates, dataset)
        result = dataset.aggregate(*aggs)
        result = ray.data.from_items([result]) if isinstance(result, dict) else (self._create_empty_aggregate_result(aggregates) if result is None else result)
        return self._apply_renames(result, renames)

    def _extract_aggregates(self, select_ast: exp.Select) -> List[Tuple[str, exp.Expression]]:
        """Extract aggregate expressions and their output names from the SELECT AST."""
        aggregates = []
        for expr in select_ast.args["expressions"]:
            agg_expr = expr.this if isinstance(expr, exp.Alias) else expr
            if is_aggregate_function(agg_expr):
                aggregates.append((str(expr.alias) if isinstance(expr, exp.Alias) else self._generate_aggregate_name(expr), agg_expr))
        return aggregates

    def _build_aggregates(self, aggs: List[Tuple[str, exp.Expression]], dataset: Dataset) -> Tuple[List[Any], Dict[str, str]]:
        """Build aggregate objects and a column rename mapping."""
        aggregates, renames = [], {}
        column_mapping = create_column_mapping(list(dataset.columns()) if dataset else [], self.config.case_sensitive)
        for output_name, agg_expr in aggs:
            agg, col_name = self._build_single_aggregate(agg_expr, column_mapping, agg_module)
            aggregates.append(agg)
            if output_name != col_name:
                renames[col_name] = output_name
        return aggregates, renames

    def _build_single_aggregate(self, agg_expr: exp.Expression, column_mapping: Dict[str, str], agg_module) -> Tuple[Any, str]:
        """Build a single aggregate object."""
        func_name = get_function_name_from_expression(agg_expr).lower()
        if func_name not in SUPPORTED_AGGREGATES:
            raise NotImplementedError(f"Aggregate function '{func_name.upper()}' not supported")
        target_column, agg_class = self._extract_target_column(agg_expr, column_mapping), getattr(agg_module, AGGREGATE_MAPPING[func_name])
        if func_name == "count":
            aggregate, column_name = (agg_class(), "count()") if target_column is None else (agg_class(target_column), f"count({target_column})")
        elif target_column:
            aggregate, column_name = agg_class(target_column), f"{AGGREGATE_MAPPING[func_name].lower()}({target_column})"
        else:
            raise ValueError(f"Invalid aggregate specification: {agg_expr}")
        return aggregate, column_name

    def _extract_target_column(self, agg_expr: exp.Expression, column_mapping: Dict[str, str]) -> Optional[str]:
        """Extract the target column name for an aggregate function, or None for COUNT(*)."""
        def _resolve_column(col_expr: exp.Column) -> str:
            return self._resolve_column_name(str(col_expr.name).split(".")[-1] if "." in str(col_expr.name) else str(col_expr.name), column_mapping)
        if hasattr(agg_expr, "this") and agg_expr.this:
            if isinstance(agg_expr.this, exp.Column):
                return _resolve_column(agg_expr.this)
            if isinstance(agg_expr.this, exp.Star):
                return None
        expressions = agg_expr.args.get("expressions", []) if isinstance(agg_expr, (exp.Anonymous, exp.AggFunc)) else None
        if expressions is not None:
            if len(expressions) == 0:
                return None
            if len(expressions) == 1:
                if isinstance(expressions[0], exp.Column):
                    return _resolve_column(expressions[0])
                raise NotImplementedError(f"Complex aggregate arguments not supported: {expressions[0]}")
            raise NotImplementedError(f"Multi-argument aggregates not supported: {agg_expr}")
        raise ValueError(f"Unexpected aggregate expression type: {type(agg_expr)}")

    def _generate_aggregate_name(self, agg_expr: exp.Expression) -> str:
        """Generate a default output column name for an aggregate expression."""
        func_name, target = get_function_name_from_expression(agg_expr).upper(), agg_expr.args.get("this")
        if isinstance(target, exp.Star):
            return f"{func_name}(*)"
        if isinstance(target, exp.Column):
            return f"{func_name}({str(target.name)})"
        return f"{func_name}({target})" if target else f"{func_name}()"

    def _create_empty_aggregate_result(self, aggregates: List[Tuple[str, exp.Expression]]) -> Dataset:
        """Create a single-row result for aggregate queries on empty datasets."""
        return ray.data.from_items([{name: 0 if get_function_name_from_expression(expr).upper() in ("COUNT", "SUM") else None for name, expr in aggregates}])

    def _apply_having_clause(self, dataset: Dataset, ast: exp.Select, clause=None) -> Dataset:
        """Apply HAVING or WHERE clause filtering."""
        clause = clause or ast.args.get("having")
        if not clause:
            return dataset
        try:
            return dataset.filter(expr=self.converter.convert(clause.this))
        except UnsupportedOperationError:
            return dataset.filter(lambda row: bool(ExpressionCompiler(self.config).compile(clause.this)(row)))

    def _resolve_from_clause(self, ast: exp.Select) -> Tuple[Dataset, str]:
        """Resolve the FROM clause and return the dataset and table name."""
        from_clause = ast.args.get("from")
        if not from_clause:
            available = list(self.registry.list_tables())
            raise ValueError(f"No FROM clause specified. Registered tables: {available}.")
        table_expr = (from_clause.expressions[0] if hasattr(from_clause, "expressions") and from_clause.expressions
                     else from_clause.this if hasattr(from_clause, "this") and from_clause.this else None)
        if not table_expr:
            available = list(self.registry.list_tables())
            raise ValueError(f"No FROM clause specified. Registered tables: {available}.")
        if isinstance(table_expr, exp.Subquery):
            if not isinstance(table_expr.this, exp.Select):
                raise UnsupportedOperationError(f"Subquery with {type(table_expr.this).__name__} statement", suggestion="Only SELECT subqueries are supported in FROM clause")
            alias = str(table_expr.alias) if table_expr.alias else f"subquery_{id(table_expr)}"
            result = self.execute(table_expr.this)
            if result is None:
                raise ValueError(f"Subquery '{alias}' returned None result")
            self.registry.register(alias, result)
            return result, alias
        table_name = str(table_expr.name) if hasattr(table_expr, "name") else None
        if not table_name:
            raise ValueError("FROM clause table name is missing")
        dataset = self.registry.get(table_name)
        if dataset is None:
            raise ValueError(f"Table '{table_name}' has None dataset")
        return dataset, table_name
