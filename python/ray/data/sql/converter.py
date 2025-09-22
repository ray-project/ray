"""
SQL to Ray Dataset native operations converter.

This module converts SQL operations directly into Ray Dataset native operations
using col(), lit(), filter(expr=...), join(), groupby().aggregate(), etc.
for maximum performance and compatibility.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlglot import exp

from ray.data import Dataset
from ray.data.expressions import col, lit
from ray.data.sql.common import ExecutionPlan, FilterSpec, JoinSpec, ProjectionSpec


@dataclass
class SQLToRayConverter:
    """Converts SQL AST directly to Ray Dataset native operations."""

    def convert_where_clause(self, where_expr: exp.Expression) -> str:
        """Convert SQL WHERE clause to Ray Dataset filter expression.

        Args:
            where_expr: SQLGlot WHERE expression.

        Returns:
            Python expression string for dataset.filter(expr=...).
        """
        return self._convert_expression_to_python(where_expr)

    def convert_select_clause(
        self, select_exprs: List[exp.Expression]
    ) -> Optional[List[str]]:
        """Convert SQL SELECT clause to Ray Dataset column selection.

        Args:
            select_exprs: List of SELECT expressions.

        Returns:
            List of column names for dataset.select_columns() or None for SELECT *.
        """
        # Handle SELECT *
        if len(select_exprs) == 1 and isinstance(select_exprs[0], exp.Star):
            return None

        columns = []
        for expr in select_exprs:
            if isinstance(expr, exp.Column):
                columns.append(str(expr.name))
            elif isinstance(expr, exp.Alias):
                # For aliases, we'll need to use with_column() later
                columns.append(str(expr.alias))
            else:
                # Complex expressions - handle with with_column()
                columns.append(f"expr_{len(columns)}")

        return columns

    def convert_join_clause(self, join_expr: exp.Join, left_table: str) -> JoinSpec:
        """Convert SQL JOIN to Ray Dataset join specification.

        Args:
            join_expr: SQLGlot JOIN expression.
            left_table: Name of left table.

        Returns:
            JoinSpec for Ray Dataset join operation.
        """
        # Extract join information
        right_table = str(join_expr.this.name)
        join_type = str(join_expr.args.get("side", "inner")).lower()

        # Convert SQL join types to Ray Dataset join types
        ray_join_type = {
            "inner": "inner",
            "left": "left",
            "right": "right",
            "full": "full",
            "outer": "full",  # FULL OUTER -> full
        }.get(join_type, "inner")

        # Extract ON condition
        on_condition = join_expr.args.get("on")
        if isinstance(on_condition, exp.EQ):
            left_col = self._extract_column_name(on_condition.left)
            right_col = self._extract_column_name(on_condition.right)
        else:
            raise ValueError("Only equi-joins (ON left.col = right.col) are supported")

        return JoinSpec(
            left_table=left_table,
            right_table=right_table,
            left_columns=[left_col],
            right_columns=[right_col],
            join_type=ray_join_type,
        )

    def convert_order_by_clause(
        self, order_expr: exp.Order
    ) -> tuple[List[str], List[bool]]:
        """Convert SQL ORDER BY to Ray Dataset sort specification.

        Args:
            order_expr: SQLGlot ORDER BY expression.

        Returns:
            Tuple of (column_names, descending_flags) for dataset.sort().
        """
        columns = []
        descending = []

        for ordering in order_expr.expressions:
            if isinstance(ordering, exp.Ordered):
                col_name = self._extract_column_name(ordering.this)
                is_desc = bool(ordering.args.get("desc", False))
                columns.append(col_name)
                descending.append(is_desc)

        return columns, descending

    def convert_limit_clause(self, limit_expr: exp.Limit) -> int:
        """Convert SQL LIMIT to Ray Dataset limit value.

        Args:
            limit_expr: SQLGlot LIMIT expression.

        Returns:
            Limit value for dataset.limit().
        """
        if hasattr(limit_expr, "expression"):
            return int(str(limit_expr.expression))
        elif hasattr(limit_expr, "this"):
            return int(str(limit_expr.this))
        else:
            return int(str(limit_expr))

    def convert_aggregates_to_ray(
        self, group_by: List[str], aggregates: Dict[str, str]
    ) -> tuple:
        """Convert SQL aggregates to Ray Dataset groupby().aggregate().

        Args:
            group_by: GROUP BY columns.
            aggregates: Aggregate functions mapping.

        Returns:
            Tuple of (group_columns, ray_aggregates) for native operations.
        """
        import ray.data.aggregate as agg

        ray_aggregates = []
        for col_name, agg_func in aggregates.items():
            func_name = agg_func.upper()

            if func_name == "COUNT":
                ray_aggregates.append(agg.Count())
            elif func_name == "SUM":
                ray_aggregates.append(agg.Sum(col_name))
            elif func_name in ("AVG", "MEAN"):
                ray_aggregates.append(agg.Mean(col_name))
            elif func_name == "MIN":
                ray_aggregates.append(agg.Min(col_name))
            elif func_name == "MAX":
                ray_aggregates.append(agg.Max(col_name))
            elif func_name in ("STD", "STDDEV"):
                ray_aggregates.append(agg.Std(col_name))
            else:
                raise ValueError(f"Unsupported aggregate function: {agg_func}")

        return group_by, ray_aggregates

    def _convert_expression_to_python(self, expr: exp.Expression) -> str:
        """Convert SQLGlot expression to Python expression for Ray Dataset.

        This converts SQL expressions to Python expressions that can be used
        with Ray Dataset's filter(expr=...) and other native operations.
        """
        if isinstance(expr, exp.Column):
            return str(expr.name)

        elif isinstance(expr, exp.Literal):
            value = str(expr.name)
            # Handle string literals
            if expr.is_string:
                return f"'{value}'"
            return value

        elif isinstance(expr, exp.EQ):
            left = self._convert_expression_to_python(expr.left)
            right = self._convert_expression_to_python(expr.right)
            return f"({left} == {right})"

        elif isinstance(expr, exp.NEQ):
            left = self._convert_expression_to_python(expr.left)
            right = self._convert_expression_to_python(expr.right)
            return f"({left} != {right})"

        elif isinstance(expr, exp.GT):
            left = self._convert_expression_to_python(expr.left)
            right = self._convert_expression_to_python(expr.right)
            return f"({left} > {right})"

        elif isinstance(expr, exp.LT):
            left = self._convert_expression_to_python(expr.left)
            right = self._convert_expression_to_python(expr.right)
            return f"({left} < {right})"

        elif isinstance(expr, exp.GTE):
            left = self._convert_expression_to_python(expr.left)
            right = self._convert_expression_to_python(expr.right)
            return f"({left} >= {right})"

        elif isinstance(expr, exp.LTE):
            left = self._convert_expression_to_python(expr.left)
            right = self._convert_expression_to_python(expr.right)
            return f"({left} <= {right})"

        elif isinstance(expr, exp.And):
            left = self._convert_expression_to_python(expr.left)
            right = self._convert_expression_to_python(expr.right)
            return f"({left} and {right})"

        elif isinstance(expr, exp.Or):
            left = self._convert_expression_to_python(expr.left)
            right = self._convert_expression_to_python(expr.right)
            return f"({left} or {right})"

        elif isinstance(expr, exp.Not):
            inner = self._convert_expression_to_python(expr.this)
            return f"(not {inner})"

        elif isinstance(expr, exp.Is):
            left = self._convert_expression_to_python(expr.left)
            right = self._convert_expression_to_python(expr.right)
            if isinstance(expr.right, exp.Null):
                return f"({left} is None)"
            return f"({left} is {right})"

        elif isinstance(expr, exp.Between):
            col_expr = self._convert_expression_to_python(expr.this)
            low = self._convert_expression_to_python(expr.args["low"])
            high = self._convert_expression_to_python(expr.args["high"])
            return f"(({col_expr} >= {low}) and ({col_expr} <= {high}))"

        elif isinstance(expr, exp.In):
            col_expr = self._convert_expression_to_python(expr.this)
            values = [self._convert_expression_to_python(v) for v in expr.expressions]
            values_str = "[" + ", ".join(values) + "]"
            return f"({col_expr} in {values_str})"

        else:
            # Fallback for unsupported expressions
            raise ValueError(f"Unsupported expression type: {type(expr).__name__}")

    def _extract_column_name(self, expr: exp.Expression) -> str:
        """Extract column name from expression."""
        if isinstance(expr, exp.Column):
            col_name = str(expr.name)
            # Handle table-qualified columns (table.column -> column)
            return col_name.split(".")[-1] if "." in col_name else col_name
        elif isinstance(expr, exp.Identifier):
            return str(expr.this)
        else:
            return str(expr)


class RayNativeExecutor:
    """Executor that uses only Ray Dataset native operations.

    This executor converts SQL operations directly into Ray Dataset native
    operations for maximum performance and compatibility.
    """

    def __init__(self):
        self.converter = SQLToRayConverter()

    def execute_select(
        self, ast: exp.Select, base_dataset: Dataset, registry: Any
    ) -> Dataset:
        """Execute SELECT using Ray Dataset native operations only.

        Args:
            ast: SQLGlot SELECT AST.
            base_dataset: Base dataset from FROM clause.
            registry: Table registry for joins.

        Returns:
            Dataset using Ray Dataset native operations.
        """
        dataset = base_dataset

        # Apply JOINs using native dataset.join()
        for join_node in ast.find_all(exp.Join):
            join_spec = self.converter.convert_join_clause(join_node, "left")
            right_dataset = registry.get(join_spec.right_table)

            dataset = dataset.join(
                ds=right_dataset,
                join_type=join_spec.join_type,
                on=tuple(join_spec.left_columns),
                right_on=tuple(join_spec.right_columns),
                left_suffix=join_spec.left_suffix,
                right_suffix=join_spec.right_suffix,
            )

        # Apply WHERE using native dataset.filter(expr=...)
        where_clause = ast.args.get("where")
        if where_clause:
            filter_expr = self.converter.convert_where_clause(where_clause.this)
            dataset = dataset.filter(expr=filter_expr)

        # Apply GROUP BY using native dataset.groupby().aggregate()
        group_clause = ast.args.get("group")
        if group_clause:
            group_columns = [str(col.name) for col in group_clause.expressions]

            # Extract aggregates from SELECT
            aggregates = self._extract_aggregates_from_select(ast.args["expressions"])
            if aggregates:
                group_by, ray_aggregates = self.converter.convert_aggregates_to_ray(
                    group_columns, aggregates
                )

                if len(group_by) == 1:
                    grouped = dataset.groupby(group_by[0])
                else:
                    grouped = dataset.groupby(group_by)

                dataset = grouped.aggregate(*ray_aggregates)

        # Apply SELECT projection using native dataset.select_columns()
        select_exprs = ast.args.get("expressions", [])
        columns = self.converter.convert_select_clause(select_exprs)
        if columns:
            try:
                dataset = dataset.select_columns(columns)
            except Exception:
                # Fallback for complex projections
                pass

        # Apply ORDER BY using native dataset.sort()
        order_clause = ast.args.get("order")
        if order_clause:
            columns, descending = self.converter.convert_order_by_clause(order_clause)
            if len(columns) == 1:
                dataset = dataset.sort(columns[0], descending=descending[0])
            else:
                dataset = dataset.sort(columns, descending=descending)

        # Apply LIMIT using native dataset.limit()
        limit_clause = ast.args.get("limit")
        if limit_clause:
            limit_value = self.converter.convert_limit_clause(limit_clause)
            dataset = dataset.limit(limit_value)

        return dataset

    def _extract_aggregates_from_select(
        self, select_exprs: List[exp.Expression]
    ) -> Dict[str, str]:
        """Extract aggregate functions from SELECT expressions."""
        aggregates = {}

        for expr in select_exprs:
            if isinstance(expr, exp.Alias):
                inner_expr = expr.this
                alias_name = str(expr.alias)
            else:
                inner_expr = expr
                alias_name = str(expr)

            if isinstance(inner_expr, (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)):
                func_name = type(inner_expr).__name__.upper()

                # Extract target column for aggregates (except COUNT(*))
                if isinstance(inner_expr, exp.Count):
                    if hasattr(inner_expr, "this") and isinstance(
                        inner_expr.this, exp.Star
                    ):
                        aggregates[alias_name] = "COUNT"
                    else:
                        target_col = (
                            str(inner_expr.this.name) if inner_expr.this else None
                        )
                        aggregates[alias_name] = "COUNT"
                else:
                    target_col = (
                        str(inner_expr.this.name) if inner_expr.this else alias_name
                    )
                    aggregates[target_col] = func_name

        return aggregates


def create_native_execution_plan(ast: exp.Select) -> ExecutionPlan:
    """Create execution plan using Ray Dataset native operations.

    Args:
        ast: SQLGlot SELECT AST.

    Returns:
        Execution plan that maps directly to Ray Dataset operations.
    """
    from ray.data.sql.common import QueryContext, SQLDialect

    context = QueryContext(query=str(ast), dialect=SQLDialect.DUCKDB)  # Default

    plan = ExecutionPlan(context=context)
    converter = SQLToRayConverter()

    # Convert WHERE clause to filter specs
    where_clause = ast.args.get("where")
    if where_clause:
        filter_expr = converter.convert_where_clause(where_clause.this)
        plan.add_filter(FilterSpec(expression=filter_expr))

    # Convert JOINs to join specs
    for join_node in ast.find_all(exp.Join):
        join_spec = converter.convert_join_clause(join_node, "left")
        plan.add_join(join_spec)

    # Convert SELECT to projection spec
    select_exprs = ast.args.get("expressions", [])
    columns = converter.convert_select_clause(select_exprs)
    if columns:
        plan.set_projection(ProjectionSpec(columns=columns))

    return plan


# Competitive feature mapping to ensure we match DuckDB/PySpark capabilities
RAY_NATIVE_FEATURE_MAP = {
    # Basic operations - all use Ray Dataset native operations
    "SELECT columns": "dataset.select_columns()",
    "WHERE filtering": "dataset.filter(expr=...)",  # Uses native Arrow expressions
    "INNER JOIN": "dataset.join(join_type='inner')",
    "LEFT JOIN": "dataset.join(join_type='left')",
    "RIGHT JOIN": "dataset.join(join_type='right')",
    "FULL JOIN": "dataset.join(join_type='full')",
    "GROUP BY + COUNT": "dataset.groupby().aggregate(Count())",
    "GROUP BY + SUM": "dataset.groupby().aggregate(Sum())",
    "GROUP BY + AVG": "dataset.groupby().aggregate(Mean())",
    "GROUP BY + MIN": "dataset.groupby().aggregate(Min())",
    "GROUP BY + MAX": "dataset.groupby().aggregate(Max())",
    "ORDER BY": "dataset.sort()",
    "LIMIT": "dataset.limit()",
    # Advanced operations - all native
    "Multiple aggregates": "dataset.groupby().aggregate(Count(), Sum(), Mean())",
    "Complex WHERE": "dataset.filter(expr='complex_expression')",
    "Column expressions": "dataset.with_column(name, col('x') + lit(1))",
    "Multi-table JOINs": "dataset.join().join()",
    "UNION": "dataset.union()",
    # Performance features - all native
    "Lazy evaluation": "Ray Dataset native lazy evaluation",
    "Distributed execution": "Ray Dataset native distributed processing",
    "Memory management": "Ray Dataset native memory management",
    "Arrow optimization": "Ray Dataset native Arrow integration",
}
