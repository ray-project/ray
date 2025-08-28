"""
Analyzers for SQL projection and aggregation operations.

This module provides analyzers for SELECT projections and aggregate functions,
converting SQL expressions into Ray Data operations.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple

from sqlglot import exp

from ray.data import Dataset
from ray.data.sql.compiler import ExpressionCompiler
from ray.data.sql.config import SQLConfig
from ray.data.sql.utils import (
    AGGREGATE_MAPPING,
    SUPPORTED_AGGREGATES,
    create_column_mapping,
    get_function_name_from_expression,
    is_aggregate_function,
    normalize_identifier,
    safe_get_column,
    setup_logger,
)


class ProjectionAnalyzer:
    """Analyzes SELECT projection expressions and produces output column names and evaluation functions.

    The ProjectionAnalyzer is responsible for transforming SQL SELECT clause expressions
    into Ray Dataset operations. It handles various types of projections including:
    - Simple column selection (SELECT name, age)
    - Wildcard selection (SELECT *)
    - Table-qualified wildcards (SELECT users.*)
    - Expression evaluation (SELECT age + 5 AS age_plus_five)
    - Literal values (SELECT 'hello', 42)
    - Function calls (SELECT UPPER(name))

    The analyzer produces two outputs:
    1. Column names for the resulting dataset
    2. Evaluation functions that compute each column's values

    Examples:
        .. testcode::

            config = SQLConfig()
            analyzer = ProjectionAnalyzer(config)
            names, funcs = analyzer.analyze_projections(select_exprs, dataset)
    """

    def __init__(self, config: SQLConfig):
        """Initialize the projection analyzer with configuration.

        Args:
            config: SQL configuration controlling expression compilation behavior.
        """
        # Store configuration for expression processing
        self.config = config

        # Expression compiler for converting SQL expressions to Python functions
        self.compiler = ExpressionCompiler(config)

        # Logger for debugging projection analysis
        self._logger = setup_logger("ProjectionAnalyzer")

    def analyze_projections(
        self,
        select_exprs: List[exp.Expression],
        dataset: Dataset,
        table_name: Optional[str] = None,
    ) -> Tuple[List[str], List[Callable]]:
        """Analyze the SELECT clause expressions and return column names and Ray Data expressions.

        This method processes all expressions in the SELECT clause and converts them
        into a format suitable for Ray Dataset operations. It handles the full spectrum
        of SQL SELECT constructs and produces the column metadata and evaluation
        functions needed for the final dataset projection.

        Args:
            select_exprs: List of SQLGlot expressions from the SELECT clause.
            dataset: Source dataset for column information and validation.
            table_name: Optional table name for qualified column resolution.

        Returns:
            Tuple containing:
            - List of output column names for the result dataset
            - List of evaluation functions (callables) for computing each column

        Raises:
            Exception: If any expression cannot be analyzed or compiled.
        """
        # Initialize output collections
        column_names = []  # Names for the output dataset columns
        exprs = []  # Evaluation functions for each column

        # Get source dataset schema information
        cols = list(dataset.columns()) if dataset else []
        column_mapping = create_column_mapping(cols, self.config.case_sensitive)

        # Process each expression in the SELECT clause
        for idx, expr in enumerate(select_exprs):
            try:
                # Analyze this single expression and get its output names/functions
                names, eval_exprs = self._analyze_single_expression(
                    expr, idx, cols, column_mapping, table_name
                )

                # Accumulate results from this expression
                column_names.extend(names)
                exprs.extend(eval_exprs)

            except Exception as e:
                # Log detailed error information for debugging
                self._logger.error(f"Failed to analyze projection {expr}: {e}")
                raise

        return column_names, exprs

    def _analyze_single_expression(
        self,
        expr: exp.Expression,
        index: int,
        cols: List[str],
        column_mapping: Dict[str, str],
        table_name: Optional[str],
    ) -> Tuple[List[str], List[Callable]]:
        """Analyze a single SELECT expression."""
        # Handle SELECT *
        if isinstance(expr, exp.Star):
            return self._handle_star_expression(cols)

        # Handle SELECT table.*
        if isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star):
            return self._handle_table_star_expression(cols, table_name)

        # Handle SELECT ... AS alias
        if isinstance(expr, exp.Alias):
            alias_name = str(expr.alias)
            try:
                compiled = self.compiler.compile(expr.this)
                return [alias_name], [compiled]
            except Exception:
                # fallback to lambda
                func = self.compiler._compile_expression(expr.this)
                return [alias_name], [func]

        # Handle SELECT column
        if isinstance(expr, exp.Column):
            return self._handle_column_expression(expr, column_mapping, cols)

        # Handle SELECT <expression>
        try:
            compiled = self.compiler.compile(expr)
            return [f"col_{index}"], [compiled]
        except Exception:
            func = self.compiler._compile_expression(expr)
            return [f"col_{index}"], [func]

    def _handle_star_expression(
        self, cols: List[str]
    ) -> Tuple[List[str], List[Callable]]:
        """Handle SELECT * expressions."""
        if not cols:
            return [], []

        names = cols[:]
        funcs = [
            lambda row, col=col: safe_get_column(row, col, self.config.case_sensitive)
            for col in cols
        ]
        return names, funcs

    def _handle_table_star_expression(
        self, cols: List[str], table_name: Optional[str]
    ) -> Tuple[List[str], List[Callable]]:
        """Handle SELECT table.* expressions."""
        if not cols:
            raise ValueError(f"Cannot expand {table_name}.* - no columns available")

        names = cols[:]
        funcs = [
            lambda row, col=col: safe_get_column(row, col, self.config.case_sensitive)
            for col in cols
        ]
        return names, funcs

    def _handle_column_expression(
        self, expr: exp.Column, column_mapping: Dict[str, str], cols: List[str]
    ) -> Tuple[List[str], List[Callable]]:
        """Handle SELECT column expressions."""
        col_name = str(expr.name)

        # Handle qualified column names (table.column) by extracting just the column part
        if "." in col_name:
            col_name = col_name.split(".")[-1]

        normalized_name = normalize_identifier(col_name, self.config.case_sensitive)
        actual_column = column_mapping.get(normalized_name)

        if not actual_column:
            raise ValueError(
                f"Column '{col_name}' not found. Available columns: {cols}"
            )

        def func(row, col=actual_column):
            return safe_get_column(row, col, self.config.case_sensitive)

        return [actual_column], [func]


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

    def __init__(self, config: SQLConfig):
        """Initialize AggregateAnalyzer.

        Args:
            config: SQL configuration object containing settings for analysis.
        """
        self.config = config
        self._logger = setup_logger("AggregateAnalyzer")

    def extract_group_by_keys(self, select_ast: exp.Select) -> Optional[List[str]]:
        """Extract GROUP BY column names from the SELECT AST."""
        group_by = select_ast.args.get("group")
        if not group_by:
            return None
        return [str(expr.name) for expr in group_by.expressions]

    def extract_aggregates(
        self, select_ast: exp.Select
    ) -> List[Tuple[str, exp.Expression]]:
        """Extract aggregate expressions and their output names from the SELECT AST."""
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
        """Build aggregate objects and a column rename mapping."""
        import ray.data.aggregate as agg_module

        aggregates = []
        renames = {}
        cols = list(dataset.columns()) if dataset else []
        column_mapping = create_column_mapping(cols, self.config.case_sensitive)

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
        """Build a single aggregate object."""
        func_name = get_function_name_from_expression(agg_expr).lower()
        if func_name not in SUPPORTED_AGGREGATES:
            raise NotImplementedError(
                f"Aggregate function '{func_name.upper()}' not supported"
            )

        target_column = self._extract_target_column(agg_expr, column_mapping)

        if target_column and func_name not in ("count",):
            self._validate_aggregate_column(target_column, func_name)

        agg_class = getattr(agg_module, AGGREGATE_MAPPING[func_name])

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
            func_output = AGGREGATE_MAPPING[func_name].lower()
            column_name = f"{func_output}({target_column})"
        else:
            raise ValueError(f"Invalid aggregate specification: {agg_expr}")

        return aggregate, column_name

    def _is_aggregate(self, expr: exp.Expression) -> bool:
        """Return True if the expression is an aggregate function."""
        return is_aggregate_function(expr)

    def _extract_target_column(
        self, agg_expr: exp.Expression, column_mapping: Dict[str, str]
    ) -> Optional[str]:
        """Extract the target column name for an aggregate function, or None for COUNT(*)."""
        # Handle specific aggregate function classes (Sum, Count, etc.)
        if hasattr(agg_expr, "this") and agg_expr.this:
            if isinstance(agg_expr.this, exp.Column):
                col_name = str(agg_expr.this.name)
                # Handle qualified column names (table.column) by extracting just the column part
                if "." in col_name:
                    col_name = col_name.split(".")[-1]
                normalized = normalize_identifier(col_name, self.config.case_sensitive)
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
                    normalized = normalize_identifier(
                        col_name, self.config.case_sensitive
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
                    normalized = normalize_identifier(
                        col_name, self.config.case_sensitive
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
        """Generate a default output column name for an aggregate expression."""
        func_name = get_function_name_from_expression(agg_expr).upper()
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
        """Validate that the aggregate function can be applied to the column."""
        # This is a simplified validation - in practice, you'd check the actual data
        if func_name not in ("count",):
            self._logger.debug(
                f"Validating column '{column}' for aggregate '{func_name.upper()}'"
            )

    def _get_function_name(self, agg_expr: exp.Expression) -> str:
        """Get the function name from an aggregate expression."""
        return get_function_name_from_expression(agg_expr)
