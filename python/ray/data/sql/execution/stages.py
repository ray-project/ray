"""
SQL execution stages for Ray Data SQL API.

This module provides abstract base classes and concrete implementations for
SQL execution stages following the stage builder pattern. Each stage represents
a specific SQL operation that can be composed into an execution pipeline.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ray.data import Dataset
from ray.data.sql.config import SQLConfig
from ray.data.sql.exceptions import SQLExecutionError
from ray.data.sql.registry.base import TableRegistry
from sqlglot import exp


@dataclass
class SQLContext:
    """Context object passed between SQL execution stages.

    This context maintains state and metadata throughout the execution pipeline,
    allowing stages to communicate and build upon each other's results.
    """

    # Core execution state
    dataset: Optional[Dataset] = None
    ast: Optional[exp.Expression] = None
    config: Optional[SQLConfig] = None
    registry: Optional[TableRegistry] = None

    # Query metadata
    table_name: Optional[str] = None
    column_names: Optional[List[str]] = None

    # Execution tracking
    stage_results: Dict[str, Any] = None
    errors: List[str] = None
    warnings: List[str] = None

    def __post_init__(self):
        """Initialize mutable default values."""
        if self.stage_results is None:
            self.stage_results = {}
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []

    def with_dataset(self, dataset: Dataset) -> "SQLContext":
        """Create a new context with updated dataset."""
        return SQLContext(
            dataset=dataset,
            ast=self.ast,
            config=self.config,
            registry=self.registry,
            table_name=self.table_name,
            column_names=self.column_names,
            stage_results=self.stage_results.copy(),
            errors=self.errors.copy(),
            warnings=self.warnings.copy(),
        )

    def with_table_name(self, table_name: str) -> "SQLContext":
        """Create a new context with updated table name."""
        return SQLContext(
            dataset=self.dataset,
            ast=self.ast,
            config=self.config,
            registry=self.registry,
            table_name=table_name,
            column_names=self.column_names,
            stage_results=self.stage_results.copy(),
            errors=self.errors.copy(),
            warnings=self.warnings.copy(),
        )

    def add_error(self, error: str) -> None:
        """Add an error to the context."""
        self.errors.append(error)

    def add_warning(self, warning: str) -> None:
        """Add a warning to the context."""
        self.warnings.append(warning)

    def has_errors(self) -> bool:
        """Check if context has any errors."""
        return len(self.errors) > 0


class SQLStage(ABC):
    """Abstract base class for SQL execution stages.

    Each stage represents a specific SQL operation (FROM, WHERE, JOIN, etc.)
    that can be composed into an execution pipeline. Stages follow the
    Command pattern and can be validated, executed, and optimized independently.
    """

    def __init__(self, config: SQLConfig):
        """Initialize the stage with configuration.

        Args:
            config: SQL configuration for this stage.
        """
        self.config = config

    @abstractmethod
    def validate(self, context: SQLContext) -> None:
        """Validate stage preconditions.

        Args:
            context: Current execution context.

        Raises:
            SQLExecutionError: If validation fails.
        """
        pass

    @abstractmethod
    def execute(self, context: SQLContext) -> SQLContext:
        """Execute this stage and return updated context.

        Args:
            context: Current execution context.

        Returns:
            Updated context with stage results.

        Raises:
            SQLExecutionError: If execution fails.
        """
        pass

    @abstractmethod
    def get_stage_name(self) -> str:
        """Get the name of this stage for debugging."""
        pass

    # Keep optimization simple - no complex enterprise optimization framework needed


class FromStage(SQLStage):
    """Stage that resolves the FROM clause and loads the base dataset."""

    def get_stage_name(self) -> str:
        return "FROM"

    def validate(self, context: SQLContext) -> None:
        """Validate FROM clause requirements."""
        if not context.ast:
            raise SQLExecutionError("No AST provided for FROM stage")

        if not context.registry:
            raise SQLExecutionError("No table registry provided for FROM stage")

        # Check if FROM clause exists or if default table is available
        from_clause = (
            context.ast.args.get("from")
            if isinstance(context.ast, exp.Select)
            else None
        )
        if not from_clause:
            tables = context.registry.list_tables()
            if len(tables) != 1:
                raise SQLExecutionError(
                    "No FROM clause specified and no single default table available"
                )

    def execute(self, context: SQLContext) -> SQLContext:
        """Execute FROM clause resolution."""
        if not isinstance(context.ast, exp.Select):
            raise SQLExecutionError("FROM stage requires SELECT AST")

        from_clause = context.ast.args.get("from")

        if from_clause:
            # Extract table name from FROM clause
            if hasattr(from_clause, "expressions") and from_clause.expressions:
                table_expr = from_clause.expressions[0]
            elif hasattr(from_clause, "this") and from_clause.this:
                table_expr = from_clause.this
            else:
                raise SQLExecutionError("Invalid FROM clause")

            table_name = str(table_expr.name)
            dataset = context.registry.get(table_name)

            return context.with_dataset(dataset).with_table_name(table_name)
        else:
            # Use default table
            tables = context.registry.list_tables()
            table_name = tables[0]
            dataset = context.registry.get(table_name)

            return context.with_dataset(dataset).with_table_name(table_name)


class WhereStage(SQLStage):
    """Stage that applies WHERE clause filtering."""

    def get_stage_name(self) -> str:
        return "WHERE"

    def validate(self, context: SQLContext) -> None:
        """Validate WHERE clause requirements."""
        if not context.dataset:
            raise SQLExecutionError("No dataset available for WHERE stage")

        if not context.ast:
            raise SQLExecutionError("No AST provided for WHERE stage")

    def execute(self, context: SQLContext) -> SQLContext:
        """Execute WHERE clause filtering."""
        if not isinstance(context.ast, exp.Select):
            return context

        where_clause = context.ast.args.get("where")
        if not where_clause:
            return context  # No WHERE clause, return unchanged

        try:
            from ray.data.sql.compiler.expressions import ExpressionCompiler

            compiler = ExpressionCompiler(self.config)
            filter_func = compiler.compile(where_clause.this)
            filtered_dataset = context.dataset.filter(filter_func)

            return context.with_dataset(filtered_dataset)
        except Exception as e:
            raise SQLExecutionError(f"WHERE clause execution failed: {e}")


class JoinStage(SQLStage):
    """Stage that applies JOIN operations."""

    def get_stage_name(self) -> str:
        return "JOIN"

    def validate(self, context: SQLContext) -> None:
        """Validate JOIN requirements."""
        if not context.dataset:
            raise SQLExecutionError("No dataset available for JOIN stage")

        if not context.registry:
            raise SQLExecutionError("No table registry provided for JOIN stage")

    def execute(self, context: SQLContext) -> SQLContext:
        """Execute JOIN operations."""
        if not isinstance(context.ast, exp.Select):
            return context

        # Apply all JOINs sequentially
        dataset = context.dataset
        for join_node in context.ast.find_all(exp.Join):
            from ray.data.sql.execution.handlers import JoinHandler

            join_handler = JoinHandler(self.config)
            dataset = join_handler.apply_single_join(
                dataset, join_node, context.registry
            )

        return context.with_dataset(dataset)


class ProjectionStage(SQLStage):
    """Stage that applies SELECT clause projection."""

    def get_stage_name(self) -> str:
        return "PROJECTION"

    def validate(self, context: SQLContext) -> None:
        """Validate projection requirements."""
        if not context.dataset:
            raise SQLExecutionError("No dataset available for PROJECTION stage")

    def execute(self, context: SQLContext) -> SQLContext:
        """Execute SELECT projection."""
        if not isinstance(context.ast, exp.Select):
            return context

        select_exprs = context.ast.args.get("expressions", [])

        # Handle SELECT *
        if len(select_exprs) == 1 and isinstance(select_exprs[0], exp.Star):
            return context  # No projection needed

        try:
            from ray.data.sql.execution.analyzers import ProjectionAnalyzer

            analyzer = ProjectionAnalyzer(self.config)
            column_names, funcs = analyzer.analyze_projections(
                select_exprs, context.dataset, context.table_name
            )

            # Apply projection
            if self._is_simple_column_selection(column_names, funcs):
                projected_dataset = context.dataset.select_columns(column_names)
            else:

                def project_row(row):
                    result = {}
                    for name, func in zip(column_names, funcs):
                        try:
                            result[name] = func(row)
                        except Exception:
                            result[name] = None
                    return result

                projected_dataset = context.dataset.map(project_row)

            return context.with_dataset(projected_dataset)
        except Exception as e:
            raise SQLExecutionError(f"Projection execution failed: {e}")

    def _is_simple_column_selection(self, column_names: List[str], funcs: List) -> bool:
        """Check if this is a simple column selection without expressions."""
        for func in funcs:
            if hasattr(func, "__name__") and func.__name__.startswith("get_"):
                continue  # Simple column accessor
            else:
                return False  # Complex expression
        return True


class OrderByStage(SQLStage):
    """Stage that applies ORDER BY sorting."""

    def get_stage_name(self) -> str:
        return "ORDER BY"

    def validate(self, context: SQLContext) -> None:
        """Validate ORDER BY requirements."""
        if not context.dataset:
            raise SQLExecutionError("No dataset available for ORDER BY stage")

    def execute(self, context: SQLContext) -> SQLContext:
        """Execute ORDER BY sorting."""
        if not isinstance(context.ast, exp.Select):
            return context

        order_clause = context.ast.args.get("order")
        if not order_clause:
            return context  # No ORDER BY clause

        try:
            from ray.data.sql.execution.handlers import OrderHandler

            order_handler = OrderHandler(self.config)
            sorted_dataset = order_handler.apply_order_by(context.dataset, context.ast)

            return context.with_dataset(sorted_dataset)
        except Exception as e:
            raise SQLExecutionError(f"ORDER BY execution failed: {e}")


class LimitStage(SQLStage):
    """Stage that applies LIMIT clause."""

    def get_stage_name(self) -> str:
        return "LIMIT"

    def validate(self, context: SQLContext) -> None:
        """Validate LIMIT requirements."""
        if not context.dataset:
            raise SQLExecutionError("No dataset available for LIMIT stage")

    def execute(self, context: SQLContext) -> SQLContext:
        """Execute LIMIT clause."""
        if not isinstance(context.ast, exp.Select):
            return context

        limit_clause = context.ast.args.get("limit")
        if not limit_clause:
            return context  # No LIMIT clause

        try:
            from ray.data.sql.execution.handlers import LimitHandler

            limit_handler = LimitHandler(self.config)
            limited_dataset = limit_handler.apply_limit(context.dataset, context.ast)

            return context.with_dataset(limited_dataset)
        except Exception as e:
            raise SQLExecutionError(f"LIMIT execution failed: {e}")


class SQLPipeline:
    """Pipeline that executes SQL stages in sequence.

    The pipeline orchestrates the execution of multiple SQL stages,
    handling context passing, error collection, and optimization.
    """

    def __init__(self, stages: List[SQLStage]):
        """Initialize pipeline with stages.

        Args:
            stages: List of stages to execute in order.
        """
        self.stages = stages

    def execute(self, context: SQLContext) -> Dataset:
        """Execute all stages in the pipeline.

        Args:
            context: Initial execution context.

        Returns:
            Final dataset result.

        Raises:
            SQLExecutionError: If any stage fails.
        """
        current_context = context

        for stage in self.stages:
            # Validate stage preconditions
            stage.validate(current_context)

            # Execute stage
            current_context = stage.execute(current_context)

            # Check for errors
            if current_context.has_errors():
                errors = "; ".join(current_context.errors)
                raise SQLExecutionError(f"Pipeline execution failed: {errors}")

        if not current_context.dataset:
            raise SQLExecutionError("Pipeline execution produced no dataset")

        return current_context.dataset

    # Keep pipeline simple - focus on execution, not complex optimization


class SQLStageBuilder:
    """Builder for creating SQL execution pipelines.

    The builder provides a fluent interface for constructing SQL execution
    pipelines with proper stage ordering and validation.
    """

    def __init__(self, config: SQLConfig):
        """Initialize builder with configuration.

        Args:
            config: SQL configuration for stages.
        """
        self.config = config
        self.stages: List[SQLStage] = []

    def add_from_stage(self) -> "SQLStageBuilder":
        """Add FROM clause resolution stage."""
        self.stages.append(FromStage(self.config))
        return self

    def add_join_stage(self) -> "SQLStageBuilder":
        """Add JOIN operations stage."""
        self.stages.append(JoinStage(self.config))
        return self

    def add_where_stage(self) -> "SQLStageBuilder":
        """Add WHERE clause filtering stage."""
        self.stages.append(WhereStage(self.config))
        return self

    def add_projection_stage(self) -> "SQLStageBuilder":
        """Add SELECT projection stage."""
        self.stages.append(ProjectionStage(self.config))
        return self

    def add_order_by_stage(self) -> "SQLStageBuilder":
        """Add ORDER BY sorting stage."""
        self.stages.append(OrderByStage(self.config))
        return self

    def add_limit_stage(self) -> "SQLStageBuilder":
        """Add LIMIT clause stage."""
        self.stages.append(LimitStage(self.config))
        return self

    def add_stage(self, stage: SQLStage) -> "SQLStageBuilder":
        """Add a custom stage to the pipeline.

        Args:
            stage: Custom stage to add.

        Returns:
            Builder for chaining.
        """
        self.stages.append(stage)
        return self

    def build(self) -> SQLPipeline:
        """Build the final SQL pipeline.

        Returns:
            Constructed SQL pipeline.
        """
        return SQLPipeline(self.stages)

    def build_select_pipeline(self) -> SQLPipeline:
        """Build a standard SELECT query pipeline.

        Returns:
            Pipeline with standard SELECT stages in correct order.
        """
        return (
            self.add_from_stage()
            .add_join_stage()
            .add_where_stage()
            .add_projection_stage()
            .add_order_by_stage()
            .add_limit_stage()
            .build()
        )


# Convenience function for creating standard pipelines
def create_select_pipeline(config: SQLConfig) -> SQLPipeline:
    """Create a standard SELECT query execution pipeline.

    Args:
        config: SQL configuration.

    Returns:
        Standard SELECT pipeline with proper stage ordering.
    """
    return SQLStageBuilder(config).build_select_pipeline()
