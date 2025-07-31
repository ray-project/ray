"""
Configuration classes and data structures for Ray Data SQL API.

This module contains all configuration classes, enums, and data structures
used throughout the SQL engine. It follows Ray Data patterns for configuration
management and lazy evaluation.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from ray.data import Dataset


class LogLevel(Enum):
    """Enum for log levels used throughout the SQL engine."""

    ERROR = 0
    INFO = 1
    DEBUG = 2


@dataclass
class SQLConfig:
    """Configuration for the SQL engine.

    Args:
        log_level: Controls verbosity of logging.
        case_sensitive: Whether SQL identifiers are case sensitive.
        strict_mode: If True, errors are raised for ambiguous or invalid queries.
        max_join_partitions: Controls parallelism for distributed joins.
        enable_pushdown_optimization: Enables predicate pushdown.
        enable_sqlglot_optimizer: Enables SQLGlot's AST optimizer.
        enable_custom_optimizer: Enables custom AST optimizations.
        enable_logical_planning: Enables logical plan generation.
    """

    log_level: LogLevel = LogLevel.ERROR
    case_sensitive: bool = False
    strict_mode: bool = True
    max_join_partitions: int = 10
    enable_pushdown_optimization: bool = True
    enable_sqlglot_optimizer: bool = True
    enable_custom_optimizer: bool = True
    enable_logical_planning: bool = True


@dataclass
class QueryResult:
    """Unified result of a SQL query execution with comprehensive metrics.

    This class consolidates QueryResult, ExecutionResult, ExecutionStats, and QueryMetrics
    into a single, cohesive interface for query results and performance metrics.

    Args:
        dataset: The resulting Ray Dataset.
        execution_time: Total execution time in seconds.
        row_count: Number of rows in the result.
        parse_time: Time spent parsing the query.
        optimize_time: Time spent optimizing the query.
        plan_time: Time spent planning the query.
        execute_time: Time spent executing the query.
        query_text: Original SQL query text.
        operations_applied: List of operations applied during execution.
        warnings: List of warnings generated during execution.
        metadata: Additional metadata about the execution.
    """

    dataset: Dataset
    execution_time: float
    row_count: int
    parse_time: float = 0.0
    optimize_time: float = 0.0
    plan_time: float = 0.0
    execute_time: float = 0.0
    query_text: str = ""
    operations_applied: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def add_warning(self, message: str) -> None:
        """Add a warning to the result."""
        self.warnings.append(message)

    def add_metadata(self, key: str, value: Any) -> None:
        """Add metadata to the result."""
        self.metadata[key] = value

    def log_stats(self, logger) -> None:
        """Log execution statistics."""
        logger.info(f"Query executed successfully in {self.execution_time:.3f}s")
        logger.info(f"  - Parse: {self.parse_time:.3f}s")
        if self.optimize_time > 0:
            logger.info(f"  - Optimize: {self.optimize_time:.3f}s")
        if self.plan_time > 0:
            logger.info(f"  - Plan: {self.plan_time:.3f}s")
        logger.info(f"  - Execute: {self.execute_time:.3f}s")
        logger.info(f"  - Rows returned: {self.row_count}")
        if self.warnings:
            logger.warning(f"  - Warnings: {len(self.warnings)}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary for serialization."""
        return {
            "query_text": self.query_text,
            "execution_time": self.execution_time,
            "parse_time": self.parse_time,
            "optimize_time": self.optimize_time,
            "plan_time": self.plan_time,
            "execute_time": self.execute_time,
            "row_count": self.row_count,
            "operations_applied": self.operations_applied,
            "warnings": self.warnings,
            "metadata": self.metadata,
        }


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

    # Backward compatibility properties
    @property
    def left_column(self) -> str:
        """Get the first left column for backward compatibility."""
        return self.left_columns[0] if self.left_columns else ""

    @property
    def right_column(self) -> str:
        """Get the first right column for backward compatibility."""
        return self.right_columns[0] if self.right_columns else ""


@dataclass
class ColumnInfo:
    """Information about a database column.

    Args:
        name: Column name.
        type: SQL data type.
        nullable: Whether the column can contain NULL values.
        table: Name of the table containing this column.
    """

    name: str
    type: str
    nullable: bool = True
    table: Optional[str] = None


@dataclass
class TableSchema:
    """Schema information for a table.

    Args:
        name: Table name.
        columns: Dictionary mapping column names to ColumnInfo objects.
    """

    name: str
    columns: Dict[str, ColumnInfo] = field(default_factory=dict)

    def add_column(self, name: str, type: str, nullable: bool = True) -> None:
        """Add a column to the schema.

        Args:
            name: Column name.
            type: SQL data type.
            nullable: Whether the column can contain NULL values.
        """
        self.columns[name] = ColumnInfo(
            name=name, type=type, nullable=nullable, table=self.name
        )

    def get_column(self, name: str) -> Optional[ColumnInfo]:
        """Get column information by name.

        Args:
            name: Column name to look up.

        Returns:
            ColumnInfo object if found, None otherwise.
        """
        return self.columns.get(name)


# ExecutionStats has been consolidated into QueryResult for better API clarity


class LogicalPlan:
    """Represents a logical plan for SQL execution.

    The LogicalPlan class represents a single operation in a query execution
    plan, such as table scan, join, or aggregation.

    Args:
        operation: Type of operation to perform.
        **kwargs: Additional operation-specific parameters.
    """

    class Operation(Enum):
        """Types of logical operations."""

        SCAN = "scan"
        SELECT = "select"
        SORT = "sort"
        AGGREGATE = "aggregate"
        JOIN = "join"
        SET = "set"

    def __init__(self, operation: Operation, **kwargs):
        self.operation = operation
        self.kwargs = kwargs
        self.children: List["LogicalPlan"] = []

    def add_child(self, child: "LogicalPlan") -> None:
        """Add a child operation to this plan.

        Args:
            child: Child logical plan.
        """
        self.children.append(child)

    def __repr__(self) -> str:
        return f"LogicalPlan({self.operation.value}, {self.kwargs})"


# Enhanced error handling for SQLGlot
class SQLGlotError(Exception):
    """Enhanced SQLGlot error with line/column information."""

    def __init__(
        self, message: str, line_no: Optional[int] = None, col_no: Optional[int] = None
    ):
        super().__init__(message)
        self.line_no = line_no
        self.col_no = col_no
