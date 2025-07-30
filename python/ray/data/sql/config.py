"""
Configuration classes and data structures for Ray Data SQL API.

This module contains all configuration classes, enums, and data structures
used throughout the SQL engine. It follows Ray Data patterns for configuration
management and lazy evaluation.
"""

import logging
import time
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
    """Result of a SQL query execution.

    Args:
        dataset: The resulting Ray Dataset.
        execution_time: Total execution time in seconds.
        row_count: Number of rows in the result.
        parse_time: Time spent parsing the query.
        optimize_time: Time spent optimizing the query.
        plan_time: Time spent planning the query.
        execute_time: Time spent executing the query.
    """

    dataset: Dataset
    execution_time: float
    row_count: int
    parse_time: float = 0.0
    optimize_time: float = 0.0
    plan_time: float = 0.0
    execute_time: float = 0.0


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
        self.columns[name] = ColumnInfo(name=name, type=type, nullable=nullable, table=self.name)

    def get_column(self, name: str) -> Optional[ColumnInfo]:
        """Get column information by name.

        Args:
            name: Column name to look up.

        Returns:
            ColumnInfo object if found, None otherwise.
        """
        return self.columns.get(name)


@dataclass
class ExecutionStats:
    """Statistics about query execution.

    Args:
        total_time: Total execution time in seconds.
        parse_time: Time spent parsing the query.
        sqlglot_optimize_time: Time spent in SQLGlot optimization.
        custom_optimize_time: Time spent in custom optimization.
        plan_time: Time spent in logical planning.
        execute_time: Time spent executing the query.
        row_count: Number of rows returned.
    """

    total_time: float = 0.0
    parse_time: float = 0.0
    sqlglot_optimize_time: float = 0.0
    custom_optimize_time: float = 0.0
    plan_time: float = 0.0
    execute_time: float = 0.0
    row_count: int = 0

    def log_stats(self, logger: logging.Logger) -> None:
        """Log execution statistics.

        Args:
            logger: Logger instance to use for output.
        """
        logger.info(f"Query executed successfully in {self.total_time:.3f}s")
        logger.info(f"  - Parse: {self.parse_time:.3f}s")
        if self.sqlglot_optimize_time > 0:
            logger.info(f"  - SQLGlot Optimize: {self.sqlglot_optimize_time:.3f}s")
        if self.custom_optimize_time > 0:
            logger.info(f"  - Custom Optimize: {self.custom_optimize_time:.3f}s")
        if self.plan_time > 0:
            logger.info(f"  - Plan: {self.plan_time:.3f}s")
        logger.info(f"  - Execute: {self.execute_time:.3f}s")
        logger.info(f"  - Rows returned: {self.row_count}")


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
        self.children: List['LogicalPlan'] = []

    def add_child(self, child: 'LogicalPlan') -> None:
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
    
    def __init__(self, message: str, line_no: Optional[int] = None, col_no: Optional[int] = None):
        super().__init__(message)
        self.line_no = line_no
        self.col_no = col_no 