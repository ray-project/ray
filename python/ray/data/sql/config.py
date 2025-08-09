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
    """Enum for log levels used throughout the SQL engine.

    This enum defines the verbosity levels for logging output:
    - ERROR: Only log errors and critical issues (minimal output)
    - INFO: Log general information plus errors (moderate output)
    - DEBUG: Log detailed debugging information (verbose output)
    """

    ERROR = 0  # Minimal logging - only errors and critical issues
    INFO = 1  # Standard logging - general info, warnings, and errors
    DEBUG = 2  # Verbose logging - detailed execution traces and debug info


@dataclass
class SQLConfig:
    """Configuration for the SQL engine.

    This configuration class controls all aspects of SQL query processing,
    from parsing and optimization to execution and performance tuning.

    Args:
        log_level: Controls verbosity of logging output throughout the engine.
        case_sensitive: Whether SQL identifiers (table/column names) are case sensitive.
        strict_mode: If True, errors are raised for ambiguous or invalid queries.
        max_join_partitions: Controls parallelism for distributed joins (higher = more parallel).
        enable_pushdown_optimization: Enables predicate pushdown to reduce data movement.
        enable_sqlglot_optimizer: Enables SQLGlot's built-in AST optimizer.
        enable_custom_optimizer: Enables custom Ray Data-specific AST optimizations.
        enable_logical_planning: Enables logical plan generation for complex queries.
    """

    # Logging configuration - controls how much output the engine produces
    log_level: LogLevel = LogLevel.ERROR

    # SQL parsing behavior - controls identifier handling
    case_sensitive: bool = False  # False = "Table" and "table" are the same

    # Error handling behavior - controls how strict the engine is
    strict_mode: bool = True  # True = fail fast on ambiguous queries

    # Performance tuning - controls distributed execution
    max_join_partitions: int = 10  # Higher values = more parallelism but more overhead

    # Optimization flags - enable/disable various optimization passes
    enable_pushdown_optimization: bool = True  # Push filters close to data sources
    enable_sqlglot_optimizer: bool = True  # Use SQLGlot's built-in optimizations
    enable_custom_optimizer: bool = True  # Use Ray Data-specific optimizations
    enable_logical_planning: bool = True  # Generate logical execution plans


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

    This dataclass stores join information that maps directly to the Ray Data Join API
    parameters: join_type, on, right_on, left_suffix, right_suffix, num_partitions.

    The Ray Data Join API signature is:
    dataset.join(other, on, join_type, num_partitions, left_suffix, right_suffix)

    This class captures all the information needed to construct that call.

    Args:
        left_table: Name of the left table in the SQL query.
        right_table: Name of the right table in the SQL query.
        left_columns: Join columns in the left table (tuple for API compatibility).
        right_columns: Join columns in the right table (tuple for API compatibility).
        join_type: Type of join - maps to Ray Data join types:
                  - "inner" -> inner join (only matching rows)
                  - "left_outer" -> left join (all left rows)
                  - "right_outer" -> right join (all right rows)
                  - "full_outer" -> full join (all rows from both sides)
        left_dataset: The actual left Ray Dataset object.
        right_dataset: The actual right Ray Dataset object.
        left_suffix: Suffix for left operand columns when names conflict (default: "").
        right_suffix: Suffix for right operand columns when names conflict (default: "_r").
        num_partitions: Number of partitions for join operation (controls parallelism).
    """

    # Table identification - used for SQL parsing and error messages
    left_table: str
    right_table: str

    # Join columns - stored as tuples to match Ray Data API expectations
    left_columns: Tuple[str, ...]  # Columns from left table to join on
    right_columns: Tuple[str, ...]  # Columns from right table to join on

    # Join configuration - maps directly to Ray Data join parameters
    join_type: str  # One of: inner, left_outer, right_outer, full_outer

    # Dataset references - the actual data to be joined
    left_dataset: Optional[Dataset] = None  # Left side of the join
    right_dataset: Optional[Dataset] = None  # Right side of the join

    # Column naming - handles conflicts when both tables have same column names
    left_suffix: str = ""  # Suffix for left columns (usually empty)
    right_suffix: str = "_r"  # Suffix for right columns (default "_r")

    # Performance tuning - controls distributed execution
    num_partitions: int = 10  # Number of partitions for parallel join execution

    # Backward compatibility properties for legacy code that expects single columns
    @property
    def left_column(self) -> str:
        """Get the first left column for backward compatibility.

        Legacy code may expect a single column instead of a tuple.
        This property provides the first column from the left_columns tuple.

        Returns:
            The first left join column, or empty string if no columns.
        """
        return self.left_columns[0] if self.left_columns else ""

    @property
    def right_column(self) -> str:
        """Get the first right column for backward compatibility.

        Legacy code may expect a single column instead of a tuple.
        This property provides the first column from the right_columns tuple.

        Returns:
            The first right join column, or empty string if no columns.
        """
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
    plan, such as table scan, join, or aggregation. Logical plans form a tree
    structure where each node represents an operation and children represent
    inputs to that operation.

    Example:
        A query like "SELECT name FROM users WHERE age > 25" becomes:
        LogicalPlan(SELECT, columns=["name"])
        └── LogicalPlan(SCAN, table="users", filter="age > 25")

    Args:
        operation: Type of operation to perform (from Operation enum).
        **kwargs: Additional operation-specific parameters (e.g., table name, columns).
    """

    class Operation(Enum):
        """Types of logical operations supported in query plans.

        Each operation type corresponds to a different SQL construct:
        - SCAN: Reading data from a table (FROM clause)
        - SELECT: Projecting columns (SELECT clause)
        - SORT: Ordering results (ORDER BY clause)
        - AGGREGATE: Grouping and aggregation (GROUP BY clause)
        - JOIN: Combining tables (JOIN clause)
        - SET: Set operations like UNION (set operations)
        """

        SCAN = "scan"  # Table scan - read data from a registered table
        SELECT = "select"  # Column projection - select specific columns
        SORT = "sort"  # Result ordering - sort by specified columns
        AGGREGATE = "aggregate"  # Grouping/aggregation - GROUP BY with aggregates
        JOIN = "join"  # Table joining - combine multiple tables
        SET = "set"  # Set operations - UNION, INTERSECT, EXCEPT

    def __init__(self, operation: Operation, **kwargs):
        """Initialize a logical plan node.

        Args:
            operation: The type of operation this node performs.
            **kwargs: Operation-specific parameters stored as attributes.
        """
        self.operation = operation  # The operation type for this node
        self.kwargs = kwargs  # Parameters specific to this operation
        self.children: List["LogicalPlan"] = []  # Child operations (inputs)

    def add_child(self, child: "LogicalPlan") -> None:
        """Add a child operation to this plan.

        Child operations represent inputs to this operation. For example,
        a JOIN operation would have two children (left and right inputs).

        Args:
            child: Child logical plan that provides input to this operation.
        """
        self.children.append(child)

    def __repr__(self) -> str:
        """Return a string representation of this logical plan node.

        Returns:
            Human-readable string showing operation type and parameters.
        """
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
