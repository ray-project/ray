"""
Enhanced abstractions and data classes for Ray Data SQL API.

This module provides comprehensive abstractions following Ray Data patterns,
including data classes for SQL operations, abstract base classes for handlers,
and structured query plan representations.
"""

import abc
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Protocol, Union

from sqlglot import exp

from ray.data import Dataset
from ray.data.sql.config import SQLConfig


# Core SQL Operation Data Classes


class SQLOperationType(Enum):
    """Types of SQL operations supported by the engine."""

    SCAN = "scan"
    SELECT = "select"
    PROJECT = "project"
    FILTER = "filter"
    JOIN = "join"
    AGGREGATE = "aggregate"
    ORDER = "order"
    LIMIT = "limit"
    UNION = "union"


@dataclass
class SQLExpression:
    """Represents a compiled SQL expression with metadata."""

    expression: Union[str, Callable]
    return_type: type
    dependencies: List[str] = field(default_factory=list)
    is_aggregate: bool = False
    alias: Optional[str] = None

    def __post_init__(self):
        """Validate expression after initialization."""
        # The validation `if self.is_aggregate and not self.dependencies` is removed
        # as it incorrectly flags valid aggregates like COUNT(*).
        pass


@dataclass
class ColumnReference:
    """Represents a reference to a table column."""

    name: str
    table: Optional[str] = None
    alias: Optional[str] = None
    data_type: Optional[type] = None

    @property
    def qualified_name(self) -> str:
        """Get the fully qualified column name."""
        return f"{self.table}.{self.name}" if self.table else self.name

    @property
    def output_name(self) -> str:
        """Get the output name (alias if present, otherwise name)."""
        return self.alias or self.name


@dataclass
class SelectOperation:
    """Data class representing a SELECT operation."""

    expressions: List[SQLExpression]
    distinct: bool = False
    star_select: bool = False
    literal_only: bool = False

    def __post_init__(self):
        """Validate select operation after initialization."""
        if not self.expressions and not self.star_select:
            raise ValueError(
                "Select operation must have expressions or be a star select"
            )

    @property
    def output_columns(self) -> List[str]:
        """Get list of output column names."""
        return [expr.alias or "value" for expr in self.expressions]

    @property
    def has_aggregates(self) -> bool:
        """Check if any expressions are aggregates."""
        return any(expr.is_aggregate for expr in self.expressions)


@dataclass
class FilterOperation:
    """Data class representing a WHERE filter operation."""

    condition: SQLExpression
    table_references: List[str] = field(default_factory=list)

    def can_pushdown(self, table_name: str) -> bool:
        """Check if this filter can be pushed down to a specific table."""
        return len(self.table_references) == 1 and table_name in self.table_references


@dataclass
class JoinOperation:
    """Enhanced data class for JOIN operations following Ray Data patterns."""

    join_type: str
    left_table: str
    right_table: str
    left_on: List[str]
    right_on: List[str]
    left_suffix: str = ""
    right_suffix: str = "_r"
    condition: Optional[SQLExpression] = None

    def __post_init__(self):
        """Validate join operation."""
        if len(self.left_on) != len(self.right_on):
            raise ValueError("Left and right join keys must have same length")

        valid_types = ["inner", "left_outer", "right_outer", "full_outer"]
        if self.join_type not in valid_types:
            raise ValueError(
                f"Invalid join type: {self.join_type}. Must be one of {valid_types}"
            )

    @property
    def is_equi_join(self) -> bool:
        """Check if this is an equi-join (only equality conditions)."""
        return self.condition is None or not self.condition.dependencies

    def to_ray_join_params(self) -> Dict[str, Any]:
        """Convert to Ray Dataset join parameters."""
        # Convert to tuples for Ray Dataset API compatibility
        left_on = tuple(self.left_on) if len(self.left_on) > 1 else self.left_on[0]
        right_on = tuple(self.right_on) if len(self.right_on) > 1 else self.right_on[0]

        return {
            "on": left_on,
            "right_on": right_on,
            "how": self.join_type,
            "left_suffix": self.left_suffix,
            "right_suffix": self.right_suffix,
        }


@dataclass
class AggregateOperation:
    """Data class representing aggregation with GROUP BY."""

    group_by: List[ColumnReference]
    aggregates: List[SQLExpression]
    having: Optional[FilterOperation] = None

    def __post_init__(self):
        """Validate aggregate operation."""
        if not self.aggregates:
            raise ValueError(
                "Aggregate operation must have at least one aggregate function"
            )

        for agg in self.aggregates:
            if not agg.is_aggregate:
                raise ValueError(f"Expression {agg.alias} is not an aggregate function")

    @property
    def group_keys(self) -> List[str]:
        """Get list of grouping key names."""
        return [col.output_name for col in self.group_by]


@dataclass
class OrderOperation:
    """Data class representing ORDER BY operation."""

    @dataclass
    class SortKey:
        """Individual sort key specification."""

        column: ColumnReference
        ascending: bool = True
        nulls_first: bool = False

    keys: List[SortKey] = field(default_factory=list)

    def __post_init__(self):
        """Validate order operation."""
        if not self.keys:
            raise ValueError("Order operation must have at least one sort key")

    def to_ray_sort_params(self) -> Union[str, List[str]]:
        """Convert to Ray Dataset sort parameters."""
        if len(self.keys) == 1:
            key = self.keys[0]
            return (
                key.column.output_name
                if key.ascending
                else f"-{key.column.output_name}"
            )

        result = []
        for key in self.keys:
            col_name = key.column.output_name
            result.append(col_name if key.ascending else f"-{col_name}")
        return result


@dataclass
class LimitOperation:
    """Data class representing LIMIT operation."""

    count: int
    offset: int = 0

    def __post_init__(self):
        """Validate limit operation."""
        if self.count < 0:
            raise ValueError("Limit count cannot be negative")
        if self.offset < 0:
            raise ValueError("Limit offset cannot be negative")


# Query Plan Abstractions


@dataclass
class QueryPlan:
    """Represents a complete SQL query execution plan following Ray Data patterns."""

    operation_type: SQLOperationType
    source_tables: List[str] = field(default_factory=list)
    operations: List[Any] = field(default_factory=list)  # SQLOperation instances
    estimated_cost: float = 0.0
    parallelism_hint: Optional[int] = None

    def add_operation(self, operation: Any) -> None:
        """Add an operation to the plan."""
        self.operations.append(operation)

    def get_operations_by_type(self, op_type: type) -> List[Any]:
        """Get all operations of a specific type."""
        return [op for op in self.operations if isinstance(op, op_type)]

    @property
    def has_joins(self) -> bool:
        """Check if plan contains join operations."""
        return any(isinstance(op, JoinOperation) for op in self.operations)

    @property
    def has_aggregates(self) -> bool:
        """Check if plan contains aggregate operations."""
        return any(isinstance(op, AggregateOperation) for op in self.operations)


# ExecutionResult has been consolidated into QueryResult in config.py for better API clarity


# Abstract Base Classes for Handlers


class SQLOperationHandler(abc.ABC):
    """Abstract base class for SQL operation handlers."""

    def __init__(self, config: SQLConfig):
        self.config = config

    @abc.abstractmethod
    def can_handle(self, operation: Any) -> bool:
        """Check if this handler can process the given operation."""
        pass

    @abc.abstractmethod
    def apply(self, dataset: Dataset, operation: Any) -> Dataset:
        """Apply the operation to the dataset."""
        pass

    @abc.abstractmethod
    def estimate_cost(self, operation: Any, input_size: int) -> float:
        """Estimate the computational cost of the operation."""
        pass


class SQLAnalyzer(abc.ABC):
    """Abstract base class for SQL AST analyzers."""

    def __init__(self, config: SQLConfig):
        self.config = config

    @abc.abstractmethod
    def analyze(self, ast: exp.Expression) -> Any:
        """Analyze the AST and extract relevant information."""
        pass

    @abc.abstractmethod
    def validate(self, ast: exp.Expression) -> List[str]:
        """Validate the AST and return any error messages."""
        pass


# Protocol Definitions


class SQLConfigurable(Protocol):
    """Protocol for components that can be configured."""

    def configure(self, config: SQLConfig) -> None:
        """Configure the component with SQL config."""
        ...


class Optimizable(Protocol):
    """Protocol for components that support optimization."""

    def optimize(self, query_plan: QueryPlan) -> QueryPlan:
        """Optimize the query plan."""
        ...


class Cacheable(Protocol):
    """Protocol for components that support caching."""

    def get_cache_key(self) -> str:
        """Get a unique cache key for this component."""
        ...

    def is_cacheable(self) -> bool:
        """Check if this component can be cached."""
        ...


# Factory Classes


class SQLOperationFactory:
    """Factory for creating SQL operation instances."""

    @staticmethod
    def create_select(expressions: List[SQLExpression], **kwargs) -> SelectOperation:
        """Create a SELECT operation."""
        return SelectOperation(expressions=expressions, **kwargs)

    @staticmethod
    def create_filter(condition: SQLExpression, **kwargs) -> FilterOperation:
        """Create a FILTER operation."""
        return FilterOperation(condition=condition, **kwargs)

    @staticmethod
    def create_join(
        join_type: str,
        left_table: str,
        right_table: str,
        left_on: List[str],
        right_on: List[str],
        **kwargs,
    ) -> JoinOperation:
        """Create a JOIN operation."""
        return JoinOperation(
            join_type=join_type,
            left_table=left_table,
            right_table=right_table,
            left_on=left_on,
            right_on=right_on,
            **kwargs,
        )

    @staticmethod
    def create_aggregate(
        group_by: List[ColumnReference], aggregates: List[SQLExpression], **kwargs
    ) -> AggregateOperation:
        """Create an AGGREGATE operation."""
        return AggregateOperation(group_by=group_by, aggregates=aggregates, **kwargs)

    @staticmethod
    def create_order(keys: List[OrderOperation.SortKey], **kwargs) -> OrderOperation:
        """Create an ORDER operation."""
        return OrderOperation(keys=keys, **kwargs)

    @staticmethod
    def create_limit(count: int, **kwargs) -> LimitOperation:
        """Create a LIMIT operation."""
        return LimitOperation(count=count, **kwargs)


# Enhanced Error Classes


class SQLOperationError(Exception):
    """Base exception for SQL operation errors."""

    def __init__(
        self,
        message: str,
        operation: Optional[Any] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.operation = operation
        self.context = context or {}


class SQLValidationError(SQLOperationError):
    """Exception raised for SQL validation errors."""

    pass


class SQLExecutionError(SQLOperationError):
    """Exception raised for SQL execution errors."""

    pass


class SQLOptimizationError(SQLOperationError):
    """Exception raised for SQL optimization errors."""

    pass


# Metrics and Monitoring


@dataclass
class OperationMetrics:
    """Metrics for individual SQL operations."""

    operation_type: str
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    rows_input: int = 0
    rows_output: int = 0
    memory_used: float = 0.0

    @property
    def duration(self) -> float:
        """Get operation duration in seconds."""
        if self.end_time is None:
            return time.time() - self.start_time
        return self.end_time - self.start_time

    def finish(self, rows_output: int) -> None:
        """Mark operation as finished."""
        self.end_time = time.time()
        self.rows_output = rows_output


# QueryMetrics has been consolidated into QueryResult in config.py for better API clarity
