"""
Common data structures and patterns for Ray Data SQL API.

This module provides shared dataclasses and common patterns used throughout
the SQL engine to ensure consistency, maintainability, and extensibility.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from ray.data import Dataset
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class SQLDialect(Enum):
    """SQL dialects supported by Ray Data SQL."""

    DUCKDB = "duckdb"
    POSTGRES = "postgres"
    MYSQL = "mysql"
    SQLITE = "sqlite"
    SPARK = "spark"
    BIGQUERY = "bigquery"


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class ColumnInfo:
    """Information about a table column."""

    name: str
    data_type: str
    nullable: bool = True

    def __post_init__(self):
        if not self.name:
            raise ValueError("Column name cannot be empty")


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class TableInfo:
    """Information about a registered table."""

    name: str
    dataset: Dataset
    columns: List[ColumnInfo] = field(default_factory=list)
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None

    def __post_init__(self):
        if not self.name:
            raise ValueError("Table name cannot be empty")
        if not isinstance(self.dataset, Dataset):
            raise TypeError("dataset must be a Ray Dataset")


@dataclass
class QueryMetrics:
    """Metrics collected during query execution."""

    parse_time_ms: float = 0.0
    compile_time_ms: float = 0.0
    execution_time_ms: float = 0.0
    rows_processed: int = 0
    cache_hits: int = 0
    cache_misses: int = 0

    @property
    def total_time_ms(self) -> float:
        """Total query processing time."""
        return self.parse_time_ms + self.compile_time_ms + self.execution_time_ms


@dataclass
class QueryContext:
    """Context information for query execution."""

    query: str
    dialect: SQLDialect = SQLDialect.DUCKDB
    case_sensitive: bool = False
    metrics: QueryMetrics = field(default_factory=QueryMetrics)
    variables: Dict[str, Any] = field(default_factory=dict)

    def add_variable(self, name: str, value: Any) -> None:
        """Add a variable to the context."""
        self.variables[name] = value

    def get_variable(self, name: str, default: Any = None) -> Any:
        """Get a variable from the context."""
        return self.variables.get(name, default)


@dataclass
class JoinSpec:
    """Specification for a JOIN operation."""

    left_table: str
    right_table: str
    left_columns: List[str]
    right_columns: List[str]
    join_type: str = "inner"
    left_suffix: str = ""
    right_suffix: str = "_r"

    def __post_init__(self):
        if not self.left_columns or not self.right_columns:
            raise ValueError("Join columns cannot be empty")
        if len(self.left_columns) != len(self.right_columns):
            raise ValueError("Left and right join columns must have same length")


@dataclass
class FilterSpec:
    """Specification for a filter operation."""

    expression: str
    compiled_function: Optional[Any] = None

    def __post_init__(self):
        if not self.expression:
            raise ValueError("Filter expression cannot be empty")


@dataclass
class ProjectionSpec:
    """Specification for a projection operation."""

    columns: List[str]
    expressions: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        if not self.columns:
            raise ValueError("Projection must include at least one column")


@dataclass
class AggregateSpec:
    """Specification for an aggregation operation."""

    group_by: List[str] = field(default_factory=list)
    aggregates: Dict[str, str] = field(default_factory=dict)
    having: Optional[str] = None

    def __post_init__(self):
        if not self.aggregates:
            raise ValueError("Aggregate specification must include aggregates")


@dataclass
class OrderSpec:
    """Specification for ordering operations."""

    columns: List[str]
    descending: List[bool] = field(default_factory=list)

    def __post_init__(self):
        if not self.columns:
            raise ValueError("Order specification must include columns")
        # Default to ascending if not specified
        if not self.descending:
            self.descending = [False] * len(self.columns)
        elif len(self.descending) != len(self.columns):
            raise ValueError("Descending flags must match number of columns")


@dataclass
class LimitSpec:
    """Specification for limit operations."""

    count: int
    offset: int = 0

    def __post_init__(self):
        if self.count < 0:
            raise ValueError("Limit count cannot be negative")
        if self.offset < 0:
            raise ValueError("Limit offset cannot be negative")


@dataclass
class ExecutionPlan:
    """Complete execution plan for a SQL query."""

    context: QueryContext
    tables: Dict[str, TableInfo] = field(default_factory=dict)
    joins: List[JoinSpec] = field(default_factory=list)
    filters: List[FilterSpec] = field(default_factory=list)
    projection: Optional[ProjectionSpec] = None
    aggregation: Optional[AggregateSpec] = None
    ordering: Optional[OrderSpec] = None
    limit: Optional[LimitSpec] = None

    def add_table(self, table_info: TableInfo) -> None:
        """Add table information to the plan."""
        self.tables[table_info.name] = table_info

    def add_join(self, join_spec: JoinSpec) -> None:
        """Add a join specification to the plan."""
        self.joins.append(join_spec)

    def add_filter(self, filter_spec: FilterSpec) -> None:
        """Add a filter specification to the plan."""
        self.filters.append(filter_spec)

    def set_projection(self, projection_spec: ProjectionSpec) -> None:
        """Set the projection specification."""
        self.projection = projection_spec

    def set_aggregation(self, aggregate_spec: AggregateSpec) -> None:
        """Set the aggregation specification."""
        self.aggregation = aggregate_spec

    def set_ordering(self, order_spec: OrderSpec) -> None:
        """Set the ordering specification."""
        self.ordering = order_spec

    def set_limit(self, limit_spec: LimitSpec) -> None:
        """Set the limit specification."""
        self.limit = limit_spec

    @property
    def is_aggregate_query(self) -> bool:
        """Check if this is an aggregation query."""
        return self.aggregation is not None

    @property
    def has_joins(self) -> bool:
        """Check if this query has joins."""
        return len(self.joins) > 0

    @property
    def complexity_score(self) -> int:
        """Calculate query complexity score for optimization decisions."""
        score = 0
        score += len(self.tables)
        score += len(self.joins) * 2  # Joins are expensive
        score += len(self.filters)
        score += 2 if self.aggregation else 0  # Aggregation is expensive
        score += 1 if self.ordering else 0
        return score


# Common validation functions
def validate_table_name(name: str) -> bool:
    """Validate if a table name is acceptable."""
    if not name or not isinstance(name, str):
        return False
    if len(name) > 64:
        return False
    return name.replace("_", "").replace("-", "").isalnum()


def validate_column_name(name: str) -> bool:
    """Validate if a column name is acceptable."""
    if not name or not isinstance(name, str):
        return False
    if len(name) > 64:
        return False
    return name.replace("_", "").isalnum()


# Common SQL reserved words
SQL_RESERVED_WORDS: Set[str] = {
    "select",
    "from",
    "where",
    "join",
    "inner",
    "left",
    "right",
    "full",
    "on",
    "and",
    "or",
    "not",
    "null",
    "true",
    "false",
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "group",
    "by",
    "order",
    "having",
    "limit",
    "offset",
    "distinct",
    "union",
    "case",
    "when",
    "then",
    "else",
    "end",
    "as",
    "in",
    "between",
    "like",
    "is",
    "exists",
    "cast",
    "insert",
    "update",
    "delete",
    "create",
    "drop",
    "alter",
    "table",
    "database",
}


def is_reserved_word(word: str) -> bool:
    """Check if a word is a SQL reserved word."""
    return word.lower() in SQL_RESERVED_WORDS
