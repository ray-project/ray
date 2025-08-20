"""
Query execution module for Ray Data SQL API.

This module provides the complete query execution infrastructure for converting
SQL operations into Ray Dataset operations. It contains the core components
responsible for executing parsed SQL queries against distributed datasets.

Key Components:
- QueryExecutor: Main orchestrator that coordinates query execution
- Handlers: Specialized processors for different SQL operations (JOIN, WHERE, ORDER BY, etc.)
- Analyzers: Components that analyze SQL constructs and generate execution plans

The execution model follows SQL evaluation order:
1. FROM clause processing (table resolution)
2. JOIN operations (if present)
3. WHERE clause filtering
4. GROUP BY aggregation (if present)
5. HAVING clause filtering (if present)
6. SELECT projection
7. ORDER BY sorting (if present)
8. LIMIT row restriction (if present)

All execution maintains Ray Dataset's lazy evaluation semantics and distributed
processing capabilities, ensuring efficient execution on large-scale data.
"""

from ray.data.sql.execution.analyzers import AggregateAnalyzer, ProjectionAnalyzer
from ray.data.sql.execution.executor import QueryExecutor
from ray.data.sql.execution.handlers import (
    FilterHandler,
    JoinHandler,
    LimitHandler,
    OrderHandler,
)

__all__ = [
    "AggregateAnalyzer",
    "FilterHandler",
    "JoinHandler",
    "LimitHandler",
    "OrderHandler",
    "ProjectionAnalyzer",
    "QueryExecutor",
]
