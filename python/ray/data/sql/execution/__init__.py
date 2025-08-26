"""
Query execution module for Ray Data SQL API.

This module provides the complete query execution infrastructure for converting
SQL operations into Ray Dataset operations. It contains the core components
responsible for executing parsed SQL queries against distributed datasets.

Key Components:
- SQLExecutionEngine: Main execution engine that handles all SQL operations
- JoinHandler: Handles JOIN operations (INNER, LEFT, RIGHT, FULL)
- AggregateAnalyzer: Analyzes and processes GROUP BY and aggregate functions
- ExpressionCompiler: Compiles SQL expressions to Python functions

The execution model follows SQL evaluation order:
1. FROM clause processing (table resolution)
2. JOIN operations (if present)
3. WHERE clause filtering
4. GROUP BY aggregation (if present)
5. SELECT projection
6. ORDER BY sorting (if present)
7. LIMIT row restriction (if present)

All execution maintains Ray Dataset's lazy evaluation semantics and distributed
processing capabilities, ensuring efficient execution on large-scale data.
"""

from ray.data.sql.execution.engine import SQLExecutionEngine

__all__ = [
    "SQLExecutionEngine",
]
