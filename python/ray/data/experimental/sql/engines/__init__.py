"""
SQL query optimizer engines for Ray Data.

This package provides pluggable optimizer backends that can be swapped
based on configuration. All engines produce optimization hints that are
executed by Ray Data's native operations.

Supported Engines:
- SQLGlot: Multi-dialect SQL parser with rule-based optimization
- DataFusion: Apache Arrow cost-based query optimizer

Design:
- Each engine in its own directory (engines/sqlglot/, engines/datafusion/)
- All engines implement OptimizerBackend protocol
- Core SQL engine is agnostic to optimizer choice
- Easy to add new engines or remove existing ones
"""

from ray.data.experimental.sql.engines.base import OptimizerBackend, QueryOptimizations

__all__ = [
    "OptimizerBackend",
    "QueryOptimizations",
]
