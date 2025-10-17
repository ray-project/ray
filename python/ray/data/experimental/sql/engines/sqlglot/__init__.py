"""
SQLGlot optimizer engine for Ray Data SQL.

This engine uses SQLGlot for multi-dialect SQL parsing and optimization.

Features:
- Supports 20+ SQL dialects (MySQL, PostgreSQL, Spark SQL, BigQuery, etc.)
- Rule-based query optimization
- AST-level transformations
- Fast pure-Python implementation
"""

from ray.data.experimental.sql.engines.sqlglot.sqlglot_backend import SQLGlotBackend

__all__ = ["SQLGlotBackend"]
