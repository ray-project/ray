"""
SQLGlot optimizer backend for Ray Data SQL.

Implements OptimizerBackend interface for SQLGlot engine.
"""

from typing import Dict, Optional

from ray.data import Dataset
from ray.data.experimental.sql.engines.base import OptimizerBackend, QueryOptimizations


class SQLGlotBackend(OptimizerBackend):
    """
    SQLGlot optimizer backend.

    Provides multi-dialect SQL parsing and rule-based optimization.
    """

    def __init__(self):
        """Initialize SQLGlot backend."""
        self.available = True  # SQLGlot is always available (required dependency)

    def is_available(self) -> bool:
        """SQLGlot is always available.

        Returns:
            Always True (SQLGlot is required dependency).
        """
        return True

    def get_engine_name(self) -> str:
        """Get engine name.

        Returns:
            "sqlglot"
        """
        return "sqlglot"

    def optimize_query(
        self, query: str, datasets: Dict[str, Dataset], dialect: str = "duckdb"
    ) -> Optional[QueryOptimizations]:
        """
        Optimize query using SQLGlot's rule-based optimizer.

        SQLGlot provides basic optimization hints. Full optimization
        is handled by the standard QueryExecutor with its handlers.

        Args:
            query: SQL query string in the specified dialect.
            datasets: Registered datasets (not used by SQLGlot).
            dialect: SQL dialect (SQLGlot handles this natively).

        Returns:
            Basic QueryOptimizations (SQLGlot optimization happens in parser).
        """
        # SQLGlot optimization is integrated into the parser
        # SQLGlot natively supports the dialect, no translation needed
        # Return basic hints - actual optimization happens during execution
        return QueryOptimizations(
            predicate_pushdown=True,
            projection_pushdown=True,
            engine_name="sqlglot",
        )

    def supports_dialect(self, dialect: str) -> bool:
        """Check if SQLGlot supports the dialect.

        Args:
            dialect: SQL dialect name.

        Returns:
            True for most common dialects (SQLGlot supports 20+).
        """
        supported_dialects = {
            "duckdb",
            "postgres",
            "postgresql",
            "mysql",
            "sqlite",
            "spark",
            "bigquery",
            "snowflake",
            "redshift",
            "oracle",
            "mssql",
            "tsql",
            "hive",
            "presto",
            "trino",
            "clickhouse",
            "databricks",
        }
        return dialect.lower() in supported_dialects
