"""
DataFusion optimizer backend for Ray Data SQL.

Implements OptimizerBackend interface for DataFusion engine.
"""

from typing import Dict, Optional

from ray.data import Dataset
from ray.data.experimental.sql.engines.base import OptimizerBackend, QueryOptimizations

# Import DataFusion optimizer (moved to this directory)
try:
    from ray.data.experimental.sql.engines.datafusion.datafusion_optimizer import (
        DataFusionOptimizer,
    )

    DATAFUSION_AVAILABLE = True
except ImportError:
    DATAFUSION_AVAILABLE = False
    DataFusionOptimizer = None


class DataFusionBackend(OptimizerBackend):
    """
    Apache DataFusion optimizer backend.

    Provides advanced cost-based query optimization with table statistics.
    """

    def __init__(self):
        """Initialize DataFusion backend."""
        self.available = DATAFUSION_AVAILABLE
        if self.available:
            try:
                self.optimizer = DataFusionOptimizer()
                self.available = self.optimizer.is_available()
            except Exception:
                self.available = False
                self.optimizer = None
        else:
            self.optimizer = None

    def is_available(self) -> bool:
        """Check if DataFusion is available.

        Returns:
            True if DataFusion package is installed and functional.
        """
        return self.available

    def get_engine_name(self) -> str:
        """Get engine name.

        Returns:
            "datafusion"
        """
        return "datafusion"

    def optimize_query(
        self, query: str, datasets: Dict[str, Dataset], dialect: str = "duckdb"
    ) -> Optional[QueryOptimizations]:
        """
        Optimize query using DataFusion's cost-based optimizer.

        Automatically translates non-PostgreSQL dialects to PostgreSQL
        before sending to DataFusion (which only supports PostgreSQL syntax).

        Args:
            query: SQL query string in user's chosen dialect.
            datasets: Registered Ray Datasets.
            dialect: User's SQL dialect (translated to PostgreSQL if needed).

        Returns:
            QueryOptimizations with DataFusion's decisions, or None if optimization fails.
        """
        if not self.available or self.optimizer is None:
            return None

        try:
            # Get DataFusion optimizations (with automatic dialect translation)
            df_optimizations = self.optimizer.optimize_query(query, datasets, dialect)

            if df_optimizations is None:
                return None

            # Convert DataFusion-specific optimizations to engine-agnostic format
            return QueryOptimizations(
                join_order=df_optimizations.join_order,
                join_algorithms=df_optimizations.join_algorithms,
                filter_placement=df_optimizations.filter_placement,
                projection_columns=df_optimizations.projection_columns,
                aggregation_strategy=df_optimizations.aggregation_strategy,
                sort_keys=df_optimizations.sort_keys,
                parallelism_hint=df_optimizations.parallelism_hint,
                predicate_pushdown=df_optimizations.predicate_pushdown,
                projection_pushdown=df_optimizations.projection_pushdown,
                engine_name="datafusion",
            )

        except Exception:
            return None

    def supports_dialect(self, dialect: str) -> bool:
        """Check if DataFusion supports the dialect.

        Args:
            dialect: SQL dialect name.

        Returns:
            True only for PostgreSQL-compatible syntax.
        """
        # DataFusion primarily supports PostgreSQL-like SQL
        return dialect.lower() in {"postgres", "postgresql", "duckdb"}
