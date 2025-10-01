"""
Base interfaces for SQL query optimizer engines.

Defines the protocol that all optimizer engines must implement,
ensuring the core SQL engine can work with any backend.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from ray.data import Dataset


@dataclass
class QueryOptimizations:
    """
    Engine-agnostic optimization decisions.

    This dataclass represents optimization hints from any engine (SQLGlot,
    DataFusion, Calcite, etc.) in a standardized format that Ray Data can execute.

    All engines produce these same optimization hints, allowing the core SQL
    engine to be completely agnostic to which optimizer was used.
    """

    # Join optimization
    join_order: List[Tuple[str, str]] = None  # Optimized join sequence
    join_algorithms: Dict[str, str] = None  # Join type for each join

    # Filter optimization
    filter_placement: List[Dict[str, Any]] = None  # Where to apply filters
    predicate_pushdown: bool = True  # Whether filters were pushed down

    # Projection optimization
    projection_columns: List[str] = None  # Columns to select early
    projection_pushdown: bool = True  # Whether projections were pushed down

    # Aggregation optimization
    aggregation_strategy: Optional[str] = None  # Hash vs sort aggregation

    # Other hints
    sort_keys: Optional[List[str]] = None  # Sort operations
    parallelism_hint: Optional[int] = None  # Suggested parallelism
    engine_name: str = "unknown"  # Which engine produced these hints

    def __post_init__(self):
        """Initialize empty lists/dicts if None."""
        if self.join_order is None:
            self.join_order = []
        if self.join_algorithms is None:
            self.join_algorithms = {}
        if self.filter_placement is None:
            self.filter_placement = []
        if self.projection_columns is None:
            self.projection_columns = []


class OptimizerBackend(ABC):
    """
    Abstract base class for SQL query optimizer engines.

    All optimizer backends (SQLGlot, DataFusion, Calcite, etc.) must implement
    this interface to be compatible with the Ray Data SQL engine.

    This allows easy swapping of optimizer backends without changing the
    core SQL execution engine.
    """

    @abstractmethod
    def is_available(self) -> bool:
        """Check if this optimizer backend is available.

        Returns:
            True if the backend can be used (dependencies installed, etc.).
        """
        pass

    @abstractmethod
    def get_engine_name(self) -> str:
        """Get the name of this optimizer engine.

        Returns:
            Engine name (e.g., "sqlglot", "datafusion", "calcite").
        """
        pass

    @abstractmethod
    def optimize_query(
        self, query: str, datasets: Dict[str, Dataset], dialect: str = "duckdb"
    ) -> Optional[QueryOptimizations]:
        """
        Optimize SQL query and return standardized optimization hints.

        Args:
            query: SQL query string in the specified dialect.
            datasets: Registered Ray Datasets for the query.
            dialect: SQL dialect of the query (e.g., "mysql", "postgres", "spark").

        Returns:
            QueryOptimizations with engine-agnostic hints, or None if optimization fails.
        """
        pass

    @abstractmethod
    def supports_dialect(self, dialect: str) -> bool:
        """Check if this engine supports the given SQL dialect.

        Args:
            dialect: SQL dialect name (e.g., "postgres", "mysql").

        Returns:
            True if the dialect is supported.
        """
        pass
