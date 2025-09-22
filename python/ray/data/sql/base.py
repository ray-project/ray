"""
Base classes and interfaces for Ray Data SQL API.

This module provides abstract base classes and common interfaces
to ensure consistency and extensibility across the SQL engine.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ray.data import Dataset
from ray.data.sql.common import ExecutionPlan, QueryContext


class SQLComponent(ABC):
    """Base class for all SQL engine components."""

    @abstractmethod
    def get_component_name(self) -> str:
        """Get the name of this component for debugging."""
        pass


class QueryProcessor(SQLComponent):
    """Abstract base class for query processing components."""

    @abstractmethod
    def process(self, context: QueryContext) -> QueryContext:
        """Process the query context and return updated context."""
        pass

    @abstractmethod
    def validate(self, context: QueryContext) -> None:
        """Validate the context for this processor."""
        pass


class DataHandler(SQLComponent):
    """Abstract base class for data operation handlers."""

    @abstractmethod
    def apply(self, dataset: Dataset, plan: ExecutionPlan) -> Dataset:
        """Apply this handler's operation to the dataset."""
        pass

    @abstractmethod
    def can_handle(self, plan: ExecutionPlan) -> bool:
        """Check if this handler can process the given plan."""
        pass


class QueryOptimizer(SQLComponent):
    """Abstract base class for query optimizers."""

    @abstractmethod
    def optimize(self, plan: ExecutionPlan) -> ExecutionPlan:
        """Optimize the execution plan."""
        pass

    @abstractmethod
    def estimate_cost(self, plan: ExecutionPlan) -> float:
        """Estimate the cost of executing this plan."""
        pass


class CacheManager(SQLComponent):
    """Abstract base class for cache management."""

    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Get an item from the cache."""
        pass

    @abstractmethod
    def put(self, key: str, value: Any) -> None:
        """Put an item in the cache."""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear the cache."""
        pass

    @abstractmethod
    def size(self) -> int:
        """Get current cache size."""
        pass


@dataclass
class ExecutionResult:
    """Result of query execution with metadata."""

    dataset: Dataset
    metrics: Dict[str, Any] = None
    warnings: List[str] = None

    def __post_init__(self):
        if self.metrics is None:
            self.metrics = {}
        if self.warnings is None:
            self.warnings = []


class ExecutionEngine(SQLComponent):
    """Abstract base class for SQL execution engines."""

    @abstractmethod
    def execute_plan(self, plan: ExecutionPlan) -> ExecutionResult:
        """Execute a complete execution plan."""
        pass

    @abstractmethod
    def supports_feature(self, feature: str) -> bool:
        """Check if this engine supports a specific feature."""
        pass

    @abstractmethod
    def get_supported_features(self) -> List[str]:
        """Get list of all supported features."""
        pass
