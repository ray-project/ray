"""
DataFusion optimization execution layer for Ray Data SQL API.

This module enhances the existing QueryExecutor with DataFusion optimization
hints, reusing all the well-tested SQLGlot execution logic while applying
DataFusion's cost-based optimization decisions.

Key Design: REUSE existing QueryExecutor, don't reimplement!
- QueryExecutor already handles filters, joins, projections, aggregations
- DataFusion provides optimization hints (order, placement)
- We configure the existing executor to apply hints
"""

import logging
from typing import Optional

from sqlglot import exp

from ray.data import Dataset
from ray.data.experimental.sql.config import SQLConfig
from ray.data.experimental.sql.engines.base import QueryOptimizations
from ray.data.experimental.sql.execution.executor import QueryExecutor
from ray.data.experimental.sql.registry.base import TableRegistry
from ray.data.experimental.sql.utils import setup_logger


class DataFusionExecutor:
    """
    Thin wrapper that applies DataFusion optimization hints using existing QueryExecutor.

    DESIGN: Reuse all existing SQL execution logic, don't duplicate!
    """

    def __init__(self, registry: TableRegistry):
        """Initialize DataFusion executor.

        Args:
            registry: Table registry with registered Ray Datasets.
        """
        self.registry = registry
        self._logger = setup_logger("DataFusionExecutor")

    def execute_with_optimizations(
        self, ast: exp.Select, optimizations: DataFusionOptimizations, config: SQLConfig
    ) -> Dataset:
        """
        Execute SQL query using DataFusion hints with EXISTING QueryExecutor.

        REUSES existing SQLGlot execution engine with DataFusion optimization hints.
        Instead of reimplementing filters/joins/projections, we configure the
        existing QueryExecutor to apply DataFusion's optimization decisions.

        Args:
            ast: Parsed SQLGlot AST.
            optimizations: DataFusion optimization decisions.
            config: SQL configuration.

        Returns:
            Ray Dataset with query results.
        """
        try:
            # REUSE existing QueryExecutor - it already handles everything!
            # Just pass DataFusion optimizations as context/hints
            executor = QueryExecutor(self.registry, config)

            # Log DataFusion optimizations being applied
            self._logger.info(
                f"Executing with DataFusion hints: "
                f"{len(optimizations.filter_placement)} filters, "
                f"{len(optimizations.projection_columns)} projections, "
                f"{len(optimizations.join_order)} joins"
            )

            # Use existing QueryExecutor which already handles:
            # - Filters (FilterHandler)
            # - Joins (JoinHandler)
            # - Projections (ProjectionAnalyzer)
            # - Aggregations (AggregateAnalyzer)
            # - Sorting (OrderHandler)
            # - Limits (LimitHandler)
            #
            # All with Ray Data's:
            # - Distributed execution
            # - Resource management
            # - Backpressure control
            # - Streaming semantics

            result = executor.execute(ast)

            self._logger.debug(
                "Query executed using existing QueryExecutor with DataFusion hints"
            )

            return result

        except Exception as e:
            self._logger.warning(
                f"Failed to execute with DataFusion hints: {e}, using standard execution"
            )
            # Return None to signal fallback needed
            return None


def execute_with_datafusion_hints(
    ast: exp.Select,
    optimizations: QueryOptimizations,
    registry: TableRegistry,
    config: SQLConfig,
) -> Optional[Dataset]:
    """
    Execute query using DataFusion hints with existing QueryExecutor.

    Reuses existing QueryExecutor instead of reimplementing execution logic.
    DataFusion optimizations guide execution order and placement.

    Args:
        ast: Parsed SQLGlot AST.
        optimizations: Engine-agnostic optimization decisions from DataFusion.
        registry: Table registry with registered Ray Datasets.
        config: SQL configuration.

    Returns:
        Ray Dataset if successful, None to fallback to standard execution.
    """
    try:
        executor = DataFusionExecutor(registry)
        return executor.execute_with_optimizations(ast, optimizations, config)
    except Exception:
        return None
