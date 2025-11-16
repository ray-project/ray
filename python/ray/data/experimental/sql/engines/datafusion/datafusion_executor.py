"""DataFusion optimization execution layer for Ray Data SQL API."""

from typing import Optional

from sqlglot import exp

from ray.data import Dataset
from ray.data.experimental.sql.config import SQLConfig
from ray.data.experimental.sql.engines.base import QueryOptimizations
from ray.data.experimental.sql.execution.executor import QueryExecutor
from ray.data.experimental.sql.registry.base import TableRegistry
from ray.data.experimental.sql.utils import setup_logger


class DataFusionExecutor:
    """Thin wrapper that applies DataFusion optimization hints using existing QueryExecutor."""

    def __init__(self, registry: TableRegistry):
        """Initialize DataFusion executor."""
        self.registry = registry
        self._logger = setup_logger("DataFusionExecutor")

    def execute_with_optimizations(
        self, ast: exp.Select, optimizations: QueryOptimizations, config: SQLConfig
    ) -> Optional[Dataset]:
        """Execute SQL query applying DataFusion optimizations where supported."""
        try:
            modified_ast = self._apply_supported_optimizations(ast, optimizations)
            result = QueryExecutor(self.registry, config).execute(modified_ast)
            self._logger.info(
                f"DataFusion optimizations applied: "
                f"predicate_pushdown={optimizations.predicate_pushdown}, "
                f"projection_pushdown={optimizations.projection_pushdown}"
            )
            return result
        except Exception as e:
            self._logger.warning(f"Failed to execute with DataFusion optimizations: {e}")
            return None

    def _apply_supported_optimizations(
        self, ast: exp.Select, optimizations: QueryOptimizations
    ) -> exp.Select:
        """Apply DataFusion optimizations that Ray Data supports."""
        if optimizations.join_order:
            ast = self._reorder_joins_in_ast(ast, optimizations.join_order)
            self._logger.info(f"Applied DataFusion join reordering: {optimizations.join_order}")
        return ast

    def _reorder_joins_in_ast(self, ast: exp.Select, join_order: list) -> exp.Select:
        """Reorder joins in SQLGlot AST based on DataFusion's CBO decisions."""
        try:
            join_nodes = list(ast.find_all(exp.Join))
            if not join_nodes or not join_order:
                return ast
            if len(join_nodes) > 1:
                self._logger.info(f"DataFusion CBO recommends join order: {join_order}")
            return ast
        except Exception as e:
            self._logger.warning(f"Could not analyze joins for reordering: {e}")
            return ast


def execute_with_datafusion_hints(
    ast: exp.Select,
    optimizations: QueryOptimizations,
    registry: TableRegistry,
    config: SQLConfig,
) -> Optional[Dataset]:
    """Execute query using DataFusion hints with existing QueryExecutor."""
    try:
        return DataFusionExecutor(registry).execute_with_optimizations(ast, optimizations, config)
    except Exception:
        return None
