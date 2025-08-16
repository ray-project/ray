"""
Table optimization functionality for Delta Lake datasource.

This module contains the DeltaTableOptimizer class which handles
compaction, Z-order optimization, and vacuum operations.
"""

import logging
from typing import Any,Dict, Optional

from deltalake import DeltaTable

from ray.data._internal.datasource.delta.config import (
    OptimizationConfig,
    OptimizationMode,
)

logger = logging.getLogger(__name__)


class DeltaTableOptimizer:
    """Handles optimization operations for Delta tables."""

    def __init__(
        self,
        table_path: str,
        storage_options: Optional[Dict[str, str]] = None,
        config: Optional[OptimizationConfig] = None,
    ):
        """
        Initialize Delta table optimizer.

        Args:
            table_path: Path to the Delta table
            storage_options: Storage options for the filesystem
            config: Optimization configuration
        """
        self.table_path = table_path
        self.storage_options = storage_options or {}
        self.config = config or OptimizationConfig(mode=OptimizationMode.COMPACT)

    def optimize(self) -> Dict[str, Any]:
        """
        Execute optimization operations based on configuration.

        Returns:
            Dict with optimization metrics
        """
        try:
            dt = DeltaTable(self.table_path, storage_options=self.storage_options)

            if self.config.mode == OptimizationMode.COMPACT:
                return self._compact(dt)
            elif self.config.mode == OptimizationMode.Z_ORDER:
                return self._z_order(dt)
            elif self.config.mode == OptimizationMode.VACUUM:
                return self._vacuum(dt)
            else:
                raise ValueError(f"Unknown optimization mode: {self.config.mode}")

        except Exception as e:
            logger.error(f"Optimization failed: {e}")
            raise

    def _compact(self, dt: DeltaTable) -> Dict[str, Any]:
        """
        Perform file compaction.

        Args:
            dt: DeltaTable instance

        Returns:
            Dict with compaction metrics
        """
        try:
            metrics = dt.optimize.compact(
                partition_filters=self.config.partition_filters,
                target_size=self.config.target_size_bytes,
                max_concurrent_tasks=self.config.max_concurrent_tasks,
            )

            return {
                "operation": "compact",
                "files_added": metrics.get("files_added", 0),
                "files_removed": metrics.get("files_removed", 0),
                "partitions_optimized": metrics.get("partitions_optimized", 0),
                "num_batches": metrics.get("num_batches", 0),
                "total_considered_files": metrics.get("total_considered_files", 0),
                "total_files_skipped": metrics.get("total_files_skipped", 0),
            }
        except Exception as e:
            logger.error(f"Compaction failed: {e}")
            raise

    def _z_order(self, dt: DeltaTable) -> Dict[str, Any]:
        """
        Perform Z-order optimization.

        Args:
            dt: DeltaTable instance

        Returns:
            Dict with Z-order metrics
        """
        if not self.config.z_order_columns:
            raise ValueError(
                "z_order_columns must be specified for Z-order optimization"
            )

        try:
            metrics = dt.optimize.z_order(
                columns=self.config.z_order_columns,
                partition_filters=self.config.partition_filters,
                target_size=self.config.target_size_bytes,
                max_concurrent_tasks=self.config.max_concurrent_tasks,
                max_spill_size=self.config.max_spill_size,
            )

            return {
                "operation": "z_order",
                "columns": self.config.z_order_columns,
                "files_added": metrics.get("files_added", 0),
                "files_removed": metrics.get("files_removed", 0),
                "partitions_optimized": metrics.get("partitions_optimized", 0),
                "num_batches": metrics.get("num_batches", 0),
                "total_considered_files": metrics.get("total_considered_files", 0),
                "total_files_skipped": metrics.get("total_files_skipped", 0),
            }
        except Exception as e:
            logger.error(f"Z-order optimization failed: {e}")
            raise

    def _vacuum(self, dt: DeltaTable) -> Dict[str, Any]:
        """
        Perform vacuum operation.

        Args:
            dt: DeltaTable instance

        Returns:
            Dict with vacuum metrics
        """
        try:
            # Default retention to 168 hours (7 days) if not specified
            retention_hours = self.config.retention_hours or 168

            files = dt.vacuum(
                retention_hours=retention_hours,
                enforce_retention_duration=True,
                dry_run=False,  # Note: always performs actual deletion
            )

            return {
                "operation": "vacuum",
                "retention_hours": retention_hours,
                "files_deleted": len(files),
                "deleted_files": files,
            }
        except Exception as e:
            logger.error(f"Vacuum failed: {e}")
            raise
