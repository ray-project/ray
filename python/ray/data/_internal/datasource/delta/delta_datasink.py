"""
Main Delta Lake datasink implementation.

This module contains the DeltaDatasink class which serves as the primary
entry point for Delta Lake write operations in Ray Data.
"""

import logging
from typing import (
    Dict,
    Iterable,
    List,
    Optional,
)

import pyarrow as pa
import pyarrow.fs as pa_fs

from deltalake.transaction import AddAction as DeltaAddAction


from ray.data._internal.datasource.delta.config import (
    DeltaSinkWriteResult,
    DeltaWriteConfig,
    WriteMode,
)
from ray.data._internal.datasource.delta.merger import DeltaTableMerger
from ray.data._internal.datasource.delta.optimizer import DeltaTableOptimizer
from ray.data._internal.datasource.delta.utilities import (
    AWSUtilities,
    AzureUtilities,
    DeltaUtilities,
    GCPUtilities,
    try_get_deltatable,
)

from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.file_datasink import _FileDatasink

logger = logging.getLogger(__name__)


class DeltaDatasink(_FileDatasink):
    """
    A Ray Data datasink for Delta Lake tables.

    This datasink provides comprehensive Delta Lake functionality including:
    - Standard write modes (append, overwrite, error, ignore)
    - Advanced merge operations with DeltaMergeBuilder syntax
    - SCD (Slowly Changing Dimensions) Types 1, 2, and 3
    - Scalable microbatch processing
    - Multi-cloud support with automatic credential detection
    - Table optimization (compaction, Z-ordering, vacuum)

    Supported Storage Systems:
    - Local filesystem: /path/to/table or file:///path/to/table
    - AWS S3: s3://bucket/path/to/table or s3a://bucket/path/to/table
    - Google Cloud Storage: gs://bucket/path/to/table or gcs://bucket/path/to/table
    - Azure Blob Storage/ADLS: abfss://container@account.dfs.core.windows.net/path
    - Azure Data Lake: abfs://container@account.dfs.core.windows.net/path
    - HDFS: hdfs://namenode:port/path/to/table
    """

    def __init__(
        self,
        path: str,
        *,
        mode: str = WriteMode.APPEND.value,
        partition_cols: Optional[List[str]] = None,
        filesystem: Optional[pa_fs.FileSystem] = None,
        schema: Optional[pa.Schema] = None,
        **write_kwargs,
    ):
        """
        Initialize the Delta Lake datasink.

        Args:
            path: Path to the Delta table
            mode: Write mode (append, overwrite, error, ignore, merge)
            partition_cols: Columns to partition by
            filesystem: PyArrow filesystem to use
            schema: Table schema
            **write_kwargs: Additional write configuration options
        """
        self.path = path
        self.mode = WriteMode(mode)
        self.partition_cols = partition_cols or []
        self.schema = schema
        self.write_kwargs = write_kwargs

        # Initialize Delta utilities
        self.delta_utils = DeltaUtilities(
            path, storage_options=write_kwargs.get("storage_options")
        )

        # Cloud provider utilities
        self.aws_utils = AWSUtilities()
        self.gcp_utils = GCPUtilities()
        self.azure_utils = AzureUtilities()

        # Validate path
        self.delta_utils.validate_path(path)

        # Extract specific configuration objects
        self.merge_config = write_kwargs.pop("merge_config", None)
        self.optimization_config = write_kwargs.pop("optimization_config", None)

        # Delta-specific configurations
        self.delta_write_config = DeltaWriteConfig(
            mode=self.mode,
            partition_cols=partition_cols,
            schema=schema,
            merge_config=self.merge_config,
            optimization_config=self.optimization_config,
            **write_kwargs,
        )

        # Set up filesystem
        if filesystem is not None:
            self.filesystem = filesystem

        # Detect cloud provider from path scheme
        path_lower = self.path.lower()
        self.is_aws = path_lower.startswith(("s3://", "s3a://"))
        self.is_gcp = path_lower.startswith(("gs://", "gcs://"))
        self.is_azure = path_lower.startswith(("abfss://", "abfs://", "adl://"))
        self.storage_options = self._get_storage_options()

        super().__init__(path, filesystem=filesystem, **write_kwargs)

    def _get_storage_options(self) -> Dict[str, str]:
        """
        Get storage options based on the path and detected cloud provider.

        Returns:
            Dict with storage options
        """
        storage_options = self.write_kwargs.get("storage_options", {})

        if self.is_aws:
            aws_options = self.aws_utils.get_s3_storage_options(self.path)
            storage_options.update(aws_options)
        elif self.is_azure:
            azure_options = self.azure_utils.get_azure_storage_options(self.path)
            storage_options.update(azure_options)

        return storage_options

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> DeltaSinkWriteResult:
        """
        Write blocks to the Delta table.

        Args:
            blocks: Iterable of blocks to write
            ctx: Task context for execution

        Returns:
            DeltaSinkWriteResult with write metrics and metadata
        """
        _check_import(self, module="deltalake", package="deltalake")

        # Convert blocks to PyArrow tables and combine
        tables = []
        for block in blocks:
            if BlockAccessor.for_block(block).num_rows() > 0:
                table = BlockAccessor.for_block(block).to_arrow()
                tables.append(table)

        if not tables:
            # No data to write
            return DeltaSinkWriteResult(actions=[], schema=self.schema)

        # Combine all tables
        combined_table = pa.concat_tables(tables)

        # Execute write based on mode
        if self.mode == WriteMode.MERGE:
            return self._execute_merge_write(combined_table)
        else:
            return self._execute_standard_write(combined_table)

    def _execute_standard_write(self, table: pa.Table) -> DeltaSinkWriteResult:
        """Execute standard write operations (append, overwrite, etc.)."""
        try:
            from deltalake import write_deltalake

            # Prepare write arguments
            write_args = {
                "table_or_uri": self.path,
                "data": table,
                "mode": self.mode.value,
                "partition_by": self.partition_cols,
                "storage_options": self.storage_options,
                "schema_mode": self.delta_write_config.schema_mode,
                "engine": self.delta_write_config.engine,
            }

            # Add optional parameters if specified
            if self.delta_write_config.name:
                write_args["name"] = self.delta_write_config.name
            if self.delta_write_config.description:
                write_args["description"] = self.delta_write_config.description
            if self.delta_write_config.configuration:
                write_args["configuration"] = self.delta_write_config.configuration
            if self.delta_write_config.storage_options:
                write_args["storage_options"].update(
                    self.delta_write_config.storage_options
                )

            # Execute write
            write_deltalake(**write_args)

            # Get table info for result
            dt = try_get_deltatable(self.path, self.storage_options)
            if dt:
                actions = self._get_add_actions_from_table(dt)
                result = DeltaSinkWriteResult(actions=actions, schema=table.schema)
            else:
                result = DeltaSinkWriteResult(actions=[], schema=table.schema)

            # Perform post-write optimization if configured
            if self.optimization_config:
                try:
                    optimizer = DeltaTableOptimizer(
                        self.path,
                        storage_options=self.storage_options,
                        config=self.optimization_config,
                    )
                    optimization_result = optimizer.optimize()
                    logger.info(
                        f"Post-write optimization completed: {optimization_result}"
                    )

                    # Add optimization metrics to write result if possible
                    if hasattr(result, "optimization_metrics"):
                        result.optimization_metrics = optimization_result
                    elif isinstance(result, DeltaSinkWriteResult):
                        result.optimization_metrics = optimization_result

                except Exception as e:
                    logger.warning(
                        f"Post-write optimization failed but write operation succeeded. "
                        f"This is not critical and your data has been written successfully. "
                        f"Optimization error: {e}. "
                        f"You can manually run optimization later using the standalone optimization functions. "
                        "Example: ray.data._internal.datasource.delta.vacuum_delta_table()"
                    )

            return result

        except Exception as e:
            logger.error(f"Standard write operation failed: {e}")
            raise

    def _execute_merge_write(self, table: pa.Table) -> DeltaSinkWriteResult:
        """Execute merge write operations."""
        if not self.merge_config:
            raise ValueError("merge_config is required for merge operations")

        try:
            merger = DeltaTableMerger(self.path, storage_options=self.storage_options)
            merge_result = merger.execute_merge(table, self.merge_config)

            # Get updated table info
            dt = try_get_deltatable(self.path, self.storage_options)
            if dt:
                actions = self._get_add_actions_from_table(dt)
                result = DeltaSinkWriteResult(
                    actions=actions, schema=table.schema, merge_metrics=merge_result
                )
            else:
                result = DeltaSinkWriteResult(
                    actions=[], schema=table.schema, merge_metrics=merge_result
                )

            # Perform post-merge optimization if configured
            if self.optimization_config:
                try:
                    optimizer = DeltaTableOptimizer(
                        self.path,
                        storage_options=self.storage_options,
                        config=self.optimization_config,
                    )
                    optimization_result = optimizer.optimize()

                    # Add optimization metrics to write result
                    if hasattr(result, "optimization_metrics"):
                        result.optimization_metrics = optimization_result
                    elif isinstance(result, DeltaSinkWriteResult):
                        result.optimization_metrics = optimization_result

                except Exception as e:
                    logger.warning(
                        f"Post-merge optimization failed but merge operation succeeded. "
                        f"This is not critical and your data has been merged successfully. "
                        f"Optimization error: {e}. "
                        f"You can manually run optimization later using the standalone optimization functions. "
                        "Example: ray.data._internal.datasource.delta.vacuum_delta_table()"
                    )

            return result

        except Exception as e:
            logger.error(f"Merge operation failed: {e}")
            raise

    def _get_add_actions_from_table(self, dt) -> List:
        """Extract add actions from Delta table for result metadata."""
        try:
            actions = []

            # Get the latest version's add actions
            for action in dt.get_add_actions(flatten=True).to_pylist():
                # Try to create DeltaAddAction with stats
                try:
                    delta_action = DeltaAddAction(
                        path=str(action.path),
                        size_bytes=int(action.size_bytes) if action.size_bytes else 0,
                        partition_values=dict(action.partition_values)
                        if action.partition_values
                        else {},
                        modification_time=int(action.modification_time)
                        if action.modification_time
                        else 0,
                        data_change=bool(action.data_change)
                        if hasattr(action, "data_change")
                        else True,
                        stats=str(action.stats) if action.stats else None,
                    )
                except TypeError:
                    # If that fails, try without stats parameter
                    delta_action = DeltaAddAction(
                        path=str(action.path),
                        size_bytes=int(action.size_bytes) if action.size_bytes else 0,
                        partition_values=dict(action.partition_values)
                        if action.partition_values
                        else {},
                        modification_time=int(action.modification_time)
                        if action.modification_time
                        else 0,
                        data_change=bool(action.data_change)
                        if hasattr(action, "data_change")
                        else True,
                    )

                actions.append(delta_action)

            return actions

        except Exception as e:
            logger.warning(f"Could not extract add actions: {e}")
            return []

    @property
    def supports_distributed_writes(self) -> bool:
        """Delta Lake supports distributed writes."""
        return True

    @property
    def num_rows_per_write(self) -> Optional[int]:
        """Return the number of rows per write operation."""
        return None

    def get_name(self) -> str:
        """Get the name of this datasink."""
        return "Delta"
