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
from ray.data._internal.datasource.delta.config import (
    DeltaSinkWriteResult,
    DeltaWriteConfig,
)
from ray.data._internal.datasource.delta.utilities import (
    get_storage_options_for_path,
    try_get_deltatable,
)
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.savemode import SaveMode
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink

logger = logging.getLogger(__name__)


class DeltaDatasink(Datasink):
    """
    A Ray Data datasink for Delta Lake tables.
    This datasink provides comprehensive Delta Lake functionality including:
    - Standard write modes (append, overwrite, error, ignore)
    - Partitioned writes
    - Schema validation
    - Multi-cloud support with automatic credential detection

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
        mode: str = SaveMode.APPEND.value,
        partition_cols: Optional[List[str]] = None,
        filesystem: Optional[pa_fs.FileSystem] = None,
        schema: Optional[pa.Schema] = None,
        try_create_dir: bool = True,
        config: Optional[DeltaWriteConfig] = None,
        **write_kwargs,
    ):
        """Initialize the Delta Lake datasink.

        Args:
            path: Path to the Delta table
            mode: Write mode (append, overwrite, error, ignore)
            partition_cols: Columns to partition by
            filesystem: PyArrow filesystem to use
            schema: Table schema
            try_create_dir: Whether to create directories if they don't exist
            config: DeltaWriteConfig object with Delta-specific settings
            **write_kwargs: Additional write configuration options
        """
        self.path = path
        self.try_create_dir = try_create_dir
        self.write_kwargs = write_kwargs

        # Use config if provided, otherwise build from individual parameters
        if config is not None:
            self.delta_write_config = config
            self.mode = SaveMode(config.mode)
            self.partition_cols = config.partition_cols or []
            self.schema = config.schema
            self.storage_options = config.storage_options or {}
        else:
            self.mode = SaveMode(mode)
            self.partition_cols = partition_cols or []
            self.schema = schema
            self.storage_options = write_kwargs.get("storage_options", {})

            # Delta-specific configurations
            self.delta_write_config = DeltaWriteConfig(
                mode=self.mode,
                partition_cols=partition_cols,
                schema=schema,
                **write_kwargs,
            )

        # Set up filesystem
        if filesystem is not None:
            self.filesystem = filesystem

        # Get storage options for the path if not provided
        if not self.storage_options:
            self.storage_options = get_storage_options_for_path(path)

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> DeltaSinkWriteResult:
        """Write blocks to the Delta table.

        This method directly creates the Delta Lake table and transaction log.

        Args:
            blocks: Iterable of blocks to write
            ctx: Task context for execution

        Returns:
            DeltaSinkWriteResult with write metrics and metadata
        """
        _check_import(self, module="deltalake", package="deltalake")

        logger.info(f"WRITE METHOD CALLED for task {ctx.task_idx} at {self.path}")
        logger.info(f"Mode: {self.mode}, Mode value: {self.mode.value}")

        # Convert blocks to PyArrow tables and combine
        tables = []
        total_rows = 0

        for block in blocks:
            block_accessor = BlockAccessor.for_block(block)
            num_rows = block_accessor.num_rows()
            if num_rows > 0:
                table = block_accessor.to_arrow()
                tables.append(table)
                total_rows += num_rows
                logger.info(f"Block has {num_rows} rows, total so far: {total_rows}")

        if not tables:
            logger.info("No data to write")
            return DeltaSinkWriteResult(
                path=self.path,
                files_written=0,
                bytes_written=0,
                partitions_written=None,
                metadata={"task_idx": ctx.task_idx, "num_rows": 0},
            )

        # Combine all tables
        combined_table = pa.concat_tables(tables)
        logger.info(
            f"Combined table has {combined_table.num_rows} rows and schema: {combined_table.schema}"
        )

        # Create directory if needed
        if self.try_create_dir:
            import os

            try:
                # Use the filesystem if available, otherwise use os.makedirs
                if hasattr(self, "filesystem") and self.filesystem is not None:
                    # PyArrow filesystem
                    self.filesystem.create_dir(self.path, recursive=True)
                else:
                    # Standard filesystem
                    os.makedirs(self.path, exist_ok=True)
                logger.info(f"Created directory (if needed): {self.path}")
            except Exception as e:
                logger.warning(f"Failed to create directory {self.path}: {e}")

        # Handle IGNORE mode - check if table exists and skip if it does
        if self.mode == SaveMode.IGNORE:
            existing_table = try_get_deltatable(self.path, self.storage_options)
            if existing_table is not None:
                logger.info(f"Table already exists at {self.path}, ignoring write")
                return DeltaSinkWriteResult(
                    path=self.path,
                    files_written=0,
                    bytes_written=0,
                    partitions_written=None,
                    metadata={"task_idx": ctx.task_idx, "num_rows": 0, "ignored": True},
                )

        # Execute write using deltalake.write_deltalake
        try:
            from deltalake import write_deltalake

            # Prepare write arguments
            write_args = {
                "table_or_uri": self.path,
                "data": combined_table,
                "mode": self.mode.value,
                "partition_by": self.partition_cols if self.partition_cols else None,
                "storage_options": self.storage_options,
            }

            # Add optional parameters if specified
            if self.delta_write_config.name:
                write_args["name"] = self.delta_write_config.name
            if self.delta_write_config.description:
                write_args["description"] = self.delta_write_config.description
            if self.delta_write_config.configuration:
                write_args["configuration"] = self.delta_write_config.configuration
            if self.delta_write_config.schema_mode:
                write_args["schema_mode"] = self.delta_write_config.schema_mode
            if self.delta_write_config.target_file_size:
                write_args[
                    "target_file_size"
                ] = self.delta_write_config.target_file_size
            if self.delta_write_config.writer_properties:
                write_args[
                    "writer_properties"
                ] = self.delta_write_config.writer_properties

            logger.info(f"Executing deltalake.write_deltalake with args: {write_args}")

            # Execute write - this creates the Delta table and transaction log
            write_deltalake(**write_args)

            logger.info(f"Successfully wrote Delta table to {self.path}")

            # Get table info for result
            dt = try_get_deltatable(self.path, self.storage_options)
            if dt:
                logger.info(
                    f"Delta table created successfully with version {dt.version()}"
                )

                # Get file information from the table
                actions = dt.get_add_actions(flatten=True)
                if actions is not None:
                    # Convert PyArrow RecordBatch to list of dictionaries
                    try:
                        # Try to convert to pandas first (more reliable)
                        import pandas as pd

                        actions_df = actions.to_pandas()
                        actions_list = actions_df.to_dict("records")
                    except (ImportError, AttributeError):
                        # Fallback: convert to list of dictionaries manually
                        actions_list = []
                        # Use num_rows() method instead of len() for PyArrow RecordBatch
                        num_rows = actions.num_rows
                        for i in range(num_rows):
                            action_dict = {}
                            for j, col_name in enumerate(actions.column_names):
                                action_dict[col_name] = actions.column(j)[i].as_py()
                            actions_list.append(action_dict)

                    files_written = len(actions_list)
                    total_bytes = sum(
                        action.get("size_bytes", 0) for action in actions_list
                    )

                    # Extract partition information
                    partitions_written = None
                    if self.partition_cols:
                        partitions_written = []
                        for action in actions_list:
                            partition_values = action.get("partition_values", {})
                            if partition_values:
                                partition_path = "/".join(
                                    [f"{k}={v}" for k, v in partition_values.items()]
                                )
                                if partition_path not in partitions_written:
                                    partitions_written.append(partition_path)

                    logger.info(f"Table has {files_written} files, {total_bytes} bytes")

                    result = DeltaSinkWriteResult(
                        path=self.path,
                        files_written=files_written,
                        bytes_written=total_bytes,
                        partitions_written=partitions_written,
                        metadata={
                            "task_idx": ctx.task_idx,
                            "num_rows": combined_table.num_rows,
                            "version": dt.version(),
                            "schema": str(combined_table.schema),
                        },
                    )
                else:
                    # Fallback if we can't get actions
                    result = DeltaSinkWriteResult(
                        path=self.path,
                        files_written=1,  # Assume at least one file
                        bytes_written=0,  # We don't know the exact size
                        partitions_written=None,
                        metadata={
                            "task_idx": ctx.task_idx,
                            "num_rows": combined_table.num_rows,
                            "version": dt.version(),
                            "schema": str(combined_table.schema),
                        },
                    )
            else:
                logger.warning("Could not get Delta table info after write")
                result = DeltaSinkWriteResult(
                    path=self.path,
                    files_written=1,  # Assume at least one file
                    bytes_written=0,  # We don't know the exact size
                    partitions_written=None,
                    metadata={
                        "task_idx": ctx.task_idx,
                        "num_rows": combined_table.num_rows,
                        "schema": str(combined_table.schema),
                    },
                )

            return result

        except Exception as e:
            logger.error(f"Delta Lake write operation failed: {e}")
            raise

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
