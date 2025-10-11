"""
Delta Lake datasink implementation with two-phase commit for ACID compliance.
"""

import json
import logging
import os
import time
import uuid
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional

import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq

from ray.data._internal.datasource.delta.config import DeltaWriteConfig, WriteMode
from ray.data._internal.datasource.delta.utilities import (
    DeltaUtilities,
    try_get_deltatable,
)
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink, WriteResult

if TYPE_CHECKING:
    from deltalake.transaction import AddAction

logger = logging.getLogger(__name__)


class DeltaDatasink(Datasink[List["AddAction"]]):
    """
    Ray Data datasink for Delta Lake tables using two-phase commit.

    Ensures ACID compliance at dataset level:
    1. write(): Per-task Parquet writes, returns AddAction metadata
    2. on_write_complete(): Collects all AddActions, single transaction commit

    Supports: append, overwrite, error, ignore modes
    Storage: Local, S3, GCS, Azure, HDFS
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
        _check_import(self, module="deltalake", package="deltalake")

        self.path = path
        self.mode = self._validate_mode(mode)
        self.partition_cols = partition_cols or []
        self.schema = schema
        self.write_kwargs = write_kwargs

        # Initialize Delta utilities
        self.delta_utils = DeltaUtilities(
            path, storage_options=write_kwargs.get("storage_options")
        )
        self.storage_options = self.delta_utils.storage_options

        # Delta write configuration
        self.delta_write_config = DeltaWriteConfig(
            mode=self.mode,
            partition_cols=partition_cols,
            schema=schema,
            **write_kwargs,
        )

        # Set up filesystem
        self.filesystem = filesystem or pa_fs.FileSystem.from_uri(path)[0]

    def _validate_mode(self, mode: str) -> WriteMode:
        """Validate and return WriteMode."""
        if mode not in ["append", "overwrite", "error", "ignore"]:
            if mode == "merge":
                raise ValueError(
                    "Merge mode not supported in v1. Use 'append' or 'overwrite' modes."
                )
            raise ValueError(
                f"Invalid mode '{mode}'. "
                f"Supported: 'append', 'overwrite', 'error', 'ignore'"
            )
        return WriteMode(mode)

    def on_write_start(self) -> None:
        """Check ERROR and IGNORE modes before writing files to prevent wasted I/O."""
        _check_import(self, module="deltalake", package="deltalake")

        existing_table = try_get_deltatable(self.path, self.storage_options)

        # For ERROR mode, fail immediately if table exists
        if self.mode == WriteMode.ERROR and existing_table:
            raise ValueError(
                f"Delta table already exists at {self.path}. "
                f"Use mode='append' or 'overwrite'."
            )
        
        # For IGNORE mode, fail immediately if table exists to prevent wasted computation
        # Note: This is a deliberate early exit to save I/O operations
        if self.mode == WriteMode.IGNORE and existing_table:
            # We raise a special exception that should be caught and handled gracefully
            # by the calling code to skip the write entirely
            raise FileExistsError(
                f"Delta table already exists at {self.path}. "
                f"Skipping write due to mode='ignore'."
            )

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> List["AddAction"]:
        """
        Phase 1: Write Parquet files, return AddAction metadata (no commit).

        Args:
            blocks: Data blocks to write
            ctx: Task context

        Returns:
            List of AddAction objects for written files
        """
        _check_import(self, module="deltalake", package="deltalake")

        # Convert blocks to PyArrow tables
        tables = []
        for block in blocks:
            if BlockAccessor.for_block(block).num_rows() > 0:
                tables.append(BlockAccessor.for_block(block).to_arrow())

        if not tables:
            return []

        combined_table = pa.concat_tables(tables)

        # Validate partition columns
        if self.partition_cols:
            missing = [
                c for c in self.partition_cols if c not in combined_table.column_names
            ]
            if missing:
                raise ValueError(
                    f"Partition columns {missing} not in table. "
                    f"Available: {combined_table.column_names}"
                )

        # Partition and write
        add_actions = []
        if self.partition_cols:
            partitioned_tables = self._partition_table(
                combined_table, self.partition_cols
            )
            for partition_values, partition_table in partitioned_tables.items():
                action = self._write_partition(
                    partition_table, partition_values, ctx.task_idx
                )
                add_actions.append(action)
        else:
            action = self._write_partition(combined_table, (), ctx.task_idx)
            add_actions.append(action)

        return add_actions

    def _partition_table(
        self, table: pa.Table, partition_cols: List[str]
    ) -> Dict[tuple, pa.Table]:
        """Partition table by columns efficiently using vectorized operations."""
        import pyarrow.compute as pc
        from collections import defaultdict

        partitions = {}

        if len(partition_cols) == 1:
            # Single column: use optimized unique + filter
            col = partition_cols[0]
            for val in pc.unique(table[col]):
                val_py = val.as_py()
                mask = pc.equal(table[col], val)
                partitions[(val_py,)] = table.filter(mask)
        else:
            # Multi-column: vectorized approach using to_pylist()
            # Convert partition columns to Python lists once (vectorized)
            partition_values_lists = [
                table[col].to_pylist() for col in partition_cols
            ]
            
            # Group row indices by partition key using zip (vectorized)
            partition_indices = defaultdict(list)
            for i, partition_tuple in enumerate(zip(*partition_values_lists)):
                partition_indices[partition_tuple].append(i)
            
            # Create table slices for each partition using take()
            for partition_tuple, indices in partition_indices.items():
                partitions[partition_tuple] = table.take(indices)

        return partitions

    def _write_partition(
        self,
        table: pa.Table,
        partition_values: tuple,
        task_idx: int,
    ) -> "AddAction":
        """Write partition and create AddAction."""
        from deltalake.transaction import AddAction

        # Generate filename
        filename = f"part-{task_idx:05d}-{uuid.uuid4().hex}.parquet"

        # Build partition path
        partition_path = ""
        partition_dict = {}
        if self.partition_cols and partition_values:
            parts = []
            for col, val in zip(self.partition_cols, partition_values):
                val_str = "" if val is None else str(val)
                parts.append(f"{col}={val_str}")
                partition_dict[col] = None if val is None else str(val)
            partition_path = "/".join(parts) + "/"

        # Write file
        relative_path = partition_path + filename
        full_path = os.path.join(self.path, relative_path)
        file_size = self._write_parquet_file(table, full_path)

        # Compute statistics
        stats = self._compute_statistics(table)

        return AddAction(
            path=relative_path,
            size=file_size,
            partition_values=partition_dict,
            modification_time=int(time.time() * 1000),
            data_change=True,
            stats=stats,
        )

    def _write_parquet_file(self, table: pa.Table, file_path: str) -> int:
        """Write Parquet file and return size."""
        # Remove partition columns from file
        if self.partition_cols:
            table = table.drop(self.partition_cols)

        # Create parent directory
        parent = os.path.dirname(file_path)
        if parent:
            try:
                self.filesystem.create_dir(parent, recursive=True)
            except Exception:
                pass

        # Write file
        pq.write_table(
            table,
            file_path,
            filesystem=self.filesystem,
            compression=self.write_kwargs.get("compression", "snappy"),
            write_statistics=True,
        )

        return self.filesystem.get_file_info(file_path).size

    def _compute_statistics(self, table: pa.Table) -> str:
        """Compute statistics JSON for table."""
        import pyarrow.compute as pc

        stats = {"numRecords": len(table)}
        min_values = {}
        max_values = {}
        null_counts = {}

        for col_name in table.column_names:
            column = table[col_name]
            null_counts[col_name] = column.null_count

            # Compute min/max for numeric and string types
            try:
                if column.null_count < len(column):
                    col_type = column.type
                    if pa.types.is_integer(col_type) or pa.types.is_floating(col_type):
                        min_val = pc.min(column).as_py()
                        max_val = pc.max(column).as_py()
                        if min_val is not None:
                            min_values[col_name] = min_val
                        if max_val is not None:
                            max_values[col_name] = max_val
                    elif pa.types.is_string(col_type) or pa.types.is_large_string(
                        col_type
                    ):
                        min_val = pc.min(column).as_py()
                        max_val = pc.max(column).as_py()
                        if min_val is not None:
                            min_values[col_name] = str(min_val)
                        if max_val is not None:
                            max_values[col_name] = str(max_val)
            except Exception:
                pass

        if min_values:
            stats["minValues"] = min_values
        if max_values:
            stats["maxValues"] = max_values
        if null_counts:
            stats["nullCount"] = null_counts

        return json.dumps(stats)

    def on_write_complete(self, write_result: WriteResult[List["AddAction"]]):
        """
        Phase 2: Commit all files in single transaction.

        Args:
            write_result: Aggregated results from all tasks

        Returns:
            None
        """
        from deltalake.transaction import create_table_with_add_actions

        # Collect all AddActions
        all_add_actions = []
        for task_actions in write_result.write_returns:
            all_add_actions.extend(task_actions)

        if not all_add_actions:
            return

        existing_table = try_get_deltatable(self.path, self.storage_options)

        if not existing_table:
            # Create new table
            schema = self._infer_schema(all_add_actions)
            from deltalake import Schema as DeltaSchema

            delta_schema = DeltaSchema.from_arrow(schema)

            create_table_with_add_actions(
                table_uri=self.path,
                schema=delta_schema,
                add_actions=all_add_actions,
                mode=self.mode.value,
                partition_by=self.partition_cols or None,
                name=self.delta_write_config.name,
                description=self.delta_write_config.description,
                configuration=self.delta_write_config.configuration,
                storage_options=self.storage_options,
                commit_properties=self.delta_write_config.commit_properties,
                post_commithook_properties=self.delta_write_config.post_commithook_properties,
            )
        else:
            # Append to existing table
            # These checks handle race conditions where table was created
            # between on_write_start and on_write_complete
            if self.mode == WriteMode.ERROR:
                raise ValueError(
                    f"Race condition: Delta table was created at {self.path} "
                    f"after write started. Data files written but not committed."
                )
            if self.mode == WriteMode.IGNORE:
                # Silently skip commit if table was created during write
                return

            mode_value = "overwrite" if self.mode == WriteMode.OVERWRITE else "append"

            # Create and commit write transaction
            transaction = existing_table.create_write_transaction(
                actions=all_add_actions,
                mode=mode_value,
                schema=existing_table.schema(),
                partition_by=self.partition_cols or None,
                commit_properties=self.delta_write_config.commit_properties,
                post_commithook_properties=self.delta_write_config.post_commithook_properties,
            )
            # Commit the transaction to persist data
            transaction.commit()

    def _infer_schema(self, add_actions: List["AddAction"]) -> pa.Schema:
        """Infer schema from first file and partition columns."""
        if self.schema:
            return self.schema

        # Read schema from first file
        first_file = os.path.join(self.path, add_actions[0].path)
        schema = pq.read_schema(first_file, filesystem=self.filesystem)

        # Add partition columns
        if self.partition_cols:
            for col in self.partition_cols:
                if col in add_actions[0].partition_values:
                    val = add_actions[0].partition_values[col]
                    col_type = self._infer_partition_type(val)
                    schema = schema.append(pa.field(col, col_type))

        return schema

    def _infer_partition_type(self, value: Optional[str]) -> pa.DataType:
        """Infer PyArrow type from partition value."""
        if not value:
            return pa.string()

        try:
            int(value)
            return pa.int64()
        except ValueError:
            pass

        try:
            float(value)
            return pa.float64()
        except ValueError:
            pass

        return pa.string()

    def on_write_failed(self, error: Exception) -> None:
        """Handle write failure."""
        logger.error(
            f"Delta write failed for {self.path}: {error}. "
            f"Uncommitted files will be cleaned by Delta vacuum."
        )

    @property
    def supports_distributed_writes(self) -> bool:
        return True

    @property
    def min_rows_per_write(self) -> Optional[int]:
        return None

    def get_name(self) -> str:
        return "Delta"
