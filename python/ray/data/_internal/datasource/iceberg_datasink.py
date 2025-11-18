"""
Module to write a Ray Dataset into an iceberg table, by using the Ray Datasink API.
"""
import logging
import uuid
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Union

import ray
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.savemode import SaveMode
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink, WriteResult
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.catalog import Catalog
    from pyiceberg.manifest import DataFile
    from pyiceberg.table import Table, Transaction

    from ray.data.expressions import Expr


logger = logging.getLogger(__name__)


def _append_and_commit(
    txn: "Transaction",
    data_files: List["DataFile"],
    snapshot_properties: Dict[str, str],
) -> None:
    """Helper to append data files to a transaction and commit.

    Args:
        txn: PyIceberg transaction object
        data_files: List of DataFile objects to append
        snapshot_properties: Custom properties to write to snapshot summary
    """
    with txn._append_snapshot_producer(snapshot_properties) as append_files:
        for data_file in data_files:
            append_files.append_data_file(data_file)
    txn.commit_transaction()


@ray.remote(num_cpus=1)
def _commit_upsert_task(
    table_identifier: str,
    catalog_name: str,
    catalog_kwargs: Dict[str, Any],
    data_files: List["DataFile"],
    join_keys_dicts: List[Dict[str, List[Any]]],
    join_cols: List[str],
    snapshot_properties: Dict[str, str],
) -> None:
    """
    Ray task to commit upsert transaction with copy-on-write strategy.

    This runs as a Ray task to avoid OOM on the driver when:
    - There are many join keys to merge
    - The transaction commit is memory or time intensive

    Args:
        table_identifier: Iceberg table identifier
        catalog_name: Catalog name
        catalog_kwargs: Catalog initialization kwargs
        data_files: List of DataFile objects to commit
        join_keys_dicts: List of dictionaries mapping column names to lists of key values
        join_cols: List of join column names
        snapshot_properties: Custom properties to write to snapshot summary
    """
    import functools
    from collections import defaultdict

    import pyarrow as pa
    from pyiceberg import catalog
    from pyiceberg.table.upsert_util import create_match_filter

    # Load the table
    cat = catalog.load_catalog(catalog_name, **catalog_kwargs)
    table = cat.load_table(table_identifier)

    # Merge all join keys dictionaries
    merged_keys_dict = defaultdict(list)
    for join_keys_dict in join_keys_dicts:
        for col, values in join_keys_dict.items():
            merged_keys_dict[col].extend(values)

    # Start transaction
    txn = table.transaction()

    # Create delete filter if we have join keys
    if merged_keys_dict:
        # Create PyArrow table from join keys
        keys_table = pa.table(dict(merged_keys_dict))

        # Filter out rows with any NULL values in join columns
        # (NULL != NULL in SQL semantics)
        masks = (pa.compute.is_valid(keys_table[col]) for col in join_cols)
        mask = functools.reduce(pa.compute.and_, masks)
        keys_table = keys_table.filter(mask)

        # Only delete if we have non-NULL keys
        if len(keys_table) > 0:
            # Use PyIceberg's helper to build delete filter
            delete_filter = create_match_filter(keys_table, join_cols)
            txn.delete(
                delete_filter=delete_filter,
                snapshot_properties=snapshot_properties,
            )

    # Append new data files (includes updates and inserts) and commit
    _append_and_commit(txn, data_files, snapshot_properties)


@DeveloperAPI
class IcebergDatasink(
    Datasink[Union[List["DataFile"], tuple[List["DataFile"], Dict[str, List[Any]]]]]
):
    """
    Iceberg datasink to write a Ray Dataset into an existing Iceberg table.
    This datasink handles concurrent writes by:
    - Each worker writes Parquet files to storage and returns DataFile metadata
    - The driver collects all DataFile objects and performs a single commit
    """

    def __init__(
        self,
        table_identifier: str,
        catalog_kwargs: Optional[Dict[str, Any]] = None,
        snapshot_properties: Optional[Dict[str, str]] = None,
        mode: SaveMode = SaveMode.APPEND,
        overwrite_filter: Optional["Expr"] = None,
        upsert_kwargs: Optional[Dict[str, Any]] = None,
        overwrite_kwargs: Optional[Dict[str, Any]] = None,
        upsert_commit_memory: Optional[int] = None,
    ):
        """
        Initialize the IcebergDatasink

        Args:
            table_identifier: The identifier of the table such as `default.taxi_dataset`
            catalog_kwargs: Optional arguments to use when setting up the Iceberg catalog
            snapshot_properties: Custom properties to write to snapshot summary
            mode: Write mode - APPEND, UPSERT, or OVERWRITE. Defaults to APPEND.
                - APPEND: Add new data without checking for duplicates
                - UPSERT: Update existing rows or insert new ones based on a join condition
                - OVERWRITE: Replace table data (all data or filtered subset)
            overwrite_filter: Optional filter for OVERWRITE mode to perform partial overwrites.
                Must be a Ray Data expression from `ray.data.expressions`. Only rows matching
                this filter are replaced. If None with OVERWRITE mode, replaces all table data.
            upsert_kwargs: Optional arguments for upsert operations.
                Supported parameters: join_cols (List[str]), case_sensitive (bool),
                branch (str). Note: This implementation uses a copy-on-write strategy
                that always updates all columns for matched keys and inserts all new keys.
            overwrite_kwargs: Optional arguments to pass through to PyIceberg's table.overwrite()
                method. Supported parameters include case_sensitive (bool) and branch (str).
                See PyIceberg documentation for details.
            upsert_commit_memory: [For UPSERT mode only] The heap memory in bytes
                to reserve for the upsert commit operation. If None,
                uses Ray's default memory allocation.

        Note:
            Schema evolution is automatically enabled. New columns in the incoming data
            are automatically added to the table schema.
        """
        self.table_identifier = table_identifier
        self._catalog_kwargs = (catalog_kwargs or {}).copy()
        self._snapshot_properties = snapshot_properties or {}
        self._mode = mode
        self._overwrite_filter = overwrite_filter
        self._upsert_kwargs = (upsert_kwargs or {}).copy()
        self._overwrite_kwargs = (overwrite_kwargs or {}).copy()
        self._upsert_commit_memory = upsert_commit_memory

        # Validate kwargs are only set for relevant modes
        if self._upsert_kwargs and self._mode != SaveMode.UPSERT:
            raise ValueError(
                f"upsert_kwargs can only be specified when mode is SaveMode.UPSERT, but mode is {self._mode}"
            )
        if self._overwrite_kwargs and self._mode != SaveMode.OVERWRITE:
            raise ValueError(
                f"overwrite_kwargs can only be specified when mode is SaveMode.OVERWRITE, but mode is {self._mode}"
            )

        # Remove invalid parameters from overwrite_kwargs if present
        for invalid_param, reason in [
            (
                "overwrite_filter",
                "should be passed as a separate parameter to write_iceberg()",
            ),
            (
                "delete_filter",
                "is an internal PyIceberg parameter; use 'overwrite_filter' instead",
            ),
        ]:
            if self._overwrite_kwargs.pop(invalid_param, None) is not None:
                logger.warning(
                    f"Removed '{invalid_param}' from overwrite_kwargs: {reason}"
                )

        if "name" in self._catalog_kwargs:
            self._catalog_name = self._catalog_kwargs.pop("name")
        else:
            self._catalog_name = "default"

        self._table: "Table" = None
        self._write_uuid: uuid.UUID = None

    def __getstate__(self) -> dict:
        """Exclude `_table` during pickling."""
        state = self.__dict__.copy()
        state.pop("_table", None)
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._table = None

    def _get_catalog(self) -> "Catalog":
        from pyiceberg import catalog

        return catalog.load_catalog(self._catalog_name, **self._catalog_kwargs)

    def _reload_table(self) -> None:
        """Reload the Iceberg table from the catalog."""
        catalog = self._get_catalog()
        self._table = catalog.load_table(self.table_identifier)

    def _get_join_cols(self) -> List[str]:
        """Get join columns for upsert, using table identifier fields as fallback."""
        join_cols = self._upsert_kwargs.get("join_cols", [])
        if not join_cols:
            # Use table's identifier fields as fallback
            for field_id in self._table.metadata.schema().identifier_field_ids:
                col_name = self._table.metadata.schema().find_column_name(field_id)
                if col_name:
                    join_cols.append(col_name)
        return join_cols

    def _update_schema(self, incoming_schema: "pa.Schema") -> None:
        """
        Update the table schema to accommodate incoming data using union-by-name semantics.

        This automatically handles:
        - Adding new columns from the incoming schema
        - Type promotion (e.g., int32 -> int64) where compatible
        - Preserving existing columns not in the incoming schema
        - Concurrent schema updates from multiple workers (with retry logic)

        Note:
            Each worker calls this once with the first block's schema. All blocks
            within a worker are validated to have the same schema before calling this.

        Args:
            incoming_schema: The PyArrow schema from the incoming data
        """
        from pyiceberg.exceptions import CommitFailedException

        max_retries = 3

        for attempt in range(max_retries):
            try:
                with self._table.update_schema() as update:
                    update.union_by_name(incoming_schema)
                # Succeeded, reload to get latest table version and exit.
                self._reload_table()
                return
            except CommitFailedException:
                if attempt < max_retries - 1:
                    logger.debug(
                        f"Schema update conflict - another worker modified schema, "
                        f"reloading and retrying (attempt {attempt + 1}/{max_retries})"
                    )
                    self._reload_table()
                else:
                    logger.error(
                        "Failed to update schema after %d retries due to conflicts.",
                        max_retries,
                    )
                    raise

    def on_write_start(self) -> None:
        """Initialize table for writing and create a shared write UUID."""
        self._table = self._get_catalog().load_table(self.table_identifier)
        self._write_uuid = uuid.uuid4()

        # Validate join_cols for UPSERT mode before writing any files
        if self._mode == SaveMode.UPSERT:
            join_cols = self._upsert_kwargs.get("join_cols", [])
            if not join_cols:
                # Check if table has identifier fields as fallback
                identifier_field_ids = (
                    self._table.metadata.schema().identifier_field_ids
                )
                if not identifier_field_ids:
                    raise ValueError(
                        "join_cols must be specified in upsert_kwargs for UPSERT mode "
                        "when table has no identifier fields"
                    )

    def write(
        self, blocks: Iterable[Block], ctx: TaskContext
    ) -> Union[List["DataFile"], tuple[List["DataFile"], Dict[str, List[Any]]]]:
        """
        Write blocks to Parquet files in storage and return DataFile metadata.

        This runs on each worker in parallel. Files are written directly to storage
        (S3, HDFS, etc.) and only metadata is returned to the driver.

        Args:
            blocks: Iterable of Ray Data blocks to write
            ctx: TaskContext object containing task-specific information

        Returns:
            For APPEND/OVERWRITE: List of DataFile objects.
            For UPSERT: Tuple of (List of DataFile objects, Dict mapping col names to key values).
        """
        from pyiceberg.io.pyarrow import _dataframe_to_data_files

        if self._table is None:
            self._reload_table()

        all_data_files = []
        join_keys_dict = defaultdict(list)
        first_schema = None

        # Extract join keys for copy-on-write upsert
        extract_join_keys = self._mode == SaveMode.UPSERT

        for block in blocks:
            pa_table = BlockAccessor.for_block(block).to_arrow()
            if pa_table.num_rows > 0:
                # Schema validation and update strategy:
                # 1. Each worker validates all its blocks have the same schema
                # 2. Schema is updated once per worker (not per block) to minimize conflicts
                # 3. Concurrent updates from different workers are handled by retry logic
                if first_schema is None:
                    first_schema = pa_table.schema
                    # Update schema once per worker (handles concurrent updates from other workers)
                    self._update_schema(first_schema)
                elif not pa_table.schema.equals(first_schema):
                    raise ValueError(
                        f"Schema mismatch within worker: expected {first_schema}, "
                        f"got {pa_table.schema}. All blocks must have the same schema."
                    )

                # Extract join key values for copy-on-write upsert
                # These will be used to build a delete filter for matching rows
                # Example: Upserting [{col_a: 2}, {col_a: 3}] extracts {col_a: [2, 3]}
                #          which becomes filter: col_a IN (2, 3)
                if extract_join_keys:
                    join_cols = self._get_join_cols()
                    for col in join_cols:
                        join_keys_dict[col].extend(pa_table[col].to_pylist())

                # Write data files to storage (distributed!)
                # _dataframe_to_data_files writes Parquet files and returns DataFile metadata
                data_files = list(
                    _dataframe_to_data_files(
                        table_metadata=self._table.metadata,
                        write_uuid=self._write_uuid,
                        df=pa_table,
                        io=self._table.io,
                    )
                )
                all_data_files.extend(data_files)

        # Return appropriate type based on whether we extracted join keys
        if extract_join_keys:
            return (all_data_files, join_keys_dict)
        return all_data_files

    def on_write_complete(self, write_result: WriteResult) -> None:
        """
        Complete the write by committing all data files in a single transaction.

        This runs on the driver after all workers finish writing files.
        Collects all DataFile objects from all workers and performs a single
        atomic commit based on the configured mode.
        """
        # Reload table to get latest metadata
        self._reload_table()

        # Collect all data files and join keys (if applicable) from all workers
        all_data_files = []
        join_keys_dicts = []

        # Check if we're in upsert mode (returns tuple with join keys)
        use_copy_on_write_upsert = self._mode == SaveMode.UPSERT

        for write_return in write_result.write_returns:
            if not write_return:
                continue

            if use_copy_on_write_upsert:
                # For copy-on-write upsert, write() returns (data_files, join_keys_dict)
                data_files, join_keys_dict = write_return
                all_data_files.extend(data_files)
                # Collect join keys dicts (will be merged in Ray task)
                join_keys_dicts.append(join_keys_dict)
            else:
                # For other modes, write() returns just data_files
                all_data_files.extend(write_return)

        if not all_data_files:
            return

        # Create transaction and commit based on mode
        if self._mode == SaveMode.APPEND:
            self._commit_append(all_data_files)
        elif self._mode == SaveMode.OVERWRITE:
            self._commit_overwrite(all_data_files)
        elif self._mode == SaveMode.UPSERT:
            # Execute entire commit in Ray task to avoid OOM on driver
            join_cols = self._get_join_cols()

            # Configure task options with memory if specified
            task_options = {}
            if self._upsert_commit_memory is not None:
                task_options["memory"] = self._upsert_commit_memory

            ray.get(
                _commit_upsert_task.options(**task_options).remote(
                    table_identifier=self.table_identifier,
                    catalog_name=self._catalog_name,
                    catalog_kwargs=self._catalog_kwargs,
                    data_files=all_data_files,
                    join_keys_dicts=join_keys_dicts,
                    join_cols=join_cols,
                    snapshot_properties=self._snapshot_properties,
                )
            )
        else:
            raise ValueError(f"Unsupported mode: {self._mode}")

    def _commit_append(self, data_files: List["DataFile"]) -> None:
        """Commit data files using APPEND mode."""
        txn = self._table.transaction()
        _append_and_commit(txn, data_files, self._snapshot_properties)

    def _commit_overwrite(self, data_files: List["DataFile"]) -> None:
        """Commit data files using OVERWRITE mode."""
        txn = self._table.transaction()
        # Delete matching data if filter provided
        if self._overwrite_filter is not None:
            from ray.data._internal.datasource.iceberg_datasource import (
                _IcebergExpressionVisitor,
            )

            visitor = _IcebergExpressionVisitor()
            pyi_filter = visitor.visit(self._overwrite_filter)

            txn.delete(
                delete_filter=pyi_filter,
                snapshot_properties=self._snapshot_properties,
                **self._overwrite_kwargs,
            )
        else:
            # Full overwrite - delete all
            from pyiceberg.expressions import AlwaysTrue

            txn.delete(
                delete_filter=AlwaysTrue(),
                snapshot_properties=self._snapshot_properties,
                **self._overwrite_kwargs,
            )

        # Append new data files and commit
        _append_and_commit(txn, data_files, self._snapshot_properties)
