"""
Module to write a Ray Dataset into an Apache Iceberg table using the Ray Datasink API.

This module leverages PyIceberg (https://py.iceberg.apache.org/) to write Ray Data
to Iceberg tables. It supports append and overwrite operations with proper transaction
handling.

Key features:
- Append mode: Add new data to existing tables
- Overwrite mode: Replace existing data (full table or filtered partitions)
- Transaction safety: Uses Iceberg's ACID transactions
- Partition-aware: Works with Iceberg's partition transforms
"""

import logging
import uuid
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Literal, Optional

from packaging import version

from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink, WriteResult
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.manifest import DataFile


logger = logging.getLogger(__name__)


@DeveloperAPI
class IcebergDatasink(Datasink[List["DataFile"]]):
    """
    Datasink for writing Ray Datasets to Apache Iceberg tables.

    This datasink leverages PyIceberg (https://py.iceberg.apache.org/) to write data
    to Iceberg tables with full transaction support. It supports both append and
    overwrite modes, with optional filtering for dynamic partition overwrites.

    Write modes:
        - "append": Add new data files to the table without removing existing data
        - "overwrite": Replace data in the table. Can be combined with overwrite_filter
          for dynamic partition overwrites

    Examples:
        Append data to an existing table:
            >>> ds.write_iceberg(  # doctest: +SKIP
            ...     table_identifier="db.table",
            ...     catalog_kwargs={"type": "glue"},
            ...     mode="append"
            ... )

        Overwrite entire table:
            >>> ds.write_iceberg(  # doctest: +SKIP
            ...     table_identifier="db.table",
            ...     catalog_kwargs={"type": "glue"},
            ...     mode="overwrite"
            ... )

        Dynamic partition overwrite (replace only affected partitions):
            >>> ds.write_iceberg(  # doctest: +SKIP
            ...     table_identifier="db.table",
            ...     catalog_kwargs={"type": "glue"},
            ...     mode="overwrite",
            ...     overwrite_filter=GreaterThanOrEqual("year", 2024)
            ... )

    Note:
        For row-level operations (DELETE, UPDATE, MERGE INTO), use Ray Data's
        existing primitives combined with overwrite mode:
        - Delete: Filter out unwanted rows, then overwrite
        - Update: Use map_batches() to transform rows, then overwrite
        - Merge/Upsert: Union datasets, deduplicate, then overwrite
    """

    def __init__(
        self,
        table_identifier: str,
        catalog_kwargs: Optional[Dict[str, Any]] = None,
        snapshot_properties: Optional[Dict[str, str]] = None,
        mode: Literal["append", "overwrite"] = "append",
        overwrite_filter: Optional["BooleanExpression"] = None,
    ):
        """
        Initialize the IcebergDatasink.

        Args:
            table_identifier: Fully qualified table identifier (e.g., "db_name.table_name")
            catalog_kwargs: Arguments to pass to PyIceberg's catalog.load_catalog()
                function. Common keys include:
                - "name": Catalog name (default: "default")
                - "type": Catalog type (e.g., "glue", "sql", "hive", "rest")
                - Additional catalog-specific configuration
                See https://py.iceberg.apache.org/configuration/ for details
            snapshot_properties: Custom properties to attach to the snapshot when
                committing, useful for tracking metadata (e.g.,
                {"commit_time": "2024-01-01T00:00:00Z", "app_name": "ray_data"})
            mode: Write mode - "append" adds new data, "overwrite" replaces existing data.
                Defaults to "append"
            overwrite_filter: Optional PyIceberg BooleanExpression to filter which data
                to overwrite. Only used when mode="overwrite". Enables dynamic partition
                overwriting where only partitions matching the filter are replaced.
                See https://py.iceberg.apache.org/api/#expressions for filter syntax
        """
        self.table_identifier = table_identifier
        self._catalog_kwargs = catalog_kwargs if catalog_kwargs is not None else {}
        self._snapshot_properties = (
            snapshot_properties if snapshot_properties is not None else {}
        )
        self._mode = mode
        self._overwrite_filter = overwrite_filter

        if mode not in ("append", "overwrite"):
            raise ValueError(f"Invalid mode '{mode}'. Must be 'append' or 'overwrite'.")

        if overwrite_filter is not None and mode != "overwrite":
            logger.warning(
                "overwrite_filter is specified but mode is not 'overwrite'. "
                "The filter will be ignored."
            )

        if "name" in self._catalog_kwargs:
            self._catalog_name = self._catalog_kwargs.pop("name")
        else:
            self._catalog_name = "default"

        # Will be initialized in on_write_start()
        from pyiceberg.io import FileIO
        from pyiceberg.table import Transaction
        from pyiceberg.table.metadata import TableMetadata

        self._uuid: str = None
        self._io: FileIO = None
        self._txn: Transaction = None
        self._table_metadata: TableMetadata = None
        self._manifest_merge_enabled: bool = False

    # Since iceberg transaction is not pickle-able, because of the table and catalog properties
    # we need to exclude the transaction object during serialization and deserialization during pickle
    def __getstate__(self) -> dict:
        """Exclude `_txn` during pickling.

        Note: The transaction object is only used on the driver (in on_write_start
        and on_write_complete). Write tasks only need _io and _table_metadata for
        writing files, so _txn can be safely excluded from serialization.
        """
        state = self.__dict__.copy()
        del state["_txn"]
        return state

    def __setstate__(self, state: dict) -> None:
        """Restore state after unpickling.

        The transaction is set to None since it's only needed on the driver.
        """
        self.__dict__.update(state)
        self._txn = None

    def _get_catalog(self) -> "Catalog":
        """
        Load the Iceberg catalog using PyIceberg's load_catalog function.

        Returns:
            Iceberg Catalog instance configured with the provided catalog_kwargs

        See: https://py.iceberg.apache.org/api/#catalog
        """
        from pyiceberg import catalog

        return catalog.load_catalog(self._catalog_name, **self._catalog_kwargs)

    def on_write_start(self) -> None:
        """
        Prepare for the write transaction.

        This method is called once before writing begins. It:
        1. Loads the Iceberg catalog and table
        2. Starts a transaction for ACID guarantees
        3. Extracts FileIO and metadata for distributed write tasks
        4. Validates partition transforms are compatible with PyArrow
        5. Checks manifest merge configuration

        Raises:
            ValueError: If the table has partition transforms that don't support
                PyArrow (e.g., custom transforms not implemented in PyIceberg)
        """
        import pyiceberg
        from pyiceberg.table import TableProperties

        # Handle version compatibility for property_as_bool utility function
        if version.parse(pyiceberg.__version__) >= version.parse("0.9.0"):
            from pyiceberg.utils.properties import property_as_bool
        else:
            from pyiceberg.table import PropertyUtil

            property_as_bool = PropertyUtil.property_as_bool

        # Load table and start transaction
        catalog = self._get_catalog()
        table = catalog.load_table(self.table_identifier)
        self._txn = table.transaction()
        self._io = self._txn._table.io
        self._table_metadata = self._txn.table_metadata
        self._uuid = uuid.uuid4()

        logger.info(
            f"Starting Iceberg write transaction for table {self.table_identifier} "
            f"in {self._mode} mode"
        )

        # Validate partition transforms are supported
        # PyIceberg requires partition transforms to support PyArrow for writing
        # See: https://py.iceberg.apache.org/api/#partition-transforms
        # Use getattr() to safely check for supports_pyarrow_transform attribute
        # Some transform types may not have this attribute (e.g., IdentityTransform)
        unsupported_partitions = []
        for field in self._table_metadata.spec().fields:
            transform = field.transform
            # Check if transform supports PyArrow
            # Default to True if attribute doesn't exist (assume supported)
            supports_pyarrow = getattr(transform, "supports_pyarrow_transform", True)
            if not supports_pyarrow:
                unsupported_partitions.append(field)

        if unsupported_partitions:
            raise ValueError(
                f"Not all partition types are supported for writes. The following "
                f"partitions cannot be written using PyArrow: {unsupported_partitions}. "
                f"Supported transforms include: identity, bucket, truncate, year, month, "
                f"day, hour. Custom transforms may not be supported."
            )

        # Check if manifest merging is enabled for this table
        # Manifest merging combines small manifest files to reduce metadata overhead
        # See: https://iceberg.apache.org/docs/latest/configuration/#table-properties
        self._manifest_merge_enabled = property_as_bool(
            self._table_metadata.properties,
            TableProperties.MANIFEST_MERGE_ENABLED,
            TableProperties.MANIFEST_MERGE_ENABLED_DEFAULT,
        )

    def write(self, blocks: Iterable[Block], ctx: TaskContext) -> List["DataFile"]:
        """
        Write Ray Data blocks to Parquet files and return DataFile metadata.

        This method is called multiple times in parallel across Ray tasks for
        distributed writing. Each invocation:
        1. Converts Ray Data blocks to PyArrow tables
        2. Validates schema compatibility with the Iceberg table
        3. Writes actual Parquet files to storage using PyIceberg's internal API
        4. Returns DataFile metadata for transaction commit

        Args:
            blocks: Iterable of Ray Data blocks to write (streamed in)
            ctx: Task execution context from Ray Data

        Returns:
            List of DataFile metadata objects describing the written Parquet files

        Note:
            This enables true distributed, streaming writes:
            - Each Ray task writes its own Parquet files in parallel
            - Files are written as blocks arrive (streaming)
            - Only metadata is returned (not the data itself)
            - All files are committed in a single transaction in on_write_complete()
        """
        from pyiceberg.io.pyarrow import (
            _check_pyarrow_schema_compatible,
            _dataframe_to_data_files,
        )
        from pyiceberg.table import DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE
        from pyiceberg.utils.config import Config

        data_files_list: List["DataFile"] = []

        for block in blocks:
            # Convert Ray Data block to PyArrow table
            pa_table = BlockAccessor.for_block(block).to_arrow()

            # Check if timestamp downcasting is enabled
            # PyIceberg may need to downcast nanosecond timestamps to microseconds
            # for compatibility with older Parquet readers
            downcast_ns_timestamp_to_us = (
                Config().get_bool(DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE) or False
            )

            # Validate that the PyArrow schema matches the Iceberg table schema
            # This ensures type compatibility before writing
            _check_pyarrow_schema_compatible(
                self._table_metadata.schema(),
                provided_schema=pa_table.schema,
                downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us,
            )

            # Skip empty blocks to avoid creating zero-row files
            if pa_table.shape[0] <= 0:
                continue

            # Generate unique UUID for this write task
            # Used for tracking data files in the Iceberg manifest
            task_uuid = uuid.uuid4()

            # Write the data to Parquet files and get data file metadata
            # This handles partitioning, sorting, and other write-time transforms
            data_files = _dataframe_to_data_files(
                self._table_metadata, pa_table, self._io, task_uuid
            )
            data_files_list.extend(data_files)

        return data_files_list

    def on_write_complete(self, write_result: WriteResult[List["DataFile"]]):
        """
        Commit all written data files to the Iceberg table in a single ACID transaction.

        This method is called after all distributed write tasks complete. It:
        1. Collects DataFile metadata from all write tasks
        2. Uses the transaction started in on_write_start() to commit files
        3. Creates exactly ONE transaction log entry (ACID guarantee)

        The commit behavior depends on the mode:
        - append: Uses fast_append or merge_append based on table properties
        - overwrite: Deletes matching files, then adds new data files

        Args:
            write_result: Collection of DataFile metadata from all write tasks

        Returns:
            None. The transaction is committed atomically.

        Note:
            The transaction was already started in on_write_start(). This ensures
            that the transaction captures the table state at the start of the write,
            providing optimistic concurrency control.

            See: https://py.iceberg.apache.org/api/#table-transactions
        """
        # Collect all DataFiles from all write tasks
        all_data_files = []
        for task_data_files in write_result.write_returns:
            all_data_files.extend(task_data_files)

        if not all_data_files:
            logger.warning(
                f"No data files written for table {self.table_identifier}. "
                "All write tasks returned empty results. Skipping commit."
            )
            # Don't commit transaction if no data was written
            # The transaction will be garbage collected without creating a snapshot
            return

        total_files = len(all_data_files)
        logger.info(
            f"Committing {total_files} data files to Iceberg table "
            f"{self.table_identifier} in {self._mode} mode"
        )

        # Create snapshot update with custom properties
        update_snapshot = self._txn.update_snapshot(
            snapshot_properties=self._snapshot_properties
        )

        if self._mode == "append":
            # Append mode: Add data files without removing existing files
            # Use manifest merge or fast append based on table configuration
            # fast_append: Faster but creates more manifest files
            # merge_append: Slower but maintains fewer manifest files
            append_method = (
                update_snapshot.merge_append
                if self._manifest_merge_enabled
                else update_snapshot.fast_append
            )

            with append_method() as append_files:
                append_files.commit_uuid = self._uuid
                for data_file in all_data_files:
                    append_files.append_data_file(data_file)

            logger.info(
                f"Successfully appended {total_files} data files to table "
                f"{self.table_identifier}"
            )

        elif self._mode == "overwrite":
            # Overwrite mode: Delete matching files, then add new files
            # Supports optional filter for dynamic partition overwrites

            # Check if overwrite_files API is available
            # This method was added in PyIceberg 0.10.0
            if hasattr(update_snapshot, "overwrite_files"):
                # Use overwrite_files API (PyIceberg 0.10.0+)
                with update_snapshot.overwrite_files() as overwrite_files:
                    overwrite_files.commit_uuid = self._uuid

                    # Delete existing files based on filter
                    if self._overwrite_filter is not None:
                        # Dynamic partition overwrite: only delete files matching filter
                        overwrite_files.delete_by_predicate(self._overwrite_filter)
                        logger.info(
                            f"Overwriting data in table {self.table_identifier} "
                            f"matching filter: {self._overwrite_filter}"
                        )
                    else:
                        # Full table overwrite: delete all files
                        from pyiceberg.expressions import AlwaysTrue

                        overwrite_files.delete_by_predicate(AlwaysTrue())
                        logger.info(f"Overwriting entire table {self.table_identifier}")

                    # Add all new data files
                    for data_file in all_data_files:
                        overwrite_files.append_data_file(data_file)
            else:
                # Fallback for older PyIceberg versions (< 0.10.0)
                # Use delete + append approach
                import pyiceberg

                logger.warning(
                    f"PyIceberg version {pyiceberg.__version__} detected. "
                    "Using fallback overwrite implementation. "
                    "For optimal performance, upgrade to PyIceberg >= 0.10.0"
                )

                # First, delete existing data
                with update_snapshot.delete() as delete_snapshot:
                    delete_snapshot.commit_uuid = self._uuid

                    if self._overwrite_filter is not None:
                        delete_snapshot.delete_by_predicate(self._overwrite_filter)
                        logger.info(
                            f"Deleting data in table {self.table_identifier} "
                            f"matching filter: {self._overwrite_filter}"
                        )
                    else:
                        from pyiceberg.expressions import AlwaysTrue

                        delete_snapshot.delete_by_predicate(AlwaysTrue())
                        logger.info(
                            f"Deleting all data from table {self.table_identifier}"
                        )

                # Then append new data
                append_method = (
                    update_snapshot.merge_append
                    if self._manifest_merge_enabled
                    else update_snapshot.fast_append
                )

                with append_method() as append_files:
                    append_files.commit_uuid = self._uuid
                    for data_file in all_data_files:
                        append_files.append_data_file(data_file)

            logger.info(
                f"Successfully overwrote table {self.table_identifier} with "
                f"{total_files} data files"
            )

        # Commit the transaction atomically
        # This creates exactly one snapshot/transaction log entry
        try:
            self._txn.commit_transaction()
            logger.info(
                f"Iceberg transaction committed for table {self.table_identifier}"
            )
        except Exception as e:
            logger.error(
                f"Failed to commit transaction for table {self.table_identifier}: {e}"
            )
            raise

    def on_write_failed(self, error: Exception) -> None:
        """
        Handle write failures by attempting to rollback the transaction.

        This method is called when a write operation fails. It attempts to clean up
        any partial state by rolling back the transaction if one exists.

        Args:
            error: The exception that caused the write to fail
        """
        logger.error(
            f"Write operation failed for table {self.table_identifier}: {error}"
        )

        # Attempt to rollback the transaction if it exists
        if self._txn is not None:
            try:
                # PyIceberg transactions don't have explicit rollback,
                # but not committing leaves them uncommitted
                # The transaction will be garbage collected
                logger.info(
                    f"Transaction for table {self.table_identifier} will not be committed. "
                    "Partial writes will be rolled back automatically."
                )
            except Exception as rollback_error:
                logger.warning(
                    f"Error during transaction cleanup for table "
                    f"{self.table_identifier}: {rollback_error}"
                )
