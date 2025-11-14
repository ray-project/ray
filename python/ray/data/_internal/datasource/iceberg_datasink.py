"""
Module to write a Ray Dataset into an iceberg table, by using the Ray Datasink API.
"""
import logging
import uuid
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

import ray
from ray._private.ray_constants import env_integer
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.savemode import SaveMode
from ray.data._internal.util import GiB
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

# Default memory allocation for Iceberg upsert commit task
DEFAULT_ICEBERG_UPSERT_COMMIT_TASK_MEMORY = env_integer(
    "RAY_DATA_ICEBERG_UPSERT_COMMIT_TASK_MEMORY", 4 * GiB
)


@DeveloperAPI
class IcebergDatasink(Datasink[List["DataFile"]]):
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
            upsert_kwargs: Optional arguments to pass through to PyIceberg's table.upsert()
                method. Supported parameters include join_cols (List[str]),
                when_matched_update_all (bool), when_not_matched_insert_all (bool),
                case_sensitive (bool), branch (str). See PyIceberg documentation for details.
            overwrite_kwargs: Optional arguments to pass through to PyIceberg's table.overwrite()
                method. Supported parameters include case_sensitive (bool) and branch (str).
                See PyIceberg documentation for details.

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

        # Validate kwargs are only set for relevant modes
        if self._upsert_kwargs and self._mode != SaveMode.UPSERT:
            raise ValueError(
                f"upsert_kwargs can only be specified when mode is SaveMode.UPSERT, "
                f"but mode is {self._mode}"
            )
        if self._overwrite_kwargs and self._mode != SaveMode.OVERWRITE:
            raise ValueError(
                f"overwrite_kwargs can only be specified when mode is SaveMode.OVERWRITE, "
                f"but mode is {self._mode}"
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

    def _update_schema(self, incoming_schema: "pa.Schema") -> None:
        """
        Update the table schema to accommodate incoming data using union-by-name semantics.

        This automatically handles:
        - Adding new columns from the incoming schema
        - Type promotion (e.g., int32 -> int64) where compatible
        - Preserving existing columns not in the incoming schema

        Args:
            incoming_schema: The PyArrow schema from the incoming data
        """
        from pyiceberg.exceptions import CommitFailedException

        max_retries = 3

        for attempt in range(max_retries):
            try:
                with self._table.update_schema() as update:
                    update.union_by_name(incoming_schema)
            except CommitFailedException:
                if attempt < max_retries - 1:
                    logger.debug(
                        f"Schema update conflict - another worker modified schema, "
                        f"reloading and retrying (attempt {attempt + 1}/{max_retries})"
                    )
                else:
                    logger.debug(
                        "Schema update conflict - another worker modified schema, reloading"
                    )
            finally:
                self._reload_table()

    def on_write_start(self) -> None:
        """Initialize table for writing and create a shared write UUID."""
        self._table = self._get_catalog().load_table(self.table_identifier)
        self._write_uuid = uuid.uuid4()

    def write(self, blocks: Iterable[Block], ctx: TaskContext) -> List["DataFile"]:
        """
        Write blocks to Parquet files in storage and return DataFile metadata.

        This runs on each worker in parallel. Files are written directly to storage
        (S3, HDFS, etc.) and only metadata is returned to the driver.

        Args:
            blocks: Iterable of Ray Data blocks to write
            ctx: TaskContext object containing task-specific information

        Returns:
            List of DataFile objects representing the written files.
        """
        from pyiceberg.io.pyarrow import _dataframe_to_data_files

        if self._table is None:
            self._reload_table()

        all_data_files = []

        for block in blocks:
            pa_table = BlockAccessor.for_block(block).to_arrow()
            if pa_table.num_rows > 0:
                # Update schema if needed (handles concurrent schema updates)
                self._update_schema(pa_table.schema)

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

        return all_data_files

    def on_write_complete(self, write_result: WriteResult[List["DataFile"]]) -> None:
        """
        Complete the write by committing all data files in a single transaction.

        This runs on the driver after all workers finish writing files.
        Collects all DataFile objects from all workers and performs a single
        atomic commit based on the configured mode.
        """
        # Reload table to get latest metadata
        self._reload_table()

        # Collect all data files from all workers
        all_data_files = []
        for data_files_list in write_result.write_returns:
            if data_files_list:
                all_data_files.extend(data_files_list)

        if not all_data_files:
            return

        # Create transaction and commit based on mode
        if self._mode == SaveMode.APPEND:
            self._commit_append(all_data_files)
        elif self._mode == SaveMode.OVERWRITE:
            self._commit_overwrite(all_data_files)
        elif self._mode == SaveMode.UPSERT:
            self._commit_upsert(all_data_files)
        else:
            raise ValueError(f"Unsupported mode: {self._mode}")

    def _append_data_files_to_transaction(
        self, txn: "Transaction", data_files: List["DataFile"]
    ) -> None:
        """Helper to append data files to a transaction."""
        with txn._append_snapshot_producer(self._snapshot_properties) as append_files:
            for data_file in data_files:
                append_files.append_data_file(data_file)

    def _commit_append(self, data_files: List["DataFile"]) -> None:
        """Commit data files using APPEND mode."""
        txn = self._table.transaction()
        self._append_data_files_to_transaction(txn, data_files)
        txn.commit_transaction()

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

        # Append new data files
        self._append_data_files_to_transaction(txn, data_files)
        txn.commit_transaction()

    def _commit_upsert(self, data_files: List["DataFile"]) -> None:
        """
        Commit data files using UPSERT mode.

        For upsert, we need to read back the data files to perform merge logic.
        The write I/O was still distributed (workers wrote files in parallel),
        but the merge logic happens in a Ray task to protect the driver from OOM.
        """
        import ray

        # Run the upsert commit in a Ray task to protect the driver
        ray.get(
            _commit_upsert_task.remote(
                table_identifier=self.table_identifier,
                catalog_name=self._catalog_name,
                catalog_kwargs=self._catalog_kwargs,
                data_files=data_files,
                upsert_kwargs=self._upsert_kwargs,
            )
        )


@ray.remote(num_cpus=1, memory=DEFAULT_ICEBERG_UPSERT_COMMIT_TASK_MEMORY)
def _commit_upsert_task(
    table_identifier: str,
    catalog_name: str,
    catalog_kwargs: Dict[str, Any],
    data_files: List["DataFile"],
    upsert_kwargs: Dict[str, Any],
) -> None:
    """
    Remote task to commit UPSERT operations.

    This runs in a separate Ray task to protect the driver from OOM
    when reading back data files for upsert merge logic.
    """
    import pyarrow.parquet as pq
    from pyiceberg import catalog

    # Load the catalog and table
    loaded_catalog = catalog.load_catalog(catalog_name, **catalog_kwargs)
    table = loaded_catalog.load_table(table_identifier)

    # Create a single transaction
    txn = table.transaction()

    # Process each data file one at a time to avoid OOM
    for data_file in data_files:
        # Read the parquet file from storage
        file_path = data_file.file_path
        with table.io.new_input(file_path).open() as f:
            pa_table = pq.read_table(f)

        # Upsert this table
        txn.upsert(df=pa_table, **upsert_kwargs)

    # Single commit at the end - no concurrency conflicts!
    txn.commit_transaction()
