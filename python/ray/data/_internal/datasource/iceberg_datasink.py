"""
Module to write a Ray Dataset into an iceberg table, by using the Ray Datasink API.
"""
import logging
import uuid
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.savemode import SaveMode
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink, WriteResult
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.catalog import Catalog
    from pyiceberg.manifest import DataFile
    from pyiceberg.table import Table

    from ray.data.expressions import Expr


logger = logging.getLogger(__name__)


@DeveloperAPI
class IcebergDatasink(Datasink[Tuple[List["DataFile"], "pa.Schema"]]):
    """
    Iceberg datasink to write a Ray Dataset into an existing Iceberg table.
    This datasink handles concurrent writes by:
    - Each worker writes Parquet files to storage and returns DataFile metadata
    - The driver collects all DataFile objects and performs a single commit

    Schema evolution is supported:
    - New columns in incoming data are automatically added to the table schema
    - Type promotion across blocks is handled via schema reconciliation on the driver

    Write modes:
    - APPEND: Add new data without modifying existing data (default)
    - OVERWRITE: Replace table data (all data or filtered subset)
    """

    def __init__(
        self,
        table_identifier: str,
        catalog_kwargs: Optional[Dict[str, Any]] = None,
        snapshot_properties: Optional[Dict[str, str]] = None,
        mode: SaveMode = SaveMode.APPEND,
        overwrite_filter: Optional["Expr"] = None,
        overwrite_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the IcebergDatasink

        Args:
            table_identifier: The identifier of the table such as `default.taxi_dataset`
            catalog_kwargs: Optional arguments to use when setting up the Iceberg catalog
            snapshot_properties: Custom properties to write to snapshot summary
            mode: Write mode - APPEND or OVERWRITE. Defaults to APPEND.
                - APPEND: Add new data without checking for duplicates
                - OVERWRITE: Replace table data (all data or filtered subset)
            overwrite_filter: Optional filter for OVERWRITE mode to perform partial overwrites.
                Must be a Ray Data expression from `ray.data.expressions`. Only rows matching
                this filter are replaced. If None with OVERWRITE mode, replaces all table data.
            overwrite_kwargs: Optional arguments to pass through to PyIceberg's delete method.
                Supported parameters include case_sensitive (bool) and branch (str).
                See PyIceberg documentation for details.

        Note:
            Schema evolution is automatically enabled. New columns in the incoming data
            are automatically added to the table schema. The schema is extracted from
            the first input bundle when on_write_start is called.
        """
        self.table_identifier = table_identifier
        self._catalog_kwargs = (catalog_kwargs or {}).copy()
        self._snapshot_properties = (snapshot_properties or {}).copy()
        self._mode = mode
        self._overwrite_filter = overwrite_filter
        self._overwrite_kwargs = (overwrite_kwargs or {}).copy()

        # Validate kwargs are only set for relevant modes
        if self._overwrite_kwargs and self._mode != SaveMode.OVERWRITE:
            raise ValueError(
                f"overwrite_kwargs can only be specified when mode is SaveMode.OVERWRITE, "
                f"but mode is {self._mode}"
            )
        if self._overwrite_filter and self._mode != SaveMode.OVERWRITE:
            raise ValueError(
                f"overwrite_filter can only be specified when mode is SaveMode.OVERWRITE, "
                f"but mode is {self._mode}"
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

    def _update_schema(self, incoming_schema: "pa.Schema") -> None:
        """
        Update the table schema to accommodate incoming data using union-by-name semantics.

        .. warning::
            This method must only be called from the driver process.
            It performs schema evolution which requires exclusive table access.

        Args:
            incoming_schema: The PyArrow schema to merge with the table schema
        """
        import ray
        from ray._private.worker import WORKER_MODE

        is_driver = ray.get_runtime_context().worker.mode != WORKER_MODE
        assert is_driver, "Schema update must be called from the driver process"
        with self._table.update_schema() as update:
            update.union_by_name(incoming_schema)
        # Succeeded, reload to get latest table version and exit.
        self._reload_table()

    def _try_update_schema(self, incoming_schema: "pa.Schema") -> None:
        """
        Evolve table schema if incoming data has new columns.

        This must be called before files are written to avoid PyIceberg
        name mapping errors when the incoming data has columns not in
        the table schema.

        Args:
            incoming_schema: The PyArrow schema of the incoming data
        """
        from pyiceberg.io import pyarrow as pyi_pa_io

        table_schema: "pa.Schema" = pyi_pa_io.schema_to_pyarrow(self._table.schema())
        table_field_names = set(table_schema.names)
        incoming_field_names = set(incoming_schema.names)

        # Only evolve schema if there are new columns.
        # Don't trigger evolution for type mismatches on existing columns
        # (e.g., empty datasets with inferred types like double instead of string).
        new_columns = incoming_field_names - table_field_names
        if new_columns:
            self._update_schema(incoming_schema)

    def _append_and_commit(
        self, txn: "Table.transaction", data_files: List["DataFile"]
    ) -> None:
        """Append data files to a transaction and commit.

        Args:
            txn: PyIceberg transaction object
            data_files: List of DataFile objects to append
        """
        with txn._append_snapshot_producer(self._snapshot_properties) as append_files:
            for data_file in data_files:
                append_files.append_data_file(data_file)
        txn.commit_transaction()

    def on_write_start(self, schema: Optional["pa.Schema"] = None) -> None:
        """Initialize table for writing and create a shared write UUID.

        Args:
            schema: The PyArrow schema of the data being written. This is
                automatically extracted from the first input bundle by the
                Write operator. Used to evolve the table schema before writing
                to avoid PyIceberg name mapping errors.
        """
        self._reload_table()
        self._write_uuid = uuid.uuid4()

        # Evolve schema BEFORE any files are written
        # This prevents PyIceberg name mapping errors when incoming data has new columns
        if schema is not None:
            self._try_update_schema(schema)

    def write(
        self, blocks: Iterable[Block], ctx: TaskContext
    ) -> Tuple[List["DataFile"], "pa.Schema"]:
        """
        Write blocks to Parquet files in storage and return DataFile metadata with schema.

        This runs on each worker in parallel. Files are written directly to storage
        (S3, HDFS, etc.) and only metadata is returned to the driver.
        Schema updates are NOT performed here - they happen on the driver.

        Args:
            blocks: Iterable of Ray Data blocks to write
            ctx: TaskContext object containing task-specific information

        Returns:
            Tuple of (List of DataFile objects, PyArrow schema from first non-empty block).
        """
        import pyarrow as pa
        from pyiceberg.io.pyarrow import _dataframe_to_data_files

        # Workers receive a pickled datasink with _table=None (excluded during
        # serialization), so we reload it on first use.
        if self._table is None:
            self._reload_table()

        all_data_files = []
        first_schema = None

        for block in blocks:
            pa_table = BlockAccessor.for_block(block).to_arrow()
            if pa_table.num_rows > 0:
                # Track schema for reconciliation on driver
                # We allow different schemas between blocks (nullability, missing columns, type promotion)
                # Schema reconciliation happens on driver using unify_schemas
                if first_schema is None:
                    first_schema = pa_table.schema

                # Write data files to storage
                data_files = list(
                    _dataframe_to_data_files(
                        table_metadata=self._table.metadata,
                        write_uuid=self._write_uuid,
                        df=pa_table,
                        io=self._table.io,
                    )
                )
                all_data_files.extend(data_files)

        # Return schema along with data files
        # Use first_schema (may be None if no blocks had rows)
        if first_schema is None:
            first_schema = pa.schema([])

        return (all_data_files, first_schema)

    def on_write_complete(self, write_result: WriteResult) -> None:
        """
        Complete the write by reconciling schemas and committing all data files.

        This runs on the driver after all workers finish writing files.
        Collects all DataFile objects and schemas from all workers, reconciles schemas
        (allowing type promotion), updates table schema if needed, then performs a single
        atomic commit.
        """
        # Reload table to get latest metadata
        self._reload_table()

        # Collect all data files and schemas from all workers
        all_data_files = []
        all_schemas = []

        for write_return in write_result.write_returns:
            if not write_return:
                continue

            data_files, schema = write_return
            if data_files:  # Only add schema if we have data files
                all_data_files.extend(data_files)
                all_schemas.append(schema)

        if not all_data_files:
            return

        # Reconcile all schemas from workers using unify_schemas with type promotion enabled
        # This handles nullability differences, missing columns, and type promotion
        from ray.data._internal.arrow_ops.transform_pyarrow import unify_schemas

        reconciled_schema = unify_schemas(all_schemas, promote_types=True)

        # Get table schema and union with reconciled schema using unify_schemas with promotion
        from pyiceberg.io import pyarrow as pyi_pa_io

        table_schema = pyi_pa_io.schema_to_pyarrow(self._table.schema())
        final_reconciled_schema = unify_schemas(
            [table_schema, reconciled_schema], promote_types=True
        )

        # Create transaction and commit schema update + data files atomically
        txn = self._table.transaction()

        # Update table schema within the transaction if it differs
        if not final_reconciled_schema.equals(table_schema):
            with txn.update_schema() as update:
                update.union_by_name(final_reconciled_schema)

        # Reload table to get latest metadata after schema update
        self._reload_table()

        # Create transaction and commit based on mode
        if self._mode == SaveMode.APPEND:
            self._commit_append(all_data_files)
        elif self._mode == SaveMode.OVERWRITE:
            self._commit_overwrite(all_data_files)
        else:
            raise ValueError(f"Unsupported mode: {self._mode}")

    def _commit_append(self, data_files: List["DataFile"]) -> None:
        """Commit data files using APPEND mode."""
        txn = self._table.transaction()
        self._append_and_commit(txn, data_files)

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
        self._append_and_commit(txn, data_files)
