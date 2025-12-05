"""
Module to write a Ray Dataset into an iceberg table, by using the Ray Datasink API.
"""
import logging
import uuid
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink, WriteResult
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.catalog import Catalog
    from pyiceberg.manifest import DataFile
    from pyiceberg.table import Table


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
    """

    def __init__(
        self,
        table_identifier: str,
        catalog_kwargs: Optional[Dict[str, Any]] = None,
        snapshot_properties: Optional[Dict[str, str]] = None,
        incoming_schema: Optional["pa.Schema"] = None,
    ):
        """
        Initialize the IcebergDatasink

        Args:
            table_identifier: The identifier of the table such as `default.taxi_dataset`
            catalog_kwargs: Optional arguments to use when setting up the Iceberg catalog
            snapshot_properties: Custom properties to write to snapshot summary
            incoming_schema: Optional PyArrow schema of the incoming data. Used to evolve
                table schema before writing to avoid name mapping errors.

        Note:
            Schema evolution is automatically enabled. New columns in the incoming data
            are automatically added to the table schema.
        """
        self.table_identifier = table_identifier
        self._catalog_kwargs = (catalog_kwargs or {}).copy()
        self._snapshot_properties = (snapshot_properties or {}).copy()
        self._incoming_schema = incoming_schema

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

        This is called from the driver after reconciling all schemas.

        Args:
            incoming_schema: The PyArrow schema to merge with the table schema
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
                        f"Schema update conflict - reloading and retrying "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    self._reload_table()
                else:
                    logger.error(
                        "Failed to update schema after %d retries due to conflicts.",
                        max_retries,
                    )
                    raise

    def _evolve_schema_if_needed(self, incoming_schema: "pa.Schema") -> None:
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

    def on_write_start(self) -> None:
        """Initialize table for writing and create a shared write UUID."""
        self._reload_table()
        self._write_uuid = uuid.uuid4()

        # Evolve schema BEFORE any files are written
        # This prevents PyIceberg name mapping errors when incoming data has new columns
        if self._incoming_schema is not None:
            self._evolve_schema_if_needed(self._incoming_schema)

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
        all_schemas_with_table = [table_schema, reconciled_schema]
        final_reconciled_schema = unify_schemas(
            all_schemas_with_table, promote_types=True
        )

        # Update table schema if it differs
        if not final_reconciled_schema.equals(table_schema):
            self._update_schema(final_reconciled_schema)

        # Reload table to get latest metadata after schema update
        self._reload_table()

        # Create transaction and commit
        txn = self._table.transaction()
        with txn._append_snapshot_producer(self._snapshot_properties) as append_files:
            for data_file in all_data_files:
                append_files.append_data_file(data_file)
        txn.commit_transaction()
