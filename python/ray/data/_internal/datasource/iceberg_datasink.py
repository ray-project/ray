"""
Module to write a Ray Dataset into an iceberg table, by using the Ray Datasink API.
"""
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Union

from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.savemode import SaveMode
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink, WriteResult
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.catalog import Catalog
    from pyiceberg.manifest import DataFile
    from pyiceberg.schema import Schema
    from pyiceberg.table import Table
    from pyiceberg.table.update.schema import UpdateSchema

logger = logging.getLogger(__name__)


@dataclass
class IcebergWriteResult:
    """Result from writing blocks to Iceberg storage.

    Attributes:
        data_files: List of DataFile objects containing metadata about written Parquet files.
        upsert_keys: Dictionary mapping column names to lists of key values for upsert operations.
        schemas: List of PyArrow schemas from all non-empty blocks.
    """

    data_files: List["DataFile"] = field(default_factory=list)
    upsert_keys: Optional[Dict[str, List[Any]]] = None
    schemas: List["pa.Schema"] = field(default_factory=list)


_UPSERT_COLS_ID = "join_cols"


@DeveloperAPI
class IcebergDatasink(Datasink[IcebergWriteResult]):
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
            upsert_kwargs: Optional arguments for upsert operations.
                Supported parameters: join_cols (List[str]), case_sensitive (bool),
                branch (str). Note: This implementation uses a copy-on-write strategy
                that always updates all columns for matched keys and inserts all new keys.
            overwrite_kwargs: Optional arguments to pass through to PyIceberg's table.overwrite()
                method. Supported parameters include case_sensitive (bool) and branch (str).
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
        self._upsert_kwargs = (upsert_kwargs or {}).copy()
        self._overwrite_kwargs = (overwrite_kwargs or {}).copy()

        # Validate kwargs are only set for relevant modes
        if self._upsert_kwargs and self._mode != SaveMode.UPSERT:
            raise ValueError(
                f"upsert_kwargs can only be specified when mode is SaveMode.UPSERT, but mode is {self._mode}"
            )
        if self._overwrite_kwargs and self._mode != SaveMode.OVERWRITE:
            raise ValueError(
                f"overwrite_kwargs can only be specified when mode is SaveMode.OVERWRITE, but mode is {self._mode}"
            )
        if self._overwrite_filter and self._mode != SaveMode.OVERWRITE:
            raise ValueError(
                f"overwrite_filter can only be specified when mode is SaveMode.OVERWRITE, but mode is {self._mode}"
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

    def _get_upsert_cols(self) -> List[str]:
        """Get join columns for upsert, using table identifier fields as fallback."""
        upsert_cols = self._upsert_kwargs.get(_UPSERT_COLS_ID, [])
        if not upsert_cols:
            # Use table's identifier fields as fallback
            for field_id in self._table.metadata.schema().identifier_field_ids:
                col_name = self._table.metadata.schema().find_column_name(field_id)
                if col_name:
                    upsert_cols.append(col_name)
        return upsert_cols

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

    def _commit_upsert(
        self,
        txn: "Table.transaction",
        data_files: List["DataFile"],
        upsert_keys_dicts: List[Dict[str, List[Any]]],
    ) -> None:
        """
        Commit upsert transaction with copy-on-write strategy.

        Args:
            txn: PyIceberg transaction object
            data_files: List of DataFile objects to commit
            upsert_keys_dicts: List of dictionaries mapping column names to lists of key values
        """
        import functools
        from collections import defaultdict

        import pyarrow as pa
        from pyiceberg.table.upsert_util import create_match_filter

        # Merge all join keys dictionaries
        merged_keys_dict = defaultdict(list)
        for upsert_keys_dict in upsert_keys_dicts:
            for col, values in upsert_keys_dict.items():
                merged_keys_dict[col].extend(values)

        # Create delete filter if we have join keys
        if merged_keys_dict:
            # Create PyArrow table from join keys
            keys_table = pa.table(dict(merged_keys_dict))

            # Filter out rows with any NULL values in join columns
            # (NULL != NULL in SQL semantics)
            upsert_cols = self._get_upsert_cols()
            masks = (pa.compute.is_valid(keys_table[col]) for col in upsert_cols)
            mask = functools.reduce(pa.compute.and_, masks)
            keys_table = keys_table.filter(mask)

            # Only delete if we have non-NULL keys
            if len(keys_table) > 0:
                # Use PyIceberg's helper to build delete filter
                delete_filter = create_match_filter(keys_table, upsert_cols)

                # Prepare kwargs for delete
                delete_kwargs = self._upsert_kwargs.copy()
                delete_kwargs.pop(_UPSERT_COLS_ID, None)

                txn.delete(
                    delete_filter=delete_filter,
                    snapshot_properties=self._snapshot_properties,
                    **delete_kwargs,
                )

        # Append new data files (includes updates and inserts) and commit
        self._append_and_commit(txn, data_files)

    def _preserve_identifier_field_requirements(
        self, update: "UpdateSchema", table_schema: "Schema"
    ) -> None:
        """Ensure identifier fields remain required after schema union.

        When union_by_name is called with a schema that has nullable fields,
        PyIceberg may make identifier fields optional. Since identifier fields
        must be required, this helper ensures they remain required after union.

        Example:
            Table schema:   id: int (required, identifier), val: string
            Input schema:   id: int (optional),             val: string

            `union_by_name` merges them to:
                            id: int (optional),             val: string

            This violates the identifier constraint. This function forces `id`
            back to required in the pending update.

        Args:
            update: The UpdateSchema object from update_schema() context manager
            table_schema: The current table schema to get identifier field IDs from
        """
        from pyiceberg.types import NestedField

        identifier_field_ids = table_schema.identifier_field_ids
        for field_id in identifier_field_ids:
            # Check if this field has a pending update
            if field_id in update._updates:
                updated_field = update._updates[field_id]
                # If it was made optional (likely by union_by_name), force it back to required
                if not updated_field.required:
                    # Directly update the pending change to enforce required=True.
                    # We create a new NestedField because it might be immutable.
                    # We bypass _set_column_requirement because it has a check that
                    # incorrectly returns early if the original field is already required,
                    # ignoring the fact that we are overwriting a pending update.
                    update._updates[field_id] = NestedField(
                        field_id=updated_field.field_id,
                        name=updated_field.name,
                        field_type=updated_field.field_type,
                        doc=updated_field.doc,
                        required=True,
                        initial_default=updated_field.initial_default,
                        write_default=updated_field.write_default,
                    )

    def _update_schema_with_union(
        self,
        update: "UpdateSchema",
        new_schema: Union["pa.Schema", "Schema"],
        table_schema: "Schema",
    ) -> None:
        """Update schema using union_by_name while preserving identifier field requirements.

        Args:
            update: The UpdateSchema object.
            new_schema: The new schema to union with the table schema.
            table_schema: The current table schema.
        """
        update.union_by_name(new_schema)
        self._preserve_identifier_field_requirements(update, table_schema)

    def on_write_start(self, schema: Optional["pa.Schema"] = None) -> None:
        """Initialize table for writing and create a shared write UUID.

        Args:
            schema: The PyArrow schema of the data being written. This is
                automatically extracted from the first input bundle by the
                Write operator. Used to evolve the table schema before writing
                to avoid PyIceberg name mapping errors.
        """
        self._reload_table()

        # Evolve schema BEFORE any files are written
        # This prevents PyIceberg name mapping errors when incoming data has new columns
        if schema is not None:
            table_schema = self._table.metadata.schema()
            with self._table.update_schema() as update:
                self._update_schema_with_union(update, schema, table_schema)
            # Succeeded, reload to get latest table version and exit.
            self._reload_table()

        # Validate join_cols for UPSERT mode before writing any files
        if self._mode == SaveMode.UPSERT:
            upsert_cols = self._upsert_kwargs.get(_UPSERT_COLS_ID, [])
            if not upsert_cols:
                # Check if table has identifier fields as fallback
                identifier_field_ids = (
                    self._table.metadata.schema().identifier_field_ids
                )
                if not identifier_field_ids:
                    raise ValueError(
                        "join_cols must be specified in upsert_kwargs for UPSERT mode "
                        "when table has no identifier fields"
                    )

    def write(self, blocks: Iterable[Block], ctx: TaskContext) -> IcebergWriteResult:
        """
        Write blocks to Parquet files in storage and return DataFile metadata with schemas.

        This runs on each worker in parallel. Files are written directly to storage
        (S3, HDFS, etc.) and only metadata is returned to the driver.
        Schema updates are NOT performed here - they happen on the driver.

        Args:
            blocks: Iterable of Ray Data blocks to write
            ctx: TaskContext object containing task-specific information

        Returns:
            IcebergWriteResult containing DataFile objects, upsert keys, and schemas.
        """
        from collections import defaultdict

        from pyiceberg.io.pyarrow import _dataframe_to_data_files

        # Workers receive a pickled datasink with _table=None (excluded during
        # serialization), so we reload it on first use.
        if self._table is None:
            self._reload_table()

        all_data_files = []
        upsert_keys_dict = defaultdict(list)
        block_schemas = []
        use_copy_on_write_upsert = self._mode == SaveMode.UPSERT

        for block in blocks:
            pa_table = BlockAccessor.for_block(block).to_arrow()
            if pa_table.num_rows > 0:
                block_schemas.append(pa_table.schema)

                # Extract join key values for copy-on-write upsert
                if use_copy_on_write_upsert:
                    upsert_cols = self._get_upsert_cols()
                    for col in upsert_cols:
                        upsert_keys_dict[col].extend(pa_table[col].to_pylist())

                # Write data files to storage
                data_files = list(
                    _dataframe_to_data_files(
                        table_metadata=self._table.metadata,
                        df=pa_table,
                        io=self._table.io,
                    )
                )
                all_data_files.extend(data_files)

        return IcebergWriteResult(
            data_files=all_data_files,
            upsert_keys=upsert_keys_dict or None,
            schemas=block_schemas,
        )

    def _commit_overwrite(
        self, txn: "Table.transaction", data_files: List["DataFile"]
    ) -> None:
        """Commit data files using OVERWRITE mode."""
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

    def on_write_complete(self, write_result: WriteResult) -> None:
        """
        Complete the write by reconciling schemas and committing all data files.

        This runs on the driver after all workers finish writing files.
        Collects all DataFile objects and schemas from all workers, reconciles schemas
        (allowing type promotion), updates table schema if needed, then performs a single
        atomic commit.
        """
        # Collect all data files and schemas from all workers
        all_data_files: List["DataFile"] = []
        all_schemas: List["pa.Schema"] = []
        upsert_keys_dicts: List[Dict[str, List[Any]]] = []

        for write_return in write_result.write_returns:
            if not write_return:
                continue

            if write_return.data_files:  # Only add schema if we have data files
                all_data_files.extend(write_return.data_files)
                all_schemas.extend(write_return.schemas)
                if write_return.upsert_keys:
                    upsert_keys_dicts.append(write_return.upsert_keys)

        if not all_data_files:
            return

        # Reconcile all schemas from all blocks across all workers
        # Get table schema and union with reconciled schema using unify_schemas with promotion
        from pyiceberg.io import pyarrow as pyi_pa_io

        from ray.data._internal.arrow_ops.transform_pyarrow import unify_schemas

        table_schema = pyi_pa_io.schema_to_pyarrow(self._table.schema())
        final_reconciled_schema = unify_schemas(
            [table_schema] + all_schemas, promote_types=True
        )

        # Create transaction and commit schema update + data files atomically
        txn = self._table.transaction()

        # Update table schema within the transaction if it differs
        if not final_reconciled_schema.equals(table_schema):
            current_table_schema = self._table.metadata.schema()
            with txn.update_schema() as update:
                self._update_schema_with_union(
                    update, final_reconciled_schema, current_table_schema
                )

        # Create transaction and commit based on mode
        if self._mode == SaveMode.APPEND:
            self._append_and_commit(txn, all_data_files)
        elif self._mode == SaveMode.OVERWRITE:
            self._commit_overwrite(txn, all_data_files)
        elif self._mode == SaveMode.UPSERT:
            self._commit_upsert(txn, all_data_files, upsert_keys_dicts)
        else:
            raise ValueError(f"Unsupported mode: {self._mode}")
