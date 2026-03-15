"""
Module to write a Ray Dataset into an iceberg table, by using the Ray Datasink API.
"""
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional, Union

from ray._common.retry import call_with_retry
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.savemode import SaveMode
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.datasink import Datasink, WriteResult
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow as pa
    from pyiceberg.catalog import Catalog
    from pyiceberg.io import FileIO
    from pyiceberg.manifest import DataFile
    from pyiceberg.schema import Schema
    from pyiceberg.table import Table
    from pyiceberg.table.metadata import TableMetadata
    from pyiceberg.table.update.schema import UpdateSchema

logger = logging.getLogger(__name__)


@dataclass
class IcebergWriteResult:
    """Result from writing blocks to Iceberg storage.

    Attributes:
        data_files: List of DataFile objects containing metadata about written Parquet files.
        upsert_keys: PyArrow table containing key columns for upsert operations.
        schemas: List of PyArrow schemas from all non-empty blocks.
    """

    data_files: List["DataFile"] = field(default_factory=list)
    upsert_keys: Optional["pa.Table"] = None
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
        self._io: "FileIO" = None
        self._table_metadata: "TableMetadata" = None
        self._data_context = DataContext.get_current()

    def __getstate__(self) -> dict:
        """Exclude `_table` during pickling."""
        state = self.__dict__.copy()
        state.pop("_table", None)
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._table = None

    def _with_retry(self, func: Callable, description: str) -> Any:
        """Execute a function with retry logic.

        This helper encapsulates the common retry pattern for Iceberg catalog
        operations, using the configured retry parameters from DataContext.

        Args:
            func: The callable to execute with retry logic.
            description: Human-readable description for logging/error messages.

        Returns:
            The result of calling func.
        """
        iceberg_config = self._data_context.iceberg_config
        return call_with_retry(
            func,
            description=description,
            match=iceberg_config.catalog_retried_errors,
            max_attempts=iceberg_config.catalog_max_attempts,
            max_backoff_s=iceberg_config.catalog_retry_max_backoff_s,
        )

    def _get_catalog(self) -> "Catalog":
        from pyiceberg import catalog

        return self._with_retry(
            lambda: catalog.load_catalog(self._catalog_name, **self._catalog_kwargs),
            description=f"load Iceberg catalog '{self._catalog_name}'",
        )

    def _reload_table(self) -> None:
        """Reload the Iceberg table from the catalog."""
        cat = self._get_catalog()
        self._table = self._with_retry(
            lambda: cat.load_table(self.table_identifier),
            description=f"load Iceberg table '{self.table_identifier}'",
        )
        self._io = self._table.io
        self._table_metadata = self._table.metadata

    def _get_upsert_cols(self) -> List[str]:
        """Get join columns for upsert, using table identifier fields as fallback."""
        upsert_cols = self._upsert_kwargs.get(_UPSERT_COLS_ID, [])
        if not upsert_cols:
            # Use table's identifier fields as fallback
            schema = self._table_metadata.schema()
            for field_id in schema.identifier_field_ids:
                col_name = schema.find_column_name(field_id)
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

        self._with_retry(
            txn.commit_transaction,
            description=f"commit transaction to Iceberg table '{self.table_identifier}'",
        )

    def _commit_upsert(
        self,
        txn: "Table.transaction",
        data_files: List["DataFile"],
        upsert_keys: Optional["pa.Table"],
    ) -> None:
        """
        Commit upsert transaction with copy-on-write strategy.

        Args:
            txn: PyIceberg transaction object
            data_files: List of DataFile objects to commit
            upsert_keys: PyArrow table containing upsert key columns
        """
        import functools
        import time

        import pyarrow as pa
        from pyiceberg.table.upsert_util import create_match_filter

        # Create delete filter if we have join keys
        if upsert_keys is not None and len(upsert_keys) > 0:
            # Filter out rows with any NULL values in join columns
            # (NULL != NULL in SQL semantics)
            upsert_cols = self._get_upsert_cols()
            logger.info(
                "[upsert commit] Filtering NULL keys from %d rows on cols %s",
                len(upsert_keys),
                upsert_cols,
            )
            t0 = time.perf_counter()
            masks = (pa.compute.is_valid(upsert_keys[col]) for col in upsert_cols)
            mask = functools.reduce(pa.compute.and_, masks)
            keys_table = upsert_keys.filter(mask)
            logger.info(
                "[upsert commit] NULL filter done in %.2fs: %d -> %d rows (dropped %d NULLs)",
                time.perf_counter() - t0,
                len(upsert_keys),
                len(keys_table),
                len(upsert_keys) - len(keys_table),
            )

            # Only delete if we have non-NULL keys
            if len(keys_table) > 0:
                logger.info(
                    "[upsert commit] Building delete filter from %d keys (cols: %s) ...",
                    len(keys_table),
                    upsert_cols,
                )
                t0 = time.perf_counter()
                # Use PyIceberg's helper to build delete filter
                delete_filter = create_match_filter(keys_table, upsert_cols)
                logger.info(
                    "[upsert commit] create_match_filter done in %.2fs: filter type=%s",
                    time.perf_counter() - t0,
                    type(delete_filter).__name__,
                )

                # Prepare kwargs for delete
                delete_kwargs = self._upsert_kwargs.copy()
                delete_kwargs.pop(_UPSERT_COLS_ID, None)

                logger.info("[upsert commit] Executing txn.delete() ...")
                t0 = time.perf_counter()
                txn.delete(
                    delete_filter=delete_filter,
                    snapshot_properties=self._snapshot_properties,
                    **delete_kwargs,
                )
                logger.info(
                    "[upsert commit] txn.delete() done in %.2fs",
                    time.perf_counter() - t0,
                )
        else:
            logger.info("[upsert commit] No upsert keys — skipping delete phase")

        # Append new data files (includes updates and inserts) and commit
        logger.info(
            "[upsert commit] Appending %d data files and committing ...",
            len(data_files),
        )
        t0 = time.perf_counter()
        self._append_and_commit(txn, data_files)
        logger.info(
            "[upsert commit] Append+commit done in %.2fs",
            time.perf_counter() - t0,
        )

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

            def _update_schema():
                with self._table.update_schema() as update:
                    self._update_schema_with_union(update, schema, table_schema)

            self._with_retry(
                _update_schema,
                description=f"update schema for Iceberg table '{self.table_identifier}'",
            )
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
        from pyiceberg.io.pyarrow import _dataframe_to_data_files

        all_data_files = []
        upsert_keys_tables = []
        block_schemas = []
        use_copy_on_write_upsert = self._mode == SaveMode.UPSERT

        for block in blocks:
            pa_table = BlockAccessor.for_block(block).to_arrow()
            if pa_table.num_rows > 0:
                block_schemas.append(pa_table.schema)

                # Extract join key values for copy-on-write upsert
                if use_copy_on_write_upsert:
                    upsert_cols = self._get_upsert_cols()
                    if len(upsert_cols) > 0:
                        upsert_keys_tables.append(pa_table.select(upsert_cols))

                # Write data files to storage with retry for transient errors
                def _write_data_files():
                    return list(
                        _dataframe_to_data_files(
                            table_metadata=self._table_metadata,
                            df=pa_table,
                            io=self._io,
                        )
                    )

                iceberg_config = self._data_context.iceberg_config
                data_files = call_with_retry(
                    _write_data_files,
                    description=f"write data files to Iceberg table '{self.table_identifier}'",
                    match=self._data_context.retried_io_errors,
                    max_attempts=iceberg_config.write_file_max_attempts,
                    max_backoff_s=iceberg_config.write_file_retry_max_backoff_s,
                )
                all_data_files.extend(data_files)

        # Combine all upsert key tables into one
        from ray.data._internal.arrow_ops.transform_pyarrow import concat

        upsert_keys = concat(upsert_keys_tables) if upsert_keys_tables else None

        result = IcebergWriteResult(
            data_files=all_data_files,
            upsert_keys=upsert_keys,
            schemas=block_schemas,
        )

        return result

    def _commit_overwrite(
        self, txn: "Table.transaction", data_files: List["DataFile"]
    ) -> None:
        """Commit data files using OVERWRITE mode."""
        from pyiceberg.expressions import AlwaysTrue

        # Default - Full overwrite - delete all
        pyi_filter = AlwaysTrue()

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

        # Append new data files and commit
        self._append_and_commit(txn, data_files)

    def on_write_complete(self, write_result: WriteResult) -> None:
        """
        Commit the write transaction to Iceberg.
        This method is called on the driver after all write tasks have completed.
        """
        import time

        t_start = time.perf_counter()
        logger.info("[on_write_complete] Starting commit phase (mode=%s)", self._mode)

        write_returns = write_result.write_returns

        context = DataContext.get_current()
        if context.checkpoint_config:
            from ray.data.checkpoint.checkpoint_filter import IcebergCheckpointLoader

            try:
                existing_data_file_paths = set()
                for r in write_returns:
                    if not r or not r.data_files:
                        continue
                    for df in r.data_files:
                        file_path = getattr(df, "file_path", None)
                        if file_path is not None:
                            existing_data_file_paths.add(file_path)

                loader = IcebergCheckpointLoader(context.checkpoint_config)
                previous_results = loader.load_write_results()
                deduped_previous_results = []
                num_skipped_results = 0
                for r in previous_results:
                    if not r:
                        continue
                    if r.data_files:
                        all_duplicate = True
                        for df in r.data_files:
                            file_path = getattr(df, "file_path", None)
                            if (
                                file_path is None
                                or file_path not in existing_data_file_paths
                            ):
                                all_duplicate = False
                                break
                        if all_duplicate:
                            num_skipped_results += 1
                            continue
                        for df in r.data_files:
                            file_path = getattr(df, "file_path", None)
                            if file_path is not None:
                                existing_data_file_paths.add(file_path)
                    deduped_previous_results.append(r)

                write_returns.extend(deduped_previous_results)
                logger.info(
                    "[on_write_complete] Loaded %d results from checkpoint (skipped_duplicates=%d, merged_total=%d)",
                    len(deduped_previous_results),
                    num_skipped_results,
                    len(write_returns),
                )
            except Exception as e:
                logger.warning(
                    "[on_write_complete] Failed to load checkpoint results: %s", e
                )

        valid_results = [
            r
            for r in write_returns
            if r and (r.data_files or (r.upsert_keys is not None))
        ]

        if not valid_results:
            logger.info("[on_write_complete] No data to commit to Iceberg table.")
            return

        # Collect all data files and schemas from all workers
        all_data_files: List["DataFile"] = []
        all_schemas: List["pa.Schema"] = []
        upsert_keys_tables: List["pa.Table"] = []
        seen_data_file_paths = set()

        for result in valid_results:
            if result.data_files:
                for df in result.data_files:
                    file_path = getattr(df, "file_path", None)
                    if file_path is not None:
                        if file_path in seen_data_file_paths:
                            continue
                        seen_data_file_paths.add(file_path)
                    all_data_files.append(df)
                all_schemas.extend(result.schemas)
            if result.upsert_keys is not None:
                upsert_keys_tables.append(result.upsert_keys)

        logger.info(
            "[on_write_complete] Collected results: %d data files, %d schema blocks, "
            "%d upsert key batches from workers (%.2fs)",
            len(all_data_files),
            len(all_schemas),
            len(upsert_keys_tables),
            time.perf_counter() - t_start,
        )

        if not all_data_files:
            logger.info("[on_write_complete] No data files written, nothing to commit")
            return

        # Concatenate all upsert keys from all workers into a single table
        from ray.data._internal.arrow_ops.transform_pyarrow import concat

        if upsert_keys_tables:
            total_key_rows = sum(len(t) for t in upsert_keys_tables)
            logger.info(
                "[on_write_complete] Concatenating %d upsert key batches (%d total rows) ...",
                len(upsert_keys_tables),
                total_key_rows,
            )
            t0 = time.perf_counter()
            upsert_keys = concat(upsert_keys_tables)
            logger.info(
                "[on_write_complete] upsert key concat done in %.2fs: %d rows, cols=%s",
                time.perf_counter() - t0,
                len(upsert_keys),
                upsert_keys.column_names,
            )
        else:
            upsert_keys = None

        # Reconcile all schemas from all blocks across all workers
        # Get table schema and union with reconciled schema using unify_schemas with promotion
        from pyiceberg.io import pyarrow as pyi_pa_io

        from ray.data._internal.arrow_ops.transform_pyarrow import unify_schemas

        logger.info("[on_write_complete] Reconciling %d schemas ...", len(all_schemas))
        t0 = time.perf_counter()
        table_schema = pyi_pa_io.schema_to_pyarrow(self._table.schema())
        final_reconciled_schema = unify_schemas(
            [table_schema] + all_schemas, promote_types=True
        )
        logger.info(
            "[on_write_complete] Schema reconciliation done in %.2fs",
            time.perf_counter() - t0,
        )

        # Create transaction and commit schema update + data files atomically
        txn = self._table.transaction()

        # Update table schema within the transaction if it differs
        if not final_reconciled_schema.equals(table_schema):
            logger.info(
                "[on_write_complete] Schema changed — updating table schema ..."
            )
            t0 = time.perf_counter()
            current_table_schema = self._table.metadata.schema()
            with txn.update_schema() as update:
                self._update_schema_with_union(
                    update, final_reconciled_schema, current_table_schema
                )
            logger.info(
                "[on_write_complete] Schema update done in %.2fs",
                time.perf_counter() - t0,
            )
        else:
            logger.info("[on_write_complete] Schema unchanged, skipping update")

        # Create transaction and commit based on mode
        logger.info(
            "[on_write_complete] Starting %s commit for %d data files ...",
            self._mode,
            len(all_data_files),
        )
        t0 = time.perf_counter()
        if self._mode == SaveMode.APPEND:
            self._append_and_commit(txn, all_data_files)
        elif self._mode == SaveMode.OVERWRITE:
            self._commit_overwrite(txn, all_data_files)
        elif self._mode == SaveMode.UPSERT:
            self._commit_upsert(txn, all_data_files, upsert_keys)
        else:
            raise ValueError(f"Unsupported mode: {self._mode}")
        logger.info(
            "[on_write_complete] Commit complete in %.2fs (total on_write_complete=%.2fs)",
            time.perf_counter() - t0,
            time.perf_counter() - t_start,
        )
