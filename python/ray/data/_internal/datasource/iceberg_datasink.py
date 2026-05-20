"""
Module to write a Ray Dataset into an iceberg table, by using the Ray Datasink API.
"""
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional, Union

import ray
from ray._common.retry import call_with_retry
from ray.data._internal.datasource.parquet_datasource import (
    PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
)
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
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.io import FileIO
    from pyiceberg.manifest import DataFile
    from pyiceberg.schema import Schema
    from pyiceberg.table import DataScan, FileScanTask, Table
    from pyiceberg.table.metadata import TableMetadata
    from pyiceberg.table.update.schema import UpdateSchema

logger = logging.getLogger(__name__)

_REWRITE_STALL_TIMEOUT_S = 600

@ray.remote
def _rewrite_iceberg_file(
    file_scan_task: "FileScanTask",
    keys_ref: "pa.Table",
    upsert_cols: List[str],
    table_metadata: "TableMetadata",
    io: "FileIO",
) -> "tuple[Optional[DataFile], List[DataFile]]":
    """Read one Iceberg file, anti-join against upsert keys, write preserved rows.

    Preserved rows are rows in the file that are not in the upsert batch. The
    coarse range filter would delete them (see ``IcebergDatasink._build_coarse_range_filter``),
    so we preserve them by writing them as new data files before the delete.

    Returns (original DataFile to delete, list of new preserved DataFiles).
    If the entire file is matched (no preserved rows), returns (file, []).
    If the file has no matched rows at all, returns (None, []), leave it untouched.
    """
    import hashlib
    import time as _time
    import uuid as _uuid

    import pyarrow as pa
    from pyiceberg.expressions import AlwaysTrue
    from pyiceberg.io.pyarrow import ArrowScan, _dataframe_to_data_files

    file_path = file_scan_task.file.file_path
    file_size_mb = file_scan_task.file.file_size_in_bytes / 1e6
    t_start = _time.perf_counter()

    batch = ArrowScan(
        table_metadata=table_metadata,
        io=io,
        projected_schema=table_metadata.schema(),
        row_filter=AlwaysTrue(),
    ).to_table(tasks=[file_scan_task])

    t_read = _time.perf_counter()
    logger.debug(
        "[rewrite] read %d rows / %.1f MB (compressed) from %s in %.2fs",
        len(batch),
        file_size_mb,
        file_path.split("/")[-1],
        t_read - t_start,
    )

    if len(batch) == 0:
        return (None, [])

    # Cast batch key columns to match keys_ref types so PyArrow's join doesn't
    # raise ArrowInvalid on utf8/large_utf8 or similar width mismatches.
    key_cast = {f.name: f.type for f in keys_ref.schema if f.name in upsert_cols}
    batch_keys = batch.select(upsert_cols)
    for col_name, target_type in key_cast.items():
        if batch_keys.schema.field(col_name).type != target_type:
            col_idx = batch_keys.schema.get_field_index(col_name)
            batch_keys = batch_keys.set_column(
                col_idx, col_name, batch_keys[col_name].cast(target_type)
            )

    idx_col = pa.array(range(len(batch)), type=pa.int64())
    preserved_keys = batch_keys.append_column("__row_idx__", idx_col).join(
        keys_ref, keys=upsert_cols, join_type="left anti"
    )

    if len(preserved_keys) == 0:
        # Every row in this file is being upserted — delete the whole file, no preserved file needed.
        logger.debug(
            "[rewrite] %s: all %d rows matched -> whole-file delete",
            file_path.split("/")[-1],
            len(batch),
        )
        return (file_scan_task.file, [])

    if len(preserved_keys) == len(batch):
        # No rows in this file match any upsert key — leave it alone entirely.
        logger.debug(
            "[rewrite] %s: 0 rows matched -> untouched", file_path.split("/")[-1]
        )
        return (None, [])

    preserved_rows = batch.take(preserved_keys["__row_idx__"])
    # Derive a deterministic write_uuid from the source file path so that
    # task retries overwrite the same object rather than leaking orphan files.
    preserved_write_uuid = _uuid.UUID(hashlib.md5(file_path.encode()).hexdigest())
    preserved_files = list(
        _dataframe_to_data_files(
            table_metadata=table_metadata,
            df=preserved_rows,
            io=io,
            write_uuid=preserved_write_uuid,
        )
    )
    logger.debug(
        "[rewrite] %s: %d/%d rows preserved -> wrote %d preserved file(s) in %.2fs",
        file_path.split("/")[-1],
        len(preserved_keys),
        len(batch),
        len(preserved_files),
        _time.perf_counter() - t_read,
    )
    return (file_scan_task.file, preserved_files)


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
            identifier_cols = []
            schema = self._table_metadata.schema()
            for field_id in schema.identifier_field_ids:
                col_name = schema.find_column_name(field_id)
                if col_name:
                    identifier_cols.append(col_name)
            return identifier_cols

        case_sensitive = self._upsert_kwargs.get("case_sensitive", True)

        # To support case insensitivity, we need to define a mapping of
        # provided (possibly case-modified) names to their original names in the schema
        if not case_sensitive:
            schema = self._table_metadata.schema()
            lower_to_original_mapping = {
                col.name.lower(): col.name for col in schema.fields
            }
            resolved_upsert_cols = []
            for upsert_col in upsert_cols:
                resolved_col = lower_to_original_mapping.get(upsert_col.lower())
                if resolved_col is None:
                    raise ValueError(
                        f"Upsert join column {upsert_col!r} does not match any column in "
                        f"table schema (case-insensitive)."
                    )
                resolved_upsert_cols.append(resolved_col)
            upsert_cols = resolved_upsert_cols

        return upsert_cols

    def _build_coarse_range_filter(
        self,
        keys_table: "pa.Table",
        upsert_cols: List[str],
    ) -> "BooleanExpression":
        """Build an O(1) coarse range filter covering all upsert key values.

        For each upsert column computes AND(GTE(col, min), LTE(col, max)).
        The filter may match rows outside the upsert batch (filter overshoot);
        callers must anti-join to identify and preserve those rows.
        """
        import pyarrow.compute as pc
        from pyiceberg.expressions import (
            AlwaysTrue,
            And,
            GreaterThanOrEqual,
            LessThanOrEqual,
        )

        expr = None
        for col_name in upsert_cols:
            mm = pc.min_max(keys_table[col_name])
            min_val = mm["min"].as_py()
            max_val = mm["max"].as_py()
            if min_val is None:
                continue
            col_expr = And(
                GreaterThanOrEqual(col_name, min_val),
                LessThanOrEqual(col_name, max_val),
            )
            expr = col_expr if expr is None else And(expr, col_expr)

        return expr if expr is not None else AlwaysTrue()

    def _commit_upsert_scan_merge(
        self,
        txn: "Table.transaction",
        data_files: List["DataFile"],
        keys_table: "pa.Table",
        upsert_cols: List[str],
    ) -> None:
        """Upsert commit using coarse range filter + per-file distributed anti-join.

        ┌─────────────────────────────────────────────────────────────┐
        │  Stage 1: Build coarse filter (driver)                      │
        │    keys_table ──► min/max per col ──► coarse_filter         │
        └─────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
        ┌─────────────────────────────────────────────────────────────┐
        │  Stage 2: Plan candidate files (driver)                     │
        │    table.scan(coarse_filter).plan_files()                   │
        │        ──► file_scan_tasks                                  │
        └─────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
        ┌─────────────────────────────────────────────────────────────┐
        │  Stage 3: Rewrite (one _rewrite_iceberg_file task per file) │
        │    read file ─► anti-join keys ─► write preserved rows      │
        │    returns (old_file, preserved_files)                      │
        └─────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
        ┌─────────────────────────────────────────────────────────────┐
        │  Stage 4: Atomic overwrite (driver)                         │
        │    delete  old_file         (each rewritten candidate)      │
        │    append  preserved_files  (preserved rows kept)           │
        │    append  data_files       (new upsert payload)            │
        └─────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                            commit_transaction

        1. Build an O(1) coarse range filter using min-max covering upsert key values (for each column).
        2. plan_files() on the driver to find candidate files that could be updated
        3. Dispatch one Ray task per candidate file. Each task reads its file,
           anti-joins against the upsert keys to find preserved rows (rows that
           the coarse delete would remove but that are NOT being upserted), and
           writes them as new data files directly to storage.
        4. Commit atomically via txn.update_snapshot().overwrite(): delete each
           original candidate file and append preserved files + new upsert data files.
        """
        import time

        case_sensitive = self._upsert_kwargs.get("case_sensitive", True)
        branch = self._upsert_kwargs.get("branch", "main")
        unknown = set(self._upsert_kwargs) - {
            _UPSERT_COLS_ID,
            "case_sensitive",
            "branch",
        }
        if unknown:
            logger.warning(
                "[scan-merge] ignoring unsupported upsert_kwargs: %s", sorted(unknown)
            )

        # Dedup keys to minimise per-task anti-join hash table size.
        keys_table = keys_table.group_by(upsert_cols).aggregate([])

        coarse_filter = self._build_coarse_range_filter(keys_table, upsert_cols)
        logger.debug("[scan-merge] coarse_filter=%s", coarse_filter)

        # plan_files() reads only manifest metadata, no Parquet data on the driver.
        t0 = time.perf_counter()
        scan: "DataScan" = self._table.scan(
            row_filter=coarse_filter, case_sensitive=case_sensitive
        )
        # Use the specific branch for the scan
        scan = scan.use_ref(branch)
        file_scan_tasks: List["FileScanTask"] = list(scan.plan_files())

        logger.info(
            "[scan-merge] planned %d candidate file(s) in %.2fs",
            len(file_scan_tasks),
            time.perf_counter() - t0,
        )

        if not file_scan_tasks:
            # No existing files match the coarse filter, so it's a pure insert.
            self._append_and_commit(txn, data_files, branch=branch)
            return

        # Put the deduped keys in the object store once; all tasks share one copy.
        keys_ref = ray.put(keys_table)

        t0 = time.perf_counter()
        refs = [
            _rewrite_iceberg_file.options(
                memory=int(
                    task.file.file_size_in_bytes
                    * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT
                )
            ).remote(task, keys_ref, upsert_cols, self._table_metadata, self._io)
            for task in file_scan_tasks
        ]
        logger.info("[scan-merge] dispatched %d rewrite task(s)", len(refs))

        # Collect results with periodic progress logs so long rewrites aren't silent.
        results = []
        pending = list(refs)
        _LOG_INTERVAL = max(1, len(refs) // 10)  # log ~10 times total
        while pending:
            done, pending = ray.wait(
                pending, num_returns=min(_LOG_INTERVAL, len(pending)),
                timeout=_REWRITE_STALL_TIMEOUT_S, fetch_local=True
            )
            results.extend(ray.get(done))
            logger.debug(
                "[scan-merge] rewrite progress: %d/%d file(s) done (%.1fs elapsed)",
                len(results),
                len(refs),
                time.perf_counter() - t0,
            )

        logger.info(
            "[scan-merge] all %d file(s) rewritten in %.2fs",
            len(refs),
            time.perf_counter() - t0,
        )

        # Count how many files were wholly deleted vs partially rewritten.
        n_whole_delete = n_partial = n_untouched = 0
        for old, preserved_files in results:
            if old is None:
                n_untouched += 1
            elif preserved_files:
                n_partial += 1
            else:
                n_whole_delete += 1
        logger.info(
            "[scan-merge] files: %d whole-delete, %d partial-rewrite, %d untouched",
            n_whole_delete,
            n_partial,
            n_untouched,
        )

        # Single atomic commit: schema update (already staged in txn), and overwrite.
        # _OverwriteFiles handles both file-level deletes and appends in one snapshot.
        t0 = time.perf_counter()
        with txn.update_snapshot(
            snapshot_properties=self._snapshot_properties, branch=branch
        ).overwrite() as snap:
            for old_file, preserved_files in results:
                if old_file is not None:
                    snap.delete_data_file(old_file)
                for preserved_file in preserved_files:
                    snap.append_data_file(preserved_file)
            for df in data_files:
                snap.append_data_file(df)

        self._with_retry(
            txn.commit_transaction,
            description=f"commit upsert transaction to Iceberg table '{self.table_identifier}'",
        )
        logger.info("[scan-merge] committed in %.2fs", time.perf_counter() - t0)

    def _append_and_commit(
        self,
        txn: "Table.transaction",
        data_files: List["DataFile"],
        branch: str = "main",
    ) -> None:
        """Append data files to a transaction and commit.

        Args:
            txn: PyIceberg transaction object
            data_files: List of DataFile objects to append
            branch: Iceberg branch to commit the snapshot to. Defaults to "main"
                to match pyiceberg's default
        """
        with txn._append_snapshot_producer(
            self._snapshot_properties, branch=branch
        ) as append_files:
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
                self._commit_upsert_scan_merge(txn, data_files, keys_table, upsert_cols)
                return
        else:
            logger.info("[upsert commit] No upsert keys — skipping delete phase")

        # No non-NULL keys — just append new data files and commit
        logger.info(
            "[upsert commit] Appending %d data files and committing ...",
            len(data_files),
        )
        t0 = time.perf_counter()
        branch = self._upsert_kwargs.get("branch", "main")
        self._append_and_commit(txn, data_files, branch=branch)
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

        return IcebergWriteResult(
            data_files=all_data_files,
            upsert_keys=upsert_keys,
            schemas=block_schemas,
        )

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
        Complete the write by reconciling schemas and committing all data files.

        This runs on the driver after all workers finish writing files.
        Collects all DataFile objects and schemas from all workers, reconciles schemas
        (allowing type promotion), updates table schema if needed, then performs a single
        atomic commit.
        """
        import time

        t_start = time.perf_counter()
        logger.info("[on_write_complete] Starting commit phase (mode=%s)", self._mode)

        # Collect all data files and schemas from all workers
        all_data_files: List["DataFile"] = []
        all_schemas: List["pa.Schema"] = []
        upsert_keys_tables: List["pa.Table"] = []

        for write_return in write_result.write_returns:
            if not write_return:
                continue

            if write_return.data_files:  # Only add schema if we have data files
                all_data_files.extend(write_return.data_files)
                all_schemas.extend(write_return.schemas)
                if write_return.upsert_keys is not None:
                    upsert_keys_tables.append(write_return.upsert_keys)

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
