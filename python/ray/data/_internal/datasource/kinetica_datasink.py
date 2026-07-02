"""
Kinetica Datasink for Ray Data.

This module provides a Ray Data Datasink implementation for writing data
to Kinetica databases.
"""

import logging
from dataclasses import dataclass, field
from enum import IntEnum
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Union

from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink

if TYPE_CHECKING:
    import pyarrow as pa
    from gpudb import GPUdb


logger = logging.getLogger(__name__)


class KineticaSinkMode(IntEnum):
    """Defines how the datasink should handle existing tables."""

    CREATE = 1
    """Create a new table. Fails if the table already exists."""

    APPEND = 2
    """Append to an existing table, or create if it doesn't exist."""

    OVERWRITE = 3
    """Drop and recreate the table if it exists."""


@dataclass
class KineticaTableSettings:
    """Settings for Kinetica table creation."""

    is_replicated: bool = False
    """If True, creates a replicated table (data copied to all nodes)."""

    chunk_size: int = 8000000
    """Number of records per chunk for data storage."""

    ttl: int = -1
    """Time-to-live in minutes. -1 means no expiration."""

    primary_keys: List[str] = field(default_factory=list)
    """List of column names to use as primary key."""

    shard_keys: List[str] = field(default_factory=list)
    """List of column names to use as shard key."""

    persist: bool = True
    """If True, the table will be persisted to disk."""

    collection_name: Optional[str] = None
    """Optional schema/collection name for the table."""

    update_on_existing_pk: bool = True
    """If True, updates existing records with matching primary keys.
    If False, insert fails on PK conflict."""


class KineticaDatasink(Datasink):
    """
    A Ray Data Datasink for writing to Kinetica databases.

    This datasink supports distributed writes using Kinetica's multihead
    ingestion capabilities for optimal performance.

    Example:
        >>> import ray  # doctest: +SKIP
        >>> ds = ray.data.from_items([{"id": 1}])  # doctest: +SKIP
        >>> ds.write_datasink(  # doctest: +SKIP
        ...     KineticaDatasink(
        ...         url="http://localhost:9191",
        ...         table_name="my_table",
        ...         username="admin",
        ...         password="password",
        ...     )
        ... )
    """

    def __init__(
        self,
        url: str,
        table_name: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        mode: Union[KineticaSinkMode, str] = KineticaSinkMode.APPEND,
        schema: Optional["pa.Schema"] = None,
        table_settings: Optional[KineticaTableSettings] = None,
        batch_size: int = 10000,
        use_multihead: bool = True,
        options: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the Kinetica datasink.

        Args:
            url: URL of the Kinetica server (e.g., "http://localhost:9191").
            table_name: Name of the target table.
            username: Username for authentication.
            password: Password for authentication.
            mode: How to handle existing tables ("create", "append", "overwrite").
            schema: Optional PyArrow schema for table creation.
            table_settings: Optional table configuration settings.
            batch_size: Number of records per batch for ingestion.
            use_multihead: Whether to use multihead ingestion for parallelism.
            options: Additional GPUdb client options.
        """
        super().__init__()
        _check_import(self, module="gpudb", package="gpudb")

        self._url = url
        self._table_name = table_name
        self._username = username
        self._password = password

        # Handle string mode values
        if isinstance(mode, str):
            mode_map = {
                "create": KineticaSinkMode.CREATE,
                "append": KineticaSinkMode.APPEND,
                "overwrite": KineticaSinkMode.OVERWRITE,
            }
            mode_lower = mode.lower()
            if mode_lower not in mode_map:
                raise ValueError(
                    f"Invalid mode '{mode}'. "
                    "Must be one of: 'create', 'append', 'overwrite'"
                )
            mode = mode_map[mode_lower]

        self._mode = mode
        self._schema = schema
        self._table_settings = table_settings or KineticaTableSettings()

        # Validate batch_size to prevent silent failures in _write_simple
        if batch_size <= 0:
            raise ValueError(f"batch_size must be a positive integer, got {batch_size}")
        self._batch_size = batch_size

        self._use_multihead = use_multihead
        self._options = options or {}

        # Serializable column definitions
        self._column_defs: Optional[List[Dict[str, Any]]] = None
        self._schema_string: Optional[str] = None
        self._column_properties: Optional[Dict[str, List[str]]] = None

        # Track whether table setup has been performed
        self._table_initialized: bool = False

        # Validate configuration (no DDL side effects - those happen in on_write_start)
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate configuration without performing DDL operations.

        This is called during __init__ to validate parameters early.
        DDL operations (CREATE/DROP TABLE) are deferred to on_write_start
        to follow Ray's datasink contract.

        Raises:
            GPUdbException: If CREATE/OVERWRITE mode is used without a schema.
        """
        from gpudb import GPUdbException

        # CREATE and OVERWRITE modes require a schema upfront
        if self._mode == KineticaSinkMode.CREATE:
            if self._schema is None:
                raise GPUdbException("Schema must be provided when using mode='create'")

        elif self._mode == KineticaSinkMode.OVERWRITE:
            if self._schema is None:
                raise GPUdbException(
                    "Schema must be provided when using mode='overwrite'"
                )
        # APPEND mode: schema is optional (can use existing table schema)

    def _initialize_table(self) -> None:
        """Set up the table for writing.

        This is called from on_write_start when the write job actually begins.
        Performs DDL operations (CREATE/DROP TABLE) as needed based on mode.

        For CREATE: Creates new table (fails if table exists).
        For OVERWRITE: Drops existing table and creates new one.
        For APPEND: Creates table if it doesn't exist and schema is provided,
                   otherwise uses existing table schema.
        """
        from gpudb import GPUdbException

        from ray.data._internal.datasource.kinetica_type_utils import (
            arrow_schema_to_kinetica_columns,
        )

        client = self._init_client()
        table_exists = self._table_exists(client)

        if self._mode == KineticaSinkMode.CREATE:
            if table_exists:
                raise GPUdbException(
                    f"Table '{self._table_name}' already exists. "
                    "Use mode='append' or mode='overwrite'."
                )
            columns = arrow_schema_to_kinetica_columns(
                self._schema,
                primary_keys=self._table_settings.primary_keys,
                shard_keys=self._table_settings.shard_keys,
            )
            self._create_table(client, columns)
            self._column_defs = self._columns_to_dicts(columns)

        elif self._mode == KineticaSinkMode.OVERWRITE:
            if table_exists:
                self._drop_table(client)
            columns = arrow_schema_to_kinetica_columns(
                self._schema,
                primary_keys=self._table_settings.primary_keys,
                shard_keys=self._table_settings.shard_keys,
            )
            self._create_table(client, columns)
            self._column_defs = self._columns_to_dicts(columns)

        elif self._mode == KineticaSinkMode.APPEND:
            if table_exists:
                # Table exists - get schema from existing table
                record_type = self._get_existing_record_type(client)
                self._column_defs = self._columns_to_dicts(record_type.columns)
            elif self._schema is not None:
                # Table doesn't exist but schema provided - create table
                columns = arrow_schema_to_kinetica_columns(
                    self._schema,
                    primary_keys=self._table_settings.primary_keys,
                    shard_keys=self._table_settings.shard_keys,
                )
                self._create_table(client, columns)
                self._column_defs = self._columns_to_dicts(columns)
            else:
                # Table doesn't exist and no schema - can't proceed
                raise GPUdbException(
                    f"Table '{self._table_name}' does not exist and no schema "
                    "was provided. Either provide a schema to create the table, "
                    "or use mode='create' with an explicit schema."
                )

        self._table_initialized = True

    def _init_client(self):
        """Create and return a GPUdb client instance."""
        from ray.data._internal.datasource.kinetica_type_utils import (
            create_gpudb_client,
        )

        return create_gpudb_client(
            url=self._url,
            username=self._username,
            password=self._password,
            options=self._options,
        )

    def _columns_to_dicts(self, columns) -> List[Dict[str, Any]]:
        """Convert GPUdbRecordColumn objects to serializable dicts."""
        result = []
        for col in columns:
            col_dict = {
                "name": col.name,
                "column_type": col.column_type,
                "column_properties": col.column_properties,
                "is_nullable": col.is_nullable,
            }
            # Preserve precision and scale for decimal columns
            if hasattr(col, "precision") and col.precision is not None:
                col_dict["precision"] = col.precision
            if hasattr(col, "scale") and col.scale is not None:
                col_dict["scale"] = col.scale
            result.append(col_dict)
        return result

    def _dicts_to_columns(self, dicts: List[Dict[str, Any]]):
        """Convert serializable dicts back to GPUdbRecordColumn objects.

        Note: precision and scale for decimal columns are stored in the dict
        for reference. The GPUdbRecordColumn constructor parses these values
        from the column_properties list (e.g., 'decimal(18,4)') automatically.

        If a column has precision/scale in the dict but no decimal(p,s) property,
        we reconstruct the property to ensure consistency.
        """
        from gpudb import GPUdbRecordColumn

        result = []
        for d in dicts:
            properties = list(d["column_properties"])  # Make a copy

            # Ensure decimal columns have proper decimal(p,s) property format.
            # This handles edge cases where only 'decimal' without (p,s) exists.
            if "precision" in d and "scale" in d:
                has_decimal_with_params = any(
                    prop.startswith("decimal(") for prop in properties
                )
                if not has_decimal_with_params:
                    # Remove bare 'decimal' if present and add with params
                    properties = [p for p in properties if p != "decimal"]
                    properties.append(f"decimal({d['precision']},{d['scale']})")

            result.append(
                GPUdbRecordColumn(
                    name=d["name"],
                    column_type=d["column_type"],
                    column_properties=properties,
                    is_nullable=d["is_nullable"],
                )
            )
        return result

    def _table_exists(self, client: Any) -> bool:
        """Check if the target table exists.

        Args:
            client: GPUdb client instance.

        Returns:
            True if the table exists, False otherwise.

        Raises:
            GPUdbException: If an error occurs that is not related to the
                table not existing (e.g., auth failures, connection errors).
        """
        from gpudb import GPUdbException

        try:
            response = client.has_table(table_name=self._table_name)
            return response.get("table_exists", False)
        except GPUdbException as e:
            # Only swallow "table not found" type errors.
            # Re-raise auth errors, connection errors, etc.
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "not found" in error_msg:
                return False
            raise

    def _drop_table(self, client: Any) -> None:
        """Drop the target table if it exists.

        Uses Kinetica's clear_table API which drops the table completely
        (including its schema), allowing OVERWRITE mode to apply a new schema.

        Uses the 'no_error_if_not_exists' option to avoid exceptions when the
        table doesn't exist, making this operation idempotent.

        Args:
            client: GPUdb client instance.

        Raises:
            GPUdbException: If the table exists but cannot be dropped
                (e.g., due to permissions).
        """
        from gpudb import GPUdbException

        try:
            # clear_table drops the table completely (including schema),
            # allowing OVERWRITE mode to apply a new schema.
            client.clear_table(
                table_name=self._table_name,
                options={"no_error_if_not_exists": "true"},
            )
            logger.info(f"Dropped table '{self._table_name}'")
        except GPUdbException as e:
            # Real error (e.g., permissions) - propagate it
            raise GPUdbException(
                f"Failed to drop table '{self._table_name}' in OVERWRITE mode: {e}"
            ) from e

    def _create_table(self, client, columns):
        """Create the target table with the given column definitions."""
        from gpudb import GPUdbRecordType

        record_type = GPUdbRecordType(
            columns=columns,
            label=self._table_name,
        )

        options = {
            # Avoid error if table already exists (e.g., race condition
            # or retry scenario). The table schema must still be compatible.
            "no_error_if_exists": "true",
        }

        if self._table_settings.is_replicated:
            options["is_replicated"] = "true"

        if self._table_settings.chunk_size is not None:
            options["chunk_size"] = str(self._table_settings.chunk_size)

        if self._table_settings.ttl >= 0:
            options["ttl"] = str(self._table_settings.ttl)

        if not self._table_settings.persist:
            options["no_persist"] = "true"

        if self._table_settings.collection_name:
            options["collection_name"] = self._table_settings.collection_name

        type_id = record_type.create_type(client)

        client.create_table(
            table_name=self._table_name,
            type_id=type_id,
            options=options,
        )

        logger.info(f"Created table '{self._table_name}' with type_id '{type_id}'")

        self._schema_string = record_type.schema_string
        self._column_properties = record_type.column_properties

        return record_type

    def _get_existing_record_type(self, client):
        """Get the record type for an existing table."""
        from gpudb import GPUdbException, GPUdbRecordType

        response = client.show_table(
            table_name=self._table_name,
            options={"get_column_info": "true"},
        )

        type_ids = response.get("type_ids", [])
        type_schemas = response.get("type_schemas", [])
        properties_list = response.get("properties", [{}])

        if not type_ids or not type_schemas:
            raise GPUdbException(
                f"Could not retrieve type information for table '{self._table_name}'"
            )

        type_schema = type_schemas[0]
        props_dict = properties_list[0] if properties_list else {}

        self._schema_string = type_schema
        self._column_properties = props_dict

        record_type = GPUdbRecordType(
            schema_string=type_schema,
            column_properties=props_dict,
        )

        return record_type

    def on_write_start(self, schema: Optional["pa.Schema"] = None) -> None:
        """
        Called before writing begins.

        Performs table setup (DDL operations) when the write job actually starts.
        This follows Ray's datasink contract where side effects should not occur
        during datasink instantiation, only when writes begin.

        Args:
            schema: Optional PyArrow schema passed by Ray Data framework.
        """
        # Store schema if provided by Ray Data framework
        if schema is not None and self._schema is None:
            self._schema = schema

        # Perform table setup (CREATE/DROP) when writes actually begin
        if not self._table_initialized:
            self._initialize_table()

    def _get_record_type(self):
        """Reconstruct GPUdbRecordType from stored schema info."""
        from gpudb import GPUdbRecordType

        if self._schema_string is not None:
            return GPUdbRecordType(
                schema_string=self._schema_string,
                column_properties=self._column_properties,
            )
        elif self._column_defs is not None:
            columns = self._dicts_to_columns(self._column_defs)
            return GPUdbRecordType(columns=columns, label=self._table_name)
        return None

    def _create_gpudb_table(self, client: "GPUdb", table_exists: bool = False):
        """Create a GPUdbTable instance for writing records.

        Args:
            client: GPUdb client instance.
            table_exists: If True, the table already exists and table creation
                options (is_replicated, chunk_size, ttl, etc.) will be ignored
                since they only apply when creating a new table.

        Returns:
            GPUdbTable instance configured for writing.
        """
        from gpudb import GPUdbException, GPUdbTable, GPUdbTableOptions

        record_type = self._get_record_type()

        if record_type is None:
            raise GPUdbException(
                "Cannot create GPUdbTable: no schema information available"
            )

        # Only set table creation options when the table doesn't exist.
        # These options have no effect on existing tables and could cause
        # issues with some GPUdb SDK versions.
        table_options = GPUdbTableOptions()
        if not table_exists:
            if self._table_settings.is_replicated:
                table_options.is_replicated = True

            if self._table_settings.chunk_size is not None:
                table_options.chunk_size = self._table_settings.chunk_size

            if self._table_settings.ttl >= 0:
                table_options.ttl = self._table_settings.ttl

            if not self._table_settings.persist:
                table_options.no_persist = True

            if self._table_settings.collection_name:
                table_options.collection_name = self._table_settings.collection_name

        gpudb_table = GPUdbTable(
            _type=record_type,
            name=self._table_name,
            db=client,
            options=table_options,
            use_multihead_io=self._use_multihead,
            multihead_ingest_batch_size=self._batch_size,
            # Buffer records instead of flushing per insertion.
            # This allows the error check to prevent committing partial data
            # when errors occur. flush_data_to_server() is called explicitly
            # after confirming no errors.
            flush_multi_head_ingest_per_insertion=False,
        )

        return gpudb_table

    def _try_create_gpudb_table(self, client: Any, table_exists: bool = False):
        """Try to create GPUdbTable, handling errors based on multihead setting.

        Args:
            client: GPUdb client instance.
            table_exists: If True, the table already exists and table creation
                options will be skipped.

        Returns:
            GPUdbTable instance if successful, None if failed and multihead
            is not required.

        Raises:
            RuntimeError: If multihead is required but GPUdbTable creation fails.
        """
        try:
            return self._create_gpudb_table(client, table_exists=table_exists)
        except Exception as e:
            if self._use_multihead:
                raise RuntimeError(
                    "Failed to create GPUdbTable for multihead ingest. "
                    "Multihead ingestion was explicitly requested but "
                    "could not be initialized. Set use_multihead=False "
                    f"to use single-head insert instead. Error: {e}"
                ) from e
            else:
                logger.warning(
                    "Could not create GPUdbTable, falling back to "
                    f"direct insert: {e}"
                )
                return None

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> Any:
        """
        Write blocks of data to Kinetica.

        Args:
            blocks: Iterable of data blocks to write.
            ctx: Ray task context.

        Returns:
            Write statistics.
        """
        from ray.data._internal.datasource.kinetica_type_utils import (
            convert_arrow_batch_to_records,
        )

        client = self._init_client()
        total_inserted = 0
        total_updated = 0
        errors = []

        kinetica_columns = None
        if self._column_defs is not None:
            kinetica_columns = self._dicts_to_columns(self._column_defs)

        gpudb_table = None
        if self._schema_string is not None or self._column_defs is not None:
            # Table already exists at this point (created in on_write_start)
            gpudb_table = self._try_create_gpudb_table(client, table_exists=True)

        for block in blocks:
            accessor = BlockAccessor.for_block(block)
            arrow_table = accessor.to_arrow()

            for batch in arrow_table.to_batches():
                records = convert_arrow_batch_to_records(
                    batch,
                    kinetica_columns,
                )

                if gpudb_table is not None:
                    # With buffered mode (flush_multi_head_ingest_per_insertion=False),
                    # records are buffered and counts are only meaningful after flush.
                    # We only track errors here; counts are captured after final flush.
                    batch_errors = self._write_with_gpudb_table(gpudb_table, records)
                else:
                    # Simple path returns actual counts per batch
                    inserted, updated, batch_errors = self._write_simple(
                        client, records
                    )
                    total_inserted += inserted
                    total_updated += updated

                errors.extend(batch_errors)

        # Check for errors collected during insert_records calls.
        # Note: With flush_multi_head_ingest_per_insertion=False, most errors
        # will only surface during flush_data_to_server(), not here.
        if errors:
            error_summary = "; ".join(errors[:5])  # Show first 5 errors
            if len(errors) > 5:
                error_summary += f" ... and {len(errors) - 5} more errors"
            raise RuntimeError(
                f"Failed to write {len(errors)} record batch(es) to Kinetica "
                f"table '{self._table_name}': {error_summary}"
            )

        # Flush buffered records and handle any errors that surface during flush.
        # With flush_multi_head_ingest_per_insertion=False, per-record errors
        # are only detected here, not during insert_records calls.
        if gpudb_table is not None:
            from gpudb import GPUdbException
            from gpudb.gpudb_multihead_io import InsertionException

            try:
                gpudb_table.flush_data_to_server()
            except InsertionException as e:
                # InsertionException contains the records that failed to insert
                failed_records = e.get_records()
                raise RuntimeError(
                    f"Failed to flush records to Kinetica table "
                    f"'{self._table_name}': {e}. "
                    f"{len(failed_records)} record(s) failed to insert."
                ) from e
            except GPUdbException as e:
                raise RuntimeError(
                    f"Failed to flush records to Kinetica table "
                    f"'{self._table_name}': {e}"
                ) from e

            # Get total counts from GPUdbTable after all records are flushed.
            # These represent all records written through this table instance.
            total_inserted = gpudb_table.total_inserted
            total_updated = gpudb_table.total_updated

        return {
            "num_inserted": total_inserted,
            "num_updated": total_updated,
        }

    def _write_with_gpudb_table(
        self, gpudb_table: Any, records: List[Dict[str, Any]]
    ) -> List[str]:
        """Write records using GPUdbTable.

        With flush_multi_head_ingest_per_insertion=False, records are buffered
        and actual insert/update counts are only available after
        flush_data_to_server(). This method only returns errors; counts are
        retrieved from gpudb_table.total_inserted and gpudb_table.total_updated
        after the final flush.

        Args:
            gpudb_table: GPUdbTable instance for multihead ingestion.
            records: List of record dictionaries to write.

        Returns:
            List of error messages (empty if successful).
        """
        from gpudb import GPUdbException
        from gpudb.gpudb_multihead_io import InsertionException

        errors = []

        try:
            response = gpudb_table.insert_records(
                records,
                options={
                    "update_on_existing_pk": "true"
                    if self._table_settings.update_on_existing_pk
                    else "false",
                    "return_individual_errors": "true",
                },
            )

            # Check for per-record errors in the response.
            # When return_individual_errors is true, the response's info map
            # contains bad_record_indices (comma-separated list of indices)
            # and error_N entries with the error message for each failed record.
            if response is not None:
                # Handle both attribute-style (AttrDict) and dict-style responses
                info = getattr(response, "info", None)
                if info is None and hasattr(response, "get"):
                    info = response.get("info", {})
                if info:
                    bad_indices = info.get("bad_record_indices", "")
                    if bad_indices:
                        # Extract individual error messages for each bad record
                        for idx in bad_indices.split(","):
                            idx = idx.strip()
                            if idx:
                                error_key = f"error_{idx}"
                                default_err = f"Unknown error at index {idx}"
                                error_msg = info.get(error_key, default_err)
                                errors.append(f"Record {idx}: {error_msg}")
                                logger.error(
                                    f"Error inserting record at index {idx} "
                                    f"via GPUdbTable: {error_msg}"
                                )

        except InsertionException as e:
            # InsertionException is raised by the multihead ingestor and
            # contains the list of records that failed to insert
            failed_count = len(e.get_records())
            errors.append(f"{e} ({failed_count} record(s) failed)")
            logger.error(f"InsertionException via GPUdbTable: {e}")
        except GPUdbException as e:
            errors.append(str(e))
            logger.error(f"Error inserting records via GPUdbTable: {e}")

        return errors

    def _write_simple(self, client, records: List[Dict[str, Any]]) -> tuple:
        """Write records using simple insert_records API with JSON encoding."""
        import base64
        import json

        from gpudb import GPUdbException

        def json_serializer(obj):
            """Custom JSON serializer for bytes (vector columns)."""
            if isinstance(obj, bytes):
                return base64.b64encode(obj).decode("ascii")
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        inserted = 0
        updated = 0
        errors = []

        for i in range(0, len(records), self._batch_size):
            batch = records[i : i + self._batch_size]

            try:
                json_records = [json.dumps(r, default=json_serializer) for r in batch]

                response = client.insert_records(
                    table_name=self._table_name,
                    data=json_records,
                    list_encoding="json",
                    options={
                        "update_on_existing_pk": "true"
                        if self._table_settings.update_on_existing_pk
                        else "false",
                        "return_individual_errors": "true",
                    },
                )

                # Handle both attribute-style (AttrDict) and dict-style responses.
                # Use explicit None check to preserve valid zero counts.
                count_ins = getattr(response, "count_inserted", None)
                if count_ins is None and hasattr(response, "get"):
                    count_ins = response.get("count_inserted", 0)
                if count_ins is not None:
                    inserted += count_ins

                count_upd = getattr(response, "count_updated", None)
                if count_upd is None and hasattr(response, "get"):
                    count_upd = response.get("count_updated", 0)
                if count_upd is not None:
                    updated += count_upd

                # Check for per-record errors in the response info map.
                # When return_individual_errors is true, errors are reported via
                # bad_record_indices and error_N entries.
                info = getattr(response, "info", None)
                if info is None and hasattr(response, "get"):
                    info = response.get("info", {})
                if info:
                    bad_indices = info.get("bad_record_indices", "")
                    if bad_indices:
                        # Extract individual error messages for each bad record
                        for idx in bad_indices.split(","):
                            idx = idx.strip()
                            if idx:
                                error_key = f"error_{idx}"
                                default_err = f"Unknown error at index {idx}"
                                error_msg = info.get(error_key, default_err)
                                errors.append(f"Record {idx}: {error_msg}")
                                logger.error(
                                    f"Error inserting record at index {idx} "
                                    f"via simple insert: {error_msg}"
                                )
                    else:
                        # Fallback: check legacy 'errors' field
                        response_errors = info.get("errors")
                        if response_errors:
                            if isinstance(response_errors, list):
                                errors.extend(response_errors)
                            else:
                                errors.append(str(response_errors))

            except GPUdbException as e:
                errors.append(str(e))
                logger.error(f"Error inserting records via simple insert: {e}")

        return inserted, updated, errors

    @property
    def supports_distributed_writes(self) -> bool:
        """Whether this datasink supports distributed writes.

        All modes support distributed writes:
        - CREATE/OVERWRITE: Schema is required upfront, table structure is known
        - APPEND: Table must exist (enforced in on_write_start), so schema is
          always available from the existing table

        Returns:
            Always True since all modes have deterministic table structure.
        """
        return True

    def get_name(self) -> str:
        """Return the name of this datasink."""
        return f"Kinetica({self._table_name})"

    @property
    def min_rows_per_write(self) -> Optional[int]:
        """Target minimum number of rows per write task.

        This overrides the base class property to hint to Ray Data
        how many rows should be bundled together for each write call.
        """
        return self._batch_size
