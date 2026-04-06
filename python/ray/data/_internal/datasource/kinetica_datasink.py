"""
Kinetica Datasink for Ray Data.

This module provides a Ray Data Datasink implementation for writing data
to Kinetica databases.
"""

from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Dict, Iterable, List, Optional, Union, TYPE_CHECKING
import logging

from ray.data._internal.util import _check_import
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink

if TYPE_CHECKING:
    import pyarrow as pa


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


class KineticaDatasink(Datasink):
    """
    A Ray Data Datasink for writing to Kinetica databases.

    This datasink supports distributed writes using Kinetica's multihead
    ingestion capabilities for optimal performance.

    Example:
        >>> import ray
        >>> from ray.data._internal.datasource.kinetica_datasink import (
        ...     KineticaDatasink,
        ...     KineticaSinkMode,
        ... )
        >>>
        >>> ds = ray.data.from_items([{"id": 1, "name": "Alice"}])
        >>> ds.write_datasink(
        ...     KineticaDatasink(
        ...         url="http://localhost:9191",
        ...         table_name="my_table",
        ...         username="admin",
        ...         password="password",
        ...         mode=KineticaSinkMode.APPEND,
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
            mode = mode_map.get(mode.lower(), KineticaSinkMode.APPEND)

        self._mode = mode
        self._schema = schema
        self._table_settings = table_settings or KineticaTableSettings()
        self._batch_size = batch_size
        self._use_multihead = use_multihead
        self._options = options or {}

        # Serializable column definitions
        self._column_defs: Optional[List[Dict[str, Any]]] = None
        self._schema_string: Optional[str] = None
        self._column_properties: Optional[Dict[str, List[str]]] = None
        self._table_ready: bool = False

    def _init_client(self):
        """Create and return a GPUdb client instance."""
        from gpudb import GPUdb

        gpudb_options = GPUdb.Options()

        if self._username:
            gpudb_options.username = self._username
        if self._password:
            gpudb_options.password = self._password

        for key, value in self._options.items():
            if hasattr(gpudb_options, key):
                setattr(gpudb_options, key, value)

        return GPUdb(host=self._url, options=gpudb_options)

    def _columns_to_dicts(self, columns) -> List[Dict[str, Any]]:
        """Convert GPUdbRecordColumn objects to serializable dicts."""
        return [
            {
                "name": col.name,
                "column_type": col.column_type,
                "column_properties": col.column_properties,
                "is_nullable": col.is_nullable,
            }
            for col in columns
        ]

    def _dicts_to_columns(self, dicts: List[Dict[str, Any]]):
        """Convert serializable dicts back to GPUdbRecordColumn objects."""
        from gpudb import GPUdbRecordColumn

        return [
            GPUdbRecordColumn(
                name=d["name"],
                column_type=d["column_type"],
                column_properties=d["column_properties"],
                is_nullable=d["is_nullable"],
            )
            for d in dicts
        ]

    def _table_exists(self, client) -> bool:
        """Check if the target table exists."""
        from gpudb import GPUdbException

        try:
            response = client.has_table(table_name=self._table_name)
            return response.get("table_exists", False)
        except GPUdbException:
            return False

    def _drop_table(self, client) -> None:
        """Drop the target table if it exists."""
        from gpudb import GPUdbException

        try:
            client.clear_table(table_name=self._table_name)
            logger.info(f"Dropped table '{self._table_name}'")
        except GPUdbException as e:
            logger.warning(f"Failed to drop table '{self._table_name}': {e}")

    def _create_table(self, client, columns):
        """Create the target table with the given column definitions."""
        from gpudb import GPUdbRecordType

        record_type = GPUdbRecordType(
            columns=columns,
            label=self._table_name,
        )

        options = {}

        if self._table_settings.is_replicated:
            options["is_replicated"] = "true"

        if self._table_settings.chunk_size:
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
        from gpudb import GPUdbRecordType, GPUdbException

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

        Args:
            schema: Optional PyArrow schema passed by Ray Data framework.
        """
        from gpudb import GPUdbException
        from ray.data._internal.datasource.kinetica_type_utils import (
            arrow_schema_to_kinetica_columns,
        )

        if schema is not None and self._schema is None:
            self._schema = schema

        client = self._init_client()

        try:
            table_exists = self._table_exists(client)

            if self._mode == KineticaSinkMode.CREATE:
                if table_exists:
                    raise GPUdbException(
                        f"Table '{self._table_name}' already exists. "
                        "Use mode='append' or mode='overwrite'."
                    )
                if self._schema is None:
                    raise GPUdbException(
                        "Schema must be provided when using mode='create'"
                    )
                columns = arrow_schema_to_kinetica_columns(
                    self._schema,
                    primary_keys=self._table_settings.primary_keys,
                    shard_keys=self._table_settings.shard_keys,
                )
                self._create_table(client, columns)
                self._column_defs = self._columns_to_dicts(columns)
                self._table_ready = True

            elif self._mode == KineticaSinkMode.OVERWRITE:
                if table_exists:
                    self._drop_table(client)
                if self._schema is None:
                    raise GPUdbException(
                        "Schema must be provided when using mode='overwrite'"
                    )
                columns = arrow_schema_to_kinetica_columns(
                    self._schema,
                    primary_keys=self._table_settings.primary_keys,
                    shard_keys=self._table_settings.shard_keys,
                )
                self._create_table(client, columns)
                self._column_defs = self._columns_to_dicts(columns)
                self._table_ready = True

            elif self._mode == KineticaSinkMode.APPEND:
                if table_exists:
                    record_type = self._get_existing_record_type(client)
                    self._column_defs = self._columns_to_dicts(record_type.columns)
                    self._table_ready = True
                elif self._schema is not None:
                    columns = arrow_schema_to_kinetica_columns(
                        self._schema,
                        primary_keys=self._table_settings.primary_keys,
                        shard_keys=self._table_settings.shard_keys,
                    )
                    self._create_table(client, columns)
                    self._column_defs = self._columns_to_dicts(columns)
                    self._table_ready = True
                else:
                    logger.info(
                        f"Table '{self._table_name}' does not exist and no schema provided. "
                        "Schema will be inferred from first data block."
                    )

        finally:
            pass

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

    def _create_gpudb_table(self, client):
        """Create a GPUdbTable instance for writing records."""
        from gpudb import GPUdbTable, GPUdbTableOptions, GPUdbException

        record_type = self._get_record_type()

        if record_type is None:
            raise GPUdbException(
                "Cannot create GPUdbTable: no schema information available"
            )

        table_options = GPUdbTableOptions()

        if self._table_settings.is_replicated:
            table_options.is_replicated = True

        if self._table_settings.chunk_size:
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
            flush_multi_head_ingest_per_insertion=True,
        )

        return gpudb_table

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
            arrow_schema_to_kinetica_columns,
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
            try:
                gpudb_table = self._create_gpudb_table(client)
            except Exception as e:
                logger.warning(
                    f"Could not create GPUdbTable, falling back to direct insert: {e}"
                )

        try:
            for block in blocks:
                accessor = BlockAccessor.for_block(block)
                arrow_table = accessor.to_arrow()

                if kinetica_columns is None:
                    kinetica_columns = arrow_schema_to_kinetica_columns(
                        arrow_table.schema,
                        primary_keys=self._table_settings.primary_keys,
                        shard_keys=self._table_settings.shard_keys,
                    )
                    self._create_table(client, kinetica_columns)
                    try:
                        gpudb_table = self._create_gpudb_table(client)
                    except Exception as e:
                        logger.warning(f"Could not create GPUdbTable: {e}")

                for batch in arrow_table.to_batches():
                    records = convert_arrow_batch_to_records(
                        batch,
                        kinetica_columns,
                    )

                    if gpudb_table is not None:
                        inserted, updated, batch_errors = self._write_with_gpudb_table(
                            gpudb_table, records
                        )
                    else:
                        inserted, updated, batch_errors = self._write_simple(
                            client, records
                        )

                    total_inserted += inserted
                    total_updated += updated
                    errors.extend(batch_errors)

            if gpudb_table is not None:
                gpudb_table.flush_data_to_server()

        finally:
            pass

        return {
            "num_inserted": total_inserted,
            "num_updated": total_updated,
            "errors": errors,
        }

    def _write_with_gpudb_table(
        self, gpudb_table, records: List[Dict[str, Any]]
    ) -> tuple:
        """Write records using GPUdbTable."""
        from gpudb import GPUdbException

        inserted = 0
        updated = 0
        errors = []

        try:
            gpudb_table.insert_records(
                records,
                options={
                    "update_on_existing_pk": "true",
                    "return_individual_errors": "true",
                },
            )

            inserted = gpudb_table.latest_insert_records_count
            updated = gpudb_table.latest_update_records_count

        except GPUdbException as e:
            errors.append(str(e))
            logger.error(f"Error inserting records via GPUdbTable: {e}")

        return inserted, updated, errors

    def _write_simple(self, client, records: List[Dict[str, Any]]) -> tuple:
        """Write records using simple insert_records API with JSON encoding."""
        from gpudb import GPUdbException
        import json

        inserted = 0
        updated = 0
        errors = []

        for i in range(0, len(records), self._batch_size):
            batch = records[i : i + self._batch_size]

            try:
                json_records = [json.dumps(r) for r in batch]

                response = client.insert_records(
                    table_name=self._table_name,
                    data=json_records,
                    list_encoding="json",
                    options={
                        "update_on_existing_pk": "true",
                        "return_individual_errors": "true",
                    },
                )

                inserted += response.get("count_inserted", 0)
                updated += response.get("count_updated", 0)

                if response.get("info", {}).get("errors"):
                    errors.extend(response["info"]["errors"])

            except GPUdbException as e:
                errors.append(str(e))

        return inserted, updated, errors

    @property
    def supports_distributed_writes(self) -> bool:
        """Whether this datasink supports distributed writes."""
        return True

    def get_name(self) -> str:
        """Return the name of this datasink."""
        return f"Kinetica({self._table_name})"

    @property
    def num_rows_per_write(self) -> Optional[int]:
        """Target number of rows per write task."""
        return self._batch_size
