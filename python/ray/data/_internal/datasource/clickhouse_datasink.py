import logging
import re
from dataclasses import dataclass
from enum import IntEnum
from typing import (
    Any,
    Dict,
    Iterable,
    Optional,
)

import pyarrow
import pyarrow.types as pat

from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink, WriteReturnType
from ray.util.annotations import DeveloperAPI, PublicAPI

logger = logging.getLogger(__name__)

DEFAULT_DECIMAL_PRECISION = 38
DEFAULT_DECIMAL_SCALE = 10


def _pick_best_arrow_field_for_order_by(schema: pyarrow.Schema) -> str:
    if len(schema) == 0:
        return "tuple()"
    # Prefer a timestamp column if available
    for f in schema:
        if pat.is_timestamp(f.type):
            return f.name
    # Next prefer a non-string column
    for f in schema:
        if not (pat.is_string(f.type) or pat.is_large_string(f.type)):
            return f.name
    # Otherwise pick the first column
    return schema[0].name


def _arrow_to_clickhouse_type(field: pyarrow.Field) -> str:
    """Convert a PyArrow field to an appropriate ClickHouse column type."""
    t = field.type
    if pat.is_decimal(t):
        precision = t.precision or DEFAULT_DECIMAL_PRECISION
        scale = t.scale or DEFAULT_DECIMAL_SCALE
        return f"Decimal({precision}, {scale})"
    if pat.is_boolean(t):
        return "UInt8"
    if pat.is_int8(t):
        return "Int8"
    if pat.is_int16(t):
        return "Int16"
    if pat.is_int32(t):
        return "Int32"
    if pat.is_int64(t):
        return "Int64"
    if pat.is_uint8(t):
        return "UInt8"
    if pat.is_uint16(t):
        return "UInt16"
    if pat.is_uint32(t):
        return "UInt32"
    if pat.is_uint64(t):
        return "UInt64"
    if pat.is_float16(t):
        return "Float32"
    if pat.is_float32(t):
        return "Float32"
    if pat.is_float64(t):
        return "Float64"
    if pat.is_timestamp(t):
        return "DateTime64(3)"
    return "String"


@dataclass
class ClickHouseTableSettings:
    """
    Additional table creation instructions for ClickHouse.

    Attributes:
        engine: The engine definition for the created table. Defaults
            to "MergeTree()".
        order_by: The ORDER BY clause for the table.
        partition_by: The PARTITION BY clause for the table.
        primary_key: The PRIMARY KEY clause for the table.
        settings: Additional SETTINGS clause for the table
            (comma-separated or any valid string).
    """

    engine: str = "MergeTree()"
    order_by: Optional[str] = None
    partition_by: Optional[str] = None
    primary_key: Optional[str] = None
    settings: Optional[str] = None


@PublicAPI(stability="alpha")
class SinkMode(IntEnum):
    """
    Enum of possible modes for sinking data

    Attributes:
        CREATE: Create a new table; fail if that table already exists.
        APPEND: Use an existing table if present, otherwise create one; then append data.
        OVERWRITE: Drop the table if it already exists, then re-create it and write.
    """

    # Create a new table and fail if that table already exists.
    CREATE = 1

    # Append data to an existing table, or create one if it does not exist.
    APPEND = 2

    # Drop the table if it already exists, then re-create it and write.
    OVERWRITE = 3


@DeveloperAPI
class ClickHouseDatasink(Datasink):
    """ClickHouse Ray Datasink.

    A Ray Datasink for writing data into ClickHouse, with support for distributed
    writes and mode-based table management (create, append, or overwrite).

    Args:
        table: Fully qualified table identifier (e.g., "default.my_table").
        dsn: A string in DSN (Data Source Name) HTTP format
            (e.g., "clickhouse+http://username:password@host:8123/default").
            For more information, see `ClickHouse Connection String doc
            <https://clickhouse.com/docs/en/integrations/sql-clients/cli#connection_string>`_.
        mode: One of SinkMode.CREATE, SinkMode.APPEND,
            or SinkMode.OVERWRITE.
            - **CREATE**: Create a new table; fail if that table already exists.
              Requires a user-supplied schema if the table doesn’t already exist.
            - **APPEND**: Use an existing table if present, otherwise create one.
              If the table does not exist, the user must supply a schema. Data
              is then appended to the table.
            - **OVERWRITE**: Drop the table if it exists, then re-create it.
              **Always requires** a user-supplied schema to define the new table.
        schema: An optional PyArrow schema object that, if provided, will
            override any inferred schema for table creation.
            - If you are creating a new table (CREATE or APPEND when the table
              doesn’t exist) or overwriting an existing table, you **must**
              provide a schema.
            - If you’re appending to an already-existing table, the schema is
              not strictly required unless you want to cast data or enforce
              column types. If omitted, the existing table definition is used.
        client_settings: Optional ClickHouse server settings to be used with the
            session/every request.
        client_kwargs: Additional keyword arguments to pass to the
            ClickHouse client.
        table_settings: An optional dataclass with additional table creation
            instructions (e.g., engine, order_by, partition_by, primary_key, settings).
        max_insert_block_rows: If you have extremely large blocks, specifying
            a limit here will chunk the insert into multiple smaller insert calls.
            Defaults to None (no chunking).
    """

    NAME = "ClickHouse"

    _CREATE_TABLE_TEMPLATE = """
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns}
        )
        ENGINE = {engine}
        ORDER BY {order_by}
        {additional_props}
    """
    _DROP_TABLE_TEMPLATE = """DROP TABLE IF EXISTS {table_name}"""
    _CHECK_TABLE_EXISTS_TEMPLATE = """EXISTS {table_name}"""
    _SHOW_CREATE_TABLE_TEMPLATE = """SHOW CREATE TABLE {table_name}"""

    def __init__(
        self,
        table: str,
        dsn: str,
        mode: SinkMode = SinkMode.CREATE,
        schema: Optional[pyarrow.Schema] = None,
        client_settings: Optional[Dict[str, Any]] = None,
        client_kwargs: Optional[Dict[str, Any]] = None,
        table_settings: Optional[ClickHouseTableSettings] = None,
        max_insert_block_rows: Optional[int] = None,
    ) -> None:
        self._table = table
        self._dsn = dsn
        self._mode = mode
        self._schema = schema
        self._client_settings = client_settings or {}
        self._client_kwargs = client_kwargs or {}
        self._table_settings = table_settings or ClickHouseTableSettings()
        self._max_insert_block_rows = max_insert_block_rows
        self._table_dropped = False

    def _init_client(self):
        _check_import(self, module="clickhouse_connect", package="clickhouse-connect")
        import clickhouse_connect

        return clickhouse_connect.get_client(
            dsn=self._dsn,
            settings=self._client_settings,
            **self._client_kwargs,
        )

    def _generate_create_table_sql(
        self,
        schema: pyarrow.Schema,
    ) -> str:
        engine = self._table_settings.engine
        if self._table_settings.order_by is not None:
            order_by = self._table_settings.order_by
        else:
            order_by = _pick_best_arrow_field_for_order_by(schema)
        additional_clauses = []
        if self._table_settings.partition_by is not None:
            additional_clauses.append(
                f"PARTITION BY {self._table_settings.partition_by}"
            )
        if self._table_settings.primary_key is not None:
            additional_clauses.append(
                f"PRIMARY KEY ({self._table_settings.primary_key})"
            )
        if self._table_settings.settings is not None:
            additional_clauses.append(f"SETTINGS {self._table_settings.settings}")
        additional_props = ""
        if additional_clauses:
            additional_props = "\n" + "\n".join(additional_clauses)
        columns_def = []
        for field in schema:
            ch_type = _arrow_to_clickhouse_type(field)
            columns_def.append(f"`{field.name}` {ch_type}")
        columns_str = ",\n    ".join(columns_def)
        return self._CREATE_TABLE_TEMPLATE.format(
            table_name=self._table,
            columns=columns_str,
            engine=engine,
            order_by=order_by,
            additional_props=additional_props,
        )

    def _table_exists(self, client) -> bool:
        try:
            result = client.query(
                self._CHECK_TABLE_EXISTS_TEMPLATE.format(table_name=self._table)
            )
            if result is None:
                return False
            for helper in ("scalar", "first_item", "first_value"):
                _check_import(
                    self, module="clickhouse_connect", package="clickhouse-connect"
                )
                from clickhouse_connect.driver.exceptions import Error as CHError

                if hasattr(result, helper):
                    try:
                        return bool(getattr(result, helper)())
                    except (TypeError, ValueError, CHError) as exc:
                        # Helper exists but failed – continue probing.
                        logger.debug(
                            "Helper %s failed: %s; will try fallbacks", helper, exc
                        )
            # Fallback to inspecting common container attributes.
            for attr in ("result_rows", "rows", "data"):
                rows = getattr(result, attr, None)
                if rows:
                    first = rows[0]
                    # Unwrap an extra layer if present (i.e. [[1]] or [(1,)])
                    if isinstance(first, (list, tuple)):
                        first = first[0] if first else 0
                    return bool(first)
            return False
        except Exception as e:
            logger.warning(f"Could not verify if table {self._table} exists: {e}")
            return False

    def _get_existing_order_by(self, client) -> Optional[str]:
        logger.debug(
            f"Retrieving ORDER BY clause from SHOW CREATE TABLE for {self._table}"
        )
        try:
            show_create_sql = self._SHOW_CREATE_TABLE_TEMPLATE.format(
                table_name=self._table
            )
            result = client.command(show_create_sql)
            ddl_str = str(result)
            pattern = r"(?is)\border\s+by\s+(.*?)(?=\bengine\b|$)"
            match = re.search(pattern, ddl_str)
            if match:
                return match.group(1).strip()
            return None
        except Exception as e:
            logger.warning(
                f"Could not retrieve SHOW CREATE TABLE for {self._table}: {e}"
            )
            return None

    @property
    def supports_distributed_writes(self) -> bool:
        return True

    def on_write_start(self) -> None:
        client = None
        try:
            client = self._init_client()
            table_exists = self._table_exists(client)
            schema_required = (
                # Overwrite always needs a schema because it recreates the table
                self._mode == SinkMode.OVERWRITE
                # For CREATE or APPEND we need a schema only when the table is
                # absent and will therefore be created in this call.
                or (
                    self._mode in (SinkMode.CREATE, SinkMode.APPEND)
                    and not table_exists
                )
            )
            if schema_required and self._schema is None:
                if self._mode == SinkMode.OVERWRITE:
                    raise ValueError(
                        f"Overwriting table {self._table} requires a user‑provided schema."
                    )
                else:
                    raise ValueError(
                        f"Table {self._table} does not exist in mode='{self._mode.name}' and "
                        "no schema was provided. Cannot create the table without a schema."
                    )
            # OVERWRITE MODE
            if self._mode == SinkMode.OVERWRITE:
                # If we plan to overwrite, drop the table if it exists,
                # then re-create it using the user-provided schema.
                if table_exists and self._table_settings.order_by is None:
                    # Collect existing ORDER BY. This lets us preserve it if the user
                    # hasn't specified one explicitly.
                    existing_order_by = self._get_existing_order_by(client)
                    if existing_order_by is not None:
                        self._table_settings.order_by = existing_order_by
                        logger.info(
                            f"Reusing old ORDER BY from overwritten table:"
                            f" {existing_order_by}"
                        )
                # DROP and CREATE the table.
                drop_sql = self._DROP_TABLE_TEMPLATE.format(table_name=self._table)
                logger.info(f"Mode=OVERWRITE => {drop_sql}")
                client.command(drop_sql)
                self._table_dropped = True
                create_sql = self._generate_create_table_sql(self._schema)
                client.command(create_sql)
            # CREATE MODE
            elif self._mode == SinkMode.CREATE:
                # If table already exists in CREATE mode, fail immediately.
                if table_exists:
                    msg = (
                        f"Table {self._table} already exists in mode='CREATE'. "
                        "Use mode='APPEND' or 'OVERWRITE' instead."
                    )
                    logger.error(msg)
                    raise ValueError(msg)
                # Otherwise, create it (requires user-provided schema).
                create_sql = self._generate_create_table_sql(self._schema)
                client.command(create_sql)
            # APPEND MODE
            elif self._mode == SinkMode.APPEND:
                if table_exists:
                    # Table exists – validate or adopt ORDER BY.
                    existing_order_by = self._get_existing_order_by(client)
                    user_order_by = self._table_settings.order_by
                    if user_order_by is not None:
                        # The user explicitly set an order_by. Check if it conflicts:
                        if existing_order_by and existing_order_by != user_order_by:
                            raise ValueError(
                                f"Conflict with order_by. The existing table {self._table} "
                                f"has ORDER BY {existing_order_by}, but the user specified "
                                f"ORDER BY {user_order_by}. Please drop or overwrite the table, "
                                f"or use the same ordering."
                            )
                    elif existing_order_by:
                        self._table_settings.order_by = existing_order_by
                        logger.info(
                            f"Reusing existing ORDER BY for table {self._table}: {existing_order_by}"
                        )
                else:
                    # Table doesn't exist, so create it with user schema
                    create_sql = self._generate_create_table_sql(self._schema)
                    client.command(create_sql)

        except Exception as e:
            logger.error(
                f"Could not complete on_write_start for table {self._table}: {e}"
            )
            raise e
        finally:
            if client:
                client.close()

    def get_name(self) -> str:
        return self.NAME

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> WriteReturnType:
        client = self._init_client()
        total_inserted = 0
        try:
            for block in blocks:
                arrow_table = BlockAccessor.for_block(block).to_arrow()
                row_count = arrow_table.num_rows
                if self._schema is not None:
                    arrow_table = arrow_table.cast(self._schema, safe=True)
                if self._max_insert_block_rows is not None:
                    max_chunk_size = self._max_insert_block_rows
                else:
                    # If max_insert_block_rows is not set, insert all rows in one go
                    max_chunk_size = row_count if row_count > 0 else 1
                offsets = list(range(0, row_count, max_chunk_size))
                offsets.append(row_count)
                for i in range(len(offsets) - 1):
                    start = offsets[i]
                    end = offsets[i + 1]
                    chunk = arrow_table.slice(start, end - start)
                    client.insert_arrow(self._table, chunk)
                    total_inserted += chunk.num_rows
        except Exception as e:
            logger.error(f"Failed to write block(s) to table {self._table}: {e}")
            raise e
        finally:
            client.close()
        return [total_inserted]
