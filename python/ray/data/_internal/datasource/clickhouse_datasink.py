import logging
import re
from enum import IntEnum
from typing import (
    Iterable,
    Optional,
    Dict,
    Any,
)
import pyarrow
import pyarrow.types as pat
from ray.data.block import BlockAccessor
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _check_import
from ray.data.block import Block
from ray.data.datasource.datasink import Datasink, WriteReturnType
from ray.util.annotations import DeveloperAPI, PublicAPI

logger = logging.getLogger(__name__)


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


def _arrow_type_name(arrow_type: pyarrow.DataType) -> str:
    if pat.is_boolean(arrow_type):
        return "bool"
    elif pat.is_int8(arrow_type):
        return "int8"
    elif pat.is_int16(arrow_type):
        return "int16"
    elif pat.is_int32(arrow_type):
        return "int32"
    elif pat.is_int64(arrow_type):
        return "int64"
    elif pat.is_uint8(arrow_type):
        return "uint8"
    elif pat.is_uint16(arrow_type):
        return "uint16"
    elif pat.is_uint32(arrow_type):
        return "uint32"
    elif pat.is_uint64(arrow_type):
        return "uint64"
    elif pat.is_float16(arrow_type):
        return "float16"
    elif pat.is_float32(arrow_type):
        return "float32"
    elif pat.is_float64(arrow_type):
        return "float64"
    elif pat.is_decimal(arrow_type):
        return "decimal"
    elif pat.is_string(arrow_type) or pat.is_large_string(arrow_type):
        return "string"
    elif pat.is_binary(arrow_type) or pat.is_large_binary(arrow_type):
        return "string"
    elif pat.is_timestamp(arrow_type):
        return "timestamp"
    else:
        return "string"


def _arrow_to_clickhouse_type(name: str, field: pyarrow.Field) -> str:
    arrow_to_ch = {
        "bool": "UInt8",
        "int8": "Int8",
        "int16": "Int16",
        "int32": "Int32",
        "int64": "Int64",
        "uint8": "UInt8",
        "uint16": "UInt16",
        "uint32": "UInt32",
        "uint64": "UInt64",
        "float16": "Float32",
        "float32": "Float32",
        "float64": "Float64",
        "string": "String",
        "binary": "String",
        "timestamp": "DateTime64(3)",
    }
    if name == "decimal":
        precision = field.type.precision or 38
        scale = field.type.scale or 10
        return f"Decimal({precision}, {scale})"
    return arrow_to_ch.get(name, "String")


@PublicAPI(stability="alpha")
class SinkMode(IntEnum):
    """
    Enum of possible modes for sinking data

    Attributes:
        CREATE: Create a new table; fail if it already exists.
        APPEND: Use an existing table if present; otherwise create one; then append data.
        OVERWRITE: Drop an existing table (if any) and create a fresh table before writing.
    """

    # Create a new table and fail if that table already exists.
    CREATE = 1

    APPEND = 2
    """Append data to an existing table, or create one if it does not exist."""

    OVERWRITE = 3
    """Drop the table if it already exists, then re-create it and write."""


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
            - CREATE: Create a new table; fail if it already exists.
            - APPEND: Use an existing table if present, otherwise create one;
              data will be appended to the table.
            - OVERWRITE: Drop an existing table (if any) and re-create it.
        client_settings: Optional ClickHouse server settings to be used with the
            session/every request. For more information, see
            `ClickHouse Client Settings doc
            <https://clickhouse.com/docs/en/integrations/python#settings-argument>`_.
        client_kwargs: Additional keyword arguments to pass to the
            ClickHouse client. For more information, see
            `ClickHouse Core Settings doc
            <https://clickhouse.com/docs/en/integrations/python#additional-options>`_.
        table_settings: A dictionary containing additional table creation
            instructions. For example, specifying engine, order_by, partition_by,
            primary_key, or custom settings:
            ``{"engine": "ReplacingMergeTree()", "order_by": "id"}``.
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
        client_settings: Optional[Dict[str, Any]] = None,
        client_kwargs: Optional[Dict[str, Any]] = None,
        table_settings: Optional[Dict[str, Any]] = None,
        max_insert_block_rows: Optional[int] = None,
    ) -> None:
        self._table = table
        self._dsn = dsn
        self._mode = mode
        self._client_settings = client_settings or {}
        self._client_kwargs = client_kwargs or {}
        self._table_settings = table_settings or {}
        self._max_insert_block_rows = max_insert_block_rows
        self._table_dropped = False

    def _init_client(self):
        _check_import(self, module="clickhouse_connect", package="clickhouse-connect")
        import clickhouse_connect

        return clickhouse_connect.get_client(
            dsn=self._dsn,
            settings=self._client_settings or {},
            **self._client_kwargs or {},
        )

    def _generate_create_table_sql(
        self,
        schema: pyarrow.Schema,
    ) -> str:
        engine = self._table_settings.get("engine", "MergeTree()")
        order_by = self._table_settings.get(
            "order_by", _pick_best_arrow_field_for_order_by(schema)
        )
        additional_clauses = []
        if "partition_by" in self._table_settings:
            additional_clauses.append(
                f"PARTITION BY {self._table_settings['partition_by']}"
            )
        if "primary_key" in self._table_settings:
            additional_clauses.append(
                f"PRIMARY KEY ({self._table_settings['primary_key']})"
            )
        if "settings" in self._table_settings:
            additional_clauses.append(f"SETTINGS {self._table_settings['settings']}")
        additional_props = ""
        if additional_clauses:
            additional_props = "\n" + "\n".join(additional_clauses)
        columns_def = []
        for field in schema:
            arrow_name = _arrow_type_name(field.type)
            ch_type = _arrow_to_clickhouse_type(arrow_name, field)
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
            check_query = self._CHECK_TABLE_EXISTS_TEMPLATE.format(
                table_name=self._table
            )
            result = client.query(check_query)
            # The result from 'EXISTS table_name' is [[1]] if it exists,
            # or [[0]] if it does not.
            if result and result.result_rows:
                return result.result_rows[0][0] == 1
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
            if self._mode == SinkMode.OVERWRITE:
                # Drop table if it exists.
                drop_sql = self._DROP_TABLE_TEMPLATE.format(table_name=self._table)
                logger.info(f"Mode=OVERWRITE => {drop_sql}")
                client.command(drop_sql)
                self._table_dropped = True
                # If the old table existed and no explicit "order_by" is set,
                # adopt the existing ORDER BY from the old table DDL.
                if table_exists and self._table_settings.get("order_by") is None:
                    existing_order_by = self._get_existing_order_by(client)
                    if existing_order_by is not None:
                        self._table_settings["order_by"] = existing_order_by
                        logger.info(
                            f"Reusing old ORDER BY from overwritten table: {existing_order_by}"
                        )
            elif self._mode == SinkMode.CREATE:
                if table_exists:
                    msg = (
                        f"Table {self._table} already exists in mode='CREATE'. "
                        "Use mode='APPEND' or 'OVERWRITE' instead."
                    )
                    logger.error(msg)
                    raise RuntimeError(msg)
            elif self._mode == SinkMode.APPEND:
                if table_exists and self._table_settings.get("order_by") is None:
                    existing_order_by = self._get_existing_order_by(client)
                    if existing_order_by:
                        self._table_settings["order_by"] = existing_order_by
                        logger.info(
                            f"Reusing existing ORDER BY for table {self._table}: {existing_order_by}"
                        )
        except Exception as e:
            logger.warning(
                f"Could not complete on_write_start for table {self._table}: {e}"
            )
            raise
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
                # For now, we're creating the table here because
                # the arrow schema from a block is required to generate the
                # ClickHouse DDL. The first worker that calls this
                # will create the table. Subsequent workers skip it.
                if not self._table_exists(client):
                    create_sql = self._generate_create_table_sql(
                        arrow_table.schema,
                    )
                    client.command(create_sql)
                # Chunk the arrow table if `max_insert_block_rows` is set.
                if (
                    self._max_insert_block_rows
                    and row_count > self._max_insert_block_rows
                ):
                    offset = 0
                    while offset < row_count:
                        slice_end = min(offset + self._max_insert_block_rows, row_count)
                        chunk = arrow_table.slice(offset, slice_end - offset)
                        client.insert_arrow(self._table, chunk)
                        total_inserted += chunk.num_rows
                        offset = slice_end
                else:
                    # Insert the entire table at once.
                    client.insert_arrow(self._table, arrow_table)
                    total_inserted += row_count
        except Exception as e:
            logger.warning(f"Failed to write block(s) to table {self._table}: {e}")
            raise
        finally:
            client.close()
        return [total_inserted]
