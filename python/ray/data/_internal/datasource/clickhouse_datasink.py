import logging
from typing import Iterable, Optional, Dict, Any

import pyarrow
import pyarrow.types as pat

import ray
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.util import _check_import
from ray.data.block import Block
from ray.data.datasource.datasink import Datasink

logger = logging.getLogger(__name__)


class ClickHouseDatasink(Datasink[None]):
    """
    A Ray datasink for writing data into ClickHouse.

    Args:
        table: Fully qualified table identifier (e.g., "default.my_table").
        dsn: A string in DSN (Data Source Name) HTTP format
            (e.g., "clickhouse+http://username:password@host:8123/default").
            For more information, see `ClickHouse Connection String doc
            <https://clickhouse.com/docs/en/integrations/sql-clients/cli#connection_string>`_.
        overwrite: If True, any existing table with the same name will be dropped
            before writing new data. If False, the table will be created only if missing
            and new data appended otherwise.
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
    """

    _CREATE_TABLE_TEMPLATE = """
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns}
        )
        ENGINE = {engine}
        ORDER BY {order_by}
        {additional_props}
    """

    _DROP_TABLE_TEMPLATE = """DROP TABLE IF EXISTS {table_name}"""

    _CHECK_TABLE_EXISTS_TEMPLATE = """
        SELECT 1
        FROM system.tables
        WHERE database = '{db_name}' AND name = '{tbl_name}'
        LIMIT 1
    """

    _SHOW_CREATE_TABLE_TEMPLATE = """SHOW CREATE TABLE {table_name}"""

    def __init__(
        self,
        table: str,
        dsn: str,
        overwrite: bool = False,
        client_settings: Optional[Dict[str, Any]] = None,
        client_kwargs: Optional[Dict[str, Any]] = None,
        table_settings: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._table = table
        self._dsn = dsn
        self._overwrite = overwrite
        self._client_settings = client_settings or {}
        self._client_kwargs = client_kwargs or {}
        self._table_settings = table_settings or {}

    def _init_client(self):
        _check_import(self, module="clickhouse_connect", package="clickhouse-connect")
        import clickhouse_connect

        return clickhouse_connect.get_client(
            dsn=self._dsn,
            settings=self._client_settings or {},
            **self._client_kwargs or {},
        )

    def on_write_start(self) -> None:
        """
        Called on the driver before writing any blocks.

        If overwrite=True, we drop the table if it exists, capturing its ORDER BY.
        """
        if self._overwrite:
            client = None
            try:
                client = self._init_client()
                if (
                    self._table_exists(client, self._table)
                    and self._table_settings.get("order_by") is None
                ):
                    # Attempt to capture the existing ORDER BY
                    self._table_settings["order_by"] = self._get_existing_order_by(
                        client, self._table
                    )
                    logger.info(
                        f"Existing ORDER BY for table {self._table}: {self._table_settings.get('order_by')}"
                    )
                drop_sql = self._DROP_TABLE_TEMPLATE.format(table_name=self._table)
                client.command(drop_sql)
            except Exception as e:
                logger.warning(f"Failed to drop table {self._table}: {e}")
            finally:
                if client:
                    client.close()
        else:
            logger.debug(
                f"overwrite=False, will create {self._table} only if missing, then append."
            )

    def write(self, blocks: Iterable[Block], ctx: TaskContext) -> None:
        """
        Write the provided blocks into ClickHouse by launching remote tasks.
        """

        @cached_remote_fn
        def _write_single_block(
            block: Block,
            table: str,
            overwrite: bool,
            table_settings: Dict[str, Any],
            create_table_template: str,
        ):
            from ray.data.block import BlockAccessor

            client = self._init_client()
            try:
                arrow_table = BlockAccessor.for_block(block).to_arrow()
                need_to_create = overwrite
                if not overwrite:
                    if not self._table_exists(client, table):
                        need_to_create = True
                if need_to_create:
                    create_sql = _generate_create_table_sql(
                        arrow_table.schema,
                        table,
                        table_settings,
                        create_table_template,
                    )
                    client.command(create_sql)
                client.insert_arrow(table, arrow_table)
            except Exception as e:
                logger.warning(f"Failed to write block to table {table}: {e}")
                raise
            finally:
                client.close()

        def _generate_create_table_sql(
            schema: pyarrow.Schema,
            table_name: str,
            tbl_settings: Dict[str, Any],
            template: str,
        ) -> str:
            engine = tbl_settings.get("engine", "MergeTree()")
            order_by = tbl_settings.get("order_by")
            if order_by is None:
                order_by = _pick_best_arrow_field_for_order_by(schema)
            additional_clauses = []
            if "partition_by" in tbl_settings:
                additional_clauses.append(
                    f"PARTITION BY {tbl_settings['partition_by']}"
                )
            if "primary_key" in tbl_settings:
                additional_clauses.append(
                    f"PRIMARY KEY ({tbl_settings['primary_key']})"
                )
            if "settings" in tbl_settings:
                additional_clauses.append(f"SETTINGS {tbl_settings['settings']}")
            additional_props = ""
            if additional_clauses:
                additional_props = "\n" + "\n".join(additional_clauses)
            columns_def = []
            for field in schema:
                arrow_name = _arrow_type_name_local(field.type)
                ch_type = _arrow_to_clickhouse_type_local(arrow_name, field)
                columns_def.append(f"`{field.name}` {ch_type}")
            columns_str = ",\n    ".join(columns_def)
            create_sql = template.format(
                table_name=table_name,
                columns=columns_str,
                engine=engine,
                order_by=order_by,
                additional_props=additional_props,
            )
            return create_sql

        def _pick_best_arrow_field_for_order_by(schema: pyarrow.Schema) -> str:
            if len(schema) == 0:
                return "tuple()"
            for f in schema:
                if pat.is_timestamp(f.type):
                    return f.name
            for f in schema:
                if not (pat.is_string(f.type) or pat.is_large_string(f.type)):
                    return f.name
            return schema[0].name

        def _arrow_type_name_local(arrow_type: pyarrow.DataType) -> str:
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

        def _arrow_to_clickhouse_type_local(name: str, field: pyarrow.Field) -> str:
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

        tasks = []
        for block in blocks:
            tasks.append(
                _write_single_block.remote(
                    block,
                    self._table,
                    self._overwrite,
                    self._table_settings,
                    self._CREATE_TABLE_TEMPLATE,
                )
            )
        ray.get(tasks)

    def _table_exists(self, client, fq_table_name: str) -> bool:
        try:
            db_name, tbl_name = fq_table_name.split(".", 1)
            check_query = self._CHECK_TABLE_EXISTS_TEMPLATE.format(
                db_name=db_name, tbl_name=tbl_name
            )
            result = client.query(check_query)
            return bool(result and len(result.result_rows) > 0)
        except Exception as e:
            logger.warning(f"Could not verify if table {fq_table_name} exists: {e}")
            return False

    def _get_existing_order_by(self, client, fq_table_name: str) -> Optional[str]:
        try:
            show_create_sql = self._SHOW_CREATE_TABLE_TEMPLATE.format(
                table_name=fq_table_name
            )
            result = client.command(show_create_sql)
            ddl_str = str(result)
            idx = ddl_str.upper().find("ORDER BY")
            if idx == -1:
                return None
            after_order = ddl_str[idx + len("ORDER BY") :]
            engine_pos = after_order.upper().find("ENGINE")
            if engine_pos == -1:
                order_by_clause = after_order.strip()
            else:
                order_by_clause = after_order[:engine_pos].strip()
            return order_by_clause
        except Exception as e:
            logger.warning(
                f"Could not retrieve SHOW CREATE TABLE for {fq_table_name}: {e}"
            )
            return None
