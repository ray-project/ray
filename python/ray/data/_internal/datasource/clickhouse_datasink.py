import logging
from typing import (
    Iterable,
    Optional,
    Dict,
    Any,
    List,
    Literal,
)

import pyarrow
import pyarrow.types as pat

import ray
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.util import _check_import
from ray.data.block import Block
from ray.data.datasource.datasink import Datasink
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


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
        mode: One of "create", "append", or "overwrite":
            "create": Create a new table; fail if it already exists.
            "append": Use an existing table if present, otherwise create one;
            data will be appended to the table.
            "overwrite": Drop an existing table (if any) and re-create it.
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
        mode: Literal["create", "append", "overwrite"] = "create",
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

    @property
    def supports_distributed_writes(self) -> bool:
        return True

    def _init_client(self):
        _check_import(self, module="clickhouse_connect", package="clickhouse-connect")
        import clickhouse_connect

        return clickhouse_connect.get_client(
            dsn=self._dsn,
            settings=self._client_settings or {},
            **self._client_kwargs or {},
        )

    def on_write_start(self) -> None:
        client = None
        try:
            client = self._init_client()
            table_exists = self._table_exists(client, self._table)

            if self._mode == "overwrite":
                drop_sql = self._DROP_TABLE_TEMPLATE.format(table_name=self._table)
                logger.info(f"Mode=overwrite => {drop_sql}")
                client.command(drop_sql)
                # If the old table existed and we don't have an order_by set,
                # adopt the old ORDER BY from the existing table.
                if table_exists and self._table_settings.get("order_by") is None:
                    existing_order_by = self._get_existing_order_by(client, self._table)
                    if existing_order_by is not None:
                        self._table_settings["order_by"] = existing_order_by
                        logger.info(
                            f"Using existing ORDER BY from overwritten table: {existing_order_by}"
                        )

            elif self._mode == "create":
                if table_exists:
                    msg = (
                        f"Table {self._table} already exists in mode='create'. "
                        "Use mode='append' or 'overwrite' instead."
                    )
                    logger.error(msg)
                    raise RuntimeError(msg)

            elif self._mode == "append":
                if table_exists and self._table_settings.get("order_by") is None:
                    existing_order_by = self._get_existing_order_by(client, self._table)
                    if existing_order_by is not None:
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

    def write(self, blocks: Iterable[Block], ctx: TaskContext) -> List[int]:
        @cached_remote_fn
        def _write_single_block(
            block: Block,
            table: str,
            mode: str,
            table_settings: Dict[str, Any],
            create_table_template: str,
            max_insert_block_rows: Optional[int],
            dsn: str,
            client_settings: Dict[str, Any],
            client_kwargs: Dict[str, Any],
        ):
            from ray.data.block import BlockAccessor

            _check_import(
                None, module="clickhouse_connect", package="clickhouse-connect"
            )
            import clickhouse_connect

            def _table_exists(local_client, fq_table_name: str) -> bool:
                db_name, tbl_name = fq_table_name.split(".", 1)
                check_query = (
                    f"SELECT 1 FROM system.tables WHERE database = '{db_name}' "
                    f"AND name = '{tbl_name}' LIMIT 1"
                )
                try:
                    result = local_client.query(check_query)
                    return bool(result and len(result.result_rows) > 0)
                except:
                    return False

            def _generate_create_table_sql(
                schema: pyarrow.Schema,
                table_name: str,
                tbl_settings: Dict[str, Any],
                template: str,
            ) -> str:
                engine = tbl_settings.get("engine", "MergeTree()")
                order_by = tbl_settings.get(
                    "order_by", _pick_best_arrow_field_for_order_by(schema)
                )
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
                return template.format(
                    table_name=table_name,
                    columns=columns_str,
                    engine=engine,
                    order_by=order_by,
                    additional_props=additional_props,
                )

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

            local_client = clickhouse_connect.get_client(
                dsn=dsn,
                settings=client_settings,
                **client_kwargs,
            )
            try:
                arrow_table = BlockAccessor.for_block(block).to_arrow()
                row_count = arrow_table.num_rows
                create_needed = False
                if mode in ("overwrite", "create"):
                    # For "overwrite", we already dropped table in on_write_start(),
                    # so it won't exist now. We'll always create a new table.
                    create_needed = True
                elif mode == "append":
                    # If table doesn't exist, create it
                    if not _table_exists(local_client, table):
                        create_needed = True
                if create_needed:
                    create_sql = _generate_create_table_sql(
                        arrow_table.schema,
                        table,
                        table_settings,
                        create_table_template,
                    )
                    local_client.command(create_sql)
                # Possibly chunk the arrow table if it's large
                if max_insert_block_rows and row_count > max_insert_block_rows:
                    total_inserted = 0
                    offset = 0
                    while offset < row_count:
                        slice_end = min(offset + max_insert_block_rows, row_count)
                        chunk = arrow_table.slice(offset, slice_end - offset)
                        local_client.insert_arrow(table, chunk)
                        total_inserted += chunk.num_rows
                        offset = slice_end
                    return total_inserted
                else:
                    local_client.insert_arrow(table, arrow_table)
                    return row_count
            except Exception as e:
                logger.warning(f"Failed to write block to table {table}: {e}")
                raise
            finally:
                local_client.close()

        tasks = []
        for block in blocks:
            tasks.append(
                _write_single_block.remote(
                    block,
                    self._table,
                    self._mode,
                    self._table_settings,
                    self._CREATE_TABLE_TEMPLATE,
                    self._max_insert_block_rows,
                    self._dsn,
                    self._client_settings,
                    self._client_kwargs,
                )
            )
        return ray.get(tasks)

    def on_write_complete(self, write_results) -> None:
        if hasattr(write_results, "write_returns"):
            raw_results = write_results.write_returns
        else:
            raw_results = write_results
        if not raw_results:
            return
        all_row_counts = []
        for sublist in raw_results:
            all_row_counts.extend(sublist)
        total_inserted = sum(all_row_counts)
        logger.info(
            f"ClickHouseDatasink on_write_complete: inserted {total_inserted} total rows "
            f"into {self._table}."
        )

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
