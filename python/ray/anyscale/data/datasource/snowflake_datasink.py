from typing import Any, Dict, Iterable, Optional, Tuple

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.util import _check_import, call_with_retry
from ray.data.block import Block, BlockAccessor
from ray.data.datasource import Datasink


class SnowflakeDatasink(Datasink):
    def __init__(self, table: str, connection_parameters: Dict[str, Any]):
        _check_import(self, module="snowflake", package="snowflake-connector-python")

        self._table = table
        self._connection_parameters = connection_parameters

    def write(self, blocks: Iterable[Block], ctx: TaskContext) -> None:
        from snowflake.connector import connect
        from snowflake.connector.pandas_tools import write_pandas

        builder = DelegatingBlockBuilder()
        for block in blocks:
            builder.add_block(block)
        block = builder.build()

        database, schema, table = _resolve_object_name(self._table)
        if database is None:
            database = self._connection_parameters.get("database", None)
        if schema is None:
            schema = self._connection_parameters.get("schema", None)

        df = BlockAccessor.for_block(block).to_pandas()
        with connect(**self._connection_parameters) as connection:
            call_with_retry(
                lambda: write_pandas(
                    connection,
                    df,
                    table,
                    database=database,
                    schema=schema,
                    auto_create_table=True,
                ),
                description="write block to Snowflake",
            )


def _resolve_object_name(name) -> Tuple[Optional[str], Optional[str], str]:
    num_dots = sum(c == "." for c in name)
    if num_dots == 0:
        database = None
        schema = None
        table = name
    elif num_dots == 1:
        database = None
        schema, table = name.split(".")
    elif num_dots == 2:
        database, schema, table = name.split(".")
    else:
        raise ValueError(
            "Expected schema object name to contain at most two dots ('.'),"
            f"but got {num_dots}. Check that the provided name is valid. To learn more,"
            "read https://docs.snowflake.com/en/sql-reference/name-resolution."
        )
    return database, schema, table
