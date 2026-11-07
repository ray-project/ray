import re
from typing import Any, Dict, Iterable, Optional, Tuple

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink

# A Snowflake identifier is either double-quoted, with `""` escaping a quote, or
# unquoted, in which case it can't contain dots or quotes.
_IDENTIFIER = r'"(?:[^"]|"")+"|[^".]+'
_OBJECT_NAME_PATTERN = re.compile(
    rf"({_IDENTIFIER})(?:\.({_IDENTIFIER}))?(?:\.({_IDENTIFIER}))?"
)


class SnowflakeDatasink(Datasink[None]):
    """Writes blocks to a Snowflake table with ``write_pandas``.

    ``write_pandas`` stages the data as Parquet files and loads them with
    ``COPY INTO``. It creates the table if it doesn't exist.
    """

    def __init__(self, table: str, connection_parameters: Dict[str, Any]):
        _check_import(self, module="snowflake", package="snowflake-connector-python")

        self._database, self._schema, self._table = _parse_object_name(table)
        self._connection_parameters = connection_parameters

    def write(self, blocks: Iterable[Block], ctx: TaskContext) -> None:
        from snowflake.connector import connect
        from snowflake.connector.pandas_tools import write_pandas

        builder = DelegatingBlockBuilder()
        for block in blocks:
            builder.add_block(block)
        block_accessor = BlockAccessor.for_block(builder.build())
        if block_accessor.num_rows() == 0:
            return

        with connect(**self._connection_parameters) as connection:
            write_pandas(
                connection,
                block_accessor.to_pandas(),
                self._table,
                database=self._database,
                schema=self._schema,
                auto_create_table=True,
            )


def _parse_object_name(name: str) -> Tuple[Optional[str], Optional[str], str]:
    """Split ``[database.][schema.]table`` and resolve each part like Snowflake.

    Snowflake stores unquoted identifiers in uppercase and matches them without
    regard to case, so this function uppercases them. Double-quoted identifiers
    are case-sensitive, so it keeps them as written, without the quotes.
    ``write_pandas`` quotes every part it receives. Parts that aren't given are
    ``None``, and ``write_pandas`` uses the connection's database and schema.

    Args:
        name: The table name, optionally qualified by a schema and database.

    Returns:
        A three-tuple of the database, schema and table.
    """
    match = _OBJECT_NAME_PATTERN.fullmatch(name)
    if match is None:
        raise ValueError(
            f"Invalid Snowflake table name: {name!r}. Expected 'table', "
            "'schema.table' or 'database.schema.table'. To learn more, read "
            "https://docs.snowflake.com/en/sql-reference/name-resolution."
        )

    parts = [_resolve_identifier(part) for part in match.groups() if part is not None]
    database, schema = None, None
    if len(parts) == 3:
        database, schema, table = parts
    elif len(parts) == 2:
        schema, table = parts
    else:
        (table,) = parts
    return database, schema, table


def _resolve_identifier(identifier: str) -> str:
    if identifier.startswith('"'):
        return identifier[1:-1].replace('""', '"')
    return identifier.upper()
