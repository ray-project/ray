import re
from typing import Any, Dict, Iterable, NamedTuple, Optional

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink

# A Snowflake identifier is either double-quoted, with `""` escaping a quote and
# anything else allowed between the quotes, or unquoted, in which case it starts
# with a letter or an underscore and continues with letters, digits, underscores
# or dollar signs. Unquoted names have to be checked against that rule rather
# than just split on dots: `write_pandas` double-quotes every part it's given, so
# a name Snowflake would reject unquoted, like `my-table`, would otherwise become
# the perfectly valid but different `"MY-TABLE"`, which `auto_create_table`
# happily creates. To learn more, read
# https://docs.snowflake.com/en/sql-reference/identifiers-syntax.
_QUOTED_IDENTIFIER = r'"(?:[^"]|"")+"'
_UNQUOTED_IDENTIFIER = r"[A-Za-z_][A-Za-z0-9_$]*"
_IDENTIFIER = rf"{_QUOTED_IDENTIFIER}|{_UNQUOTED_IDENTIFIER}"
_OBJECT_NAME_PATTERN = re.compile(
    rf"({_IDENTIFIER})(?:\.({_IDENTIFIER}))?(?:\.({_IDENTIFIER}))?"
)


class _ObjectName(NamedTuple):
    """The resolved parts of a Snowflake object name.

    ``database`` and ``schema`` are ``None`` if the name doesn't qualify the
    table, in which case ``write_pandas`` uses the connection's database and
    schema.
    """

    database: Optional[str]
    schema: Optional[str]
    table: str


class SnowflakeDatasink(Datasink[None]):
    """Writes blocks to a Snowflake table with ``write_pandas``.

    ``write_pandas`` stages the data as Parquet files and loads them with
    ``COPY INTO``. It creates the table if it doesn't exist.
    """

    def __init__(self, table: str, connection_parameters: Dict[str, Any]):
        _check_import(self, module="snowflake", package="snowflake-connector-python")

        self._object_name = _parse_object_name(table)
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
                self._object_name.table,
                database=self._object_name.database,
                schema=self._object_name.schema,
                auto_create_table=True,
                # Ray Data's blocks are Arrow-backed, so a timezone-aware timestamp
                # reaches `write_pandas` as a `pd.ArrowDtype` column. Without logical
                # types in the staged Parquet, `write_pandas` writes those to
                # Snowflake incorrectly, and its own warning about it doesn't fire
                # because it tests `is_datetime64tz_dtype`, which is False for
                # `ArrowDtype`. To learn more, read
                # https://github.com/snowflakedb/snowflake-connector-python/issues/1687.
                use_logical_type=True,
            )


def _parse_object_name(name: str) -> _ObjectName:
    """Split ``[database.][schema.]table`` and resolve each part like Snowflake.

    Snowflake stores unquoted identifiers in uppercase and matches them without
    regard to case, so this function uppercases them. Double-quoted identifiers
    are case-sensitive, so it keeps them as written, without the quotes.
    ``write_pandas`` quotes every part it receives.

    Args:
        name: The table name, optionally qualified by a schema and database.

    Returns:
        The resolved database, schema and table.
    """
    match = _OBJECT_NAME_PATTERN.fullmatch(name)
    if match is None:
        raise ValueError(
            f"Invalid Snowflake table name: {name!r}. Expected 'table', "
            "'schema.table' or 'database.schema.table'. Double-quote any part "
            "that starts with a digit or contains characters other than "
            "letters, digits, '_' and '$'. To learn more, read "
            "https://docs.snowflake.com/en/sql-reference/identifiers-syntax."
        )

    parts = [_resolve_identifier(part) for part in match.groups() if part is not None]
    database, schema = None, None
    if len(parts) == 3:
        database, schema, table = parts
    elif len(parts) == 2:
        schema, table = parts
    else:
        (table,) = parts
    return _ObjectName(database, schema, table)


def _resolve_identifier(identifier: str) -> str:
    if identifier.startswith('"'):
        return identifier[1:-1].replace('""', '"')
    return identifier.upper()
