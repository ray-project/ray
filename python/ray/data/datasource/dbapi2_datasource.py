from typing import Any, Callable, Dict, List, Optional
from ray.data.block import Block
from ray.util.annotations import DeveloperAPI, PublicAPI

from ray.data.datasource.database_datasource import (
    DatabaseConnector,
    DatabaseDatasource,
    DatabaseConnection,
    pylist_to_pandas,
    pylist_to_pyarrow,
    block_to_pylist,
)


def cursor_to_pyvalue(cursor: Any) -> Any:
    results = cursor.fetchone()
    return results[0] if results else None


def cursor_to_pandas(cursor: Any) -> Block:
    results = cursor.fetchall()
    if results:
        columns = [col_desc[0] for col_desc in cursor.description]
        return pylist_to_pandas(results, columns)
    else:
        return None


def cursor_to_pyarrow(cursor: Any) -> Block:
    results = cursor.fetchall()
    if results:
        columns = [col_desc[0] for col_desc in cursor.description]
        return pylist_to_pyarrow(results, columns)
    else:
        return None


@DeveloperAPI
class DBAPI2Connector(DatabaseConnector):
    """Generic Python DB API 2 connector that creates a DB connections in remote ray tasks.
    The connector implements the  DB operations that can be used by readers and writers
    when interacting with the database.

    Attributes:
        connection: The DB API connection to the database.
        connect_fn: The native function used to connect to the database.
        connection_props: The connection args to be passed to the connect function
    """

    def __init__(
        self,
        connect_fn: Callable[..., DatabaseConnection],
        to_value_fn: Callable[[Any], Any] = cursor_to_pyvalue,
        to_block_fn: Callable[[Any], Block] = cursor_to_pyarrow,
        from_block_fn: Callable[[Block], Any] = block_to_pylist,
        **connect_properties,
    ):
        """
        Constructor for the DBAPI2 Connector.
        Args:
            open_fn (Callable[..., DatabaseConnection]): The DB API connect method
                specific to the DB.
            connection_props (Optional[Properties]): The connection args to be passed
                to the connect function
        """
        self.connect_fn = connect_fn
        super().__init__(to_value_fn, to_block_fn, from_block_fn, **connect_properties)

    def _open(self) -> DatabaseConnection:
        return self.connect_fn(**self.connect_properties)

    def _commit(self) -> None:
        self.connection.commit()  # type: ignore

    def _rollback(self) -> None:
        self.connection.rollback()  # type: ignore

    def _close(self) -> None:
        self.connection.close()  # type: ignore

    def _execute(self, query: str, *args, block: Optional[Any] = None, **kwargs) -> Any:
        cursor = self.connection.cursor()  # type: ignore
        if block:
            cursor.executemany(query, block, *args, **kwargs)
        else:
            queries = query.split(";")
            for q in queries:
                cursor.execute(q, *args, **kwargs)

        return cursor


@PublicAPI(stability="alpha")
class DBAPI2Datasource(DatabaseDatasource):
    """A Ray datasource for database operations with a Python DB API 2 library.

    To create a DBAPI2 reader for your database, call the connector constructor
    and pass the open function and the connection properties. Then create a
    datasource with the connector.
    >>> connect_prop = {...}
    >>> connector = DBAPI2Connector(connect_fn, connect_prop, ...)
    >>> datasource = DBAPI2Datasource(connector)

    To read a table, call ray.data.read_datasource with the datasource and
    table name:
    >>> dataset = read_datasource(datasource, table='my_table')

    To read using a subquery, call ray.data.read_datasource with the datasource
    and the subquery:
    >>> dataset = read_datasource(datasource, subquery='select * from my_table')

    To write to a table directly, call dataset.write_datasource with the
    datasource and the table name:
    >>> dataset.write_datasource(datasource, table='my_table')

    To write to staging tables per block, and then write all stages
    to a destination call dataset.write_datasource with the datasource,
    the table name and specify write_mode='stage':
    >>> dataset.write_datasource(datasource, table='my_table', write_mode='stage')

    Attributes:
        connector: The connector that is used for accessing
            the database.
    """

    READ_QUERIES = dict(
        # read_mode set to 'direct'
        read_direct="SELECT * FROM ({table_or_query})",
        num_rows_direct="SELECT COUNT(*) FROM ({table_or_query})",
        sample_direct="SELECT * FROM ({table_or_query}) LIMIT 100",
        # read_mode set to 'partition'
        read_partition="SELECT * FROM ({table_or_query}) "
        + "LIMIT {num_rows} OFFSET {row_start}",
        num_rows_partition="SELECT COUNT(*) FROM ({table_or_query})",
        sample_partition="SELECT * FROM ({table_or_query}) LIMIT 100",
    )

    WRITE_QUERIES = dict(
        # write_mode set to 'direct'
        write_direct="INSERT INTO {table} ({column_list}) " + "VALUES ({param_list})",
        # write mode set to 'stage'
        prepare_stage="CREATE TABLE IF NOT EXISTS {table}_stage_{partition} "
        + "AS SELECT * FROM {table} LIMIT 0",
        write_stage="INSERT INTO {table}_stage_{partition} ({column_list}) "
        + "VALUES ({param_list})",
        complete_stage="INSERT INTO {table} ({column_list}) "
        + "SELECT {column_list} FROM {table}_stage_{partition}",
        cleanup_stage="DROP TABLE IF EXISTS {table}_stage_{partition}",
    )

    def __init__(
        self,
        connector: DatabaseConnector,
        read_modes: List[str] = None,
        write_modes: List[str] = None,
        read_queries: Optional[Dict[str, str]] = None,
        write_queries: Optional[Dict[str, str]] = None,
        template_keys: Optional[List[str]] = None,
    ):
        super().__init__(
            connector,
            read_modes=read_modes or ["partition", "direct"],
            write_modes=write_modes or ["direct", "stage"],
            read_queries={**DBAPI2Datasource.READ_QUERIES, **read_queries},
            write_queries={**DBAPI2Datasource.WRITE_QUERIES, **write_queries},
            template_keys=["table", "query", "table_or_query"] + template_keys,
        )

    def _get_template_kwargs(
        self,
        table: Optional[str] = None,
        query: Optional[str] = None,
        **kwargs: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not (query or table):
            raise ValueError("Missing one of either query or table.")
        elif query and table:
            raise ValueError("Specify only the query or table, not both values.")

        return super()._get_template_kwargs(
            table=table, query=query, table_or_query=table or query, **kwargs
        )
