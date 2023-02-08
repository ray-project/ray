from typing import Any, Callable, Dict, Generic, List, Optional
import logging
from ray.data.block import Block,  BlockAccessor
from ray.util.annotations import DeveloperAPI, PublicAPI
import pandas

from ray.data.datasource.database_datasource import (
    DatabaseConnector, 
    DatabaseDatasource,
    DatabaseConnection, 
    BlockFormat, 
    QueryResult
)

logger = logging.getLogger(__name__)
   
@DeveloperAPI
class DBAPI2Connector(DatabaseConnector[DatabaseConnection]):
    """ Generic Python DB API 2 connector that creates a DB connections in remote ray tasks. The connector implements the  DB operations
    that can be used by readers and writers when interacting with the database.

    Attributes:
        connection: The DB API connection to the database.
        connect_fn: The native function used to connect to the database.
        connection_props: The connection args to be passed to the connect function
    """
    def __init__(self, 
        connect_fn: Callable[..., DatabaseConnection], 
        **connection_properties
    ):     
        """ 
        Constructor for the DBAPI2 Connector.
        Args:
            open_fn (Callable[..., DatabaseConnection]): The DB API connect method specific to the DB
            connection_props (Optional[Properties]): The connection args to be passed to the connect function
        """
        super().__init__(**connection_properties)
        self.connect_fn = connect_fn   
    
    def _open(self) -> DatabaseConnection:
        return self.connect_fn(**self.connection_properties)
    
    def _commit(self) -> None:
        self.connection.commit() # type: ignore  
  
    def _rollback(self) -> None:
        self.connection.rollback() # type: ignore  
        
    def _close(self) -> None:
        self.connection.close()  # type: ignore  
          
    def query(self, query: str, data: Optional[Any]=None, **query_args) -> Any:
        queries = query.split(';')
        for q in queries:
            if 'insert' in q.lower():
                ret = self._execute(q, data=data, **query_args)
            else:
                ret = self._execute(q, **query_args)
        return ret
    
    def query_value(self, query: str, **query_args: Dict[str, Any]) -> QueryResult:
        results = self._execute(query, **query_args).fetchone()
        return results[0] if results else None
    
    def _execute(self, query: str, data: Optional[Any]=None, args: List = [], **query_args: Dict[str, Any]):
        if not self.connection:
            raise ValueError(f'cannot execute "{query}" connection not open')
        
        if logger.debug:
            many = 'many' if data else ''
            query_args_str = ', ' + ','.join([f'{k}={v}' for k,v in query_args.items()]) if query_args else ''
            log_str = f'cursor.execute{many}("{query}"{query_args_str}'
            logger.debug(log_str)
        
        try:
            cursor = self.connection.cursor()  # type: ignore  
            if data:
                cursor.executemany(query, data, *args, **query_args)
            else:
                cursor.execute(query, *args, **query_args)
        except BaseException as e:
            logger.error(f'cannot execute {query}', exc_info=True)
            raise e

        return cursor
        
    def query_block(self, query: str, **query_args: Dict[str, Any]) -> Block:
        cursor = self._execute(query, **query_args)
        results = cursor.fetchall()
        if results:
            columns = [col_desc[0] for col_desc in cursor.description]
            df = pandas.DataFrame(columns=columns, data=results)
            return df
            
        else:
            return None
    
    def insert_block(self, query: str, block: Block, **query_args) -> None:
        accessor = BlockAccessor.for_block(block)
        table = accessor.to_arrow()
        pydict = table.to_pydict()
        inc_columns = table.schema.names

        data = [[pydict[c][r] for c in inc_columns] for r in range(table.num_rows)]
        self._execute(query, data=data, **query_args)
              
@PublicAPI(stability='alpha')
class DBAPI2Datasource(DatabaseDatasource, Generic[DatabaseConnection]):
    """A Ray datasource for reading and writing to database tables with a DB API 2 compliant library.
    
    To create a DBAPI2 reader for your database, call the connector constructor and pass the 
    open function and the connection properties. Then create a datasource with the connector.
    >>> connector = DBAPI2Connector(connect_fn, connect_prop_1=?, connect_prop_2=?, ...)
    >>> datasource = DBAPI2Datasource(connector)
    
    To read a table, call ray.data.read_datasource with the datasource and table name:
    >>> dataset = read_datasource(datasource, table='my_table')
    
    To read using a subquery, call ray.data.read_datasource with the datasource and the subquery:
    >>> dataset = read_datasource(datasource, subquery='select * from my_table')

    To write to a table directly, call dataset.write_datasource with the datasource and the table name:
    >>> dataset.write_datasource(datasource, table='my_table')
    
    To write to staging tables per block, and then write all stages to a destination,
    call dataset.write_datasource with the datasource, the table name and specify write_mode='stage':
    >>> dataset.write_datasource(datasource, table='my_table', write_mode='stage')
    
    Attributes:
        connector (DatabaseConnector): The connector that is used for accessing the database.
     """
    
    def __init__(self,
        connector:  DatabaseConnector,
        read_queries: Dict[str, str] = {},
        write_queries: Dict[str, str] = {},
        read_mode:  Optional[str] = 'partitioned',
        write_mode:  Optional[str] = None
    ):                         
        # default read queries
        NUM_ROWS_QUERY = 'SELECT COUNT(*) FROM ({table_or_query})'
        SAMPLE_QUERY = 'SELECT * FROM ({table_or_query}) LIMIT 100'
        READ_QUERY = 'SELECT * FROM ({table_or_query})'
        PARTITIONED_READ_QUERY = READ_QUERY + ' LIMIT {num_rows} OFFSET {row_start}'
        default_read_queries = dict(
            read_query = READ_QUERY,
            num_rows_query = NUM_ROWS_QUERY,
            sample_query = SAMPLE_QUERY,
            read_query_partitioned = PARTITIONED_READ_QUERY,
            num_rows_query_partitioned = NUM_ROWS_QUERY,
            sample_query_partitioned = SAMPLE_QUERY
        )         

        # default write queries
        default_write_queries = dict(
            write_query = 'INSERT INTO {table} ({column_list}) VALUES ({qmark_list})',
            write_query_staged = 'INSERT INTO {table}_stage_{block_id} ({column_list}) VALUES ({qmark_list})',
            write_prepare_query_staged = 'CREATE OR REPLACE TABLE {table}_stage_{block_id} LIKE {table}',
            on_write_complete_query_staged = '''
                INSERT INTO {table} ({column_list}) SELECT {column_list} FROM {table}_stage_{block_id}; 
                DROP TABLE IF EXISTS {table}_stage_{block_id}
            '''
        )
        
        super().__init__(
            connector, 
            {**default_read_queries, **read_queries},
            {**default_write_queries, **write_queries},
            read_mode,
            write_mode
        )
    
    def _get_template_args(self, 
        table: Optional[str] = None, 
        query: Optional[str] = None,
        **kwargs: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not (query or table):
            raise ValueError('Missing one of either query or table.')
        elif query and table:
            raise ValueError('Specify only the query or table, not both values.')
        
        return dict(
            table=table,
            query=query,
            table_or_query= table or query,
        )
        
    def _get_query_args(self, 
        table: Optional[str] = None, 
        query: Optional[str] = None,
        **kwargs: Dict[str, Any]
    ) -> Dict[str, Any]:
        return kwargs