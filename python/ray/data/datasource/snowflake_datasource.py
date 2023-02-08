import logging
from typing import Any, Dict, Iterable, List, Optional

from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.data.datasource.database_datasource import (
    DatabaseConnector,
    DatabaseReadTask,
    DatabaseReader,
    BlockFormat
)

from ray.data.datasource.dbapi2_datasource import (
    DBAPI2Connector,
    DBAPI2Datasource
)

logger = logging.getLogger(__name__)
   
def _snowflake_load_private_key(props: Dict) -> None:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

    private_key_file = props.get('private_key_file', None)
    private_key = props.get('private_key', None)
    
    if not private_key_file and not private_key:
        return
     
    password = props.get('pk_password', None)
    if isinstance(password, str):
        password = password.encode()
    
    pk_bytes = None
    if private_key:
        if isinstance(private_key, str):
            pk_bytes = bytes(private_key, "ascii")
        elif isinstance(private_key, bytes):
            pk_bytes = private_key
        elif not isinstance(private_key, RSAPrivateKey):
            raise ValueError('The private_key property must be a string, bytes or an RSAPrivateKey.')
            
    elif private_key_file:        
        del props['private_key_file']
        with open(private_key_file, "rb") as f:
            pk_bytes = f.read()

    if pk_bytes:
        pem = serialization.load_pem_private_key(
            pk_bytes, 
            password=password,
            backend=default_backend()
        )
            
        props['private_key'] = pem.private_bytes(
            serialization.Encoding.DER,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    
@DeveloperAPI
class SnowflakeConnector(DBAPI2Connector):
    from snowflake.connector.result_batch import ResultBatch
    
    def __init__(self, **connection_properties: Dict[str, Any]):
        from snowflake.connector import connect as snowflake_connect_fn
        _snowflake_load_private_key(connection_properties)
        super().__init__(snowflake_connect_fn, **{'paramstyle':'qmark', **connection_properties})
    
    def insert_block(self, query: str, block: Block, **query_args: Dict[str, Any]) -> None:
        from snowflake.connector.pandas_tools import write_pandas
        
        if logger.debug:
            query_args_str = ', ' + ','.join([f'{k}={v}' for k,v in query_args.items()]) if query_args else ''
            logger.debug(f'cursor.write_pandas(connection, df, table_name="{query}"{query_args_str}')

        accessor = BlockAccessor.for_block(block)
        df = accessor.to_batch_format('pandas')    
        write_pandas(self.connection, df, table_name=query, **{'parallel':1, **query_args})  # type: ignore
       
    def query_batches(self, query:str, **query_args: Dict[str, Any]) -> List[ResultBatch]:
        cursor = self.query(query, **query_args)
        batches = cursor.get_result_batches()
        batches = [b for b in batches if b.rowcount > 0]
        return batches        

@DeveloperAPI
class SnowflakeReadTask(DatabaseReadTask):
    from snowflake.connector.result_batch import ResultBatch
    
    def __init__(self, batch: ResultBatch, **kwargs):
        super().__init__(**kwargs)
        self.batch = batch
             
    def _read_fn(self) -> Iterable[Block]:
        return [self.batch.to_pandas()]                  

@DeveloperAPI
class SnowflakeReader(DatabaseReader): 
    def _create_read_task(self, **kwargs) -> DatabaseReadTask:
        return SnowflakeReadTask(
            connector=self.connector, 
            queries=self.queries, 
            query_args=self.query_args, 
            **kwargs
        )
                           
    def get_read_tasks(self, parallelism: int) -> List[SnowflakeReadTask]:               
        if self.num_rows == 0:
            return []
        
        # read batches
        if 'read_query' not in self.queries:
            raise ValueError('Snowflake reader requires a read query to be defined')
        
        with self.connector:
            batches = self.connector.query_batches( # type: ignore 
                str(self.queries['read_query']), 
                **self.query_args
            )

        accessor = BlockAccessor.for_block(self.sample)
        schema = accessor.schema()
        row_size = accessor.size_bytes() / accessor.num_rows()
        
        # create tasks
        tasks = []
        for batch in batches:
            if batch.uncompressed_size:
                size = batch.uncompressed_size
            else:
                size = int(row_size * batch.rowcount)
                
            metadata = BlockMetadata(
                batch.rowcount, 
                size, 
                schema, 
                None, 
                None
            ) 
            tasks.append(self._create_read_task(metadata=metadata, batch=batch))   

        return tasks
         
@PublicAPI   
class SnowflakeDatasource(DBAPI2Datasource):
    """A Ray datasource for reading and writing data to Snowflake.
        See [Snowflake connector API](https://docs.snowflake.com/en/user-guide/python-connector-api.html#module-snowflake-connector).
    """ 
    
    def __init__(self, 
        connector: DatabaseConnector,
        read_queries: Dict[str, str] = {},
        write_queries: Dict[str, str] = {},
        read_mode: Optional[str] = None,
        write_mode: Optional[str] = 'batch'
    ):
        super().__init__(
                connector, 
                read_queries,
                {'write_query_batch':'{table}', **write_queries},
                read_mode,
                write_mode
            )
    
    def _create_reader(self, **kwargs) -> DatabaseReader:
        return SnowflakeReader(**kwargs)