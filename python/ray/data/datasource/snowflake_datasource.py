import logging
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.data.datasource.database_datasource import (
    DatabaseConnector,
    DatabaseReadTask,
    _DatabaseReader
)

from ray.data.datasource.dbapi2_datasource import (
    DBAPI2Connector,
    DBAPI2Datasource
)

import pandas

if TYPE_CHECKING:
    from snowflake.connector.result_batch import ResultBatch

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
    def __init__(self, **connect_properties):
        from snowflake.connector import connect as snowflake_connect_fn
        _snowflake_load_private_key(connect_properties)
        super().__init__(
            snowflake_connect_fn,
            to_block_fn= lambda d: d if isinstance(d, pandas.DataFrame) else d.fetch_pandas_all(),
            from_block_fn= lambda block: BlockAccessor.for_block(block).to_pandas(), 
            **{'paramstyle':'qmark', **connect_properties}
        )
    
    def _execute(self, query: str, data: Optional[Any] = None, **query_kwargs) -> Any:        
        from snowflake.connector.pandas_tools import write_pandas
        if data is not None and isinstance(data, pandas.DataFrame):
            write_pandas(self.connection, data, table_name=query, **{'parallel':1, **query_kwargs})  # type: ignore
            return None
        else:
            return super()._execute(query, data=data, )
            
    def query_batches(self, query:str, **kwargs) -> List['ResultBatch']:
        cursor = self.execute(query, **kwargs)
        batches = cursor.get_result_batches()
        batches = [b for b in batches if b.rowcount > 0]
        return batches
    
    def read_batch(self, batch: 'ResultBatch', **kwargs) -> Block:
        return batch.to_pandas()    
            
@DeveloperAPI
class SnowflakeReader(_DatabaseReader):                           
    def get_read_tasks(self, parallelism: int) -> List[DatabaseReadTask]:               
        if self.num_rows == 0:
            return []
        
        # read batches
        query = self.queries.get('query_all')
        if not query:
            raise ValueError('Snowflake reader requires a read query to be defined')
        
        with self.connector as con:
            batches = con.query_batches(query, **self.query_kwargs)

        accessor = BlockAccessor.for_block(self.sample)
        schema = accessor.schema()
        row_size = accessor.size_bytes() / accessor.num_rows()
        row_start = 0
        
        # create tasks
        tasks = []
        for i, batch in enumerate(batches):
            num_rows = batch.rowcount
            size_bytes = int(row_size * num_rows)            
            metadata = BlockMetadata(num_rows, size_bytes, schema, None, None)    
          
            queries = self.queries.templatize(
                metadata = metadata,
                partition = i,
                row_start=row_start, 
                num_rows=num_rows
            )
            
            tasks.append(
                DatabaseReadTask(
                    self.connector,
                    metadata, 
                    queries,
                    {**self.query_kwargs, 'batch': batch}
                )
            )  
            row_start += num_rows
            
        return tasks
         
@PublicAPI   
class SnowflakeDatasource(DBAPI2Datasource):
    """A Ray datasource for reading and writing data to Snowflake.
        See [Snowflake connector API](https://docs.snowflake.com/en/user-guide/python-connector-api.html#module-snowflake-connector).
    """ 
    READ_QUERIES = dict(
        query_all_batch = DBAPI2Datasource.READ_QUERIES['read_direct'],
        read_batch = 'call_fn(read_batch)',
        num_rows_batch = DBAPI2Datasource.READ_QUERIES['num_rows_direct'],
        sample_batch = DBAPI2Datasource.READ_QUERIES['sample_direct'],
    )
    
    def __init__(self, 
        connector: DatabaseConnector,
        *,
        read_queries: Dict[str, str] = {},
        write_queries: Dict[str, str] = {},
        template_keys: List[str] = []
    ):
        super().__init__(
            connector, 
            read_queries={**SnowflakeDatasource.READ_QUERIES, **read_queries},
            write_queries=write_queries,
            template_keys=template_keys
        )
    
    def create_reader(self, mode: str = 'batch', **kwargs) -> _DatabaseReader:
        if mode == 'batch':
            template_kwargs = self._get_template_kwargs(**kwargs)
            query_kwargs = self._get_query_kwargs(**kwargs)       
            queries = self.read_queries.templatize(mode=mode, **template_kwargs)            
            return SnowflakeReader(self.connector, queries, query_kwargs)
        else:
            return _DatabaseReader(self.connector, queries, query_kwargs)