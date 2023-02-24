import logging
from typing import Any, Dict, List,  TYPE_CHECKING

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
        from snowflake import connector
        connector.paramstyle='qmark'
        _snowflake_load_private_key(connect_properties)
        super().__init__(
            connector.connect,
            to_block_fn= lambda d: d if isinstance(d, pandas.DataFrame) else d.fetch_pandas_all(),
            from_block_fn= lambda block: BlockAccessor.for_block(block).to_pandas(), 
            paramstyle='qmark',
            **connect_properties
        )
            
    def query_batches(self, query:str, **kwargs) -> List['ResultBatch']:
        cursor = self.execute(query, **kwargs)
        batches = cursor.get_result_batches()
        batches = [b for b in batches if b.rowcount > 0]
        return batches
    
    def write_pandas(self, table: str, block: Block, *args, **kwargs) -> None:
        from snowflake.connector.pandas_tools import write_pandas
        _type = str(type(block))
        data = BlockAccessor.for_block(block).to_pandas()
        write_pandas(self.connection, data, *args, table_name=table, **{'parallel':1, **kwargs})    
    
    def read_batch(self, batch: 'ResultBatch', **kwargs) -> Block:
        return batch.to_pandas()    
            
@DeveloperAPI
class _SnowflakeReader(_DatabaseReader):                           
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
        query_all_resultbatch = DBAPI2Datasource.READ_QUERIES['read_direct'],
        read_resultbatch = 'call_fn(read_batch)',
        num_rows_resultbatch = DBAPI2Datasource.READ_QUERIES['num_rows_direct'],
        sample_resultbatch = DBAPI2Datasource.READ_QUERIES['sample_direct'],
    )
    
    WRITE_QUERIES = dict(
        write_writepandas= 'call_fn(write_pandas,{table})',       
    )
    
    def __init__(self, 
        connector: DatabaseConnector,
        read_queries: Dict[str, str] = {},
        write_queries: Dict[str, str] = {},
        template_keys: List[str] = []
    ):
        super().__init__(
            connector,
            read_modes = ['resultbatch', 'partitioned', 'direct'],
            write_modes = ['writepandas', 'direct', 'stage'],
            read_queries={**SnowflakeDatasource.READ_QUERIES, **read_queries},
            write_queries={**SnowflakeDatasource.WRITE_QUERIES, **write_queries},
            template_keys=template_keys
        )
    
    def _create_reader(self, mode:str, *args, **kwargs) -> _DatabaseReader:
        if mode == 'resultbatch':
            return _SnowflakeReader(mode, *args, **kwargs)
        else:
            return super()._create_reader(mode, *args, **kwargs)