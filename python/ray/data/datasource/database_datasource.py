from abc import ABC, abstractmethod
from collections import defaultdict
import dataclasses
from functools import cached_property
from math import ceil
from typing import Any, Dict, Generic, Iterable, List, Literal, Optional, TypeVar, Union
import logging
import ray
from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata, BlockExecStats, BlockAccessor
from ray.data.datasource import Reader, Datasource, ReadTask
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.data._internal.remote_fn import cached_remote_fn

from pyarrow import Table
from pandas import DataFrame

logger = logging.getLogger(__name__)

QueryResult = Optional[Union[None, int, str, bool, float, bytes]]

BlockFormat = Literal['pyarrow', 'pandas', 'native']
DatabaseConnection = TypeVar("DatabaseConnection")

def _transpose_list(lst: List[List[Any]]) -> List[List[Any]]: 
     return list(map(list, zip(*lst)))
  
def _list_to_column_map(lst: List[List[Any]], columns: List[str]) -> Dict[str,List[Any]]: 
    return {c:d for c,d in zip(columns, _transpose_list(lst))}

def _list_to_records(lst: List[List[Any]], columns: List[str]) -> List[Dict[str, Any]]:
    return [{c:v for c,v in zip(columns, row)} for row in lst]
 
def _to_block(
    block: Union[Block, dict], 
    columns: Optional[List[str]] = None,
    format:BlockFormat = 'pyarrow'
) -> Block:
    
    if isinstance(block, list):
        if not columns:
            raise ValueError('native format requires columns to be specified')

        if format == 'native':
             return _list_to_records(block, columns)
        else:                
            column_map = _list_to_column_map(block, columns)  
            if format == 'pandas':
                return DataFrame.from_dict(column_map)
            elif format == 'pyarrow':
                return Table.from_pydict(column_map)
            elif format is None:
                raise ValueError('format cannot be None.')
            else:
                raise ValueError(f'unknown format '+format)
    else:
        accessor = BlockAccessor.for_block(block)
        return getattr(accessor, f'to_{format}')()        
            
def _get_columns(metadata):
    if metadata.schema:
        return metadata.schema.names
    else:
        raise ValueError('cannot get columns as metadata is missing schema')

@DeveloperAPI
class DefaultArgs(defaultdict):  
    def __missing__(self, key):
        return '{' + str(key) + '}'

@DeveloperAPI
class QueryTemplateCollection(dict):
    """ Dictionary like class that provides functions for templating queries"""
    
    def filter(self, mode: Optional[str] = None) -> 'QueryTemplateCollection':
        if mode:
            suffix = f'_{mode}'
            filtered = {k.replace(suffix, ''):v for k,v in self.items() if k.endswith(suffix)}
            return QueryTemplateCollection(**filtered)
        else:
            return QueryTemplateCollection(**self)
    
    def replace(self, query_key: str, **replacements) -> Optional[str]:
        query = self.get(query_key, None)
        if not query:
            return None
        
        replacements = DefaultArgs(**replacements)
        return query.format_map(replacements)
        
    def replace_all(self, **replacements) -> 'QueryTemplateCollection':
        templated = {k:self.replace(k, **replacements) for k,_ in self.items()}
        return QueryTemplateCollection(**templated)
  
@DeveloperAPI
class DatabaseConnector(ABC, Generic[DatabaseConnection]):
    """ Abstract class that implements the  DB operations used by readers and writers 
    when interacting with the database.

    Attributes:
        connection_properties: The connection properties of the database connection.
        connection : The underlying connection object used to interact with the database.
        
    Current subclasses:
        DBAPI2Connector
    """
    
    def __init__(self, **connection_properties):
        self.connection_properties = connection_properties
        self.connection: Optional[DatabaseConnection] = None
        self._ref_count: int = 0
    
    @abstractmethod
    def _open(self) -> DatabaseConnection:
        ...
    
    @abstractmethod
    def _commit(self) -> None:
        ...
    
    @abstractmethod
    def _rollback(self) -> None:
        ...
    
    @abstractmethod
    def _close(self) -> None:
        ...
    
    @abstractmethod
    def query(self, query: str, **query_args) -> Any:
        ...

    @abstractmethod
    def query_value(self, query: str, **query_args) -> QueryResult:
        ...

    @abstractmethod
    def query_block(self, query: str, format: BlockFormat = 'pyarrow', **query_args) -> Block:
        ...
    
    @abstractmethod
    def insert_block(self, query: str, block: Block, **query_args: Dict[str, Any]) -> None:
        ...
                
    def open(self) -> None:
        if self._ref_count == 0:
            if logger.debug:
                logger.debug(f'opening db connection)')
            self.connection = self._open()
        
        if logger.debug:
            logger.debug(f'starting db transaction)')
        self._ref_count += 1
   
    def commit(self) -> None:
        if self.connection:
            self._commit()
    
    def rollback(self) -> None:
        if self.connection:
            self._commit()
            
    def close(self):
        if not self.connection:
            raise ValueError('connection not open')
        
        self.commit()
        
        self._ref_count -= 1
        if self._ref_count == 0:
            if logger.debug:
                logger.debug(f'closing db connection)')
            self._close()
            self.connection = None
             
    def query_int(self, query: str, **query_args) -> Optional[int]:
        value = self.query_value(query, **query_args)
        if isinstance(value, int) or value == None:
            return value
        else:
            raise ValueError(f'returned value is not a int. {value}')

    def query_str(self, query: str, **query_args) -> Optional[str]:
        value = self.query_value(query, **query_args)
        if isinstance(value, str) or value == None:
            return value
        else:
            raise ValueError(f'returned value is not a str. {value}')
         
    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) :
        self.close()
        
    def __getstate__(self):
        state = self.__dict__.copy()
        # remove unserializable objects from state
        # so passing engines to tasks doesn't cause serialization error
        if '_connection' in state:
            del state['_connection']
            
        # ref counting is per process
        if '_ref_count' in state:
            state['_ref_count'] = 0

        return state
       
@DeveloperAPI
class DatabaseReadTask(ReadTask):
    def __init__(self, 
        connector: DatabaseConnector,
        metadata: BlockMetadata,
        queries: QueryTemplateCollection,
        format: BlockFormat = 'pyarrow',
        **query_args: Dict[str, Any],
    ):
        self._metadata = metadata
        self.connector = connector
        self.queries = queries
        self.format: BlockFormat = format
        self.query_args = query_args
                  
    def _read_fn(self) -> Iterable[Block]:
        if 'read_query' not in self.queries:
            raise ValueError('Database read task require a read_query to be defined')
        
        with self.connector:
            block = self.connector.query_block(
                self.queries['read_query'], 
                format=self.format, 
                **self.query_args
            )
            return [block]    
             
@DeveloperAPI
class DatabaseReader(Reader):
    def __init__(self,
        connector: DatabaseConnector,
        queries: QueryTemplateCollection,
        **query_args: Dict[str, Any]
    ):
        self.connector= connector
        self.queries = queries
        self.query_args = query_args
    
    @cached_property
    def num_rows(self) -> int:
        query = self.queries.get('num_rows_query')
        if not query:
            raise ValueError('number of rows query not specified.')
        
        with self.connector:
            num_rows = self.connector.query_int(query, **self.query_args)
            if num_rows is None:
                raise ValueError('number of rows query returns no value.')
            else:
                return num_rows
                
    @cached_property
    def sample(self) -> Block:
        query = self.queries.get('sample_query')
        if not query:
            raise ValueError('sample query not specified.')
        
        with self.connector:
            return self.connector.query_block(query, **self.query_args)

    def estimate_inmemory_data_size(self):
        if self.num_rows and self.sample is not None:    
            accessor = BlockAccessor.for_block(self.sample)
            return ceil(accessor.size_bytes()/accessor.num_rows()*self.num_rows)
        else:
            return None
    
    def _create_read_task(self, **kwargs) -> DatabaseReadTask:
        return DatabaseReadTask(**kwargs) # type: ignore  
            
    def get_read_tasks(self, parallelism: int) -> List[DatabaseReadTask]:
        """Gets the read tasks for reading from the database table in parallel. Will generate a task that will read
        divide reading from the table based on the number of rows and parallelism.

        Args:
            parallelism: The parallelism will be the same as the number of tasks returned.
        Returns:
            A list of read tasks.
        """           
        if self.num_rows == 0 or 'read_query' not in self.queries:
            return []
        
        elif self.num_rows < parallelism:
            parallelism = self.num_rows
        
        normal_block_size = self.num_rows // parallelism
        n_blocks_with_extra_row = self.num_rows % parallelism
        
        accessor = BlockAccessor.for_block(self.sample)
        average_row_size = accessor.size_bytes() // accessor.num_rows()
        
        schema = BlockAccessor.for_block(self.sample).schema()
        
        tasks = []
        row_start = 0   
        for i in range(0, parallelism):
            num_rows = normal_block_size 
            if i < n_blocks_with_extra_row:
                num_rows += 1
            
            size_bytes=average_row_size*num_rows                          
            metadata = BlockMetadata(num_rows, size_bytes, schema, None, None)        
            queries = self.queries.replace_all(row_start=row_start, num_rows=num_rows)
            task = self._create_read_task(
                connector=self.connector,
                metadata=metadata, 
                queries=queries, 
                **self.query_args
            )
            tasks.append(task)
            
            row_start += num_rows      
                
        return tasks  

@DeveloperAPI
class DatabaseWriteTask:
    def __init__(self, 
        connector: DatabaseConnector,
        metadata: BlockMetadata,
        block_ref: ObjectRef[Block],
        queries: QueryTemplateCollection,
        **query_args: Dict[str, Any]
    ):
        self.connector = connector
        self._metadata = metadata
        self.block_ref = block_ref
        self.queries = queries
        self.query_args = query_args
     
    def _write_fn(self):        
        if 'write_query' not in self.queries:
            raise ValueError('write task requires a write_query to be defined')   
        
        with self.connector:
            block = ray.get(self.block_ref)  # type: ignore
            self.connector.insert_block(
                query=str(self.queries['write_query']), 
                block=block, 
                **self.query_args
            ) 
                   
    def __call__(self) -> BlockMetadata: 
        stats = BlockExecStats.builder() 
        self._write_fn() 
        exec_stats=stats.build()
        self._metadata = dataclasses.replace(self._metadata, exec_stats=exec_stats)
        return self._metadata
    
    def on_write_complete(self, connector: DatabaseConnector) -> None:
        with connector:
            if 'on_write_complete_query' in self.queries:
                connector.query(str(self.queries['on_write_complete_query']), **self.query_args)
                
    def on_write_failed(self, connector: DatabaseConnector, error: Exception) -> None:
        with connector:
            if 'on_write_failed_query' in self.queries:
                connector.query(str(self.queries['on_write_failed_query']), **self.query_args)
                
def _execute_write_task(task: DatabaseWriteTask) -> DatabaseWriteTask:
    task()
    return task

@PublicAPI
class DatabaseDatasource(Datasource, ABC):
    """An abstract Ray datasource for reading and writing to databases.

    Attributes:
        connector: The connector to use for accessing the database.
        read_queries : A dictionary of read query templates.
        write_queries : A dictionary of write query templates.
        read_mode: The default read mode.
        write_mode: The default write mode.
        
    Current subclasses:
        DBAPI2Datasource
    """
    
    def __init__(self, 
        connector: DatabaseConnector,
        read_queries: Dict[str, str] = {},
        write_queries: Dict[str, str] = {},
        read_mode:  Optional[str] = None,
        write_mode:  Optional[str] = None
    ):  
        self.connector = connector    
        self.read_queries = QueryTemplateCollection(**read_queries)     
        self.write_queries = QueryTemplateCollection(**write_queries)
        self.read_mode = read_mode
        self.write_mode = write_mode
    
    def _get_template_args(self, **kwargs: Dict[str, Any]) -> Dict[str, Any]:        
        return kwargs
    
    def _get_query_args(self, **kwargs: Dict[str, Any]) -> Dict[str, Any]:        
        return kwargs
    
    def _create_reader(self, **kwargs) -> DatabaseReader:
        return DatabaseReader(**kwargs)
        
    def _create_write_task(self, **kwargs) -> DatabaseReader:
        return DatabaseWriteTask(**kwargs)
       
    def create_reader(self, mode: Optional[str] = None, **kwargs) -> DatabaseReader:
        template_args =  self._get_template_args(**kwargs)
        query_args = self._get_query_args(**kwargs)
        
        queries = self.read_queries\
            .filter(mode or self.read_mode)\
            .replace_all(**template_args)
                        
        return self._create_reader(
            connector=self.connector, 
            queries=queries, 
            **query_args
        )         
       
    def do_write(self,
        blocks: List[ObjectRef[Block]],
        metadatas: List[BlockMetadata],
        ray_remote_args: Dict[str, Any],
        mode: Optional[str] = None,
        **kwargs: Dict[str, Any]
    ) -> List[ObjectRef[DatabaseWriteTask]]:
        template_args =  self._get_template_args(**kwargs)
        query_args = self._get_query_args(**kwargs)
            
        remote_fn = cached_remote_fn(_execute_write_task).options(num_returns=1, **ray_remote_args)   
        results = []
        for n, block_ref, metadata in zip(range(0, len(blocks)), blocks, metadatas):        
            columns = _get_columns(metadata)
            replacements = dict(
                block_id = str(n),
                column_list= ','.join(columns),
                qmark_list= ','.join(['?']*len(columns)),
                **template_args
            )
                       
            metadata = dataclasses.replace(metadata)
            queries = self.write_queries.filter(mode or self.write_mode)
            queries = queries.replace_all(**replacements)
                
            task = self._create_write_task(
                connector=self.connector, 
                metadata=metadata, 
                block_ref=block_ref, 
                queries=queries, 
                **query_args
            )
            results.append(remote_fn.remote(task))
                         
        return results
    
    def on_write_complete(self, tasks: List[DatabaseWriteTask]) -> None:
        with self.connector:
            for task in tasks:
                task.on_write_complete(self.connector)
                
    def on_write_failed(self, tasks: List[ObjectRef[DatabaseWriteTask]], error: Exception) -> None:
        with self.connector:
            for task_ref in tasks:
                task: DatabaseWriteTask = ray.get(task_ref) # type: ignore
                task.on_write_failed(self.connector, error)