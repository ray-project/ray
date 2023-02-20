from abc import ABC, abstractmethod
from collections import defaultdict
import dataclasses
import textwrap
from functools import cached_property
from math import ceil
from typing import Any, Callable, Dict, Generic, Iterable, List, Optional, TypeVar, Union
import logging
import pandas, pyarrow
import ray
from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata, BlockExecStats, BlockAccessor
from ray.data.datasource import Reader, Datasource, ReadTask
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.data._internal.remote_fn import cached_remote_fn
  
DatabaseConnection = TypeVar("DatabaseConnection")

def _transpose_list(pylist: List[List[Any]]) -> List[List[Any]]: 
     return list(map(list, zip(*pylist)))
  
def pylist_to_pydict_of_lists(pylist: List[List[Any]], columns: List[str]) -> Dict[str,List[Any]]: 
    return {c:d for c,d in zip(columns, _transpose_list(pylist))}      

def pylist_to_pylist_of_dicts(pylist: List[List[Any]], columns: List[str]) -> List[Dict[str, Any]]:
    return [{c:v for c,v in zip(columns, row)} for row in pylist]

def block_to_pylist(block: Block) -> List[List[Any]]:
    return [[*pydict.values()] for pydict in block_to_pylist_of_dicts(block)]

def block_to_pylist_of_dicts(block: Block) -> List[Dict[str, Any]]:
    return BlockAccessor.for_block(block).to_arrow().to_pylist()

def block_to_pydict_of_lists(block: Block) -> List[Dict[str, Any]]:
    return BlockAccessor.for_block(block).to_arrow().to_pydict()

def pylist_to_pandas(pylist: List[List[Any]], columns: List[str]) -> Block:
    return pandas.DataFrame(columns=columns, data=pylist)           

def pylist_to_pyarrow(pylist: List[List[Any]], columns: List[str]) -> Block:
    return pyarrow.Table.from_pylist(pylist_to_pylist_of_dicts(pylist, columns))          
                 
def _execute_write_task(task: 'DatabaseWriteTask') -> 'DatabaseWriteTask':
    task()
    return task

def _desc_val(v: Any) -> str:
    value_type = type(v)
    if value_type in (int, bool, float):
        desc = f"{v}"
    elif value_type == str:
        desc = f"'{v}'"
    elif value_type == list:
        desc = '[' + ','.join([_desc_val(d) for d in v]) +']'
    elif value_type == dict:
        desc = 'dict(' + ','.join([v1+'='+_desc_val(v2) for v1,v2 in v.items()]) +')'
    else:
        desc = f"type({value_type})" 
    
    if len(desc) > 100:
        desc = desc[0:50] + ' ... ' + desc[-50:]
            
    return desc
            
def _desc_method(method:str, *args, **kwargs) -> str:
    desc = ''   
    if len(args) > 0:
        desc = '\n'.join([_desc_val(v) for v in args])
        if len(kwargs) > 0:
            desc = desc + '\n'
    if len(kwargs) > 0:
        desc = desc + '\n'.join([k+'='+_desc_val(v) for k,v in kwargs.items()])    
    desc = textwrap.indent(desc, '  ')
    return method + ':\n' + desc
       
@DeveloperAPI
class DefaultArgs(defaultdict):  
    def __missing__(self, key):
        return '{' + str(key) + '}'

@DeveloperAPI
class QueryTemplates(dict):
    """ Dictionary like class that provides functions for filtering and templating queries"""
    
    def templatize(self, 
        metadata: Optional[BlockMetadata] = None, 
        partition: Optional[int] = None,
        mode: Optional[str] = None,
        column_template: str = '{column}',
        partition_template: str = '{partition}',
        param_template: str = '?',
        **kwargs
    ) -> 'QueryTemplates':
        replacements = {}
        if metadata:
            replacements['column_list'] = self._get_column_list_string(column_template, metadata)
            replacements['param_list'] = self._get_param_list_string(param_template, metadata)
        
        if partition is not None:
            replacements['partition'] = self._get_partition(partition_template, metadata, partition)
    
        return self.filter(mode).replace_all(**{**replacements, **kwargs} )   
        
    def filter(self, mode: Optional[str] = None) -> 'QueryTemplates':
        if mode != None:
            suffix = f'_{mode}'
            filtered = {k.replace(suffix, ''):v for k,v in self.items() if k.endswith(suffix)}
            return type(self)(**filtered)
        else:
            return type(self)(**self)
    
    def replace(self, query_key: str, **replacements) -> Optional[str]:
        query = self.get(query_key, None)
        if not query:
            return None
        
        replacements = DefaultArgs(**replacements)
        formatted = query.format_map(replacements)
        return textwrap.dedent(formatted.strip('\n'))
        
    def replace_all(self, **replacements) -> 'QueryTemplates':
        templated = {k:self.replace(k, **replacements) for k,_ in self.items()}
        return type(self)(**templated)
  
    def _get_columns(self, metadata: BlockMetadata):
        if metadata.schema:
            return metadata.schema.names
        else:
            raise ValueError('cannot get columns as metadata is missing schema')
    
    def _get_partition(self, partition_template: str, metadata: BlockMetadata, partition: int) -> str:
        return partition_template.format(partition = str(partition)) 

    def _get_column_list_string(self, column_template: str, metadata: BlockMetadata) -> str:
        columns = self._get_columns(metadata)
        column_str = ','.join([column_template.format(column=c) for c in columns])
        return column_str
            
    def _get_param_list_string(self, param_template: str, metadata: BlockMetadata) -> str:
        columns = self._get_columns(metadata)
        return ','.join([param_template.format(column=c) for c in columns])
    
@DeveloperAPI
class DatabaseConnector(ABC):
    """ Abstract class that implements the  DB operations used by readers and writers 
    when interacting with the database.

    Attributes:
        connect_properties: The connection properties of the database connection.
        connection : The underlying connection object used to interact with the database.
        
    Current subclasses:
        DBAPI2Connector
    """
      
    def __init__(self,
        to_value_fn: Callable[[Any], Any], 
        to_block_fn: Callable[[Any], Block],
        from_block_fn: Callable[[Block], Any],
        **connect_properties
    ):
        self.to_value_fn = to_value_fn
        self.to_block_fn = to_block_fn
        self.from_block_fn = from_block_fn
        self.connect_properties = connect_properties
        self.connection: Optional[DatabaseConnection] = None
        self._ref_count: int = 0
        self._logger = logging.getLogger(self.__class__.__name__)
    
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
    def _execute(self, query: str, **kwargs) -> Any:
        ...
                    
    def open(self) -> None:
        if self._ref_count == 0:
            self.debug('opening database connection')
            try:  
                self.connection = self._open()
            except BaseException as e:
                self.connection = None
                self._logger.error('error opening database connection', exc_info=True)    
                raise e              
        self._ref_count += 1
   
    def commit(self) -> None:
        if not self.connection:
            raise ValueError('database connection not open')

        self.debug('committing database transaction')         
        try:  
            self._commit()
        except BaseException as e:
            self._logger.error('committing database transaction', exc_info=True)
            raise e
                
    def rollback(self) -> None:
        if not self.connection:
            raise ValueError('database connection not open')
        
        self.debug('rolling back database transaction')       
        try:  
            self._rollback()
        except BaseException as e:
            self._logger.error('error rolling back database transaction', exc_info=True)
            raise e
            
    def close(self):
        if not self.connection:
            raise ValueError('database connection not open')
        
        self.commit()
        
        self._ref_count -= 1
        if self._ref_count == 0:
            self.debug('closing database connection')  
            try:  
                self._close()  
            except BaseException as e:
                self._logger.error('error closing database connection', exc_info=True)
                raise e
            finally:
                self.connection = None    

    def query(self, query: str, **kwargs) -> None:
        self.execute(query, **kwargs)
            
    def query_value(self, query: str, **kwargs) -> Any:
        return self.to_value_fn(self.execute(query, **kwargs))
                    
    def query_int(self, query: str, **kwargs) -> Optional[int]:
        value = self.query_value(query, **kwargs)
        return int(value) if value else None

    def query_str(self, query: str, **kwargs) -> Optional[str]:
        value = self.query_value(query, **kwargs)
        return str(value) if value else None
    
    def query_block(self, query: str, **kwargs) -> Block:
        return self.to_block_fn(self.execute(query, **kwargs))
    
    def insert_block(self, query: str, block: Block, **kwargs) -> None:
        data = self.from_block_fn(block)
        self.execute(query, data=data, **kwargs)

    def execute(self, query:str, **kwargs) -> Any:      
        self.debug('executing on database', query, **kwargs)  
        try:    
            if 'call_fn(' == query[:8]:
                return self.call_fn(query, **kwargs)
            else:
                return self._execute(query, **kwargs)
        except BaseException as e:
            self._logger.error(
                _desc_method('error executing on database', query, **kwargs), 
                exc_info=True
            )
            raise e
    
    def call_fn(self, query: str, **kwargs) -> Any:
        argv = query[8:-1].split(',')
        fn_name,fn_args = argv[0], argv[1:]
        return self.__getattribute__(fn_name)(*{*fn_args}, **kwargs)
                           
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
    
    def debug(self, message: str, *args, **kwargs):
        if True:#self._logger.isEnabledFor(logging.DEBUG):
            if len(args) > 0:
                #print(_desc_method(message, *args, **kwargs))
                self._logger.debug(_desc_method(message, *args, **kwargs))
            else:
                #print(message)   
                self._logger.debug(message)   
@DeveloperAPI
class DatabaseReadTask(ReadTask):
    def __init__(self, 
        connector: DatabaseConnector,
        metadata: BlockMetadata,
        queries: QueryTemplates,
        query_kwargs: Dict[str,Any] = {},
    ):
        self._metadata = metadata
        self.connector = connector
        self.queries = queries
        self.query_kwargs = query_kwargs
                  
    def _read_fn(self) -> Iterable[Block]:
        query = self.queries.get('read')
        if query:       
            with self.connector:
                return [self.connector.query_block(query, **self.query_kwargs)]
        else:
            return []  
             
@DeveloperAPI
class DatabaseReader(Reader):
    def __init__(self,
        connector: DatabaseConnector,
        queries: QueryTemplates,
        query_kwargs: Dict[str,Any] = {}
    ):
        self.connector = connector
        self.queries = queries
        self.query_kwargs = query_kwargs
    
    @cached_property
    def num_rows(self) -> int:
        query = self.queries.get('num_rows')
        if not query:
            raise ValueError('number of rows query not specified.')
        
        with self.connector:
            num_rows = self.connector.query_int(query, **self.query_kwargs)
            if num_rows is None:
                raise ValueError('number of rows query returns no value.')
            else:
                return num_rows
                
    @cached_property
    def sample(self) -> Block:
        query = self.queries.get('sample')
        if not query:
            raise ValueError('sample query not specified.')
        
        with self.connector:
            return self.connector.query_block(query, **self.query_kwargs)

    def estimate_inmemory_data_size(self):
        if self.num_rows and self.sample is not None:    
            accessor = BlockAccessor.for_block(self.sample)
            return ceil(accessor.size_bytes()/accessor.num_rows()*self.num_rows)
        else:
            return None 
            
    def get_read_tasks(self, parallelism: int) -> List[DatabaseReadTask]:
        """Gets the read tasks for reading from the database table in parallel. 
        Will generate a task that will read divide reading from the table based on the number of rows and parallelism.

        Args:
            parallelism: The parallelism will be the same as the number of tasks returned.
        Returns:
            A list of read tasks.
        """           
        if self.num_rows == 0:
            return []
        
        elif self.num_rows < parallelism:
            parallelism = self.num_rows
        
        normal_block_size = self.num_rows // parallelism
        n_blocks_with_extra_row = self.num_rows % parallelism
        
        accessor = BlockAccessor.for_block(self.sample)
        average_row_size = accessor.size_bytes() // accessor.num_rows()
        
        schema = accessor.schema()
        
        tasks = []
        row_start = 0   
        for i in range(0, parallelism):
            num_rows = normal_block_size 
            if i < n_blocks_with_extra_row:
                num_rows += 1
            
            size_bytes=average_row_size*num_rows                          
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
                    self.query_kwargs
                )
            )
            
            row_start += num_rows      
                
        return tasks  

@DeveloperAPI
class DatabaseWriteTask:
    def __init__(self, 
        connector: DatabaseConnector,
        metadata: BlockMetadata,
        block_ref: ObjectRef[Block],
        queries: QueryTemplates,
        query_kwargs: Dict[str,Any] = {}
    ):
        self.connector = connector
        self._metadata = metadata
        self.block_ref = block_ref
        self.queries = queries
        self.query_kwargs = query_kwargs
     
    def _write_fn(self):
        query = self.queries.get('write')     
        if query:
            with self.connector as con:
                block = ray.get(self.block_ref)  # type: ignore
                con.insert_block(query, block, **self.query_kwargs)
                   
    def __call__(self) -> BlockMetadata: 
        stats = BlockExecStats.builder() 
        self._write_fn() 
        exec_stats=stats.build()
        self._metadata = dataclasses.replace(self._metadata, exec_stats=exec_stats)
        return self._metadata
    
    def prepare(self, connector: DatabaseConnector) -> None:
        query = self.queries.get('prepare')
        if query:
            connector.query(query, **self.query_kwargs)   
    
    def complete(self, connector: DatabaseConnector) -> None:
        query = self.queries.get('complete')
        if query:
            connector.query(query, **self.query_kwargs)  
    
    def all_complete(self, connector: DatabaseConnector) -> None:
        query = self.queries.get('all_complete')
        if query:
            connector.query(query, **self.query_kwargs)  
                        
    def failed(self, connector: DatabaseConnector, error: Exception) -> None:
        query = self.queries.get('failed')
        if query:
            connector.query(query, **self.query_kwargs)  
    
    def cleanup(self, connector: DatabaseConnector) -> None:
        query = self.queries.get('cleanup')
        if query:
            try:
                connector.query(query, **self.query_kwargs)
            except BaseException as e:
                pass   
            
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
        *,
        read_queries: Dict[str, str] = {},
        write_queries: Dict[str, str] = {},
        template_keys: List[str] = []
    ):  
        self.connector = connector    
        self.read_queries = QueryTemplates(**read_queries)     
        self.write_queries = QueryTemplates(**write_queries)
        self.template_keys = ['column_template', 'partition_template', 'param_template'] + template_keys
    
    def _get_template_kwargs(self, **kwargs: Dict[str, Any]) -> Dict[str, Any]:     
        return {k:v for k,v in kwargs.items() if k in self.template_keys}
    
    def _get_query_kwargs(self, **kwargs: Dict[str, Any]) -> Dict[str, Any]:       
        return {k:v for k,v in kwargs.items() if k not in self.template_keys}   
     
    def create_reader(self, mode: str, **kwargs) -> DatabaseReader:
        template_kwargs =  self._get_template_kwargs(**kwargs)
        query_kwargs = self._get_query_kwargs(**kwargs)       
        queries = self.read_queries.templatize(mode = mode, **template_kwargs)         
        return DatabaseReader(self.connector, queries, query_kwargs)         
       
    def do_write(self,
        blocks: List[ObjectRef[Block]],
        metadatas: List[BlockMetadata],
        *,
        ray_remote_args: Dict[str, Any],
        mode: str,
        **kwargs: Dict[str, Any]
    ) -> List[ObjectRef]:
        template_kwargs =  self._get_template_kwargs(**kwargs)
        query_kwargs = self._get_query_kwargs(**kwargs)
            
        # create tasks
        tasks = []
        for n, block_ref, metadata in zip(range(0, len(blocks)), blocks, metadatas):        
            queries = self.write_queries.templatize(
                mode = mode,
                metadata = metadata,
                partition = n,
                **template_kwargs
            )

            tasks.append(
                DatabaseWriteTask(
                    self.connector, 
                    metadata, 
                    block_ref, 
                    queries,
                    query_kwargs
                )
            )
        
        # prepare tasks in single transactions
        with self.connector as con: 
            for task in tasks:   
                task.prepare(con) 

        # execute tasks remotely
        remote_fn = cached_remote_fn(_execute_write_task).options(num_returns=1, **ray_remote_args)
        result_refs = [remote_fn.remote(task) for task in tasks]              
        return result_refs
    
    def on_write_complete(self, tasks: List[DatabaseWriteTask]) -> None:
        if len(tasks) > 0:
            with self.connector as con:
                try:
                    for task in tasks:
                        task.complete(con)             
                    tasks[0].all_complete(con)
                    con.commit()              
                finally:
                    tasks[0].cleanup(con)   
                
    def on_write_failed(self, task_refs: List[ObjectRef[DatabaseWriteTask]], error: Exception) -> None:
        if len(task_refs) > 0:
            with self.connector as con:
                try:
                    tasks: List[DatabaseWriteTask] = ray.get(task_refs)
                    for task in tasks:
                        task.failed(con, error) 
                    con.commit()            
                finally:
                    tasks[0].cleanup(con)   