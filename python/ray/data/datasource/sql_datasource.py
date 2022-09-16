import logging
from typing import List, Optional, Dict, Any
import pandas as pd

from ray.data.datasource.datasource import Datasource, Reader, ReadTask
from ray.data.block import (
    Block,
    BlockMetadata,
)
from ray.data._internal.remote_fn import cached_remote_fn
from ray.types import ObjectRef
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


class _SQLDatasourceReader(Reader):
    def __init__(self, url, queries, kwargs):
        self._url = url
        self._queries = queries
        self._kwargs = kwargs

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        def make_block(url, query, kwargs) -> Block:
            import sqlalchemy
            engine = sqlalchemy.create_engine(url)
            return pd.read_sql_query(query, engine, **kwargs)

        read_tasks: List[ReadTask] = []
        for query in self._queries:
            metadata = BlockMetadata(
                num_rows=None,
                size_bytes=None,
                schema=None,
                input_files=None,
                exec_stats=None,
            )
            read_task = ReadTask(
                lambda url=self._url, query=query, kwargs=self._kwargs: [
                    make_block(url, query, kwargs)
                ],
                metadata,
            )
            read_tasks.append(read_task)
        return read_tasks


@PublicAPI
class SQLDatasource(Datasource):
    def create_reader(self, url, queries, kwargs) -> Reader:
        return _SQLDatasourceReader(url, queries, kwargs)

    def do_write(
        self,
        blocks: List[ObjectRef[Block]],
        metadata: List[BlockMetadata],
        ray_remote_args: Optional[Dict[str, Any]],
        url,
        table,
        kwargs,
    ) -> List[ObjectRef[Any]]:
        def write_block(url, table, block, kwargs):
            import sqlalchemy
            engine = sqlalchemy.create_engine(url)
            block.to_sql(table, engine, **kwargs)

        if ray_remote_args is None:
            ray_remote_args = {}

        write_block = cached_remote_fn(write_block).options(**ray_remote_args)
        write_tasks = []
        for block in blocks:
            write_task = write_block.remote(url, table, block, kwargs)
            write_tasks.append(write_task)
        return write_tasks
