# flake8: noqa

# fmt: off
# __read_single_query_start__
from ray.data.block import Block

def _read_single_query(uri, database, collection, query, schema, kwargs) -> Block:
    import pymongo
    from pymongoarrow.api import aggregate_arrow_all

    client = pymongo.MongoClient(uri)
    return aggregate_arrow_all(
        client[database][collection], query, schema=schema, **kwargs
    )
# __read_single_query_end__
# fmt: on

# fmt: off
# __mongo_datasource_reader_start__
from typing import Any, Dict, List, Optional
from ray.data.datasource.datasource import Datasource, Reader, ReadTask
from ray.data.block import BlockMetadata

class _MongoDatasourceReader(Reader):
    def __init__(self, uri, database, collection, pipelines, schema, kwargs):
        self._uri = uri
        self._database = database
        self._collection = collection
        self._pipelines = pipelines
        self._schema = schema
        self._kwargs = kwargs

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        read_tasks: List[ReadTask] = []
        for pipeline in self._pipelines:
            metadata = BlockMetadata(
                num_rows=None,
                size_bytes=None,
                schema=None,
                input_files=None,
                exec_stats=None,
            )
            read_task = ReadTask(
                lambda uri=self._uri, database=self._database,
                       collection=self._collection, pipeline=pipeline,
                       schema=self._schema, kwargs=self._kwargs: [
                    _read_single_query(
                        uri, database, collection, pipeline, schema, kwargs
                    )
                ],
                metadata,
            )
            read_tasks.append(read_task)
        return read_tasks
# __mongo_datasource_reader_end__
# fmt: on

# fmt: off
# __write_single_block_start__
def _write_single_block(uri, database, collection, block: Block):
    import pymongo
    from pymongoarrow.api import write

    client = pymongo.MongoClient(uri)
    write(client[database][collection], block)
# __write_single_block_end__
# fmt: on

# fmt: off
# __write_multiple_blocks_start__
from ray.data._internal.remote_fn import cached_remote_fn
from ray.types import ObjectRef

def _write_multiple_blocks(
    self,
    blocks: List[ObjectRef[Block]],
    metadata: List[BlockMetadata],
    ray_remote_args: Optional[Dict[str, Any]],
    uri,
    database,
    collection,
) -> List[ObjectRef[Any]]:
    write_block = cached_remote_fn(_write_single_block).options(**ray_remote_args)
    write_tasks = []
    for block in blocks:
        write_task = write_block.remote(uri, database, collection, block)
        write_tasks.append(write_task)
    return write_tasks
# __write_multiple_blocks_end__
# fmt: on

# fmt: off
# __mongo_datasource_start__
class MongoDatasource(Datasource):
    def create_reader(
        self, uri, database, collection, pipelines, schema, kwargs
    ) -> Reader:
        return _MongoDatasourceReader(
            uri, database, collection, pipelines, schema, kwargs
        )

    def do_write(
        self,
        blocks: List[ObjectRef[Block]],
        metadata: List[BlockMetadata],
        ray_remote_args: Optional[Dict[str, Any]],
        uri,
        database,
        collection,
    ) -> List[ObjectRef[Any]]:
        return _write_multiple_blocks(
            blocks, metadata, ray_remote_args, uri, database, collection
        )
# __mongo_datasource_end__
# fmt: on
