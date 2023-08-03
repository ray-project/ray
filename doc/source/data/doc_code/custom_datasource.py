# flake8: noqa

# fmt: off
# __read_single_partition_start__
from ray.data.block import Block

# This connects to MongoDB, executes the pipeline against it, converts the result
# into Arrow format and returns the result as a Block.
def _read_single_partition(
    uri, database, collection, pipeline, schema, kwargs
) -> Block:
    import pymongo
    from pymongoarrow.api import aggregate_arrow_all

    client = pymongo.MongoClient(uri)
    # Read more about this API here:
    # https://mongo-arrow.readthedocs.io/en/stable/api/api.html#pymongoarrow.api.aggregate_arrow_all
    return aggregate_arrow_all(
        client[database][collection], pipeline, schema=schema, **kwargs
    )
# __read_single_partition_end__
# fmt: on

# fmt: off
# __mongo_datasource_reader_start__
from typing import Any, Dict, List, Optional
from ray.data.datasource.datasource import Datasource, Reader, ReadTask
from ray.data.block import BlockMetadata

class _MongoDatasourceReader(Reader):
    # This is constructed by the MongoDatasource, which will supply these args
    # about MongoDB.
    def __init__(self, uri, database, collection, pipelines, schema, kwargs):
        self._uri = uri
        self._database = database
        self._collection = collection
        self._pipelines = pipelines
        self._schema = schema
        self._kwargs = kwargs

    # Create a list of ``ReadTask``, one for each pipeline (i.e. a partition of
    # the MongoDB collection). Those tasks will be executed in parallel.
    # Note: The ``parallelism`` which is supposed to indicate how many ``ReadTask`` to
    # return will have no effect here, since we map each query into a ``ReadTask``.
    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        read_tasks: List[ReadTask] = []
        for pipeline in self._pipelines:
            # The metadata about the block that we know prior to actually executing
            # the read task.
            metadata = BlockMetadata(
                num_rows=None,
                size_bytes=None,
                schema=self._schema,
                input_files=None,
                exec_stats=None,
            )
            # Supply a no-arg read function (which returns a block) and pre-read
            # block metadata.
            read_task = ReadTask(
                lambda uri=self._uri, database=self._database,
                       collection=self._collection, pipeline=pipeline,
                       schema=self._schema, kwargs=self._kwargs: [
                    _read_single_partition(
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
# This connects to MongoDB and writes a block into it.
# Note this is an insertion, i.e. each record in the block are treated as
# new document to the MongoDB (so no mutation of existing documents).
def _write_single_block(uri, database, collection, block: Block):
    import pymongo
    from pymongoarrow.api import write

    client = pymongo.MongoClient(uri)
    # Read more about this API here:
    # https://mongo-arrow.readthedocs.io/en/stable/api/api.html#pymongoarrow.api.write
    write(client[database][collection], block)
# __write_single_block_end__
# fmt: on

# fmt: off
# __write_multiple_blocks_start__
from ray.data._internal.remote_fn import cached_remote_fn
from ray.types import ObjectRef
from ray.data.datasource.datasource import WriteResult

# This writes a list of blocks into MongoDB. Each block is handled by a task and
# tasks are executed in parallel.
def _write_multiple_blocks(
    blocks: List[ObjectRef[Block]],
    metadata: List[BlockMetadata],
    ray_remote_args: Optional[Dict[str, Any]],
    uri,
    database,
    collection,
) -> List[ObjectRef[WriteResult]]:
    # The ``cached_remote_fn`` turns the ``_write_single_block`` into a Ray
    # remote function.
    write_block = cached_remote_fn(_write_single_block).options(**ray_remote_args)
    write_tasks = []
    for block in blocks:
        # Create a Ray remote function for each block.
        write_task = write_block.remote(uri, database, collection, block)
        write_tasks.append(write_task)
    return write_tasks
# __write_multiple_blocks_end__
# fmt: on

# fmt: off
# __mongo_datasource_start__
# MongoDB datasource, for reading from and writing to MongoDB.
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
    ) -> List[ObjectRef[WriteResult]]:
        return _write_multiple_blocks(
            blocks, metadata, ray_remote_args, uri, database, collection
        )
# __mongo_datasource_end__
# fmt: on
