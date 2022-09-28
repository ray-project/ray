import logging
from typing import Any, Dict, List, Optional

from ray.data.datasource.datasource import Datasource, Reader, ReadTask
from ray.data.block import (
    Block,
    BlockMetadata,
)
from ray.data._internal.remote_fn import cached_remote_fn
from ray.types import ObjectRef
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


class _MongoDatasourceReader(Reader):
    def __init__(self, uri, database, collection, pipelines, schema, kwargs):
        self._uri = uri
        self._database = database
        self._collection = collection
        self._pipelines = pipelines
        self._schema = schema
        self._kwargs = kwargs

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        def make_block(uri, database, collection, pipeline, schema, kwargs) -> Block:
            import pymongo
            from pymongoarrow.api import aggregate_arrow_all

            client = pymongo.MongoClient(uri)
            return aggregate_arrow_all(
                client[database][collection], pipeline, schema=schema, **kwargs
            )

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
                lambda uri=self._uri, database=self._database, collection=self._collection, pipeline=pipeline, schema=self._schema, kwargs=self._kwargs: [  # noqa: E501
                    make_block(uri, database, collection, pipeline, schema, kwargs)
                ],
                metadata,
            )
            read_tasks.append(read_task)
        return read_tasks


@PublicAPI(stability="alpha")
class MongoDatasource(Datasource):
    """Datasource for reading from and writing to MongoDB.

    A MongoDB is described by three elements: URI, Database and Collection.
    The URI points to an MongoDB instance. For the format of URI, see
    https://www.mongodb.com/docs/manual/reference/connection-string/.
    A collection is similar to the table concept in SQL databases. The
    MongoDatasource is for reading and writing collections.

    To read the MongoDB in parallel, users are supposed to provide a list of MongoDB
    queries, with each corresponding to a block to be created for Dataset. Those
    queries are usually formulated as disjoint range queries over a specific field (
    i.e. partition key).

    Implementation wise, we will use pymongo to connect to MongoDB, and use pymongoarrow
    to convert MongoDB documents to/from Arrow format, which is a supported block format
    in Dataset.

    Examples:
        >>> import ray
        >>> from ray.data.datasource import MongoDatasource
        >>> ds = ray.data.read_datasource( # doctest: +SKIP
        ...     MongoDatasource(),
        ...     uri=MY_MONGO_URI,
        ...     database=MY_MONGO_DB,
        ...     collection=MY_MONGO_COLLECTION,
        ...     schema=MY_MONGO_SCHEMA,
        ... )
    """

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
        def write_block(uri, database, collection, block: Block):
            import pymongo
            from pymongoarrow.api import write

            client = pymongo.MongoClient(uri)
            write(client[database][collection], block)

        if ray_remote_args is None:
            ray_remote_args = {}

        write_block = cached_remote_fn(write_block).options(**ray_remote_args)
        write_tasks = []
        for block in blocks:
            write_task = write_block.remote(uri, database, collection, block)
            write_tasks.append(write_task)
        return write_tasks
