import logging
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.datasource.datasource import Datasource, Reader, ReadTask, WriteResult
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pymongoarrow.api

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class MongoDatasource(Datasource):
    """Datasource for reading from and writing to MongoDB.

    Examples:
        >>> import ray
        >>> from ray.data.datasource import MongoDatasource
        >>> from pymongoarrow.api import Schema # doctest: +SKIP
        >>> ds = ray.data.read_datasource( # doctest: +SKIP
        ...     MongoDatasource(), # doctest: +SKIP
        ...     uri="mongodb://username:password@mongodb0.example.com:27017/?authSource=admin", # noqa: E501 # doctest: +SKIP
        ...     database="my_db", # doctest: +SKIP
        ...     collection="my_collection", # doctest: +SKIP
        ...     schema=Schema({"col1": pa.string(), "col2": pa.int64()}), # doctest: +SKIP
        ... ) # doctest: +SKIP
    """

    def create_reader(self, **kwargs) -> Reader:
        return _MongoDatasourceReader(**kwargs)

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
        uri: str,
        database: str,
        collection: str,
    ) -> WriteResult:
        import pymongo

        _validate_database_collection_exist(
            pymongo.MongoClient(uri), database, collection
        )

        def write_block(uri: str, database: str, collection: str, block: Block):
            from pymongoarrow.api import write

            block = BlockAccessor.for_block(block).to_arrow()
            client = pymongo.MongoClient(uri)
            write(client[database][collection], block)

        builder = DelegatingBlockBuilder()
        for block in blocks:
            builder.add_block(block)
        block = builder.build()

        write_block(uri, database, collection, block)

        # TODO: decide if we want to return richer object when the task
        # succeeds.
        return "ok"


class _MongoDatasourceReader(Reader):
    def __init__(
        self,
        uri: str,
        database: str,
        collection: str,
        pipeline: Optional[List[Dict]] = None,
        schema: Optional["pymongoarrow.api.Schema"] = None,
        **mongo_args,
    ):
        self._uri = uri
        self._database = database
        self._collection = collection
        self._pipeline = pipeline
        self._schema = schema
        self._mongo_args = mongo_args
        # If pipeline is unspecified, read the entire collection.
        if not pipeline:
            self._pipeline = [{"$match": {"_id": {"$exists": "true"}}}]
        # Initialize Mongo client lazily later when creating read tasks.
        self._client = None

    def estimate_inmemory_data_size(self) -> Optional[int]:
        # TODO(jian): Add memory size estimation to improve auto-tune of parallelism.
        return None

    def _get_match_query(self, pipeline: List[Dict]) -> Dict:
        if len(pipeline) == 0 or "$match" not in pipeline[0]:
            return {}
        return pipeline[0]["$match"]

    def _get_or_create_client(self):
        import pymongo

        if self._client is None:
            self._client = pymongo.MongoClient(self._uri)
            _validate_database_collection_exist(
                self._client, self._database, self._collection
            )
            self._avg_obj_size = self._client[self._database].command(
                "collstats", self._collection
            )["avgObjSize"]

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        from bson.objectid import ObjectId

        self._get_or_create_client()
        coll = self._client[self._database][self._collection]
        match_query = self._get_match_query(self._pipeline)
        partitions_ids = list(
            coll.aggregate(
                [
                    {"$match": match_query},
                    {"$bucketAuto": {"groupBy": "$_id", "buckets": parallelism}},
                ],
                allowDiskUse=True,
            )
        )

        def make_block(
            uri: str,
            database: str,
            collection: str,
            pipeline: List[Dict],
            min_id: ObjectId,
            max_id: ObjectId,
            right_closed: bool,
            schema: "pymongoarrow.api.Schema",
            kwargs: dict,
        ) -> Block:
            import pymongo
            from pymongoarrow.api import aggregate_arrow_all

            # A range query over the partition.
            match = [
                {
                    "$match": {
                        "_id": {
                            "$gte": min_id,
                            "$lte" if right_closed else "$lt": max_id,
                        }
                    }
                }
            ]
            client = pymongo.MongoClient(uri)
            return aggregate_arrow_all(
                client[database][collection], match + pipeline, schema=schema, **kwargs
            )

        read_tasks: List[ReadTask] = []

        for i, partition in enumerate(partitions_ids):
            metadata = BlockMetadata(
                num_rows=partition["count"],
                size_bytes=partition["count"] * self._avg_obj_size,
                schema=None,
                input_files=None,
                exec_stats=None,
            )
            make_block_args = (
                self._uri,
                self._database,
                self._collection,
                self._pipeline,
                partition["_id"]["min"],
                partition["_id"]["max"],
                i == len(partitions_ids) - 1,
                self._schema,
                self._mongo_args,
            )
            read_task = ReadTask(
                lambda args=make_block_args: [make_block(*args)],
                metadata,
            )
            read_tasks.append(read_task)

        return read_tasks


def _validate_database_collection_exist(client, database: str, collection: str):
    db_names = client.list_database_names()
    if database not in db_names:
        raise ValueError(f"The destination database {database} doesn't exist.")
    collection_names = client[database].list_collection_names()
    if collection not in collection_names:
        raise ValueError(f"The destination collection {collection} doesn't exist.")
