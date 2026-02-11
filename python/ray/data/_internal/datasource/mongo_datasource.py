import logging
from typing import TYPE_CHECKING, Dict, List, Optional

from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

if TYPE_CHECKING:
    import pymongoarrow.api

    from ray.data.context import DataContext

logger = logging.getLogger(__name__)


class MongoDatasource(Datasource):
    """Datasource for reading from and writing to MongoDB."""

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
        """Estimate the in-memory size of the data to be read.

        Uses MongoDB collection statistics to estimate size based on:
        - Average object size (avgObjSize from collStats)
        - Estimated document count matching the pipeline

        Note: This is an approximate estimation. The actual in-memory size may vary
        depending on data characteristics and PyArrow conversion overhead.

        Returns:
            Optional[int]: Estimated size in bytes, or None if cannot estimate.
        """
        try:
            # Initialize client if not already done
            self._get_or_create_client()

            # Check if _avg_obj_size was initialized successfully
            avg_obj_size = getattr(self, "_avg_obj_size", 0)
            if avg_obj_size == 0:
                return None

            coll = self._client[self._database][self._collection]

            # Get match query from pipeline
            match_query = self._get_match_query(self._pipeline)

            # Check if this is the default "match all" pipeline
            # The default pipeline [{"$match": {"_id": {"$exists": "true"}}}]
            # matches all documents, so we should use the faster estimated count
            is_default_pipeline = match_query == {"_id": {"$exists": "true"}}

            # Estimate document count based on query
            if match_query and not is_default_pipeline:
                # Use count_documents for user-specified filtered queries
                try:
                    estimated_count = coll.count_documents(match_query)
                except Exception as e:
                    logger.debug(
                        f"count_documents failed, using estimated_document_count: {e}"
                    )
                    estimated_count = coll.estimated_document_count()
            else:
                # Use estimated_document_count for full collection scans (faster)
                estimated_count = coll.estimated_document_count()

            if estimated_count == 0:
                return None

            # Estimate total size: count * avg_obj_size
            # Add 20% buffer for PyArrow conversion overhead (empirically determined)
            PYARROW_OVERHEAD_FACTOR = 1.2
            estimated_size = int(
                estimated_count * avg_obj_size * PYARROW_OVERHEAD_FACTOR
            )

            logger.debug(
                f"MongoDB memory estimation: "
                f"{estimated_count} docs * {avg_obj_size} bytes/doc "
                f"= {estimated_size} bytes (with {PYARROW_OVERHEAD_FACTOR}x overhead)"
            )

            return estimated_size
        except ValueError:
            # Re-raise ValueError for database/collection validation errors
            raise
        except Exception as e:
            logger.warning(f"Failed to estimate MongoDB data size: {e}")
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
                "collStats", self._collection
            )["avgObjSize"]

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List[ReadTask]:
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
                per_task_row_limit=per_task_row_limit,
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
