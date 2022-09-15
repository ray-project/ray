import logging
from typing import List, Optional

import pymongo
from pymongoarrow.api import aggregate_arrow_all

from ray.data.datasource.datasource import Datasource, Reader, ReadTask
from ray.data.block import (
    Block,
    BlockMetadata,
)
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


@PublicAPI
class MongoDatasource(Datasource):
    def create_reader(
        self, uri, database, collection, pipelines, schema, kwargs
    ) -> Reader:
        return _MongoDatasourceReader(
            uri, database, collection, pipelines, schema, kwargs
        )
