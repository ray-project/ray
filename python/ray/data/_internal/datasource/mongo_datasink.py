import logging
from typing import Iterable

from ray.data._internal.datasource.mongo_datasource import (
    _validate_database_collection_exist,
)
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink

logger = logging.getLogger(__name__)


class MongoDatasink(Datasink[None]):
    def __init__(self, uri: str, database: str, collection: str) -> None:
        _check_import(self, module="pymongo", package="pymongo")
        _check_import(self, module="pymongoarrow", package="pymongoarrow")

        self.uri = uri
        self.database = database
        self.collection = collection

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> None:
        import pymongo

        _validate_database_collection_exist(
            pymongo.MongoClient(self.uri), self.database, self.collection
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

        write_block(self.uri, self.database, self.collection, block)
