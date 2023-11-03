import logging
from typing import Any, Iterable

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink
from ray.data.datasource.mongo_datasource import _validate_database_collection_exist

logger = logging.getLogger(__name__)


class _MongoDatasink(Datasink):
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
    ) -> Any:
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

        return "ok"
