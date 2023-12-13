import logging
from typing import List, Optional
import numpy as np

from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class SparkDatasource(Datasource):

    def __init__(self, spark_dataframe, rows_per_chunk):
        from pyspark.sql.chunk_api import persist_dataframe_as_chunks

        self.chunk_meta_list = persist_dataframe_as_chunks(spark_dataframe, rows_per_chunk)
        self._estimate_inmemory_data_size = sum(
            chunk_meta.byte_count for chunk_meta in self.chunk_meta_list
        )

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return self._estimate_inmemory_data_size

    def _get_read_task(self, index, parallelism):
        from pyspark.sql.chunk_api import read_chunk

        num_chunks = len(self.chunk_meta_list)

        # get chunk list to be read in this task
        chunk_index_list = list(np.array_split(range(num_chunks), parallelism)[index])

        num_rows = sum(
            self.chunk_meta_list[chunk_index].row_count
            for chunk_index in chunk_index_list
        )
        size_bytes = sum(
            self.chunk_meta_list[chunk_index].byte_count
            for chunk_index in chunk_index_list
        )

        metadata = BlockMetadata(
            num_rows=num_rows,
            size_bytes=size_bytes,
            schema=None,
            input_files=None,
            exec_stats=None,
        )

        def read_fn():
            for chunk_index in chunk_index_list:
                chunk_id = self.chunk_meta_list[chunk_index].id
                yield read_chunk(chunk_id)

        return ReadTask(read_fn=read_fn, metadata=metadata)

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        assert parallelism > 0, f"Invalid parallelism {parallelism}"

        if parallelism > self.num_chunks:
            parallelism = self.num_chunks

        return [
            self._get_read_task(index, parallelism)
            for index in range(parallelism)
        ]

    def get_name(self):
        """Return a human-readable name for this datasource.
        This will be used as the names of the read tasks.
        """
        return "Spark"

    def dispose_spark_cache(self):
        from pyspark.sql.chunk_api import unpersist_chunks

        chunk_ids = [
            chunk_meta.id
            for chunk_meta in self.chunk_meta_list
        ]
        unpersist_chunks(chunk_ids)