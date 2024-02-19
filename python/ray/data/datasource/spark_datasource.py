import logging
from typing import Iterator, List, Optional
import numpy as np
import pyarrow

from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.util.annotations import PublicAPI


logger = logging.getLogger(__name__)


def check_requirements():
    from ray.util.spark.utils import get_spark_session

    spark = get_spark_session()
    if spark.conf.get(
        "spark.databricks.pyspark.dataFrameChunk.enabled", "false"
    ).lower() != "true":
        raise RuntimeError(
            "In databricks runtime, if you want to use 'ray.data.from_spark' API, "
            "you need to set spark cluster config "
            "'spark.databricks.pyspark.dataFrameChunk.enabled' to 'true'."
        )

    if spark.conf.get(
        "spark.databricks.acl.dfAclsEnabled", "false"
    ).lower() != "false":
        raise RuntimeError(
            "In databricks runtime, if you want to use 'ray.data.from_spark' API, "
            "you should use an assigned mode databricks cluster."
        )


def _persist_dataframe_as_chunks(spark_dataframe, bytes_per_chunk):
    try:
        from pyspark.databricks.sql.chunk import persistDataFrameAsChunks
    except ImportError:
        raise RuntimeError(
            "Current Databricks Runtime version does not support Ray SparkDatasource."
        )
    return persistDataFrameAsChunks(spark_dataframe, bytes_per_chunk)


def _read_chunk_fn(chunk_id_list) -> Iterator["pyarrow.Table"]:
    from pyspark.databricks.sql.chunk import readChunk
    for chunk_id in chunk_id_list:
        yield readChunk(chunk_id)


def _unpersist_chunks(chunk_ids):
    from pyspark.databricks.sql.chunk import unpersistChunks
    return unpersistChunks(chunk_ids)


@PublicAPI(stability="alpha")
class SparkDatasource(Datasource):

    def __init__(self, spark_dataframe, bytes_per_chunk):
        self.chunk_meta_list = _persist_dataframe_as_chunks(spark_dataframe, bytes_per_chunk)
        self.num_chunks = len(self.chunk_meta_list)

        self._estimate_inmemory_data_size = sum(
            chunk_meta.byte_count for chunk_meta in self.chunk_meta_list
        )

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return self._estimate_inmemory_data_size

    def _get_read_task(self, index, parallelism):
        # get chunk list to be read in this task
        chunk_index_list = list(np.array_split(range(self.num_chunks), parallelism)[index])

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

        chunk_id_list = [
            self.chunk_meta_list[chunk_index].id
            for chunk_index in chunk_index_list
        ]

        return ReadTask(
            lambda l=chunk_id_list: _read_chunk_fn(l),
            metadata,
        )

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
        chunk_ids = [
            chunk_meta.id
            for chunk_meta in self.chunk_meta_list
        ]
        _unpersist_chunks(chunk_ids)
