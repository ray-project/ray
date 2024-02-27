import os
from typing import Iterator, List, Optional

import numpy as np
import pyarrow

from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.util.annotations import PublicAPI

_DEFAULT_SPARK_DATAFRAME_CHUNK_BYTES = 32 * 1024 * 1024


def validate_requirements():
    from ray.util.spark.utils import get_spark_session

    spark = get_spark_session()
    if (
        spark.conf.get(
            "spark.databricks.pyspark.dataFrameChunk.enabled", "false"
        ).lower()
        != "true"
    ):
        raise RuntimeError(
            "In databricks runtime, if you want to use 'ray.data.from_spark' API, "
            "you need to set spark cluster config "
            "'spark.databricks.pyspark.dataFrameChunk.enabled' to 'true'."
        )

    if spark.conf.get("spark.databricks.acl.dfAclsEnabled", "false").lower() != "false":
        raise RuntimeError(
            "In databricks runtime, if you want to use 'ray.data.from_spark' API, "
            "you must use an assigned mode databricks cluster."
        )


def _persist_dataframe_as_chunks(spark_dataframe, bytes_per_chunk):
    try:
        from pyspark.databricks.sql.chunk import persistDataFrameAsChunks
    except ImportError:
        raise RuntimeError(
            "Current Databricks Runtime version does not support Ray SparkDatasource."
        )
    return persistDataFrameAsChunks(spark_dataframe, bytes_per_chunk)


def _read_chunk_fn(chunk_ids) -> Iterator["pyarrow.Table"]:
    if read_chunk_fn_path := os.environ.get(
        "_RAY_DATABRICKS_FROM_SPARK_READ_CHUNK_FN_PATH"
    ):
        import ray.cloudpickle as pickle

        # This is for testing.
        with open(read_chunk_fn_path, "rb") as f:
            readChunk = pickle.load(f)

    else:
        from pyspark.databricks.sql.chunk import readChunk

    yield from map(readChunk, chunk_ids)


def _unpersist_chunks(chunk_ids):
    from pyspark.databricks.sql.chunk import unpersistChunks

    return unpersistChunks(chunk_ids)


@PublicAPI(stability="alpha")
class DatabricksSparkDatasource(Datasource):
    """Spark datasource, for reading data from a Spark dataframe in Databricks
    runtime.
    """

    def __init__(
        self, spark_dataframe, bytes_per_chunk=_DEFAULT_SPARK_DATAFRAME_CHUNK_BYTES
    ):
        """
        Args:
            spark_dataframe: A `Spark DataFrame`_
            bytes_per_chunk: The chunk size to use when the Spark dataframe
                is split into chunks.
        """
        self.chunk_meta_list = _persist_dataframe_as_chunks(
            spark_dataframe, bytes_per_chunk
        )
        self.num_chunks = len(self.chunk_meta_list)

        self._estimate_inmemory_data_size = sum(
            chunk_meta.byte_count for chunk_meta in self.chunk_meta_list
        )

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return self._estimate_inmemory_data_size

    def _get_read_task(self, index, parallelism):
        # get chunk list to be read in this task
        chunk_index_list = list(
            np.array_split(range(self.num_chunks), parallelism)[index]
        )

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

        chunk_ids = [
            self.chunk_meta_list[chunk_index].id for chunk_index in chunk_index_list
        ]

        return ReadTask(
            lambda: _read_chunk_fn(chunk_ids),
            metadata,
        )

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        assert parallelism > 0, f"Invalid parallelism {parallelism}"

        parallelism = min(parallelism, self.num_chunks)

        return [self._get_read_task(index, parallelism) for index in range(parallelism)]

    def get_name(self):
        """Return a human-readable name for this datasource.
        This will be used as the names of the read tasks.
        """
        return "Spark"

    def dispose_spark_cache(self):
        _unpersist_chunks([chunk_meta.id for chunk_meta in self.chunk_meta_list])

    def __del__(self):
        try:
            self.dispose_spark_cache()
        except Exception:
            pass
