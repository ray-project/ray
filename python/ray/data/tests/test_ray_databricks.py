import os
import shutil
import time
import pytest
import pyarrow as pa
import pandas as pd
from ray import cloudpickle as pickle
import uuid
import math
import tempfile
from collections import namedtuple
from contextlib import contextmanager
from unittest import mock
import ray


ChunkMeta = namedtuple('ChunkMeta', ['id', 'row_count', 'byte_count'])


@contextmanager
def setup_mock(default_chunk_bytes, tmp_dir=None):
    """
    `ray.data.from_spark` supports Databricks runtime, but it relies on databricks
    internal APIs.
    So in unit tests, we have to set up mocks for testing it.

    Note that the mocked `persist_df_as_chunks` function accepts
    pandas dataframe as a fake spark dataframe.
    so in unit test we don't need to create real spark dataframe,
    this simplifies unit testing code.
    """
    tmp_dir = tmp_dir or tempfile.mkdtemp()

    def persist_df_as_chunks(pandas_df, bytes_per_chunk):
        arrow_tb = pa.Table.from_pandas(pandas_df)

        total_nbytes = arrow_tb.nbytes
        num_rows = len(pandas_df)

        num_chunks = math.ceil(total_nbytes / bytes_per_chunk)
        rows_per_chunk = num_rows // num_chunks

        def gen_chunk(chunk_idx):
            chunk_id = uuid.uuid4().hex

            start_row_idx = chunk_idx * rows_per_chunk
            end_row_idx = start_row_idx + rows_per_chunk

            chunk_pdf = pandas_df[start_row_idx: end_row_idx]

            chunk_table = pa.Table.from_pandas(chunk_pdf)

            with open(os.path.join(tmp_dir, chunk_id), "wb") as f:
                pickle.dump(chunk_table, f)

            return ChunkMeta(
                id=chunk_id,
                row_count=len(chunk_pdf),
                byte_count=chunk_table.nbytes,
            )

        return [
            gen_chunk(chunk_idx)
            for chunk_idx in range(num_chunks)
        ]

    def read_chunk(chunk_id):
        with open(os.path.join(tmp_dir, chunk_id), "rb") as f:
            return pickle.load(f)

    read_chunk_fn_path = os.path.join(tmp_dir, "read_chunk_fn.pkl")
    with open(read_chunk_fn_path, "wb") as fp:
        pickle.dump(read_chunk, fp)

    MOCK_ENV = "_RAY_DATABRICKS_FROM_SPARK_READ_CHUNK_FN_PATH"

    def unpersist_chunk(chunk_id):
        os.remove(os.path.join(tmp_dir, chunk_id))

    def is_in_databricks_runtime():
        return True

    with mock.patch(
        "ray.data.datasource.spark_datasource.check_requirements",
        return_value=None,
    ), mock.patch(
        "ray.data.datasource.spark_datasource._persist_dataframe_as_chunks",
        persist_df_as_chunks,
    ), mock.patch(
        "ray.data.datasource.spark_datasource._unpersist_chunks",
        unpersist_chunk,
    ), mock.patch(
        "ray.util.spark.utils.is_in_databricks_runtime",
        is_in_databricks_runtime,
    ), mock.patch(
        "ray.data.read_api._DATABRICKS_SPARK_DATAFRAM_CHUNK_BYTES",
        default_chunk_bytes
    ), mock.patch.dict(os.environ, {
        MOCK_ENV: read_chunk_fn_path,
    }):
        yield


def test_from_simple_databricks_spark_dataframe():
    fake_spark_df = pd.DataFrame({
        "x": range(1000)
    })

    tmp_dir = tempfile.mkdtemp()
    with setup_mock(default_chunk_bytes=1000, tmp_dir=tmp_dir):
        ray_ds = ray.data.from_spark(fake_spark_df)
        result = ray_ds.to_pandas()
        del ray_ds

    pd.testing.assert_frame_equal(result, fake_spark_df)

    time.sleep(1)  # waiting for ray_ds GC

    # assert all chunk data files are removed from the tmp dir.
    os.listdir(tmp_dir) == ['read_chunk_fn.pkl']

    ray.shutdown()


def test_from_mul_cols_databricks_spark_dataframe():
    fake_spark_df = pd.DataFrame({
        "x": range(1000)
    })

    with setup_mock(default_chunk_bytes=1000):
        ray_ds = ray.data.from_spark(fake_spark_df)
        result = ray_ds.to_pandas()
        del ray_ds

    pd.testing.assert_frame_equal(result, fake_spark_df)
    ray.shutdown()


def test_large_size_row_databricks_spark_dataframe():
    fake_spark_df = pd.DataFrame([
        {
            'a': "".join([
                uuid.uuid4().hex
                for _ in range(100)
            ])
        } for _ in range(10)
    ])

    with setup_mock(default_chunk_bytes=3500):
        ray_ds = ray.data.from_spark(fake_spark_df)
        result = ray_ds.to_pandas()
        del ray_ds

    pd.testing.assert_frame_equal(result, fake_spark_df)
    ray.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
