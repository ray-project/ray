import gc
import math
import os
import time
import uuid
from contextlib import contextmanager
from typing import NamedTuple
from unittest import mock

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray import cloudpickle as pickle


class ChunkMeta(NamedTuple):
    id: str
    row_count: int
    byte_count: int


@contextmanager
def setup_mock(default_chunk_bytes, tmp_path, monkeypatch):
    """
    `ray.data.from_spark` supports Databricks runtime, but it relies on databricks
    internal APIs.
    So in unit tests, we have to set up mocks for testing it.

    Note that the mocked `persist_df_as_chunks` function accepts
    pandas dataframe as a fake spark dataframe.
    so in unit test we don't need to create real spark dataframe,
    this simplifies unit testing code.
    """

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

            chunk_pdf = pandas_df[start_row_idx:end_row_idx]

            chunk_table = pa.Table.from_pandas(chunk_pdf)

            with open(tmp_path / chunk_id, "wb") as f:
                pickle.dump(chunk_table, f)

            return ChunkMeta(
                id=chunk_id,
                row_count=len(chunk_pdf),
                byte_count=chunk_table.nbytes,
            )

        return [gen_chunk(chunk_idx) for chunk_idx in range(num_chunks)]

    def read_chunk(chunk_id):
        with open(tmp_path / chunk_id, "rb") as f:
            return pickle.load(f)

    read_chunk_fn_path = tmp_path / "read_chunk_fn.pkl"
    with open(read_chunk_fn_path, "wb") as fp:
        pickle.dump(read_chunk, fp)

    MOCK_ENV = "_RAY_DATABRICKS_FROM_SPARK_READ_CHUNK_FN_PATH"

    def unpersist_chunks(chunk_ids):
        for chunk_id in chunk_ids:
            os.remove(os.path.join(tmp_path, chunk_id))

    with mock.patch(
        "ray.data.datasource.spark_datasource.validate_requirements",
        return_value=None,
    ), mock.patch(
        "ray.data.datasource.spark_datasource._persist_dataframe_as_chunks",
        persist_df_as_chunks,
    ), mock.patch(
        "ray.data.datasource.spark_datasource._unpersist_chunks",
        unpersist_chunks,
    ), mock.patch(
        "ray.util.spark.utils.is_in_databricks_runtime",
        return_value=True,
    ), mock.patch(
        "ray.data.datasource.spark_datasource._DEFAULT_SPARK_DATAFRAME_CHUNK_BYTES",
        default_chunk_bytes,
    ):
        monkeypatch.setenv(MOCK_ENV, read_chunk_fn_path.as_posix())
        yield


@pytest.fixture(autouse=True)
def shutdown_ray_per_test():
    yield
    ray.shutdown()


def test_from_simple_databricks_spark_dataframe(tmp_path, monkeypatch):
    fake_spark_df = pd.DataFrame({"x": range(1000)})

    with setup_mock(
        default_chunk_bytes=1000, tmp_path=tmp_path, monkeypatch=monkeypatch
    ):
        ray_ds = ray.data.from_spark(fake_spark_df)
        result = ray_ds.to_pandas()
        del ray_ds
        gc.collect()
        time.sleep(1)  # waiting for ray_ds GC

        # assert all chunk data files are removed from the tmp dir.
        assert [file.name for file in tmp_path.iterdir()] == ["read_chunk_fn.pkl"]

    pd.testing.assert_frame_equal(result, fake_spark_df)


def test_from_mul_cols_databricks_spark_dataframe(tmp_path, monkeypatch):
    fake_spark_df = pd.DataFrame({"x": range(1000)})

    with setup_mock(
        default_chunk_bytes=1000, tmp_path=tmp_path, monkeypatch=monkeypatch
    ):
        ray_ds = ray.data.from_spark(fake_spark_df)
        result = ray_ds.to_pandas()
        del ray_ds

    pd.testing.assert_frame_equal(result, fake_spark_df)


def test_large_size_row_databricks_spark_dataframe(tmp_path, monkeypatch):
    fake_spark_df = pd.DataFrame([{"a": uuid.uuid4().hex * 100} for _ in range(10)])

    with setup_mock(
        default_chunk_bytes=3500, tmp_path=tmp_path, monkeypatch=monkeypatch
    ):
        ray_ds = ray.data.from_spark(fake_spark_df)
        result = ray_ds.to_pandas()
        del ray_ds

    pd.testing.assert_frame_equal(result, fake_spark_df)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
