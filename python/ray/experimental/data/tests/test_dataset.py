import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import ray

from ray.tests.conftest import *  # noqa


def test_basic(ray_start_regular_shared):
    ds = ray.experimental.data.range(5)
    assert sorted(ds.map(lambda x: x + 1).take()) == [1, 2, 3, 4, 5]
    assert ds.count() == 5
    assert sorted(ds.to_local_iterator()) == [0, 1, 2, 3, 4]


def test_convert_types(ray_start_regular_shared):
    plain_ds = ray.experimental.data.range(1)
    arrow_ds = plain_ds.map(lambda x: {"a": x})
    assert arrow_ds.take() == [{"a": 0}]
    assert "ArrowRow" in arrow_ds.map(lambda x: str(x)).take()[0]

    arrow_ds = ray.experimental.data.range_arrow(1)
    assert arrow_ds.map(lambda x: "plain_{}".format(x["value"])).take() \
        == ["plain_0"]
    assert arrow_ds.map(lambda x: {"a": (x["value"],)}).take() == \
        [{"a": (0,)}]


def test_from_items(ray_start_regular_shared):
    ds = ray.experimental.data.from_items(["hello", "world"])
    assert ds.take() == ["hello", "world"]


def test_repartition(ray_start_regular_shared):
    ds = ray.experimental.data.range(20, parallelism=10)
    assert ds.num_blocks() == 10
    assert ds.sum() == 190
    assert ds._block_sizes() == [2] * 10

    ds2 = ds.repartition(5)
    assert ds2.num_blocks() == 5
    assert ds2.sum() == 190
    # TODO: would be nice to re-distribute these more evenly
    ds2._block_sizes() == [10, 10, 0, 0, 0]

    ds3 = ds2.repartition(20)
    assert ds3.num_blocks() == 20
    assert ds3.sum() == 190
    ds2._block_sizes() == [2] * 10 + [0] * 10

    large = ray.experimental.data.range(10000, parallelism=10)
    large = large.repartition(20)
    assert large._block_sizes() == [500] * 20


def test_repartition_arrow(ray_start_regular_shared):
    ds = ray.experimental.data.range_arrow(20, parallelism=10)
    assert ds.num_blocks() == 10
    assert ds.count() == 20
    assert ds._block_sizes() == [2] * 10

    ds2 = ds.repartition(5)
    assert ds2.num_blocks() == 5
    assert ds2.count() == 20
    ds2._block_sizes() == [10, 10, 0, 0, 0]

    ds3 = ds2.repartition(20)
    assert ds3.num_blocks() == 20
    assert ds3.count() == 20
    ds2._block_sizes() == [2] * 10 + [0] * 10

    large = ray.experimental.data.range_arrow(10000, parallelism=10)
    large = large.repartition(20)
    assert large._block_sizes() == [500] * 20


def test_parquet(ray_start_regular_shared, tmp_path):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    table = pa.Table.from_pandas(df1)
    pq.write_table(table, os.path.join(tmp_path, "test1.parquet"))
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    table = pa.Table.from_pandas(df2)
    pq.write_table(table, os.path.join(tmp_path, "test2.parquet"))

    ds = ray.experimental.data.read_parquet(tmp_path)
    values = [[s["one"], s["two"]] for s in ds.take()]

    assert sorted(values) == [[4, "e"], [4, "e"], [5, "f"], [5, "f"], [6, "g"],
                              [6, "g"]]


def test_pyarrow(ray_start_regular_shared):
    ds = ray.experimental.data.range_arrow(5)
    assert ds.map(lambda x: {"b": x["value"] + 2}).take() == \
        [{"b": 2}, {"b": 3}, {"b": 4}, {"b": 5}, {"b": 6}]
    assert ds.map(lambda x: {"b": x["value"] + 2}) \
        .filter(lambda x: x["b"] % 2 == 0).take() == \
        [{"b": 2}, {"b": 4}, {"b": 6}]
    assert ds.filter(lambda x: x["value"] == 0) \
        .flat_map(lambda x: [{"b": x["value"] + 2}, {"b": x["value"] + 20}]) \
        .take() == [{"b": 2}, {"b": 20}]


def test_map_batch(ray_start_regular_shared):
    # Test Pyarrow
    ds = ray.experimental.data.range(5)
    assert sorted(
        ds.map_batches(lambda x: x + 1,
                       batch_format="arrow").take()) == [1, 2, 3, 4, 5]
    assert ds.count() == 5
    assert sorted(ds.to_local_iterator()) == [0, 1, 2, 3, 4]

    # Test pandas
    ds = ray.experimental.data.range(5)
    ds = ds.map_batches(lambda df: df.count()).take()
    for d in ds:
        # count of each data frame is always 1 in this test example.
        assert d.values == [1]
    assert len(ds) == 5

    # Test pandas batch
    size = 600
    parallelism = 200
    ds = ray.experimental.data.range(size, parallelism=parallelism)
    # Choose a value that cannot divide cleanly to test the edge case.
    # Add 1 to each row at the dataframe.
    ds = ds.map_batches(lambda df: df + 1, batch_size=7)

    # Make sure the total rows are correctly set.
    assert ds.count() == size
    # Make sure the block size is correctly set. When parallelism is 200,
    # there are 600 // 200 rows per block, meaning there are 200 blocks.
    assert ds.num_blocks() == size // (size // parallelism)

    # Check all rows have added 1 to it.
    ds = ds.take(limit=size)
    for i in range(600):
        dict_row = ds[i]
        row = list(dict_row.values())[0]
        assert row == i + 1
    assert len(ds) == size


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
