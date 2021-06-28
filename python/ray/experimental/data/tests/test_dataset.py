import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import ray

from ray.tests.conftest import *  # noqa


def test_basic_actors(shutdown_only):
    ray.init(num_cpus=2)
    ds = ray.experimental.data.range(5)
    assert sorted(ds.map(lambda x: x + 1,
                         compute="actors").take()) == [1, 2, 3, 4, 5]


def test_basic(ray_start_regular_shared):
    ds = ray.experimental.data.range(5)
    assert sorted(ds.map(lambda x: x + 1).take()) == [1, 2, 3, 4, 5]
    assert ds.count() == 5
    assert sorted(ds.iter_rows()) == [0, 1, 2, 3, 4]


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


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
