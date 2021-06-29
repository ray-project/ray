import os

import dask.dataframe as dd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import ray

from ray.util.dask import ray_dask_get

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


def test_from_pandas(ray_start_regular_shared):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.experimental.data.from_pandas([ray.put(df1), ray.put(df2)])
    values = [(r["one"], r["two"]) for r in ds.take(6)]
    rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
    assert values == rows


def test_to_pandas(ray_start_regular_shared):
    n = 5
    df = pd.DataFrame({"value": list(range(n))})
    ds = ray.experimental.data.range_arrow(n)
    dfds = pd.concat(ray.get(ds.to_pandas()), ignore_index=True)
    assert df.equals(dfds)


def test_pandas_roundtrip(ray_start_regular_shared):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.experimental.data.from_pandas([ray.put(df1), ray.put(df2)])
    dfds = pd.concat(ray.get(ds.to_pandas()))
    assert pd.concat([df1, df2]).equals(dfds)


def test_parquet_read(ray_start_regular_shared, tmp_path):
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


def test_parquet_write(ray_start_regular_shared, tmp_path):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    df = pd.concat([df1, df2])
    ds = ray.experimental.data.from_pandas([ray.put(df1), ray.put(df2)])
    path = os.path.join(tmp_path, "test_parquet_dir")
    os.mkdir(path)
    ds.write_parquet(path)
    path1 = os.path.join(path, "data0.parquet")
    path2 = os.path.join(path, "data1.parquet")
    dfds = pd.concat([pd.read_parquet(path1), pd.read_parquet(path2)])
    assert df.equals(dfds)


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


def test_map_batch(ray_start_regular_shared, tmp_path):
    # Test input validation
    ds = ray.experimental.data.range(5)
    with pytest.raises(ValueError):
        ds.map_batches(
            lambda x: x + 1, batch_format="pyarrow", batch_size=-1).take()

    # Test pandas
    df = pd.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    table = pa.Table.from_pandas(df)
    pq.write_table(table, os.path.join(tmp_path, "test1.parquet"))
    ds = ray.experimental.data.read_parquet(tmp_path)
    ds_list = ds.map_batches(lambda df: df + 1, batch_size=1).take()
    print(ds_list)
    values = [s["one"] for s in ds_list]
    assert values == [2, 3, 4]
    values = [s["two"] for s in ds_list]
    assert values == [3, 4, 5]

    # Test Pyarrow
    ds = ray.experimental.data.read_parquet(tmp_path)
    ds_list = ds.map_batches(
        lambda pa: pa, batch_size=1, batch_format="pyarrow").take()
    values = [s["one"] for s in ds_list]
    assert values == [1, 2, 3]
    values = [s["two"] for s in ds_list]
    assert values == [2, 3, 4]

    # Test batch
    size = 300
    ds = ray.experimental.data.range(size)
    ds_list = ds.map_batches(lambda df: df + 1, batch_size=17).take(limit=size)
    for i in range(size):
        # The pandas column is "0", and it originally has rows from 0~299.
        # After the map batch, it should have 1~300.
        row = ds_list[i]
        assert row["0"] == i + 1
    assert ds.count() == 300

    # Test the lambda returns different types than the batch_format
    # pandas => list block
    ds = ray.experimental.data.read_parquet(tmp_path)
    ds_list = ds.map_batches(lambda df: [1], batch_size=1).take()
    assert ds_list == [1, 1, 1]
    assert ds.count() == 3

    # pyarrow => list block
    ds = ray.experimental.data.read_parquet(tmp_path)
    ds_list = ds.map_batches(
        lambda df: [1], batch_size=1, batch_format="pyarrow").take()
    assert ds_list == [1, 1, 1]
    assert ds.count() == 3

    # Test the wrong return value raises an exception.
    ds = ray.experimental.data.read_parquet(tmp_path)
    with pytest.raises(ValueError):
        ds_list = ds.map_batches(
            lambda df: 1, batch_size=2, batch_format="pyarrow").take()


def test_from_dask(ray_start_regular_shared):
    df = pd.DataFrame({"one": list(range(100)), "two": list(range(100))})
    ddf = dd.from_pandas(df, npartitions=10)
    ds = ray.experimental.data.from_dask(ddf)
    dfds = pd.concat(ray.get(ds.to_pandas()))
    assert df.equals(dfds)


def test_to_dask(ray_start_regular_shared):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    df = pd.concat([df1, df2])
    ds = ray.experimental.data.from_pandas([ray.put(df1), ray.put(df2)])
    ddf = ds.to_dask()
    # Explicit Dask-on-Ray
    assert df.equals(ddf.compute(scheduler=ray_dask_get))
    # Implicit Dask-on-Ray.
    assert df.equals(ddf.compute())


def test_json_read(ray_start_regular_shared, tmp_path):
    # Single file.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(tmp_path, "test1.json")
    df1.to_json(path1, orient="records", lines=True)
    ds = ray.experimental.data.read_json(path1)
    assert df1.equals(ray.get(ds.to_pandas())[0])

    # Two files, parallelism=2.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(tmp_path, "test2.json")
    df2.to_json(path2, orient="records", lines=True)
    ds = ray.experimental.data.read_json([path1, path2], parallelism=2)
    dsdf = pd.concat(ray.get(ds.to_pandas()))
    assert pd.concat([df1, df2]).equals(dsdf)

    # Three files, parallelism=2.
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    path3 = os.path.join(tmp_path, "test3.json")
    df3.to_json(path3, orient="records", lines=True)
    df = pd.concat([df1, df2, df3], ignore_index=True)
    ds = ray.experimental.data.read_json([path1, path2, path3], parallelism=2)
    dsdf = pd.concat(ray.get(ds.to_pandas()), ignore_index=True)
    assert df.equals(dsdf)


def test_json_write(ray_start_regular_shared, tmp_path):
    # Single block, single file.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    ds = ray.experimental.data.from_pandas([ray.put(df1)])
    path1 = os.path.join(tmp_path, "test1.json")
    ds.write_json(path1)
    assert df1.equals(pd.read_json(path1))
    os.remove(path1)

    # Two blocks, two files.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.experimental.data.from_pandas([ray.put(df1), ray.put(df2)])
    path2 = os.path.join(tmp_path, "test2.json")
    ds.write_json([path1, path2])
    assert pd.concat([df1, df2]).equals(
        pd.concat([pd.read_json(path1),
                   pd.read_json(path2)]))
    os.remove(path1)
    os.remove(path2)

    # Three blocks, two files.
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    ds = ray.experimental.data.from_pandas(
        [ray.put(df1), ray.put(df2), ray.put(df3)])
    ds.write_json([path1, path2])
    assert pd.concat(
        [df1, df2, df3], ignore_index=True).equals(
            pd.concat(
                [pd.read_json(path1), pd.read_json(path2)], ignore_index=True))
    os.remove(path1)
    os.remove(path2)

    # Two blocks, three files.
    ds = ray.experimental.data.from_pandas([ray.put(df1), ray.put(df2)])
    path3 = os.path.join(tmp_path, "test3.json")
    ds.write_json([path1, path2, path3])
    # path3 should never be written since there are only 2 blocks.
    with pytest.raises(ValueError):
        pd.read_json(path3)
    assert pd.concat([df1, df2]).equals(
        pd.concat([pd.read_json(path1),
                   pd.read_json(path2)]))
    os.remove(path1)
    os.remove(path2)


def test_csv_read(ray_start_regular_shared, tmp_path):
    # Single file.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(tmp_path, "test1.csv")
    df1.to_csv(path1, index=False)
    ds = ray.experimental.data.read_csv(path1)
    dsdf = ray.get(ds.to_pandas())[0]
    assert df1.equals(dsdf)

    # Two files, parallelism=2.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(tmp_path, "test2.csv")
    df2.to_csv(path2, index=False)
    ds = ray.experimental.data.read_csv([path1, path2], parallelism=2)
    dsdf = pd.concat(ray.get(ds.to_pandas()))
    df = pd.concat([df1, df2])
    assert df.equals(dsdf)

    # Three files, parallelism=2.
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    path3 = os.path.join(tmp_path, "test3.csv")
    df3.to_csv(path3, index=False)
    ds = ray.experimental.data.read_csv([path1, path2, path3], parallelism=2)
    df = pd.concat([df1, df2, df3], ignore_index=True)
    dsdf = pd.concat(ray.get(ds.to_pandas()), ignore_index=True)
    assert df.equals(dsdf)


def test_csv_write(ray_start_regular_shared, tmp_path):
    # Single block, single file.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    ds = ray.experimental.data.from_pandas([ray.put(df1)])
    path1 = os.path.join(tmp_path, "test1.csv")
    ds.write_csv(path1)
    dsdf = pd.read_csv(path1)
    assert df1.equals(dsdf)
    os.remove(path1)

    # Two blocks, two files.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.experimental.data.from_pandas([ray.put(df1), ray.put(df2)])
    path2 = os.path.join(tmp_path, "test2.csv")
    ds.write_csv([path1, path2])
    df = pd.concat([df1, df2])
    dsdf = pd.concat([pd.read_csv(path1), pd.read_csv(path2)])
    assert df.equals(dsdf)
    os.remove(path1)
    os.remove(path2)

    # Three blocks, two files.
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    ds = ray.experimental.data.from_pandas(
        [ray.put(df1), ray.put(df2), ray.put(df3)])
    ds.write_csv([path1, path2])
    df = pd.concat([df1, df2, df3], ignore_index=True)
    dsdf = pd.concat(
        [pd.read_csv(path1), pd.read_csv(path2)], ignore_index=True)
    assert df.equals(dsdf)
    os.remove(path1)
    os.remove(path2)

    # Two blocks, three files.
    ds = ray.experimental.data.from_pandas([ray.put(df1), ray.put(df2)])
    path3 = os.path.join(tmp_path, "test3.csv")
    ds.write_csv([path1, path2, path3])
    # path3 should never be written since there are only 2 blocks.
    with pytest.raises(FileNotFoundError):
        pd.read_csv(path3)
    df = pd.concat([df1, df2])
    dsdf = pd.concat([pd.read_csv(path1), pd.read_csv(path2)])
    assert df.equals(dsdf)
    os.remove(path1)
    os.remove(path2)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
