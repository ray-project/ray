import ray.util.iter as parallel_it
import ray.util.data as ml_data
import pytest

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import os


def test_read_parquet(ray_start_regular_shared, tmp_path):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    table = pa.Table.from_pandas(df1)
    pq.write_table(table, os.path.join(tmp_path, "test1.parquet"))
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    table = pa.Table.from_pandas(df2)
    pq.write_table(table, os.path.join(tmp_path, "test2.parquet"))

    # without columns
    ds = ml_data.read_parquet(tmp_path, num_shards=2)
    result = list(ds.gather_sync())
    assert df1.equals(result[0])
    assert df2.equals(result[1])

    # with columns one
    ds = ml_data.read_parquet(tmp_path, num_shards=2, columns=["one"])
    result = list(ds.gather_sync())
    assert df1[["one"]].equals(result[0])
    assert df2[["one"]].equals(result[1])

    # with columns two
    ds = ml_data.read_parquet(tmp_path, num_shards=2, columns=["two"])
    result = list(ds.gather_sync())
    assert df1[["two"]].equals(result[0])
    assert df2[["two"]].equals(result[1])


def test_from_parallel_it(ray_start_regular_shared):
    para_it = parallel_it.from_range(4).for_each(lambda x: [x])
    ds = ml_data.from_parallel_iter(para_it, batch_size=2)
    assert repr(ds) == (
        "MLDataset[from_range[4, shards=2]" ".for_each().batch(2).to_pandas()]"
    )
    collected = list(ds.gather_sync())
    assert len(collected) == 2
    assert all(d.shape == (2, 1) for d in collected)
    expected = para_it.flatten().batch(2).gather_sync().flatten()
    flattened = ds.gather_sync().for_each(lambda x: x[0].to_list()).flatten()
    assert list(flattened) == list(expected)


def test_batch(ray_start_regular_shared):
    para_it = parallel_it.from_range(16).for_each(lambda x: [x])
    ds = ml_data.from_parallel_iter(para_it, batch_size=2)
    collected = list(ds.gather_sync())
    assert len(collected) == 8
    assert all(d.shape == (2, 1) for d in collected)

    ds = ds.batch(4)
    assert repr(ds) == (
        "MLDataset[from_range[16, shards=2]"
        ".for_each().batch(2).to_pandas().batch(4)]"
    )
    collected = list(ds.gather_sync())
    assert len(collected) == 4
    assert all(d.shape == (4, 1) for d in collected)
    expected = para_it.flatten().batch(4).gather_sync().flatten()
    flattened = ds.gather_sync().for_each(lambda x: x[0].to_list()).flatten()
    assert list(flattened) == list(expected)


def test_local_shuffle(ray_start_regular_shared):
    para_it = parallel_it.from_range(100).for_each(lambda x: [x])

    # batch_size larger than 1 and shuffle_buffer_size larger than 1
    ds = ml_data.from_parallel_iter(para_it, batch_size=10)
    ds1 = ds.local_shuffle(shuffle_buffer_size=5)
    ds2 = ds.local_shuffle(shuffle_buffer_size=5)

    l1 = list(ds1.gather_sync())
    l2 = list(ds2.gather_sync())
    assert not all(df1.equals(df2) for df1, df2 in zip(l1, l2))

    # batch_size equals 1 and shuffle_buffer_size larger than 1
    ds = ml_data.from_parallel_iter(para_it, batch_size=1)
    ds1 = ds.local_shuffle(shuffle_buffer_size=5)
    ds2 = ds.local_shuffle(shuffle_buffer_size=5)

    l1 = list(ds1.gather_sync())
    l2 = list(ds2.gather_sync())
    assert not all(df1.equals(df2) for df1, df2 in zip(l1, l2))

    # batch_size equals 1 and shuffle_buffer_size equals 1
    ds = ml_data.from_parallel_iter(para_it, batch_size=1)
    ds1 = ds.local_shuffle(shuffle_buffer_size=1)
    ds2 = ds.local_shuffle(shuffle_buffer_size=1)

    l1 = list(ds1.gather_sync())
    l2 = list(ds2.gather_sync())
    assert all(df1.equals(df2) for df1, df2 in zip(l1, l2))


def test_union(ray_start_regular_shared):
    para_it1 = parallel_it.from_range(4, 2, False).for_each(lambda x: [x])
    ds1 = ml_data.from_parallel_iter(para_it1, True, 2, False)
    para_it2 = parallel_it.from_range(4, 2, True).for_each(lambda x: [x])
    ds2 = ml_data.from_parallel_iter(para_it2, True, 2, True)

    with pytest.raises(TypeError) as ex:
        ds1.union(ds2)
    assert "two MLDataset which have different repeated type" in str(ex.value)

    # union two MLDataset with same batch size
    para_it2 = parallel_it.from_range(4, 2, False).for_each(lambda x: [x])
    ds2 = ml_data.from_parallel_iter(para_it2, True, 2, False)
    ds = ds1.union(ds2)
    assert ds.batch_size == 2

    # union two MLDataset with different batch size
    para_it2 = parallel_it.from_range(4, 2, False).for_each(lambda x: [x])
    ds2 = ml_data.from_parallel_iter(para_it2, True, 1, False)
    ds = ds1.union(ds2)
    # batch_size 0 means batch_size unknown
    assert ds.batch_size == 0


@pytest.mark.skipif(
    True, reason="Broken on all platforms (incorrect use of gather_sync())"
)
def test_from_modin(ray_start_regular_shared):
    try:
        import modin.pandas as pd
    except ImportError:
        pytest.mark.skip(reason="Modin is not installed")
        return

    df = pd.DataFrame(np.random.randint(0, 100, size=(2 ** 8, 16))).add_prefix("col")
    ds = ml_data.MLDataset.from_modin(df, 2)
    # Not guaranteed to maintain order, so sort to ensure equality
    assert df._to_pandas().sort_index().equals(ds.gather_sync().sort_index())


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
