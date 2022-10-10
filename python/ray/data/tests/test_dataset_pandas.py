import pytest
import pandas as pd
import numpy as np

import ray

from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa


@pytest.mark.parametrize("enable_pandas_block", [False, True])
def test_from_pandas(ray_start_regular_shared, enable_pandas_block):
    ctx = ray.data.context.DatasetContext.get_current()
    old_enable_pandas_block = ctx.enable_pandas_block
    ctx.enable_pandas_block = enable_pandas_block
    try:
        df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
        df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
        ds = ray.data.from_pandas([df1, df2])
        assert ds._dataset_format() == "pandas" if enable_pandas_block else "arrow"
        values = [(r["one"], r["two"]) for r in ds.take(6)]
        rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
        assert values == rows
        # Check that metadata fetch is included in stats.
        assert "from_pandas_refs" in ds.stats()

        # test from single pandas dataframe
        ds = ray.data.from_pandas(df1)
        assert ds._dataset_format() == "pandas" if enable_pandas_block else "arrow"
        values = [(r["one"], r["two"]) for r in ds.take(3)]
        rows = [(r.one, r.two) for _, r in df1.iterrows()]
        assert values == rows
        # Check that metadata fetch is included in stats.
        assert "from_pandas_refs" in ds.stats()
    finally:
        ctx.enable_pandas_block = old_enable_pandas_block


@pytest.mark.parametrize("enable_pandas_block", [False, True])
def test_from_pandas_refs(ray_start_regular_shared, enable_pandas_block):
    ctx = ray.data.context.DatasetContext.get_current()
    old_enable_pandas_block = ctx.enable_pandas_block
    ctx.enable_pandas_block = enable_pandas_block
    try:
        df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
        df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
        ds = ray.data.from_pandas_refs([ray.put(df1), ray.put(df2)])
        assert ds._dataset_format() == "pandas" if enable_pandas_block else "arrow"
        values = [(r["one"], r["two"]) for r in ds.take(6)]
        rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
        assert values == rows
        # Check that metadata fetch is included in stats.
        assert "from_pandas_refs" in ds.stats()

        # test from single pandas dataframe ref
        ds = ray.data.from_pandas_refs(ray.put(df1))
        assert ds._dataset_format() == "pandas" if enable_pandas_block else "arrow"
        values = [(r["one"], r["two"]) for r in ds.take(3)]
        rows = [(r.one, r.two) for _, r in df1.iterrows()]
        assert values == rows
        # Check that metadata fetch is included in stats.
        assert "from_pandas_refs" in ds.stats()
    finally:
        ctx.enable_pandas_block = old_enable_pandas_block


def test_to_pandas(ray_start_regular_shared):
    n = 5
    df = pd.DataFrame({"value": list(range(n))})
    ds = ray.data.range_table(n)
    dfds = ds.to_pandas()
    assert df.equals(dfds)

    # Test limit.
    with pytest.raises(ValueError):
        dfds = ds.to_pandas(limit=3)

    # Test limit greater than number of rows.
    dfds = ds.to_pandas(limit=6)
    assert df.equals(dfds)


def test_to_pandas_refs(ray_start_regular_shared):
    n = 5
    df = pd.DataFrame({"value": list(range(n))})
    ds = ray.data.range_table(n)
    dfds = pd.concat(ray.get(ds.to_pandas_refs()), ignore_index=True)
    assert df.equals(dfds)


def test_pandas_roundtrip(ray_start_regular_shared, tmp_path):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_pandas([df1, df2])
    dfds = ds.to_pandas()
    assert pd.concat([df1, df2], ignore_index=True).equals(dfds)


def test_read_pandas_data_array_column(ray_start_regular_shared):
    df = pd.DataFrame(
        {
            "one": [1, 2, 3],
            "array": [
                np.array([1, 1, 1]),
                np.array([2, 2, 2]),
                np.array([3, 3, 3]),
            ],
        }
    )
    ds = ray.data.from_pandas(df)
    row = ds.take(1)[0]
    assert row["one"] == 1
    assert all(row["array"] == [1, 1, 1])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
