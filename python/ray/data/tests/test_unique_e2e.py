import pandas as pd
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

RANDOM_SEED = 123


def test_unique(ray_start_regular_shared_2_cpus, disable_fallback_to_object_extension):
    ds = ray.data.from_items([3, 2, 3, 1, 2, 3])
    assert set(ds.unique("item")) == {1, 2, 3}

    ds = ray.data.from_items(
        [
            {"a": 1, "b": 1},
            {"a": 1, "b": 2},
        ]
    )
    assert set(ds.unique("a")) == {1}


@pytest.mark.parametrize("batch_format", ["pandas", "pyarrow"])
def test_unique_with_nulls(
    ray_start_regular_shared_2_cpus, batch_format, disable_fallback_to_object_extension
):
    ds = ray.data.from_items([3, 2, 3, 1, 2, 3, None])
    assert set(ds.unique("item")) == {1, 2, 3, None}
    assert len(ds.unique("item")) == 4

    ds = ray.data.from_items(
        [
            {"a": 1, "b": 1},
            {"a": 1, "b": 2},
            {"a": 1, "b": None},
            {"a": None, "b": 3},
            {"a": None, "b": 4},
        ]
    )
    assert set(ds.unique("a")) == {1, None}
    assert len(ds.unique("a")) == 2
    assert set(ds.unique("b")) == {1, 2, 3, 4, None}
    assert len(ds.unique("b")) == 5

    # Check with 3 columns
    df = pd.DataFrame(
        {
            "col1": [1, 2, None, 3, None, 3, 2],
            "col2": [None, 2, 2, 3, None, 3, 2],
            "col3": [1, None, 2, None, None, None, 2],
        }
    )
    # df["col"].unique() works fine, as expected
    ds2 = ray.data.from_pandas(df)
    ds2 = ds2.map_batches(lambda x: x, batch_format=batch_format)
    assert set(ds2.unique("col1")) == {1, 2, 3, None}
    assert len(ds2.unique("col1")) == 4
    assert set(ds2.unique("col2")) == {2, 3, None}
    assert len(ds2.unique("col2")) == 3
    assert set(ds2.unique("col3")) == {1, 2, None}
    assert len(ds2.unique("col3")) == 3

    # Check with 3 columns and different dtypes
    df = pd.DataFrame(
        {
            "col1": [1, 2, None, 3, None, 3, 2],
            "col2": [None, 2, 2, 3, None, 3, 2],
            "col3": [1, None, 2, None, None, None, 2],
        }
    )
    df["col1"] = df["col1"].astype("Int64")
    df["col2"] = df["col2"].astype("Float64")
    df["col3"] = df["col3"].astype("string")
    ds3 = ray.data.from_pandas(df)
    ds3 = ds3.map_batches(lambda x: x, batch_format=batch_format)
    assert set(ds3.unique("col1")) == {1, 2, 3, None}
    assert len(ds3.unique("col1")) == 4
    assert set(ds3.unique("col2")) == {2, 3, None}
    assert len(ds3.unique("col2")) == 3
    assert set(ds3.unique("col3")) == {"1.0", "2.0", None}
    assert len(ds3.unique("col3")) == 3


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
