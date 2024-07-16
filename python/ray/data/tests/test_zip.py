import itertools

import pandas as pd
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.util import column_udf, named_values
from ray.tests.conftest import *  # noqa


def test_zip(ray_start_regular_shared):
    ds1 = ray.data.range(5, override_num_blocks=5)
    ds2 = ray.data.range(5, override_num_blocks=5).map(
        column_udf("id", lambda x: x + 1)
    )
    ds = ds1.zip(ds2)
    assert ds.schema().names == ["id", "id_1"]
    assert ds.take() == named_values(
        ["id", "id_1"], [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)]
    )
    with pytest.raises(ValueError):
        ds.zip(ray.data.range(3)).materialize()


@pytest.mark.parametrize(
    "num_blocks1,num_blocks2",
    list(itertools.combinations_with_replacement(range(1, 12), 2)),
)
def test_zip_different_num_blocks_combinations(
    ray_start_regular_shared, num_blocks1, num_blocks2
):
    n = 12
    ds1 = ray.data.range(n, override_num_blocks=num_blocks1)
    ds2 = ray.data.range(n, override_num_blocks=num_blocks2).map(
        column_udf("id", lambda x: x + 1)
    )
    ds = ds1.zip(ds2)
    assert ds.schema().names == ["id", "id_1"]
    assert ds.take() == named_values(
        ["id", "id_1"], list(zip(range(n), range(1, n + 1)))
    )


@pytest.mark.parametrize(
    "num_cols1,num_cols2,should_invert",
    [
        (1, 1, False),
        (4, 1, False),
        (1, 4, True),
        (1, 10, True),
        (10, 10, False),
    ],
)
def test_zip_different_num_blocks_split_smallest(
    ray_start_regular_shared,
    num_cols1,
    num_cols2,
    should_invert,
):
    n = 12
    num_blocks1 = 4
    num_blocks2 = 2
    ds1 = ray.data.from_items(
        [{str(i): i for i in range(num_cols1)}] * n, override_num_blocks=num_blocks1
    )
    ds2 = ray.data.from_items(
        [{str(i): i for i in range(num_cols1, num_cols1 + num_cols2)}] * n,
        override_num_blocks=num_blocks2,
    )
    ds = ds1.zip(ds2).materialize()
    bundles = ds.iter_internal_ref_bundles()
    num_blocks = sum(len(b.block_refs) for b in bundles)
    assert ds.take() == [{str(i): i for i in range(num_cols1 + num_cols2)}] * n
    if should_invert:
        assert num_blocks == num_blocks2
    else:
        assert num_blocks == num_blocks1


def test_zip_pandas(ray_start_regular_shared):
    ds1 = ray.data.from_pandas(pd.DataFrame({"col1": [1, 2], "col2": [4, 5]}))
    ds2 = ray.data.from_pandas(pd.DataFrame({"col3": ["a", "b"], "col4": ["d", "e"]}))
    ds = ds1.zip(ds2)
    assert ds.count() == 2
    assert "{col1: int64, col2: int64, col3: object, col4: object}" in str(ds)
    result = list(ds.take())
    assert result[0] == {"col1": 1, "col2": 4, "col3": "a", "col4": "d"}

    ds3 = ray.data.from_pandas(pd.DataFrame({"col2": ["a", "b"], "col4": ["d", "e"]}))
    ds = ds1.zip(ds3)
    assert ds.count() == 2
    assert "{col1: int64, col2: int64, col2_1: object, col4: object}" in str(ds)
    result = list(ds.take())
    assert result[0] == {"col1": 1, "col2": 4, "col2_1": "a", "col4": "d"}


def test_zip_arrow(ray_start_regular_shared):
    ds1 = ray.data.range(5).map(lambda r: {"id": r["id"]})
    ds2 = ray.data.range(5).map(lambda r: {"a": r["id"] + 1, "b": r["id"] + 2})
    ds = ds1.zip(ds2)
    assert ds.count() == 5
    assert "{id: int64, a: int64, b: int64}" in str(ds)
    result = list(ds.take())
    assert result[0] == {"id": 0, "a": 1, "b": 2}

    # Test duplicate column names.
    ds = ds1.zip(ds1).zip(ds1)
    assert ds.count() == 5
    assert "{id: int64, id_1: int64, id_2: int64}" in str(ds)
    result = list(ds.take())
    assert result[0] == {"id": 0, "id_1": 0, "id_2": 0}


def test_zip_multiple_block_types(ray_start_regular_shared):
    df = pd.DataFrame({"spam": [0]})
    ds_pd = ray.data.from_pandas(df)
    ds2_arrow = ray.data.from_items([{"ham": [0]}])
    assert ds_pd.zip(ds2_arrow).take_all() == [{"spam": 0, "ham": [0]}]


def test_zip_preserve_order(ray_start_regular_shared):
    def foo(x):
        import time

        if x["item"] < 5:
            time.sleep(1)
        return x

    num_items = 10
    items = list(range(num_items))
    ds1 = ray.data.from_items(items, override_num_blocks=num_items)
    ds2 = ray.data.from_items(items, override_num_blocks=num_items)
    ds2 = ds2.map_batches(foo, batch_size=1)
    result = ds1.zip(ds2).take_all()
    assert result == named_values(
        ["item", "item_1"], list(zip(range(num_items), range(num_items)))
    ), result


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
