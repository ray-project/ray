import time

import pyarrow as pa
import pytest

import ray


def test_union_basic(ray_start_10_cpus_shared):
    ds1 = ray.data.from_items([{"id": 0}])
    ds2 = ray.data.from_items([{"id": 1}])
    ds = ds1.union(ds2)
    assert sorted(row["id"] for row in ds.take_all()) == [0, 1]


def test_union_schema(ray_start_10_cpus_shared):
    ds = ray.data.range(1).union(ray.data.range(1))
    assert ds.schema().names == ["id"]
    assert ds.schema().types == [pa.int64()]


def test_union_repr(ray_start_10_cpus_shared):
    ds = ray.data.range(1).union(ray.data.range(1))
    assert repr(ds) == "Union\n+- Dataset(num_rows=?, schema=Unknown schema)"


def test_union_with_preserve_order(ray_start_10_cpus_shared, restore_data_context):
    # Test for https://github.com/ray-project/ray/issues/41524
    ctx = ray.data.DataContext.get_current()
    ctx.execution_options.preserve_order = True

    def slow_map(row):
        time.sleep(0.01)
        return row

    # To reproduce the bug, you need three or more input datasets, and the first dataset
    # can't finish first.
    ds1 = ray.data.from_items([{"id": 0}]).map(slow_map)
    ds2 = ray.data.from_items([{"id": 1}])
    ds3 = ray.data.from_items([{"id": 2}])
    ds = ds1.union(ds2, ds3)

    assert [row["id"] for row in ds.take_all()] == [0, 1, 2]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
