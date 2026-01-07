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

    # To reproduce the bug, you need three or more input datasets, and the first dataset
    # can't finish first. To emulate this behavior, the test adds a `map` to the first
    # dataset and materializes the other two.
    ds1 = ray.data.from_items([{"id": 0}]).map(lambda x: x)
    ds2 = ray.data.from_items([{"id": 1}]).materialize()
    ds3 = ray.data.from_items([{"id": 2}]).materialize()
    ds = ds1.union(ds2, ds3)

    assert [row["id"] for row in ds.take_all()] == [0, 1, 2]


def test_union_with_filter(ray_start_10_cpus_shared):
    """Test that filters are pushed through union to both branches."""
    from ray.data._internal.logical.optimizers import LogicalOptimizer
    from ray.data.expressions import col

    ds1 = ray.data.from_items([{"id": 0}, {"id": 1}, {"id": 2}])
    ds2 = ray.data.from_items([{"id": 3}, {"id": 4}, {"id": 5}])
    ds = ds1.union(ds2).filter(expr=col("id") > 2)

    # Verify the filter was pushed through the union
    optimized_plan = LogicalOptimizer().optimize(ds._plan._logical_plan)
    actual_plan_str = optimized_plan.dag.dag_str

    # After optimization, filter should be pushed to both union branches
    # So we should see: Filter(Read), Filter(Read) -> Union
    # Not: Read, Read -> Union -> Filter
    assert "Union" in actual_plan_str
    assert "Filter" in actual_plan_str
    # Ensure Filter is before Union (pushed down), not after
    assert actual_plan_str.index("Filter") < actual_plan_str.index("Union")

    # Verify correctness
    result = sorted(row["id"] for row in ds.take_all())
    assert result == [3, 4, 5]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
