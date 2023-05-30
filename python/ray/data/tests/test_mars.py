import pytest
import ray
import mars
import mars.dataframe as md
import pyarrow as pa

from ray.data.tests.test_execution_optimizer import _check_usage_record  # noqa


@pytest.fixture(scope="module")
def ray_start_regular(request):  # pragma: no cover
    try:
        yield ray.init(num_cpus=16)
    finally:
        ray.shutdown()


def test_mars(ray_start_regular):
    import pandas as pd

    cluster = mars.new_cluster_in_ray(worker_num=2, worker_cpu=1)
    n = 10000
    pdf = pd.DataFrame({"a": list(range(n)), "b": list(range(n, 2 * n))})
    df = md.DataFrame(pdf)

    # Convert mars dataframe to ray dataset
    ds = ray.data.from_mars(df)
    pd.testing.assert_frame_equal(ds.to_pandas(), df.to_pandas())
    ds2 = ds.filter(lambda row: row["a"] % 2 == 0)
    assert ds2.take(5) == [{"a": 2 * i, "b": n + 2 * i} for i in range(5)]

    # Convert ray dataset to mars dataframe
    df2 = ds2.to_mars()
    pd.testing.assert_frame_equal(
        df2.head(5).to_pandas(),
        pd.DataFrame({"a": list(range(0, 10, 2)), "b": list(range(n, n + 10, 2))}),
    )

    # Test Arrow Dataset
    pdf2 = pd.DataFrame({c: range(5) for c in "abc"})
    ds3 = ray.data.from_arrow([pa.Table.from_pandas(pdf2) for _ in range(3)])
    df3 = ds3.to_mars()
    pd.testing.assert_frame_equal(
        df3.head(5).to_pandas(),
        pdf2,
    )

    cluster.stop()


def test_from_mars_e2e(ray_start_regular, enable_optimizer):
    import pandas as pd

    cluster = mars.new_cluster_in_ray(worker_num=2, worker_cpu=1)
    n = 10000
    pdf = pd.DataFrame({"a": list(range(n)), "b": list(range(n, 2 * n))})
    df = md.DataFrame(pdf)

    # Convert mars dataframe to ray dataset
    ds = ray.data.from_mars(df)
    # `ds.take_all()` triggers execution with new backend, which is
    # needed for checking operator usage below.
    assert len(ds.take_all()) == len(df)
    pd.testing.assert_frame_equal(ds.to_pandas(), df.to_pandas())

    # Check that metadata fetch is included in stats.
    assert "FromPandasRefs" in ds.stats()
    # Underlying Mars implementation uses `FromPandasRefs` operator
    assert ds._plan._logical_plan.dag.name == "FromMars"
    _check_usage_record(["FromMars"])

    ds2 = ds.filter(lambda row: row["a"] % 2 == 0)
    assert ds2.take(5) == [{"a": 2 * i, "b": n + 2 * i} for i in range(5)]
    assert "Filter" in ds2.stats()
    assert ds2._plan._logical_plan.dag.name == "Filter"

    # Convert ray dataset to mars dataframe
    df2 = ds2.to_mars()
    pd.testing.assert_frame_equal(
        df2.head(5).to_pandas(),
        pd.DataFrame({"a": list(range(0, 10, 2)), "b": list(range(n, n + 10, 2))}),
    )
    _check_usage_record(["Filter", "FromMars"])

    # Test Arrow Dataset
    pdf2 = pd.DataFrame({c: range(5) for c in "abc"})
    ds3 = ray.data.from_arrow([pa.Table.from_pandas(pdf2) for _ in range(3)])
    assert len(ds3.take_all())
    df3 = ds3.to_mars()
    pd.testing.assert_frame_equal(
        df3.head(5).to_pandas(),
        pdf2,
    )
    assert "FromArrowRefs" in ds3.stats()
    assert ds3._plan._logical_plan.dag.name == "FromArrowRefs"
    _check_usage_record(["FromArrowRefs"])

    cluster.stop()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
