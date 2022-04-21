import pytest
import ray
import mars
import mars.tensor as mt
import mars.dataframe as md


@pytest.fixture(scope="module")
def ray_start_regular(request):  # pragma: no cover
    try:
        yield ray.init(num_cpus=16)
    finally:
        ray.shutdown()


@pytest.mark.parametrize("use_mars_api", [True, False])
def test_mars(ray_start_regular, use_mars_api):
    import pandas as pd

    cluster = mars.new_cluster_in_ray(worker_num=2, worker_cpu=1)
    n = 10000
    pdf = pd.DataFrame({"a": list(range(n)), "b": list(range(n, 2 * n))})
    df = md.DataFrame(pdf)

    # Convert mars dataframe to ray dataset
    if use_mars_api:
        ds = md.to_ray_dataset(df)
    else:
        ds = ray.data.from_mars(df)
    pd.testing.assert_dataframe_equal(ds.to_pandas(), df.to_pandas())
    ds2 = ds.filter(lambda row: row["a"] % 2 == 0)
    assert ds2.take(5) == [{"a": 2 * i, "b": n + 2 * i} for i in range(5)]

    # Convert ray dataset to mars dataframe
    if use_mars_api:
        df2 = md.read_ray_dataset(ds2)
    else:
        df2 = ds2.to_mars()
    pd.testing.assert_dataframe_equal(
        df2.head(5).execute(),
        pd.DataFrame({"a": list(range(0, 10, 2)), "b": list(range(n, n + 10, 2))}),
    )
    cluster.stop()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
