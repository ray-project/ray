import pytest
import ray
import mars
import mars.tensor as mt
import mars.dataframe as md


@pytest.fixture(scope="module")
def ray_start_regular(request):  # pragma: no cover
    param = getattr(request, "param", {})
    if not param.get("enable", True):
        yield
    else:
        num_cpus = param.get("num_cpus", 64)
        try:
            yield ray.init(num_cpus=num_cpus)
        finally:
            ray.shutdown()


def test_mars(ray_start_regular):
    cluster = mars.new_cluster_in_ray(worker_num=2)
    df = md.DataFrame(mt.random.rand(1000_0000, 4), columns=list("abcd"))
    # Convert mars dataframe to ray dataset
    import ray.data

    # ds = md.to_ray_dataset(df)
    ds = ray.data.from_mars(df)
    print(ds.schema(), ds.count())
    ds.filter(lambda row: row["a"] > 0.5).show(5)
    # Convert ray dataset to mars dataframe
    # df2 = md.read_ray_dataset(ds)
    df2 = ds.to_mars()
    print(df2.head(5).execute())
    cluster.stop()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
