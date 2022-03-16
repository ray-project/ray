import pytest
import ray
import raydp


@pytest.fixture(scope="function")
def spark_on_ray_small(request):
    ray.init(num_cpus=2, include_dashboard=False)
    spark = raydp.init_spark("test", 1, 1, "500 M")

    def stop_all():
        raydp.stop_spark()
        ray.shutdown()

    request.addfinalizer(stop_all)
    return spark


@pytest.mark.skip(
    reason=(
        "raydp.spark.spark_dataframe_to_ray_dataset needs to be updated to "
        "use ray.data.from_arrow_refs."
    )
)
def test_raydp_roundtrip(spark_on_ray_small):
    spark = spark_on_ray_small
    spark_df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["one", "two"])
    rows = [(r.one, r.two) for r in spark_df.take(3)]
    ds = ray.data.from_spark(spark_df)
    values = [(r["one"], r["two"]) for r in ds.take(6)]
    assert values == rows
    df = ds.to_spark(spark)
    rows_2 = [(r.one, r.two) for r in df.take(3)]
    assert values == rows_2


@pytest.mark.skip(
    reason="raydp need to be updated to work without redis.",
)
def test_raydp_to_spark(spark_on_ray_small):
    spark = spark_on_ray_small
    n = 5
    ds = ray.data.range_arrow(n)
    values = [r["value"] for r in ds.take(5)]
    df = ds.to_spark(spark)
    rows = [r.value for r in df.take(5)]
    assert values == rows


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
