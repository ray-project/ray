import pytest
import ray
from ray.data.tests.test_execution_optimizer import _check_usage_record
import torch
from ray.data.tests.conftest import *  # noqa
import pandas
import raydp


# RayDP tests require Ray Java. Make sure ray jar is built before running this test.
@pytest.fixture(scope="function")
def spark(request):
    ray.init(num_cpus=2, include_dashboard=False)
    spark_session = raydp.init_spark("test", 1, 1, "500M")

    def stop_all():
        raydp.stop_spark()
        ray.shutdown()

    request.addfinalizer(stop_all)
    return spark_session


def test_raydp_roundtrip(spark):
    spark_df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["one", "two"])
    rows = [(r.one, r.two) for r in spark_df.take(3)]
    ds = ray.data.from_spark(spark_df)
    values = [(r["one"], r["two"]) for r in ds.take(6)]
    assert values == rows
    df = ds.to_spark(spark)
    rows_2 = [(r.one, r.two) for r in df.take(3)]
    assert values == rows_2


def test_raydp_to_spark(spark):
    n = 5
    ds = ray.data.range(n)
    values = [r["id"] for r in ds.take(5)]
    df = ds.to_spark(spark)
    rows = [r.id for r in df.take(5)]
    assert values == rows


def test_from_spark_e2e(enable_optimizer, spark):
    spark_df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["one", "two"])

    rows = [(r.one, r.two) for r in spark_df.take(3)]
    ds = ray.data.from_spark(spark_df)
    assert len(ds.take_all()) == len(rows)
    values = [(r["one"], r["two"]) for r in ds.take(6)]
    assert values == rows

    # Check that metadata fetch is included in stats.
    assert "FromArrowRefs" in ds.stats()
    # Underlying implementation uses `FromArrowRefs` operator
    assert ds._plan._logical_plan.dag.name == "FromArrowRefs"
    _check_usage_record(["FromArrowRefs"])


def test_raydp_to_torch_iter(spark):
    spark_df = spark.createDataFrame([(1, 0), (2, 0), (3, 1)], ["feature", "label"])
    data_size = spark_df.count()
    features = [r["feature"] for r in spark_df.take(data_size)]
    features = torch.tensor(features).reshape(data_size, 1)
    labels = [r["label"] for r in spark_df.take(data_size)]
    labels = torch.tensor(labels).reshape(data_size, 1)
    ds = ray.data.from_spark(spark_df)
    dataset = ds.to_torch(label_column="label", batch_size=3)
    data_features, data_labels = next(dataset.__iter__())
    assert torch.equal(data_features, features) and torch.equal(data_labels, labels)


def test_to_pandas(spark):
    df = spark.range(100)
    ds = ray.data.from_spark(df)
    pdf = ds.to_pandas()
    pdf2 = df.toPandas()
    pandas.testing.assert_frame_equal(pdf, pdf2)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
