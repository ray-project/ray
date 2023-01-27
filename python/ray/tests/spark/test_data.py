from ray.util.spark import load_spark_dataset
import pytest
import os
from pyspark.sql import SparkSession
import sys
import tempfile


@pytest.fixture(scope="module", autouse=True)
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark
    spark.stop()


@pytest.mark.parametrize("saved_format", ["parquet", "delta"])
def test_load_spark_dataset(spark, saved_format):
    spark_df = spark.range(200).repartition(8)

    with tempfile.TemporaryDirectory() as tempdir:
        spark_df.write.save(tempdir, format=saved_format)
        ray_dataset = load_spark_dataset(tempdir, saved_format=saved_format)
        pdf = ray_dataset.to_pandas()
        assert pdf.id.tolist() == list(range(200))


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
