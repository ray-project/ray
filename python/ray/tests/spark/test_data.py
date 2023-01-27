from ray.util.spark import load_spark_dataset
from ray.util.spark.data import _convert_dbfs_path_to_local_path
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


def test_load_spark_dataset(spark):
    spark_df = spark.range(200).repartition(8)

    with tempfile.TemporaryDirectory() as tempdir:
        save_path = os.path.join(tempdir, "df")
        spark_df.write.save(save_path, format="parquet")
        ray_dataset = load_spark_dataset(save_path, saved_format="parquet")
        pdf = ray_dataset.to_pandas()
        assert set(pdf.id.tolist()) == set(range(200))


def test_convert_dbfs_path_to_local_path():
    assert _convert_dbfs_path_to_local_path("dbfs:/xx/yy/zz") == "/dbfs/xx/yy/zz"
    assert _convert_dbfs_path_to_local_path("dbfs:///xx/yy/zz") == "/dbfs/xx/yy/zz"


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
