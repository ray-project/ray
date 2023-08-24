import os
import sys
import tempfile

import pandas as pd
import pytest
from pyspark.sql import SparkSession

import ray.data
from ray.data import read_delta


def get_or_create_spark_session():
    return (
        SparkSession.builder.config(
            "spark.jars.packages", "io.delta:delta-core_2.12:2.2.0"
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


@pytest.fixture(scope="module")
def spark():
    with (get_or_create_spark_session()) as spark:
        yield spark


def test_read_delta_basic(spark):
    spark_df = spark.range(200).repartition(8)
    with tempfile.TemporaryDirectory() as tempdir:
        uri = os.path.join(tempdir, "DeltaTable")
        spark_df.write.format("delta").save(uri)

        data = read_delta(uri)
        assert data.count() == 200
        assert data.columns() == ["id"]
        assert data.schema().names == ["id"]
        assert data.schema().types[0] == "int64"


def test_read_ray_data(spark):
    df = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    ds = ray.data.from_pandas([df])
    with tempfile.TemporaryDirectory() as tempdir:
        ds.write_parquet(tempdir)
        spark.sql(f"CONVERT TO DELTA parquet.`{tempdir}`")

        data = read_delta(tempdir)
        assert data.count() == 3
        assert data.columns() == ["one", "two"]
        assert data.schema().names == ["one", "two"]
        assert data.schema().types[0] == "int64"
        assert data.schema().types[1] == "string"


def test_existing_spark_session_success():
    spark = SparkSession.builder.getOrCreate()
    spark = get_or_create_spark_session()
    spark_df = spark.range(200).repartition(8)
    with tempfile.TemporaryDirectory() as tempdir:
        spark_df.write.format("delta").save(tempdir)
        data = read_delta(tempdir)
        assert data.count() == 200

    spark.stop()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
