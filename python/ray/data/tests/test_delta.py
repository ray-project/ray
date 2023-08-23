import os
import sys
import tempfile

import pytest
from pyspark.sql import SparkSession

from ray.data import read_delta


@pytest.fixture(scope="module")
def spark():
    with (
        SparkSession.builder.config(
            "spark.jars.packages", "io.delta:delta-core_2.12:2.2.0"
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    ) as spark:
        yield spark


def test_read_delta_basic(spark):
    spark = SparkSession.builder.getOrCreate()
    spark_df = spark.range(200).repartition(8)
    with tempfile.TemporaryDirectory() as tempdir:
        uri = os.path.join(tempdir, "DeltaTable")
        spark_df.write.format("delta").save(uri)

        data = read_delta(uri)
        assert data.count() == 200
        assert data.columns() == ["id"]
        assert data.schema().names == ["id"]
        assert data.schema().types[0] == "int64"


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
