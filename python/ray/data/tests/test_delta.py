import os
import sys
import tempfile

import pandas as pd
import pytest
from pyspark.sql import SparkSession

import ray.data
from ray.data import read_delta
from ray.util.spark.utils import get_or_create_spark_session_with_delta_extension


@pytest.fixture(scope="module")
def spark():
    with (get_or_create_spark_session_with_delta_extension()) as spark:
        yield spark


def test_read_delta_basic(spark):
    spark_df = spark.createDataFrame(
        [(1, "Alice", 20), (2, "Bob", 40), (3, "Charlie", 60)], ["id", "name", "age"]
    )
    spark_df.createOrReplaceTempView("temp_table")

    with tempfile.TemporaryDirectory() as tempdir:
        spark.sql(
            f"""
            CREATE TABLE delta_table
            USING delta
            LOCATION '{tempdir}'
            AS SELECT * FROM temp_table
        """
        )

        data = read_delta(tempdir)
        assert data.count() == 3
        assert data.columns() == ["id", "name", "age"]
        data_dict = [
            {"id": 1, "name": "Alice", "age": 20},
            {"id": 2, "name": "Bob", "age": 40},
            {"id": 3, "name": "Charlie", "age": 60},
        ]
        assert sorted(data.take_all(), key=lambda x: x["id"]) == data_dict

        # Add rows
        spark.sql(
            """
            INSERT INTO delta_table VALUES
            (4, 'David', 5),
            (5, 'Eva', 50);
        """
        )
        data = read_delta(tempdir)
        assert data.count() == 5
        new_data_dict = [
            {"id": 4, "name": "David", "age": 5},
            {"id": 5, "name": "Eva", "age": 50},
        ]
        assert (
            sorted(data.take_all(), key=lambda x: x["id"]) == data_dict + new_data_dict
        )

        # Delete rows
        spark.sql(
            """
            DELETE FROM delta_table
            WHERE id <= 3
        """
        )
        data = read_delta(tempdir)
        assert data.count() == 2
        assert sorted(data.take_all(), key=lambda x: x["id"]) == new_data_dict


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
    spark = get_or_create_spark_session_with_delta_extension()
    spark_df = spark.range(200).repartition(8)
    with tempfile.TemporaryDirectory() as tempdir:
        spark_df.write.format("delta").save(tempdir)
        data = read_delta(tempdir)
        assert data.count() == 200

    spark.stop()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
