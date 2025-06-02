import pytest
import ray


def test_partial_sql_duckdb():
    ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")

    sql_ds = ds.map_sql(
        query="SELECT b['variety'] as variety, b['sepal.length'] as sepal_length FROM my_batch as b",
        view_name="my_batch",
        engine="duckdb",
    )

    res = sql_ds.take(1)

    assert len(res) == 1
    assert "variety" in res[0] and "sepal_length" in res[0]
    assert isinstance(res[0]["variety"], str)
    assert isinstance(res[0]["sepal_length"], float)


def test_partial_sql_polars():
    ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")

    sql_ds = ds.map_sql(
        query="SELECT b['variety'] as variety, b['sepal.length'] as sepal_length FROM my_batch as b",
        view_name="my_batch",
        engine="polars",
    )

    res = sql_ds.take(1)

    assert len(res) == 1
    assert "variety" in res[0] and "sepal_length" in res[0]
    assert isinstance(res[0]["variety"], str)
    assert isinstance(res[0]["sepal_length"], float)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
