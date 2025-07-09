import pytest

import ray


def test_basic_sql_polars():
    ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")
    sql_ds = ds.map_sql(
        query="SELECT variety as variety, `sepal.length` as sepal_length FROM my_batch",
        view_name="my_batch",
        engine="polars",
    )
    res = sql_ds.take(1)

    assert len(res) == 1
    assert "variety" in res[0] and "sepal_length" in res[0]
    assert isinstance(res[0]["variety"], str)
    assert isinstance(res[0]["sepal_length"], float)


def test_sql_column_select_duckdb():
    ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")
    sql_ds = ds.map_sql(
        query="SELECT b['variety'] as variety, b['sepal.length'] as sepal_length FROM my_batch as b",
        view_name="my_batch",
        engine="duckdb",
    )
    res = sql_ds.take(1)

    assert len(res) == 1
    assert "variety" in res[0] and "sepal_length" in res[0]


def test_sql_where_filter_duckdb():
    ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")
    sql_ds = ds.map_sql(
        query=(
            "SELECT b['variety'] as variety, b['sepal.length'] as sepal_length "
            "FROM my_batch as b WHERE b['sepal.length'] > 5"
        ),
        view_name="my_batch",
        engine="duckdb",
    )
    res = sql_ds.take(5)
    assert all(row["sepal_length"] > 5 for row in res)


def test_sql_in_filter_duckdb():
    ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")
    sql_ds = ds.map_sql(
        query=(
            "SELECT b['variety'] as variety, b['sepal.length'] as sepal_length "
            "FROM my_batch as b WHERE b['variety'] IN ('Setosa', 'Versicolor')"
        ),
        view_name="my_batch",
        engine="duckdb",
    )
    res = sql_ds.take(10)
    assert all(row["variety"] in ["Setosa", "Versicolor"] for row in res)


def test_sql_is_not_null_duckdb():
    ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")
    sql_ds = ds.map_sql(
        query=(
            "SELECT b['variety'] as variety, b['sepal.length'] as sepal_length "
            "FROM my_batch as b WHERE b['sepal.length'] IS NOT NULL"
        ),
        view_name="my_batch",
        engine="duckdb",
    )
    res = sql_ds.take(5)
    assert all(row["sepal_length"] is not None for row in res)


def test_sql_string_like_duckdb():
    ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")
    sql_ds = ds.map_sql(
        query=(
            "SELECT b['variety'] as variety, b['sepal.length'] as sepal_length "
            "FROM my_batch as b WHERE LOWER(b['variety']) LIKE '%set%'"
        ),
        view_name="my_batch",
        engine="duckdb",
    )
    res = sql_ds.take(5)
    assert all("set" in row["variety"].lower() for row in res)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
