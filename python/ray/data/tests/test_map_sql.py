import pyarrow as pa
import pytest
import ray


@pytest.fixture
def dataset():
    table = pa.table({"a": [1, 2, 3, 4], "b": [10, 20, 30, 40]})
    return ray.data.from_arrow(table)


def test_map_sql_basic_duckdb(dataset):
    ds2 = dataset.map_sql("SELECT * FROM batch WHERE a < 3", batch_size=2)
    result = pa.concat_tables(list(ds2.iter_batches(batch_format="pyarrow")))
    assert set(result["a"].to_pylist()) == {1, 2}
    assert set(result["b"].to_pylist()) == {10, 20}


def test_map_sql_select_expr(dataset):
    ds2 = dataset.map_sql("SELECT a*2 as double_a, b+1 as b1 FROM batch", batch_size=2)
    [batch] = list(ds2.iter_batches(batch_format="pyarrow"))
    assert set(batch.column_names) == {"double_a", "b1"}
    assert batch["double_a"].to_pylist() == [2, 4, 6, 8]
    assert batch["b1"].to_pylist() == [11, 21, 31, 41]


def test_map_sql_output_schema(dataset):
    ds2 = dataset.map_sql("SELECT b as B FROM batch", batch_size=2)
    [batch] = list(ds2.iter_batches(batch_format="pyarrow"))
    assert batch.column_names == ["B"]
    assert batch["B"].to_pylist() == [10, 20, 30, 40]


def test_map_sql_multiple_filters(dataset):
    # Handles AND filter extraction
    ds2 = dataset.map_sql("SELECT * FROM batch WHERE a > 1 AND a < 4", batch_size=2)
    result = pa.concat_tables(list(ds2.iter_batches(batch_format="pyarrow")))
    assert result["a"].to_pylist() == [2, 3]
    assert result["b"].to_pylist() == [20, 30]


def test_map_sql_in_operator(dataset):
    ds2 = dataset.map_sql("SELECT * FROM batch WHERE a IN (2, 4)", batch_size=2)
    result = pa.concat_tables(list(ds2.iter_batches(batch_format="pyarrow")))
    assert result["a"].to_pylist() == [2, 4]
    assert result["b"].to_pylist() == [20, 40]


def test_map_sql_not_supported_fallback_join(dataset):
    # Should fallback to SQL engine; filter is not extracted
    ds2 = dataset.map_sql(
        "SELECT * FROM batch t1 JOIN batch t2 ON t1.a = t2.a WHERE t1.a < 3",
        batch_size=2,
    )
    result = pa.concat_tables(list(ds2.iter_batches(batch_format="pyarrow")))
    # There should be 1,2 for t1.a, repeated N times depending on join output, just check content
    assert set(result["a"].to_pylist()) == {1, 2}


def test_map_sql_not_supported_fallback_subquery(dataset):
    # Should fallback: WHERE with subquery is not extracted
    ds2 = dataset.map_sql(
        "SELECT * FROM batch WHERE a = (SELECT MIN(a) FROM batch)", batch_size=2
    )
    result = pa.concat_tables(list(ds2.iter_batches(batch_format="pyarrow")))
    # Should just yield a=1, b=10 ("MIN(a)" = 1)
    assert result["a"].to_pylist() == [1]
    assert result["b"].to_pylist() == [10]


def test_map_sql_no_where(dataset):
    ds2 = dataset.map_sql("SELECT * FROM batch", batch_size=2)
    result = pa.concat_tables(list(ds2.iter_batches(batch_format="pyarrow")))
    assert result["a"].to_pylist() == [1, 2, 3, 4]
    assert result["b"].to_pylist() == [10, 20, 30, 40]


def test_map_sql_between(dataset):
    ds2 = dataset.map_sql("SELECT * FROM batch WHERE a BETWEEN 2 AND 3", batch_size=2)
    result = pa.concat_tables(list(ds2.iter_batches(batch_format="pyarrow")))
    assert result["a"].to_pylist() == [2, 3]
    assert result["b"].to_pylist() == [20, 30]


def test_map_sql_combined_expr(dataset):
    ds2 = dataset.map_sql(
        "SELECT * FROM batch WHERE (a = 2 OR b = 30) AND b <> 10", batch_size=2
    )
    result = pa.concat_tables(list(ds2.iter_batches(batch_format="pyarrow")))
    # a=2,b=20; a=3,b=30
    assert result["a"].to_pylist() == [2, 3]
    assert result["b"].to_pylist() == [20, 30]
