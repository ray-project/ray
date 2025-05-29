import pyarrow as pa
import pytest

import ray

try:
    import polars

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False


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


@pytest.mark.skipif(not HAS_POLARS, reason="Polars is not installed")
def test_polars_engine(dataset):
    ds2 = dataset.map_sql(
        "SELECT * FROM batch WHERE b > 20", engine="polars", batch_size=2
    )
    [batch] = list(ds2.iter_batches(batch_format="pyarrow"))
    assert set(batch["b"].to_pylist()) == {30, 40}


def test_map_sql_output_schema(dataset):
    ds2 = dataset.map_sql("SELECT b as B FROM batch", batch_size=2)
    [batch] = list(ds2.iter_batches(batch_format="pyarrow"))
    assert batch.column_names == ["B"]
    assert batch["B"].to_pylist() == [10, 20, 30, 40]
