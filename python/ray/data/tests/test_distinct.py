import pyarrow as pa
import pytest

import ray


def test_distinct_all_columns_local():
    ds = ray.data.from_arrow(
        pa.table({"a": [1, 2, 2, 1, 3], "b": ["x", "y", "y", "x", "z"]})
    )
    out = ds.distinct().sort(key=["a", "b"]).take_all()
    assert out == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 3, "b": "z"}]


def test_distinct_across_blocks():
    table = pa.table({"a": [1, 2, 2, 3, 1, 3], "b": ["x", "y", "y", "z", "x", "z"]})
    ds = ray.data.from_arrow(table).repartition(3)
    # Should still dedupe globally
    out = ds.distinct().sort(key=["a", "b"]).take_all()
    assert out == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 3, "b": "z"}]


def test_distinct_all_unique():
    ds = ray.data.from_arrow(pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}))
    out = ds.distinct().sort(key=["a", "b"]).take_all()
    assert out == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 3, "b": "z"}]


def test_distinct_all_duplicate():
    ds = ray.data.from_arrow(
        pa.table({"a": [5, 5, 5, 5], "b": ["foo", "foo", "foo", "foo"]})
    )
    out = ds.distinct().take_all()
    assert out == [{"a": 5, "b": "foo"}]


def test_distinct_empty():
    ds = ray.data.from_arrow(pa.table({"a": [], "b": []}))
    out = ds.distinct().take_all()
    assert out == []


def test_distinct_subset():
    ds = ray.data.from_arrow(
        pa.table({"a": [1, 1, 2, 3, 2, 4], "b": ["x", "y", "y", "z", "w", "foo"]})
    )
    # Test distinct on subset of columns
    out = ds.distinct(keys=["a"]).sort(key=["a"]).take_all()
    # Should keep one row for each "a" value
    assert len(out) == 4
    assert all(row["a"] in [1, 2, 3, 4] for row in out)
    # Check that we have exactly one row for each unique "a" value
    a_values = [row["a"] for row in out]
    assert set(a_values) == {1, 2, 3, 4}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
