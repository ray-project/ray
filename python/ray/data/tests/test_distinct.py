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


def test_distinct_subset_first():
    ds = ray.data.from_arrow(
        pa.table({"a": [1, 1, 2, 3, 2, 4], "b": ["x", "y", "y", "z", "w", "foo"]})
    )
    # Default is keep="first"
    out = ds.distinct(keys=["a"]).sort(key=["a"]).take_all()
    # Should keep first row for each "a": (1,"x"), (2,"y"), (3,"z"), (4,"foo")
    assert out == [
        {"a": 1, "b": "x"},
        {"a": 2, "b": "y"},
        {"a": 3, "b": "z"},
        {"a": 4, "b": "foo"},
    ]


def test_distinct_subset_last():
    ds = ray.data.from_arrow(
        pa.table({"a": [1, 1, 2, 3, 2, 4], "b": ["x", "y", "y", "z", "w", "foo"]})
    )
    out = ds.distinct(keys=["a"], keep="last").sort(key=["a"]).take_all()
    # Should keep last row for each "a": (1,"y"), (2,"w"), (3,"z"), (4,"foo")
    assert out == [
        {"a": 1, "b": "y"},
        {"a": 2, "b": "w"},
        {"a": 3, "b": "z"},
        {"a": 4, "b": "foo"},
    ]


def test_distinct_subset_keep_false():
    ds = ray.data.from_arrow(
        pa.table({"a": [1, 1, 2, 3, 4, 4], "b": ["x", "y", "y", "z", "foo", "bar"]})
    )
    # Only a=2,3 appear once
    out = ds.distinct(keys=["a"], keep=False).sort(key=["a"]).take_all()
    assert out == [{"a": 2, "b": "y"}, {"a": 3, "b": "z"}]


def test_distinct_all_columns_keep_false():
    # All rows are duplicates, so should keep nothing
    ds = ray.data.from_arrow(pa.table({"a": [1, 1, 2, 2], "b": ["x", "x", "y", "y"]}))
    out = ds.distinct(keep=False).take_all()
    assert out == []


def test_distinct_all_columns_keep_last():
    ds = ray.data.from_arrow(
        pa.table({"a": [1, 2, 2, 1, 3], "b": ["x", "y", "y", "x", "z"]})
    )
    out = ds.distinct(keep="last").sort(key=["a", "b"]).take_all()
    assert out == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 3, "b": "z"}]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
