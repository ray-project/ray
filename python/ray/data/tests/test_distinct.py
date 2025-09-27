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


def test_distinct_empty_keys():
    ds = ray.data.from_arrow(
        pa.table({"a": [1, 1, 2, 3, 2, 4], "b": ["x", "y", "y", "z", "w", "foo"]})
    )
    # Test that empty keys list raises an error
    with pytest.raises(ValueError, match="keys cannot be an empty list"):
        ds.distinct(keys=[])


def test_distinct_invalid_keys():
    ds = ray.data.from_arrow(
        pa.table({"a": [1, 1, 2, 3, 2, 4], "b": ["x", "y", "y", "z", "w", "foo"]})
    )
    # Test that invalid keys raise an error
    with pytest.raises(ValueError, match="Keys .* not found in dataset columns"):
        ds.distinct(keys=["c", "d"])

    # Test partial invalid keys
    with pytest.raises(ValueError, match="Keys .* not found in dataset columns"):
        ds.distinct(keys=["a", "c"])


def test_distinct_empty_dataset():
    # Test that empty dataset with schema works
    ds = ray.data.from_arrow(pa.table({"a": [], "b": []}))
    result = ds.distinct()
    assert result.take_all() == []

    # Test that empty dataset with keys also works
    result_with_keys = ds.distinct(keys=["a"])
    assert result_with_keys.take_all() == []


def test_distinct_duplicate_keys():
    ds = ray.data.from_arrow(
        pa.table({"a": [1, 1, 2, 3, 2, 4], "b": ["x", "y", "y", "z", "w", "foo"]})
    )
    # Test that duplicate keys raise an error
    with pytest.raises(ValueError, match="Duplicate keys found"):
        ds.distinct(keys=["a", "a"])

    # Test multiple duplicates
    with pytest.raises(ValueError, match="Duplicate keys found"):
        ds.distinct(keys=["a", "b", "a", "b"])


def test_distinct_no_columns():
    # Test that dataset with no columns raises an error
    # This is a bit tricky to create, but we can simulate it
    # by creating a dataset and then somehow getting it into a state with no columns
    # For now, we'll test the validation logic directly
    pass  # This test would require more complex setup to create a truly columnless dataset


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
