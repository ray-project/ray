import pyarrow as pa
import pytest

import ray


@pytest.fixture
def sample_data():
    """Basic dataset with duplicates for testing."""
    return pa.table({"a": [1, 2, 2, 1, 3], "b": ["x", "y", "y", "x", "z"]})


@pytest.fixture
def subset_data():
    """Dataset for testing distinct on subset of columns."""
    return pa.table({"a": [1, 1, 2, 3, 2, 4], "b": ["x", "y", "y", "z", "w", "foo"]})


@pytest.fixture
def empty_data():
    """Empty dataset with schema for testing edge cases."""
    return pa.table({"a": [], "b": []})


def test_distinct_across_blocks(sample_data):
    """Test distinct operation across multiple blocks."""
    table = pa.table({"a": [1, 2, 2, 3, 1, 3], "b": ["x", "y", "y", "z", "x", "z"]})
    ds = ray.data.from_arrow(table).repartition(3)
    # Should still dedupe globally
    out = ds.distinct().sort(key=["a", "b"]).take_all()
    assert out == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 3, "b": "z"}]


def test_distinct_all_unique(sample_data):
    """Test distinct on dataset with no duplicates."""
    ds = ray.data.from_arrow(pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}))
    out = ds.distinct().sort(key=["a", "b"]).take_all()
    assert out == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 3, "b": "z"}]


def test_distinct_all_duplicate():
    """Test distinct on dataset with all duplicate rows."""
    ds = ray.data.from_arrow(
        pa.table({"a": [5, 5, 5, 5], "b": ["foo", "foo", "foo", "foo"]})
    )
    out = ds.distinct().take_all()
    assert out == [{"a": 5, "b": "foo"}]


def test_distinct_empty(empty_data):
    """Test distinct on empty dataset."""
    ds = ray.data.from_arrow(empty_data)
    out = ds.distinct().take_all()
    assert out == []


def test_distinct_subset(subset_data):
    """Test distinct on subset of columns."""
    ds = ray.data.from_arrow(subset_data)
    # Test distinct on subset of columns
    out = ds.distinct(keys=["a"]).sort(key=["a"]).take_all()
    # Should keep one row for each "a" value
    assert len(out) == 4
    assert all(row["a"] in [1, 2, 3, 4] for row in out)
    # Check that we have exactly one row for each unique "a" value
    a_values = [row["a"] for row in out]
    assert set(a_values) == {1, 2, 3, 4}


def test_distinct_empty_keys(subset_data):
    """Test that empty keys list raises an error."""
    ds = ray.data.from_arrow(subset_data)
    with pytest.raises(ValueError, match="keys cannot be an empty list"):
        ds.distinct(keys=[])


def test_distinct_invalid_keys(subset_data):
    """Test that invalid keys raise an error."""
    ds = ray.data.from_arrow(subset_data)
    # Test that invalid keys raise an error
    with pytest.raises(ValueError, match="Keys .* not found in dataset columns"):
        ds.distinct(keys=["c", "d"])

    # Test partial invalid keys
    with pytest.raises(ValueError, match="Keys .* not found in dataset columns"):
        ds.distinct(keys=["a", "c"])


def test_distinct_empty_dataset(empty_data):
    """Test distinct on empty dataset with both all columns and keys."""
    ds = ray.data.from_arrow(empty_data)
    
    # Test that empty dataset with schema works
    result = ds.distinct()
    assert result.take_all() == []

    # Test that empty dataset with keys also works
    result_with_keys = ds.distinct(keys=["a"])
    assert result_with_keys.take_all() == []


def test_distinct_duplicate_keys(subset_data):
    """Test that duplicate keys raise an error."""
    ds = ray.data.from_arrow(subset_data)
    
    # Test that duplicate keys raise an error
    with pytest.raises(ValueError, match="Duplicate keys found"):
        ds.distinct(keys=["a", "a"])

    # Test multiple duplicates
    with pytest.raises(ValueError, match="Duplicate keys found"):
        ds.distinct(keys=["a", "b", "a", "b"])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))