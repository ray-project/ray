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
    return pa.table(
        {"a": pa.array([], type=pa.int64()), "b": pa.array([], type=pa.string())}
    )


def test_distinct_across_blocks(ray_start_regular_shared):
    """Test distinct operation across multiple blocks.

    Verifies that distinct correctly deduplicates globally across all blocks,
    not just within individual blocks.
    """
    table = pa.table({"a": [1, 2, 2, 3, 1, 3], "b": ["x", "y", "y", "z", "x", "z"]})
    ds = ray.data.from_arrow(table).repartition(3)
    out = ds.distinct().sort(key=["a", "b"]).take_all()
    assert out == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 3, "b": "z"}]


def test_distinct_all_unique(ray_start_regular_shared):
    """Test distinct on dataset with no duplicates."""
    ds = ray.data.from_arrow(pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}))
    out = ds.distinct().sort(key=["a", "b"]).take_all()
    assert out == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 3, "b": "z"}]


def test_distinct_all_duplicate(ray_start_regular_shared):
    """Test distinct on dataset with all duplicate rows.

    Also tests distinct with multiple blocks and with null values.
    """
    ds = ray.data.from_arrow(
        pa.table({"a": [5, 5, 5, 5], "b": ["foo", "foo", "foo", "foo"]})
    )
    out = ds.distinct().take_all()
    assert out == [{"a": 5, "b": "foo"}]

    ds_multi = ray.data.from_arrow(
        pa.table({"a": [5, 5, 5, 5], "b": ["foo", "foo", "foo", "foo"]})
    ).repartition(2)
    out_multi = ds_multi.distinct().take_all()
    assert out_multi == [{"a": 5, "b": "foo"}]

    ds_nulls = ray.data.from_arrow(
        pa.table(
            {
                "a": [None, None, 1, 1, None],
                "b": ["x", "x", None, None, "x"],
            }
        )
    )
    out_nulls = ds_nulls.distinct().sort(key=["a", "b"]).take_all()
    assert len(out_nulls) == 2
    assert {"a": None, "b": "x"} in out_nulls
    assert {"a": 1, "b": None} in out_nulls


def test_distinct_empty(ray_start_regular_shared, empty_data):
    """Test distinct on empty dataset."""
    ds = ray.data.from_arrow(empty_data)
    out = ds.distinct().take_all()
    assert out == []


def test_distinct_zero_columns(ray_start_regular_shared):
    """Test distinct on dataset with zero columns (empty schema).

    A dataset with zero columns is valid and distinct() should keep at most one row,
    since all rows are identical (no columns to compare). This test verifies that
    the code handles empty schemas gracefully instead of raising an error.
    """
    # Create a PyArrow table with zero columns (empty schema)
    empty_schema = pa.schema([])
    empty_table = pa.table({}, schema=empty_schema)
    ds = ray.data.from_arrow(empty_table)

    # For zero columns with zero rows, distinct should work and return empty
    out = ds.distinct().take_all()
    assert out == []

    # Verify that distinct() doesn't raise an error for empty schema
    # The fix ensures empty schema uses empty tuple for keys, which correctly
    # handles the case where all rows are considered duplicates (same empty key)


def test_distinct_subset(ray_start_regular_shared, subset_data):
    """Test distinct on subset of columns.

    When using keys parameter, distinct should only consider specified columns
    for identifying duplicates while keeping all columns in the output.
    """
    ds = ray.data.from_arrow(subset_data)
    out = ds.distinct(keys=["a"]).sort(key=["a"]).take_all()

    assert len(out) == 4
    assert all(row["a"] in [1, 2, 3, 4] for row in out)

    a_values = [row["a"] for row in out]
    assert set(a_values) == {1, 2, 3, 4}


def test_distinct_empty_keys(ray_start_regular_shared, subset_data):
    """Test that empty keys list raises an error."""
    ds = ray.data.from_arrow(subset_data)
    with pytest.raises(ValueError, match="keys cannot be an empty list"):
        ds.distinct(keys=[])


def test_distinct_duplicate_keys(ray_start_regular_shared, subset_data):
    """Test that duplicate keys raise an error."""
    ds = ray.data.from_arrow(subset_data)

    with pytest.raises(ValueError, match="Duplicate keys found"):
        ds.distinct(keys=["a", "a"])

    with pytest.raises(ValueError, match="Duplicate keys found"):
        ds.distinct(keys=["a", "b", "a", "b"])


def test_distinct_invalid_keys(ray_start_regular_shared, subset_data):
    """Test that invalid keys raise an error."""
    ds = ray.data.from_arrow(subset_data)

    with pytest.raises(ValueError, match="Keys .* not found in dataset columns"):
        ds.distinct(keys=["c", "d"])

    with pytest.raises(ValueError, match="Keys .* not found in dataset columns"):
        ds.distinct(keys=["a", "c"])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
