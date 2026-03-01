import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


@pytest.fixture
def sample_dataframes():
    """Fixture providing sample pandas DataFrames for testing.

    Returns:
        tuple: (df1, df2) where df1 has 3 rows and df2 has 3 rows
    """
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    return df1, df2


def test_from_arrow(ray_start_regular_shared, sample_dataframes):
    """Test basic from_arrow functionality with single and multiple tables."""
    df1, df2 = sample_dataframes

    ds = ray.data.from_arrow([pa.Table.from_pandas(df1), pa.Table.from_pandas(df2)])
    values = [(r["one"], r["two"]) for r in ds.take(6)]
    rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
    assert values == rows
    # Check that metadata fetch is included in stats.
    assert "FromArrow" in ds.stats()

    # test from single pyarrow table
    ds = ray.data.from_arrow(pa.Table.from_pandas(df1))
    values = [(r["one"], r["two"]) for r in ds.take(3)]
    rows = [(r.one, r.two) for _, r in df1.iterrows()]
    assert values == rows
    # Check that metadata fetch is included in stats.
    assert "FromArrow" in ds.stats()


@pytest.mark.parametrize(
    "tables,override_num_blocks,expected_blocks,expected_rows",
    [
        # Single table scenarios
        ("single", 1, 1, 3),  # Single table, 1 block
        ("single", 2, 2, 3),  # Single table split into 2 blocks
        ("single", 5, 5, 3),  # Single table, more blocks than rows
        (
            "single",
            10,
            10,
            3,
        ),  # Edge case: 3 rows split into 10 blocks (creates empty blocks)
        # Multiple tables scenarios
        ("multiple", 3, 3, 6),  # Multiple tables split into 3 blocks
        ("multiple", 10, 10, 6),  # Multiple tables, more blocks than rows
        # Empty table scenarios
        ("empty", 1, 1, 0),  # Empty table, 1 block
        ("empty", 5, 5, 0),  # Empty table, more blocks than rows
    ],
)
def test_from_arrow_override_num_blocks(
    ray_start_regular_shared,
    sample_dataframes,
    tables,
    override_num_blocks,
    expected_blocks,
    expected_rows,
):
    """Test from_arrow with override_num_blocks parameter."""
    df1, df2 = sample_dataframes
    empty_df = pd.DataFrame({"one": [], "two": []})

    # Prepare tables based on test case
    if tables == "single":
        arrow_tables = pa.Table.from_pandas(df1)
        expected_data = [(r.one, r.two) for _, r in df1.iterrows()]
    elif tables == "multiple":
        arrow_tables = [pa.Table.from_pandas(df1), pa.Table.from_pandas(df2)]
        expected_data = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
    elif tables == "empty":
        arrow_tables = pa.Table.from_pandas(empty_df)
        expected_data = []

    # Create dataset with override_num_blocks
    ds = ray.data.from_arrow(arrow_tables, override_num_blocks=override_num_blocks)

    # Verify number of blocks
    assert ds.num_blocks() == expected_blocks

    # Verify row count
    assert ds.count() == expected_rows

    # Verify data integrity (only for non-empty datasets)
    if expected_rows > 0:
        values = [(r["one"], r["two"]) for r in ds.take_all()]
        assert values == expected_data


def test_from_arrow_refs(ray_start_regular_shared, sample_dataframes):
    df1, df2 = sample_dataframes
    ds = ray.data.from_arrow_refs(
        [ray.put(pa.Table.from_pandas(df1)), ray.put(pa.Table.from_pandas(df2))]
    )
    values = [(r["one"], r["two"]) for r in ds.take(6)]
    rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
    assert values == rows
    # Check that metadata fetch is included in stats.
    assert "FromArrow" in ds.stats()

    # test from single pyarrow table ref
    ds = ray.data.from_arrow_refs(ray.put(pa.Table.from_pandas(df1)))
    values = [(r["one"], r["two"]) for r in ds.take(3)]
    rows = [(r.one, r.two) for _, r in df1.iterrows()]
    assert values == rows
    # Check that metadata fetch is included in stats.
    assert "FromArrow" in ds.stats()


def test_to_arrow_refs(ray_start_regular_shared):
    n = 5
    df = pd.DataFrame({"id": list(range(n))})
    ds = ray.data.range(n)
    dfds = pd.concat(
        [t.to_pandas() for t in ray.get(ds.to_arrow_refs())], ignore_index=True
    )
    assert df.equals(dfds)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
