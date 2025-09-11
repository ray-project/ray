import os
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem

import ray
from ray.data.block import BlockAccessor
from ray.data.datasource.file_meta_provider import _handle_read_os_error
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.data.tests.util import extract_values
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


def df_to_csv(dataframe, path, **kwargs):
    dataframe.to_csv(path, **kwargs)


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


def test_get_internal_block_refs(ray_start_regular_shared):
    blocks = ray.data.range(10, override_num_blocks=10).get_internal_block_refs()
    assert len(blocks) == 10
    out = []
    for b in ray.get(blocks):
        out.extend(extract_values("id", BlockAccessor.for_block(b).iter_rows(True)))
    out = sorted(out)
    assert out == list(range(10)), out


def test_iter_internal_ref_bundles(ray_start_regular_shared):
    n = 10
    ds = ray.data.range(n, override_num_blocks=n)
    iter_ref_bundles = ds.iter_internal_ref_bundles()

    out = []
    ref_bundle_count = 0
    for ref_bundle in iter_ref_bundles:
        for block_ref, block_md in ref_bundle.blocks:
            b = ray.get(block_ref)
            out.extend(extract_values("id", BlockAccessor.for_block(b).iter_rows(True)))
        ref_bundle_count += 1
    out = sorted(out)
    assert ref_bundle_count == n
    assert out == list(range(n)), out


def test_fsspec_filesystem(ray_start_regular_shared, tmp_path, sample_dataframes):
    """Same as `test_parquet_write` but using a custom, fsspec filesystem.

    TODO (Alex): We should write a similar test with a mock PyArrow fs, but
    unfortunately pa.fs._MockFileSystem isn't serializable, so this may require
    some effort.
    """
    df1, df2 = sample_dataframes
    table = pa.Table.from_pandas(df1)
    path1 = os.path.join(str(tmp_path), "test1.parquet")
    pq.write_table(table, path1)
    table = pa.Table.from_pandas(df2)
    path2 = os.path.join(str(tmp_path), "test2.parquet")
    pq.write_table(table, path2)

    fs = LocalFileSystem()

    ds = ray.data.read_parquet([path1, path2], filesystem=fs)

    # Test metadata-only parquet ops.
    assert not ds._plan.has_started_execution
    assert ds.count() == 6

    out_path = os.path.join(tmp_path, "out")
    os.mkdir(out_path)

    ds._set_uuid("data")
    ds.write_parquet(out_path)

    actual_data = set(pd.read_parquet(out_path).itertuples(index=False))
    expected_data = set(pd.concat([df1, df2]).itertuples(index=False))
    assert actual_data == expected_data, (actual_data, expected_data)


def test_fsspec_http_file_system(ray_start_regular_shared, http_server, http_file):
    ds = ray.data.read_text(http_file, filesystem=HTTPFileSystem())
    assert ds.count() > 0
    # Test auto-resolve of HTTP file system when it is not provided.
    ds = ray.data.read_text(http_file)
    assert ds.count() > 0


def test_read_example_data(ray_start_regular_shared, tmp_path):
    ds = ray.data.read_csv("example://iris.csv")
    assert ds.count() == 150
    assert ds.take(1) == [
        {
            "sepal.length": 5.1,
            "sepal.width": 3.5,
            "petal.length": 1.4,
            "petal.width": 0.2,
            "variety": "Setosa",
        }
    ]


@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Skip due to incompatibility tensorflow with Python 3.12+",
)
def test_from_tf(ray_start_regular_shared):
    import tensorflow as tf
    import tensorflow_datasets as tfds

    tf_dataset = tfds.load("mnist", split=["train"], as_supervised=True)[0]
    tf_dataset = tf_dataset.take(8)  # Use subset to make test run faster.

    ray_dataset = ray.data.from_tf(tf_dataset)

    actual_data = extract_values("item", ray_dataset.take_all())
    expected_data = list(tf_dataset)
    assert len(actual_data) == len(expected_data)
    for (expected_features, expected_label), (actual_features, actual_label) in zip(
        expected_data, actual_data
    ):
        tf.debugging.assert_equal(expected_features, actual_features)
        tf.debugging.assert_equal(expected_label, actual_label)


def test_read_s3_file_error(shutdown_only, s3_path):
    dummy_path = s3_path + "_dummy"
    error_message = "Please check that file exists and has properly configured access."
    with pytest.raises(OSError, match=error_message):
        ray.data.read_parquet(dummy_path)
    with pytest.raises(OSError, match=error_message):
        ray.data.read_binary_files(dummy_path)
    with pytest.raises(OSError, match=error_message):
        ray.data.read_csv(dummy_path)
    with pytest.raises(OSError, match=error_message):
        ray.data.read_json(dummy_path)
    with pytest.raises(OSError, match=error_message):
        error = OSError(
            f"Error creating dataset. Could not read schema from {dummy_path}: AWS "
            "Error [code 15]: No response body.. Is this a 'parquet' file?"
        )
        _handle_read_os_error(error, dummy_path)


# NOTE: All tests above share a Ray cluster, while the tests below do not. These
# tests should only be carefully reordered to retain this invariant!


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
