import os

import lance
import pyarrow as pa
import pytest
from pkg_resources import parse_version
from pytest_lazy_fixtures import lf as lazy_fixture

import ray
from ray._common.test_utils import wait_for_condition
from ray.data import Schema
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data.datasource.path_util import _unwrap_protocol


@pytest.mark.parametrize(
    "fs,data_path",
    [
        (None, lazy_fixture("local_path")),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
        (
            lazy_fixture("s3_fs_with_space"),
            lazy_fixture("s3_path_with_space"),
        ),  # Path contains space.
        (
            lazy_fixture("s3_fs_with_anonymous_crendential"),
            lazy_fixture("s3_path_with_anonymous_crendential"),
        ),
    ],
)
@pytest.mark.parametrize(
    "batch_size",
    [None, 100],
)
def test_lance_read_basic(fs, data_path, batch_size):
    # NOTE: Lance only works with PyArrow 12 or above.
    pyarrow_version = get_pyarrow_version()
    if pyarrow_version is not None and pyarrow_version < parse_version("12.0.0"):
        return

    df1 = pa.table({"one": [2, 1, 3, 4, 6, 5], "two": ["b", "a", "c", "e", "g", "f"]})
    setup_data_path = _unwrap_protocol(data_path)
    path = os.path.join(setup_data_path, "test.lance")
    lance.write_dataset(df1, path)

    ds_lance = lance.dataset(path)
    df2 = pa.table(
        {
            "one": [1, 2, 3, 4, 5, 6],
            "three": [4, 5, 8, 9, 12, 13],
            "four": ["u", "v", "w", "x", "y", "z"],
        }
    )
    ds_lance.merge(df2, "one")

    if batch_size is None:
        ds = ray.data.read_lance(path)
    else:
        ds = ray.data.read_lance(path, scanner_options={"batch_size": batch_size})

    # Test metadata-only ops.
    assert ds.count() == 6
    assert ds.schema() == Schema(
        pa.schema(
            {
                "one": pa.int64(),
                "two": pa.string(),
                "three": pa.int64(),
                "four": pa.string(),
            }
        )
    )

    # Test read.
    values = [[s["one"], s["two"]] for s in ds.take_all()]
    assert sorted(values) == [
        [1, "a"],
        [2, "b"],
        [3, "c"],
        [4, "e"],
        [5, "f"],
        [6, "g"],
    ]

    # Test column projection.
    ds = ray.data.read_lance(path, columns=["one"])
    values = [s["one"] for s in ds.take_all()]
    assert sorted(values) == [1, 2, 3, 4, 5, 6]
    assert ds.schema().names == ["one", "two", "three", "four"]


@pytest.mark.parametrize("data_path", [lazy_fixture("local_path")])
def test_lance_read_with_scanner_fragments(data_path):
    table = pa.table({"one": [2, 1, 3, 4, 6, 5], "two": ["b", "a", "c", "e", "g", "f"]})
    setup_data_path = _unwrap_protocol(data_path)
    path = os.path.join(setup_data_path, "test.lance")
    dataset = lance.write_dataset(table, path, max_rows_per_file=2)

    fragments = dataset.get_fragments()
    ds = ray.data.read_lance(path, scanner_options={"fragments": fragments[:1]})
    values = [[s["one"], s["two"]] for s in ds.take_all()]
    assert values == [
        [2, "b"],
        [1, "a"],
    ]


@pytest.mark.parametrize("data_path", [lazy_fixture("local_path")])
def test_lance_read_many_files(data_path):
    # NOTE: Lance only works with PyArrow 12 or above.
    pyarrow_version = get_pyarrow_version()
    if pyarrow_version is not None and pyarrow_version < parse_version("12.0.0"):
        return

    setup_data_path = _unwrap_protocol(data_path)
    path = os.path.join(setup_data_path, "test.lance")
    num_rows = 1024
    data = pa.table({"id": pa.array(range(num_rows))})
    lance.write_dataset(data, path, max_rows_per_file=1)

    def test_lance():
        ds = ray.data.read_lance(path)
        return ds.count() == num_rows

    wait_for_condition(test_lance, timeout=10)


@pytest.mark.parametrize("data_path", [lazy_fixture("local_path")])
def test_lance_write(data_path):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("str", pa.string())])

    ray.data.range(10).map(
        lambda x: {"id": x["id"], "str": f"str-{x['id']}"}
    ).write_lance(data_path, schema=schema)

    ds = lance.dataset(data_path)
    ds.count_rows() == 10
    assert ds.schema.names == schema.names
    # The schema is platform-dependent, because numpy uses int32 on Windows.
    # So we observe the schema that is written and use that.
    schema = ds.schema

    tbl = ds.to_table()
    assert sorted(tbl["id"].to_pylist()) == list(range(10))
    assert set(tbl["str"].to_pylist()) == {f"str-{i}" for i in range(10)}

    ray.data.range(10).map(
        lambda x: {"id": x["id"] + 10, "str": f"str-{x['id'] + 10}"}
    ).write_lance(data_path, mode="append")

    ds = lance.dataset(data_path)
    ds.count_rows() == 20
    tbl = ds.to_table()
    assert sorted(tbl["id"].to_pylist()) == list(range(20))
    assert set(tbl["str"].to_pylist()) == {f"str-{i}" for i in range(20)}

    ray.data.range(10).map(
        lambda x: {"id": x["id"], "str": f"str-{x['id']}"}
    ).write_lance(data_path, schema=schema, mode="overwrite")

    ds = lance.dataset(data_path)
    ds.count_rows() == 10
    assert ds.schema == schema


@pytest.mark.parametrize("data_path", [lazy_fixture("local_path")])
def test_lance_write_min_rows_per_file(data_path):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("str", pa.string())])

    ray.data.range(10).map(
        lambda x: {"id": x["id"], "str": f"str-{x['id']}"}
    ).write_lance(data_path, schema=schema, min_rows_per_file=100)

    ds = lance.dataset(data_path)
    assert ds.count_rows() == 10
    assert ds.schema == schema

    assert len(ds.get_fragments()) == 1


@pytest.mark.parametrize("data_path", [lazy_fixture("local_path")])
def test_lance_write_max_rows_per_file(data_path):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("str", pa.string())])

    ray.data.range(10).map(
        lambda x: {"id": x["id"], "str": f"str-{x['id']}"}
    ).write_lance(data_path, schema=schema, max_rows_per_file=1)

    ds = lance.dataset(data_path)
    assert ds.count_rows() == 10
    assert ds.schema == schema

    assert len(ds.get_fragments()) == 10


@pytest.mark.parametrize("data_path", [lazy_fixture("local_path")])
def test_lance_read_with_version(data_path):
    # NOTE: Lance only works with PyArrow 12 or above.
    pyarrow_version = get_pyarrow_version()
    if pyarrow_version is not None and pyarrow_version < parse_version("12.0.0"):
        return

    # Write an initial dataset (version 1)
    df1 = pa.table({"one": [2, 1, 3, 4, 6, 5], "two": ["b", "a", "c", "e", "g", "f"]})
    setup_data_path = _unwrap_protocol(data_path)
    path = os.path.join(setup_data_path, "test_version.lance")
    lance.write_dataset(df1, path)

    # Merge new data to create a later version (latest)
    ds_lance = lance.dataset(path)
    # Get the initial version
    initial_version = ds_lance.version

    df2 = pa.table(
        {
            "one": [1, 2, 3, 4, 5, 6],
            "three": [4, 5, 8, 9, 12, 13],
            "four": ["u", "v", "w", "x", "y", "z"],
        }
    )
    ds_lance.merge(df2, "one")

    # Default read should return the latest (merged) dataset.
    ds_latest = ray.data.read_lance(path)

    assert ds_latest.count() == 6
    # Latest dataset should contain merged columns
    assert "three" in ds_latest.schema().names

    # Read the initial version and ensure it contains the original columns
    ds_prev = ray.data.read_lance(path, version=initial_version)
    assert ds_prev.count() == 6
    assert ds_prev.schema().names == ["one", "two"]

    values_prev = [[s["one"], s["two"]] for s in ds_prev.take_all()]
    assert sorted(values_prev) == [
        [1, "a"],
        [2, "b"],
        [3, "c"],
        [4, "e"],
        [5, "f"],
        [6, "g"],
    ]


def test_lance_estimate_inmemory_data_size(local_path):
    """Test Lance datasource memory size estimation."""
    from ray.data._internal.datasource.lance_datasource import LanceDatasource

    # NOTE: Lance only works with PyArrow 12 or above.
    pyarrow_version = get_pyarrow_version()
    if pyarrow_version is not None and pyarrow_version < parse_version("12.0.0"):
        pytest.skip("Lance requires PyArrow >= 12.0.0")

    # Create test Lance dataset with known data
    table = pa.table(
        {
            "id": range(10000),
            "value": [f"value_{i}" * 10 for i in range(10000)],
            "number": [float(i) * 1.5 for i in range(10000)],
        }
    )

    setup_data_path = _unwrap_protocol(local_path)
    lance_path = os.path.join(setup_data_path, "test_size_estimation.lance")
    lance_ds = lance.write_dataset(table, lance_path)

    # Test full dataset estimation
    ds_source = LanceDatasource(
        uri=lance_path, scanner_options={}
    )

    estimated_size = ds_source.estimate_inmemory_data_size()

    assert estimated_size is not None, "Estimation should not be None"
    assert estimated_size > 0, "Estimated size should be positive"

    # Get actual compressed size from fragments
    fragments = list(lance_ds.get_fragments())
    actual_compressed_size = sum(
        data_file.size()
        for fragment in fragments
        for data_file in fragment.data_files()
    )

    # Estimated should be larger than compressed (decompression factor ~3x)
    assert estimated_size > actual_compressed_size, (
        f"Estimated size {estimated_size} should be larger than "
        f"compressed size {actual_compressed_size}"
    )

    # Should be within reasonable range (2.5x to 4x of compressed size)
    # Based on 3x decompression factor with some tolerance for variance
    assert actual_compressed_size * 2.5 < estimated_size < actual_compressed_size * 4, (
        f"Estimated size {estimated_size} outside expected range "
        f"[{actual_compressed_size * 2.5}, {actual_compressed_size * 4}]"
    )

    # Test with column projection
    ds_source_proj = LanceDatasource(
        uri=lance_path, columns=["id"]
    )

    estimated_size_proj = ds_source_proj.estimate_inmemory_data_size()

    assert estimated_size_proj is not None
    assert estimated_size_proj > 0
    # Projected size should be smaller than full dataset
    assert estimated_size_proj < estimated_size, (
        f"Projected size {estimated_size_proj} should be smaller than "
        f"full size {estimated_size}"
    )

    # Verify column projection ratio is reasonable
    # Selecting 1 out of 3 columns should reduce size significantly
    column_ratio = 1 / 3
    expected_proj_size = estimated_size * column_ratio
    # The projection is based on column count ratio, which may not perfectly
    # reflect actual column sizes. Allow reasonable variance (0.5x to 2x)
    # due to differences in column data sizes.
    assert expected_proj_size * 0.5 < estimated_size_proj < expected_proj_size * 2.0, (
        f"Projected size {estimated_size_proj} not within expected range "
        f"[{expected_proj_size * 0.5}, {expected_proj_size * 2.0}] "
        f"around {expected_proj_size} (column ratio: {column_ratio})"
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
