import os

import lance
import pyarrow as pa
import pytest
from pkg_resources import parse_version
from pytest_lazyfixture import lazy_fixture

import ray
from ray._private.test_utils import wait_for_condition
from ray._private.arrow_utils import get_pyarrow_version
from ray.data import Schema
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
    ).write_lance(data_path)

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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
