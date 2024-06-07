import os

import lance
import pyarrow as pa
import pytest
from pkg_resources import parse_version
from pytest_lazyfixture import lazy_fixture

import ray
from ray._private.test_utils import wait_for_condition
from ray._private.utils import _get_pyarrow_version
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
def test_lance_read_basic(fs, data_path):
    # NOTE: Lance only works with PyArrow 12 or above.
    pyarrow_version = _get_pyarrow_version()
    if pyarrow_version is not None:
        pyarrow_version = parse_version(pyarrow_version)
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

    ds = ray.data.read_lance(path)

    # Test metadata-only ops.
    assert ds.count() == 6
    assert ds.schema() is not None

    assert (
        " ".join(str(ds).split())
        == "Dataset( num_rows=6, schema={one: int64, two: string, three: int64, four: string} )"  # noqa: E501
    ), ds
    assert (
        " ".join(repr(ds).split())
        == "Dataset( num_rows=6, schema={one: int64, two: string, three: int64, four: string} )"  # noqa: E501
    ), ds

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
    pyarrow_version = _get_pyarrow_version()
    if pyarrow_version is not None:
        pyarrow_version = parse_version(pyarrow_version)
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
