import os
import pytest
from pytest_lazyfixture import lazy_fixture

import lance
import pyarrow as pa

from ray.data.datasource.path_util import _unwrap_protocol
from read_api import read_lance


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

    ds = read_lance(path)

    # Test metadata-only lance ops.
    assert ds.count() == 6
    assert ds.size_bytes() > 0
    assert ds.schema() is not None

    ### todo: brentb input_files test
    # input_files = ds.input_files()
    # assert len(input_files) == 2, input_files
    # assert ".lance" in str(input_files)

    assert (
        " ".join(str(ds).split())
        == "Dataset( num_rows=6, schema={one: int64, two: string, three: int64, four: string} )"
    ), ds
    assert (
        " ".join(repr(ds).split())
        == "Dataset( num_rows=6, schema={one: int64, two: string, three: int64, four: string} )"
    ), ds

    # Forces a data read.
    values = [[s["one"], s["two"]] for s in ds.take_all()]
    assert sorted(values) == [
        [1, "a"],
        [2, "b"],
        [3, "c"],
        [4, "e"],
        [5, "f"],
        [6, "g"],
    ]

    # Test column selection.
    ds = read_lance(path, columns=["one"])
    values = [s["one"] for s in ds.take()]
    assert sorted(values) == [1, 2, 3, 4, 5, 6]
    print(ds.schema().names)
    assert ds.schema().names == ["one"]

    # Test concurrency.
    ds = read_lance(path, concurrency=1)
    values = [s["one"] for s in ds.take()]
    assert sorted(values) == [1, 2, 3, 4, 5, 6]
