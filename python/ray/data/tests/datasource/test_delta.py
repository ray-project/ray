import os

import pandas as pd
import pyarrow as pa
import pytest
from packaging.version import parse as parse_version

import ray
from ray.data import Schema
from ray.data._internal.util import rows_same
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa

# deltalake's write_deltalake requires pyarrow >= 15 for the Arrow C Stream interface.
pytestmark = pytest.mark.skipif(
    get_pyarrow_version() < parse_version("15.0.0"),
    reason="deltalake write_deltalake requires pyarrow >= 15.0",
)


@pytest.mark.parametrize(
    "batch_size",
    [1, 100],
)
@pytest.mark.parametrize(
    "write_mode",
    ["append", "overwrite"],
)
def test_delta_read_basic(tmp_path, batch_size, write_mode):
    from deltalake import write_deltalake

    # Parse the data path.
    path = os.path.join(tmp_path, "tmp_test_delta")

    # Create a sample Delta Lake table
    df = pd.DataFrame(
        {"x": [42] * batch_size, "y": ["a"] * batch_size, "z": [3.14] * batch_size}
    )
    table = pa.Table.from_pandas(df)
    if write_mode == "append":
        write_deltalake(path, table, mode=write_mode)
        write_deltalake(path, table, mode=write_mode)
        expected = pd.concat([df, df], ignore_index=True)
    elif write_mode == "overwrite":
        write_deltalake(path, table, mode=write_mode)
        expected = df

    # Read the Delta Lake table
    ds = ray.data.read_delta(path)

    assert ds.schema() == Schema(
        pa.schema(
            {
                "x": pa.int64(),
                "y": pa.string(),
                "z": pa.float64(),
            }
        )
    )
    assert rows_same(ds.to_pandas(), expected)


@pytest.mark.parametrize(
    "columns, expected_columns",
    [
        (["a", "c"], ["a", "c"]),
        (["b"], ["b"]),
        (["a", "b", "c"], ["a", "b", "c"]),
    ],
)
def test_delta_read_column_selection(tmp_path, columns, expected_columns):
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "tmp_test_delta_cols")
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"], "c": [1.0, 2.0, 3.0]})
    write_deltalake(path, pa.Table.from_pandas(df))

    ds = ray.data.read_delta(path, columns=columns)
    expected = df[expected_columns]

    assert ds.schema().names == expected_columns
    assert rows_same(ds.to_pandas(), expected)


@pytest.mark.parametrize(
    "version, expected_data",
    [
        (0, {"x": [1, 2]}),
        (1, {"x": [3, 4, 5]}),
        (None, {"x": [3, 4, 5]}),
    ],
)
def test_delta_read_version(tmp_path, version, expected_data):
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "tmp_test_delta_version")
    write_deltalake(path, pa.table({"x": [1, 2]}))
    write_deltalake(path, pa.table({"x": [3, 4, 5]}), mode="overwrite")

    ds = ray.data.read_delta(path, version=version)
    expected = pd.DataFrame(expected_data)

    assert rows_same(ds.to_pandas(), expected)


def test_delta_read_schema_evolution(tmp_path):
    """Older files missing newer columns should be null-filled."""
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "tmp_test_delta_schema_evo")

    write_deltalake(path, pa.table({"x": [1, 2]}))
    write_deltalake(
        path,
        pa.table({"x": [3, 4], "y": ["a", "b"]}),
        mode="append",
        schema_mode="merge",
    )

    ds = ray.data.read_delta(path)
    expected = pd.DataFrame(
        {"x": [1, 2, 3, 4], "y": [None, None, "a", "b"]},
    )

    assert rows_same(ds.to_pandas(), expected)


@pytest.mark.parametrize(
    "storage_options",
    [{}, None],
)
def test_delta_read_storage_options(tmp_path, storage_options):
    """Verify that storage_options are forwarded to DeltaTable."""
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "tmp_test_delta_storage_opts")
    df = pd.DataFrame({"x": [1, 2, 3]})
    write_deltalake(path, pa.Table.from_pandas(df))

    ds = ray.data.read_delta(path, storage_options=storage_options)
    assert rows_same(ds.to_pandas(), df)


def test_delta_read_empty_table(tmp_path):
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "tmp_test_delta_empty")
    write_deltalake(path, pa.table({"x": pa.array([], type=pa.int64())}))

    ds = ray.data.read_delta(path)
    assert ds.count() == 0


def test_delta_read_rejects_multiple_paths():
    with pytest.raises(ValueError, match="Only a single Delta Lake table path"):
        ray.data.read_delta(["path1", "path2"])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
