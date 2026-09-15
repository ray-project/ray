import os
from unittest import mock

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
_pa_version = get_pyarrow_version()
assert _pa_version is not None, "pyarrow must be installed to run these tests"

pytestmark = pytest.mark.skipif(
    _pa_version < parse_version("15.0.0"),
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
    else:
        raise ValueError(f"Unexpected write_mode: {write_mode}")

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
        schema_mode="merge",  # pyrefly: ignore[unexpected-keyword]
    )

    ds = ray.data.read_delta(path)
    expected = pd.DataFrame(
        {"x": [1, 2, 3, 4], "y": [None, None, "a", "b"]},
    )
    # Match the Arrow-backed null sentinel produced by ``to_pandas()``.
    expected["y"] = expected["y"].astype("string")

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


def test_delta_read_deletion_vectors_message_is_actionable(tmp_path):
    """A deletion-vector table must fail with advice that actually works.

    Reading one is impossible through `to_pyarrow_dataset` -- pyarrow cannot
    apply the deletion-vector bitmaps, so deltalake refuses rather than return
    deleted rows, and no deltalake version changes that. The message therefore
    has to point at the table property, not at a library upgrade.

    deltalake cannot *write* a table declaring the feature either, so the
    protocol error is injected rather than provoked; what is under test is
    Ray's translation of it.
    """
    from deltalake import write_deltalake
    from deltalake.exceptions import DeltaProtocolError

    path = os.path.join(tmp_path, "tmp_test_delta_dv_message")
    write_deltalake(path, pa.table({"id": [1, 2, 3]}))

    protocol_error = DeltaProtocolError(
        "The table has set these reader features: {'deletionVectors'} but these "
        "are not yet supported by the deltalake reader."
    )

    with mock.patch(
        "deltalake.DeltaTable.to_pyarrow_dataset", side_effect=protocol_error
    ):
        with pytest.raises(RuntimeError) as excinfo:
            ray.data.read_delta(path)

    message = str(excinfo.value)
    # Names the cause and the reader path, so the reason is understandable.
    assert "deletionVectors" in message
    assert "pyarrow" in message
    # Names the property to look into, so the reader knows what to research.
    assert "delta.enableDeletionVectors" in message
    # Deliberately does NOT prescribe the steps: dropping the feature rewrites
    # data files and changes what other readers and writers see, which is not a
    # decision to make from a stack trace.
    for recipe in ("ALTER TABLE", "DROP FEATURE", "REORG TABLE"):
        assert recipe not in message, f"message should not prescribe {recipe!r}"
    # Does not send anyone chasing a library upgrade that cannot help.
    assert "deltalake>=" not in message
    # Keeps the original error for anyone debugging deeper.
    assert "not yet supported by the deltalake reader" in message


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
