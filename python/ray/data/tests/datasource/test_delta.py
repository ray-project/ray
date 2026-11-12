import os
from unittest import mock

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray.data import DataContext, Schema
from ray.data._internal.object_extensions.arrow import (
    ArrowPythonObjectArray,
    _contains_pickle_object_type,
)
from ray.data._internal.util import rows_same
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.delta_test_utils import write_column_mapped_table
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa


@pytest.fixture(params=["v1", "v2"], autouse=True)
def delta_reader(request, restore_data_context):
    """Run every test with both the V1 and the DataSourceV2 reader."""
    ctx = DataContext.get_current()
    ctx.use_datasource_v2 = True
    ctx.use_delta_datasource_v2 = request.param == "v2"
    return request.param


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


def test_delta_read_empty_table(tmp_path, delta_reader):
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "tmp_test_delta_empty")
    write_deltalake(
        path, pa.schema([("x", pa.int64()), ("y", pa.string())]).empty_table()
    )

    ds = ray.data.read_delta(path)
    assert ds.count() == 0
    assert ds.take_all() == []
    if delta_reader == "v2":
        # The V2 reader takes the schema from the log. The V1 reader takes it
        # from the data files, and an empty table has none.
        assert ds.schema().names == ["x", "y"]


def test_delta_read_does_not_unpickle_a_tampered_data_file(tmp_path):
    """A data file swapped for one with a pickled-object column must not run
    its pickle. Both readers read data files with the log's schema, so the
    column comes back as plain bytes."""
    from deltalake import DeltaTable, write_deltalake

    marker = tmp_path / "exploit_marker"

    class Exploit:
        def __reduce__(self):
            return (os.system, (f"touch {marker}",))

    path = os.path.join(tmp_path, "tmp_test_delta_tampered")
    write_deltalake(path, pa.table({"id": [1, 2], "obj": [b"a", b"b"]}))
    (data_file,) = pa.table(DeltaTable(path).get_add_actions(flatten=True))[
        "path"
    ].to_pylist()
    pq.write_table(
        pa.table(
            {"id": [1, 2], "obj": ArrowPythonObjectArray.from_objects([Exploit()] * 2)}
        ),
        os.path.join(path, data_file),
    )

    batches = list(ray.data.read_delta(path).iter_batches(batch_format="pyarrow"))

    assert sum(batch.num_rows for batch in batches) == 2
    for batch in batches:
        assert not _contains_pickle_object_type(batch.schema.field("obj").type)
    assert not marker.exists(), "pickle.load executed attacker code"


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
    feature and the protocol error are injected rather than provoked; what is
    under test is Ray's translation of it. The DataSourceV2 reader leaves such
    a table to the V1 reader, so both readers fail the same way.
    """
    from deltalake import write_deltalake
    from deltalake.exceptions import DeltaProtocolError
    from deltalake.table import ProtocolVersions

    path = os.path.join(tmp_path, "tmp_test_delta_dv_message")
    write_deltalake(path, pa.table({"id": [1, 2, 3]}))

    protocol_error = DeltaProtocolError(
        "The table has set these reader features: {'deletionVectors'} but these "
        "are not yet supported by the deltalake reader."
    )
    dv_protocol = ProtocolVersions(3, 7, ["deletionVectors"], ["deletionVectors"])

    with mock.patch(
        "deltalake.DeltaTable.protocol", return_value=dv_protocol
    ), mock.patch(
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


# Reader 2 / writer 5 tables declare no reader features, so deltalake's own
# protocol check passes and deltalake 1.6.4 to 1.6.6 returned nulls for every
# mapped column.
@pytest.mark.parametrize("mode", ["name", "id"])
def test_delta_read_rejects_column_mapping(tmp_path, mode):
    path = os.path.join(tmp_path, "column_mapped")
    write_column_mapped_table(path, mode)

    with pytest.raises(NotImplementedError) as excinfo:
        ray.data.read_delta(path)

    message = str(excinfo.value)
    assert f"`delta.columnMapping.mode` is {mode!r}" in message
    assert "https://github.com/ray-project/ray/issues/64854" in message


def test_delta_read_column_mapping_mode_none(tmp_path):
    """An explicit ``none`` mode is no column mapping, so the table reads."""
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "unmapped")
    write_deltalake(
        path,
        pa.table({"id": [1, 2]}),
        configuration={"delta.columnMapping.mode": "none"},
    )

    ds = ray.data.read_delta(path)

    assert sorted(row["id"] for row in ds.take_all()) == [1, 2]


_DATES = ["2026-01-01", "2026-01-02", "2026-01-03"]


def _write_partitioned_table(path: str) -> None:
    """Two files per date partition, so pruning is visible in the file list."""
    from deltalake import write_deltalake

    for i in range(2):
        write_deltalake(
            path,
            pa.table({"date": _DATES, "x": [10 * i + d for d in range(3)]}),
            partition_by=["date"],
            mode="append",
        )


def _input_dates(ds) -> list:
    """The partition dates of the files the read lists, without running it."""
    return sorted(
        part.split("=", 1)[1]
        for file in ds.input_files()
        for part in file.split("/")
        if part.startswith("date=")
    )


def _supports_file_pruning_predicate() -> bool:
    import inspect

    from deltalake import DeltaTable

    parameters = inspect.signature(DeltaTable.to_pyarrow_dataset).parameters
    return "file_pruning_predicate" in parameters


@pytest.mark.parametrize(
    "partition_filters, dates",
    [
        ([("date", "=", "2026-01-02")], ["2026-01-02"]),
        ([("date", "in", ["2026-01-01", "2026-01-03"])], ["2026-01-01", "2026-01-03"]),
        ([("date", ">", "2026-01-01")], ["2026-01-02", "2026-01-03"]),
        ([("date", ">=", "2026-01-02"), ("date", "<", "2026-01-03")], ["2026-01-02"]),
    ],
    ids=["eq", "in", "range", "and"],
)
def test_delta_read_partition_filters(tmp_path, partition_filters, dates):
    """Only the matching partitions' files are listed, and the rows are exact."""
    path = os.path.join(tmp_path, "partitioned")
    _write_partitioned_table(path)

    ds = ray.data.read_delta(path, partition_filters=partition_filters)

    assert _input_dates(ds) == sorted(dates * 2)
    assert sorted(row["date"] for row in ds.take_all()) == sorted(dates * 2)


def test_delta_read_partition_filters_or_groups(tmp_path):
    """An OR of AND groups needs deltalake's ``file_pruning_predicate`` (1.6.6+)."""
    path = os.path.join(tmp_path, "partitioned")
    _write_partitioned_table(path)
    partition_filters = [[("date", "=", "2026-01-01")], [("date", "=", "2026-01-03")]]

    if not _supports_file_pruning_predicate():
        with pytest.raises(ValueError, match="needs deltalake 1.6.6 or later"):
            ray.data.read_delta(path, partition_filters=partition_filters)
        return

    ds = ray.data.read_delta(path, partition_filters=partition_filters)

    assert _input_dates(ds) == ["2026-01-01"] * 2 + ["2026-01-03"] * 2


@pytest.mark.parametrize(
    "partition_filters, match",
    [
        ([("x", "=", 1)], "can only use partition columns, but 'x'"),
        ([], "non-empty list"),
        ([("date", "=")], r"\(column, op, value\) tuple"),
        ([[("date", "=", "2026-01-01")], []], "list of such lists"),
    ],
    ids=["data-column", "empty", "short-tuple", "empty-group"],
)
def test_delta_read_partition_filters_rejects_invalid(
    tmp_path, partition_filters, match
):
    path = os.path.join(tmp_path, "partitioned")
    _write_partitioned_table(path)

    with pytest.raises(ValueError, match=match):
        ray.data.read_delta(path, partition_filters=partition_filters)


def test_read_unity_catalog_partition_filters(tmp_path):
    """``partition_filters`` reaches ``read_delta`` through ``reader_kwargs``."""
    from ray.data.catalog import DatabricksUnityCatalog, ResolvedSource

    path = os.path.join(tmp_path, "partitioned")
    _write_partitioned_table(path)

    with mock.patch.object(
        DatabricksUnityCatalog, "resolve", return_value=ResolvedSource(path=path)
    ), pytest.warns(DeprecationWarning, match="read_unity_catalog"):
        ds = ray.data.read_unity_catalog(
            table="main.db.tbl",
            url="https://h.databricks.com",
            token="t",
            data_format="delta",
            reader_kwargs={"partition_filters": [("date", "=", "2026-01-02")]},
        )

    assert _input_dates(ds) == ["2026-01-02"] * 2


def test_delta_read_takes_file_sizes_from_the_log(tmp_path, delta_reader):
    """Listing makes no per-file ``get_file_info`` call; sizes come from the log.

    Partition values with special characters check that the log's paths, which
    are URL-encoded once more than the fragment paths, are decoded exactly once.
    """
    if delta_reader == "v2":
        pytest.skip(
            "The V2 reader lists files in Ray tasks when the read runs; "
            "test_delta_file_indexer.py checks its paths and log sizes."
        )
    import fsspec.implementations.local
    import pyarrow.fs as pafs
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "special")
    write_deltalake(
        path,
        pa.table({"p": ["a b", "x/y", "100%", "plain"], "x": [1, 2, 3, 4]}),
        partition_by=["p"],
    )

    class _CountingLocalFileSystem(fsspec.implementations.local.LocalFileSystem):
        info_calls = 0

        def info(self, *args, **kwargs):
            type(self).info_calls += 1
            return super().info(*args, **kwargs)

    counting = _CountingLocalFileSystem(skip_instance_cache=True)
    filesystem = pafs.SubTreeFileSystem(
        path, pafs.PyFileSystem(pafs.FSSpecHandler(counting))
    )

    ds = ray.data.read_delta(path, filesystem=filesystem)

    assert _CountingLocalFileSystem.info_calls == 0
    assert len(ds.input_files()) == 4


def test_delta_file_sizes_match_the_files(tmp_path):
    from deltalake import DeltaTable, write_deltalake

    from ray.data.read_api import _delta_file_sizes

    path = os.path.join(tmp_path, "special")
    write_deltalake(
        path,
        pa.table({"p": ["a b", "x/y", "100%", None], "x": [1, 2, 3, 4]}),
        partition_by=["p"],
    )
    dt = DeltaTable(path)

    sizes = _delta_file_sizes(dt)

    fragments = [f.path for f in dt.to_pyarrow_dataset().get_fragments()]
    assert sorted(sizes) == sorted(fragments)
    for fragment in fragments:
        assert sizes[fragment] == os.path.getsize(os.path.join(path, fragment))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
