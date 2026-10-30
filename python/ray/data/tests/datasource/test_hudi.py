import os
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from packaging.version import parse as parse_version
from pytest_lazy_fixtures import lf as lazy_fixture

import ray
from ray.data._internal.datasource import hudi_datasource
from ray.data._internal.object_extensions.arrow import (
    ArrowPythonObjectArray,
    _contains_pickle_object_type,
)
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data.datasource.path_util import (
    _resolve_paths_and_filesystem,
    _unwrap_protocol,
)
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa

MIN_PYARROW_VERSION_FOR_HUDI = parse_version("11.0.0")
PYARROW_VERSION = get_pyarrow_version()
PYARROW_VERSION_MEETS_REQUIREMENT = (
    PYARROW_VERSION and PYARROW_VERSION >= MIN_PYARROW_VERSION_FOR_HUDI
)
PYARROW_HUDI_TEST_SKIP_REASON = (
    f"Hudi only supported if pyarrow >= {MIN_PYARROW_VERSION_FOR_HUDI}"
)


def _extract_testing_table(fixture_path: str, table_dir: str, target_dir: str) -> str:
    with zipfile.ZipFile(fixture_path, "r") as zip_ref:
        zip_ref.extractall(target_dir)
    return os.path.join(target_dir, table_dir)


def _get_hudi_table_path(fs, data_path, table_name, testing_dir="test_hudi") -> str:
    setup_data_path = _unwrap_protocol(data_path)
    target_testing_dir = os.path.join(setup_data_path, testing_dir)
    fixture_path, _ = _resolve_paths_and_filesystem(
        f"example://hudi-tables/{table_name}.zip", fs
    )
    return _extract_testing_table(fixture_path[0], table_name, target_testing_dir)


@pytest.mark.skipif(
    not PYARROW_VERSION_MEETS_REQUIREMENT,
    reason=PYARROW_HUDI_TEST_SKIP_REASON,
)
@pytest.mark.parametrize(
    "fs,data_path",
    [
        (None, lazy_fixture("local_path")),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
    ],
)
def test_hudi_snapshot_query_v6_trips_table(ray_start_regular_shared, fs, data_path):
    table_path = _get_hudi_table_path(fs, data_path, "v6_trips_8i1u")

    ds = ray.data.read_hudi(table_path, filters=[("city", "=", "san_francisco")])

    assert ds.schema().names == [
        "_hoodie_commit_time",
        "_hoodie_commit_seqno",
        "_hoodie_record_key",
        "_hoodie_partition_path",
        "_hoodie_file_name",
        "ts",
        "uuid",
        "rider",
        "driver",
        "fare",
        "city",
    ]
    assert ds.count() == 4
    rows = (
        ds.select_columns(["_hoodie_commit_time", "ts", "rider", "fare"])
        .sort("fare")
        .take_all()
    )
    first_commit = "20250715043008154"
    second_commit = "20250715043011090"
    assert rows == [
        {
            "_hoodie_commit_time": first_commit,
            "ts": 1695159649087,
            "rider": "rider-A",
            "fare": 19.10,
        },
        {
            "_hoodie_commit_time": second_commit,
            "ts": 1695046462179,
            "rider": "rider-D",
            "fare": 25.0,
        },
        {
            "_hoodie_commit_time": first_commit,
            "ts": 1695091554788,
            "rider": "rider-C",
            "fare": 27.70,
        },
        {
            "_hoodie_commit_time": first_commit,
            "ts": 1695332066204,
            "rider": "rider-E",
            "fare": 93.50,
        },
    ]


@pytest.mark.skipif(
    not PYARROW_VERSION_MEETS_REQUIREMENT,
    reason=PYARROW_HUDI_TEST_SKIP_REASON,
)
@pytest.mark.parametrize(
    "fs,data_path",
    [
        (None, lazy_fixture("local_path")),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
    ],
)
def test_hudi_incremental_query_v6_trips_table(ray_start_regular_shared, fs, data_path):
    table_path = _get_hudi_table_path(fs, data_path, "v6_trips_8i1u")

    first_commit = "20250715043008154"
    second_commit = "20250715043011090"
    ds = ray.data.read_hudi(
        table_path,
        query_type="incremental",
        hudi_options={
            "hoodie.read.file_group.start_timestamp": first_commit,
            "hoodie.read.file_group.end_timestamp": second_commit,
        },
    )

    assert ds.schema().names == [
        "_hoodie_commit_time",
        "_hoodie_commit_seqno",
        "_hoodie_record_key",
        "_hoodie_partition_path",
        "_hoodie_file_name",
        "ts",
        "uuid",
        "rider",
        "driver",
        "fare",
        "city",
    ]
    assert ds.count() == 1
    rows = ds.select_columns(["_hoodie_commit_time", "ts", "rider", "fare"]).take_all()
    assert rows == [
        {
            "_hoodie_commit_time": second_commit,
            "ts": 1695046462179,
            "rider": "rider-D",
            "fare": 25.0,
        },
    ]


# Commits of the v8_nonpartitioned table, as (requested time, completion time).
V8_FIRST_COMMIT = ("20250713010535797", "20250713010536200")
V8_SECOND_COMMIT = ("20250713010536861", "20250713010537206")


@pytest.mark.skipif(
    not PYARROW_VERSION_MEETS_REQUIREMENT,
    reason=PYARROW_HUDI_TEST_SKIP_REASON,
)
@pytest.mark.parametrize(
    "fs,data_path",
    [
        (None, lazy_fixture("local_path")),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
    ],
)
def test_hudi_snapshot_query_v8_table(ray_start_regular_shared, fs, data_path):
    table_path = _get_hudi_table_path(fs, data_path, "v8_nonpartitioned")

    ds = ray.data.read_hudi(table_path)

    assert ds.schema().names[:6] == [
        "_hoodie_commit_time",
        "_hoodie_commit_seqno",
        "_hoodie_record_key",
        "_hoodie_partition_path",
        "_hoodie_file_name",
        "id",
    ]
    assert ds.count() == 4
    rows = ds.select_columns(["_hoodie_commit_time", "id", "name"]).sort("id")
    assert rows.take_all() == [
        {"_hoodie_commit_time": V8_SECOND_COMMIT[0], "id": 1, "name": "Alice"},
        {"_hoodie_commit_time": V8_FIRST_COMMIT[0], "id": 2, "name": "Bob"},
        {"_hoodie_commit_time": V8_FIRST_COMMIT[0], "id": 3, "name": "Carol"},
        {"_hoodie_commit_time": V8_SECOND_COMMIT[0], "id": 4, "name": "Diana"},
    ]


@pytest.mark.skipif(
    not PYARROW_VERSION_MEETS_REQUIREMENT,
    reason=PYARROW_HUDI_TEST_SKIP_REASON,
)
@pytest.mark.parametrize(
    "start_ts,end_ts,expected",
    [
        # Timestamps bound commit completion times on table version 8+.
        (
            V8_FIRST_COMMIT[1],
            None,
            [(1, V8_SECOND_COMMIT[0]), (4, V8_SECOND_COMMIT[0])],
        ),
        (
            V8_FIRST_COMMIT[0],
            V8_SECOND_COMMIT[1],
            [
                (1, V8_SECOND_COMMIT[0]),
                (2, V8_FIRST_COMMIT[0]),
                (3, V8_FIRST_COMMIT[0]),
                (4, V8_SECOND_COMMIT[0]),
            ],
        ),
        (
            "0",
            V8_FIRST_COMMIT[1],
            [
                (1, V8_FIRST_COMMIT[0]),
                (2, V8_FIRST_COMMIT[0]),
                (3, V8_FIRST_COMMIT[0]),
            ],
        ),
    ],
)
def test_hudi_incremental_query_v8_table(
    ray_start_regular_shared, local_path, start_ts, end_ts, expected
):
    table_path = _get_hudi_table_path(None, local_path, "v8_nonpartitioned")
    hudi_options = {"hoodie.read.file_group.start_timestamp": start_ts}
    if end_ts is not None:
        hudi_options["hoodie.read.file_group.end_timestamp"] = end_ts

    ds = ray.data.read_hudi(
        table_path, query_type="incremental", hudi_options=hudi_options
    )

    rows = ds.select_columns(["id", "_hoodie_commit_time"]).sort("id").take_all()
    assert [(r["id"], r["_hoodie_commit_time"]) for r in rows] == expected


@pytest.mark.skipif(
    not PYARROW_VERSION_MEETS_REQUIREMENT,
    reason=PYARROW_HUDI_TEST_SKIP_REASON,
)
def test_hudi_rejects_pickle_object_columns(ray_start_regular_shared, local_path):
    """A tampered base data file must not be able to run its embedded pickle."""
    table_path = _get_hudi_table_path(None, local_path, "v6_trips_8i1u")
    marker = os.path.join(local_path, "exploit_marker")

    class Exploit:
        def __reduce__(self):
            return (os.system, (f"touch {marker}",))

    # Swap a column of one base data file for an attacker-controlled pickled object.
    victim = ray.data.read_hudi(table_uri=table_path).input_files()[0]
    original = pq.ParquetFile(victim).read()
    columns = {name: original.column(name) for name in original.schema.names}
    columns["rider"] = ArrowPythonObjectArray.from_objects(
        [Exploit()] * original.num_rows
    )
    pq.write_table(pa.table(columns), victim)

    # hudi-rs reads base files against the table's schema, so the tampered column
    # doesn't come back as pickled objects.
    ds = ray.data.read_hudi(table_uri=table_path)
    for batch in ds.iter_batches(batch_format="pyarrow"):
        assert isinstance(batch, pa.Table)
        assert not _contains_pickle_object_type(batch.schema.field("rider").type)
    assert not os.path.exists(marker), "pickle.load executed attacker code"


@pytest.mark.skipif(
    not PYARROW_VERSION_MEETS_REQUIREMENT,
    reason=PYARROW_HUDI_TEST_SKIP_REASON,
)
def test_hudi_read_checks_pickle_object_columns(local_path, monkeypatch):
    """Every table the reader yields goes through the pickle-object guard."""
    table_path = _get_hudi_table_path(None, local_path, "v6_trips_8i1u")
    checked = []
    monkeypatch.setattr(
        hudi_datasource, "raise_on_pickle_object_columns", checked.append
    )

    datasource = hudi_datasource.HudiDatasource(table_path, query_type="snapshot")
    yielded = [
        table
        for read_task in datasource.get_read_tasks(parallelism=2)
        for table in read_task.read_fn()
    ]

    assert len(yielded) == 3
    assert [id(t) for t in checked] == [id(t) for t in yielded]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
