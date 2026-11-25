"""End-to-end tests for the DataSourceV2 reader of ``read_delta``.

``test_delta.py`` runs the shared suite with both readers; these tests cover
what only the V2 reader does, and compare it with the V1 reader on the inputs
where the two could disagree.
"""

import datetime
import os
import sys
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq
import pytest

import ray
from ray.data import DataContext
from ray.data.datasource.file_based_datasource import FileShuffleConfig
from ray.data.datasource.partitioning import HIVE_DEFAULT_PARTITION
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

deltalake = pytest.importorskip("deltalake")

# The V2 reader only. ``_read(..., v2=False)`` reads with the V1 reader, for
# comparison.
pytestmark = pytest.mark.parametrize("datasource_v2", [True], indirect=True)


@pytest.fixture(autouse=True)
def use_delta_datasource_v2(ray_start_regular_shared, datasource_v2):
    DataContext.get_current()._use_delta_datasource_v2 = True


def _read(path: str, *, v2: bool = True, **kwargs) -> ray.data.Dataset:
    DataContext.get_current()._use_delta_datasource_v2 = v2
    try:
        return ray.data.read_delta(path, **kwargs)
    finally:
        DataContext.get_current()._use_delta_datasource_v2 = True


def _uses_v2(ds: ray.data.Dataset) -> bool:
    """Whether ``ds`` read with the V2 reader, by its read operator's name."""
    return "ReadFilesDeltaV2" in ds.materialize().stats()


def _rows(ds: ray.data.Dataset) -> List[Dict[str, Any]]:
    return sorted(ds.take_all(), key=lambda row: repr(sorted(row.items())))


def _assert_matches_v1(path: str, transform=lambda ds: ds, **kwargs) -> None:
    v1 = transform(_read(path, v2=False, **kwargs))
    v2 = transform(_read(path, **kwargs)).materialize()

    assert _uses_v2(v2)
    assert v2.schema() == v1.schema()
    assert _rows(v2) == _rows(v1)


@pytest.fixture
def partitioned_table(tmp_path) -> str:
    """Partitions whose values need URL-encoding, null partitions and a typed
    (integer) partition column, written in two commits."""
    path = str(tmp_path / "partitioned")
    table = pa.table(
        {
            "id": pa.array(range(8), pa.int32()),
            "grp": ["a b", "e=f", None, "a b", "c/d", None, "x", "e=f"],
            "num": pa.array([1, 2, None, 1, 3, None, 9, 100], pa.int64()),
            "day": pa.array(
                [datetime.date(2024, 1, 1 + i % 3) for i in range(8)], pa.date32()
            ),
            "value": [0.5 * i for i in range(8)],
        }
    )
    deltalake.write_deltalake(path, table.slice(0, 5), partition_by=["grp", "num"])
    deltalake.write_deltalake(
        path, table.slice(5), mode="append", partition_by=["grp", "num"]
    )
    return path


@pytest.fixture
def date_partitioned_table(tmp_path) -> str:
    path = str(tmp_path / "by_day")
    deltalake.write_deltalake(
        path,
        pa.table(
            {
                "id": [1, 2, 3],
                "day": pa.array(
                    [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2), None],
                    pa.date32(),
                ),
            }
        ),
        partition_by=["day"],
    )
    return path


def test_partitioned_table_matches_v1(partitioned_table):
    _assert_matches_v1(partitioned_table)


def test_date_partitions_match_v1(date_partitioned_table):
    _assert_matches_v1(date_partitioned_table)


@pytest.mark.parametrize(
    "predicate",
    [
        col("grp") == "e=f",
        col("grp") != "a b",
        col("grp").is_null(),
        col("grp").is_in(["c/d", "x"]),
        col("num") > 2,
        col("id") >= 4,
        (col("id") < 3) & (col("grp") == "a b"),
        (col("id") > 6) | col("num").is_null(),
    ],
    ids=str,
)
def test_filters_match_v1(partitioned_table, predicate):
    _assert_matches_v1(partitioned_table, lambda ds: ds.filter(expr=predicate))


def test_columns_match_v1(partitioned_table):
    _assert_matches_v1(partitioned_table, columns=["num", "id"])


def test_include_paths_matches_v1(partitioned_table):
    """Paths are relative to the table directory, as V1 reports them."""
    _assert_matches_v1(partitioned_table, include_paths=True, columns=["path", "id"])


def test_select_columns_matches_v1(partitioned_table):
    _assert_matches_v1(
        partitioned_table, lambda ds: ds.select_columns(["grp", "value"])
    )


def test_limit_and_count(partitioned_table):
    ds = _read(partitioned_table)

    assert ds.count() == 8
    assert len(ds.limit(3).take_all()) == 3


def test_count_after_overwrite_counts_only_the_current_version(tmp_path):
    """The overwritten version's files stay in the table directory."""
    path = str(tmp_path / "table")
    deltalake.write_deltalake(path, pa.table({"x": list(range(10))}))
    deltalake.write_deltalake(path, pa.table({"x": [1, 2]}), mode="overwrite")

    ds = _read(path)

    assert ds.count() == 2
    assert sorted(row["x"] for row in ds.take_all()) == [1, 2]


def test_version_is_pinned_when_read_delta_is_called(tmp_path):
    path = str(tmp_path / "table")
    deltalake.write_deltalake(path, pa.table({"x": [1, 2]}))

    ds = _read(path)
    deltalake.write_deltalake(path, pa.table({"x": [3]}), mode="append")

    assert sorted(row["x"] for row in ds.take_all()) == [1, 2]
    assert ds.count() == 2
    assert _read(path).count() == 3


def test_reads_through_a_table_rooted_filesystem(partitioned_table):
    """``filesystem=`` must be rooted at the table directory, as for V1."""
    filesystem = pa_fs.SubTreeFileSystem(partitioned_table, pa_fs.LocalFileSystem())

    _assert_matches_v1(partitioned_table, filesystem=filesystem)


@pytest.mark.parametrize("caller_filesystem", [False, True])
def test_s3_reads_send_no_head_requests(s3_server, aws_credentials, caller_filesystem):
    """The log has every file's size, so no open asks S3 for one. A caller's
    filesystem is what ``read_unity_catalog`` passes."""
    import json

    import requests

    options = {
        "AWS_ENDPOINT_URL": s3_server,
        "AWS_ALLOW_HTTP": "true",
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_REGION": "us-west-2",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    s3 = pa_fs.S3FileSystem(
        endpoint_override=s3_server,
        region="us-west-2",
        allow_bucket_creation=True,
        **aws_credentials,
    )
    s3.create_dir("delta")
    for start in (0, 10):
        deltalake.write_deltalake(
            "s3://delta/table",
            pa.table({"id": range(start, start + 10), "grp": ["a b"] * 5 + ["x"] * 5}),
            partition_by=["grp"],
            mode="append",
            storage_options=options,
        )
    filesystem = (
        pa_fs.SubTreeFileSystem("delta/table", s3) if caller_filesystem else None
    )
    recorder = f"{s3_server}/moto-api/recorder"
    requests.post(f"{recorder}/reset-recording")
    requests.post(f"{recorder}/start-recording")

    ds = _read("s3://delta/table", storage_options=options, filesystem=filesystem)
    ids = sorted(row["id"] for row in ds.take_all())

    requests.post(f"{recorder}/stop-recording")
    recorded = requests.get(f"{recorder}/download-recording").text.splitlines()
    data_file_heads = [
        r["url"]
        for r in map(json.loads, recorded)
        if r["method"] == "HEAD" and r["url"].endswith(".parquet")
    ]
    assert _uses_v2(ds)
    assert ids == list(range(20))
    assert data_file_heads == []


def _reverse_checkpoint_rows(path: str, version: int) -> None:
    """Write the version's checkpoint with its rows in reverse order. The
    protocol leaves their order to the writer, and ``deltalake`` lists the
    ``add`` actions in it."""
    checkpoint = os.path.join(path, "_delta_log", f"{version:020d}.checkpoint.parquet")
    table = pq.read_table(checkpoint)
    pq.write_table(table.take(list(reversed(range(table.num_rows)))), checkpoint)


def test_a_version_reads_its_files_in_one_order(tmp_path):
    """Files are read in path order, however the log is checkpointed."""
    path = str(tmp_path / "table")
    for i in range(6):
        deltalake.write_deltalake(path, pa.table({"x": [i]}), mode="append")
    DataContext.get_current().execution_options.preserve_order = True
    ds = _read(path, include_paths=True)
    rows = ds.take_all()
    assert [row["path"] for row in rows] == sorted(row["path"] for row in rows)

    deltalake.DeltaTable(path).create_checkpoint()
    _reverse_checkpoint_rows(path, version=5)

    assert ds.take_all() == rows
    assert _read(path, include_paths=True).take_all() == rows


def test_seeded_shuffle_permutes_the_files(tmp_path):
    path = str(tmp_path / "table")
    for i in range(8):
        deltalake.write_deltalake(path, pa.table({"x": [i]}), mode="append")
    DataContext.get_current().execution_options.preserve_order = True

    def read_order(**kwargs) -> List[int]:
        return [row["x"] for row in _read(path, **kwargs).take_all()]

    unshuffled = read_order()
    shuffled = read_order(shuffle=FileShuffleConfig(seed=11))

    assert sorted(shuffled) == sorted(unshuffled)
    assert shuffled != unshuffled
    # The same seed, the same order.
    assert read_order(shuffle=FileShuffleConfig(seed=11)) == shuffled


@pytest.fixture
def row_group_table(tmp_path) -> str:
    """One data file with 5 row groups of 20 rows."""
    path = str(tmp_path / "row_groups")
    deltalake.write_deltalake(
        path,
        pa.table({"x": list(range(100))}),
        writer_properties=deltalake.WriterProperties(max_row_group_size=20),
    )
    return path


def test_row_groups_are_split_into_read_tasks(row_group_table, monkeypatch):
    """Which row groups the listing yields is tested in
    ``tests/datasource_v2/test_delta_footer_listing.py``."""
    monkeypatch.setenv("RAY_DATA_PARQUET_BIN_PACKING_BYTES", "1")

    ds = _read(row_group_table).materialize()

    assert ds.num_blocks() > 1
    assert sorted(row["x"] for row in ds.take_all()) == list(range(100))


@pytest.mark.parametrize("deltalake_version", ["1.5.0", "1.6.6"])
def test_falls_back_to_v1_with_pyarrow_scan_options(
    tmp_path, monkeypatch, deltalake_version
):
    """V1 reads the version the V2 check opened, even if the table changes in
    between. deltalake 1.6+ loads the file list with the log, so V1 reuses the
    checked table rather than loading it again."""
    path = str(tmp_path / "table")
    deltalake.write_deltalake(path, pa.table({"x": [1, 2]}))
    opened = []

    class _AppendAfterFirstOpen(deltalake.DeltaTable):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            opened.append(self.version())
            if len(opened) == 1:
                deltalake.write_deltalake(path, pa.table({"x": [3]}), mode="append")

    monkeypatch.setattr(deltalake, "__version__", deltalake_version)
    monkeypatch.setattr(deltalake, "DeltaTable", _AppendAfterFirstOpen)

    ds = _read(path, batch_size=2)

    assert not _uses_v2(ds)
    assert sorted(row["x"] for row in ds.take_all()) == [1, 2]
    assert opened == ([0] if deltalake_version == "1.6.6" else [0, 0])


def test_partition_filters_fall_back_to_v1(tmp_path):
    """V2 doesn't apply ``partition_filters`` yet, so it would read every
    partition; V1 lists only the matching one."""
    path = str(tmp_path / "table")
    deltalake.write_deltalake(
        path, pa.table({"x": [1, 2], "p": ["a", "b"]}), partition_by=["p"]
    )

    ds = _read(path, partition_filters=[("p", "=", "a")])

    assert not _uses_v2(ds)
    assert ds.take_all() == [{"x": 1, "p": "a"}]


def _random_prefix_table(path: str) -> None:
    """Data files under random prefixes, not partition directories."""
    deltalake.write_deltalake(
        path,
        pa.table({"id": [1, 2, 3], "p": ["a", "b", None]}),
        partition_by=["p"],
        configuration={"delta.randomizeFilePrefixes": "true"},
    )


def _utc_timestamp_table(path: str) -> None:
    """Delta writes a timestamp partition value without a zone offset."""
    ts = datetime.datetime(2024, 1, 1, 12, 30, tzinfo=datetime.timezone.utc)
    deltalake.write_deltalake(
        path,
        pa.table(
            {"id": [1, 2], "p": pa.array([ts, None], pa.timestamp("us", tz="UTC"))}
        ),
        partition_by=["p"],
    )


def _literal_null_string_table(path: str) -> None:
    """The string ``__HIVE_DEFAULT_PARTITION__`` and a null share a directory."""
    deltalake.write_deltalake(
        path,
        pa.table({"id": [1, 2, 3], "p": [HIVE_DEFAULT_PARTITION, None, "a"]}),
        partition_by=["p"],
    )


@pytest.mark.parametrize(
    "write",
    [_random_prefix_table, _utc_timestamp_table, _literal_null_string_table],
    ids=["random_prefixes", "utc_timestamp", "literal_null_string"],
)
@pytest.mark.parametrize(
    "transform",
    [
        lambda ds: ds,
        lambda ds: ds.filter(expr=col("p").is_null()),
        lambda ds: ds.filter(expr=col("p").is_not_null()),
    ],
    ids=["all", "is_null", "is_not_null"],
)
def test_partition_values_come_from_the_log(tmp_path, write, transform):
    """Tables whose paths don't give the partition values read as with V1."""
    path = str(tmp_path / "table")
    write(path)

    _assert_matches_v1(path, transform)


def test_falls_back_to_v1_when_the_flag_is_off(partitioned_table):
    assert not _uses_v2(_read(partitioned_table, v2=False))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
