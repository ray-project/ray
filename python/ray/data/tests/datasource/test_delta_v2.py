"""End-to-end tests for the DataSourceV2 reader of ``read_delta``.

``test_delta.py`` runs the shared suite with both readers; these tests cover
what only the V2 reader does, and compare it with the V1 reader on the inputs
where the two could disagree.
"""

import collections
import dataclasses
import datetime
import json
import os
import posixpath
import re
import sys
import uuid
from decimal import Decimal
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq
import pytest

import ray
from ray.data import DataContext
from ray.data._internal.datasource_v2.formats.delta.delta_file_indexer import (
    DeltaTableSnapshot,
    load_add_actions,
    resolve_data_file_path,
)
from ray.data.checkpoint import CheckpointConfig
from ray.data.checkpoint.interfaces import InvalidCheckpointingConfig
from ray.data.datasource.file_based_datasource import FileShuffleConfig
from ray.data.datasource.partitioning import HIVE_DEFAULT_PARTITION
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.delta_test_utils import (
    DELETION_VECTOR_FILES,
    DELETION_VECTOR_TABLE_ROWS,
    MAPPED_TABLE_ROWS,
    write_deletion_vector_table,
    write_mapped_table,
)
from ray.tests.conftest import *  # noqa

deltalake = pytest.importorskip("deltalake")

# The V2 reader only. ``_read(..., v2=False)`` reads with the V1 reader, for
# comparison.
pytestmark = pytest.mark.parametrize("datasource_v2", [True], indirect=True)


@pytest.fixture(autouse=True)
def use_datasource_v2_on_a_shared_cluster(ray_start_regular_shared, datasource_v2):
    """``datasource_v2`` turns ``use_datasource_v2`` on for each test."""


def _read(path: str, *, v2: bool = True, **kwargs) -> ray.data.Dataset:
    DataContext.get_current().use_datasource_v2 = v2
    try:
        return ray.data.read_delta(path, **kwargs)
    finally:
        DataContext.get_current().use_datasource_v2 = True


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
def test_s3_reads_send_no_extra_requests(s3_server, aws_credentials, caller_filesystem):
    """The log has every file's size, so no open asks S3 for one. A caller's
    filesystem is what ``read_unity_catalog`` passes. The log is loaded once,
    as by the V1 reader: the listing reuses the table ``read_delta`` opened."""
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

    def read_and_record(v2: bool):
        requests.post(f"{recorder}/reset-recording")
        requests.post(f"{recorder}/start-recording")
        ds = _read(
            "s3://delta/table", v2=v2, storage_options=options, filesystem=filesystem
        )
        ids = sorted(row["id"] for row in ds.take_all())
        requests.post(f"{recorder}/stop-recording")
        recorded = requests.get(f"{recorder}/download-recording").text.splitlines()
        return ds, ids, [json.loads(line) for line in recorded]

    ds, ids, recorded = read_and_record(v2=True)
    _, v1_ids, v1_recorded = read_and_record(v2=False)

    def log_requests(recorded):
        return [(r["method"], r["url"]) for r in recorded if "_delta_log" in r["url"]]

    data_file_heads = [
        r["url"]
        for r in recorded
        if r["method"] == "HEAD" and r["url"].endswith(".parquet")
    ]
    assert _uses_v2(ds)
    assert ids == v1_ids == list(range(20))
    assert data_file_heads == []
    # V1 loads the log once (and reads some files again without
    # ``filesystem``); V2 sends no request to the log that V1 doesn't.
    assert collections.Counter(log_requests(recorded)) <= collections.Counter(
        log_requests(v1_recorded)
    )


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


_BIG_DECIMAL = 10**19


@pytest.fixture
def clustered_table(tmp_path) -> str:
    """Four commits whose files hold disjoint ranges, like a Z-ordered table:
    the log statistics can rule out whole files."""
    path = str(tmp_path / "clustered")
    for i, start in enumerate((0, 100, 200, 300)):
        values = list(range(start, start + 10))
        deltalake.write_deltalake(
            path,
            pa.table(
                {
                    "val": values,
                    "name": [f"{chr(ord('a') + i)}{v:03d}" for v in values],
                    "day": [datetime.date(2024, 1 + i, 1 + v % 10) for v in values],
                    "score": [v / 3 if v % 7 else None for v in values],
                    # Beyond int64: ``deltalake`` clamps its statistics.
                    "big": pa.array(
                        [Decimal(_BIG_DECIMAL + v) for v in values],
                        pa.decimal128(38, 0),
                    ),
                }
            ),
            mode="append",
        )
    return path


@pytest.mark.parametrize(
    "predicate",
    [
        col("val") > 250,
        col("val") < 5,
        col("val") > 1000,
        col("val") >= 0,
        col("val") != 3,
        col("val").is_in([5, 305]),
        (col("val") > 105) & (col("val") < 205),
        (col("val") < 5) | (col("val") > 305),
        col("name") < "b",
        col("name") > "c050",
        col("name") == "d305",
        col("day") >= datetime.date(2024, 3, 1),
        col("score") > 90.0,
        col("big") == Decimal(_BIG_DECIMAL + 305),
    ],
    ids=str,
)
def test_log_statistics_pruning_keeps_every_matching_row(clustered_table, predicate):
    """The V1 reader skips files with the same statistics, so compare with the
    rows a filter on the materialized table keeps, with nothing pushed down."""
    expected = _rows(_read(clustered_table).materialize().filter(expr=predicate))

    assert _rows(_read(clustered_table).filter(expr=predicate)) == expected


def test_millisecond_timestamp_statistics_keep_matching_rows(tmp_path):
    """Spark and Databricks log timestamp statistics cut to milliseconds, so the
    log says 12:00:00.000 for a file whose only row is at 12:00:00.000500."""
    path = str(tmp_path / "table")
    at = datetime.datetime(2024, 1, 1, 12, 0, 0, 500, tzinfo=datetime.timezone.utc)
    for ts in (at, at.replace(hour=14)):
        deltalake.write_deltalake(
            path,
            pa.table({"ts": pa.array([ts], pa.timestamp("us", tz="UTC"))}),
            mode="append",
        )
    for name in os.listdir(os.path.join(path, "_delta_log")):
        if name.endswith(".json"):
            log = os.path.join(path, "_delta_log", name)
            with open(log) as f:
                text = f.read()
            with open(log, "w") as f:
                # ``...:00.000500Z`` -> ``...:00.000Z``, inside the stats JSON.
                f.write(re.sub(r"(\.\d{3})\d+Z", r"\1Z", text))
    predicate = col("ts") > at.replace(microsecond=100)

    rows = _rows(_read(path).filter(expr=predicate))

    assert [row["ts"] for row in rows] == [at, at.replace(hour=14)]


def _convert_to_v2_checkpoint(path: str, version: int) -> None:
    """Rewrite the classic checkpoint at ``version`` as a v2 checkpoint that
    lists the data files in a sidecar, and delete the commits it covers.

    delta-rs can't write v2 checkpoints, but Databricks writes them by default.
    """
    log = os.path.join(path, "_delta_log")
    prefix = f"{version:020d}"
    classic_path = os.path.join(log, f"{prefix}.checkpoint.parquet")
    classic = pq.read_table(classic_path)
    os.makedirs(os.path.join(log, "_sidecars"))
    sidecar = f"{uuid.uuid4()}.parquet"
    sidecar_path = os.path.join(log, "_sidecars", sidecar)
    pq.write_table(
        classic.filter(classic["add"].is_valid()).select(["add"]), sidecar_path
    )

    with open(os.path.join(log, f"{0:020d}.json")) as f:
        metadata = next(
            action["metaData"] for action in map(json.loads, f) if "metaData" in action
        )
    checkpoint = [
        {"checkpointMetadata": {"version": version}},
        {
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["v2Checkpoint"],
                "writerFeatures": ["v2Checkpoint"],
            }
        },
        {"metaData": metadata},
        {
            "sidecar": {
                "path": sidecar,
                "sizeInBytes": os.path.getsize(sidecar_path),
                "modificationTime": int(os.path.getmtime(sidecar_path) * 1000),
            }
        },
    ]
    with open(os.path.join(log, f"{prefix}.checkpoint.{uuid.uuid4()}.json"), "w") as f:
        f.write("".join(json.dumps(action) + "\n" for action in checkpoint))
    with open(os.path.join(log, "_last_checkpoint"), "w") as f:
        json.dump({"version": version, "size": len(checkpoint)}, f)
    # Leave the checkpoint as the only record of the files committed so far.
    os.remove(classic_path)
    for v in range(version + 1):
        os.remove(os.path.join(log, f"{v:020d}.json"))


def test_reads_tables_with_v2_checkpoints(tmp_path):
    # Two files listed only in a v2 checkpoint's sidecar, and a third committed
    # after the checkpoint.
    path = str(tmp_path / "v2_checkpoint")
    for start in (0, 100):
        deltalake.write_deltalake(
            path, pa.table({"val": list(range(start, start + 3))}), mode="append"
        )
    deltalake.DeltaTable(path).create_checkpoint()
    deltalake.write_deltalake(path, pa.table({"val": [200]}), mode="append")
    _convert_to_v2_checkpoint(path, version=1)

    ds = _read(path)

    assert _uses_v2(ds)
    assert sorted(row["val"] for row in ds.take_all()) == [0, 1, 2, 100, 101, 102, 200]


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


@pytest.fixture
def checkpointed_table(tmp_path) -> str:
    """Three commits of one file each, with two row groups of three rows; the
    rows of commit ``f`` hold ``x = f * 100 + i``."""
    path = str(tmp_path / "checkpointed")
    for f in range(3):
        deltalake.write_deltalake(
            path,
            pa.table({"x": [f * 100 + i for i in range(6)]}),
            mode="append",
            writer_properties=deltalake.WriterProperties(max_row_group_size=3),
        )
    return path


def _checkpoint_config(tmp_path, **kwargs) -> CheckpointConfig:
    return CheckpointConfig(
        checkpoint_path=str(tmp_path / "ckpt"),
        generated_id_column="generated_id",
        **kwargs,
    )


@pytest.mark.parametrize("version", [None, 0])
def test_generated_id_warns_unless_the_version_is_pinned(
    checkpointed_table, tmp_path, caplog, propagate_logs, version
):
    """The warning names the version read, which a resumed job may need."""
    DataContext.get_current().checkpoint_config = _checkpoint_config(tmp_path)

    _read(checkpointed_table, version=version).write_parquet(str(tmp_path / "out"))

    assert ("pass `version=2` to `read_delta`" in caplog.text) is (version is None)


def test_generated_ids_name_each_rows_place_in_its_data_file(
    checkpointed_table, tmp_path
):
    ctx = DataContext.get_current()
    ctx.checkpoint_config = _checkpoint_config(tmp_path)
    out = str(tmp_path / "out")

    _read(checkpointed_table, version=2).write_parquet(out)

    ctx.checkpoint_config = None
    ids = pq.read_table(out)["generated_id"].to_pylist()
    log_paths = {
        resolve_data_file_path(p)
        for p in pa.table(
            deltalake.DeltaTable(checkpointed_table).get_add_actions(flatten=True)
        )["path"].to_pylist()
    }
    # Paths are relative to the table, as everywhere in the reader.
    assert {posixpath.join(i["path_prefix"], i["file_name"]) for i in ids} == log_paths
    assert sorted((i["file_name"], i["fragment"], i["row_id"]) for i in ids) == sorted(
        (name, row_group, row)
        for name in {i["file_name"] for i in ids}
        for row_group in range(2)
        for row in range(3)
    )


def test_generated_id_resume_of_the_latest_version(checkpointed_table, tmp_path):
    """Without ``version=``, a resumed job reads the latest version. Appended
    files are new work; files a compaction rewrote would be written again, so
    the resumed job fails until it pins the version it read before."""
    ctx = DataContext.get_current()
    ctx.checkpoint_config = _checkpoint_config(
        tmp_path, delete_checkpoint_on_success=False
    )
    out = str(tmp_path / "out")

    def written_rows() -> int:
        config, ctx.checkpoint_config = ctx.checkpoint_config, None
        try:
            return pq.read_table(out).num_rows
        finally:
            ctx.checkpoint_config = config

    _read(checkpointed_table).write_parquet(out)
    deltalake.write_deltalake(
        checkpointed_table, pa.table({"x": [300, 301]}), mode="append"
    )
    _read(checkpointed_table).write_parquet(out)
    assert written_rows() == 20

    deltalake.DeltaTable(checkpointed_table).optimize.compact()
    with pytest.raises(InvalidCheckpointingConfig, match="rewrote them"):
        _read(checkpointed_table).write_parquet(out)
    _read(checkpointed_table, version=3).write_parquet(out)
    assert written_rows() == 20


def test_reads_with_pyarrow_scan_options_on_v2(partitioned_table):
    """Options that only tuned V1's reading are ignored, not a reason to read
    with V1."""
    ds = _read(partitioned_table, batch_size=2)

    assert _uses_v2(ds)
    assert len(ds.take_all()) == 8


def test_read_delta_rejects_data_files_outside_the_table(tmp_path):
    """A shallow clone's log names data files by absolute path. The V1 reader
    can't read them either; V2 says so when ``read_delta`` is called."""
    import json

    path = str(tmp_path / "table")
    deltalake.write_deltalake(path, pa.table({"x": [1]}))
    log_path = os.path.join(path, "_delta_log", f"{0:020d}.json")
    with open(log_path) as f:
        actions = [json.loads(line) for line in f]
    for action in actions:
        if "add" in action:
            action["add"]["path"] = os.path.join(path, action["add"]["path"])
    with open(log_path, "w") as f:
        f.writelines(json.dumps(action) + "\n" for action in actions)

    with pytest.raises(ValueError, match="absolute path"):
        _read(path)


def test_listing_reloads_add_actions_of_another_version(tmp_path):
    """If Ray rebuilt the ``add`` actions ``read_delta`` loaded by reopening
    the table, they may be of a later version; the listing then loads the
    pinned one."""
    path = str(tmp_path / "table")
    deltalake.write_deltalake(path, pa.table({"x": [1]}))
    deltalake.write_deltalake(path, pa.table({"x": [2]}), mode="append")
    later = load_add_actions(deltalake.DeltaTable(path, version=1))

    snapshot = DeltaTableSnapshot(
        table_uri=path, version=0, loaded_add_actions=ray.put(later)
    )

    assert snapshot.add_actions().num_rows == 1
    assert dataclasses.replace(snapshot, version=1).add_actions().num_rows == 2


@pytest.mark.parametrize(
    "partition_filters",
    [
        [("grp", "=", "a b")],
        [("grp", "in", ["e=f", "c/d"])],
        [("grp", "=", "")],
        [("grp", "!=", "a b"), ("num", ">", 1)],
    ],
    ids=["special-characters", "in", "null", "and"],
)
def test_partition_filters_match_v1(partitioned_table, partition_filters):
    """V2 reads the files ``deltalake`` selects, as V1 does: ``''`` is the
    null partition, and ``num`` compares as an integer."""
    _assert_matches_v1(partitioned_table, partition_filters=partition_filters)


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


def test_reads_with_v2_by_default(partitioned_table):
    DataContext._set_current(DataContext())

    assert _uses_v2(ray.data.read_delta(partitioned_table))


@pytest.fixture(params=["name", "id"])
def mapped_table(tmp_path, request) -> str:
    """A column-mapped table. In the ``id`` mode its files use other column
    names than the log's physical names, so only the field ids match."""
    path = str(tmp_path / "mapped")
    write_mapped_table(path, request.param)
    return path


@pytest.mark.parametrize(
    "transform, expected",
    [
        (lambda ds: ds, lambda rows: rows),
        # Log statistics and Parquet statistics are keyed by physical name.
        (
            lambda ds: ds.filter(expr=col("id") > 5),
            lambda rows: [r for r in rows if r["id"] > 5],
        ),
        # The partition column comes from the log, not the file's copy of it.
        (
            lambda ds: ds.filter(expr=col("grp") == "a"),
            lambda rows: [r for r in rows if r["grp"] == "a"],
        ),
        # A filter on a column with nested fields runs after renaming.
        (
            lambda ds: ds.filter(expr=col("s").is_null()),
            lambda rows: [r for r in rows if r["s"] is None],
        ),
        (
            lambda ds: ds.select_columns(["s", "extra"]),
            lambda rows: [{"s": r["s"], "extra": r["extra"]} for r in rows],
        ),
    ],
    ids=["all", "filter", "partition_filter", "nested_filter", "select"],
)
def test_reads_a_mapped_table(mapped_table, transform, expected):
    """Data files name columns by physical name or field id, nested fields
    too. They also hold the partition column and a row-tracking column that
    aren't read, and the first file lacks ``extra``, which is null-filled."""
    ds = transform(_read(mapped_table))

    assert _rows(ds) == sorted(
        expected(MAPPED_TABLE_ROWS), key=lambda row: repr(sorted(row.items()))
    )


def test_mapped_table_counts_from_the_log(mapped_table):
    """Which files the log statistics skip is tested in
    ``tests/datasource_v2/test_delta_footer_listing.py``."""
    assert _read(mapped_table).count() == len(MAPPED_TABLE_ROWS)


@pytest.fixture
def deletion_vector_table(tmp_path) -> str:
    path = str(tmp_path / "deletion_vectors")
    write_deletion_vector_table(path)
    # The hand-written deletion vector reads back as written.
    deleted = pa.table(deltalake.DeltaTable(path).deletion_vectors())
    assert [
        [i for i, keep in enumerate(row) if not keep]
        for row in deleted["selection_vector"].to_pylist()
    ] == [DELETION_VECTOR_FILES["part-0.parquet"][1]]
    return path


@pytest.mark.parametrize(
    "transform, expected",
    [
        (lambda ds: ds, DELETION_VECTOR_TABLE_ROWS),
        # The filter runs after the deleted rows are dropped.
        (
            lambda ds: ds.filter(expr=col("x") > 2),
            [x for x in DELETION_VECTOR_TABLE_ROWS if x > 2],
        ),
    ],
    ids=["all", "filter"],
)
def test_drops_deleted_rows(deletion_vector_table, monkeypatch, transform, expected):
    """Each row group its own read task, so positions span read units."""
    monkeypatch.setenv("RAY_DATA_PARQUET_BIN_PACKING_BYTES", "1")

    rows = transform(_read(deletion_vector_table)).take_all()

    assert sorted(row["x"] for row in rows) == expected


def test_count_and_limit_leave_out_deleted_rows(deletion_vector_table):
    """The log's row counts and the footers both count deleted rows."""
    assert _read(deletion_vector_table).count() == len(DELETION_VECTOR_TABLE_ROWS)
    rows = _read(deletion_vector_table).limit(5).take_all()
    assert len(rows) == 5
    assert {row["x"] for row in rows} <= set(DELETION_VECTOR_TABLE_ROWS)


def test_generated_ids_are_physical_row_positions(deletion_vector_table, tmp_path):
    """A row's ID is its position in its row group, counting deleted rows, so
    a later deletion vector doesn't shift the IDs of the rows after it."""
    ctx = DataContext.get_current()
    ctx.checkpoint_config = _checkpoint_config(tmp_path)
    out = str(tmp_path / "out")

    _read(deletion_vector_table, version=0).filter(expr=col("x") != 3).write_parquet(
        out
    )

    ctx.checkpoint_config = None
    ids = pq.read_table(out).to_pylist()
    assert sorted(
        (row["x"], row["generated_id"]["fragment"], row["generated_id"]["row_id"])
        for row in ids
    ) == [(0, 0, 0), (2, 0, 2), (5, 1, 2), (10, 0, 0), (11, 0, 1), (12, 0, 2)]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
