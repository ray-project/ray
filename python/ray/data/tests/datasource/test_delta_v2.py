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
import sys
import uuid
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
from ray.data._internal.datasource_v2.formats.delta.delta_scanner import (
    DeltaScanner,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_chunking_utils import (
    _row_group_unit_id,
)
from ray.data._internal.logical.operators import ListFiles, ReadFiles
from ray.data.checkpoint import CheckpointConfig
from ray.data.datasource.file_based_datasource import FileShuffleConfig
from ray.data.datasource.partitioning import HIVE_DEFAULT_PARTITION
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

deltalake = pytest.importorskip("deltalake")


@pytest.fixture(autouse=True)
def use_datasource_v2(restore_data_context):
    DataContext.get_current().use_datasource_v2 = True


def _read(path: str, *, v2: bool = True, **kwargs) -> ray.data.Dataset:
    DataContext.get_current().use_datasource_v2 = v2
    try:
        return ray.data.read_delta(path, **kwargs)
    finally:
        DataContext.get_current().use_datasource_v2 = True


def _uses_v2(ds: ray.data.Dataset) -> bool:
    dag = ds._logical_plan.dag
    return isinstance(dag, ReadFiles) and dag.datasource_name == "DeltaV2"


def _rows(ds: ray.data.Dataset) -> List[Dict[str, Any]]:
    return sorted(ds.take_all(), key=lambda row: repr(sorted(row.items())))


def _assert_matches_v1(path: str, transform=lambda ds: ds, **kwargs) -> None:
    v1 = transform(_read(path, v2=False, **kwargs))
    v2_unread = _read(path, **kwargs)
    assert _uses_v2(v2_unread)
    v2 = transform(v2_unread)

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


def test_preserve_order_matches_v1(tmp_path):
    """Files are read in the log's order, as V1 reads them (not path order)."""
    path = str(tmp_path / "table")
    for i in range(6):
        deltalake.write_deltalake(path, pa.table({"x": [i]}), mode="append")
    DataContext.get_current().execution_options.preserve_order = True

    v1 = [row["x"] for row in _read(path, v2=False).take_all()]
    v2 = [row["x"] for row in _read(path).take_all()]

    assert v2 == v1


def test_seeded_shuffle_permutes_the_files(tmp_path):
    path = str(tmp_path / "table")
    for i in range(8):
        deltalake.write_deltalake(path, pa.table({"x": [i]}), mode="append")

    def listed_paths(ds: ray.data.Dataset) -> List[str]:
        # Footer reads finish in any order; keep the listing's.
        return [row["__path"] for row in _list_files(ds, preserve_order=True)]

    unshuffled = listed_paths(_read(path))
    shuffled = listed_paths(_read(path, shuffle=FileShuffleConfig(seed=11)))

    assert sorted(shuffled) == sorted(unshuffled)
    assert shuffled != unshuffled
    # The same seed, the same order.
    assert listed_paths(_read(path, shuffle=FileShuffleConfig(seed=11))) == shuffled


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
    ],
    ids=str,
)
def test_log_statistics_pruning_matches_v1(clustered_table, predicate):
    _assert_matches_v1(clustered_table, lambda ds: ds.filter(expr=predicate))


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


def _list_files(ds: ray.data.Dataset, **kwargs) -> List[Dict[str, Any]]:
    (list_files,) = ds._logical_plan.dag.input_dependencies
    assert isinstance(list_files, ListFiles), list_files
    rows = []
    for manifest in list_files.file_indexer.list_files(
        pa.array(list_files.paths), filesystem=None, **kwargs
    ):
        block = manifest.as_block()
        # Listing manifests are always built as Arrow tables.
        assert isinstance(block, pa.Table)
        rows.extend(block.to_pylist())
    return rows


def _row_groups(rows: List[Dict[str, Any]]) -> List[int]:
    """Indices of the row groups that listing rows cover."""
    return [i for row in rows for i in row["__file_chunk_metadata"]["unit_ids"]]


def test_listing_skips_finished_row_groups(row_group_table):
    """What a resumed job's listing does with the read units it finished."""
    ds = _read(row_group_table)
    all_rows = _list_files(ds)
    (path,) = {row["__path"] for row in all_rows}
    assert sorted(_row_groups(all_rows)) == [0, 1, 2, 3, 4]

    rows = _list_files(
        ds,
        excluded_read_unit_ids={
            _row_group_unit_id(path, 0),
            _row_group_unit_id(path, 3),
        },
    )

    assert sorted(_row_groups(rows)) == [1, 2, 4]


def test_row_groups_are_split_into_read_tasks(row_group_table, monkeypatch):
    monkeypatch.setenv("RAY_DATA_PARQUET_BIN_PACKING_BYTES", "1")

    ds = _read(row_group_table).materialize()

    assert ds.num_blocks() > 1
    assert sorted(row["x"] for row in ds.take_all()) == list(range(100))


def test_filter_prunes_row_groups(row_group_table):
    rows = _list_files(_read(row_group_table), predicate=col("x") >= 80)

    assert _row_groups(rows) == [4]


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


@pytest.mark.parametrize("version, pinned", [(None, False), (2, True), (0, True)])
def test_scanner_knows_whether_the_version_was_pinned(
    checkpointed_table, version, pinned
):
    """``generated_id_column`` warns about resuming unless ``version=`` is set."""
    read_files = _read(checkpointed_table, version=version)._logical_plan.dag

    assert isinstance(read_files, ReadFiles), read_files
    assert isinstance(read_files.scanner, DeltaScanner), read_files.scanner
    assert read_files.scanner.version_pinned_by_caller is pinned


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


def test_generated_id_rerun_after_success_writes_nothing(checkpointed_table, tmp_path):
    """Resuming against the same version finds every ID again."""
    ctx = DataContext.get_current()
    ctx.checkpoint_config = _checkpoint_config(
        tmp_path, delete_checkpoint_on_success=False
    )
    out = str(tmp_path / "out")

    _read(checkpointed_table, version=2).write_parquet(out)
    _read(checkpointed_table, version=2).write_parquet(out)

    ctx.checkpoint_config = None
    assert pq.read_table(out).num_rows == 18


def test_generated_id_recovery_writes_every_row(checkpointed_table, tmp_path):
    """Fail a job partway and rerun it: every row is written and the checkpoint
    is deleted after the successful rerun."""
    ctx = DataContext.get_current()
    ctx.execution_options.preserve_order = True
    ctx.raise_original_map_exception = True
    ctx.checkpoint_config = _checkpoint_config(tmp_path)
    out = str(tmp_path / "out")

    @ray.remote(num_cpus=0)
    class Switch:
        def __init__(self):
            self._fail = True

        def turn_off(self):
            self._fail = False

        def fail(self):
            return self._fail

    switch = Switch.remote()  # pyrefly: ignore[missing-attribute]

    class InjectedFailure(Exception):
        pass

    class FailOnce:
        def __init__(self, switch):
            self._fail = ray.get(switch.fail.remote())

        def __call__(self, batch):
            if self._fail and 103 in batch["x"]:
                raise InjectedFailure("failing on x=103")
            return batch

    def run():
        _read(checkpointed_table, version=2).map_batches(
            FailOnce,
            fn_constructor_args=[switch],
            concurrency=1,
            batch_size=None,
            num_cpus=1.1,  # Keep the map out of the read's fused stage.
        ).write_parquet(out, concurrency=1)

    with pytest.raises(InjectedFailure):
        run()
    ray.get(switch.turn_off.remote())
    run()

    ctx.checkpoint_config = None
    assert not os.path.exists(tmp_path / "ckpt") or not os.listdir(tmp_path / "ckpt")
    written = set(pq.read_table(out)["x"].to_pylist())
    assert written == {f * 100 + i for f in range(3) for i in range(6)}


def test_reads_with_pyarrow_scan_options_on_v2(partitioned_table):
    """Options that only tuned V1's reading are ignored, not a reason to read
    with V1."""
    ds = _read(partitioned_table, batch_size=2)

    assert _uses_v2(ds)
    assert len(ds.take_all()) == 8


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


def test_reads_with_v1_without_datasource_v2(partitioned_table):
    assert not _uses_v2(_read(partitioned_table, v2=False))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
