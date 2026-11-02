"""End-to-end tests for the DataSourceV2 reader of ``read_delta``.

``test_delta.py`` runs the shared suite with both readers; these tests cover
what only the V2 reader does, and compare it with the V1 reader on the inputs
where the two could disagree.
"""

import datetime
import os
import posixpath
import sys
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq
import pytest
from packaging.version import parse as parse_version

import ray
from ray.data import DataContext
from ray.data._internal.datasource_v2.formats.delta.delta_file_indexer import (
    resolve_data_file_path,
)
from ray.data._internal.datasource_v2.formats.delta.delta_scanner import (
    DeltaScanner,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_chunking_utils import (
    _row_group_unit_id,
)
from ray.data._internal.logical.operators import ListFiles, ReadFiles
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data.checkpoint import CheckpointConfig
from ray.data.datasource.file_based_datasource import FileShuffleConfig
from ray.data.datasource.partitioning import HIVE_DEFAULT_PARTITION
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

_pa_version = get_pyarrow_version()
assert _pa_version is not None, "pyarrow must be installed to run these tests"

pytestmark = pytest.mark.skipif(
    _pa_version < parse_version("15.0.0"),
    reason="deltalake write_deltalake requires pyarrow >= 15.0",
)

deltalake = pytest.importorskip("deltalake")


@pytest.fixture(autouse=True)
def use_delta_datasource_v2(restore_data_context):
    ctx = DataContext.get_current()
    ctx.use_datasource_v2 = True
    ctx.use_delta_datasource_v2 = True


def _read(path: str, *, v2: bool = True, **kwargs) -> ray.data.Dataset:
    DataContext.get_current().use_delta_datasource_v2 = v2
    try:
        return ray.data.read_delta(path, **kwargs)
    finally:
        DataContext.get_current().use_delta_datasource_v2 = True


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
        col("grp") == "a b",
        col("grp") == "e=f",
        col("grp") != "a b",
        col("grp").is_null(),
        col("grp").is_not_null(),
        col("grp").is_in(["c/d", "x"]),
        col("num") == 1,
        col("num") > 2,
        col("num").is_null(),
        col("id") >= 4,
        (col("id") < 3) & (col("grp") == "a b"),
        (col("id") > 6) | col("num").is_null(),
    ],
    ids=str,
)
def test_filters_match_v1(partitioned_table, predicate):
    _assert_matches_v1(partitioned_table, lambda ds: ds.filter(expr=predicate))


@pytest.mark.parametrize(
    "columns", [["value", "grp"], ["num", "id"], ["grp"], ["day", "num", "grp"]]
)
def test_columns_match_v1(partitioned_table, columns):
    _assert_matches_v1(partitioned_table, columns=columns)


@pytest.mark.parametrize("columns", [None, ["id", "grp"], ["path", "id"]])
def test_include_paths_matches_v1(partitioned_table, columns):
    """Paths are relative to the table directory, as V1 reports them."""
    _assert_matches_v1(partitioned_table, include_paths=True, columns=columns)


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


def test_shuffle_files_reads_every_row(partitioned_table):
    ds = _read(partitioned_table, shuffle="files")

    assert sorted(row["id"] for row in ds.take_all()) == list(range(8))


def test_seeded_shuffle_permutes_the_files(tmp_path):
    path = str(tmp_path / "table")
    for i in range(8):
        deltalake.write_deltalake(path, pa.table({"x": [i]}), mode="append")

    def listed_paths(ds: ray.data.Dataset) -> List[str]:
        # Footer reads finish in any order; keep the listing's.
        return [row["__path"] for row in _list_files(ds, preserve_order=True)]

    unshuffled = listed_paths(_read(path))
    shuffled = listed_paths(_read(path, shuffle=FileShuffleConfig(seed=11)))

    assert sorted(shuffled) == unshuffled
    assert shuffled != unshuffled
    # The same seed, the same order.
    assert listed_paths(_read(path, shuffle=FileShuffleConfig(seed=11))) == shuffled


def test_unknown_columns_are_rejected(partitioned_table):
    with pytest.raises(ValueError, match="don't exist"):
        _read(partitioned_table, columns=["id", "missing"])


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


def test_lists_one_row_per_row_group_run(row_group_table):
    rows = _list_files(_read(row_group_table))

    assert len({row["__path"] for row in rows}) == 1
    assert sorted(_row_groups(rows)) == [0, 1, 2, 3, 4]


def test_listing_skips_finished_row_groups(row_group_table):
    """What a resumed job's listing does with the read units it finished."""
    ds = _read(row_group_table)
    (path,) = {row["__path"] for row in _list_files(ds)}

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


@pytest.mark.parametrize(
    "write_kwargs, read_kwargs",
    [
        ({}, {"batch_size": 2}),
        (
            {
                "configuration": {"delta.randomizeFilePrefixes": "true"},
                "partition_by": ["p"],
            },
            {},
        ),
    ],
    ids=["arrow_parquet_args", "random_file_prefixes"],
)
def test_falls_back_to_v1(tmp_path, write_kwargs, read_kwargs):
    path = str(tmp_path / "table")
    deltalake.write_deltalake(
        path, pa.table({"x": [1, 2], "p": ["a", "b"]}), **write_kwargs
    )

    ds = _read(path, **read_kwargs)

    assert not _uses_v2(ds)
    assert sorted(row["x"] for row in ds.take_all()) == [1, 2]


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


def test_utc_timestamp_partitions_fall_back_to_v1(tmp_path):
    """Delta writes the partition directory without a zone offset, which
    doesn't cast to ``timestamp[us, tz=UTC]``; V1 takes it from the log."""
    path = str(tmp_path / "by_time")
    ts = datetime.datetime(2024, 1, 1, 12, 30, tzinfo=datetime.timezone.utc)
    deltalake.write_deltalake(
        path,
        pa.table(
            {"id": [1, 2], "ts": pa.array([ts, None], pa.timestamp("us", tz="UTC"))}
        ),
        partition_by=["ts"],
    )

    ds = _read(path)

    assert not _uses_v2(ds)
    assert _rows(ds) == [{"id": 1, "ts": ts}, {"id": 2, "ts": None}]


def test_literal_null_partition_string_is_rejected(tmp_path):
    """The string ``__HIVE_DEFAULT_PARTITION__`` and a null share a directory;
    only the log tells them apart, so V2 must fail rather than read a null."""
    path = str(tmp_path / "literal")
    deltalake.write_deltalake(
        path,
        pa.table({"id": [1, 2, 3], "grp": [HIVE_DEFAULT_PARTITION, None, "a"]}),
        partition_by=["grp"],
    )

    v1_rows = _read(path, v2=False).take_all()
    assert sorted(v1_rows, key=lambda row: row["id"]) == [
        {"id": 1, "grp": HIVE_DEFAULT_PARTITION},
        {"id": 2, "grp": None},
        {"id": 3, "grp": "a"},
    ]
    with pytest.raises(ValueError, match="use_delta_datasource_v2 = False"):
        _read(path).take_all()


def test_falls_back_to_v1_when_the_flag_is_off(partitioned_table):
    assert not _uses_v2(_read(partitioned_table, v2=False))


def test_falls_back_to_v1_without_datasource_v2(partitioned_table):
    DataContext.get_current().use_datasource_v2 = False

    assert not _uses_v2(_read(partitioned_table))


def test_null_partition_values_read_as_null(partitioned_table):
    rows = _read(partitioned_table).filter(expr=col("id") == 2).take_all()

    assert rows == [
        {
            "id": 2,
            "grp": None,
            "num": None,
            "day": datetime.date(2024, 1, 3),
            "value": 1.0,
        }
    ]
    assert HIVE_DEFAULT_PARTITION not in repr(_read(partitioned_table).take_all())


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
