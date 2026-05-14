"""Integration tests for the ``TableDatasink`` framework via a real adapter.

These tests close the coverage gap left by ``test_table_datasink.py`` (which
uses a recorder-only ``FakeAdapter``). Here we plug in a minimal real-I/O
``ToyParquetAdapter`` that uses :class:`ParquetFileWriter` to write actual
Parquet files to a tmp directory, then assert behaviour end-to-end:

* Files appear on disk.
* Content round-trips via ``pyarrow.parquet.read_table``.
* ``ParquetFileWriter`` buffering / flush semantics work.
* Schema unification across workers is visible on disk.
* ``on_failure`` cleans up orphan files written by failing tasks.

All tests drive the framework in-process via ``sink.write(iter([table]), ctx)``
— the same pattern ``test_table_datasink.py`` uses — so they don't require a
Ray cluster.
"""

import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq
import pytest

from ray.data._internal.datasource.table import (
    ParquetFileWriter,
    SaveMode,
    TableAdapter,
    TableDatasink,
    TableWriteTaskResult,
)
from ray.data.datasource.datasink import WriteResult


# ----------------------------------------------------------------------
# ToyParquetAdapter — minimal real-I/O reference adapter.
# ----------------------------------------------------------------------


@dataclass
class _ToyFile:
    """Per-file metadata returned by ``ToyParquetAdapter.write_block``."""

    path: str
    size: int
    partition_values: Dict[str, Optional[str]]


_HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__"


def _hive_path_builder(
    cols: List[str], values: Tuple
) -> Tuple[str, Dict[str, Optional[str]]]:
    """Mirror Delta's Hive-style partition encoding: None -> default sentinel,
    float NaN -> "NaN". Returns (relative_dir_with_trailing_slash, value_map)."""
    if not cols:
        return ("", {})
    parts: List[str] = []
    value_map: Dict[str, Optional[str]] = {}
    for col, val in zip(cols, values):
        if val is None:
            encoded = _HIVE_DEFAULT_PARTITION
        elif isinstance(val, float) and val != val:  # NaN
            encoded = "NaN"
        else:
            encoded = str(val)
        parts.append(f"{col}={encoded}")
        value_map[col] = None if val is None else encoded
    return ("/".join(parts) + "/", value_map)


class ToyParquetAdapter(TableAdapter[_ToyFile, str]):
    """Minimal real-I/O adapter for framework integration tests.

    Writes Parquet files into ``root`` via :class:`ParquetFileWriter`.
    Supports APPEND and OVERWRITE only — does NOT implement
    :class:`SupportsUpserts` so UPSERT is rejected by the framework.

    OVERWRITE semantics: ``commit_overwrite`` deletes every ``.parquet``
    file in ``root`` that isn't in the freshly-written set. This is
    intentionally non-transactional (it's a test adapter); just enough to
    exercise the framework's per-mode dispatch.
    """

    def __init__(
        self,
        root: str,
        *,
        partition_cols: Optional[List[str]] = None,
        target_file_size_bytes: Optional[int] = None,
        compression: str = "snappy",
        fail_after_n_writes: Optional[int] = None,
    ):
        self._root = Path(root)
        self._partition_cols = list(partition_cols or [])
        self._target_file_size_bytes = target_file_size_bytes
        self._compression = compression
        self._fail_after_n_writes = fail_after_n_writes

        # Lifecycle state.
        self._mode: Optional[SaveMode] = None
        self._task_written: Set[str] = set()
        self._writer: Optional[ParquetFileWriter] = None
        self._n_writes_seen = 0

    # ------------------------------------------------------------------
    # Introspection.
    # ------------------------------------------------------------------
    @property
    def supported_modes(self) -> Set[SaveMode]:
        return {SaveMode.APPEND, SaveMode.OVERWRITE}

    def get_name(self) -> str:
        return "ToyParquet"

    # ------------------------------------------------------------------
    # Driver lifecycle.
    # ------------------------------------------------------------------
    def preflight(self, mode, partition_cols, declared_schema) -> None:
        self._mode = mode
        self._root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Worker lifecycle.
    # ------------------------------------------------------------------
    def start_task(self, ctx) -> None:
        # Build a fresh per-task ParquetFileWriter pointed at self._root.
        # SubTreeFileSystem strips the absolute tmpdir prefix so the writer
        # sees / reports paths relative to the table root (matches the
        # Delta / Iceberg convention).
        fs = pa_fs.SubTreeFileSystem(str(self._root), pa_fs.LocalFileSystem())
        self._task_written = set()
        self._writer = ParquetFileWriter(
            filesystem=fs,
            partition_cols=self._partition_cols,
            write_uuid="toy00000",
            compression=self._compression,
            write_statistics=False,
            target_file_size_bytes=self._target_file_size_bytes,
            path_builder=_hive_path_builder,
            action_factory=lambda *, path, size, partition_values, modification_time, stats: _ToyFile(
                path=path, size=size, partition_values=partition_values
            ),
            stats_factory=lambda _t: None,
            written_files=self._task_written,
        )
        self._writer.set_task_idx(int(getattr(ctx, "task_idx", 0) or 0))

    def write_block(
        self, arrow_table: pa.Table
    ) -> Tuple[List[_ToyFile], pa.Schema, Optional[pa.Table]]:
        if self._writer is None:
            return ([], arrow_table.schema, None)
        # Check the fail-injection BEFORE writing so the failing call leaves
        # no on-disk side effects. Previous successful calls' files are the
        # orphans that ``on_failure`` must clean up.
        self._n_writes_seen += 1
        if (
            self._fail_after_n_writes is not None
            and self._n_writes_seen >= self._fail_after_n_writes
        ):
            raise RuntimeError("injected write_block failure")
        actions = self._writer.add_table(arrow_table)
        return (actions, arrow_table.schema, None)

    def finalize_task(self) -> Tuple[List[_ToyFile], List[pa.Schema]]:
        if self._writer is None:
            return ([], [])
        return (self._writer.flush(), [])

    # ------------------------------------------------------------------
    # Driver commit — files are already on disk; commit is bookkeeping.
    # ------------------------------------------------------------------
    def commit_append(self, file_actions, unified_schema) -> None:
        return None

    def build_overwrite_predicate(self, overwrite_filter) -> Optional[str]:
        return None

    def commit_overwrite(self, file_actions, unified_schema, delete_predicate) -> None:
        new_names = {a.path for a in file_actions}
        for p in self._root.glob("*.parquet"):
            if p.name not in new_names:
                p.unlink()

    # ------------------------------------------------------------------
    # Failure handling.
    # ------------------------------------------------------------------
    def on_failure(self, written_paths: List[str]) -> None:
        for rel in written_paths:
            full = self._root / rel
            if full.exists():
                full.unlink()


# ----------------------------------------------------------------------
# Helpers — drive the framework in-process.
# ----------------------------------------------------------------------


class _FakeCtx:
    def __init__(self, task_idx: int = 0):
        self.task_idx = task_idx
        self.kwargs: Dict[str, Any] = {}


def _drive_one_task(
    sink: TableDatasink, batches: List[pa.Table], task_idx: int = 0
) -> TableWriteTaskResult:
    return sink.write(iter(batches), _FakeCtx(task_idx=task_idx))


def _wrap_write_result(returns: List[TableWriteTaskResult]) -> WriteResult:
    num_rows = sum(sum(getattr(a, "size", 0) for a in r.file_actions) for r in returns)
    return WriteResult(
        num_rows=num_rows, size_bytes=num_rows, write_returns=list(returns)
    )


def _list_parquet_files(root: Path) -> List[Path]:
    return sorted(root.glob("*.parquet"))


def _list_parquet_files_recursive(root: Path) -> List[Path]:
    return sorted(root.rglob("*.parquet"))


def _partition_dirs(root: Path) -> List[str]:
    """Names of top-level partition directories (e.g. 'k=1', 'k=NaN')."""
    return sorted(p.name for p in root.iterdir() if p.is_dir())


def _read_file_only(f: Path) -> pa.Table:
    """Read a single Parquet file's own columns, WITHOUT Hive partition
    inference (``pq.read_table`` would re-attach partition columns from the
    directory path). ``ParquetFile`` is a single-file reader, so the result
    contains exactly what was written to disk."""
    return pq.ParquetFile(str(f)).read()


# ----------------------------------------------------------------------
# Tests.
# ----------------------------------------------------------------------


def test_append_writes_real_parquet_and_round_trips(tmp_path):
    """APPEND through the framework writes a real Parquet file whose
    contents round-trip via pq.read_table."""
    adapter = ToyParquetAdapter(str(tmp_path))
    sink = TableDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()

    table = pa.table({"id": [1, 2, 3], "v": ["a", "b", "c"]})
    r = _drive_one_task(sink, [table])
    sink.on_write_complete(_wrap_write_result([r]))

    files = _list_parquet_files(tmp_path)
    assert len(files) == 1, f"expected 1 parquet file, got {files}"
    round_trip = pq.read_table(files[0])
    assert round_trip.column("id").to_pylist() == [1, 2, 3]
    assert round_trip.column("v").to_pylist() == ["a", "b", "c"]


def test_overwrite_replaces_existing_files(tmp_path):
    """OVERWRITE deletes everything from the previous APPEND and writes
    fresh files."""
    # First APPEND a batch.
    adapter = ToyParquetAdapter(str(tmp_path))
    sink_a = TableDatasink(adapter, SaveMode.APPEND)
    sink_a.on_write_start()
    r1 = _drive_one_task(sink_a, [pa.table({"id": [1], "v": ["old"]})])
    sink_a.on_write_complete(_wrap_write_result([r1]))
    initial_files = {p.name for p in _list_parquet_files(tmp_path)}
    assert len(initial_files) == 1

    # Now OVERWRITE with a new batch.
    adapter2 = ToyParquetAdapter(str(tmp_path))
    sink_b = TableDatasink(adapter2, SaveMode.OVERWRITE)
    sink_b.on_write_start()
    r2 = _drive_one_task(sink_b, [pa.table({"id": [2], "v": ["new"]})])
    sink_b.on_write_complete(_wrap_write_result([r2]))

    final_files = _list_parquet_files(tmp_path)
    assert len(final_files) == 1
    # The original file must have been deleted.
    assert final_files[0].name not in initial_files
    round_trip = pq.read_table(final_files[0])
    assert round_trip.column("v").to_pylist() == ["new"]


def test_parquet_file_writer_buffers_until_target_size(tmp_path):
    """With target_file_size_bytes large, multiple add_table calls accumulate
    into one file at flush time."""
    adapter = ToyParquetAdapter(
        str(tmp_path), target_file_size_bytes=10**9
    )  # effectively unbounded buffer
    sink = TableDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()

    batches = [pa.table({"id": [i], "v": [f"r{i}"]}) for i in range(5)]
    # Before driving the task: no files yet.
    assert _list_parquet_files(tmp_path) == []

    r = _drive_one_task(sink, batches)
    # finalize_task ran inside _drive_one_task -> sink.write, flushing the buffer.
    sink.on_write_complete(_wrap_write_result([r]))

    files = _list_parquet_files(tmp_path)
    assert len(files) == 1, f"expected 1 buffered+flushed file, got {len(files)}"
    round_trip = pq.read_table(files[0])
    assert sorted(round_trip.column("id").to_pylist()) == [0, 1, 2, 3, 4]


def test_parquet_file_writer_flushes_per_block_when_unbuffered(tmp_path):
    """With target_file_size_bytes None (default), each non-empty add_table
    flushes immediately -> one file per Arrow block."""
    adapter = ToyParquetAdapter(str(tmp_path))  # target=None -> immediate flush
    sink = TableDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()

    batches = [pa.table({"id": [i], "v": [f"r{i}"]}) for i in range(3)]
    r = _drive_one_task(sink, batches)
    sink.on_write_complete(_wrap_write_result([r]))

    files = _list_parquet_files(tmp_path)
    assert len(files) == 3


def test_schema_unification_int32_int64_round_trips_on_disk(tmp_path):
    """Two tasks emit different int widths; framework unifies; disk content
    is recoverable."""
    adapter = ToyParquetAdapter(str(tmp_path))
    sink = TableDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()

    r1 = _drive_one_task(
        sink,
        [pa.table({"a": pa.array([1], type=pa.int32())})],
        task_idx=0,
    )
    r2 = _drive_one_task(
        sink,
        [pa.table({"a": pa.array([10_000_000_000], type=pa.int64())})],
        task_idx=1,
    )
    sink.on_write_complete(_wrap_write_result([r1, r2]))

    files = _list_parquet_files(tmp_path)
    assert len(files) == 2
    # Concatenate file contents; framework should have unified to int64.
    all_rows = sorted(
        v for f in files for v in pq.read_table(f).column("a").to_pylist()
    )
    assert all_rows == [1, 10_000_000_000]


def test_empty_writes_still_call_commit_append(tmp_path):
    """When no worker emits any file, commit_append still fires once."""
    adapter = ToyParquetAdapter(str(tmp_path))
    sink = TableDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()
    # No task ran; pass an empty write_returns list.
    sink.on_write_complete(_wrap_write_result([]))

    # No files written, and adapter survived the lifecycle (no exception).
    assert _list_parquet_files(tmp_path) == []


def test_on_failure_removes_orphan_files(tmp_path):
    """When write_block fails partway through, the framework attaches the
    orphan paths to the exception and on_write_failed forwards them to the
    adapter, which deletes them from disk."""
    # fail_after_n_writes=2 -> the first block writes a file, the second
    # raises before writing anything.
    adapter = ToyParquetAdapter(str(tmp_path), fail_after_n_writes=2)
    sink = TableDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()

    batches = [
        pa.table({"id": [1], "v": ["good"]}),
        pa.table({"id": [2], "v": ["fails"]}),
    ]
    with pytest.raises(RuntimeError, match="injected write_block failure"):
        _drive_one_task(sink, batches)

    # The first block did land on disk before the second one failed.
    files_before_cleanup = _list_parquet_files(tmp_path)
    assert len(files_before_cleanup) == 1

    # Simulate Ray Data calling on_write_failed with the captured exception.
    err = RuntimeError("from worker")
    err._table_written_paths = [p.name for p in files_before_cleanup]
    sink.on_write_failed(err)

    # Orphans removed.
    assert _list_parquet_files(tmp_path) == []


def test_adapter_pickles_for_distributed_writes(tmp_path):
    """The adapter must round-trip through pickle so Ray Data can ship it
    to workers."""
    adapter = ToyParquetAdapter(str(tmp_path), target_file_size_bytes=4096)
    blob = pickle.dumps(adapter)
    restored = pickle.loads(blob)

    assert isinstance(restored, ToyParquetAdapter)
    assert restored.supported_modes == {SaveMode.APPEND, SaveMode.OVERWRITE}
    assert restored.get_name() == "ToyParquet"

    # Verify the restored adapter can still drive a full lifecycle.
    sink = TableDatasink(restored, SaveMode.APPEND)
    sink.on_write_start()
    r = _drive_one_task(sink, [pa.table({"id": [42]})])
    sink.on_write_complete(_wrap_write_result([r]))
    assert len(_list_parquet_files(tmp_path)) == 1


# ----------------------------------------------------------------------
# Partitioning — exercises the sort-based _partition_table rewrite
# (PR #63619 findings #3/#4/#5/#6).
# ----------------------------------------------------------------------


def test_partition_single_column_groups_correctly(tmp_path):
    """Single-column partitioning routes each distinct value to its own
    Hive directory; rows round-trip with partition column dropped from the
    payload."""
    adapter = ToyParquetAdapter(str(tmp_path), partition_cols=["k"])
    sink = TableDatasink(adapter, SaveMode.APPEND, partition_cols=["k"])
    sink.on_write_start()

    table = pa.table({"k": [1, 2, 1, 2, 1], "v": ["a", "b", "c", "d", "e"]})
    r = _drive_one_task(sink, [table])
    sink.on_write_complete(_wrap_write_result([r]))

    assert _partition_dirs(tmp_path) == ["k=1", "k=2"]
    # Reassemble all rows from all partition files; partition col is dropped
    # on disk, so re-attach it from the directory name.
    rows = []
    for f in _list_parquet_files_recursive(tmp_path):
        kval = int(f.parent.name.split("=")[1])
        t = _read_file_only(f)
        assert "k" not in t.column_names, "partition col must be dropped on disk"
        for v in t.column("v").to_pylist():
            rows.append((kval, v))
    assert sorted(rows) == [(1, "a"), (1, "c"), (1, "e"), (2, "b"), (2, "d")]


def test_partition_nan_rows_grouped_together_single_column(tmp_path):
    """All NaN rows in a float partition column land in one ``k=NaN``
    partition (not fragmented one-file-per-row). Covers #6 (within-table
    NaN grouping)."""
    adapter = ToyParquetAdapter(str(tmp_path), partition_cols=["k"])
    sink = TableDatasink(adapter, SaveMode.APPEND, partition_cols=["k"])
    sink.on_write_start()

    nan = float("nan")
    table = pa.table(
        {"k": [1.0, nan, 2.0, nan, nan], "v": ["a", "b", "c", "d", "e"]}
    )
    r = _drive_one_task(sink, [table])
    sink.on_write_complete(_wrap_write_result([r]))

    assert _partition_dirs(tmp_path) == ["k=1.0", "k=2.0", "k=NaN"]
    nan_files = list((tmp_path / "k=NaN").glob("*.parquet"))
    assert len(nan_files) == 1, "all NaN rows must coalesce into one file"
    nan_rows = _read_file_only(nan_files[0]).column("v").to_pylist()
    assert sorted(nan_rows) == ["b", "d", "e"]


def test_partition_nan_buffer_coalesces_across_calls(tmp_path):
    """With a large target_file_size_bytes, NaN-partition rows from multiple
    add_table calls accumulate into a single buffered file rather than
    fragmenting (every NaN being a distinct dict key). Covers #4."""
    adapter = ToyParquetAdapter(
        str(tmp_path), partition_cols=["k"], target_file_size_bytes=10**9
    )
    sink = TableDatasink(adapter, SaveMode.APPEND, partition_cols=["k"])
    sink.on_write_start()

    nan = float("nan")
    # Three separate blocks, each with a NaN-partition row.
    batches = [
        pa.table({"k": [nan], "v": [f"r{i}"]}) for i in range(3)
    ]
    r = _drive_one_task(sink, batches)
    sink.on_write_complete(_wrap_write_result([r]))

    nan_files = list((tmp_path / "k=NaN").glob("*.parquet"))
    assert len(nan_files) == 1, (
        f"NaN buffer should coalesce to one file, got {len(nan_files)}"
    )
    assert sorted(_read_file_only(nan_files[0]).column("v").to_pylist()) == [
        "r0",
        "r1",
        "r2",
    ]


def test_partition_multi_column_with_nulls_and_nan(tmp_path):
    """Multi-column partitioning groups correctly, including None and NaN
    in the key (covers #6 fallback-path corruption + #5 dead-code removal)."""
    adapter = ToyParquetAdapter(str(tmp_path), partition_cols=["a", "b"])
    sink = TableDatasink(adapter, SaveMode.APPEND, partition_cols=["a", "b"])
    sink.on_write_start()

    nan = float("nan")
    table = pa.table(
        {
            "a": ["x", "x", "y", None, None],
            "b": [1.0, 1.0, nan, nan, nan],
            "v": ["r0", "r1", "r2", "r3", "r4"],
        }
    )
    r = _drive_one_task(sink, [table])
    sink.on_write_complete(_wrap_write_result([r]))

    # Expected groups: (x,1.0)->{r0,r1}, (y,NaN)->{r2}, (None,NaN)->{r3,r4}
    files = _list_parquet_files_recursive(tmp_path)
    group_to_rows = {}
    for f in files:
        # parent dir is "b=...", grandparent is "a=..."
        b_dir = f.parent.name
        a_dir = f.parent.parent.name
        key = (a_dir, b_dir)
        group_to_rows.setdefault(key, []).extend(
            _read_file_only(f).column("v").to_pylist()
        )
    got = {k: sorted(v) for k, v in group_to_rows.items()}
    assert got == {
        ("a=x", "b=1.0"): ["r0", "r1"],
        ("a=y", "b=NaN"): ["r2"],
        ("a=__HIVE_DEFAULT_PARTITION__", "b=NaN"): ["r3", "r4"],
    }


def test_partition_high_cardinality_smoke(tmp_path):
    """A few hundred distinct partition values complete quickly and route
    correctly (sanity for the sort-based O(N log N) path, #3)."""
    adapter = ToyParquetAdapter(str(tmp_path), partition_cols=["k"])
    sink = TableDatasink(adapter, SaveMode.APPEND, partition_cols=["k"])
    sink.on_write_start()

    n = 300
    table = pa.table({"k": list(range(n)), "v": [f"r{i}" for i in range(n)]})
    r = _drive_one_task(sink, [table])
    sink.on_write_complete(_wrap_write_result([r]))

    assert len(_partition_dirs(tmp_path)) == n
    assert len(_list_parquet_files_recursive(tmp_path)) == n


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
