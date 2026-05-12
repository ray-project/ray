"""Tests for the generic ``LakehouseDatasink`` framework.

These tests exercise the framework in isolation by plugging in a
``FakeAdapter`` that records every lifecycle call. They guarantee that the
framework — independent of any specific lakehouse format — preserves the
contract shown in the design sequence diagram:

    preflight -> on_write_start -> (per worker: start_task, write_block*,
    finalize_task, task_metadata) -> gather_task_metadata -> reconcile_schema
    -> build_delete_predicate -> commit
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import pyarrow as pa
import pytest

from ray.data._internal.datasource.lakehouse import (
    LakehouseAdapter,
    LakehouseDatasink,
    LakehouseWriteTaskResult,
    SaveMode,
    UpsertSemantics,
)
from ray.data.datasource.datasink import WriteResult


@dataclass
class _FakeFileAction:
    path: str
    rows: int


@dataclass
class _FakeCall:
    """Record of a single framework -> adapter call."""

    method: str
    args: tuple = ()
    kwargs: Dict[str, Any] = field(default_factory=dict)


class FakeAdapter(LakehouseAdapter[_FakeFileAction]):
    """Minimal in-memory adapter that records every framework call."""

    def __init__(
        self,
        supported_modes: Optional[Set[SaveMode]] = None,
        upsert_semantics: UpsertSemantics = UpsertSemantics.COPY_ON_WRITE,
        fail_at: Optional[str] = None,
    ):
        self._supported_modes = supported_modes or {
            SaveMode.APPEND,
            SaveMode.OVERWRITE,
            SaveMode.UPSERT,
        }
        self._upsert_semantics = upsert_semantics
        self._fail_at = fail_at
        self.calls: List[_FakeCall] = []
        self._next_block_idx = 0
        # State the framework will inject.
        self._reconciled_schema: Optional[pa.Schema] = None
        self._predicate: Any = None

    # --- introspection --------------------------------------------------
    @property
    def supported_modes(self) -> Set[SaveMode]:
        return self._supported_modes

    @property
    def upsert_semantics(self) -> UpsertSemantics:
        return self._upsert_semantics

    def get_name(self) -> str:
        return "Fake"

    # --- lifecycle ------------------------------------------------------
    def _record(self, method: str, *args, **kwargs) -> None:
        self.calls.append(_FakeCall(method=method, args=args, kwargs=kwargs))
        if self._fail_at == method:
            raise RuntimeError(f"injected failure at {method}")

    def preflight(self, mode, partition_cols, declared_schema):
        self._record(
            "preflight",
            mode=mode,
            partition_cols=tuple(partition_cols),
            declared_schema=declared_schema,
        )

    def on_write_start(self, schema_from_first_bundle=None):
        self._record("on_write_start", schema=schema_from_first_bundle)

    def start_task(self, ctx):
        self._record("start_task", task_idx=getattr(ctx, "task_idx", None))

    def write_block(self, arrow_table):
        self._record("write_block", num_rows=arrow_table.num_rows)
        self._next_block_idx += 1
        upsert_keys = (
            arrow_table.select([arrow_table.column_names[0]])
            if "id" in arrow_table.column_names
            else None
        )
        return (
            [
                _FakeFileAction(
                    path=f"file-{self._next_block_idx}.parquet",
                    rows=arrow_table.num_rows,
                )
            ],
            arrow_table.schema,
            upsert_keys,
        )

    def finalize_task(self):
        self._record("finalize_task")
        return ([], [])

    def task_metadata(self) -> Dict[str, Any]:
        self._record("task_metadata")
        return {"write_uuid": "fake-uuid"}

    def gather_task_metadata(self, task_metadata: List[Dict[str, Any]]) -> None:
        self._record("gather_task_metadata", task_metadata=task_metadata)

    def reconcile_schema(self, unified_schema):
        self._record("reconcile_schema", schema=unified_schema)
        self._reconciled_schema = unified_schema

    def build_delete_predicate(
        self, mode, file_actions, upsert_keys, join_cols, overwrite_filter
    ):
        self._record(
            "build_delete_predicate",
            mode=mode,
            file_actions=tuple(file_actions),
            upsert_keys=upsert_keys,
            join_cols=tuple(join_cols),
            overwrite_filter=overwrite_filter,
        )
        if mode == SaveMode.APPEND:
            return None
        self._predicate = f"predicate-for-{mode.value}"
        return self._predicate

    def commit(self, mode, file_actions, delete_predicate):
        self._record(
            "commit",
            mode=mode,
            num_files=len(file_actions),
            delete_predicate=delete_predicate,
        )

    def on_failure(self, written_paths):
        self._record("on_failure", written_paths=tuple(written_paths))


# ----------------------------------------------------------------------
# Helpers.
# ----------------------------------------------------------------------


def _table(rows: int, cols: Optional[List[Tuple[str, pa.DataType]]] = None) -> pa.Table:
    cols = cols or [("id", pa.int64()), ("v", pa.string())]
    data = {
        name: pa.array(
            list(range(rows))
            if pa.types.is_integer(t)
            else [f"r{i}" for i in range(rows)],
            type=t,
        )
        for name, t in cols
    }
    return pa.table(data)


class _FakeCtx:
    """Simulate ``TaskContext`` for the framework's ``write`` entry-point."""

    def __init__(self, task_idx: int = 0):
        self.task_idx = task_idx
        self.kwargs: Dict[str, Any] = {}


def _drive_write(sink: LakehouseDatasink, batches: List[pa.Table], task_idx: int = 0):
    """Run the framework's ``write`` lifecycle on a single task's worth of
    Arrow tables and return the resulting ``LakehouseWriteTaskResult``."""
    return sink.write(iter(batches), _FakeCtx(task_idx=task_idx))


def _wrap_result(returns: List[LakehouseWriteTaskResult]) -> WriteResult:
    num_rows = sum(sum(a.rows for a in r.file_actions) for r in returns)
    size_bytes = num_rows * 8  # arbitrary; only the counters matter to the framework
    return WriteResult(
        num_rows=num_rows, size_bytes=size_bytes, write_returns=list(returns)
    )


# ----------------------------------------------------------------------
# Tests.
# ----------------------------------------------------------------------


def test_mode_validation_rejects_unsupported_mode():
    adapter = FakeAdapter(supported_modes={SaveMode.APPEND})
    with pytest.raises(ValueError, match="does not support mode"):
        LakehouseDatasink(adapter, SaveMode.UPSERT)


def test_mode_validation_accepts_supported_mode():
    adapter = FakeAdapter()
    sink = LakehouseDatasink(adapter, SaveMode.APPEND)
    assert sink.get_name() == "Fake"


def test_str_mode_is_coerced():
    adapter = FakeAdapter()
    sink = LakehouseDatasink(adapter, "append")
    assert sink._mode == SaveMode.APPEND


def test_invalid_mode_raises():
    adapter = FakeAdapter()
    with pytest.raises(ValueError, match="Invalid mode"):
        LakehouseDatasink(adapter, "nope")


def test_on_write_start_runs_preflight_then_adapter_hook():
    adapter = FakeAdapter()
    sink = LakehouseDatasink(adapter, SaveMode.APPEND, partition_cols=["p"])
    sink.on_write_start(schema=pa.schema([("a", pa.int64())]))
    methods = [c.method for c in adapter.calls]
    assert methods == ["preflight", "on_write_start"]
    assert adapter.calls[0].kwargs["partition_cols"] == ("p",)


def test_write_invokes_write_block_per_nonempty_table_and_skips_empties():
    adapter = FakeAdapter()
    sink = LakehouseDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()
    result = _drive_write(
        sink,
        [_table(0), _table(3), _table(0), _table(2)],
        task_idx=7,
    )
    methods = [c.method for c in adapter.calls]
    # preflight, on_write_start, start_task, write_block, write_block,
    # finalize_task, task_metadata
    assert methods == [
        "preflight",
        "on_write_start",
        "start_task",
        "write_block",
        "write_block",
        "finalize_task",
        "task_metadata",
    ]
    assert result.task_id == 7
    assert [a.rows for a in result.file_actions] == [3, 2]
    assert result.task_metadata == {"write_uuid": "fake-uuid"}


def test_on_write_complete_runs_lifecycle_in_diagram_order():
    adapter = FakeAdapter()
    sink = LakehouseDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()
    r = _drive_write(sink, [_table(4)])
    sink.on_write_complete(_wrap_result([r]))

    methods = [c.method for c in adapter.calls]
    assert methods == [
        "preflight",
        "on_write_start",
        "start_task",
        "write_block",
        "finalize_task",
        "task_metadata",
        "gather_task_metadata",
        "reconcile_schema",
        "build_delete_predicate",
        "commit",
    ]


def test_schema_reconciliation_type_promotes_across_workers():
    adapter = FakeAdapter()
    sink = LakehouseDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()
    r1 = _drive_write(sink, [_table(1, cols=[("a", pa.int32())])], task_idx=0)
    r2 = _drive_write(sink, [_table(1, cols=[("a", pa.int64())])], task_idx=1)
    sink.on_write_complete(_wrap_result([r1, r2]))

    reconcile = next(c for c in adapter.calls if c.method == "reconcile_schema")
    unified = reconcile.kwargs["schema"]
    assert unified is not None
    # int32 + int64 -> int64
    assert unified.field("a").type == pa.int64()


def test_upsert_predicate_passed_into_commit():
    adapter = FakeAdapter()
    sink = LakehouseDatasink(adapter, SaveMode.UPSERT, join_cols=["id"])
    sink.on_write_start()
    r = _drive_write(sink, [_table(2)])
    sink.on_write_complete(_wrap_result([r]))

    build = next(c for c in adapter.calls if c.method == "build_delete_predicate")
    commit = next(c for c in adapter.calls if c.method == "commit")
    assert build.kwargs["mode"] == SaveMode.UPSERT
    assert build.kwargs["join_cols"] == ("id",)
    assert commit.kwargs["delete_predicate"] == "predicate-for-upsert"


def test_overwrite_predicate_passed_into_commit():
    adapter = FakeAdapter()
    sink = LakehouseDatasink(adapter, SaveMode.OVERWRITE, overwrite_filter="my-filter")
    sink.on_write_start()
    r = _drive_write(sink, [_table(2)])
    sink.on_write_complete(_wrap_result([r]))

    build = next(c for c in adapter.calls if c.method == "build_delete_predicate")
    commit = next(c for c in adapter.calls if c.method == "commit")
    assert build.kwargs["overwrite_filter"] == "my-filter"
    assert commit.kwargs["delete_predicate"] == "predicate-for-overwrite"


def test_append_predicate_is_none():
    adapter = FakeAdapter()
    sink = LakehouseDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()
    r = _drive_write(sink, [_table(2)])
    sink.on_write_complete(_wrap_result([r]))

    commit = next(c for c in adapter.calls if c.method == "commit")
    assert commit.kwargs["delete_predicate"] is None


def test_duplicate_file_paths_across_tasks_raises():
    adapter = FakeAdapter()
    sink = LakehouseDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()
    r1 = _drive_write(sink, [_table(1)], task_idx=0)
    # Mint a second result that re-uses the same file path.
    duplicate_action = r1.file_actions[0]
    r2 = LakehouseWriteTaskResult(
        file_actions=[duplicate_action], emitted_schemas=r1.emitted_schemas
    )
    with pytest.raises(ValueError, match="Duplicate file paths"):
        sink.on_write_complete(_wrap_result([r1, r2]))


def test_commit_invoked_with_empty_actions():
    """Empty writes still get one commit call so adapters can create empty
    tables, no-op IGNORE, etc."""
    adapter = FakeAdapter()
    sink = LakehouseDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()
    sink.on_write_complete(_wrap_result([]))

    methods = [c.method for c in adapter.calls]
    # No worker ran, so no start_task / write_block / finalize_task /
    # task_metadata. But the driver still drives the commit lifecycle.
    assert methods == [
        "preflight",
        "on_write_start",
        "gather_task_metadata",
        "reconcile_schema",
        "build_delete_predicate",
        "commit",
    ]
    commit = adapter.calls[-1]
    assert commit.kwargs["num_files"] == 0


def test_on_write_failed_forwards_orphan_paths_to_adapter():
    adapter = FakeAdapter()
    sink = LakehouseDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()

    error = RuntimeError("boom")
    error._lakehouse_written_paths = ["a.parquet", "b.parquet"]
    sink.on_write_failed(error)

    failure = next(c for c in adapter.calls if c.method == "on_failure")
    assert failure.kwargs["written_paths"] == ("a.parquet", "b.parquet")


def test_on_write_failed_handles_adapter_cleanup_exception():
    adapter = FakeAdapter(fail_at="on_failure")
    sink = LakehouseDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()
    # Adapter raises inside on_failure; framework must swallow.
    error = RuntimeError("boom")
    error._lakehouse_written_paths = ["x.parquet"]
    sink.on_write_failed(error)  # must not raise


def test_write_attaches_orphan_paths_on_adapter_exception():
    adapter = FakeAdapter(fail_at="write_block")
    sink = LakehouseDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()
    with pytest.raises(RuntimeError, match="injected failure"):
        _drive_write(sink, [_table(1)])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
