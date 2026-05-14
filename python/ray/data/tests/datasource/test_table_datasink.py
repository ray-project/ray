"""Tests for the generic ``TableDatasink`` framework.

These tests exercise the framework in isolation by plugging in a
``FakeAdapter`` (APPEND + OVERWRITE only) or a ``FakeUpsertAdapter`` (also
conforming to ``SupportsUpserts``). They guarantee that the framework —
independent of any specific table format — preserves the contract shown in
the design sequence diagram:

    preflight -> on_write_start ->
    (per worker: start_task, write_block*, finalize_task, task_metadata) ->
    gather_task_metadata -> reconcile_schema ->
        (APPEND)    commit_append
        (OVERWRITE) build_overwrite_predicate -> commit_overwrite
        (UPSERT)    build_upsert_predicate -> commit_upsert
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import pyarrow as pa
import pytest

from ray.data._internal.datasource.table import (
    SaveMode,
    SupportsUpserts,
    TableAdapter,
    TableDatasink,
    TableWriteTaskResult,
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


class FakeAdapter(TableAdapter[_FakeFileAction, str]):
    """Minimal in-memory APPEND+OVERWRITE adapter that records every call.

    Does **not** support UPSERT — does not implement the ``SupportsUpserts``
    Protocol. ``FakeUpsertAdapter`` below extends this class with the upsert
    methods.
    """

    def __init__(
        self,
        supported_modes: Optional[Set[SaveMode]] = None,
        fail_at: Optional[str] = None,
    ):
        self._supported_modes = supported_modes or {
            SaveMode.APPEND,
            SaveMode.OVERWRITE,
        }
        self._fail_at = fail_at
        self.calls: List[_FakeCall] = []
        self._next_block_idx = 0
        # State the framework will inject.
        self._reconciled_schema: Optional[pa.Schema] = None

    # --- introspection --------------------------------------------------
    @property
    def supported_modes(self) -> Set[SaveMode]:
        return self._supported_modes

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

    # --- mode-specific commit methods -----------------------------------
    def commit_append(self, file_actions, unified_schema):
        self._record(
            "commit_append",
            num_files=len(file_actions),
            unified_schema=unified_schema,
        )

    def build_overwrite_predicate(self, overwrite_filter):
        self._record(
            "build_overwrite_predicate", overwrite_filter=overwrite_filter
        )
        return f"predicate-for-overwrite:{overwrite_filter!r}"

    def commit_overwrite(self, file_actions, unified_schema, delete_predicate):
        self._record(
            "commit_overwrite",
            num_files=len(file_actions),
            unified_schema=unified_schema,
            delete_predicate=delete_predicate,
        )

    def on_failure(self, written_paths):
        self._record("on_failure", written_paths=tuple(written_paths))


class FakeUpsertAdapter(FakeAdapter):
    """``FakeAdapter`` that also conforms to :class:`SupportsUpserts`."""

    upsert_semantics: UpsertSemantics = UpsertSemantics.COPY_ON_WRITE

    def __init__(
        self,
        supported_modes: Optional[Set[SaveMode]] = None,
        fail_at: Optional[str] = None,
    ):
        super().__init__(
            supported_modes=supported_modes
            or {SaveMode.APPEND, SaveMode.OVERWRITE, SaveMode.UPSERT},
            fail_at=fail_at,
        )

    def build_upsert_predicate(self, upsert_keys, join_cols):
        self._record(
            "build_upsert_predicate",
            upsert_keys=upsert_keys,
            join_cols=tuple(join_cols),
        )
        return f"predicate-for-upsert:{tuple(join_cols)}"

    def commit_upsert(self, file_actions, unified_schema, delete_predicate):
        self._record(
            "commit_upsert",
            num_files=len(file_actions),
            unified_schema=unified_schema,
            delete_predicate=delete_predicate,
        )


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


def _drive_write(sink: TableDatasink, batches: List[pa.Table], task_idx: int = 0):
    """Run the framework's ``write`` lifecycle on a single task's worth of
    Arrow tables and return the resulting ``TableWriteTaskResult``."""
    return sink.write(iter(batches), _FakeCtx(task_idx=task_idx))


def _wrap_result(returns: List[TableWriteTaskResult]) -> WriteResult:
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
        TableDatasink(adapter, SaveMode.OVERWRITE)


def test_mode_validation_accepts_supported_mode():
    adapter = FakeAdapter()
    sink = TableDatasink(adapter, SaveMode.APPEND)
    assert sink.get_name() == "Fake"


def test_str_mode_is_coerced():
    adapter = FakeAdapter()
    sink = TableDatasink(adapter, "append")
    assert sink._mode == SaveMode.APPEND


def test_invalid_mode_raises():
    adapter = FakeAdapter()
    with pytest.raises(ValueError, match="Invalid mode"):
        TableDatasink(adapter, "nope")


def test_upsert_rejected_for_non_upsert_capable_adapter():
    """An adapter that doesn't conform to ``SupportsUpserts`` must be
    refused at ``TableDatasink`` construction time when mode is UPSERT,
    even if it (incorrectly) lists UPSERT in supported_modes."""
    adapter = FakeAdapter(
        supported_modes={SaveMode.APPEND, SaveMode.OVERWRITE, SaveMode.UPSERT}
    )
    with pytest.raises(ValueError, match="does not support UPSERT"):
        TableDatasink(adapter, SaveMode.UPSERT)


def test_isinstance_supports_upserts_runtime_check():
    """``@runtime_checkable`` makes the structural check work."""
    assert not isinstance(FakeAdapter(), SupportsUpserts)
    assert isinstance(FakeUpsertAdapter(), SupportsUpserts)


def test_on_write_start_runs_preflight_then_adapter_hook():
    adapter = FakeAdapter()
    sink = TableDatasink(adapter, SaveMode.APPEND, partition_cols=["p"])
    sink.on_write_start(schema=pa.schema([("a", pa.int64())]))
    methods = [c.method for c in adapter.calls]
    assert methods == ["preflight", "on_write_start"]
    assert adapter.calls[0].kwargs["partition_cols"] == ("p",)


def test_write_invokes_write_block_per_nonempty_table_and_skips_empties():
    adapter = FakeAdapter()
    sink = TableDatasink(adapter, SaveMode.APPEND)
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


def test_on_write_complete_runs_lifecycle_in_diagram_order_for_append():
    adapter = FakeAdapter()
    sink = TableDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()
    r = _drive_write(sink, [_table(4)])
    sink.on_write_complete(_wrap_result([r]))

    methods = [c.method for c in adapter.calls]
    # APPEND skips both build_overwrite_predicate and build_upsert_predicate.
    assert methods == [
        "preflight",
        "on_write_start",
        "start_task",
        "write_block",
        "finalize_task",
        "task_metadata",
        "gather_task_metadata",
        "reconcile_schema",
        "commit_append",
    ]


def test_schema_reconciliation_type_promotes_across_workers():
    adapter = FakeAdapter()
    sink = TableDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()
    r1 = _drive_write(sink, [_table(1, cols=[("a", pa.int32())])], task_idx=0)
    r2 = _drive_write(sink, [_table(1, cols=[("a", pa.int64())])], task_idx=1)
    sink.on_write_complete(_wrap_result([r1, r2]))

    reconcile = next(c for c in adapter.calls if c.method == "reconcile_schema")
    unified = reconcile.kwargs["schema"]
    assert unified is not None
    # int32 + int64 -> int64
    assert unified.field("a").type == pa.int64()


def test_upsert_predicate_built_and_passed_into_commit_upsert():
    adapter = FakeUpsertAdapter()
    sink = TableDatasink(adapter, SaveMode.UPSERT, join_cols=["id"])
    sink.on_write_start()
    r = _drive_write(sink, [_table(2)])
    sink.on_write_complete(_wrap_result([r]))

    build = next(c for c in adapter.calls if c.method == "build_upsert_predicate")
    commit = next(c for c in adapter.calls if c.method == "commit_upsert")
    assert build.kwargs["join_cols"] == ("id",)
    assert commit.kwargs["delete_predicate"] == "predicate-for-upsert:('id',)"


def test_overwrite_predicate_built_and_passed_into_commit_overwrite():
    adapter = FakeAdapter()
    sink = TableDatasink(adapter, SaveMode.OVERWRITE, overwrite_filter="my-filter")
    sink.on_write_start()
    r = _drive_write(sink, [_table(2)])
    sink.on_write_complete(_wrap_result([r]))

    build = next(c for c in adapter.calls if c.method == "build_overwrite_predicate")
    commit = next(c for c in adapter.calls if c.method == "commit_overwrite")
    assert build.kwargs["overwrite_filter"] == "my-filter"
    assert commit.kwargs["delete_predicate"] == "predicate-for-overwrite:'my-filter'"


def test_append_invokes_only_commit_append_with_no_predicate():
    adapter = FakeAdapter()
    sink = TableDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()
    r = _drive_write(sink, [_table(2)])
    sink.on_write_complete(_wrap_result([r]))

    methods = [c.method for c in adapter.calls]
    # No build_*_predicate calls of any kind for APPEND.
    assert "build_overwrite_predicate" not in methods
    assert "build_upsert_predicate" not in methods
    commit = next(c for c in adapter.calls if c.method == "commit_append")
    assert commit.kwargs["num_files"] == 1


def test_duplicate_file_paths_across_tasks_raises():
    adapter = FakeAdapter()
    sink = TableDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()
    r1 = _drive_write(sink, [_table(1)], task_idx=0)
    # Mint a second result that re-uses the same file path.
    duplicate_action = r1.file_actions[0]
    r2 = TableWriteTaskResult(
        file_actions=[duplicate_action], emitted_schemas=r1.emitted_schemas
    )
    with pytest.raises(ValueError, match="Duplicate file paths"):
        sink.on_write_complete(_wrap_result([r1, r2]))


def test_commit_append_invoked_with_empty_actions():
    """Empty writes still get one commit_append call so adapters can create
    empty tables, no-op IGNORE, etc."""
    adapter = FakeAdapter()
    sink = TableDatasink(adapter, SaveMode.APPEND)
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
        "commit_append",
    ]
    commit = adapter.calls[-1]
    assert commit.kwargs["num_files"] == 0


def test_on_write_failed_forwards_orphan_paths_to_adapter():
    adapter = FakeAdapter()
    sink = TableDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()

    error = RuntimeError("boom")
    error._table_written_paths = ["a.parquet", "b.parquet"]
    sink.on_write_failed(error)

    failure = next(c for c in adapter.calls if c.method == "on_failure")
    assert failure.kwargs["written_paths"] == ("a.parquet", "b.parquet")


def test_on_write_failed_handles_adapter_cleanup_exception():
    adapter = FakeAdapter(fail_at="on_failure")
    sink = TableDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()
    # Adapter raises inside on_failure; framework must swallow.
    error = RuntimeError("boom")
    error._table_written_paths = ["x.parquet"]
    sink.on_write_failed(error)  # must not raise


def test_write_attaches_orphan_paths_on_adapter_exception():
    adapter = FakeAdapter(fail_at="write_block")
    sink = TableDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()
    with pytest.raises(RuntimeError, match="injected failure"):
        _drive_write(sink, [_table(1)])


# ----------------------------------------------------------------------
# PR #63619 review fixes.
# ----------------------------------------------------------------------


@pytest.mark.parametrize("mode", [SaveMode.ERROR, SaveMode.IGNORE])
def test_error_ignore_modes_route_to_commit_append(mode):
    """ERROR / IGNORE that survive preflight must commit via commit_append
    (not silently drop the written files). Regression for PR #63619 #11."""
    adapter = FakeAdapter(
        supported_modes={SaveMode.APPEND, SaveMode.ERROR, SaveMode.IGNORE}
    )
    sink = TableDatasink(adapter, mode)
    sink.on_write_start()
    r = _drive_write(sink, [_table(3)])
    sink.on_write_complete(_wrap_result([r]))

    commit = next(
        (c for c in adapter.calls if c.method == "commit_append"), None
    )
    assert commit is not None, f"{mode} must route to commit_append"
    assert commit.kwargs["num_files"] == 1
    # No overwrite/upsert commit paths should fire.
    assert not any(
        c.method in ("commit_overwrite", "commit_upsert") for c in adapter.calls
    )


def test_path_for_action_default_reads_path_attribute():
    """The default path_for_action reads ``action.path``."""
    adapter = FakeAdapter()
    action = _FakeFileAction(path="foo/bar.parquet", rows=1)
    assert adapter.path_for_action(action) == "foo/bar.parquet"


def test_path_for_action_override_drives_duplicate_detection():
    """An adapter whose action names the path field differently can override
    path_for_action; the framework's dedup must use the override. Regression
    for PR #63619 #1 (Iceberg DataFile uses file_path, not path)."""

    @dataclass
    class _IcebergLikeFile:
        file_path: str  # note: NOT ``path``
        rows: int = 0  # for the _wrap_result row counter only

    class _IcebergLikeAdapter(FakeAdapter):
        def write_block(self, arrow_table):
            self._record("write_block", num_rows=arrow_table.num_rows)
            self._next_block_idx += 1
            return (
                [
                    _IcebergLikeFile(
                        file_path=f"file-{self._next_block_idx}.parquet",
                        rows=arrow_table.num_rows,
                    )
                ],
                arrow_table.schema,
                None,
            )

        def path_for_action(self, action):
            return getattr(action, "file_path", None)

    adapter = _IcebergLikeAdapter()
    # Sanity: the default impl would miss this field, the override catches it.
    assert (
        TableAdapter.path_for_action(
            adapter, _IcebergLikeFile(file_path="x.parquet")
        )
        is None
    )
    assert adapter.path_for_action(_IcebergLikeFile(file_path="x.parquet")) == (
        "x.parquet"
    )

    # Duplicate file_path across two tasks must now be detected.
    sink = TableDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()
    r1 = _drive_write(sink, [_table(1)], task_idx=0)
    dup = r1.file_actions[0]
    r2 = TableWriteTaskResult(
        file_actions=[dup], emitted_schemas=r1.emitted_schemas
    )
    with pytest.raises(ValueError, match="Duplicate file paths"):
        sink.on_write_complete(_wrap_result([r1, r2]))


def test_upsert_with_no_keys_passes_none_predicate_safely():
    """When no worker emits upsert keys, build_upsert_predicate receives
    ``None`` and the lifecycle still completes. Regression for PR #63619 #14."""
    adapter = FakeUpsertAdapter()
    sink = TableDatasink(adapter, SaveMode.UPSERT, join_cols=["id"])
    sink.on_write_start()
    # No write tasks ran -> no key chunks -> upsert_keys is None.
    sink.on_write_complete(_wrap_result([]))

    build = next(c for c in adapter.calls if c.method == "build_upsert_predicate")
    assert build.kwargs["upsert_keys"] is None
    commit = next(c for c in adapter.calls if c.method == "commit_upsert")
    assert commit.kwargs["num_files"] == 0


def test_create_mode_routes_to_commit_append():
    """SaveMode.CREATE must dispatch to commit_append (same commit semantics
    as ERROR), not fall through to the unsupported-mode error after files are
    written. Regression for PR #63619 (cursor, high)."""
    adapter = FakeAdapter(
        supported_modes={SaveMode.CREATE, SaveMode.APPEND, SaveMode.OVERWRITE}
    )
    sink = TableDatasink(adapter, SaveMode.CREATE)
    sink.on_write_start()
    r = _drive_write(sink, [_table(2)])
    sink.on_write_complete(_wrap_result([r]))

    commit = next(
        (c for c in adapter.calls if c.method == "commit_append"), None
    )
    assert commit is not None, "CREATE must route to commit_append"
    assert commit.kwargs["num_files"] == 1
    assert not any(
        c.method in ("commit_overwrite", "commit_upsert") for c in adapter.calls
    )


def test_orphan_paths_attached_when_task_metadata_raises():
    """If a post-write step (task_metadata / upsert-key concat) raises, the
    orphan-path list must still ride out on the exception so on_write_failed
    can clean up files this task already wrote. Regression for PR #63619
    (cursor, low)."""
    adapter = FakeAdapter(fail_at="task_metadata")
    sink = TableDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()

    with pytest.raises(RuntimeError, match="injected failure at task_metadata"):
        _drive_write(sink, [_table(2)])

    # The block wrote one file before task_metadata blew up; its path must be
    # captured on the exception for cleanup.
    try:
        _drive_write(sink, [_table(2)])
    except RuntimeError as e:
        assert getattr(e, "_table_written_paths", None), (
            "orphan paths must be attached to the exception"
        )
        assert all(p.endswith(".parquet") for p in e._table_written_paths)


def test_gather_receives_one_entry_per_task_even_when_metadata_empty():
    """Tasks whose task_metadata() returns {} must still appear in the list
    handed to gather_task_metadata (one entry per task that ran). Regression
    for PR #63619 (cursor, medium)."""

    class _EmptyMetaAdapter(FakeAdapter):
        def task_metadata(self):
            self._record("task_metadata")
            return {}

    adapter = _EmptyMetaAdapter()
    sink = TableDatasink(adapter, SaveMode.APPEND)
    sink.on_write_start()
    r1 = _drive_write(sink, [_table(1)], task_idx=0)
    r2 = _drive_write(sink, [_table(1)], task_idx=1)
    sink.on_write_complete(_wrap_result([r1, r2]))

    gather = next(c for c in adapter.calls if c.method == "gather_task_metadata")
    # Pre-fix the truthiness guard dropped both empty dicts -> length 0.
    assert len(gather.kwargs["task_metadata"]) == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
