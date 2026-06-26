"""Format adapter protocol for table datasinks.

A ``TableAdapter`` plugs format-specific behaviour (Iceberg, Delta, Hudi, …)
into the generic ``TableDatasink`` framework. The framework owns distributed
plumbing (lifecycle, schema unification, upsert-key concat, orphan cleanup);
the adapter owns table loading, per-block file writing, and transactional
commit.

The contract is split in two:

* :class:`TableAdapter` — APPEND + OVERWRITE baseline. Every adapter
  implements this.
* :class:`SupportsUpserts` — opt-in :class:`typing.Protocol` for UPSERT
  capability. Adapters that support UPSERT inherit (or duck-type-conform to)
  this Protocol; the framework dispatches via ``isinstance(adapter,
  SupportsUpserts)`` and refuses ``mode=UPSERT`` for adapters that don't.

Why split? Adapters that don't support UPSERT (e.g. Delta in the current
build) shouldn't have to declare a fictional ``upsert_semantics`` value just
to satisfy a base class. The Protocol lets the type checker prove that
non-upsert adapters don't carry upsert state, and lets new adapters opt in
without inheritance gymnastics.

Sequence diagram (ASCII)::

                   ┌──────────────────────────────────────────────┐
                   │  TableDatasink   (framework, generic)        │
                   └──────────────────────────────────────────────┘
                                       │
   Ray Data ──── on_write_start ───────┤
                                       │   ┌─────────────────────────┐
                                       │──▶│ adapter.preflight        │ (1)
                                       │   │ adapter.on_write_start   │ (2)
                                       │   └─────────────────────────┘
                                       │
                  (per write task, on workers)
                                       │   ┌─────────────────────────┐
   Ray Data ──── write(blocks) ────────┤──▶│ adapter.start_task       │
                                       │   │ adapter.write_block × N  │ (3)
                                       │   │ adapter.finalize_task    │
                                       │   │ adapter.task_metadata    │
                                       │   └─────────────────────────┘
                                       │
   Ray Data ──── on_write_complete ────┤
                                       │   ┌──────────────────────────────┐
                                       │──▶│ adapter.gather_task_metadata │ (4)
                                       │   │ adapter.reconcile_schema     │ (5)
                                       │   │                              │
                                       │   │  if APPEND:                  │
                                       │   │    adapter.commit_append     │ (6a)
                                       │   │                              │
                                       │   │  if OVERWRITE:               │
                                       │   │    adapter.build_overwrite_  │ (6b)
                                       │   │      predicate               │
                                       │   │    adapter.commit_overwrite  │
                                       │   │                              │
                                       │   │  if UPSERT (SupportsUpserts):│
                                       │   │    adapter.build_upsert_     │ (6c)
                                       │   │      predicate               │
                                       │   │    adapter.commit_upsert     │
                                       │   └──────────────────────────────┘
                                       │
   Ray Data ──── on_write_failed ──────┤   ┌─────────────────────────┐
                                       │──▶│ adapter.on_failure        │ (7)
                                       │   └─────────────────────────┘

Steps:
  (1) Load table from catalog/log; validate mode legality and
      schema/partitions.
  (2) Pre-write hook fed the first input bundle's schema (Iceberg evolves
      schema here; Delta no-ops and evolves at commit time).
  (3) Per Arrow table: write Parquet to storage; return
      ``(file_actions, emitted_schema, optional upsert_keys)``.
  (4) Driver receives per-task metadata dicts (e.g. Delta's per-write UUID).
  (5) Apply the unified worker schema to the table state.
  (6a/b/c) One mode-specific commit. All paths atomically apply
      ``file_actions``.
  (7) Best-effort cleanup of files that were written by tasks that
      subsequently failed; never destroys committed data.
"""

from abc import ABC, abstractmethod
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Optional,
    Protocol,
    Set,
    Tuple,
    TypeVar,
    runtime_checkable,
)

import pyarrow as pa

from .modes import SaveMode, UpsertSemantics
from ray.data._internal.execution.interfaces import TaskContext

FileAction = TypeVar("FileAction")
DeletePredicate = TypeVar("DeletePredicate")


# ---------------------------------------------------------------------------
# Shared per-call retry-override extraction.
# ---------------------------------------------------------------------------
# The same three keys are honoured uniformly across every table adapter:
#   - commit_retry_max_attempts
#   - commit_retry_max_backoff_s
#   - commit_retried_errors
# Adapters call ``_extract_retry_overrides`` from their ``__init__`` to pop
# the keys out of the user-supplied ``**write_kwargs`` (so they don't leak
# into the underlying format library and trigger an unknown-kwarg error) and
# stash the three values for later consumption by their ``_with_retry``.

RETRY_OVERRIDE_KEYS: Tuple[str, str, str] = (
    "commit_retry_max_attempts",
    "commit_retry_max_backoff_s",
    "commit_retried_errors",
)


def _extract_retry_overrides(
    write_kwargs: Dict[str, Any],
) -> Tuple[Optional[int], Optional[int], Optional[List[str]]]:
    """Pop the standard per-call retry-override keys from ``write_kwargs``.

    Returns ``(max_attempts, max_backoff_s, retried_errors)``. Any key not
    present in ``write_kwargs`` becomes ``None`` in the returned tuple. The
    input dict is **mutated** — recognised keys are removed so they don't
    leak into format-library kwargs.

    Recognised keys (see ``RETRY_OVERRIDE_KEYS``):
        - ``commit_retry_max_attempts``
        - ``commit_retry_max_backoff_s``
        - ``commit_retried_errors``

    Each is surfaceable globally via ``DataContext.table_write_config`` and
    per-format via ``DataContext.{delta,iceberg}_config``; this helper just
    drains the per-call override layer.
    """
    return (
        write_kwargs.pop("commit_retry_max_attempts", None),
        write_kwargs.pop("commit_retry_max_backoff_s", None),
        write_kwargs.pop("commit_retried_errors", None),
    )


class TableAdapter(Generic[FileAction, DeletePredicate], ABC):
    """Plug-in for one table format. APPEND + OVERWRITE baseline.

    Subclasses implement the abstract methods. The framework calls them in
    the order shown in the module-level sequence diagram. Adapters that also
    support UPSERT additionally implement the :class:`SupportsUpserts`
    Protocol.

    Type parameters:
        FileAction: Per-file metadata produced by ``write_block`` (e.g.
            PyIceberg's ``DataFile``, deltalake's ``AddAction``).
        DeletePredicate: Format-specific predicate type produced by
            ``build_overwrite_predicate`` and consumed by ``commit_overwrite``
            (e.g. PyIceberg's ``BooleanExpression``, Delta's SQL ``str``).
    """

    # ------------------------------------------------------------------
    # Introspection — declared once per adapter class.
    # ------------------------------------------------------------------
    @property
    @abstractmethod
    def supported_modes(self) -> Set[SaveMode]:
        """SaveMode values this adapter supports.

        UPSERT may appear here only if the adapter also conforms to
        :class:`SupportsUpserts`; the framework enforces this at
        ``TableDatasink`` construction time.
        """

    # ------------------------------------------------------------------
    # Driver, before workers start.
    # ------------------------------------------------------------------
    @abstractmethod
    def preflight(
        self,
        mode: SaveMode,
        partition_cols: List[str],
        declared_schema: Optional[pa.Schema],
    ) -> None:
        """Load the underlying table and validate the requested write.

        Implementations typically:
          * reach into the catalog / log to load the current table state,
          * validate that the mode is legal against the table state
            (e.g. UPSERT requires an existing table),
          * validate partition columns / declared schema against the existing
            table.

        Must raise a descriptive error on conflict.
        """

    def on_write_start(
        self, schema_from_first_bundle: Optional[pa.Schema] = None
    ) -> None:
        """Optional pre-write hook fed the first input bundle's schema.

        Iceberg uses this hook to evolve the table schema before any files
        land, avoiding name-mapping errors during writes. Delta no-ops here
        and evolves at commit time. Default: no-op.
        """
        return None

    # ------------------------------------------------------------------
    # Worker side, executed inside each Ray write task.
    # ------------------------------------------------------------------
    def start_task(self, ctx: TaskContext) -> None:
        """Called once per task before the first ``write_block``.

        Adapters that need per-task state (e.g. a file writer, a write UUID
        pulled from ``ctx.kwargs``) should initialize it here. Default:
        no-op.
        """
        return None

    @abstractmethod
    def write_block(
        self, arrow_table: pa.Table
    ) -> Tuple[List[FileAction], pa.Schema, Optional[pa.Table]]:
        """Write a single Arrow table to the object store.

        Returns a 3-tuple of:
          * the list of per-file actions produced (may be empty if the
            adapter is buffering and didn't flush yet),
          * the emitted schema for this block (used later by
            ``reconcile_schema``),
          * the projected upsert-key sub-table for this block, or ``None``
            if not in UPSERT mode.
        """

    def finalize_task(self) -> Tuple[List[FileAction], List[pa.Schema]]:
        """Flush per-task buffers, if any.

        Returns extra ``(file_actions, schemas)`` produced when the buffer
        is drained. Default: nothing buffered.
        """
        return ([], [])

    def task_metadata(self) -> Dict[str, Any]:
        """Adapter-defined free-form metadata to ship back to the driver.

        Called by the framework once per task after ``finalize_task`` and
        embedded in the ``TableWriteTaskResult.task_metadata`` field.
        Default: empty dict.
        """
        return {}

    # ------------------------------------------------------------------
    # Driver side, after every worker finishes.
    # ------------------------------------------------------------------
    def gather_task_metadata(self, task_metadata: List[Dict[str, Any]]) -> None:
        """Receive the per-task metadata dicts produced by ``task_metadata``.

        Called once on the driver before ``reconcile_schema``. Adapters can
        merge whatever they need (e.g. a shared write UUID, total row
        counts). Default: no-op.
        """
        return None

    def reconcile_schema(self, unified_schema: Optional[pa.Schema]) -> None:
        """Driver-side schema reconciliation.

        Adapters that evolve the table schema at commit time (e.g. Delta)
        use this hook. Adapters that already evolved in ``on_write_start``
        (e.g. Iceberg) typically just stash ``unified_schema`` for later
        use. Default: no-op.
        """
        return None

    @abstractmethod
    def commit_append(
        self,
        file_actions: List[FileAction],
        unified_schema: Optional[pa.Schema],
    ) -> None:
        """Apply an APPEND write atomically.

        Adapters may treat ``file_actions == []`` as a special "empty
        commit" (e.g. to create an empty table); the framework always
        invokes ``commit_append`` once, even when nothing was written.
        """

    @abstractmethod
    def build_overwrite_predicate(
        self, overwrite_filter: Optional[Any]
    ) -> Optional[DeletePredicate]:
        """Return a format-specific predicate covering rows OVERWRITE deletes.

        ``overwrite_filter`` is the user-supplied filter expression for
        partial overwrites; ``None`` means "replace all rows".
        Implementations return ``None`` for full overwrite (commit will drop
        everything) or a predicate for partial overwrite.
        """

    @abstractmethod
    def commit_overwrite(
        self,
        file_actions: List[FileAction],
        unified_schema: Optional[pa.Schema],
        delete_predicate: Optional[DeletePredicate],
    ) -> None:
        """Apply an OVERWRITE write atomically.

        Deletes rows matching ``delete_predicate`` (or everything when
        ``delete_predicate is None``) and appends ``file_actions`` in the
        same transaction.
        """

    # ------------------------------------------------------------------
    # File-action introspection.
    # ------------------------------------------------------------------
    @abstractmethod
    def path_for_action(self, action: FileAction) -> Optional[str]:
        """Return the relative path the ``action`` represents.

        The framework calls this for two purposes:
          * duplicate-file detection across tasks (``on_write_complete``), and
          * orphan-file tracking for cleanup on failure (``write`` ->
            ``on_write_failed``).

        Each adapter must implement this explicitly to name its file type's
        path field — there is intentionally **no silent default**, so an
        adapter whose metadata doesn't expose ``.path`` can't accidentally opt
        out of dedup + cleanup. PyIceberg's ``DataFile`` exposes ``file_path``;
        deltalake's ``AddAction`` exposes ``path``. Return ``None`` only to
        *deliberately* opt a file action out of both.
        """

    # ------------------------------------------------------------------
    # Failure handling.
    # ------------------------------------------------------------------
    def on_failure(self, written_paths: List[str]) -> None:
        """Best-effort orphan file cleanup. Default: no-op."""
        return None

    def get_written_paths(self) -> List[str]:
        """Relative paths this adapter has *started* writing in the task.

        The framework queries this when a write task raises, to recover files
        that were begun but not yet returned as completed file actions — e.g.
        a Parquet file registered just before ``pq.write_table`` raised
        mid-write. Such in-flight paths never appear in the returned file
        actions, so without this hook they would escape orphan cleanup.

        Default: ``[]`` (adapters whose writer tracks started paths override
        this — e.g. by returning ``list(writer.written_files)``).
        """
        return []

    # ------------------------------------------------------------------
    # Optional introspection used by the framework for naming / scheduling.
    # ------------------------------------------------------------------
    def get_name(self) -> str:
        """Short human-readable name for write tasks. Default: class name."""
        return type(self).__name__

    @property
    def supports_distributed_writes(self) -> bool:
        """If ``False``, the framework pins write tasks to the driver."""
        return True


@runtime_checkable
class SupportsUpserts(Protocol[FileAction, DeletePredicate]):
    """Opt-in capability mixin for adapters that implement UPSERT.

    Adapters that support UPSERT either:
      * inherit from this Protocol explicitly
        (``class FooAdapter(TableAdapter[F, D], SupportsUpserts[F, D])``),
        or
      * structurally conform to the Protocol (define ``upsert_semantics``,
        ``build_upsert_predicate``, ``commit_upsert``) — the
        ``@runtime_checkable`` decorator lets ``isinstance(adapter,
        SupportsUpserts)`` succeed in either case.

    The framework checks this Protocol at ``TableDatasink`` construction
    time and refuses ``SaveMode.UPSERT`` for adapters that don't conform.
    """

    #: How this adapter implements UPSERT (e.g. COPY_ON_WRITE for Iceberg's
    #: scan-merge approach, MERGE_ON_READ for Hudi-style overlays).
    upsert_semantics: UpsertSemantics

    def build_upsert_predicate(
        self, upsert_keys: Optional[pa.Table], join_cols: List[str]
    ) -> Optional[DeletePredicate]:
        """Return a delete predicate matching ``upsert_keys`` on ``join_cols``.

        ``upsert_keys`` is the framework-concatenated key table aggregated
        across every worker, or ``None`` when no worker emitted any keys
        (e.g. every input block was empty). Implementations must treat
        ``None`` — and an empty table — as "no rows to match", returning
        ``None`` so the write degrades to a pure insert.

        Implementations also typically filter out null keys before
        constructing the predicate (SQL-style: NULL never matches), and
        return ``None`` if nothing remains.
        """
        ...

    def commit_upsert(
        self,
        file_actions: List[FileAction],
        unified_schema: Optional[pa.Schema],
        delete_predicate: Optional[DeletePredicate],
    ) -> None:
        """Apply an UPSERT write atomically.

        Deletes rows matching ``delete_predicate`` and appends
        ``file_actions`` in the same transaction.
        """
        ...
