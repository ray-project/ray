"""Format adapter protocol for lakehouse datasinks.

A ``LakehouseAdapter`` plugs format-specific behaviour (Iceberg, Delta, Hudi, …)
into the generic ``LakehouseDatasink`` framework. The framework owns distributed
plumbing (lifecycle, schema unification, upsert-key concat, orphan cleanup); the
adapter owns table loading, per-block file writing, and transactional commit.

The method set maps one-to-one to the arrows in the sequence diagram approved
in the design plan:

    framework.write_datasink
      → adapter.preflight           # diagram step 3
      → adapter.on_write_start      # diagram step 4
      (workers)
        → adapter.start_task        # called once per worker task
        → adapter.write_block       # diagram step 5 (per Arrow table)
        → adapter.finalize_task     # flush any per-task buffer
      → adapter.reconcile_schema    # diagram step 7
      → adapter.build_delete_predicate
      → adapter.commit              # diagram step 8 (single method, mode-dispatched internally)
      (on failure)
      → adapter.on_failure
"""

from abc import ABC, abstractmethod
from typing import Any, Generic, List, Optional, Set, Tuple, TypeVar

import pyarrow as pa

from .modes import SaveMode, UpsertSemantics
from ray.data._internal.execution.interfaces import TaskContext

FileAction = TypeVar("FileAction")


class LakehouseAdapter(Generic[FileAction], ABC):
    """Plug-in for one lakehouse format.

    Subclasses implement the abstract methods. The framework calls them in a
    fixed order; see module docstring.
    """

    # ------------------------------------------------------------------
    # Introspection — declared once per adapter class.
    # ------------------------------------------------------------------
    @property
    @abstractmethod
    def supported_modes(self) -> Set[SaveMode]:
        """SaveMode values this adapter supports."""

    @property
    @abstractmethod
    def upsert_semantics(self) -> UpsertSemantics:
        """How the adapter implements UPSERT.

        Adapters that don't include UPSERT in ``supported_modes`` may return
        any value; the framework only consults this when the user requests
        UPSERT.
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

    # ------------------------------------------------------------------
    # Driver side, after every worker finishes.
    # ------------------------------------------------------------------
    def reconcile_schema(self, unified_schema: Optional[pa.Schema]) -> None:
        """Driver-side schema reconciliation.

        Adapters that evolve the table schema at commit time (e.g. Delta) use
        this hook. Adapters that already evolved in ``on_write_start`` (e.g.
        Iceberg) typically just stash ``unified_schema`` for later use.
        Default: no-op.
        """
        return None

    @abstractmethod
    def build_delete_predicate(
        self,
        mode: SaveMode,
        file_actions: List[FileAction],
        upsert_keys: Optional[pa.Table],
        join_cols: List[str],
        overwrite_filter: Optional[Any],
    ) -> Optional[Any]:
        """Return a format-specific delete predicate for the commit step.

        * APPEND → ``None``
        * full OVERWRITE → ``None`` (commit will drop everything)
        * partial OVERWRITE → predicate covering the filter / dynamic
          partitions to delete
        * UPSERT → predicate matching ``upsert_keys`` against ``join_cols``
        """

    @abstractmethod
    def commit(
        self,
        mode: SaveMode,
        file_actions: List[FileAction],
        delete_predicate: Optional[Any],
    ) -> None:
        """Apply the staged write to the table in one transaction.

        Internally branches on ``mode``:
          * APPEND — append the actions, commit the version.
          * OVERWRITE — delete with ``delete_predicate``, append, commit.
          * UPSERT — delete with ``delete_predicate``, append, commit.

        Adapters may treat ``file_actions == []`` as a special "empty commit"
        (e.g. to create an empty table); the framework always calls ``commit``
        once, even when nothing was written.
        """

    # ------------------------------------------------------------------
    # Failure handling.
    # ------------------------------------------------------------------
    def on_failure(self, written_paths: List[str]) -> None:
        """Best-effort orphan file cleanup. Default: no-op."""
        return None

    # ------------------------------------------------------------------
    # Optional introspection used by the framework for naming / scheduling.
    # ------------------------------------------------------------------
    def get_name(self) -> str:
        """Short human-readable name for write tasks. Default: class name."""
        return type(self).__name__

    @property
    def supports_distributed_writes(self) -> bool:
        """If ``False``, the framework will pin write tasks to the driver."""
        return True

    @property
    def min_rows_per_write(self) -> Optional[int]:
        """Target rows per write task; ``None`` lets Ray Data decide."""
        return None
