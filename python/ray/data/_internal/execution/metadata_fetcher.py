"""Metadata-fetch strategy for the streaming executor.

Two modes, selected by ``RAY_DATA_METADATA_PREFETCH_ON_THREAD`` (default on):

- :class:`ThreadedMetadataFetcher` (default): defer every pair and fetch its
  metadata on a background :class:`MetadataPrefetcher` thread, so the scheduling
  loop never blocks on ``ray.get(meta_ref)``. The output-budget size comes from
  the block's local ``object_size`` (no RPC); completion is postponed until the
  task's deferred pairs have emitted.
- :class:`InlineMetadataFetcher`: the pre-threading behavior — fetch each pair's
  metadata inline with ``ray.get`` and emit the ``RefBundle`` immediately,
  budgeting off ``meta.size_bytes``. Bit-identical to the historical path
  (completion and task-failure are handled inline by ``on_data_ready``).
"""

import logging
import pickle
from abc import ABC, abstractmethod
from collections.abc import Hashable
from typing import Any, Dict, List, Optional, Tuple

import ray
from ray._common.utils import env_bool
from ray.data._internal.execution.interfaces.physical_operator import (
    METADATA_WAIT_TIMEOUT_S,
    DataOpTask,
    DeferredEmit,
)
from ray.data._internal.execution.metadata_prefetcher import MetadataPrefetcher
from ray.data.block import BlockMetadataWithSchema
from ray.experimental.locations import get_local_object_locations
from ray.util.debug import log_once

logger = logging.getLogger(__name__)

# Timeout (s) for the inline metadata ``ray.get``. The timeout includes shipping
# the metadata to this node, so a 0 timeout could cancel an in-flight download;
# a small non-zero value avoids that. Matches the historical value.
METADATA_GET_TIMEOUT_S = 1.0

# Selects the mode (see module docstring). Threaded by default; set to 0/false to
# fall back to the synchronous, master-identical inline path.
_PREFETCH_ON_THREAD = env_bool("RAY_DATA_METADATA_PREFETCH_ON_THREAD", True)


class MetadataFetcher(ABC):
    def start(self) -> None:
        """Start any background machinery (no-op unless overridden)."""

    def stop(self) -> None:
        """Stop any background machinery (no-op unless overridden)."""

    @abstractmethod
    def in_loop_get_size(
        self,
        task: DataOpTask,
        block_ref: "ray.ObjectRef[Any]",
        meta_ref: "ray.ObjectRef[Any]",
    ) -> Optional[int]:
        """Handle one pulled pair inside the ``on_data_ready`` loop.

        Returns the output-budget bytes for this pair, or ``None`` to mean "the
        metadata isn't available yet — stop and retry next iteration" (the
        caller breaks, leaving the refs set).
        """

    def in_loop_done(self, task: DataOpTask) -> None:
        """Called once a task is drained (generator exhausted/failed). The
        inline mode fires the done-callback here (re-raising a task failure);
        the threaded mode postpones it, so this is a no-op there."""

    def submit(self, op_key: Hashable) -> None:
        """Hand the operator's deferred pairs off for processing (no-op unless
        overridden)."""

    def register_drained(self, tasks: List[DataOpTask]) -> None:
        """Record end-of-stream/failed tasks whose completion is postponed
        (no-op unless overridden)."""

    def after_loop_batch(self) -> List[Tuple[str, BaseException]]:
        """Run once at the end of ``process_completed_tasks``. Returns
        ``(operator_name, exception)`` for each block-level fetch failure, for
        the caller's ``max_errored_blocks`` accounting. Default: nothing to do."""
        return []


class InlineMetadataFetcher(MetadataFetcher):
    """Synchronous, master-identical mode: fetch metadata inline and emit
    immediately. Holds no state and starts no thread."""

    def in_loop_get_size(
        self,
        task: DataOpTask,
        block_ref: "ray.ObjectRef[Any]",
        meta_ref: "ray.ObjectRef[Any]",
    ) -> Optional[int]:
        try:
            # The timeout includes the time to ship the metadata to this node,
            # so a 0 timeout could cancel an in-flight download. Use a small
            # non-zero value to avoid that.
            meta_bytes: bytes = ray.get(meta_ref, timeout=METADATA_GET_TIMEOUT_S)
        except ray.exceptions.GetTimeoutError:
            # We have refs to the block and its metadata, but the metadata
            # object isn't available. This can happen if the node dies. Leave
            # the pair pending and retry next iteration.
            logger.warning(
                f"Timed out ({METADATA_GET_TIMEOUT_S}s) waiting for metadata from "
                f"operator '{task.operator_name}' "
                f"(metadata_ref={meta_ref.hex()}). "
                f"Possible causes include a worker crash, node preemption, or an "
                f"overloaded worker or head node. Will retry next iteration. "
                f"If this repeats, check the Ray dashboard and logs for worker "
                f"crashes, node preemption, or overload."
            )
            return None
        return task.emit_block(block_ref, meta_bytes)

    def in_loop_done(self, task: DataOpTask) -> None:
        # Inline mode fires the done-callback the moment the generator drains
        # (matching the historical timing): all of the task's pairs have already
        # emitted inline, so ``_pending_emit_count`` is 0. A task failure is
        # re-raised after the callback, as before.
        task.mark_done()
        if task.task_error is not None:
            raise task.task_error from None


class ThreadedMetadataFetcher(MetadataFetcher):
    """Asynchronous mode: defer every pair and fetch its metadata on a
    background :class:`MetadataPrefetcher` thread."""

    def __init__(self, prefetcher: Optional[MetadataPrefetcher] = None):
        # Injectable so tests can drive a prefetcher whose results they publish
        # by hand.
        self.prefetcher = prefetcher if prefetcher is not None else MetadataPrefetcher()
        # Pairs deferred by ``in_loop_get_size`` for the current operator, flushed
        # to the prefetcher by ``submit``. Owned here (not threaded through
        # ``on_data_ready``) since deferring is the threaded mode's concern.
        self._deferred: List[DeferredEmit] = []

    def start(self) -> None:
        self.prefetcher.start()

    def stop(self) -> None:
        self.prefetcher.stop()

    def in_loop_get_size(
        self,
        task: DataOpTask,
        block_ref: "ray.ObjectRef[Any]",
        meta_ref: "ray.ObjectRef[Any]",
    ) -> Optional[int]:
        # Output-budget size from the block's local object_size (no RPC).
        # Normally known: the driver owns the just-yielded block ref, so the
        # value (which matches ``meta.size_bytes``) is in the local object
        # directory.
        # TODO: ``object_size`` is the object-store size of the block, which can
        # differ from ``meta.size_bytes`` (the in-memory/logical size). We should
        # add an explicit ``object_size_bytes`` to ``BlockMetadata`` and use it
        # directly, so the fallback below doesn't conflate the two.
        info: Optional[Dict[str, Any]] = get_local_object_locations([block_ref]).get(
            block_ref
        )
        object_size: Optional[int] = (
            info.get("object_size") if info is not None else None
        )
        if object_size is None:
            # Rare: no local size record. Fall back to a short metadata
            # ``ray.get`` for the size. Log once to flag the path without
            # spamming if it recurs.
            if log_once(f"data_object_size_unavailable_{task.operator_name}"):
                logger.warning(
                    "Local object_size unavailable for a block from operator "
                    "'%s'; falling back to its metadata for the output-budget "
                    "size.",
                    task.operator_name,
                )
            try:
                meta_with_schema: BlockMetadataWithSchema = pickle.loads(
                    ray.get(meta_ref, timeout=METADATA_WAIT_TIMEOUT_S)
                )
            except ray.exceptions.GetTimeoutError:
                # Metadata isn't local yet either. Leave this pair pending and
                # retry next iteration.
                return None
            object_size = meta_with_schema.metadata.size_bytes

        self._deferred.append(
            DeferredEmit(task=task, block_ref=block_ref, meta_ref=meta_ref)
        )
        return object_size

    def submit(self, op_key: Hashable) -> None:
        self.prefetcher.submit(op_key, self._deferred)
        self._deferred = []

    def register_drained(self, tasks: List[DataOpTask]) -> None:
        self.prefetcher.register_drained_tasks(tasks)

    def after_loop_batch(self) -> List[Tuple[str, BaseException]]:
        # Emit whatever's ready (per-op order) then fire postponed done
        # callbacks. Deferred metadata-fetch failures are returned for the
        # caller's ``max_errored_blocks`` accounting.
        return self.prefetcher.emit_ready() + self.prefetcher.fire_done_callbacks()


def make_metadata_fetcher() -> MetadataFetcher:
    """Build the metadata fetcher for the configured mode (see module
    docstring)."""
    if _PREFETCH_ON_THREAD:
        return ThreadedMetadataFetcher()
    return InlineMetadataFetcher()
