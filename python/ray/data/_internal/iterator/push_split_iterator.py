"""Consumer side of the push-based streaming_split.

Holds the receiver and its reorder buffer, ``PushSplitReceiverMixin``, and
``PushBasedDataIterator``. See ``push_split_coordinator.py`` for an
overview of the design.
"""

import logging
import queue
import sys
import threading
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
    cast,
)

import ray
from ray.data._internal.block_batching.interfaces import ResolvedBlock
from ray.data._internal.execution.interfaces import BlockEntry, RefBundle
from ray.data._internal.iterator.push_split_coordinator import (
    PushSplitCoordinator,
    ReceiverKey,
    _BlockPush,
    _EndOfEpoch,
    _ExecutorError,
    _SequencedItem,
)
from ray.data._internal.stats import DatasetStats
from ray.data._internal.util import create_streaming_split_dataset
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext
from ray.data.iterator import DataIterator, _BatchSource

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import NodeIdStr
    from ray.data.dataset import Dataset, Schema
    from ray.types import ObjectRef

logger = logging.getLogger(__name__)


@dataclass
class _BlockDelivery:
    """Local queue entry: one materialized Block + its size."""

    block: Block
    size_bytes: int
    num_rows: int


_QueueItem = Union[_BlockDelivery, _EndOfEpoch, _ExecutorError]


def streaming_split_push_based(
    dataset: "Dataset",
    n: int,
    *,
    equal: bool = False,
    locality_hints: Optional[List["NodeIdStr"]] = None,
) -> List["PushBasedDataIterator"]:
    """Push-based counterpart of :meth:`Dataset.streaming_split`.

    Same arguments and split semantics. Each returned iterator must be
    iterated from inside an actor whose class mixes in
    ``PushSplitReceiverMixin``.
    """
    # TODO(push-split): move this into dataset.py as the implementation of
    # Dataset.streaming_split, replacing the pull-based split.
    split_dataset = create_streaming_split_dataset(
        dataset, n, equal=equal, locality_hints=locality_hints
    )
    return PushBasedDataIterator.create(split_dataset, n)


# ---------------------------------------------------------------------------
# Consumer side. Deliveries arrive via PushSplitReceiverMixin's actor methods
# and land in the process-local registry, where the PushBasedDataIterator
# (running on another thread in the same process) picks them up.
# ---------------------------------------------------------------------------

_RECEIVER_REGISTRY_LOCK = threading.Lock()
_RECEIVER_REGISTRY: Dict[ReceiverKey, "_PushReceiver"] = {}


def _get_or_create_receiver(key: ReceiverKey) -> "_PushReceiver":
    with _RECEIVER_REGISTRY_LOCK:
        receiver = _RECEIVER_REGISTRY.get(key)
        if receiver is None:
            receiver = _PushReceiver()
            _RECEIVER_REGISTRY[key] = receiver
        return receiver


class _PushReceiver:
    """Receive state for one (coordinator, split) pair: the local block
    queue plus the sequence-reorder buffer.

    The reorder buffer makes delivery order independent of the hosting
    actor's concurrency: a default single-threaded actor (like a Ray Train
    worker) already executes deliveries in push order, but an actor with
    ``max_concurrency > 1`` runs them out of order.
    """

    def __init__(self):
        self.queue: "queue.Queue[_QueueItem]" = queue.Queue()
        self.lock = threading.Lock()
        # Bytes of the blocks in ``queue``: this split's prefetched bytes.
        self.queued_bytes = 0
        self.cur_epoch: Optional[int] = None
        self.reorder_epoch: Optional[int] = None
        self.reorder_next_seq = 0
        self.reorder_pending: Dict[int, _QueueItem] = {}

    def reset(self) -> None:
        """Reset before re-arriving at the epoch barrier.

        Swaps in a fresh queue object (instead of draining) so a lingering
        generator from an early-exited epoch cannot steal the new epoch's
        deliveries.
        """
        with self.lock:
            self.cur_epoch = None
            self.reorder_epoch = None
            self.reorder_next_seq = 0
            self.reorder_pending = {}
            self.queue = queue.Queue()
            self.queued_bytes = 0

    def begin_epoch(self, epoch: int) -> None:
        with self.lock:
            # Set before the first request_rows, so no delivery can arrive
            # while cur_epoch is stale.
            self.cur_epoch = epoch

    def deliver(
        self,
        epoch_id: int,
        seq: int,
        item: _SequencedItem,
        block: Optional[Block] = None,
    ) -> None:
        """Deliver one sequenced item, releasing items to the queue in seq
        order. Stale-epoch items are dropped."""
        if isinstance(item, _BlockPush):
            queue_item: _QueueItem = _BlockDelivery(
                block, item.size_bytes, item.num_rows
            )
        else:
            queue_item = item
        with self.lock:
            if epoch_id != self.cur_epoch:
                return
            if self.reorder_epoch != epoch_id:
                self.reorder_epoch = epoch_id
                self.reorder_next_seq = 0
                self.reorder_pending = {}
            self.reorder_pending[seq] = queue_item
            while self.reorder_next_seq in self.reorder_pending:
                released = self.reorder_pending.pop(self.reorder_next_seq)
                if isinstance(released, _BlockDelivery):
                    self.queued_bytes += released.size_bytes
                self.queue.put(released)
                self.reorder_next_seq += 1

    def on_block_popped(self, epoch_queue: "queue.Queue[_QueueItem]", size: int) -> int:
        """Record that the iterator took a block of ``size`` bytes from
        ``epoch_queue``. Returns the bytes that remain queued."""
        with self.lock:
            # A stale generator pops from a queue that reset() replaced; its
            # blocks are not part of the current count.
            if epoch_queue is self.queue:
                self.queued_bytes -= size
            return self.queued_bytes

    def deliver_error(self, epoch_id: int, error: _ExecutorError) -> None:
        """Deliver an _ExecutorError immediately (fail fast, unsequenced)."""
        with self.lock:
            if epoch_id == self.cur_epoch:
                self.queue.put(error)


class PushSplitReceiverMixin:
    """Receive methods for actors hosting PushBasedDataIterators; mix into
    the consumer's actor class (e.g. a Ray Train worker).

    A stateless shim over the registry, so it needs no ``__init__``
    cooperation: one actor may host several shards, and the iterator reaches
    the same ``_PushReceiver`` state through the registry.
    """

    def _push_split_deliver(
        self,
        key: ReceiverKey,
        epoch_id: int,
        seq: int,
        item: _SequencedItem,
        block: Optional[Block] = None,
    ) -> None:
        receiver = _RECEIVER_REGISTRY.get(key)
        # None when the delivery arrives after iteration ended (the iterator
        # drops its receiver then) or after this process restarted. Either
        # way the delivery is stale, so it's dropped. It can't arrive before
        # the receiver exists: the iterator creates it before registering.
        if receiver is not None:
            receiver.deliver(epoch_id, seq, item, block)

    def _push_split_deliver_error(
        self, key: ReceiverKey, epoch_id: int, error: _ExecutorError
    ) -> None:
        receiver = _RECEIVER_REGISTRY.get(key)
        # None for stale deliveries; see _push_split_deliver.
        if receiver is not None:
            receiver.deliver_error(epoch_id, error)


class PushBasedDataIterator(DataIterator):
    """DataIterator over one split of a push-based streaming split.

    Picklable; ship it into any actor whose class mixes in
    ``PushSplitReceiverMixin`` (e.g. a Ray Train worker). At iteration time
    it registers the hosting actor with the coordinator, then iterates the
    blocks the coordinator pushes into the local receiver queue.
    """

    @staticmethod
    def create(
        split_dataset: "Dataset",
        n: int,
    ) -> List["PushBasedDataIterator"]:
        """Create the coordinator and one iterator per split.

        ``split_dataset`` must already be wrapped in a ``StreamingSplit``
        logical op (see ``create_streaming_split_dataset``).
        """
        # pyrefly: ignore[missing-attribute]  # @ray.remote hides ActorClass.options
        coord_actor = PushSplitCoordinator.options(
            # n barrier-blocked start_epoch calls + headroom for other RPCs.
            max_concurrency=n + 2,
            label_selector={
                # pyrefly: ignore[missing-attribute]  # constant lives in the Cython ext
                ray._raylet.RAY_NODE_ID_KEY: ray.get_runtime_context().get_node_id()
            },
        ).remote(split_dataset, n)
        return [PushBasedDataIterator(coord_actor, i) for i in range(n)]

    def __init__(
        self,
        coord_actor: ray.actor.ActorHandle,
        output_split_idx: int,
    ):
        self._coord_actor = coord_actor
        self._output_split_idx = output_split_idx
        self._iter_stats = DatasetStats(metadata={}, parent=None)
        # Epoch this split is currently consuming. Written by the consuming
        # thread; a stale generator uses it to detect that its epoch ended.
        self._active_epoch: Optional[int] = None
        # Prefetch window, refreshed by _create_batch_source from the user's
        # iter_batches() arguments.
        self._prefetch_batches = 1
        self._prefetch_batch_size: Optional[int] = None

    def _receiver_key(self) -> ReceiverKey:
        return f"{self._coord_actor._actor_id.hex()}:{self._output_split_idx}"

    def _to_ref_bundle_iterator(
        self,
    ) -> Tuple[Iterator[RefBundle], Optional[DatasetStats], None]:
        """RefBundles over this split's blocks, for ``materialize()``.

        Blocks arrive by value, so this puts each one back into the object
        store to get a reference. The caller wants every block, so the flow
        window is unbounded: the coordinator pushes as fast as the executor
        produces. ``iter_batches`` doesn't use this; it reads the blocks
        directly through ``_create_batch_source``.
        """
        self._check_not_on_actor_task_thread()

        def gen_ref_bundles() -> Iterator[RefBundle]:
            try:
                # materialize() adds each bundle's size to the prefetched
                # bytes itself, like it does for the pull split.
                for resolved in self._iter_blocks(
                    target_rows=sys.maxsize, track_prefetched_bytes=False
                ):
                    accessor = BlockAccessor.for_block(resolved.block)
                    block_ref = cast("ObjectRef[Block]", ray.put(resolved.block))
                    entry = BlockEntry(block_ref, accessor.get_metadata())
                    yield RefBundle(
                        (entry,), owns_blocks=False, schema=accessor.schema()
                    )
            finally:
                # materialize() has no end-of-iteration hook, so finish the
                # epoch here.
                self._finish_epoch()

        return gen_ref_bundles(), self._iter_stats, None

    def _check_not_on_actor_task_thread(self) -> None:
        # A default actor runs its tasks on the main thread; blocking it
        # would starve the delivery tasks and hang.
        if (
            ray.get_runtime_context().get_actor_id() is not None
            and threading.current_thread() is threading.main_thread()
        ):
            raise RuntimeError(
                "PushBasedDataIterator can't be iterated on the actor's task "
                "thread: deliveries run as tasks on the same actor. Iterate "
                "from a background thread (as Ray Train does) or give the "
                "actor max_concurrency > 1."
            )

    def _create_batch_source(self, **batch_iterator_kwargs) -> _BatchSource:
        # Runs on the thread consuming batches (the block generator itself is
        # pulled from a helper thread).
        self._check_not_on_actor_task_thread()
        # Capture the prefetch window before iteration starts; _iter_blocks
        # reads it lazily on its first next().
        self._prefetch_batches = batch_iterator_kwargs.get("prefetch_batches", 1)
        self._prefetch_batch_size = batch_iterator_kwargs.get("batch_size")
        dataset_tags = self._get_dataset_tag()
        # Lazy import: this module loads with ``import ray.train``, and the
        # batch iterator module loads ``ray.data.checkpoint`` submodules that
        # Ray otherwise loads only when a dataset has a checkpoint config.
        from ray.data._internal.iterator.push_split_batch_iterator import (
            _MaterializedBatchIterator,
        )

        batch_iterator = _MaterializedBatchIterator(
            self._iter_blocks(),
            checkpointer=self._checkpointer,
            stats=self._iter_stats,
            dataset_tags=dataset_tags,
            **batch_iterator_kwargs,
        )
        return _BatchSource(batch_iterator, self._iter_stats, None, dataset_tags)

    def _iter_blocks(
        self,
        target_rows: Optional[int] = None,
        track_prefetched_bytes: bool = True,
    ) -> Iterator[ResolvedBlock]:
        """Join the next epoch and yield the blocks pushed to this split.

        Args:
            target_rows: The flow window, in rows. Defaults to the prefetch
                window of the current ``iter_batches`` call.
            track_prefetched_bytes: Whether to report the bytes in the local
                queue as this split's prefetched bytes.
        """
        receiver, epoch = self._join_epoch()
        if target_rows is None:
            target_rows = self._prefetch_window_rows()
        pending_consumed_bytes = 0
        # Like the pull split, don't carry prefetched bytes across epochs.
        self._iter_stats.iter_prefetched_bytes = 0
        self._update_flow_window(epoch, target_rows, 0, 0)
        # reset() gave this epoch a fresh queue, so a lingering
        # generator from an early-exited epoch can't steal deliveries;
        # the loop condition below reaps such generators.
        epoch_queue = receiver.queue

        while self._active_epoch == epoch:
            try:
                # Queue-wait time lands in the get_ref_bundles iterator
                # stat: the push analog of waiting on the coordinator.
                with self._iter_stats.iter_get_ref_bundles_s.timer():
                    item = epoch_queue.get(timeout=1.0)
            except queue.Empty:
                continue
            if isinstance(item, _EndOfEpoch):
                logger.debug(
                    f"Split {self._output_split_idx}: epoch {epoch} exhausted."
                )
                return
            if isinstance(item, _ExecutorError):
                raise item.error
            assert isinstance(item, _BlockDelivery)
            # The blocks still in the local queue are this split's prefetched
            # blocks, like the pull split's prefetch window.
            queued_bytes = receiver.on_block_popped(epoch_queue, item.size_bytes)
            if track_prefetched_bytes:
                self._iter_stats.iter_prefetched_bytes = queued_bytes
            self._update_flow_window(
                epoch, target_rows, item.num_rows, pending_consumed_bytes
            )
            pending_consumed_bytes = item.size_bytes
            yield ResolvedBlock(block=item.block)

    def _join_epoch(self) -> Tuple["_PushReceiver", int]:
        """Register this split's receiver with the coordinator and wait at
        the epoch barrier. Returns the receiver and the new epoch id."""
        try:
            self_handle = ray.get_runtime_context().current_actor
            assert self_handle is not None
        except Exception as e:
            raise RuntimeError(
                "PushBasedDataIterator must be iterated from inside a Ray "
                "actor whose class mixes in PushSplitReceiverMixin (e.g. "
                "a Ray Train worker): the coordinator pushes blocks to "
                "the hosting actor's receiver methods."
            ) from e

        key = self._receiver_key()
        receiver = _get_or_create_receiver(key)
        # Reset receiver state before the barrier; re-registering every
        # epoch is fine (idempotent overwrite).
        receiver.reset()
        ray.get(
            self._coord_actor.register_consumer.remote(
                self._output_split_idx, self_handle, key=key
            )
        )
        epoch = cast(
            int,
            ray.get(self._coord_actor.start_epoch.remote(self._output_split_idx)),
        )
        self._active_epoch = epoch
        receiver.begin_epoch(epoch)
        return receiver, epoch

    def _prefetch_window_rows(self) -> int:
        """Prefetch window, in rows: ``prefetch_batches * batch_size``.

        The coordinator computes what to send (target_rows - (rows_pushed -
        rows_consumed)) and pushes whole blocks while that is positive, so
        any positive window yields at least one block. Blocks pushed but not
        yet consumed sit in the local receiver queue — that queue IS the
        prefetch buffer. Without a batch size the window degenerates to one
        block in flight.
        """
        if self._prefetch_batches > 0 and self._prefetch_batch_size:
            return self._prefetch_batches * self._prefetch_batch_size
        return 1

    def _update_flow_window(
        self,
        epoch: int,
        target_rows: int,
        consumed_rows: int,
        consumed_bytes: int,
    ) -> None:
        """One RPC per consumed block: reports what was consumed and
        re-declares the row window, which frees room for more rows.

        Rows are reported at pop (they drive the window); bytes are reported
        one block late so the block currently being batched still counts as
        consumer-held for producer pacing.
        """
        self._coord_actor.request_rows.remote(
            self._output_split_idx,
            epoch,
            target_rows,
            consumed_rows,
            consumed_bytes,
        )

    def _on_iteration_end(self, executor) -> None:
        """Runs on the consumer thread on any end of ``iter_batches``
        (exhaustion, early break, or exception)."""
        self._finish_epoch()
        # Blocks left in the queue after an early break are dropped.
        self._iter_stats.iter_prefetched_bytes = 0

    def _finish_epoch(self) -> None:
        """Notify the coordinator that this split stopped consuming."""
        epoch = self._active_epoch
        if epoch is None:
            return
        self._active_epoch = None
        self._coord_actor.notify_split_finished.remote(epoch, self._output_split_idx)
        # Drop this split's receive state, so blocks still queued (e.g. after
        # an early break) don't stay pinned in this process. The next epoch
        # creates a fresh receiver, and late deliveries for a removed key are
        # dropped. The sentinel wakes a generator blocked on the queue.
        with _RECEIVER_REGISTRY_LOCK:
            receiver = _RECEIVER_REGISTRY.pop(self._receiver_key(), None)
        if receiver is not None:
            receiver.queue.put(_EndOfEpoch(epoch))

    def stats(self) -> str:
        stats = cast(DatasetStats, ray.get(self._coord_actor.stats.remote()))
        summary = stats.to_summary()
        summary.iter_stats = self._iter_stats.to_summary().iter_stats
        summary.iter_stats.streaming_split_coord_time.add(
            stats.streaming_split_coordinator_s.get()
        )
        return summary.to_string()

    def schema(self) -> Optional["Schema"]:
        return cast(
            Optional["Schema"],
            ray.get(self._coord_actor.get_dataset_schema.remote()),
        )

    def get_context(self) -> DataContext:
        return cast(
            DataContext, ray.get(self._coord_actor.get_dataset_context.remote())
        )

    def _get_dataset_tag(self) -> Dict[str, Optional[str]]:
        return cast(
            Dict[str, Optional[str]],
            ray.get(self._coord_actor.get_dataset_tag.remote(self._output_split_idx)),
        )
