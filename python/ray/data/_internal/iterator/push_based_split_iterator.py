"""Consumer side of the push-based streaming_split.

Holds the receiver and its reorder buffer, ``PushSplitReceiverMixin``, and
``PushBasedDataIterator``. See ``push_split_coordinator.py`` for an
overview of the design.
"""

import logging
import queue
import threading
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Union,
    cast,
)

import ray
from ray.data._internal.block_batching.interfaces import ResolvedBlock
from ray.data._internal.block_batching.iter_batches import BatchIterator
from ray.data._internal.iterator.push_split_coordinator import (
    PushSplitCoordinator,
    _BlockPush,
    _EndOfEpoch,
    _ExecutorError,
    _SequencedItem,
)
from ray.data._internal.stats import DatasetStats
from ray.data._internal.util import create_streaming_split_dataset
from ray.data.block import Block
from ray.data.context import DataContext
from ray.data.iterator import DataIterator, _BatchSource

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import NodeIdStr
    from ray.data.dataset import Dataset, Schema

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
_RECEIVER_REGISTRY: Dict[str, "_PushReceiver"] = {}


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
                self.queue.put(self.reorder_pending.pop(self.reorder_next_seq))
                self.reorder_next_seq += 1

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
        key: str,
        epoch_id: int,
        seq: int,
        item: _SequencedItem,
        block: Optional[Block] = None,
    ) -> None:
        receiver = _RECEIVER_REGISTRY.get(key)
        if receiver is not None:
            receiver.deliver(epoch_id, seq, item, block)

    def _push_split_deliver_error(
        self, key: str, epoch_id: int, error: _ExecutorError
    ) -> None:
        receiver = _RECEIVER_REGISTRY.get(key)
        if receiver is not None:
            receiver.deliver_error(epoch_id, error)


class _MaterializedBatchIterator(BatchIterator):
    """BatchIterator over already-materialized blocks: skips the ref-level
    prefetch/resolve stages, inherits everything else unchanged."""

    def __init__(self, blocks: Iterator[ResolvedBlock], **kwargs):
        # BatchIterator stores its input and hands it to _pipeline, which
        # this class overrides to consume blocks instead of RefBundles.
        super().__init__(cast(Any, blocks), **kwargs)

    def _pipeline(self, ref_bundles: Iterator[Any]):
        batch_iter = self._blocks_to_batches(ref_bundles)
        batch_iter = self._format_batches(batch_iter)
        if self._preserve_order:
            batch_iter = self._restore_original_batch_order(batch_iter)
        batch_iter = self._finalize_batches(batch_iter)
        yield from batch_iter


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
        return [PushBasedDataIterator(coord_actor, i, n) for i in range(n)]

    def __init__(
        self,
        coord_actor: ray.actor.ActorHandle,
        output_split_idx: int,
        world_size: int,
    ):
        self._coord_actor = coord_actor
        self._output_split_idx = output_split_idx
        self._world_size = world_size
        self._iter_stats = DatasetStats(metadata={}, parent=None)
        # Epoch this split is currently consuming. Written by the consuming
        # thread; a stale generator uses it to detect that its epoch ended.
        self._active_epoch: Optional[int] = None
        # Prefetch window, refreshed by _create_batch_source from the user's
        # iter_batches() arguments.
        self._prefetch_batches = 1
        self._prefetch_batch_size: Optional[int] = None

    def _receiver_key(self) -> str:
        return f"{self._coord_actor._actor_id.hex()}:{self._output_split_idx}"

    def _to_ref_bundle_iterator(self):
        # Blocks arrive by value, so there are no RefBundles to return;
        # iter_batches goes through _create_batch_source instead.
        raise NotImplementedError(
            "Push-based streaming splits receive blocks by value and don't "
            "support RefBundle-based APIs such as materialize(). Iterate the "
            "split with iter_batches() instead."
        )

    def _create_batch_source(self, **batch_iterator_kwargs) -> _BatchSource:
        # Runs on the thread consuming batches (the block generator itself is
        # pulled from a helper thread). A default actor runs its tasks on the
        # main thread; blocking it would starve the delivery tasks and hang.
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
        if self._checkpointer is not None:
            raise NotImplementedError(
                "Data checkpointing isn't supported for push-based streaming "
                "splits yet."
            )
        # Capture the prefetch window before iteration starts; _iter_blocks
        # reads it lazily on its first next().
        self._prefetch_batches = batch_iterator_kwargs.get("prefetch_batches", 1)
        self._prefetch_batch_size = batch_iterator_kwargs.get("batch_size")
        dataset_tags = self._get_dataset_tag()
        batch_iterator = _MaterializedBatchIterator(
            self._iter_blocks(),
            stats=self._iter_stats,
            dataset_tags=dataset_tags,
            **batch_iterator_kwargs,
        )
        return _BatchSource(batch_iterator, self._iter_stats, None, dataset_tags)

    def _iter_blocks(self) -> Iterator[ResolvedBlock]:
        """Register with the coordinator, start the epoch, and yield the
        blocks pushed to this split."""
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
        with _RECEIVER_REGISTRY_LOCK:
            receiver = _RECEIVER_REGISTRY.get(key)
            if receiver is None:
                receiver = _PushReceiver()
                _RECEIVER_REGISTRY[key] = receiver
        # Reset receiver state before the barrier; re-registering every
        # epoch is fine (idempotent overwrite).
        receiver.reset()
        ray.get(
            self._coord_actor.register.remote(
                self._output_split_idx, self_handle, key=key
            )
        )
        epoch = cast(
            int,
            ray.get(self._coord_actor.start_epoch.remote(self._output_split_idx)),
        )
        self._active_epoch = epoch
        receiver.begin_epoch(epoch)

        # Prefetch window, in rows: declare a `prefetch_batches *
        # batch_size` row window and report consumption; the coordinator
        # computes what to send (target_rows - (rows_pushed -
        # rows_consumed)) and pushes whole blocks while that is
        # positive, so any positive window yields at least one block.
        # Blocks pushed but not yet consumed sit in the local receiver
        # queue — that queue IS the prefetch buffer. Without a batch
        # size the window degenerates to one block in flight.
        if self._prefetch_batches > 0 and self._prefetch_batch_size:
            target_rows = self._prefetch_batches * self._prefetch_batch_size
        else:
            target_rows = 1

        def report(consumed_rows: int, consumed_bytes: int) -> None:
            # One RPC per consumed block. Rows are reported at pop (they
            # drive the window); bytes are reported one block late so
            # the block currently being batched still counts as
            # consumer-held for producer pacing.
            self._coord_actor.request_rows.remote(
                self._output_split_idx,
                epoch,
                target_rows,
                consumed_rows,
                consumed_bytes,
            )

        pending_consumed_bytes = 0
        report(0, 0)
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
            report(item.num_rows, pending_consumed_bytes)
            pending_consumed_bytes = item.size_bytes
            yield ResolvedBlock(block=item.block)

    def _on_iteration_end(self, executor) -> None:
        """Notify the coordinator on any end of iteration (exhaustion, early
        break, or exception); runs on the consumer thread."""
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

    def world_size(self) -> int:
        return self._world_size

    def _get_dataset_tag(self) -> Dict[str, Optional[str]]:
        return cast(
            Dict[str, Optional[str]],
            ray.get(self._coord_actor.get_dataset_tag.remote(self._output_split_idx)),
        )
