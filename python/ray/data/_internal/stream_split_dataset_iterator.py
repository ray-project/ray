import copy
import logging
import sys
import threading
from typing import (
    List,
    Dict,
    Optional,
    Iterator,
    Callable,
    Any,
    Union,
    TYPE_CHECKING,
)

import ray

from ray.data.dataset_iterator import DatasetIterator
from ray.data.block import Block, DataBatch
from ray.data.context import DatasetContext
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data._internal.execution.legacy_compat import (
    execute_to_legacy_bundle_iterator,
)
from ray.data._internal.block_batching import batch_block_refs
from ray.data._internal.execution.operators.output_splitter import OutputSplitter
from ray.data._internal.execution.interfaces import NodeIdStr, RefBundle
from ray.types import ObjectRef
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

if TYPE_CHECKING:
    import pyarrow
    from ray.data import Dataset

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

logger = logging.getLogger(__name__)


class StreamSplitDatasetIterator(DatasetIterator):
    """Implements a collection of iterators over a shared data stream."""

    @staticmethod
    def create(
        base_dataset: "Dataset",
        n: int,
        equal: bool,
        locality_hints: Optional[List[NodeIdStr]],
    ) -> List["StreamSplitDatasetIterator"]:
        """Create a split iterator from the given base Dataset and options.

        See also: `Dataset.streaming_split`.
        """
        ctx = DatasetContext.get_current()

        # To avoid deadlock, the concurrency on this actor must be set to at least `n`.
        coord_actor = SplitCoordinator.options(
            max_concurrency=n,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                ray.get_runtime_context().get_node_id(), soft=False
            ),
        ).remote(ctx, base_dataset, n, equal, locality_hints)

        return [
            StreamSplitDatasetIterator(base_dataset, coord_actor, i) for i in range(n)
        ]

    def __init__(
        self,
        base_dataset: "Dataset",
        coord_actor: ray.actor.ActorHandle,
        output_split_idx: int,
    ):
        self._base_dataset = base_dataset
        self._coord_actor = coord_actor
        self._output_split_idx = output_split_idx

    def iter_batches(
        self,
        *,
        prefetch_blocks: int = 0,
        batch_size: int = 256,
        batch_format: Literal["default", "numpy", "pandas"] = "default",
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
        _collate_fn: Optional[Callable[[DataBatch], Any]] = None,
    ) -> Iterator[DataBatch]:
        """Implements DatasetIterator."""

        def gen_blocks() -> Iterator[ObjectRef[Block]]:
            future: ObjectRef[
                Optional[ObjectRef[Block]]
            ] = self._coord_actor.get.remote(self._output_split_idx)
            while True:
                block_ref: Optional[ObjectRef[Block]] = ray.get(future)
                if not block_ref:
                    break
                else:
                    future = self._coord_actor.get.remote(self._output_split_idx)
                    yield block_ref

        yield from batch_block_refs(
            gen_blocks(),
            stats=None,
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            batch_format=batch_format,
            drop_last=drop_last,
            collate_fn=_collate_fn,
            shuffle_buffer_min_size=local_shuffle_buffer_size,
            shuffle_seed=local_shuffle_seed,
        )

    def stats(self) -> str:
        """Implements DatasetIterator."""
        return self._base_dataset.stats()

    def schema(self) -> Union[type, "pyarrow.lib.Schema"]:
        """Implements DatasetIterator."""
        return self._base_dataset.schema()


@ray.remote(num_cpus=0)
class SplitCoordinator:
    """Coordinator actor for routing blocks to output splits.

    This actor runs a streaming executor locally on its main thread. Clients can
    retrieve results via actor calls running on other threads.
    """

    def __init__(
        self,
        ctx: DatasetContext,
        dataset: "Dataset",
        n: int,
        equal: bool,
        locality_hints: Optional[List[NodeIdStr]],
    ):
        # Automatically set locality with output to the specified location hints.
        if locality_hints:
            ctx.execution_options.locality_with_output = locality_hints
            logger.info(f"Auto configuring locality_with_output={locality_hints}")

        DatasetContext._set_current(ctx)
        self._base_dataset = dataset
        self._n = n
        self._equal = equal
        self._locality_hints = locality_hints
        self._finished = False
        self._lock = threading.RLock()
        # Guarded by self._lock.
        self._next_bundle: Dict[int, RefBundle] = {}

        executor = StreamingExecutor(copy.deepcopy(ctx.execution_options))

        def add_split_op(dag):
            return OutputSplitter(dag, n, equal, locality_hints)

        self._output_iterator = execute_to_legacy_bundle_iterator(
            executor,
            dataset._plan,
            True,
            dataset._plan._dataset_uuid,
            dag_rewrite=add_split_op,
        )

    def get(self, output_split_idx: int) -> Optional[ObjectRef[Block]]:
        """Blocking get operation.

        This is intended to be called concurrently from multiple clients.
        """
        try:
            # Ensure there is at least one bundle.
            with self._lock:
                if output_split_idx in self._next_bundle:
                    next_bundle = self._next_bundle[output_split_idx]
                else:
                    next_bundle = None

            # Fetch next bundle if needed.
            if next_bundle is None:
                # This is a BLOCKING call, so do it outside the lock.
                next_bundle = self._output_iterator.get_next(output_split_idx)

            block = next_bundle.blocks.pop()[0]

            # Accumulate any remaining blocks in next_bundle map as needed.
            with self._lock:
                self._next_bundle[output_split_idx] = next_bundle
                if not next_bundle.blocks:
                    del self._next_bundle[output_split_idx]

            return block
        except StopIteration:
            return None
