import copy
import logging
import threading
import time
from dataclasses import replace
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Tuple, Union

import ray
from ray.data._internal.execution.interfaces import NodeIdStr, RefBundle
from ray.data._internal.execution.legacy_compat import execute_to_legacy_bundle_iterator
from ray.data._internal.execution.operators.output_splitter import OutputSplitter
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data._internal.stats import DatasetStats
from ray.data._internal.util import create_dataset_tag
from ray.data.block import Block, BlockMetadata
from ray.data.iterator import DataIterator
from ray.types import ObjectRef
from ray.util.debug import log_once
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

if TYPE_CHECKING:
    import pyarrow

    from ray.data import Dataset

logger = logging.getLogger(__name__)


BLOCKED_CLIENT_WARN_TIMEOUT = 30


class StreamSplitDataIterator(DataIterator):
    """Implements a collection of iterators over a shared data stream."""

    @staticmethod
    def create(
        base_dataset: "Dataset",
        n: int,
        equal: bool,
        locality_hints: Optional[List[NodeIdStr]],
    ) -> List["StreamSplitDataIterator"]:
        """Create a split iterator from the given base Dataset and options.

        See also: `Dataset.streaming_split`.
        """
        # To avoid deadlock, the concurrency on this actor must be set to at least `n`.
        coord_actor = SplitCoordinator.options(
            max_concurrency=n,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                ray.get_runtime_context().get_node_id(), soft=False
            ),
        ).remote(base_dataset, n, equal, locality_hints)

        return [
            StreamSplitDataIterator(base_dataset, coord_actor, i, n) for i in range(n)
        ]

    def __init__(
        self,
        base_dataset: "Dataset",
        coord_actor: ray.actor.ActorHandle,
        output_split_idx: int,
        world_size: int,
    ):
        self._base_dataset = base_dataset
        self._coord_actor = coord_actor
        self._output_split_idx = output_split_idx
        self._world_size = world_size
        self._iter_stats = DatasetStats(metadata={}, parent=None)

    def _to_ref_bundle_iterator(
        self,
    ) -> Tuple[Iterator[RefBundle], Optional[DatasetStats], bool]:
        def gen_blocks() -> Iterator[RefBundle]:
            cur_epoch = ray.get(
                self._coord_actor.start_epoch.remote(self._output_split_idx)
            )
            future: ObjectRef[
                Optional[ObjectRef[Block]]
            ] = self._coord_actor.get.remote(cur_epoch, self._output_split_idx)
            while True:
                block_ref_and_md: Optional[
                    Tuple[ObjectRef[Block], BlockMetadata]
                ] = ray.get(future)
                if not block_ref_and_md:
                    break
                else:
                    future = self._coord_actor.get.remote(
                        cur_epoch, self._output_split_idx
                    )
                    yield RefBundle(blocks=(block_ref_and_md,), owns_blocks=False)

        return gen_blocks(), self._iter_stats, False

    def stats(self) -> str:
        """Implements DataIterator."""
        # Merge the locally recorded iter stats and the remotely recorded
        # stream execution stats.
        stats = ray.get(self._coord_actor.stats.remote())
        summary = stats.to_summary()
        summary.iter_stats = self._iter_stats.to_summary().iter_stats
        summary.iter_stats.streaming_split_coord_time.add(
            stats.streaming_split_coordinator_s.get()
        )
        return summary.to_string()

    def schema(self) -> Union[type, "pyarrow.lib.Schema"]:
        """Implements DataIterator."""
        return self._base_dataset.schema()

    def world_size(self) -> int:
        """Returns the number of splits total."""
        return self._world_size

    def _get_dataset_tag(self):
        return create_dataset_tag(
            self._base_dataset._plan._dataset_name,
            self._base_dataset._uuid,
            self._output_split_idx,
        )


@ray.remote(num_cpus=0)
class SplitCoordinator:
    """Coordinator actor for routing blocks to output splits.

    This actor runs a streaming executor locally on its main thread. Clients can
    retrieve results via actor calls running on other threads.
    """

    def __init__(
        self,
        dataset: "Dataset",
        n: int,
        equal: bool,
        locality_hints: Optional[List[NodeIdStr]],
    ):
        # Automatically set locality with output to the specified location hints.
        if locality_hints:
            dataset.context.execution_options.locality_with_output = locality_hints
            logger.info(f"Auto configuring locality_with_output={locality_hints}")

        # Set current DataContext.
        ray.data.DataContext._set_current(dataset.context)

        self._base_dataset = dataset
        self._n = n
        self._equal = equal
        self._locality_hints = locality_hints
        self._lock = threading.RLock()
        self._executor = None

        # Guarded by self._lock.
        self._next_bundle: Dict[int, RefBundle] = {}
        self._unfinished_clients_in_epoch = n
        self._cur_epoch = -1

        def gen_epochs():
            while True:
                executor = StreamingExecutor(
                    copy.deepcopy(dataset.context.execution_options),
                    create_dataset_tag(
                        self._base_dataset._name, self._base_dataset._uuid
                    ),
                )
                self._executor = executor

                def add_split_op(dag):
                    return OutputSplitter(dag, n, equal, locality_hints)

                output_iterator = execute_to_legacy_bundle_iterator(
                    executor,
                    dataset._plan,
                    dag_rewrite=add_split_op,
                )
                yield output_iterator

        self._next_epoch = gen_epochs()
        self._output_iterator = None
        # Used for debugging https://github.com/ray-project/ray/issues/45225
        self._debug_info = {}

    def stats(self) -> DatasetStats:
        """Returns stats from the base dataset."""
        if self._executor:
            return self._executor.get_stats()
        return self._base_dataset._plan.stats()

    def start_epoch(self, split_idx: int) -> str:
        """Called to start an epoch.

        Returns:
            UUID for the epoch, which must be used when accessing results via get().
        """

        # Wait for all clients to arrive at the barrier before starting a new epoch.
        epoch_id = self._barrier(split_idx)
        return epoch_id

    def get(
        self, epoch_id: int, output_split_idx: int
    ) -> Optional[Tuple[ObjectRef[Block], BlockMetadata]]:
        """Blocking get operation.

        This is intended to be called concurrently from multiple clients.
        """
        start_time = time.perf_counter()
        if epoch_id != self._cur_epoch:
            raise ValueError(
                "Invalid iterator: the dataset has moved on to another epoch."
            )

        try:
            # Ensure there is at least one bundle.
            with self._lock:
                if output_split_idx in self._next_bundle:
                    next_bundle = self._next_bundle[output_split_idx]
                else:
                    next_bundle = None

            # Fetch next bundle if needed.
            while next_bundle is None or not next_bundle.blocks:
                # This is a BLOCKING call, so do it outside the lock.
                next_bundle = self._output_iterator.get_next(output_split_idx)

            block = next_bundle.blocks[-1]
            next_bundle = replace(next_bundle, blocks=next_bundle.blocks[:-1])

            # Accumulate any remaining blocks in next_bundle map as needed.
            with self._lock:
                self._next_bundle[output_split_idx] = next_bundle
                if not next_bundle.blocks:
                    del self._next_bundle[output_split_idx]

            return block
        except StopIteration:
            return None
        finally:
            stats = self.stats()
            if stats and stats.streaming_split_coordinator_s:
                stats.streaming_split_coordinator_s.add(
                    time.perf_counter() - start_time
                )

    def _barrier(self, split_idx: int) -> int:
        """Arrive and block until the start of the given epoch."""

        self._debug_info[split_idx] = {}
        # Decrement and await all clients to arrive here.
        with self._lock:
            starting_epoch = self._cur_epoch
            self._debug_info[split_idx]["starting_epoch"] = starting_epoch
            self._unfinished_clients_in_epoch -= 1

        start_time = time.time()
        while (
            self._cur_epoch == starting_epoch and self._unfinished_clients_in_epoch != 0
        ):
            if time.time() - start_time > BLOCKED_CLIENT_WARN_TIMEOUT:
                if log_once(f"stream_split_blocked_{split_idx}_{starting_epoch}"):
                    logger.warning(
                        f"StreamSplitDataIterator(epoch={starting_epoch}, "
                        f"split={split_idx}) blocked waiting on other clients "
                        f"for more than {BLOCKED_CLIENT_WARN_TIMEOUT}s. All "
                        "clients must read from the DataIterator splits at "
                        "the same time. This warning will not be printed again "
                        "for this epoch."
                    )
            time.sleep(0.1)

        # Advance to the next epoch.
        self._debug_info[split_idx]["entering_lock"] = (
            self._cur_epoch,
            self._output_iterator is None,
            time.time(),
        )
        with self._lock:
            self._debug_info[split_idx]["entered_lock"] = (
                self._cur_epoch,
                self._output_iterator is None,
                time.time(),
            )
            if self._cur_epoch == starting_epoch:
                self._cur_epoch += 1
                self._unfinished_clients_in_epoch = self._n
                self._output_iterator = next(self._next_epoch)
                self._debug_info[split_idx]["set_iter"] = (
                    self._cur_epoch,
                    self._output_iterator is None,
                    time.time(),
                )
            self._debug_info[split_idx]["leaving_lock"] = (
                self._cur_epoch,
                self._output_iterator is None,
                time.time(),
            )

        assert self._output_iterator is not None, self._debug_info
        return starting_epoch + 1
