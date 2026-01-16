import logging
import threading
import time
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Tuple, Union

import ray
from ray.data._internal.execution.interfaces import NodeIdStr, RefBundle
from ray.data._internal.execution.legacy_compat import execute_to_legacy_bundle_iterator
from ray.data._internal.stats import DatasetStats
from ray.data.block import Block
from ray.data.context import DataContext
from ray.data.iterator import DataIterator
from ray.types import ObjectRef
from ray.util.debug import log_once
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

if TYPE_CHECKING:
    import pyarrow

    from ray.data.dataset import Dataset

logger = logging.getLogger(__name__)


BLOCKED_CLIENT_WARN_TIMEOUT = 30


class _DatasetWrapper:
    # A temporary workaround for https://github.com/ray-project/ray/issues/52549

    def __init__(self, dataset: "Dataset") -> None:
        self._dataset = dataset


class StreamSplitDataIterator(DataIterator):
    """Implements a collection of iterators over a shared data stream."""

    @staticmethod
    def create(
        base_dataset: "Dataset",
        n: int,
        locality_hints: Optional[List[NodeIdStr]],
    ) -> List["StreamSplitDataIterator"]:
        """Create a split iterator from the given base Dataset and options.

        See also: `Dataset.streaming_split`.
        """
        # To avoid deadlock, the concurrency on this actor must be set to at least `n`.
        # We add 1 to the concurrency to allow for a shutdown_executor thread to run.
        coord_actor = SplitCoordinator.options(
            max_concurrency=n + 1,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                ray.get_runtime_context().get_node_id(), soft=False
            ),
        ).remote(_DatasetWrapper(base_dataset), n, locality_hints)

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
                block_ref_and_md: Optional[RefBundle] = ray.get(future)
                if not block_ref_and_md:
                    break
                else:
                    future = self._coord_actor.get.remote(
                        cur_epoch, self._output_split_idx
                    )
                    yield RefBundle(
                        blocks=block_ref_and_md.blocks,
                        owns_blocks=False,
                        schema=block_ref_and_md.schema,
                    )

        self._base_dataset._plan._run_index += 1
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

    def get_context(self) -> DataContext:
        return self._base_dataset.context

    def world_size(self) -> int:
        """Returns the number of splits total."""
        return self._world_size

    def _get_dataset_tag(self):
        return f"{self._base_dataset.get_dataset_id()}_split_{self._output_split_idx}"


@ray.remote(num_cpus=0)
class SplitCoordinator:
    """Coordinator actor for routing blocks to output splits.

    This actor runs a streaming executor locally on its main thread. Clients can
    retrieve results via actor calls running on other threads.
    """

    def __init__(
        self,
        dataset_wrapper: _DatasetWrapper,
        n: int,
        locality_hints: Optional[List[NodeIdStr]],
    ):
        dataset = dataset_wrapper._dataset

        # Set current DataContext.
        # This needs to be a deep copy so that updates to the base dataset's
        # context does not affect this process's global DataContext.
        self._data_context = dataset.context.copy()
        ray.data.DataContext._set_current(self._data_context)

        if self._data_context.execution_options.locality_with_output is True:
            self._data_context.execution_options.locality_with_output = locality_hints
            logger.info(f"Auto configuring locality_with_output={locality_hints}")
        self._base_dataset = dataset
        self._n = n
        self._locality_hints = locality_hints
        self._lock = threading.RLock()
        self._executor = None

        # Guarded by self._lock.
        self._next_bundle: Dict[int, RefBundle] = {}
        self._unfinished_clients_in_epoch = n
        self._cur_epoch = -1

        # Add a new stats field to track coordinator overhead
        self._coordinator_overhead_s = 0.0

        def gen_epochs():
            while True:
                self._executor = self._base_dataset._plan.create_executor()
                output_iterator = execute_to_legacy_bundle_iterator(
                    self._executor, dataset._plan
                )
                yield output_iterator

        self._next_epoch = gen_epochs()
        self._output_iterator = None
        # Store the error raised from the `gen_epoch` call.
        self._gen_epoch_error: Optional[Exception] = None

    def stats(self) -> DatasetStats:
        """Returns stats from the base dataset."""
        if self._executor:
            stats = self._executor.get_stats()
        else:
            stats = self._base_dataset._plan.stats()

        # Set the tracked overhead time
        stats.streaming_split_coordinator_s.add(self._coordinator_overhead_s)

        return stats

    def start_epoch(self, split_idx: int) -> str:
        """Called to start an epoch.

        Returns:
            UUID for the epoch, which must be used when accessing results via get().
        """

        # Wait for all clients to arrive at the barrier before starting a new epoch.
        epoch_id = self._barrier(split_idx)
        return epoch_id

    def get(self, epoch_id: int, output_split_idx: int) -> Optional[RefBundle]:
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

            schema = next_bundle.schema
            block = next_bundle.blocks[-1]
            next_bundle = RefBundle(
                blocks=next_bundle.blocks[:-1],
                schema=next_bundle.schema,
                owns_blocks=next_bundle.owns_blocks,
                output_split_idx=next_bundle.output_split_idx,
            )

            # Accumulate any remaining blocks in next_bundle map as needed.
            with self._lock:
                self._next_bundle[output_split_idx] = next_bundle
                if not next_bundle.blocks:
                    del self._next_bundle[output_split_idx]

            return RefBundle(
                [block], schema=schema, owns_blocks=next_bundle.owns_blocks
            )
        except StopIteration:
            return None
        finally:
            # Track overhead time in the instance variable
            self._coordinator_overhead_s += time.perf_counter() - start_time

    def shutdown_executor(self):
        """Shuts down the internal data executor."""
        with self._lock:
            # Call shutdown on the executor
            if self._executor is not None:
                self._executor.shutdown(force=False)

    def _barrier(self, split_idx: int) -> int:
        """Arrive and block until the start of the given epoch."""

        # Decrement and await all clients to arrive here.
        with self._lock:
            starting_epoch = self._cur_epoch
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
        with self._lock:
            if self._cur_epoch == starting_epoch:
                self._cur_epoch += 1
                self._unfinished_clients_in_epoch = self._n
                try:
                    self._output_iterator = next(self._next_epoch)
                except Exception as e:
                    self._gen_epoch_error = e

        if self._gen_epoch_error is not None:
            # If there was an error when advancing to the next epoch,
            # re-raise it for all threads.
            raise self._gen_epoch_error

        assert self._output_iterator is not None
        return starting_epoch + 1
