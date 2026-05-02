import copy
import logging
from typing import TYPE_CHECKING, Dict, List

import ray
import ray.train
from ray.actor import ActorHandle
from ray.exceptions import GetTimeoutError
from ray.train.v2._internal.data_integration.interfaces import (
    DatasetShardMetadata,
    DatasetShardProvider,
)
from ray.train.v2._internal.execution.callback import (
    ControllerCallback,
    ReplicaGroupCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.worker_group.execution_group import (
    ExecutionGroup,
    ReplicaGroup,
)
from ray.train.v2._internal.execution.worker_group.worker_group import (
    Worker,
    WorkerGroupContext,
)
from ray.types import ObjectRef

if TYPE_CHECKING:
    from ray.data import DataIterator, Dataset
    from ray.data.context import DataContext

logger = logging.getLogger(__name__)


class RayDatasetShardProvider:
    """A shard provider that Train workers use to access a DataIterator for a dataset."""

    def __init__(self, ds_iterators: Dict[str, "DataIterator"]):
        # Maps dataset_name to a DataIterator.
        self._dataset_iterators = ds_iterators

    def get_dataset_shard(self, dataset_info: DatasetShardMetadata) -> "DataIterator":
        if dataset_info.dataset_name not in self._dataset_iterators:
            raise KeyError(
                f"Dataset shard for '{dataset_info.dataset_name}' not found. "
                "Please ensure that the dataset is passed through the Trainer `datasets` "
                "argument."
            )

        return self._dataset_iterators[dataset_info.dataset_name]


class DatasetsCallback(WorkerGroupCallback, ReplicaGroupCallback, ControllerCallback):
    """A callback for managing Ray Datasets for the worker group."""

    def __init__(
        self,
        train_run_context: TrainRunContext,
        datasets: Dict[str, "Dataset"],
    ):
        self._datasets = datasets
        self._data_config = copy.deepcopy(train_run_context.dataset_config)
        self._scaling_config = train_run_context.scaling_config
        self._coordinator_actors: List[ActorHandle] = []
        self._shutdown_refs: List[ObjectRef] = []
        # Populated in before_init_train_context_on_worker_group. Indexed by
        # world rank. Shard providers are kept alive across replica group
        # replacement so that a replacement worker picks up the same
        # DataIterator (and its cursor) its predecessor was using.
        self._shard_providers: List[RayDatasetShardProvider] = []
        # Parallel to _shard_providers. Flipped to False when a replica group
        # is shut down (so we can confirm the shard is free before reassigning
        # it to a replacement worker), and back to True when the replacement
        # starts.
        self._shard_provider_active: List[bool] = []

        # Capture the current DataContext to propagate it to
        # the Train workers later.
        # The propagation works in the following way:
        # 1. This callback is created when user create the Trainer.
        # 2. Then this callback will be passed to the Controller actor.
        # 3. Lastly, when the worker group is initialized, the Controller
        #    will call the `after_worker_group_start` callback to propagate
        #    the DataContext to Train workers.
        from ray.data.context import DataContext

        self._data_context = copy.deepcopy(DataContext.get_current())

    def get_train_total_resources(
        self, scaling_config: ray.train.ScalingConfig
    ) -> Dict[str, float]:
        """Return the resources reserved for training, so that Data can exclude
        these resources logically from its available pool."""
        if scaling_config.elasticity_enabled:
            # If Train is running with a variable number of workers,
            # we can't provide a fixed number of resources to exclude.
            # Instead, Train and Data should coordinate via the autoscaling
            # coordinator to allocate resources dynamically.
            return {}
        return scaling_config.total_resources

    def _get_coordinator_actors(
        self, ds_iterators_per_rank: List[Dict[str, "DataIterator"]]
    ) -> List[ActorHandle]:
        """
        Returns a list of each unique SplitCoordinator actor handle given the iterators per rank.
        These handles will later be used to call shutdown on the actors.
        """
        from ray.data._internal.iterator.stream_split_iterator import (
            StreamSplitDataIterator,
        )

        # Note: Currently, we only need to check rank 0 for split iterators.
        # In the future, if datasets can be split across only a subset of ranks,
        # we may need to process all ranks.
        rank_0_iterators = ds_iterators_per_rank[0]
        coord_actors = [
            iterator._coord_actor
            for iterator in rank_0_iterators.values()
            if isinstance(iterator, StreamSplitDataIterator)
        ]
        return coord_actors

    def _shutdown_data_executors(self):
        """Eagerly shutdown the data executors of the split coordinator actors."""
        self._shutdown_refs = [
            coord.shutdown_executor.remote() for coord in self._coordinator_actors
        ]

    # --------------------------
    # WorkerGroupCallback
    # --------------------------

    def before_init_train_context_on_worker_group(
        self, workers: List[Worker]
    ) -> Dict[str, List[DatasetShardProvider]]:
        world_size = len(workers)
        worker_node_ids = [worker.metadata.node_id for worker in workers]

        # Notify the DataConfig about the total resources reserved for training.
        total_train_resources = self.get_train_total_resources(self._scaling_config)
        self._data_config.set_train_total_resources(
            total_train_resources.get("CPU", 0), total_train_resources.get("GPU", 0)
        )

        datasets = {k: v() if callable(v) else v for k, v in self._datasets.items()}
        ds_iterators_per_rank = self._data_config.configure(
            datasets=datasets,
            world_size=world_size,
            worker_handles=None,
            worker_node_ids=worker_node_ids,
        )
        assert len(ds_iterators_per_rank) == world_size

        self._coordinator_actors = self._get_coordinator_actors(ds_iterators_per_rank)

        shard_providers_per_rank = [
            RayDatasetShardProvider(ds_iterators=ds_iterators_per_rank[rank])
            for rank in range(world_size)
        ]
        self._shard_providers = shard_providers_per_rank
        self._shard_provider_active = [True] * world_size
        return {"dataset_shard_provider": shard_providers_per_rank}

    def after_worker_group_shutdown(
        self, worker_group_context: WorkerGroupContext
    ) -> None:
        self._shutdown_data_executors()

    def after_worker_group_abort(
        self, worker_group_context: WorkerGroupContext
    ) -> None:
        self._shutdown_data_executors()

    # --------------------------
    # ExecutionGroupCallback (fires on both worker group and replica group start)
    # --------------------------

    def after_execution_group_start(self, execution_group: ExecutionGroup):
        # Propagate DataContext to all workers in the (possibly partial)
        # execution group. This runs on full worker group startup and also on
        # replica group replacement so that replacement workers see the same
        # DataContext as their predecessors.
        from ray.data.context import DataContext

        def _propagate_data_context(ctx: "DataContext"):
            DataContext._set_current(ctx)

        execution_group.execute(
            _propagate_data_context,
            self._data_context,
        )

    # --------------------------
    # ReplicaGroupCallback
    # We should only call these methods if we are replacing worker(s) in a fixed
    # size training group. If the number of workers changes, we need to shift from
    # "handing a dataset shard to the replacement worker" to
    # "having multiple workers pull from a common queue."
    # --------------------------

    def before_replica_group_shutdown(self, replica_group: ReplicaGroup):
        # Mark the shard provider slots for this replica's ranks as inactive
        # so they can be handed to replacement workers, and abort each
        # streaming_split iterator's reservations on the SplitCoordinator
        # so any blocks the predecessor pulled-but-didn't-fully-ack are
        # sliced into the requeue for the replacement to consume.
        from ray.data._internal.iterator.stream_split_iterator import (
            StreamSplitDataIterator,
        )

        for w in replica_group.get_workers():
            assert w.distributed_context is not None, (
                "Worker in replica group is missing distributed_context in "
                "before_replica_group_shutdown."
            )
            rank = w.distributed_context.world_rank
            assert 0 <= rank < len(self._shard_provider_active), (
                f"Outgoing worker has world_rank={rank} outside of the "
                f"range [0, {len(self._shard_provider_active)})."
            )
            provider = self._shard_providers[rank]
            for ds_iter in provider._dataset_iterators.values():
                if isinstance(ds_iter, StreamSplitDataIterator):
                    # Synchronous so the requeue is populated before the
                    # replacement comes online and calls coord.get. The
                    # coord ignores abort() when 2PC tracking is off, so
                    # this is safe to call unconditionally.
                    ray.get(
                        ds_iter._coord_actor.abort.remote(
                            ds_iter._output_split_idx,
                        )
                    )
            self._shard_provider_active[rank] = False

    def before_init_train_context_on_replica_group(
        self, workers: List[Worker]
    ) -> Dict[str, List[DatasetShardProvider]]:
        # Reuse the existing per-rank shard providers for the replacement
        # workers. The underlying DataIterator (and its coordinator actor) is
        # preserved across the replacement, so the replacement picks up where
        # the failed worker left off.
        from ray.data._internal.iterator.stream_split_iterator import (
            StreamSplitDataIterator,
        )

        providers = []
        for w in workers:
            rank = w.distributed_context.world_rank
            assert 0 <= rank < len(self._shard_providers), (
                f"Replacement worker has world_rank={rank} outside of the "
                f"range [0, {len(self._shard_providers)})."
            )
            assert not self._shard_provider_active[rank], (
                f"Shard provider for rank {rank} is still marked active; "
                "before_replica_group_shutdown was not called for the "
                "outgoing replica."
            )
            self._shard_provider_active[rank] = True
            provider = self._shard_providers[rank]
            # Mark each StreamSplitDataIterator so the replacement worker
            # rejoins the in-progress epoch instead of arriving at the
            # start_epoch barrier the predecessor already passed (which would
            # deadlock the SplitCoordinator). Flag is one-shot: the iterator
            # auto-clears it after the first iter_batches() call.
            for ds_iter in provider._dataset_iterators.values():
                if isinstance(ds_iter, StreamSplitDataIterator):
                    ds_iter.set_rejoined_epoch(True)
            providers.append(provider)
        return {"dataset_shard_provider": providers}

    # --------------------------
    # ControllerCallback
    # --------------------------

    async def before_controller_shutdown(self):
        try:
            ray.get(self._shutdown_refs, timeout=5)
        except GetTimeoutError:
            logger.error("Ray Data executor shutdown task timed out after 5 seconds.")
        except Exception:
            logger.exception("Failed to gracefully terminate Ray Data executors.")
