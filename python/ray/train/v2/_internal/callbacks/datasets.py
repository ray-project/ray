import copy
import logging
from typing import TYPE_CHECKING, Dict, List, Optional

import ray
import ray.train
from ray.train.v2._internal.data_integration.interfaces import (
    DatasetShardMetadata,
    DatasetShardProvider,
    GenDataset,
)
from ray.train.v2._internal.execution.callback import WorkerGroupCallback
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.worker_group.worker_group import (
    Worker,
    WorkerGroup,
    WorkerGroupContext,
)
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

if TYPE_CHECKING:
    from ray.data import DataIterator, Dataset, NodeIdStr
    from ray.data.context import DataContext

logger = logging.getLogger(__name__)


class RayDatasetShardProvider:
    def __init__(
        self,
        datasets: Dict[str, GenDataset],
        data_config: ray.train.DataConfig,
        data_context: "DataContext",
        world_size: int,
        worker_node_ids: List["NodeIdStr"],
    ):
        from ray.train.v2._internal.data_integration.dataset_manager import (
            DatasetManager,
        )

        self._dataset_names = set(datasets)
        self._dataset_manager = (
            ray.remote(DatasetManager)
            .options(
                num_cpus=0,
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    ray.get_runtime_context().get_node_id(), soft=False
                ),
            )
            .remote(
                datasets=datasets,
                data_config=data_config,
                data_context=data_context,
                world_size=world_size,
                worker_node_ids=worker_node_ids,
            )
        )
        self._cached_dataset_shards: Dict[str, "DataIterator"] = {}

    def get_dataset_shard(self, dataset_info: DatasetShardMetadata) -> "DataIterator":
        dataset_name = dataset_info.dataset_name
        if dataset_name not in self._dataset_names:
            raise KeyError(
                f"Dataset shard for '{dataset_name}' not found. "
                "Please ensure that the dataset is passed through the Trainer `datasets` "
                "argument."
            )

        if dataset_name not in self._cached_dataset_shards:
            self._cached_dataset_shards[dataset_name] = ray.get(
                self._dataset_manager.get_dataset_shard.remote(dataset_info)
            )

        return self._cached_dataset_shards[dataset_name]

    def shutdown_data_executors(self) -> None:
        """
        Attempts to eagerly shutdown the data executors for datasets, freeing resources allocated to data execution.
        """
        try:
            self._dataset_manager.shutdown_data_executors.remote()
        except Exception:
            logger.debug("Failed to invoke remote cleanup of Dataset Manager.")


class DatasetsCallback(WorkerGroupCallback):
    """A callback for managing Ray Datasets for the worker group."""

    def __init__(
        self,
        train_run_context: TrainRunContext,
        datasets: Dict[str, "Dataset"],
    ):
        self._datasets = datasets
        self._data_config = copy.deepcopy(train_run_context.dataset_config)
        self._scaling_config = train_run_context.scaling_config
        self._dataset_shard_provider: Optional[RayDatasetShardProvider] = None

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

    # --------------------------
    # WorkerGroupCallback
    # --------------------------

    def before_init_train_context(
        self, workers: List[Worker]
    ) -> Dict[str, List[DatasetShardProvider]]:
        world_size = len(workers)
        worker_node_ids = [worker.metadata.node_id for worker in workers]
        datasets = {k: v() if callable(v) else v for k, v in self._datasets.items()}

        # TODO: Move this to the constructor.
        # Notify the DataConfig about the total resources reserved for training.
        total_train_resources = self.get_train_total_resources(self._scaling_config)
        self._data_config.set_train_total_resources(
            total_train_resources.get("CPU", 0), total_train_resources.get("GPU", 0)
        )

        self._dataset_shard_provider = RayDatasetShardProvider(
            datasets=datasets,
            data_config=self._data_config,
            data_context=self._data_context,
            world_size=world_size,
            worker_node_ids=worker_node_ids,
        )
        return {"dataset_shard_provider": [self._dataset_shard_provider] * world_size}

    def after_worker_group_start(self, worker_group: WorkerGroup):
        # Propagate DataContext
        from ray.data.context import DataContext

        def _propagate_data_context(ctx: "DataContext"):
            DataContext._set_current(ctx)

        worker_group.execute(
            _propagate_data_context,
            self._data_context,
        )

    def after_worker_group_shutdown(
        self, worker_group_context: WorkerGroupContext
    ) -> None:
        shard_provider = self._dataset_shard_provider
        if shard_provider:
            shard_provider.shutdown_data_executors()

    def after_worker_group_abort(
        self, worker_group_context: WorkerGroupContext
    ) -> None:
        shard_provider = self._dataset_shard_provider
        if shard_provider:
            shard_provider.shutdown_data_executors()
