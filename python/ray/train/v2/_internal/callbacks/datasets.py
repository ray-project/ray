import copy
from typing import Dict, List

import ray.train
from ray.data import DataIterator
from ray.data.context import DataContext
from ray.train.v2._internal.data_integration.interfaces import (
    DatasetShardMetadata,
    DatasetShardProvider,
)
from ray.train.v2._internal.execution.callback import WorkerGroupCallback
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.worker_group.worker_group import (
    Worker,
    WorkerGroup,
)


class RayDatasetShardProvider:
    """A shard provider that Train workers use to access a DataIterator for a dataset."""

    def __init__(self, ds_iterators: Dict[str, DataIterator]):
        # Maps dataset_name to a DataIterator.
        self._dataset_iterators = ds_iterators

    def get_dataset_shard(self, dataset_info: DatasetShardMetadata) -> DataIterator:
        if dataset_info.dataset_name not in self._dataset_iterators:
            raise KeyError(
                f"Dataset shard for '{dataset_info.dataset_name}' not found. "
                "Please ensure that the dataset is passed through the Trainer `datasets` "
                "argument."
            )

        return self._dataset_iterators[dataset_info.dataset_name]


class DatasetsSetupCallback(WorkerGroupCallback):
    """The callback to setup Ray Datasets for the worker group."""

    def __init__(self, train_run_context: TrainRunContext):
        self._datasets = train_run_context.datasets
        self._data_config = copy.deepcopy(train_run_context.dataset_config)
        self._scaling_config = train_run_context.scaling_config

        # Capture the current DataContext to propagate it to
        # the Train workers later.
        # The propagation works in the following way:
        # 1. This callback is created when user create the Trainer.
        # 2. Then this callback will be passed to the Controller actor.
        # 3. Lastly, when the worker group is initialized, the Controller
        #    will call the `after_worker_group_start` callback to propagate
        #    the DataContext to Train workers.
        self._data_context = copy.deepcopy(DataContext.get_current())

    def get_train_total_resources(
        self, scaling_config: ray.train.ScalingConfig
    ) -> Dict[str, float]:
        """Return the resources reserved for training, so that Data can exclude
        these resources logically from its available pool."""
        return scaling_config.total_resources

    # --------------------------
    # WorkerGroupCallback
    # --------------------------

    def before_init_train_context(
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

        shard_providers_per_rank = [
            RayDatasetShardProvider(ds_iterators=ds_iterators_per_rank[rank])
            for rank in range(world_size)
        ]
        return {"dataset_shard_provider": shard_providers_per_rank}

    def after_worker_group_start(self, worker_group: WorkerGroup):
        # Propagate DataContext
        def _propagate_data_context(ctx: DataContext):
            DataContext._set_current(ctx)

        worker_group.execute(
            _propagate_data_context,
            self._data_context,
        )
