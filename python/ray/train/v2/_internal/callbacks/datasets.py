import copy
from typing import Any, Callable, Dict, List, Union

import ray.train
from ray.data import Dataset
from ray.data.context import DataContext
from ray.train.v2._internal.execution.callback import WorkerGroupCallback
from ray.train.v2._internal.execution.worker_group.worker_group import (
    Worker,
    WorkerGroup,
)

# A type representing either a ray.data.Dataset or a function that returns a
# ray.data.Dataset and accepts no arguments.
GenDataset = Union[Dataset, Callable[[], Dataset]]


class DatasetsSetupCallback(WorkerGroupCallback):
    """The callback to setup Ray Datasets for the worker group."""

    def __init__(
        self,
        datasets: Dict[str, GenDataset],
        data_config: ray.train.DataConfig,
        scaling_config: ray.train.ScalingConfig,
    ):
        self._datasets = datasets
        self._data_config = data_config
        self._scaling_config = scaling_config

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

    def before_init_train_context(self, workers: List[Worker]) -> Dict[str, List[Any]]:
        # Configure dataset shards
        datasets = {k: v() if callable(v) else v for k, v in self._datasets.items()}
        node_ids = [worker.metadata.node_id for worker in workers]

        # Notify the DataConfig about the total resources reserved for training.
        total_train_resources = self.get_train_total_resources(self._scaling_config)
        self._data_config.set_train_total_resources(
            total_train_resources.get("CPU", 0), total_train_resources.get("GPU", 0)
        )

        dataset_shards = self._data_config.configure(
            datasets,
            world_size=len(workers),
            worker_handles=None,
            worker_node_ids=node_ids,
        )
        assert len(dataset_shards) == len(workers)

        return {"dataset_shards": dataset_shards}

    def after_worker_group_start(self, worker_group: WorkerGroup):
        # Propagate DataContext
        def _propagate_data_context(ctx: DataContext):
            DataContext._set_current(ctx)

        worker_group.execute(
            _propagate_data_context,
            self._data_context,
        )
