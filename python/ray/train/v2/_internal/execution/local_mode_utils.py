import logging
from typing import Any, Callable, Dict, Optional

from ray.data import DataIterator
from ray.train import Checkpoint, Result
from ray.train.trainer import GenDataset
from ray.train.v2._internal.execution.train_fn_utils import (
    TrainFnUtils,
    get_train_fn_utils,
    set_train_fn_utils,
)
from ray.train.v2._internal.util import date_str
from ray.train.v2.api.context import (
    TrainContext as ExternalTrainContext,
    TrainContextWithoutRayTrainController,
)

logger = logging.getLogger(__name__)


class LocalModeTrainFnUtils(TrainFnUtils):
    """TrainFnUtils for jobs launched without ray train controller.
    This is more for testing purposes, and some functionality is missing.
    """

    def __init__(
        self,
        experiment_name: str,
        local_world_size: int,
        local_rank: int,
        dataset_shards: Optional[Dict[str, DataIterator]] = None,
    ):
        self._context = TrainContextWithoutRayTrainController(
            experiment_name=experiment_name,
            local_world_size=local_world_size,
            local_rank=local_rank,
        )
        self._dataset_shards = dataset_shards
        self._last_metrics = None

    def report(
        self,
        metrics: Dict[str, Any],
        checkpoint: Optional[Checkpoint] = None,
        checkpoint_dir_name: Optional[str] = None,
    ) -> None:
        self._last_metrics = metrics

    def get_checkpoint(self) -> Optional[Checkpoint]:
        return None

    def get_dataset_shard(self, dataset_name: str) -> DataIterator:
        assert (
            self._dataset_shards is not None and dataset_name in self._dataset_shards
        ), f"Dataset shard {dataset_name} not found."
        return self._dataset_shards[dataset_name]

    def get_context(self) -> ExternalTrainContext:
        return self._context

    def is_running_with_ray_train_controller(self) -> bool:
        return False

    def _get_last_metrics(self) -> Optional[Dict[str, Any]]:
        """return the last metrics reported by the training function.
        This function should only be called by TorchBackendWithoutRayTrainController
        """
        return self._last_metrics


class BackendForLocalMode:
    def __init__(self, datasets: Optional[Dict[str, GenDataset]] = None):
        if datasets is not None:
            datasets = {k: v() if callable(v) else v for k, v in datasets.items()}

        self.local_world_size = 1
        self.local_rank = 0

        set_train_fn_utils(
            LocalModeTrainFnUtils(
                experiment_name=self._get_experiment_name(),
                local_world_size=self.local_world_size,
                local_rank=self.local_rank,
                dataset_shards=datasets,
            )
        )

    def _get_experiment_name(self) -> str:
        return f"local_training-{date_str()}"

    def fit(self, train_func: Callable[[], None]) -> Result:
        train_func()
        train_fn_utils = get_train_fn_utils()
        assert isinstance(train_fn_utils, LocalModeTrainFnUtils)
        return Result(
            metrics=train_fn_utils._get_last_metrics(),
            checkpoint=None,
            path=None,
            error=None,
        )
