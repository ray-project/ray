import logging
import os
import sys
from typing import Any, Callable, Dict, List, Optional

import torch
import torch.distributed as dist

from ray.data import DataIterator
from ray.train import Checkpoint, GenDataset, Result
from ray.train.v2._internal.execution.train_fn_utils import (
    TrainFnUtils,
    set_train_fn_utils,
)
from ray.train.v2.api.context import (
    TrainContext as ExternalTrainContext,
    TrainContextWithoutRayTrain,
)

logger = logging.getLogger(__name__)


class TorchWithoutRayTrainTrainFnUtils(TrainFnUtils):
    def __init__(
        self,
        experiment_name: str,
        local_world_size: int,
        local_rank: int,
        dataset_shards: Optional[Dict[str, DataIterator]] = None,
    ):
        self._context = TrainContextWithoutRayTrain(
            experiment_name=experiment_name,
            local_world_size=local_world_size,
            local_rank=local_rank,
        )
        self._dataset_shards = dataset_shards

    def report(
        self,
        metrics: Dict[str, Any],
        checkpoint: Optional[Checkpoint] = None,
        checkpoint_dir_name: Optional[str] = None,
    ) -> None:
        logger.info(f"Report metrics: {metrics}")

    def get_checkpoint(self) -> Optional[Checkpoint]:
        return None

    def get_dataset_shard(self, dataset_name: str) -> DataIterator:
        if self._dataset_shards is None:
            raise ValueError("Dataset shards are not provided.")
        return self._dataset_shards[dataset_name]

    def get_context(self) -> ExternalTrainContext:
        return self._context

    def is_running_with_ray_train(self) -> bool:
        return False

    def get_devices(self) -> List[torch.device]:
        return local_running_get_devices()


def launched_by_torchrun() -> bool:
    """Return True if this process looks like it came from `torchrun`."""
    env_markers = {
        "LOCAL_RANK",
        "LOCAL_WORLD_SIZE",
        "WORLD_SIZE",
        "TORCHELASTIC_RUN_ID",
    }  # torchrun ≥1.10
    argv_markers = (
        "--local-rank",
        "--local_rank",
    )  # torchrun always passes one of these

    # Any of the env vars *or* the CLI flag counts as evidence
    return bool(
        (env_markers & os.environ.keys())
        or any(a.startswith(argv_markers) for a in sys.argv)
    )


def local_running_get_devices() -> List[torch.device]:
    """Return a list of devices to use for training."""
    if torch.cuda.is_available():
        return [torch.device(f"cuda:{torch.cuda.current_device()}")]
    else:
        return [torch.device("cpu")]


class TorchBackendWithoutRayTrain:
    def __init__(self, datasets: Optional[Dict[str, GenDataset]] = None):
        if datasets is not None:
            raise NotImplementedError(
                "Ray Data Datasets are not supported for no-ray-train mode yet."
            )

        self.launched_by_torchrun = launched_by_torchrun()
        if self.launched_by_torchrun:
            dist.init_process_group(
                backend="nccl" if torch.cuda.is_available() else "gloo"
            )
            self.local_world_size = dist.get_world_size()
            self.local_rank = dist.get_rank()
            torch.cuda.set_device(self.local_rank)
        else:
            self.local_world_size = 1
            self.local_rank = 0

        set_train_fn_utils(
            TorchWithoutRayTrainTrainFnUtils(
                experiment_name="train-without-ray-train",
                local_world_size=self.local_world_size,
                local_rank=self.local_rank,
                dataset_shards=self.datasets,
            )
        )

    def fit(self, train_func: Callable[[], None]) -> Result:
        train_func()
        if self.launched_by_torchrun:
            dist.destroy_process_group()
        return Result(metrics={}, checkpoint=None, path=None, error=None)
