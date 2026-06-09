"""Launcher-agnostic worker context.

Framework adapters write their training loop against this interface once and
run under both launchers: ``ray`` (Ray Train TorchTrainer) and ``torchrun``
(the parity baseline from the benchmark modernization proposal).
"""

import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Where rank-0 workers drop their final metrics for the driver to pick up.
# Anyscale clusters have /mnt/cluster_storage; fall back to /tmp for local runs.
def default_metrics_path(experiment_name: str) -> str:
    root = (
        "/mnt/cluster_storage"
        if os.path.isdir("/mnt/cluster_storage")
        else "/tmp/train_benchmark"
    )
    os.makedirs(root, exist_ok=True)
    return os.path.join(root, f"{experiment_name}_metrics.json")


class TrainContext(ABC):
    @property
    @abstractmethod
    def world_rank(self) -> int:
        ...

    @property
    @abstractmethod
    def world_size(self) -> int:
        ...

    @property
    @abstractmethod
    def local_rank(self) -> int:
        ...

    @abstractmethod
    def device(self):
        """Torch device for this worker."""

    @abstractmethod
    def report(self, metrics: Dict[str, Any], checkpoint_dir: Optional[str] = None):
        """Report intermediate metrics and optionally persist a checkpoint dir."""

    @abstractmethod
    def get_checkpoint_dir(self) -> Optional[str]:
        """Local directory of the latest checkpoint to restore from, if any."""


class RayTrainContext(TrainContext):
    def __init__(self):
        import ray.train

        self._train = ray.train
        self._ctx = ray.train.get_context()
        # Keep a checkpoint context manager alive for the duration of the run.
        self._checkpoint_cm = None

    @property
    def world_rank(self) -> int:
        return self._ctx.get_world_rank()

    @property
    def world_size(self) -> int:
        return self._ctx.get_world_size()

    @property
    def local_rank(self) -> int:
        return self._ctx.get_local_rank()

    def device(self):
        import ray.train.torch

        return ray.train.torch.get_device()

    def report(self, metrics: Dict[str, Any], checkpoint_dir: Optional[str] = None):
        checkpoint = (
            self._train.Checkpoint.from_directory(checkpoint_dir)
            if checkpoint_dir
            else None
        )
        self._train.report(metrics, checkpoint=checkpoint)

    def get_checkpoint_dir(self) -> Optional[str]:
        checkpoint = self._train.get_checkpoint()
        if checkpoint is None:
            return None
        self._checkpoint_cm = checkpoint.as_directory()
        return self._checkpoint_cm.__enter__()


class TorchrunContext(TrainContext):
    """Parity-baseline context reading the standard torchrun env vars."""

    def __init__(self, experiment_name: str):
        import torch

        self._experiment_name = experiment_name
        self._world_rank = int(os.environ.get("RANK", 0))
        self._world_size = int(os.environ.get("WORLD_SIZE", 1))
        self._local_rank = int(os.environ.get("LOCAL_RANK", 0))

        if torch.cuda.is_available():
            torch.cuda.set_device(self._local_rank)
            self._device = torch.device(f"cuda:{self._local_rank}")
        else:
            self._device = torch.device("cpu")

    @property
    def world_rank(self) -> int:
        return self._world_rank

    @property
    def world_size(self) -> int:
        return self._world_size

    @property
    def local_rank(self) -> int:
        return self._local_rank

    def device(self):
        return self._device

    def report(self, metrics: Dict[str, Any], checkpoint_dir: Optional[str] = None):
        # Torchrun baseline only measures perf parity; checkpoints stay local
        # and metrics go to the shared results file for the driver to read.
        if self.world_rank == 0:
            logger.info(f"[torchrun] report: {metrics}")
            with open(default_metrics_path(self._experiment_name), "w") as f:
                json.dump(metrics, f)

    def get_checkpoint_dir(self) -> Optional[str]:
        return None


def get_train_context(launcher: str, experiment_name: str) -> TrainContext:
    if launcher == "ray":
        return RayTrainContext()
    elif launcher == "torchrun":
        return TorchrunContext(experiment_name)
    raise ValueError(f"Unknown launcher: {launcher}")
