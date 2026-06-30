"""Launcher-agnostic worker context.

Framework adapters write their training loop against this interface once and
run under both launchers: ``ray`` (Ray Train TorchTrainer) and ``torchrun``
(the parity baseline from the benchmark modernization proposal).
"""

import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


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
        # to_directory() returns a concrete path (downloaded once); unlike
        # as_directory()'s context manager, there's no __enter__ left dangling
        # without an __exit__. The dir persists for the run — fine for a
        # one-time restore at startup.
        return checkpoint.to_directory()


class TorchrunContext(TrainContext):
    """Minimal context for the torchrun parity baseline.

    Reads the standard torchrun env vars and picks the device. Checkpoint and
    validation are intentionally no-ops: the parity run only needs throughput
    numbers — the real benchmark (with checkpoint/validation, fault tolerance,
    etc.) runs on Ray Train. Final metrics return via the actor's run() value
    (see torchrun_ray_launcher), so there's nothing to persist here.
    """

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
        # Parity baseline: log metrics on rank 0; checkpoints are not persisted.
        if self.world_rank == 0:
            logger.info(f"[torchrun] report: {metrics}")

    def get_checkpoint_dir(self) -> Optional[str]:
        return None
