"""Launcher-agnostic worker context.

Framework adapters write their training loop against this interface once and
run under both launchers: ``ray`` (Ray Train TorchTrainer) and ``torchrun``
(the parity baseline from the benchmark modernization proposal).
"""

import logging
import os
import shutil
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def _shared_root() -> str:
    """Shared cluster storage root (visible to all nodes), or /tmp locally."""
    root = (
        "/mnt/cluster_storage"
        if os.path.isdir("/mnt/cluster_storage")
        else "/tmp/train_benchmark"
    )
    os.makedirs(root, exist_ok=True)
    return root


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

        # Durable checkpoint destination on shared storage, so the torchrun
        # baseline exercises the same save+persist work as the Ray path (for an
        # apples-to-apples e2e comparison). Counter advances in lockstep across
        # ranks since every rank calls report() the same number of times.
        self._checkpoint_base = os.path.join(
            _shared_root(), experiment_name, "checkpoints"
        )
        self._checkpoint_counter = 0

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
        # Persist the checkpoint on EVERY rank — DeepSpeed shards are per-rank,
        # so all ranks must write their shard into the shared destination.
        # Final metrics are returned to the driver via the actor's run() value
        # (see torchrun_ray_launcher), so there's no metrics file to write.
        if checkpoint_dir is not None:
            self._persist_checkpoint(checkpoint_dir)
        if self.world_rank == 0:
            logger.info(f"[torchrun] report: {metrics}")

    def _persist_checkpoint(self, checkpoint_dir: str) -> None:
        dest = os.path.join(
            self._checkpoint_base, f"checkpoint_{self._checkpoint_counter:06d}"
        )
        self._checkpoint_counter += 1
        # dirs_exist_ok so all ranks merge their distinct shard files into one
        # checkpoint directory without colliding.
        shutil.copytree(checkpoint_dir, dest, dirs_exist_ok=True)
        if self.world_rank == 0:
            logger.info(f"[torchrun] persisted checkpoint -> {dest}")

    def get_checkpoint_dir(self) -> Optional[str]:
        # Restore-from-checkpoint is not wired for the torchrun baseline yet;
        # the save path above is what the e2e comparison needs.
        return None
