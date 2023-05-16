import ray
from ray.air import session
from ray.air.constants import MODEL_KEY
from ray.data.dataset import DataIterator
from ray.train.lightning.lightning_checkpoint import LightningCheckpoint

import logging
import shutil
import torch
import tempfile
from packaging.version import Version
from typing import Any, Dict, Optional
from torch.utils.data import IterableDataset, DataLoader

import pytorch_lightning as pl
from pytorch_lightning.callbacks import ModelCheckpoint
from pytorch_lightning.plugins.environments import LightningEnvironment
from pytorch_lightning.strategies import DDPStrategy

_LIGHTNING_GREATER_EQUAL_2_0 = Version(pl.__version__) >= Version("2.0.0")
_TORCH_GREATER_EQUAL_1_12 = Version(torch.__version__) >= Version("1.12.0")
_TORCH_FSDP_AVAILABLE = _TORCH_GREATER_EQUAL_1_12 and torch.distributed.is_available()

if _LIGHTNING_GREATER_EQUAL_2_0:
    from pytorch_lightning.strategies import FSDPStrategy
else:
    from pytorch_lightning.strategies import DDPFullyShardedStrategy as FSDPStrategy

if _TORCH_FSDP_AVAILABLE:
    from torch.distributed.fsdp import (
        FullStateDictConfig,
        FullyShardedDataParallel,
        StateDictType,
    )


logger = logging.getLogger(__name__)

LIGHTNING_REPORT_STAGE_KEY = "_report_on"


def get_worker_root_device():
    """Get the first torch device of the current worker if there are multiple."""
    devices = ray.train.torch.get_device()
    if isinstance(devices, list):
        return devices[0]
    else:
        return devices


class RayDDPStrategy(DDPStrategy):
    """Subclass of DDPStrategy to ensure compatibility with Ray orchestration."""

    @property
    def root_device(self) -> torch.device:
        return get_worker_root_device()

    @property
    def distributed_sampler_kwargs(self) -> Dict[str, Any]:
        return dict(
            num_replicas=self.world_size,
            rank=self.global_rank,
        )


class RayFSDPStrategy(FSDPStrategy):
    """Subclass of FSDPStrategy to ensure compatibility with Ray orchestration."""

    @property
    def root_device(self) -> torch.device:
        return get_worker_root_device()

    @property
    def distributed_sampler_kwargs(self) -> Dict[str, Any]:
        return dict(
            num_replicas=self.world_size,
            rank=self.global_rank,
        )

    def lightning_module_state_dict(self) -> Dict[str, Any]:
        """Gathers the full state dict to rank 0 on CPU."""
        assert self.model is not None, "Failed to get the state dict for a None model!"

        if _LIGHTNING_GREATER_EQUAL_2_0 and _TORCH_FSDP_AVAILABLE:
            with FullyShardedDataParallel.state_dict_type(
                module=self.model,
                state_dict_type=StateDictType.FULL_STATE_DICT,
                state_dict_config=FullStateDictConfig(
                    offload_to_cpu=True, rank0_only=True
                ),
            ):
                state_dict = self.model.state_dict()
                prefix_len = len("_forward_module.")
                return {k[prefix_len:]: v for k, v in state_dict.items()}
        else:
            # Otherwise Lightning uses Fairscale FSDP, no need to unshard by ourself.
            return super().lightning_module_state_dict()


class RayEnvironment(LightningEnvironment):
    """Setup Lightning DDP training environment for Ray cluster."""

    def world_size(self) -> int:
        return session.get_world_size()

    def global_rank(self) -> int:
        return session.get_world_rank()

    def local_rank(self) -> int:
        return session.get_local_rank()

    def node_rank(self) -> int:
        return session.get_node_rank()

    def set_world_size(self, size: int) -> None:
        # Disable it since `world_size()` directly returns data from AIR session.
        pass

    def set_global_rank(self, rank: int) -> None:
        # Disable it since `global_rank()` directly returns data from AIR session.
        pass

    def teardown(self):
        pass


class RayIterableDataset(IterableDataset):
    def __init__(self, dataset: "DataIterator", config: Dict[str, Any]) -> None:
        super().__init__()
        self.dataset = dataset
        self.config = config

    def __iter__(self):
        return self.dataset.iter_torch_batches(**self.config)


class RayDataModule(pl.LightningDataModule):
    def __init__(
        self,
        dataset_iter_config: Dict[str, Any],
        train_dataset: "DataIterator",
        val_dataset: Optional["DataIterator"] = None,
    ) -> None:
        super().__init__()

        def _train_dataloader() -> DataLoader:
            assert train_dataset
            ds = RayIterableDataset(train_dataset, dataset_iter_config)
            return DataLoader(ds, batch_size=1, collate_fn=lambda x: x[0])

        def _val_dataloader() -> DataLoader:
            assert val_dataset
            ds = RayIterableDataset(val_dataset, dataset_iter_config)
            return DataLoader(ds, batch_size=1, collate_fn=lambda x: x[0])

        if train_dataset:
            self.train_dataloader = _train_dataloader

        # ``pl.Trainer`` checks if the val_dataloader method has been overridden
        # to determine whether to enable the validation loop. To align with this
        # setting, we only override this method when `val_dataset` is not `None`.
        if val_dataset:
            self.val_dataloader = _val_dataloader


class RayModelCheckpoint(ModelCheckpoint):
    """
    AIR customized ModelCheckpoint callback.

    A subclass of ``pytorch_lightning.callbacks.ModelCheckpoint``.
    This callback function reports the latest metrics to the AIR session and
    creates an AIR checkpoint whenever a lightning checkpoint is saved.
    """

    def setup(self, *args, **kwargs) -> None:
        super().setup(*args, **kwargs)
        self.is_checkpoint_step = False

    def _session_report(self, trainer: "pl.Trainer", stage: str):
        """Report latest metrics dict and checkpoint to AIR training session.

        This method is called whenever a new checkpoint is created. It creates
        a `LightningCheckpoint` and reports it to the AIR session along with
        the latest metrics.
        """

        # Align the frequency of checkpointing and logging
        if not self.is_checkpoint_step:
            return

        # Report latest logged metrics
        metrics = {LIGHTNING_REPORT_STAGE_KEY: stage}
        for k, v in self._monitor_candidates(trainer).items():
            if isinstance(v, torch.Tensor):
                metrics[k] = v.item()

        # Report latest saved checkpoint
        # Note that AIR only takes the checkpoint of rank 0.
        # Save a dummy checkpoint on the other workers to avoid blocking.
        with tempfile.TemporaryDirectory() as tmpdir:
            if trainer.global_rank == 0:
                shutil.copy(self.last_model_path, f"{tmpdir}/{MODEL_KEY}")
                checkpoint = LightningCheckpoint.from_directory(path=tmpdir)
            else:
                checkpoint = LightningCheckpoint.from_dict(
                    {"rank": session.get_world_rank()}
                )
            session.report(metrics=metrics, checkpoint=checkpoint)

        self.is_checkpoint_step = False

    def _save_last_checkpoint(self, *args, **kwargs) -> None:
        super()._save_last_checkpoint(*args, **kwargs)
        self.is_checkpoint_step = True

    def on_train_batch_end(self, trainer: "pl.Trainer", *args, **kwargs) -> None:
        super().on_train_batch_end(trainer, *args, **kwargs)
        self._session_report(trainer=trainer, stage="train_batch_end")

    def on_train_epoch_end(self, trainer: "pl.Trainer", *args, **kwargs) -> None:
        super().on_train_epoch_end(trainer, *args, **kwargs)
        self._session_report(trainer=trainer, stage="train_epoch_end")

    def on_validation_end(self, trainer: "pl.Trainer", *args, **kwargs) -> None:
        super().on_validation_end(trainer, *args, **kwargs)
        self._session_report(trainer=trainer, stage="validation_end")
