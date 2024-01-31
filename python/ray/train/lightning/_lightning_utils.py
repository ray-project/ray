import logging
import os
import shutil
import tempfile
from typing import Any, Dict

import torch
from packaging.version import Version

import ray
from ray import train
from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.train import Checkpoint
from ray.util import PublicAPI


def import_lightning():  # noqa: F402
    try:
        import lightning.pytorch as pl
    except ModuleNotFoundError:
        import pytorch_lightning as pl
    return pl


pl = import_lightning()

_LIGHTNING_GREATER_EQUAL_2_0 = Version(pl.__version__) >= Version("2.0.0")
_TORCH_GREATER_EQUAL_1_12 = Version(torch.__version__) >= Version("1.12.0")
_TORCH_FSDP_AVAILABLE = _TORCH_GREATER_EQUAL_1_12 and torch.distributed.is_available()

try:
    from lightning.pytorch.plugins.environments import LightningEnvironment
except ModuleNotFoundError:
    from pytorch_lightning.plugins.environments import LightningEnvironment

if _LIGHTNING_GREATER_EQUAL_2_0:
    FSDPStrategy = pl.strategies.FSDPStrategy
else:
    FSDPStrategy = pl.strategies.DDPFullyShardedStrategy

if _TORCH_FSDP_AVAILABLE:
    from torch.distributed.fsdp import (
        FullStateDictConfig,
        FullyShardedDataParallel,
        StateDictType,
    )


logger = logging.getLogger(__name__)

LIGHTNING_REPORT_STAGE_KEY = "_report_on"


@PublicAPI(stability="beta")
class RayDDPStrategy(pl.strategies.DDPStrategy):
    """Subclass of DDPStrategy to ensure compatibility with Ray orchestration.

    For a full list of initialization arguments, please refer to:
    https://lightning.ai/docs/pytorch/stable/api/lightning.pytorch.strategies.DDPStrategy.html

    Note that `process_group_backend`, `timeout`, and `start_method` are disabled here,
    please specify these arguments in :class:`~ray.train.torch.TorchConfig` instead.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        record_extra_usage_tag(TagKey.TRAIN_LIGHTNING_RAYDDPSTRATEGY, "1")

    @property
    def root_device(self) -> torch.device:
        return ray.train.torch.get_device()

    @property
    def distributed_sampler_kwargs(self) -> Dict[str, Any]:
        return dict(
            num_replicas=self.world_size,
            rank=self.global_rank,
        )


@PublicAPI(stability="beta")
class RayFSDPStrategy(FSDPStrategy):  # noqa: F821
    """Subclass of FSDPStrategy to ensure compatibility with Ray orchestration.

    For a full list of initialization arguments, please refer to:
    https://lightning.ai/docs/pytorch/stable/api/lightning.pytorch.strategies.FSDPStrategy.html
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        record_extra_usage_tag(TagKey.TRAIN_LIGHTNING_RAYFSDPSTRATEGY, "1")

    @property
    def root_device(self) -> torch.device:
        return ray.train.torch.get_device()

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


@PublicAPI(stability="beta")
class RayDeepSpeedStrategy(pl.strategies.DeepSpeedStrategy):
    """Subclass of DeepSpeedStrategy to ensure compatibility with Ray orchestration.

    For a full list of initialization arguments, please refer to:
    https://lightning.ai/docs/pytorch/stable/api/lightning.pytorch.strategies.DeepSpeedStrategy.html
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        record_extra_usage_tag(TagKey.TRAIN_LIGHTNING_RAYDEEPSPEEDSTRATEGY, "1")

    @property
    def root_device(self) -> torch.device:
        return ray.train.torch.get_device()

    @property
    def distributed_sampler_kwargs(self) -> Dict[str, Any]:
        return dict(
            num_replicas=self.world_size,
            rank=self.global_rank,
        )


@PublicAPI(stability="beta")
class RayLightningEnvironment(LightningEnvironment):  # noqa: F821
    """Setup Lightning DDP training environment for Ray cluster."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        record_extra_usage_tag(TagKey.TRAIN_LIGHTNING_RAYLIGHTNINGENVIRONMENT, "1")

    def world_size(self) -> int:
        return train.get_context().get_world_size()

    def global_rank(self) -> int:
        return train.get_context().get_world_rank()

    def local_rank(self) -> int:
        return train.get_context().get_local_rank()

    def node_rank(self) -> int:
        return train.get_context().get_node_rank()

    def set_world_size(self, size: int) -> None:
        # Disable it since `world_size()` directly returns data from Train context.
        pass

    def set_global_rank(self, rank: int) -> None:
        # Disable it since `global_rank()` directly returns data from Train.
        pass

    def teardown(self):
        pass


@PublicAPI(stability="beta")
def prepare_trainer(trainer: pl.Trainer) -> pl.Trainer:
    """Prepare the PyTorch Lightning Trainer for distributed execution."""

    # Check strategy class
    valid_strategy_class = [RayDDPStrategy, RayFSDPStrategy, RayDeepSpeedStrategy]

    if not any(isinstance(trainer.strategy, cls) for cls in valid_strategy_class):
        raise RuntimeError(
            f"Invalid strategy class: {type(trainer.strategy)}. To use "
            "PyTorch Lightning with Ray, the strategy object should be one of "
            f"{[cls.__name__ for cls in valid_strategy_class]} class "
            "or its subclass."
        )

    # Check cluster environment
    cluster_environment = getattr(trainer.strategy, "cluster_environment", None)
    if cluster_environment and not isinstance(
        cluster_environment, RayLightningEnvironment
    ):
        raise RuntimeError(
            "Invalid cluster environment plugin. The expected class is"
            "`ray.train.lightning.RayLightningEnvironment` "
            f"but got {type(cluster_environment)}!"
        )

    record_extra_usage_tag(TagKey.TRAIN_LIGHTNING_PREPARE_TRAINER, "1")
    return trainer


@PublicAPI(stability="beta")
class RayTrainReportCallback(pl.callbacks.Callback):
    """A simple callback that reports checkpoints to Ray on train epoch end.

    This callback is a subclass of `lightning.pytorch.callbacks.Callback
    <https://lightning.ai/docs/pytorch/stable/api/lightning.pytorch.callbacks.Callback.html#lightning.pytorch.callbacks.Callback>`_.

    It fetches the latest `trainer.callback_metrics` and reports together with
    the checkpoint on each training epoch end.

    Checkpoints will be saved in the following structure::

        checkpoint_00000*/      Ray Train Checkpoint
        └─ checkpoint.ckpt      PyTorch Lightning Checkpoint

    For customized reporting and checkpointing logic, implement your own
    `lightning.pytorch.callbacks.Callback` following this user
    guide: :ref:`Saving and Loading Checkpoints <train-dl-saving-checkpoints>`.
    """

    CHECKPOINT_NAME = "checkpoint.ckpt"

    def __init__(self) -> None:
        super().__init__()
        self.trial_name = train.get_context().get_trial_name()
        self.local_rank = train.get_context().get_local_rank()
        self.tmpdir_prefix = os.path.join(tempfile.gettempdir(), self.trial_name)
        if os.path.isdir(self.tmpdir_prefix) and self.local_rank == 0:
            shutil.rmtree(self.tmpdir_prefix)

        record_extra_usage_tag(TagKey.TRAIN_LIGHTNING_RAYTRAINREPORTCALLBACK, "1")

    def on_train_epoch_end(self, trainer, pl_module) -> None:
        # Creates a checkpoint dir with fixed name
        tmpdir = os.path.join(self.tmpdir_prefix, str(trainer.current_epoch))
        os.makedirs(tmpdir, exist_ok=True)

        # Fetch metrics
        metrics = trainer.callback_metrics
        metrics = {k: v.item() for k, v in metrics.items()}

        # (Optional) Add customized metrics
        metrics["epoch"] = trainer.current_epoch
        metrics["step"] = trainer.global_step

        # Save checkpoint to local
        ckpt_path = os.path.join(tmpdir, self.CHECKPOINT_NAME)
        trainer.save_checkpoint(ckpt_path, weights_only=False)

        # Report to train session
        checkpoint = Checkpoint.from_directory(tmpdir)
        train.report(metrics=metrics, checkpoint=checkpoint)

        # Add a barrier to ensure all workers finished reporting here
        torch.distributed.barrier()

        if self.local_rank == 0:
            shutil.rmtree(tmpdir)
