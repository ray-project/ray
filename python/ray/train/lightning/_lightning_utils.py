import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

import torch
from packaging.version import Version

import ray
import ray.train
from ray._common.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.train import Checkpoint
from ray.train.v2._internal.constants import is_v2_enabled
from ray.util import PublicAPI


def import_lightning():  # noqa: F402
    try:
        import lightning.pytorch as pl
    except ModuleNotFoundError:
        import pytorch_lightning as pl
    return pl


pl = import_lightning()

if is_v2_enabled():
    from ray.train.v2.api.report_config import CheckpointUploadMode

if TYPE_CHECKING:
    from ray.train.v2.api.validation_config import ValidationTaskConfig

_LIGHTNING_GREATER_EQUAL_2_0 = Version(pl.__version__) >= Version("2.0.0")
_LIGHTNING_LESS_THAN_2_1 = Version(pl.__version__) < Version("2.1.0")
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
        """This worker's device, from :func:`ray.train.torch.get_device`."""
        return ray.train.torch.get_device()

    @property
    def distributed_sampler_kwargs(self) -> Dict[str, Any]:
        """Keyword arguments for the distributed sampler.

        ``num_replicas`` is the world size and ``rank`` is this worker's global rank.
        """
        return dict(
            num_replicas=self.world_size,
            rank=self.global_rank,
        )


@PublicAPI(stability="beta")
class RayFSDPStrategy(FSDPStrategy):  # noqa: F821
    """Subclass of FSDPStrategy to ensure compatibility with Ray orchestration.

    For a full list of initialization arguments, please refer to:
    https://lightning.ai/docs/pytorch/stable/api/lightning.pytorch.strategies.FSDPStrategy.html

    .. note::
        It is recommended to upgrade `lightning>=2.1` or above when using FSDP
        with Lightning, since Lightning starts to natively support `state_dict_type`,
        `sharding_strategy`, `auto_wrap_policy` and other FSDP configurations from 2.1.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        record_extra_usage_tag(TagKey.TRAIN_LIGHTNING_RAYFSDPSTRATEGY, "1")

    @property
    def root_device(self) -> torch.device:
        """This worker's device, from :func:`ray.train.torch.get_device`."""
        return ray.train.torch.get_device()

    @property
    def distributed_sampler_kwargs(self) -> Dict[str, Any]:
        """Keyword arguments for the distributed sampler.

        ``num_replicas`` is the world size and ``rank`` is this worker's global rank.
        """
        return dict(
            num_replicas=self.world_size,
            rank=self.global_rank,
        )

    def lightning_module_state_dict(self) -> Dict[str, Any]:
        """Gathers the full state dict to rank 0 on CPU.

        FSDP checkpointing is broken in Lightning 2.0.x. This subclass patches the
        behavior to perform a full state dict checkpointing, gathering the checkpoint
        shards on rank 0 CPU. Upgrade to `lightning>=2.1` to do sharded state dict
        checkpointing.

        See the note in the class docstring for more details.

        Returns:
            The model's state dict. With Lightning 2.0.x and PyTorch FSDP, it's the
            full state dict gathered on rank 0 and offloaded to CPU, with the
            ``_forward_module.`` prefix removed from its keys. Otherwise, it's the
            state dict that the parent ``FSDPStrategy`` returns.
        """

        assert self.model is not None, "Failed to get the state dict for a None model!"

        if (
            _TORCH_FSDP_AVAILABLE
            and _LIGHTNING_GREATER_EQUAL_2_0
            and _LIGHTNING_LESS_THAN_2_1
        ):
            with FullyShardedDataParallel.state_dict_type(
                module=self.model,
                state_dict_type=StateDictType.FULL_STATE_DICT,
                state_dict_config=FullStateDictConfig(
                    offload_to_cpu=True, rank0_only=True
                ),
            ):
                state_dict = self.model.state_dict()

                ckpt_state_dict = {}
                prefix_len = len("_forward_module.")
                for k, v in state_dict.items():
                    if k.startswith("_forward_module."):
                        non_prefixed_key = k[prefix_len:]
                        ckpt_state_dict[non_prefixed_key] = v
                    else:
                        ckpt_state_dict[k] = v
                return ckpt_state_dict
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
        """This worker's device, from :func:`ray.train.torch.get_device`."""
        return ray.train.torch.get_device()

    @property
    def distributed_sampler_kwargs(self) -> Dict[str, Any]:
        """Keyword arguments for the distributed sampler.

        ``num_replicas`` is the world size and ``rank`` is this worker's global rank.
        """
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
        """Return the number of workers in the training run.

        Returns:
            The world size from the Ray Train context.
        """
        return ray.train.get_context().get_world_size()

    def global_rank(self) -> int:
        """Return this worker's rank across all workers.

        Returns:
            The world rank from the Ray Train context.
        """
        return ray.train.get_context().get_world_rank()

    def local_rank(self) -> int:
        """Return this worker's rank among the workers on its node.

        Returns:
            The local rank from the Ray Train context.
        """
        return ray.train.get_context().get_local_rank()

    def node_rank(self) -> int:
        """Return the rank of the node this worker runs on.

        Returns:
            The node rank from the Ray Train context.
        """
        return ray.train.get_context().get_node_rank()

    def set_world_size(self, size: int) -> None:
        """Do nothing, because :meth:`world_size` reads the Ray Train context.

        Args:
            size: Ignored.
        """
        # Disable it since `world_size()` directly returns data from Train context.
        pass

    def set_global_rank(self, rank: int) -> None:
        """Do nothing, because :meth:`global_rank` reads the Ray Train context.

        Args:
            rank: Ignored.
        """
        # Disable it since `global_rank()` directly returns data from Train.
        pass

    def teardown(self):
        """Do nothing, in place of the parent environment's teardown."""
        pass


@PublicAPI(stability="beta")
def prepare_trainer(trainer: pl.Trainer) -> pl.Trainer:
    """Prepare the PyTorch Lightning Trainer for distributed execution.

    Checks that the trainer's strategy is a :class:`RayDDPStrategy`,
    :class:`RayFSDPStrategy`, or :class:`RayDeepSpeedStrategy`, or a subclass of
    one, and that its cluster environment, if it has one, is a
    :class:`RayLightningEnvironment`.

    Args:
        trainer: The Lightning ``Trainer`` to prepare.

    Returns:
        The same ``trainer`` object.

    Raises:
        RuntimeError: If the trainer's strategy or cluster environment isn't one
            of the classes above.
    """

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

    Args:
        checkpoint_upload_mode: The manner in which to upload the checkpoint.
            See :ref:`Checkpoint upload modes <train-checkpoint-upload-modes>`
            for more details. Ray Train v2 only. Defaults to
            ``CheckpointUploadMode.SYNC``.
        validation: Whether to asynchronously validate the checkpoint, or a
            ``ValidationTaskConfig`` for the validation task. See
            :ref:`Validating checkpoints asynchronously <train-validating-checkpoints>`
            for more details. Ray Train v2 only.
        checkpoint_frequency: Only report a checkpoint and metrics every
            ``checkpoint_frequency`` epochs. Defaults to ``1``, which reports on
            every epoch. If ``max_epochs`` isn't a multiple of
            ``checkpoint_frequency``, the epochs after the last multiple aren't
            reported.

    Raises:
        TypeError: If ``checkpoint_frequency`` isn't an integer.
        ValueError: If ``checkpoint_frequency`` is less than 1, or if
            ``checkpoint_upload_mode`` or ``validation`` is set on Ray Train v1.
    """

    CHECKPOINT_NAME = "checkpoint.ckpt"

    def __init__(
        self,
        # v2-only arguments
        checkpoint_upload_mode: Optional["CheckpointUploadMode"] = None,
        validation: Union[bool, "ValidationTaskConfig"] = False,
        checkpoint_frequency: int = 1,
    ) -> None:
        super().__init__()
        if not isinstance(checkpoint_frequency, int) or isinstance(
            checkpoint_frequency, bool
        ):
            raise TypeError(
                "`checkpoint_frequency` must be an integer, got "
                f"{type(checkpoint_frequency).__name__}."
            )
        if checkpoint_frequency < 1:
            raise ValueError(
                "`checkpoint_frequency` must be a positive integer, got "
                f"{checkpoint_frequency}."
            )
        self.checkpoint_frequency = checkpoint_frequency
        self.checkpoint_upload_mode = checkpoint_upload_mode
        self.validation = validation

        if is_v2_enabled():
            if checkpoint_upload_mode is None:
                self.checkpoint_upload_mode = CheckpointUploadMode.SYNC
        else:
            if checkpoint_upload_mode is not None:
                raise ValueError(
                    "`checkpoint_upload_mode` is only supported in Ray Train v2. "
                    "To enable it, please set `RAY_TRAIN_V2_ENABLED=1`."
                )
            if validation:
                raise ValueError(
                    "`validation` is only supported in Ray Train v2. "
                    "To enable it, please set `RAY_TRAIN_V2_ENABLED=1`."
                )

        job_id = ray.get_runtime_context().get_job_id()
        experiment_name = ray.train.get_context().get_experiment_name()

        self.tmpdir_prefix = Path(
            tempfile.gettempdir(),
            f"lightning_checkpoints-job_id={job_id}-name={experiment_name}-world_rank={ray.train.get_context().get_world_rank()}",
        ).as_posix()
        if os.path.isdir(self.tmpdir_prefix):
            shutil.rmtree(self.tmpdir_prefix)

        record_extra_usage_tag(TagKey.TRAIN_LIGHTNING_RAYTRAINREPORTCALLBACK, "1")

    def on_train_epoch_end(
        self, trainer: pl.Trainer, pl_module: pl.LightningModule
    ) -> None:
        """Report metrics and a checkpoint to Ray Train at the end of an epoch.

        Reports only every ``checkpoint_frequency`` epochs.

        Args:
            trainer: The Lightning trainer. The callback reports its
                ``callback_metrics`` and saves its checkpoint.
            pl_module: The Lightning module being trained. Unused.
        """
        # Only report (metrics + checkpoint) every `checkpoint_frequency` epochs.
        if (trainer.current_epoch + 1) % self.checkpoint_frequency != 0:
            return

        # Fetch metrics
        metrics = trainer.callback_metrics
        metrics = {k: v.item() for k, v in metrics.items()}

        # (Optional) Add customized metrics
        metrics["epoch"] = trainer.current_epoch
        metrics["step"] = trainer.global_step

        # Creates a checkpoint dir with fixed name
        tmpdir = Path(self.tmpdir_prefix, str(trainer.current_epoch)).as_posix()
        os.makedirs(tmpdir, exist_ok=True)

        # Save checkpoint to local
        ckpt_path = Path(tmpdir, self.CHECKPOINT_NAME).as_posix()
        # TODO: with CheckpointUploadMode.ASYNC, this does cpu -> disk synchronously
        # and disk -> remote asynchronously. We can add a new CheckpointIO class to do
        # cpu -> remote asynchronously and a checkpoint_upload_fn to wait for it.
        trainer.save_checkpoint(ckpt_path, weights_only=False)
        checkpoint = Checkpoint.from_directory(tmpdir)

        # Report to train session
        if is_v2_enabled():
            ray.train.report(
                metrics=metrics,
                checkpoint=checkpoint,
                checkpoint_upload_mode=self.checkpoint_upload_mode,
                validation=self.validation,
            )
        else:
            ray.train.report(metrics=metrics, checkpoint=checkpoint)

        # Add a barrier to ensure all workers finished reporting here
        trainer.strategy.barrier()

        # With CheckpointUploadMode.ASYNC, the upload may still be in progress
        # after report() returns. Let ray.train.report delete_local_checkpoint_after_upload
        # handle cleanup instead.
        if is_v2_enabled() and self.checkpoint_upload_mode is not None:
            # Check here because CheckpointUploadMode is only imported when is_v2_enabled() is True
            if (
                self.checkpoint_upload_mode.default_delete_local_checkpoint_after_upload()
            ):
                return

        shutil.rmtree(tmpdir)
