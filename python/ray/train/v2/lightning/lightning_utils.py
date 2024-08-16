import os
import shutil
import tempfile
from pathlib import Path

import ray.train
from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.train.lightning._lightning_utils import (
    RayTrainReportCallback as RayTrainReportCallbackV1,
)
from ray.train.lightning._lightning_utils import import_lightning
from ray.util import PublicAPI

pl = import_lightning()


@PublicAPI(stability="beta")
class RayTrainReportCallback(RayTrainReportCallbackV1):
    """A simple callback that reports checkpoints to Ray on train epoch end.

    This callback is a subclass of `lightning.pytorch.callbacks.Callback
    <https://lightning.ai/docs/pytorch/stable/api/lightning.pytorch.callbacks.Callback.html#lightning.pytorch.callbacks.Callback>`_.

    It fetches the latest `trainer.callback_metrics` and reports together with
    the checkpoint on each training epoch end.

    Checkpoints will be saved in the following structure:

        checkpoint_{timestamp}/      Ray Train's checkpoint folder
        └─ checkpoint.ckpt           Lightning's checkpoint format

    For customized reporting and checkpointing logic, implement your own
    `lightning.pytorch.callbacks.Callback` following this user
    guide: :ref:`Saving and Loading Checkpoints <train-dl-saving-checkpoints>`.
    """

    def __init__(self) -> None:
        # TODO: Upstream this change into ray.train.lightning.
        # The difference in this version is removing the trial directory usage.
        job_id = ray.get_runtime_context().get_job_id()
        experiment_name = ray.train.get_context().get_experiment_name()
        self.local_rank = ray.train.get_context().get_local_rank()

        # Create a root temporary directory for storing local checkpoints
        # before persisting to storage.
        # Lightning's checkpointing implementation requires that this directory
        # is a common path across all workers.
        # Construct the path prefix with the job id and experiment name,
        # which are shared across workers for a Ray Train run.
        # This path should not be shared across different Ray Train runs.
        self.tmpdir_prefix = Path(
            tempfile.gettempdir(),
            f"lightning_checkpoints-job_id={job_id}-name={experiment_name}",
        ).as_posix()
        if os.path.isdir(self.tmpdir_prefix) and self.local_rank == 0:
            shutil.rmtree(self.tmpdir_prefix)

        record_extra_usage_tag(TagKey.TRAIN_LIGHTNING_RAYTRAINREPORTCALLBACK, "1")
