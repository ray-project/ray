import logging
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator, Optional, Type

from torch.utils.data import DataLoader, Dataset, IterableDataset

import ray
from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.data.iterator import _IterableFromIterator
from ray.train import Checkpoint
from ray.util import PublicAPI

logger = logging.getLogger(__name__)


TRANSFORMERS_IMPORT_ERROR: Optional[ImportError] = None

try:
    import transformers.trainer
    from transformers import Trainer
    from transformers.trainer_callback import TrainerCallback
except ImportError as e:
    TRANSFORMERS_IMPORT_ERROR = e
    TrainerCallback = object


@PublicAPI(stability="beta")
class RayTrainReportCallback(TrainerCallback):
    """A simple callback to report checkpoints and metrics to Ray Tarin.

    This callback is a subclass of `transformers.TrainerCallback
    <https://huggingface.co/docs/transformers/main/en/main_classes/callback#transformers.TrainerCallback>`_
    and overrides the `TrainerCallback.on_save()` method. After
    a new checkpoint get saved, it fetches the latest metric dictionary
    from `TrainerState.log_history` and reports it with the latest checkpoint
    to Ray Train.

    Checkpoints will be saved in the following structure::

        checkpoint_00000*/   Ray Train Checkpoint
        └─ checkpoint/       Hugging Face Transformers Checkpoint

    For customized reporting and checkpointing logic, implement your own
    `transformers.TrainerCallback` following this user
    guide: :ref:`Saving and Loading Checkpoints <train-dl-saving-checkpoints>`.

    Note that users should ensure that the logging, evaluation, and saving frequencies
    are properly configured so that the monitoring metric is always up-to-date
    when `transformers.Trainer` saves a checkpoint.

    Suppose the monitoring metric is reported from evaluation stage:

    Some valid configurations:
        - evaluation_strategy == save_strategy == "epoch"
        - evaluation_strategy == save_strategy == "steps", save_steps % eval_steps == 0

    Some invalid configurations:
        - evaluation_strategy != save_strategy
        - evaluation_strategy == save_strategy == "steps", save_steps % eval_steps != 0

    """

    CHECKPOINT_NAME = "checkpoint"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        record_extra_usage_tag(TagKey.TRAIN_TRANSFORMERS_RAYTRAINREPORTCALLBACK, "1")

    def on_save(self, args, state, control, **kwargs):
        """Event called after a checkpoint save."""
        with TemporaryDirectory() as tmpdir:
            # Aggregate all the logged metrics
            metrics = {}
            for log in state.log_history:
                metrics.update(log)

            # Copy ckpt files and construct a Ray Train Checkpoint
            source_ckpt_path = transformers.trainer.get_last_checkpoint(args.output_dir)
            if source_ckpt_path is not None:
                target_ckpt_path = Path(tmpdir, self.CHECKPOINT_NAME).as_posix()
                shutil.copytree(source_ckpt_path, target_ckpt_path)
                checkpoint = Checkpoint.from_directory(tmpdir)
            else:
                checkpoint = None

            # Report latest metrics and checkpoint to Ray Train
            ray.train.report(metrics=metrics, checkpoint=checkpoint)


class RayTorchIterableDataset(IterableDataset):
    """Wrapper class for ray data iterables."""

    def __init__(self, data_iterable) -> None:
        super().__init__()
        self.data_iterable = data_iterable

    def __iter__(self) -> Iterator:
        return iter(self.data_iterable)


@PublicAPI(stability="beta")
def prepare_trainer(trainer: "Trainer") -> "Trainer":
    """Prepare your HuggingFace Transformer Trainer for Ray Train.

    This utility function enable the trainer integrates with Ray Data Integration.
    Internally, it overrides the `get_train_dataloader` and `get_eval_dataloader`
    methods and inject the data integration logics if the `train_dataset` and
    `eval_dataset` are Ray Data Iterables.
    """

    if TRANSFORMERS_IMPORT_ERROR is not None:
        raise TRANSFORMERS_IMPORT_ERROR

    base_trainer_class: Type[transformers.trainer.Trainer] = trainer.__class__

    class RayTransformersTrainer(base_trainer_class):
        """A Wrapper of `transformers.Trainer` for Ray Data Integration."""

        def get_train_dataloader(self) -> DataLoader:
            if isinstance(self.train_dataset, _IterableFromIterator):
                dataset = RayTorchIterableDataset(self.train_dataset)
                return DataLoader(dataset, batch_size=1, collate_fn=lambda x: x[0])
            else:
                return super().get_train_dataloader()

        def get_eval_dataloader(
            self, eval_dataset: Optional[Dataset] = None
        ) -> DataLoader:
            if eval_dataset is None:
                eval_dataset = self.eval_dataset

            if isinstance(eval_dataset, _IterableFromIterator):
                dataset = RayTorchIterableDataset(eval_dataset)
                return DataLoader(dataset, batch_size=1, collate_fn=lambda x: x[0])
            else:
                return super().get_eval_dataloader(eval_dataset)

    trainer.__class__ = RayTransformersTrainer

    record_extra_usage_tag(TagKey.TRAIN_TRANSFORMERS_PREPARE_TRAINER, "1")
    return trainer
