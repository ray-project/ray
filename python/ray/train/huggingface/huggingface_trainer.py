import inspect
import os
import shutil
import tempfile
import warnings
from distutils.version import LooseVersion
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Type

import transformers
import transformers.modeling_utils
import transformers.trainer
import transformers.training_args
from torch.utils.data import Dataset as TorchDataset

from ray import train
from ray.air import session
from ray.air._internal.checkpointing import (
    save_preprocessor_to_dir,
)
from ray.air.checkpoint import Checkpoint
from ray.air.config import DatasetConfig, RunConfig, ScalingConfig
from ray.train.constants import (
    EVALUATION_DATASET_KEY,
    PREPROCESSOR_KEY,
    TRAIN_DATASET_KEY,
    TUNE_CHECKPOINT_ID,
)
from ray.train.data_parallel_trainer import _DataParallelCheckpointManager
from ray.train.huggingface._huggingface_utils import (
    CHECKPOINT_PATH_ON_NODE_KEY,
    NODE_IP_KEY,
    TrainReportCallback,
    process_datasets,
    wrap_transformers_trainer,
)
from ray.train.torch import TorchConfig, TorchTrainer
from ray.train.trainer import GenDataset
from ray.tune.trainable import Trainable
from ray.tune.utils.file_transfer import delete_on_node, sync_dir_between_nodes
from ray.util import PublicAPI, get_node_ip_address
from ray.util.ml_utils.checkpoint_manager import CheckpointStorage, _TrackedCheckpoint

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor

# This trainer uses a special checkpoint syncing logic.
# Because HF checkpoints are very large dirs (at least several GBs),
# we use directory checkpoints that are synced between nodes when
# required instead of serializing the checkpoints and sending
# bytes over nodes. This is a much more performant solution for
# large directory checkpoints. The current implementation
# is special for HuggingFaceTrainer, but can and should be
# made generic.
# TODO(ml-team): Make dir syncing checkpoint logic generic.


# The checkpoint is turned into a dict with node ip & path
# in HuggingFaceTrainer.as_trainable
# TODO(team-ml): Refactor checkpoint management along with Tune.
class _SyncedTrackedCheckpoint(_TrackedCheckpoint):
    def commit(self, path: Optional[Path] = None) -> None:
        if (
            self.storage_mode == CheckpointStorage.MEMORY
            or not path
            or not isinstance(self.dir_or_data, dict)
        ):
            return

        source_ip = self.dir_or_data[NODE_IP_KEY]
        source_path = self.dir_or_data[CHECKPOINT_PATH_ON_NODE_KEY]
        target_ip = get_node_ip_address()

        if source_ip == target_ip:
            # Move contents of source_path, but not source_path
            # itself. shutil.move is already recursive.
            for inner in Path(source_path).iterdir():
                shutil.move(str(inner.absolute()), str(path))
            shutil.rmtree(source_path, ignore_errors=True)
        else:
            sync_dir_between_nodes(
                source_ip=source_ip,
                source_path=source_path,
                target_ip=target_ip,
                target_path=str(path),
                return_futures=False,
                max_size_bytes=None,
            )
            delete_on_node(node_ip=source_ip, path=source_path)
        save_preprocessor_to_dir(self.dir_or_data.pop(PREPROCESSOR_KEY, None), path)
        # add tune checkpoint id
        with open(path.joinpath(TUNE_CHECKPOINT_ID), "w") as f:
            f.write(str(self.id))


class _DataParallelSyncingCheckpointManager(_DataParallelCheckpointManager):
    def _process_persistent_checkpoint(self, checkpoint: _TrackedCheckpoint):
        sync_checkpoint = _SyncedTrackedCheckpoint(
            dir_or_data=checkpoint.dir_or_data,
            storage_mode=checkpoint.storage_mode,
            checkpoint_id=checkpoint.id,
            metrics=checkpoint.metrics,
            node_ip=checkpoint.node_ip,
        )

        super(
            _DataParallelSyncingCheckpointManager, self
        )._process_persistent_checkpoint(checkpoint=sync_checkpoint)


@PublicAPI(stability="alpha")
class HuggingFaceTrainer(TorchTrainer):
    """A Trainer for data parallel HuggingFace Transformers on PyTorch training.

    This Trainer runs the ``transformers.Trainer.train()`` method on multiple
    Ray Actors. The training is carried out in a distributed fashion through PyTorch
    DDP. These actors already have the necessary torch process group already
    configured for distributed PyTorch training.

    The training function ran on every Actor will first run the
    specified ``trainer_init_per_worker`` function to obtain an instantiated
    ``transformers.Trainer`` object. The ``trainer_init_per_worker`` function
    will have access to preprocessed train and evaluation datsets.

    If the ``datasets`` dict contains a training dataset (denoted by
    the "train" key), then it will be split into multiple dataset
    shards, with each Actor training on a single shard.
    All the other datasets will not be split.

    Please note that if you use a custom ``transformers.Trainer`` subclass,
    the ``get_train_dataloader`` method will be wrapped around to disable
    sharding by ``transformers.IterableDatasetShard``, as the dataset will
    already be sharded on the Ray AIR side.

    HuggingFace loggers will be automatically disabled, and the ``local_rank``
    argument in ``TrainingArguments`` will be automatically set. Please note
    that if you want to use CPU training, you will need to set the ``no_cuda``
    argument in ``TrainingArguments`` manually - otherwise, an exception
    (segfault) may be thrown. Furthermore, 'steps' value for ``save_strategy``,
    ``logging_strategy`` and ``evaluation_strategy`` is not yet supported.

    This Trainer requires ``transformers>=4.19.0`` package.

    Example:
        .. code-block:: python

            # Based on
            # huggingface/notebooks/examples/language_modeling_from_scratch.ipynb

            # Hugging Face imports
            from datasets import load_dataset
            import transformers
            from transformers import AutoConfig, AutoModelForCausalLM, AutoTokenizer

            import ray
            from ray.train.huggingface import HuggingFaceTrainer

            model_checkpoint = "gpt2"
            tokenizer_checkpoint = "sgugger/gpt2-like-tokenizer"
            block_size = 128

            datasets = load_dataset("wikitext", "wikitext-2-raw-v1")
            tokenizer = AutoTokenizer.from_pretrained(tokenizer_checkpoint)

            def tokenize_function(examples):
                return tokenizer(examples["text"])

            tokenized_datasets = datasets.map(
                tokenize_function, batched=True, num_proc=1, remove_columns=["text"]
            )

            def group_texts(examples):
                # Concatenate all texts.
                concatenated_examples = {
                    k: sum(examples[k], []) for k in examples.keys()
                }
                total_length = len(concatenated_examples[list(examples.keys())[0]])
                # We drop the small remainder, we could add padding if the model
                # supported it.
                # instead of this drop, you can customize this part to your needs.
                total_length = (total_length // block_size) * block_size
                # Split by chunks of max_len.
                result = {
                    k: [
                        t[i : i + block_size]
                        for i in range(0, total_length, block_size)
                    ]
                    for k, t in concatenated_examples.items()
                }
                result["labels"] = result["input_ids"].copy()
                return result

            lm_datasets = tokenized_datasets.map(
                group_texts,
                batched=True,
                batch_size=1000,
                num_proc=1,
            )
            ray_train_ds = ray.data.from_huggingface(lm_datasets["train"])
            ray_evaluation_ds = ray.data.from_huggingface(
                lm_datasets["evaluation"]
            )

            def trainer_init_per_worker(train_dataset, eval_dataset, **config):
                model_config = AutoConfig.from_pretrained(model_checkpoint)
                model = AutoModelForCausalLM.from_config(model_config)
                args = transformers.TrainingArguments(
                    output_dir=f"{model_checkpoint}-wikitext2",
                    evaluation_strategy="epoch",
                    learning_rate=2e-5,
                    weight_decay=0.01,
                )
                return transformers.Trainer(
                    model=model,
                    args=args,
                    train_dataset=train_dataset,
                    eval_dataset=eval_dataset,
                )

            scaling_config = {"num_workers": 3}
            # If using GPUs, use the below scaling config instead.
            # scaling_config = {"num_workers": 3, "use_gpu": True}
            trainer = HuggingFaceTrainer(
                trainer_init_per_worker=trainer_init_per_worker,
                scaling_config=scaling_config,
                datasets={"train": ray_train_ds, "evaluation": ray_evaluation_ds},
            )
            result = trainer.fit()

    Args:
        trainer_init_per_worker: The function that returns an instantiated
            ``transformers.Trainer`` object and takes in the following arguments:
            train ``Torch.Dataset``, optional evaluation ``Torch.Dataset``
            and config as kwargs. The Torch Datasets are automatically
            created by converting the Ray Datasets internally before
            they are passed into the function.
        datasets: Any Ray Datasets to use for training. Use
            the key "train" to denote which dataset is the training
            dataset and (optionally) key "evaluation" to denote the evaluation
            dataset. Can only contain a training dataset
            and up to one extra dataset to be used for evaluation.
            If a ``preprocessor`` is provided and has not already been fit,
            it will be fit on the training dataset. All datasets will be
            transformed by the ``preprocessor`` if one is provided.
        trainer_init_config: Configurations to pass into
            ``trainer_init_per_worker`` as kwargs.
        torch_config: Configuration for setting up the PyTorch backend. If set to
            None, use the default configuration. This replaces the ``backend_config``
            arg of ``DataParallelTrainer``. Same as in ``TorchTrainer``.
        scaling_config: Configuration for how to scale data parallel training.
        dataset_config: Configuration for dataset ingest.
        run_config: Configuration for the execution of the training run.
        preprocessor: A ray.data.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    _checkpoint_manager_cls = _DataParallelSyncingCheckpointManager

    _dataset_config = {
        "train": DatasetConfig(fit=True, split=False, required=True),
        "evaluation": DatasetConfig(split=False),
    }

    def __init__(
        self,
        trainer_init_per_worker: Callable[
            [TorchDataset, Optional[TorchDataset], Any], transformers.trainer.Trainer
        ],
        *,
        datasets: Dict[str, GenDataset],
        trainer_init_config: Optional[Dict] = None,
        torch_config: Optional[TorchConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[Dict[str, DatasetConfig]] = None,
        run_config: Optional[RunConfig] = None,
        preprocessor: Optional["Preprocessor"] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):

        # Functionality required for HuggingFaceTrainer only added in this
        # version
        if LooseVersion(transformers.__version__) < LooseVersion("4.19.0"):
            raise RuntimeError(
                "HuggingFaceTrainer requires transformers>=4.19.0, but you "
                f"have {transformers.__version__} which is incompatible. "
                "Update on all nodes with `pip install -U 'transformers>=4.19.0'`."
            )

        self._validate_trainer_init_per_worker(
            trainer_init_per_worker, "trainer_init_per_worker"
        )

        trainer_init_config = trainer_init_config.copy() if trainer_init_config else {}
        if "_trainer_init_per_worker" in trainer_init_config:
            raise ValueError(
                "'_trainer_init_per_worker' is a reserved key in `trainer_init_config`."
            )
        trainer_init_config["_trainer_init_per_worker"] = trainer_init_per_worker

        super().__init__(
            train_loop_per_worker=_huggingface_train_loop_per_worker,
            train_loop_config=trainer_init_config,
            torch_config=torch_config,
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    def _validate_trainer_init_per_worker(
        self, trainer_init_per_worker: Callable, fn_name: str
    ) -> None:
        num_params = len(inspect.signature(trainer_init_per_worker).parameters)
        if num_params < 3:
            raise ValueError(
                f"{fn_name} should take in at least 3 arguments, "
                f"but it accepts {num_params} arguments instead."
            )

    def _validate_attributes(self):
        for key, conf in self._dataset_config.items():
            if conf.use_stream_api:
                raise ValueError(
                    "HuggingFaceTrainer does not support `use_stream_api`."
                )
        gpus_per_worker = self.scaling_config.get("num_gpus_per_worker", 0)
        if gpus_per_worker > 1:
            raise ValueError(
                f"You have assigned {gpus_per_worker} GPUs per worker. "
                "This is not supported by HuggingFace, which expects "
                "one GPU per worker in DDP mode and will fail "
                "if more are assigned."
            )
        if gpus_per_worker != int(gpus_per_worker):
            raise ValueError(
                f"You have assigned {gpus_per_worker} GPUs per worker, "
                "but fractional GPUs are not supported by HuggingFace."
            )

        super()._validate_attributes()

    def _convert_directory_checkpoint_to_sync_if_needed(
        self, checkpoint: Checkpoint
    ) -> Checkpoint:
        """Replace the directory checkpoint with a node ip & path dict checkpoint.

        This dict checkpoint will be used to sync the directory.
        If we were to use a directory checkpoint directly, it would get deepcopied &
        serialized unnecessarily."""
        with checkpoint.as_directory() as checkpoint_path:
            # Load checkpoint from path.
            checkpoint_path = Path(checkpoint_path).expanduser().absolute()
            if not checkpoint_path.joinpath(TUNE_CHECKPOINT_ID).exists():
                # If the ID file is missing, we assume that this is already
                # a sync checkpoint
                dict_checkpoint = checkpoint.to_dict()
                if (
                    NODE_IP_KEY not in dict_checkpoint
                    or CHECKPOINT_PATH_ON_NODE_KEY not in dict_checkpoint
                ):
                    raise ValueError(
                        "Wrong checkpoint format. Ensure the checkpoint is a "
                        "result of `HuggingFaceTrainer`."
                    )
                return checkpoint
            with open(checkpoint_path.joinpath(TUNE_CHECKPOINT_ID), "r") as f:
                tune_checkpoint_id = int(f.read())

            return Checkpoint.from_dict(
                {
                    NODE_IP_KEY: get_node_ip_address(),
                    CHECKPOINT_PATH_ON_NODE_KEY: str(checkpoint_path),
                    TUNE_CHECKPOINT_ID: tune_checkpoint_id,
                }
            )

    def setup(self) -> None:
        if self.resume_from_checkpoint:
            self.resume_from_checkpoint = (
                self._convert_directory_checkpoint_to_sync_if_needed(
                    self.resume_from_checkpoint
                )
            )

    def as_trainable(self) -> Type[Trainable]:
        original_param_dict = self._param_dict.copy()
        resume_from_checkpoint: Optional[Checkpoint] = self._param_dict.get(
            "resume_from_checkpoint", None
        )
        if resume_from_checkpoint:
            self._param_dict[
                "resume_from_checkpoint"
            ] = self._convert_directory_checkpoint_to_sync_if_needed(
                resume_from_checkpoint
            )
        try:
            ret = super().as_trainable()
        finally:
            self._param_dict = original_param_dict
        return ret


def _huggingface_train_loop_per_worker(config):
    """Per-worker training loop for HuggingFace Transformers."""
    trainer_init_per_worker = config.pop("_trainer_init_per_worker")

    # Env vars necessary for HF to setup DDP
    os.environ["RANK"] = str(train.world_rank())
    os.environ["WORLD_SIZE"] = str(train.world_size())
    os.environ["LOCAL_RANK"] = str(train.local_rank())

    train_dataset = train.get_dataset_shard(TRAIN_DATASET_KEY)
    eval_dataset = train.get_dataset_shard(EVALUATION_DATASET_KEY)

    train_torch_dataset, eval_torch_dataset = process_datasets(
        train_dataset,
        eval_dataset,
    )

    trainer: transformers.trainer.Trainer = trainer_init_per_worker(
        train_torch_dataset, eval_torch_dataset, **config
    )

    if trainer.args.push_to_hub and not trainer.args.hub_token:
        warnings.warn(
            "You have set `push_to_hub=True` but didn't specify `hub_token`. "
            "Pushing to hub will most likely fail, as the credentials will not "
            "be automatically propagated from the local enviroment to the Ray Actors. "
            "If that happens, specify `hub_token` in `TrainingArguments`."
        )

    if (
        trainer.args.evaluation_strategy == "steps"
        or trainer.args.save_strategy == "steps"
        or trainer.args.logging_strategy == "steps"
    ):
        raise ValueError(
            "'steps' value for `evaluation_strategy`, `logging_strategy` "
            "or `save_strategy` is not yet supported."
        )

    trainer = wrap_transformers_trainer(trainer)

    # ensure no HF logging callbacks are added
    # aside from doubling functionality with our callbacks,
    # the Wandb callbacks causes training to freeze
    integration_callbacks = transformers.trainer.get_reporting_integration_callbacks(
        trainer.args.report_to
    )
    for callback in integration_callbacks:
        trainer.pop_callback(callback)

    trainer.add_callback(TrainReportCallback)

    checkpoint = session.get_checkpoint()
    checkpoint_path = None
    remove_checkpoint_path = False
    if checkpoint:
        assert isinstance(checkpoint, Checkpoint)
        checkpoint_dict = checkpoint.to_dict()
        source_ip = checkpoint_dict[NODE_IP_KEY]
        source_path = checkpoint_dict[CHECKPOINT_PATH_ON_NODE_KEY]
        target_ip = get_node_ip_address()
        if source_ip == target_ip:
            checkpoint_path = source_path
        else:
            checkpoint_path = tempfile.mkdtemp(
                suffix=Path(trainer.args.output_dir).name
            )
            remove_checkpoint_path = True
            sync_dir_between_nodes(
                source_ip=source_ip,
                source_path=source_path,
                target_ip=target_ip,
                target_path=checkpoint_path,
                return_futures=False,
                max_size_bytes=None,
            )
    trainer.train(resume_from_checkpoint=checkpoint_path)
    if remove_checkpoint_path:
        shutil.rmtree(checkpoint_path, ignore_errors=True)
