import gc
import inspect
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, Generator, Iterator, List, Optional, Type
from unittest.mock import patch

import torch
import transformers.trainer
import ray.cloudpickle as cpickle
from torch.utils.data import DataLoader
from torch.utils.data import Dataset as TorchDataset
from torch.utils.data import IterableDataset
from transformers.trainer_callback import TrainerCallback
from transformers.training_args import TrainingArguments

from ray import train, tune
from ray.util import PublicAPI, get_node_ip_address
from ray.data.dataset import Dataset
from ray.ml.checkpoint import Checkpoint
from ray.ml.config import RunConfig, ScalingConfig
from ray.ml.constants import EVALUATION_DATASET_KEY, PREPROCESSOR_KEY, TRAIN_DATASET_KEY
from ray.ml.preprocessor import Preprocessor
from ray.ml.train.integrations.torch import TorchTrainer
from ray.ml.trainer import GenDataset
from ray.train.checkpoint import TuneCheckpointManager
from ray.train.constants import TUNE_CHECKPOINT_ID
from ray.train.session import get_session
from ray.train.torch import TorchConfig
from ray.tune.utils.file_transfer import delete_on_node, sync_dir_between_nodes


# This trainer uses a special checkpoint syncing logic.
# Because HF checkpoints are very large dirs (at least several GBs),
# we use directory checkpoints that are synced between nodes when
# required instead of serializing the checkpoints and sending
# bytes over nodes. This is a much more performant solution for
# large directory checkpoints. The current implementation
# is special for HuggingFaceTrainer, but can and should be
# made generic.
# TODO(ml-team): Make dir syncing checkpoint logic generic.

NODE_IP_KEY = "node_ip"
CHECKPOINT_PATH_ON_NODE_KEY = "checkpoint_path_on_node"


# TODO(team-ml): Refactor checkpoint management along with Tune.
class _DataParallelSyncingCheckpointManager(TuneCheckpointManager):
    """Same as _DataParallelCheckpointManager, but syncs the dir instead
    of serializing it."""

    def add_tune_checkpoint_id(self, path: str):
        # Store the checkpoint_id in the file so that the Tune trial can be
        # resumed after failure or cancellation.
        with open(Path(path).joinpath(TUNE_CHECKPOINT_ID), "w") as f:
            f.write(str(self._latest_checkpoint_id))

    def on_init(self, preprocessor: Preprocessor):
        self.preprocessor = preprocessor
        super(_DataParallelSyncingCheckpointManager, self).on_init()

    def write_checkpoint(self, checkpoint: Dict):
        # If inside a Tune Trainable, then checkpoint with Tune.
        with tune.checkpoint_dir(step=self._latest_checkpoint_id) as checkpoint_dir:
            source_ip = checkpoint[NODE_IP_KEY]
            source_path = checkpoint[CHECKPOINT_PATH_ON_NODE_KEY]
            target_ip = get_node_ip_address()
            if source_ip == target_ip:
                # Move contents of source_path, but not source_path
                # itself. shutil.move is already recursive.
                for path in Path(source_path).iterdir():
                    shutil.move(str(path.absolute()), checkpoint_dir)
                shutil.rmtree(source_path, ignore_errors=True)
            else:
                sync_dir_between_nodes(
                    source_ip=source_ip,
                    source_path=source_path,
                    target_ip=target_ip,
                    target_path=checkpoint_dir,
                    return_futures=False,
                    max_size_bytes=None,
                )
                delete_on_node(node_ip=source_ip, path=source_path)
            with open(Path(checkpoint_dir).joinpath(PREPROCESSOR_KEY), "wb") as f:
                cpickle.dump(self.preprocessor, f)
            self.add_tune_checkpoint_id(checkpoint_dir)

    @property
    def latest_checkpoint_dir(self) -> Optional[Path]:
        raise NotImplementedError


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
    the ``get_train_dataloader`` method will be overriden to disable distributed
    sampling, as the dataset will already be sharded.

    Hugging Face loggers will be automatically disabled, and the ``local_rank``
    argument in ``TrainingArguments`` will be automatically set. Please note
    that if you want to use CPU training, you will need to set the ``no_cuda``
    argument in ``TrainingArguments`` manually - otherwise, an exception
    may be thrown.

    Example:
        .. code-block:: python
            # Based on
            # huggingface/notebooks/examples/language_modeling_from_scratch.ipynb

            # Hugging Face imports
            from datasets import load_dataset
            import transformers
            from transformers import AutoConfig, AutoModelForCausalLM, AutoTokenizer

            import ray
            from ray.ml.train.integrations.huggingface import HuggingFaceTrainer

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
            ray_train_ds = ray.data.from_arrow(lm_datasets["train"]._data.table)
            ray_evaluation_ds = ray.data.from_arrow(
                lm_datasets["evaluation"]._data.table
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
        run_config: Configuration for the execution of the training run.
        preprocessor: A ray.ml.preprocessor.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    _checkpoint_manager_cls: Type[
        TuneCheckpointManager
    ] = _DataParallelSyncingCheckpointManager

    def __init__(
        self,
        *,
        trainer_init_per_worker: Callable[
            [TorchDataset, Optional[TorchDataset], Any], transformers.trainer.Trainer
        ],
        datasets: Dict[str, GenDataset],
        trainer_init_config: Optional[Dict] = None,
        torch_config: Optional[TorchConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):

        self._validate_trainer_init_per_worker(
            trainer_init_per_worker, "trainer_init_per_worker"
        )

        if TRAIN_DATASET_KEY not in datasets:
            raise KeyError(
                f"'{TRAIN_DATASET_KEY}' key must be preset in `datasets`. "
                f"Got {list(self.datasets.keys())}"
            )
        if not all(
            key in (TRAIN_DATASET_KEY, EVALUATION_DATASET_KEY) for key in datasets
        ):
            raise KeyError(
                f"Only '{TRAIN_DATASET_KEY}' and '{EVALUATION_DATASET_KEY}' "
                "keys can be preset in `datasets`. "
                f"Got {list(self.datasets.keys())}"
            )

        super().__init__(
            train_loop_per_worker=self._create_train_func(trainer_init_per_worker),
            train_loop_config=trainer_init_config,
            torch_config=torch_config,
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    def __new__(cls, *args, **kwargs):
        """Store the init args as attributes so this can be merged with Tune hparams."""
        # This if will be entered in the driver-side Trainer.
        # The Trainer inside the trainable will have a dict
        # checkpoint created here.
        # This is required to ensure that the dir syncing logic
        # is used instead of serializing several gigabytes of data
        # when a Checkpoint is sent to a Ray Actor.
        if "resume_from_checkpoint" in kwargs:
            resume_from_checkpoint: Checkpoint = kwargs["resume_from_checkpoint"]
            (
                checkpoint_type,
                checkpoint_path,
            ) = resume_from_checkpoint.get_internal_representation()
            if checkpoint_type != "local_path":
                raise ValueError(
                    "Unexpected checkpoint type in `resume_from_checkpoint`. "
                    f"Expected 'local_path', got '{checkpoint_type}'"
                )
            if checkpoint_path:
                # Load checkpoint from path.
                checkpoint_path = Path(checkpoint_path).expanduser().absolute()
                if not checkpoint_path.exists():
                    raise ValueError(
                        f"Checkpoint path {checkpoint_path} does not exist."
                    )
                with open(checkpoint_path.joinpath(TUNE_CHECKPOINT_ID), "r") as f:
                    tune_checkpoint_id = int(f.read())

                kwargs["resume_from_checkpoint"] = Checkpoint.from_dict(
                    {
                        NODE_IP_KEY: get_node_ip_address(),
                        CHECKPOINT_PATH_ON_NODE_KEY: str(checkpoint_path),
                        TUNE_CHECKPOINT_ID: tune_checkpoint_id,
                    }
                )
        return super(HuggingFaceTrainer, cls).__new__(cls, *args, **kwargs)

    def _validate_trainer_init_per_worker(
        self, trainer_init_per_worker: Callable, fn_name: str
    ) -> None:
        num_params = len(inspect.signature(trainer_init_per_worker).parameters)
        if num_params < 3:
            raise ValueError(
                f"{fn_name} should take in at least 3 arguments, "
                f"but it accepts {num_params} arguments instead."
            )

    def _validate_train_loop_per_worker(
        self, train_loop_per_worker: Callable, fn_name: str
    ) -> None:
        # Do not validate train_loop_per_worker. We validate
        # trainer_init_per_worker instead.
        pass

    def _create_train_func(
        self,
        trainer_init_per_worker: Callable[
            [TorchDataset, Optional[TorchDataset], Any], transformers.trainer.Trainer
        ],
    ):
        def train_loop_per_worker(config):
            # Env vars necessary for HF to setup DDP
            os.environ["RANK"] = str(train.world_rank())
            os.environ["WORLD_SIZE"] = str(train.world_size())
            os.environ["LOCAL_RANK"] = str(train.local_rank())

            train_dataset = train.get_dataset_shard(TRAIN_DATASET_KEY)
            eval_dataset = train.get_dataset_shard(EVALUATION_DATASET_KEY)
            train_columns = set(train_dataset.schema(fetch_if_missing=True).names)
            if "labels" not in train_columns:
                raise ValueError(
                    "'labels' column must be present in the training dataset!"
                )
            train_columns.remove("labels")
            if eval_dataset:
                eval_columns = set(eval_dataset.schema(fetch_if_missing=True).names)
                if "labels" not in eval_columns:
                    raise ValueError(
                        "'labels' column must be present in the evaluation dataset!"
                    )
                eval_columns.remove("labels")

                if not eval_columns.issuperset(train_columns):
                    raise ValueError(
                        "Evaluation dataset must have a superset of the columns in "
                        "the training dataset. "
                        f"Missing columns: {list(train_columns - eval_columns)}"
                    )

            # HF-supported format
            feature_columns = {column: [column] for column in train_columns}

            # we use batch size 1 here, as it will be converted to
            # desired size inside transformers.Trainer. Possible optimization
            # in the future
            batch_size = 1
            train_torch_dataset = _process_dataset_for_hf(
                train_dataset, feature_columns, batch_size=batch_size
            )

            if eval_dataset:
                eval_torch_dataset = _process_dataset_for_hf(
                    eval_dataset, feature_columns, batch_size=batch_size
                )
            else:
                eval_torch_dataset = None

            # ensure no HF logging callbacks are added
            # aside from doubling functionality with our callbacks,
            # the Wandb callbacks causes training to freeze
            # TODO(yard1): Automatically set `no_cuda`
            with patch(
                "transformers.trainer.get_reporting_integration_callbacks", lambda x: []
            ):
                trainer: transformers.trainer.Trainer = trainer_init_per_worker(
                    train_torch_dataset, eval_torch_dataset, **config
                )

            if trainer.args.local_rank != train.local_rank():
                raise RuntimeError(
                    "local_rank set in TrainingArguments doesn't match "
                    "Ray Train local_rank "
                    f"({trainer.args.local_rank} != {train.local_rank()}. "
                    "Ensure you are not setting local_rank manually."
                )

            base_training_arguments_class: Type[
                TrainingArguments
            ] = trainer.args.__class__

            class RayTrainingArguments(base_training_arguments_class):
                @property
                def device(self) -> "torch.device":
                    if get_session() is None:
                        return super().device
                    return train.torch.get_device()

            base_trainer_class: Type[transformers.trainer.Trainer] = trainer.__class__

            class RayTrainer(base_trainer_class):
                def get_train_dataloader(self):
                    if get_session() is None:
                        return super().get_train_dataloader()
                    return DataLoader(
                        self.train_dataset,
                        batch_size=self.args.per_device_train_batch_size,
                        collate_fn=self.data_collator,
                        num_workers=self.args.dataloader_num_workers,
                        pin_memory=self.args.dataloader_pin_memory,
                    )

                def _wrap_model(self, model, training=True):
                    if get_session() is None:
                        return super()._wrap_model(model, training=training)

                    if not training:
                        return model
                    kwargs = {}
                    # same logic as in transformers.Trainer
                    if self.args.ddp_find_unused_parameters is not None:
                        kwargs[
                            "find_unused_parameters"
                        ] = self.args.ddp_find_unused_parameters
                    elif isinstance(model, transformers.trainer.PreTrainedModel):
                        # find_unused_parameters breaks checkpointing as per
                        # https://github.com/huggingface/transformers/pull/4659#issuecomment-643356021
                        kwargs[
                            "find_unused_parameters"
                        ] = not model.is_gradient_checkpointing
                    else:
                        kwargs["find_unused_parameters"] = True

                    if self.args.ddp_bucket_cap_mb is not None:
                        kwargs["bucket_cap_mb"] = self.args.ddp_bucket_cap_mb
                    return train.torch.prepare_model(model, ddp_kwargs=kwargs)

                def _save(self, *args, **kwargs):
                    # Workaround for RayTrainingArguments not being
                    # pickleable due to it being defined in a local
                    # scope
                    self.args.__class__ = base_training_arguments_class
                    ret = super()._save(*args, **kwargs)
                    self.args.__class__ = RayTrainingArguments
                    return ret

            trainer.__class__ = RayTrainer
            trainer.args.__class__ = RayTrainingArguments
            trainer.args.no_cuda = not torch.cuda.is_available()
            trainer.args.save_on_each_node = True
            trainer.add_callback(_TrainReportCallback)
            if trainer.args.device.type == "cuda":
                torch.cuda.set_device(trainer.args.device)

            checkpoint = train.load_checkpoint()
            checkpoint_path = None
            remove_checkpoint_path = False
            if checkpoint:
                source_ip = checkpoint[NODE_IP_KEY]
                source_path = checkpoint[CHECKPOINT_PATH_ON_NODE_KEY]
                target_ip = get_node_ip_address()
                if source_ip == target_ip:
                    checkpoint_path = source_path
                else:
                    # TODO(yard1): Confirm if tempdir is the right approach here.
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

        return train_loop_per_worker


class _HFIterableDatasetWithLen(IterableDataset):
    """Special Torch IterableDataset with preset length."""

    def __init__(self, generator: Generator, length: int):
        self.generator = generator
        self._len = length

    def __iter__(self) -> Iterator[Dict[str, torch.Tensor]]:
        it = self.generator
        for x in it:
            # HF-specific format
            yield {**x[0], "labels": x[1]}

    def __len__(self):
        return self._len


class _TrainReportCallback(TrainerCallback):
    """HF TrainerCallback for Ray Train metric reporting & checkpointing."""

    def __init__(self) -> None:
        # HF first logs metrics, and then checkpoints. With Ray AIR, we need the
        # opposite. Therefore, if we detect that a checkpoint will be created,
        # we delay the train.report call after the checkpoint is reported
        # to Ray Train.
        self.delayed_report = None
        # Avoid double reporting at the end.
        # TODO(yard1): Train statistics are only reported at the end. Combine
        # the second to last report and the last report somehow. We want
        # steps/epochs to match the training iteration.
        self.last_step = None
        super().__init__()

    def on_log(self, args, state, control, model=None, logs=None, **kwargs):
        if state.global_step == self.last_step:
            return
        self.last_step = state.global_step
        report = {**logs, "step": state.global_step, "epoch": state.epoch}
        if control.should_save:
            self.delayed_report = report
        else:
            train.report(**report)

    def on_save(self, args, state, control, **kwargs):
        checkpoint_path = Path(
            transformers.trainer.get_last_checkpoint(args.output_dir)
        ).absolute()
        if checkpoint_path:
            train.save_checkpoint(
                **{
                    NODE_IP_KEY: get_node_ip_address(),
                    CHECKPOINT_PATH_ON_NODE_KEY: str(checkpoint_path),
                }
            )
        if self.delayed_report:
            train.report(**self.delayed_report)
            self.delayed_report = None
        gc.collect()


def _process_dataset_for_hf(
    dataset: Dataset, feature_columns: Dict[str, List[str]], batch_size: int = 1
) -> IterableDataset:
    """Converts a Ray Dataset into a HF-compatible Torch Dataset."""
    torch_dataset = dataset.to_torch(
        batch_size=batch_size,
        feature_columns=feature_columns,
        label_column="labels",
        unsqueeze_label_tensor=False,
        unsqueeze_feature_tensors=False,
    )
    try:
        count = dataset.count()
    except ValueError:
        # pipeline case
        count = None
    if count:
        torch_dataset = _HFIterableDatasetWithLen(torch_dataset, count)
    return torch_dataset
