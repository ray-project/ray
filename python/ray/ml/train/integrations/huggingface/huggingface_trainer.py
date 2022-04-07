from typing import Any, Callable, List, Optional, Dict, Type
import os
import inspect
from unittest.mock import patch

import torch
import transformers.trainer
from transformers.training_args import TrainingArguments
from transformers.trainer_callback import TrainerCallback
from torch.utils.data import Dataset as TorchDataset, IterableDataset, DataLoader


from ray import train
from ray.data.dataset import Dataset
from ray.train.torch import TorchConfig
from ray.train.session import get_session
from ray.ml.trainer import GenDataset
from ray.ml.train.integrations.torch import TorchTrainer
from ray.ml.config import ScalingConfig, RunConfig
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.util import PublicAPI
from ray.ml.constants import TRAIN_DATASET_KEY, EVALUATION_DATASET_KEY


class _HFIterableDatasetWithLen(IterableDataset):
    def __init__(self, generator, length: int):
        self.generator = generator
        self._len = length

    def __iter__(self):
        it = self.generator
        for x in it:
            yield {**x[0], "labels": x[1]}

    def __len__(self):
        return self._len


class _TrainReportCallback(TrainerCallback):
    def on_log(self, args, state, control, model=None, logs=None, **kwargs):
        train.report(**{**logs, "step": state.global_step, "epoch": state.epoch})


def _process_dataset_for_hf(
    dataset: Dataset, feature_columns: Dict[str, List[str]], batch_size: int = 1
) -> IterableDataset:
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
    argument in ``TrainingArguments`` will be automatically set.

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
        trainer_init_config: Configurations to pass into
            ``trainer_init_per_worker`` as kwargs.
        torch_config: Configuration for setting up the PyTorch backend. If set to
            None, use the default configuration. This replaces the ``backend_config``
            arg of ``DataParallelTrainer``. Same as in ``TorchTrainer``.
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        datasets: Any Ray Datasets to use for training. Use
            the key "train" to denote which dataset is the training
            dataset and (optionally) key "evaluation" to denote the evaluation
            dataset. Can only contain a training dataset
            and up to one extra dataset to be used for evaluation.
            If a ``preprocessor`` is provided and has not already been fit,
            it will be fit on the training dataset. All datasets will be
            transformed by the ``preprocessor`` if one is provided.
        preprocessor: A ray.ml.preprocessor.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    def __init__(
        self,
        *,
        trainer_init_per_worker: Callable[
            [TorchDataset, Optional[TorchDataset], Any], transformers.trainer.Trainer
        ],
        trainer_init_config: Optional[Dict] = None,
        torch_config: Optional[TorchConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):

        self._validate_train_loop_per_worker(
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

    def _validate_train_loop_per_worker(
        self, train_loop_per_worker: Callable, fn_name: str
    ) -> None:
        num_params = len(inspect.signature(train_loop_per_worker).parameters)
        if num_params != 3:
            raise ValueError(
                f"{fn_name} should take in 3 arguments, "
                f"but it accepts {num_params} arguments instead."
            )

    def _create_train_func(
        self,
        trainer_init_per_worker: Callable[
            [TorchDataset, Optional[TorchDataset], Any], transformers.trainer.Trainer
        ],
    ):
        def train_loop_per_worker(config):
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
            with patch(
                "transformers.trainer.get_reporting_integration_callbacks", lambda x: []
            ):
                trainer: transformers.trainer.Trainer = trainer_init_per_worker(
                    train_torch_dataset, eval_torch_dataset, **config
                )

            if not trainer.args.local_rank == train.local_rank():
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

            trainer.__class__ = RayTrainer
            trainer.args.__class__ = RayTrainingArguments
            trainer.add_callback(_TrainReportCallback)
            if trainer.args.device.type == "cuda":
                torch.cuda.set_device(trainer.args.device)
            trainer.train()

        return train_loop_per_worker
