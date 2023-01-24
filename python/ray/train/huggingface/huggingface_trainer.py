import importlib.util
import inspect
import os
import sys
import warnings
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Type

try:
    from packaging.version import Version
except ImportError:
    from distutils.version import LooseVersion as Version


import transformers
import transformers.modeling_utils
import transformers.trainer
import transformers.training_args
from transformers.trainer_utils import IntervalStrategy
from transformers.utils import is_datasets_available
from torch.utils.data import Dataset as TorchDataset

from ray.air import session
from ray.air.checkpoint import Checkpoint
from ray.air.config import DatasetConfig, RunConfig, ScalingConfig
from ray.train.constants import (
    EVALUATION_DATASET_KEY,
    TRAIN_DATASET_KEY,
)
from ray.train.huggingface._huggingface_utils import (
    TrainReportCallback,
    process_datasets,
    wrap_transformers_trainer,
)
from ray.train.torch import TorchConfig, TorchTrainer
from ray.train.trainer import GenDataset
from ray.util import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor

# Due to HF Dataset's dynamic module system, we need to dynamically import the
# datasets_modules module on every actor when training.
# We accomplish this by simply running the following bit of code directly
# in module you are currently viewing. This ensures that when we
# unpickle the HuggingFaceTrainer, it will be ran before pickle tries to
# import datasets_modules and prevents an exception from being thrown.
# Same logic is present inside HF Transformers Ray integration:
# https://github.com/huggingface/transformers/blob/\
# 7d5fde991d598370d961be8cb7add6541e2b59ce/src/transformers/integrations.py#L271
# Also see https://github.com/ray-project/ray/issues/28084
if "datasets_modules" not in sys.modules and is_datasets_available():
    import datasets.load

    dynamic_modules_path = os.path.join(
        datasets.load.init_dynamic_modules(), "__init__.py"
    )
    # load dynamic_modules from path
    spec = importlib.util.spec_from_file_location(
        "datasets_modules", dynamic_modules_path
    )
    datasets_modules = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = datasets_modules
    spec.loader.exec_module(datasets_modules)

# This trainer uses a special checkpoint syncing logic.
# Because HF checkpoints are very large dirs (at least several GBs),
# we use directory checkpoints that are synced between nodes when
# required instead of serializing the checkpoints and sending
# bytes over nodes. This is a much more performant solution for
# large directory checkpoints. The current implementation
# is special for HuggingFaceTrainer, but can and should be
# made generic.
# TODO(ml-team): Make dir syncing checkpoint logic generic.


@PublicAPI(stability="alpha")
class HuggingFaceTrainer(TorchTrainer):
    """A Trainer for data parallel HuggingFace Transformers on PyTorch training.

    This Trainer runs the ``transformers.Trainer.train()`` method on multiple
    Ray Actors. The training is carried out in a distributed fashion through PyTorch
    DDP. These actors already have the necessary torch process group already
    configured for distributed PyTorch training. If you have PyTorch >= 1.12.0
    installed, you can also run FSDP training by specifying the ``fsdp`` argument
    in ``TrainingArguments``. For more information on configuring FSDP,
    refer to `Hugging Face documentation <https://huggingface.co/docs/transformers/\
main/en/main_classes/trainer#transformers.TrainingArguments>`__.

    The training function ran on every Actor will first run the
    specified ``trainer_init_per_worker`` function to obtain an instantiated
    ``transformers.Trainer`` object. The ``trainer_init_per_worker`` function
    will have access to preprocessed train and evaluation datasets.

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
    (segfault) may be thrown.

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
            from ray.air.config import ScalingConfig

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
                lm_datasets["validation"]
            )

            def trainer_init_per_worker(train_dataset, eval_dataset, **config):
                model_config = AutoConfig.from_pretrained(model_checkpoint)
                model = AutoModelForCausalLM.from_config(model_config)
                args = transformers.TrainingArguments(
                    output_dir=f"{model_checkpoint}-wikitext2",
                    evaluation_strategy="epoch",
                    save_strategy="epoch",
                    logging_strategy="epoch",
                    learning_rate=2e-5,
                    weight_decay=0.01,
                )
                return transformers.Trainer(
                    model=model,
                    args=args,
                    train_dataset=train_dataset,
                    eval_dataset=eval_dataset,
                )

            scaling_config = ScalingConfig(num_workers=3)
            # If using GPUs, use the below scaling config instead.
            # scaling_config = ScalingConfig(num_workers=3, use_gpu=True)
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

    _dataset_config = {
        # training dataset should be split by us
        "train": DatasetConfig(fit=True, split=True, required=True),
        # do not split eval dataset, as HF has a system to parallelize
        # evaluation across workers, and it requires each worker
        # to have the full eval dataset
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
        if Version(transformers.__version__) < Version("4.19.0"):
            raise RuntimeError(
                "HuggingFaceTrainer requires transformers>=4.19.0, but you "
                f"have {transformers.__version__} which is incompatible. "
                "Update on all nodes with `pip install -U 'transformers>=4.19.0'`."
            )

        self._validate_trainer_init_per_worker(
            trainer_init_per_worker, "trainer_init_per_worker"
        )

        super().__init__(
            train_loop_per_worker=_huggingface_train_loop_per_worker,
            train_loop_config=self._create_trainer_init_config(
                trainer_init_per_worker, trainer_init_config
            ),
            torch_config=torch_config,
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    @classmethod
    def _create_trainer_init_config(
        cls,
        trainer_init_per_worker: Callable[
            [TorchDataset, Optional[TorchDataset], Any],
            transformers.trainer.Trainer,
        ],
        trainer_init_config: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        trainer_init_config = trainer_init_config.copy() if trainer_init_config else {}
        if "_trainer_init_per_worker" in trainer_init_config:
            raise ValueError(
                "'_trainer_init_per_worker' is a reserved key in `trainer_init_config`."
            )
        trainer_init_config["_trainer_init_per_worker"] = trainer_init_per_worker
        return trainer_init_config

    @classmethod
    def restore(
        cls: Type["HuggingFaceTrainer"],
        path: str,
        trainer_init_per_worker: Optional[
            Callable[
                [TorchDataset, Optional[TorchDataset], Any],
                transformers.trainer.Trainer,
            ]
        ] = None,
        trainer_init_config: Optional[Dict] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional["Preprocessor"] = None,
        scaling_config: Optional[ScalingConfig] = None,
    ) -> "HuggingFaceTrainer":
        train_loop_config = (
            cls._create_trainer_init_config(
                trainer_init_per_worker, trainer_init_config
            )
            if trainer_init_config
            else None
        )

        return super(HuggingFaceTrainer, cls).restore(
            train_loop_per_worker=_huggingface_train_loop_per_worker,
            train_loop_config=train_loop_config,
            path=path,
            datasets=datasets,
            preprocessor=preprocessor,
            scaling_config=scaling_config,
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
        gpus_per_worker = self.scaling_config.num_gpus_per_worker
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


def _huggingface_train_loop_per_worker(config):
    """Per-worker training loop for HuggingFace Transformers."""
    trainer_init_per_worker = config.pop("_trainer_init_per_worker")

    # Env vars necessary for HF to setup DDP
    os.environ["RANK"] = str(session.get_world_rank())
    os.environ["WORLD_SIZE"] = str(session.get_world_size())
    os.environ["LOCAL_RANK"] = str(session.get_local_rank())

    train_dataset = session.get_dataset_shard(TRAIN_DATASET_KEY)
    eval_dataset = session.get_dataset_shard(EVALUATION_DATASET_KEY)

    train_torch_dataset, eval_torch_dataset = process_datasets(
        train_dataset,
        eval_dataset,
    )

    trainer: transformers.trainer.Trainer = trainer_init_per_worker(
        train_torch_dataset, eval_torch_dataset, **config
    )

    strategies = [
        strategy
        for strategy in (trainer.args.evaluation_strategy, trainer.args.save_strategy)
        if strategy not in ("no", IntervalStrategy.NO)
    ]
    strategies = [trainer.args.logging_strategy] + strategies
    if not all(strategy == strategies[0] for strategy in strategies[1:]):
        raise ValueError(
            "When using Ray AIR,`logging_strategy`, `evaluation_strategy` "
            "and `save_strategy` must all be set to the same value. "
            "`evaluation_strategy` or `save_strategy` may also be set to 'no'.\n"
            f"Got `logging_strategy`={trainer.args.logging_strategy}\n"
            f"`evaluation_strategy`={trainer.args.evaluation_strategy}\n"
            f"`save_strategy`={trainer.args.save_strategy}"
        )

    if trainer.args.save_strategy in ("steps", IntervalStrategy.STEPS):
        if (
            trainer.args.save_steps < trainer.args.logging_steps
            or trainer.args.save_steps % trainer.args.logging_steps != 0
        ):
            raise ValueError(
                "When using 'steps' `save_strategy`, `save_steps` must be "
                "equal or bigger to `logging_steps`, and must be divisible "
                "by `logging_steps` (so that saving occurs at the same time "
                f"logging does). Got `save_steps`={trainer.args.save_steps}, "
                f"`logging_steps`={trainer.args.logging_steps}."
            )

    if trainer.args.evaluation_strategy in ("steps", IntervalStrategy.STEPS):
        if trainer.args.logging_steps != trainer.args.eval_steps:
            raise ValueError(
                "`logging_steps` must be equal to `eval_steps`. "
                f"Got `logging_steps`={trainer.args.logging_steps}, "
                f"`eval_steps`={trainer.args.eval_steps}"
            )

    if trainer.args.load_best_model_at_end:
        raise ValueError(
            "As Ray AIR replaces Hugging Face checkpointing, "
            "`load_best_model_at_end` must be set to False.\n"
            "You can obtain the AIR Checkpoint with "
            "`Result.checkpoint` returned by the `fit()` method "
            "of this Trainer, and the model itself by calling "
            "`Checkpoint.get_model()`.\n"
            "You can configure the checkpointing by setting "
            "`run_config.checkpoint_config`."
        )

    if trainer.args.push_to_hub and not trainer.args.hub_token:
        warnings.warn(
            "You have set `push_to_hub=True` but didn't specify `hub_token`. "
            "Pushing to hub will most likely fail, as the credentials will not "
            "be automatically propagated from the local enviroment to the Ray Actors. "
            "If that happens, specify `hub_token` in `TrainingArguments`."
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
    if checkpoint:
        with checkpoint.as_directory() as checkpoint_path:
            trainer.train(resume_from_checkpoint=checkpoint_path)
    else:
        trainer.train()
