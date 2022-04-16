from typing import Any, Callable, Optional, Dict

from transformers.trainer import Trainer
from torch.utils.data import Dataset as TorchDataset

from ray.train.torch import TorchConfig
from ray.ml.trainer import GenDataset
from ray.ml.train.integrations.torch import TorchTrainer
from ray.ml.config import ScalingConfig, RunConfig
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.util import PublicAPI
from ray.ml.constants import TRAIN_DATASET_KEY, EVALUATION_DATASET_KEY


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
        trainer_init_per_worker: Callable[
            [TorchDataset, Optional[TorchDataset], Any], Trainer
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

        assert TRAIN_DATASET_KEY in datasets
        assert all(
            key in (TRAIN_DATASET_KEY, EVALUATION_DATASET_KEY) for key in datasets
        )

        super().__init__(
            self._create_train_func(trainer_init_per_worker),
            trainer_init_config,
            torch_config,
            scaling_config,
            run_config,
            datasets,
            preprocessor,
            resume_from_checkpoint,
        )

    def _create_train_func(self, trainer_init_per_worker):
        def train_loop_per_worker(config):
            # Set to None just to make CI pass & show
            # the intended usage with trainer_init_per_worker
            train_dataset = None
            eval_dataset = None
            trainer = trainer_init_per_worker(train_dataset, eval_dataset, **config)
            trainer.train()

        return train_loop_per_worker
