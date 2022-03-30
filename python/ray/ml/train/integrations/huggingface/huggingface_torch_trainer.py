from typing import Any, Callable, Optional, Dict, Union, Type

from transformers.trainer import Trainer

from ray.train.torch import TorchConfig
from ray.ml.trainer import GenDataset
from ray.ml.train.integrations.torch import TorchTrainer
from ray.ml.config import ScalingConfig, RunConfig
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.util import PublicAPI


def _huggingface_train_loop_per_worker(config: Dict[str, Any]):
    config
    return


@PublicAPI(stability="alpha")
class HuggingFaceTorchTrainer(TorchTrainer):
    """A Trainer for data parallel HuggingFace Transformers on PyTorch training.

    This Trainer runs the ``transformers.Trainer.train()`` method on multiple
    Ray Actors. The training is carried out in a distributed fashion through PyTorch
    DDP. These actors already have the necessary torch process group already
    configured for distributed pytorch training.

    The training function ran on every Actor will first initialize a
    ``transformers.TrainingArguments`` object using the ``args`` dict,
    and then use it alongside ``trainer_kwargs`` to initialize an
    object with a ``trainer_class`` class (by default, this is
    ``transformers.Trainer``) and run ``train()``, using the
    provided datasets.

    It is important to ensure that all contents of ``args`` and
    ``trainer_kwargs`` are serializable by Ray. In case you have
    arguments that are not serializable, you can specify a
    ``pre_init_function`` taking in the ``args`` and
    ``trainer_kwargs`` dicts to modify them in-place on every
    Actor separately. This can be used to eg. obtain a Transformers
    model from hub.

    If the ``datasets`` dict contains a training dataset (denoted by
    the "train" key), then it will be split into multiple dataset
    shards, with each Actor training on a single shard.
    All the other datasets will not be split.

    Example:
        .. code-block:: python
            # Based on
            # huggingface/notebooks/examples/language_modeling_from_scratch.ipynb

            # Hugging Face imports
            from datasets import load_dataset
            from transformers import AutoConfig, AutoModelForCausalLM, AutoTokenizer

            import ray
            from ray.ml.train.integrations.huggingface import HuggingFaceTorchTrainer

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
            ray_validation_ds = ray.data.from_arrow(
                lm_datasets["validation"]._data.table
            )

            def pre_init_function(args, trainer_kwargs):
                model_config = AutoConfig.from_pretrained(model_checkpoint)
                model = AutoModelForCausalLM.from_config(model_config)
                trainer_kwargs["model"] = model

            scaling_config = {"num_workers": 3}
            # If using GPUs, use the below scaling config instead.
            # scaling_config = {"num_workers": 3, "use_gpu": True}
            trainer = HuggingFaceTorchTrainer(
                args={
                    "output_dir": f"{model_checkpoint}-wikitext2",
                    "evaluation_strategy": "epoch",
                    "learning_rate": 2e-5,
                    "weight_decay": 0.01,
                },
                pre_init_function=pre_init_function,
                scaling_config=scaling_config,
                datasets={"train": ray_train_ds, "validation": ray_validation_ds},
            )
            result = trainer.fit()

    Args:
        trainer_class: A subclass of ``transformers.Trainer`` to use.
            Defaults to ``transformers.Trainer``.
        args: Keyword arguments passed to ``transformers.TrainingArguments``
            during initialization on every Actor.
        pre_init_function: A function taking in the ``args`` and ``trainer_kwargs``
            dicts to modify them in-place on every Actor before ``trainer_class``
            object is initialized.
        torch_config: Configuration for setting up the PyTorch backend. If set to
            None, use the default configuration. This replaces the ``backend_config``
            arg of ``DataParallelTrainer``. Same as in ``TorchTrainer``.
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        datasets: Any Ray Datasets to use for training. Use
            the key "train" to denote which dataset is the training
            dataset. Can only contain a training dataset
            and up to one extra dataset to be used for validation.
            If a ``preprocessor`` is provided and has not already been fit,
            it will be fit on the training dataset. All datasets will be
            transformed by the ``preprocessor`` if one is provided.
        preprocessor: A ray.ml.preprocessor.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
        **trainer_kwargs: Additional kwargs to pass to ``trainer_class``
            object during initailization on every Actor.
    """

    def __init__(
        self,
        trainer_class: Type[Trainer] = Trainer,
        args: Optional[Dict[str, Any]] = None,
        pre_init_function: Optional[
            Callable[[Dict[str, Any], Dict[str, Any]], None]
        ] = None,
        torch_config: Optional[TorchConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
        **trainer_kwargs
    ):
        # Will improve during implementation
        assert "local_rank" not in args
        assert "no_cuda" not in args
        assert "train_dataset" not in trainer_kwargs
        assert "eval_dataset" not in trainer_kwargs

        self.trainer_class = trainer_class
        self.args = args
        self.trainer_kwargs = trainer_kwargs
        self.pre_init_function = pre_init_function

        super().__init__(
            None,
            None,
            torch_config,
            scaling_config,
            run_config,
            datasets,
            preprocessor,
            resume_from_checkpoint,
        )

    def _validate_train_loop_per_worker(self):
        return

    @property
    def train_loop_per_worker(
        self,
    ) -> Union[Callable[[], None], Callable[[Dict], None]]:
        return _huggingface_train_loop_per_worker

    @train_loop_per_worker.setter
    def train_loop_per_worker(
        self, val: Union[Callable[[], None], Callable[[Dict], None]]
    ):
        pass

    @property
    def train_loop_config(self) -> Optional[Dict]:
        return {
            "trainer_class": self.trainer_class,
            "args": self.args,
            "trainer_kwargs": self.trainer_kwargs,
            "pre_init_function": self.pre_init_function,
        }

    @train_loop_config.setter
    def train_loop_config(self, val: Optional[Dict]):
        pass
