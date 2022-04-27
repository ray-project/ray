from typing import Dict, Optional, Type, Union, List

import numpy as np
import pandas as pd

import torch
from datasets import Dataset as HFDataset
from transformers.modeling_utils import PreTrainedModel
from transformers.trainer import Trainer as HFTrainer
from transformers import TrainingArguments

from ray.ml.predictor import DataBatchType, Predictor
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.utils.huggingface_checkpoint_utils import load_huggingface_checkpoint


class HuggingFacePredictor(Predictor):
    """A predictor for HuggingFace Transformers PyTorch models.

    Args:
        model: The Transformers model to use for predictions.
        preprocessor: A preprocessor used to transform data batches prior
            to prediction.
        training_args: ``transformers.TrainingArguments`` to use for the prediction.
        trainer_class: ``transformers.Trainer`` subclass to use for prediction.
            Defaults to ``transformers.Trainer``.
    """

    def __init__(
        self,
        model: Union[PreTrainedModel, torch.nn.Module],
        preprocessor: Optional[Preprocessor] = None,
        *,
        training_args: Optional[TrainingArguments] = None,
        trainer_class: HFTrainer = HFTrainer,
    ):
        self.model = model
        self.preprocessor = preprocessor
        self.training_args = training_args
        self.trainer_class = trainer_class

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: Checkpoint,
        model: Union[Type[PreTrainedModel], torch.nn.Module],
        *,
        training_args: Optional[TrainingArguments] = None,
        trainer_class: HFTrainer = HFTrainer,
        **pretrained_model_kwargs,
    ) -> "HuggingFacePredictor":
        """Instantiate the predictor from a Checkpoint.

        The checkpoint is expected to be a result of ``HuggingFaceTrainer``.

        Args:
            checkpoint: The checkpoint to load the model and
                preprocessor from. It is expected to be from the result of a
                ``HuggingFaceTrainer`` run.
            model: Either a ``transformers.PreTrainedModel`` class
                (eg. ``AutoModelForCausalLM``), or a PyTorch model to load the
                weights to. This should be the same model used for training.
            training_args: ``transformers.TrainingArguments`` to use for the prediction.
                Defaults to training arguments saved inside the checkpoint.
            trainer_class: ``transformers.Trainer`` subclass to use for prediction.
                Defaults to ``transformers.Trainer``.
            **pretrained_model_kwargs: Any kwargs to pass to the
                ``model.from_pretrained()`` call. Only used if
                ``model`` is a ``PreTrainerModel`` class.
        """
        model, preprocessor, loaded_training_args = load_huggingface_checkpoint(
            checkpoint, model, **pretrained_model_kwargs
        )
        training_args = training_args or loaded_training_args
        return HuggingFacePredictor(
            model=model,
            preprocessor=preprocessor,
            training_args=training_args,
            trainer_class=trainer_class,
        )

    def to_transformers_trainer(self, **trainer_kwargs) -> HFTrainer:
        """Converts this predictor to a ``transformers.Trainer``.

        Args:
            **trainer_kwargs: Any kwargs to pass to the
                ``trainer_class`` initialization. ``model`` and
                ``args`` are preset.
        """
        if self.training_args:
            self.training_args.local_rank = -1
        trainer = self.trainer_class(
            model=self.model, args=self.training_args, **trainer_kwargs
        )
        return trainer

    def _predict(self, dataset: HFDataset) -> pd.DataFrame:
        trainer = self.to_transformers_trainer()
        ret = trainer.predict(dataset).predictions
        # TODO(yard1): Return just a numpy array once that's supported
        # by Ray Datasets
        df = pd.DataFrame([ret.tolist()]).T
        df.columns = ["predictions"]
        return df

    def predict(
        self,
        data: DataBatchType,
        feature_columns: Optional[List[str]] = None,
        dtype: Optional[Union[Dict[str, np.dtype], np.dtype]] = None,
    ) -> DataBatchType:
        """Run inference on data batch.

        The data is converted into a HuggingFace ``datasets.Dataset``
        and passed to a ``transformers.Trainer.predict()`` method.

        Args:
            data: A batch of input data. Either a pandas DataFrame or numpy
                array.
            feature_columns: The names or indices of the columns in the
                data to use as features to predict on. If None, use all
                columns.
            dtype: The numpy dtypes to cast the data to.
                Can be either a single dtype or a dict of ``column:dtype``.
                If set to None, then automatically infer the dtype.

        Examples:

        .. code-block:: python

            import numpy as np
            from datasets import load_dataset
            from transformers import AutoConfig, AutoModelForCausalLM, AutoTokenizer
            from ray.ml.predictors.huggingface import HuggingFacePredictor

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
            model_config = AutoConfig.from_pretrained(model_checkpoint)
            model = AutoModelForCausalLM.from_config(model_config)
            predictor = HuggingFacePredictor(
                model=model, preprocessor=preprocessor
            )

            predictions = predictor.predict(lm_datasets["validation"].to_pandas())


        Returns:
            DataBatchType: Prediction result.
        """
        if self.preprocessor:
            data = self.preprocessor.transform_batch(data)

        if isinstance(data, np.ndarray):
            # If numpy array, then convert to pandas dataframe.
            data = pd.DataFrame(data)

        data = data[feature_columns] if feature_columns else data
        if dtype:
            data = data.astype(dtype)

        dataset = HFDataset.from_pandas(data)
        return self._predict(dataset)
