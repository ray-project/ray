import os
from typing import Dict, Optional, Type, Union, List

import pandas as pd
import numpy as np
from ray.ml.train.integrations.huggingface.huggingface_utils import (
    HFIterableDatasetWithLen,
)
import torch
from transformers.modeling_utils import PreTrainedModel
from transformers.trainer import WEIGHTS_NAME, TRAINING_ARGS_NAME, Trainer as HFTrainer
from transformers import TrainingArguments

import ray.cloudpickle as cpickle
from ray.ml.predictor import DataBatchType
from ray.ml.predictors.integrations.torch import TorchPredictor
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.utils.torch_utils import load_torch_model
from ray.ml.constants import PREPROCESSOR_KEY


class HuggingFacePredictor(TorchPredictor):
    """A predictor for HuggingFace Transformers PyTorch models.

    Args:
        model: The Transformers model to use for predictions.
        preprocessor: A preprocessor used to transform data batches prior
            to prediction.
    """

    def __init__(
        self,
        model: Union[PreTrainedModel, torch.nn.Module],
        preprocessor: Optional[Preprocessor] = None,
        training_args: Optional[TrainingArguments] = None,
    ):
        self.model = model
        self.preprocessor = preprocessor
        self.training_args = training_args

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: Checkpoint,
        model: Union[Type[PreTrainedModel], torch.nn.Module],
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
            **pretrained_model_kwargs: Any kwargs to pass to the
                ``model.from_pretrained()`` call. Only used if
                ``model`` is a ``PreTrainerModel`` class.
        """
        with checkpoint.as_directory() as checkpoint_path:
            preprocessor_path = os.path.join(checkpoint_path, PREPROCESSOR_KEY)
            if os.path.exists(preprocessor_path):
                with open(preprocessor_path, "rb") as f:
                    preprocessor = cpickle.load(f)
            else:
                preprocessor = None
            if isinstance(model, torch.nn.Module):
                state_dict = torch.load(
                    os.path.join(checkpoint_path, WEIGHTS_NAME), map_location="cpu"
                )
                model = load_torch_model(saved_model=state_dict, model_definition=model)
            else:
                model = model.from_pretrained(
                    checkpoint_path, **pretrained_model_kwargs
                )
            training_args_path = os.path.join(checkpoint_path, TRAINING_ARGS_NAME)
            if os.path.exists(training_args_path):
                with open(training_args_path, "rb") as f:
                    training_args = torch.load(f, map_location="cpu")
            else:
                training_args = None
        return HuggingFacePredictor(
            model=model, preprocessor=preprocessor, training_args=training_args
        )

    def _convert_to_tensor(
        self,
        data: pd.DataFrame,
        feature_columns: Optional[
            Union[List[str], List[List[str]], List[int], List[List[int]]]
        ] = None,
        dtypes: Optional[torch.dtype] = None,
        unsqueeze: bool = False,
    ) -> Dict[str, torch.Tensor]:
        if not feature_columns:
            feature_columns = data.columns

        # HF-supported format
        if not isinstance(feature_columns, dict):
            feature_columns = {column: [column] for column in feature_columns}

        return super()._convert_to_tensor(
            data, feature_columns=feature_columns, dtypes=dtypes, unsqueeze=unsqueeze
        )

    def _predict(self, tensor: Dict[str, torch.Tensor]) -> pd.DataFrame:
        self.training_args.local_rank = -1
        trainer = HFTrainer(model=self.model, args=self.training_args)
        dataset = HFIterableDatasetWithLen([tensor], 1)
        # squeeze out the extra dimension added by torch.stack
        # inside the HF data collator
        ret = trainer.predict(dataset).predictions.squeeze()
        # TODO(yard1): Return just a numpy array once that's supported
        # by Ray Datasets
        df = pd.DataFrame([ret.tolist()]).T
        df.columns = ["predictions"]
        return df

    def predict(
        self,
        data: DataBatchType,
        feature_columns: Optional[List[str]] = None,
        dtype: Optional[Union[Dict[str, torch.dtype], torch.dtype]] = None,
    ) -> DataBatchType:
        """Run inference on data batch.

        The data is converted into a dict of torch Tensors before being inputted to
        the model.

        Args:
            data: A batch of input data. Either a pandas DataFrame or numpy
                array.
            feature_columns: The names or indices of the columns in the
                data to use as features to predict on. If None, use all
                columns.
            dtype: The torch dtypes to use when creating the torch tensor.
                Can be either a single dtype or a dict of ``column:dtype``.
                If set to None, then automatically infer the dtype.

        Examples:

        .. code-block:: python

            import numpy as np
            import torch
            from ray.ml.predictors.torch import TorchPredictor

            model = torch.nn.Linear(1, 1)
            predictor = TorchPredictor(model=model)

            data = np.array([[1, 2], [3, 4]])
            predictions = predictor.predict(data)

            # Only use first column as the feature
            predictions = predictor.predict(data, feature_columns=[0])

        .. code-block:: python

            import pandas as pd
            import torch
            from ray.ml.predictors.torch import TorchPredictor

            model = torch.nn.Linear(1, 1)
            predictor = TorchPredictor(model=model)

            # Pandas dataframe.
            data = pd.DataFrame([[1, 2], [3, 4]], columns=["A", "B"])

            predictions = predictor.predict(data)

            # Only use first column as the feature
            predictions = predictor.predict(data, feature_columns=["A"])


        Returns:
            DataBatchType: Prediction result.
        """
        # We are just changing the signature and docstring.
        print(data)
        return super().predict(
            data, feature_columns=feature_columns, dtype=dtype, unsqueeze=False
        )
