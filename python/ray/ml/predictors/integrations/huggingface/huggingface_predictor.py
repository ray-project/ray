import os
from typing import Optional, Type, Union, List

import pandas as pd
import torch
from transformers.modeling_utils import PreTrainedModel
from transformers.trainer import WEIGHTS_NAME

import ray.cloudpickle as cpickle
from ray.ml.predictors.integrations.torch import TorchPredictor
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.utils.torch_utils import load_torch_model, convert_pandas_to_torch_tensor
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
    ):
        self.model = model
        self.preprocessor = preprocessor

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
            model: Either a ``transformers.PreTrainedModel`` class, or a
                PyTorch model to load the weights to. This should be the
                same model used for training.
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
            if issubclass(model, PreTrainedModel):
                model = PreTrainedModel.from_pretrained(
                    checkpoint_path, **pretrained_model_kwargs
                )
            else:
                state_dict = torch.load(
                    os.path.join(checkpoint_path, WEIGHTS_NAME), map_location="cpu"
                )
                model = load_torch_model(saved_model=state_dict, model_definition=model)
        return HuggingFacePredictor(model=model, preprocessor=preprocessor)

    def _convert_to_tensor(
        self,
        data: pd.DataFrame,
        feature_columns: Optional[
            Union[List[str], List[List[str]], List[int], List[List[int]]]
        ],
        dtype: Optional[torch.dtype],
    ):
        if not feature_columns:
            feature_columns = data.columns

        # HF-supported format
        feature_columns = {column: [column] for column in feature_columns}

        # TODO(amog): Add `_convert_numpy_to_torch_tensor to use based on input type.
        # Reduce conversion cost if input is in Numpy
        return convert_pandas_to_torch_tensor(
            data, columns=feature_columns, column_dtypes=dtype, unsqueeze=False
        )
