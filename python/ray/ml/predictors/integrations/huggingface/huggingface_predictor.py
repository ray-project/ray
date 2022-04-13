import os
from typing import Optional, Type, Union, List

import numpy as np
import pandas as pd
import torch
from transformers.modeling_utils import PreTrainedModel
from transformers.trainer import WEIGHTS_NAME

import ray.cloudpickle as cpickle
from ray.ml.predictor import Predictor, DataBatchType
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.utils.torch_utils import load_torch_model, convert_pandas_to_torch_tensor
from ray.ml.constants import PREPROCESSOR_KEY


class HuggingFacePredictor(Predictor):
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
            **pretrained_model_kwargs: Any kwargs to pass to the ``model.from_pretrained()``
                call. Only used if ``model`` is a ``PreTrainerModel``.
        """
        (
            checkpoint_type,
            checkpoint_path,
        ) = checkpoint.get_internal_representation()
        if checkpoint_type != "local_path":
            checkpoint_path = checkpoint.to_directory()
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

    def predict(
        self,
        data: DataBatchType,
        feature_columns: Optional[
            Union[List[str], List[List[str]], List[int], List[List[int]]]
        ] = None,
        dtype: Optional[torch.dtype] = None,
    ) -> DataBatchType:
        """Run inference on data batch.

        The data is converted into a torch Tensor before being inputted to
        the model.

        Args:
            data: A batch of input data. Either a pandas DataFrame or numpy
                array.
            feature_columns: The names or indices of the columns in the
                data to use as features to predict on. If this arg is a
                list of lists, then the data batch will be converted into a
                multiple tensors which are then concatenated before feeding
                into the model. This is useful for multi-input models. If
                None, then use all columns in ``data``.
            dtype: The torch dtype to use when creating the torch tensor.
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
        self.model.eval()

        if self.preprocessor:
            data = self.preprocessor.transform_batch(data)

        if isinstance(data, np.ndarray):
            # If numpy array, then convert to pandas dataframe.
            data = pd.DataFrame(data)

        # TODO(amog): Add `_convert_numpy_to_torch_tensor to use based on input type.
        # Reduce conversion cost if input is in Numpy
        tensor = convert_pandas_to_torch_tensor(
            data, columns=feature_columns, column_dtypes=dtype, unsqueeze=False
        )
        prediction = self.model(tensor).cpu().detach().numpy()
        return pd.DataFrame(prediction, columns=["predictions"])
