from typing import Optional

import numpy as np
import torch

from ray.ml.predictor import Predictor, DataBatchType
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.train.integrations.torch import load_checkpoint
from ray.ml.utils.conversion_utils import (
    convert_arrow_to_data_batch,
    convert_data_batch_to_arrow,
    ArrowDataType,
)


class TorchPredictor(Predictor):
    """A predictor for PyTorch models.

    Args:
        model: The torch module to use for predictions.
        preprocessor: A preprocessor used to transform data batches prior
            to prediction.
    """

    def __init__(
        self, model: torch.nn.Module, preprocessor: Optional[Preprocessor] = None
    ):
        self.model = model
        self.preprocessor = preprocessor

    @classmethod
    def from_checkpoint(
        cls, checkpoint: Checkpoint, model: Optional[torch.nn.Module] = None
    ) -> "TorchPredictor":
        """Instantiate the predictor from a Checkpoint.

        The checkpoint is expected to be a result of ``TorchTrainer``.

        Args:
            checkpoint: The checkpoint to load the model and
                preprocessor from. It is expected to be from the result of a
                ``TorchTrainer`` run.
            model: If the checkpoint contains a model state dict, and not
                the model itself, then the state dict will be loaded to this
                ``model``.
        """
        model, preprocessor = load_checkpoint(checkpoint, model)
        return TorchPredictor(model=model, preprocessor=preprocessor)

    def _predict(
        self, data: ArrowDataType, dtype: Optional[torch.dtype] = None
    ) -> ArrowDataType:
        """Handle actual prediction."""
        self.model.eval()

        # TODO: Arrow is completely useless. We have to work with numpy here anyways.
        #  Pytorch does not have native support for arrow.
        numpy_array = convert_arrow_to_data_batch(data, np.ndarray)
        torch_tensor = torch.tensor(numpy_array, dtype=dtype)
        prediction = self.model(torch_tensor).cpu().detach().numpy()

        return convert_data_batch_to_arrow(prediction)

    def predict(
        self,
        data: DataBatchType,
        dtype: Optional[torch.dtype] = None,
    ) -> DataBatchType:
        """Run inference on data batch.

        The data is converted into a torch Tensor before being inputted to
        the model.

        Args:
            data: A batch of input data. Either a pandas DataFrame or numpy
                array. If this arg is a
                list of lists or a dict of  string-list pairs, then the
                data batch will be converted into a
                multiple tensors which are then concatenated before feeding
                into the model. This is useful for multi-input models. If
                None, then use all columns in ``data``.
            dtype: The dtypes to use for the tensors. This should match the
                format of ``feature_columns``, or be a single dtype, in which
                case it will be applied to all tensors.
                If None, then automatically infer the dtype.

        Examples:

        .. code-block:: python

            import numpy as np
            import torch
            from ray.ml.predictors.integrations.torch import TorchPredictor

            model = torch.nn.Linear(2, 1)
            predictor = TorchPredictor(model=model)

            data = np.array([[1, 2], [3, 4]])
            predictions = predictor.predict(data)

        .. code-block:: python

            import pandas as pd
            import torch
            from ray.ml.predictors.integrations.torch import TorchPredictor

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

        # TODO: Figure out a better API for this. Overriding predictor just to specify a
        #  kwarg seems very unecessary.
        return super().predict(data, dtype=dtype)
