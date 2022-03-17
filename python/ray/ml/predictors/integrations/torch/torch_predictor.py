from typing import Optional, Union, List

import numpy as np
import pandas as pd
import torch

from ray.ml.predictor import Predictor, DataBatchType
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.utils.torch_utils import load_torch_model, convert_pandas_to_torch_tensor
from ray.ml.constants import PREPROCESSOR_KEY, MODEL_KEY


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
        self.model.eval()
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
        checkpoint_dict = checkpoint.to_dict()
        preprocessor = checkpoint_dict.get(PREPROCESSOR_KEY, None)
        if MODEL_KEY not in checkpoint_dict:
            raise RuntimeError(
                f"No item with key: {MODEL_KEY} is found in the "
                f"Checkpoint. Make sure this key exists when saving the "
                f"checkpoint in ``TorchTrainer``."
            )
        model = load_torch_model(
            saved_model=checkpoint_dict[MODEL_KEY], model_definition=model
        )
        return TorchPredictor(model=model, preprocessor=preprocessor)

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
        if self.preprocessor:
            data = self.preprocessor.transform_batch(data)

        if isinstance(data, np.ndarray):
            # If numpy array, then convert to pandas dataframe.
            data = pd.DataFrame(data)

        # TODO(amog): Add `_convert_numpy_to_torch_tensor to use based on input type.
        # Reduce conversion cost if input is in Numpy
        tensor = convert_pandas_to_torch_tensor(
            data, columns=feature_columns, column_dtypes=dtype
        )
        prediction = self.model(tensor).cpu().detach().numpy()
        return pd.DataFrame(prediction, columns=["predictions"])
