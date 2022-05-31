from typing import Optional, Union, List

import numpy as np
import pandas as pd
import torch

from ray.ml.predictor import Predictor, DataBatchType
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.train.integrations.torch import load_checkpoint
from ray.ml.utils.torch_utils import convert_pandas_to_torch_tensor


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

    # parity with Datset.to_torch
    def _convert_to_tensor(
        self,
        data: pd.DataFrame,
        feature_columns: Optional[
            Union[List[str], List[List[str]], List[int], List[List[int]]]
        ] = None,
        dtypes: Optional[torch.dtype] = None,
        unsqueeze: bool = True,
    ) -> torch.Tensor:
        """Handle conversion of data to tensor.

        Same arguments as in ``convert_pandas_to_torch_tensor``."""
        # TODO(amog): Add `_convert_numpy_to_torch_tensor to use based on input type.
        # Reduce conversion cost if input is in Numpy
        if isinstance(feature_columns, dict):
            features_tensor = {
                key: convert_pandas_to_torch_tensor(
                    data,
                    feature_columns[key],
                    dtypes[key] if isinstance(dtypes, dict) else dtypes,
                    unsqueeze=unsqueeze,
                )
                for key in feature_columns
            }
        else:
            features_tensor = convert_pandas_to_torch_tensor(
                data,
                columns=feature_columns,
                column_dtypes=dtypes,
                unsqueeze=unsqueeze,
            )
        return features_tensor

    def _predict(self, tensor: torch.Tensor) -> pd.DataFrame:
        """Handle actual prediction."""
        prediction = self.model(tensor).cpu().detach().numpy()
        # If model has outputs a Numpy array (for example outputting logits),
        # these cannot be used as values in a Pandas Dataframe.
        # We have to convert the outermost dimension to a python list (but the values
        # in the list can still be Numpy arrays).
        return pd.DataFrame({"predictions": list(prediction)}, columns=["predictions"])

    def predict(
        self,
        data: DataBatchType,
        feature_columns: Optional[
            Union[List[str], List[List[str]], List[int], List[List[int]]]
        ] = None,
        dtype: Optional[torch.dtype] = None,
        unsqueeze: bool = True,
    ) -> DataBatchType:
        """Run inference on data batch.

        The data is converted into a torch Tensor before being inputted to
        the model.

        Args:
            data: A batch of input data. Either a pandas DataFrame or numpy
                array.
            feature_columns: The names or indices of the columns in the
                data to use as features to predict on. If this arg is a
                list of lists or a dict of  string-list pairs, then the
                data batch will be converted into a
                multiple tensors which are then concatenated before feeding
                into the model. This is useful for multi-input models. If
                None, then use all columns in ``data``.
            dtype: The dtypes to use for the tensors. This should match the
                format of ``feature_columns``, or be a single dtype, in which
                case it will be applied to all tensors.
                If None, then automatically infer the dtype.
            unsqueeze: If set to True, the features tensors will be unsqueezed
                (reshaped to (N, 1)) before being concatenated into the final features
                tensor. Otherwise, they will be left as is, that is (N, ).
                Defaults to True.

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
        self.model.eval()

        if self.preprocessor:
            data = self.preprocessor.transform_batch(data)

        if isinstance(data, np.ndarray):
            tensor = torch.tensor(data, dtype=dtype)
        else:
            tensor = self._convert_to_tensor(
                data, feature_columns=feature_columns, dtypes=dtype, unsqueeze=unsqueeze
            )

        return self._predict(tensor)
