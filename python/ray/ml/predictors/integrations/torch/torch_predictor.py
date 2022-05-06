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
        return pd.DataFrame(prediction, columns=["predictions"])

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
            unsqueeze_feature_tensors (bool): If set to True, the features tensors
                will be unsqueezed (reshaped to (N, 1)) before being concatenated into
                the final features tensor. Otherwise, they will be left as is, that is
                (N, ). Defaults to True.

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

        tensor = self._convert_to_tensor(
            data, feature_columns=feature_columns, dtypes=dtype, unsqueeze=unsqueeze
        )
        return self._predict(tensor)
