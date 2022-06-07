from typing import Dict, Optional, Union

import numpy as np
import pandas as pd
import torch

from ray.air.predictor import Predictor, DataBatchType
from ray.air.preprocessor import Preprocessor
from ray.air.checkpoint import Checkpoint
from ray.air.train.integrations.torch import load_checkpoint
from ray.air.utils.data_batch_conversion_utils import convert_pandas_to_batch_type
from ray.air.utils.tensor_extensions.pandas import TensorArray


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

    def _predict_pandas(
        self,
        data: pd.DataFrame,
        dtype: Optional[Union[torch.dtype, Dict[str, torch.dtype]]] = None,
    ) -> pd.DataFrame:
        if len(data.columns) == 1:
            column_name = data.columns[0]
            if isinstance(dtype, dict):
                dtype = dtype[column_name]
            model_input = torch.from_numpy(
                convert_pandas_to_batch_type(data, type=np.ndarray)
            ).to(dtype=dtype)
        else:
            array_dict = convert_pandas_to_batch_type(data, type=dict)
            model_input = {
                k: torch.from_numpy(v).to(
                    dtype=dtype[k] if isinstance(dtype, dict) else dtype
                )
                for k, v in array_dict.items()
            }

        self.model.eval()
        output = self.model(model_input).cpu().detach().numpy()

        return pd.DataFrame(
            {"predictions": TensorArray(output)}, columns=["predictions"]
        )

    def predict(
        self,
        data: DataBatchType,
        dtype: Optional[Union[torch.dtype, Dict[str, torch.dtype]]] = None,
    ) -> DataBatchType:
        """Run inference on data batch.

        If the provided data is a single array or a dataframe/table with a single
        column, it will be converted into a single PyTorch tensor before being
        inputted to the model.

        If the provided data is a multi-column table or a dict of numpy arrays,
        it will be converted into a dict of tensors before being inputted to the
        model. This is useful for multi-modal inputs (for example your model accepts
        both image and text).

        Args:
            data: A batch of input data of ``DataBatchType``.
            dtype: The dtypes to use for the tensors. Either a single dtype for all
                tensors or a mapping from column name to dtype.

        Examples:

        .. code-block:: python

            import numpy as np
            import torch
            from ray.air.predictors.integrations.torch import TorchPredictor

            model = torch.nn.Linear(2, 1)
            predictor = TorchPredictor(model=model)

            data = np.array([[1, 2], [3, 4]])
            predictions = predictor.predict(data)

        .. code-block:: python

            import pandas as pd
            import torch
            from ray.air.predictors.integrations.torch import TorchPredictor

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
        return super(TorchPredictor, self).predict(data=data, dtype=dtype)
