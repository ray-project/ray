from typing import TYPE_CHECKING, Dict, Optional, Union

import numpy as np
import pandas as pd
import torch

from ray.train.predictor import DataBatchType, Predictor
from ray.air.checkpoint import Checkpoint
from ray.air.util.data_batch_conversion import convert_pandas_to_batch_type, DataType
from ray.air.util.tensor_extensions.pandas import TensorArray
from ray.train.torch.utils import load_checkpoint

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


class TorchPredictor(Predictor):
    """A predictor for PyTorch models.

    Args:
        model: The torch module to use for predictions.
        preprocessor: A preprocessor used to transform data batches prior
            to prediction.
    """

    def __init__(
        self, model: torch.nn.Module, preprocessor: Optional["Preprocessor"] = None
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
        def tensorize(numpy_array, dtype):
            torch_tensor = torch.from_numpy(numpy_array).to(dtype)

            # Off-the-shelf torch Modules expect the input size to have at least 2
            # dimensions (batch_size, feature_size). If the tensor for the column
            # is flattened, then we unqueeze it to add an extra dimension.
            if len(torch_tensor.size()) == 1:
                torch_tensor = torch_tensor.unsqueeze(dim=1)

            return torch_tensor

        tensors = convert_pandas_to_batch_type(data, DataType.NUMPY)

        # Single numpy array.
        if isinstance(tensors, np.ndarray):
            column_name = data.columns[0]
            if isinstance(dtype, dict):
                dtype = dtype[column_name]
            model_input = tensorize(tensors, dtype)

        else:
            model_input = {
                k: tensorize(v, dtype=dtype[k] if isinstance(dtype, dict) else dtype)
                for k, v in tensors.items()
            }

        with torch.no_grad():
            self.model.eval()
            output = self.model(model_input)

        def untensorize(torch_tensor):
            numpy_array = torch_tensor.cpu().detach().numpy()
            return TensorArray(numpy_array)

        # Handle model multi-output. For example if model outputs 2 images.
        if isinstance(output, dict):
            return pd.DataFrame({k: untensorize(v) for k, v in output})
        elif isinstance(output, list) or isinstance(output, tuple):
            tensor_name = "output_"
            output_dict = {}
            for i in range(len(output)):
                output_dict[tensor_name + str(i + 1)] = untensorize(output[i])
            return pd.DataFrame(output_dict)
        else:
            return pd.DataFrame(
                {"predictions": untensorize(output)}, columns=["predictions"]
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
            from ray.train.torch import TorchPredictor

            model = torch.nn.Linear(2, 1)
            predictor = TorchPredictor(model=model)

            data = np.array([[1, 2], [3, 4]])
            predictions = predictor.predict(data, dtype=torch.float)

        .. code-block:: python

            import pandas as pd
            import torch
            from ray.train.torch import TorchPredictor

            class CustomModule(torch.nn.Module):
                def __init__(self):
                    super().__init__()
                    self.linear1 = torch.nn.Linear(1, 1)
                    self.linear2 = torch.nn.Linear(1, 1)

                def forward(self, input_dict: dict):
                    out1 = self.linear1(input_dict["A"])
                    out2 = self.linear2(input_dict["B"])
                    return out1 + out2

            predictor = TorchPredictor(model=CustomModule())

            # Pandas dataframe.
            data = pd.DataFrame([[1, 2], [3, 4]], columns=["A", "B"])

            predictions = predictor.predict(data)

        Returns:
            DataBatchType: Prediction result.
        """
        return super(TorchPredictor, self).predict(data=data, dtype=dtype)
