import abc
from typing import Dict, TypeVar, Union, List, Tuple

import numpy as np
import pandas as pd

from ray.air.util.data_batch_conversion import convert_pandas_to_batch_type, DataType
from ray.air.util.tensor_extensions.pandas import TensorArray
from ray.train.predictor import Predictor

TensorType = TypeVar("TensorType")
TensorDtype = TypeVar("TensorDtype")


class DLPredictor(Predictor):
    @abc.abstractmethod
    def tensorize(self, numpy_array: np.ndarray, dtype: TensorDtype) -> TensorType:
        """Converts a single numpy array to the tensor type for the DL framework.

        Args:
            numpy_array: The numpy array to convert to a tensor.
            dtype: The tensor dtype to use when creating the DL tensor.

        Returns:
            A deep learning framework specific tensor.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def untensorize(self, tensor: TensorType) -> np.ndarray:
        """Converts tensor framework specific tensor to a numpy array.

        Args:
            tensor: A framework specific tensor.

        Returns:
            A numpy array representing the input tensor.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def model_predict(
        self, tensor: Union[TensorType, Dict[str, TensorType]]
    ) -> Union[TensorType, Dict[str, TensorType], List[TensorType], Tuple[TensorType]]:
        """Inputs the tensor to the model for this Predictor and returns the result.

        Args:
            tensor: The tensor to input to the model.

        Returns:
            A tensor containing the model output.
        """
        raise NotImplementedError

    def _predict_pandas(
        self, data: pd.DataFrame, dtype: Union[TensorDtype, Dict[str, TensorDtype]]
    ) -> pd.DataFrame:
        tensors = convert_pandas_to_batch_type(data, DataType.NUMPY)

        # Single numpy array.
        if isinstance(tensors, np.ndarray):
            column_name = data.columns[0]
            if isinstance(dtype, dict):
                dtype = dtype[column_name]
            model_input = self.tensorize(tensors, dtype)

        else:
            model_input = {
                k: self.tensorize(
                    v, dtype=dtype[k] if isinstance(dtype, dict) else dtype
                )
                for k, v in tensors.items()
            }

        output = self.model_predict(model_input)

        # Handle model multi-output. For example if model outputs 2 images.
        if isinstance(output, dict):
            return pd.DataFrame(
                {k: TensorArray(self.untensorize(v)) for k, v in output}
            )
        elif isinstance(output, list) or isinstance(output, tuple):
            tensor_name = "output_"
            output_dict = {}
            for i in range(len(output)):
                output_dict[tensor_name + str(i + 1)] = TensorArray(
                    self.untensorize(output[i])
                )
            return pd.DataFrame(output_dict)
        else:
            return pd.DataFrame(
                {"predictions": TensorArray(self.untensorize(output))},
                columns=["predictions"],
            )
