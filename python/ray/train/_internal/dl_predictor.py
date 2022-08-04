import abc
from typing import Dict, TypeVar, Union
import collections

import numpy as np
import pandas as pd


from ray.air.util.data_batch_conversion import convert_pandas_to_batch_type, DataType
from ray.air.util.tensor_extensions.pandas import TensorArray
from ray.train.predictor import Predictor

TensorType = TypeVar("TensorType")
TensorDtype = TypeVar("TensorDtype")


class DLPredictor(Predictor):
    @abc.abstractmethod
    def _arrays_to_tensors(
        self,
        numpy_arrays: Union[np.ndarray, Dict[str, np.ndarray]],
        dtype: Union[TensorDtype, Dict[str, TensorDtype]],
    ) -> Union[TensorType, Dict[str, TensorType]]:
        """Converts a NumPy ndarray batch to the tensor type for the DL framework.

        Args:
            numpy_array: The numpy array to convert to a tensor.
            dtype: The tensor dtype to use when creating the DL tensor.
            ndarray: A (dict of) NumPy ndarray(s) that we wish to convert to a (dict of)
                tensor(s).
            dtype: A (dict of) tensor dtype(s) to use when creating the DL tensor; if
                None, the dtype will be inferred from the NumPy ndarray data.

        Returns:
            A deep learning framework specific tensor.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _tensor_to_array(self, tensor: TensorType) -> np.ndarray:
        """Converts tensor framework specific tensor to a numpy array.

        Args:
            tensor: A framework specific tensor.

        Returns:
            A numpy array representing the input tensor.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def call_model(
        self, tensor: Union[TensorType, Dict[str, TensorType]]
    ) -> Union[TensorType, Dict[str, TensorType]]:
        """Inputs the tensor to the model for this Predictor and returns the result.

        Args:
            tensor: The tensor to input to the model.

        Returns:
            A tensor or dictionary of tensors containing the model output.
        """
        raise NotImplementedError

    def _predict_numpy(
        self,
        data: Union[np.ndarray, Dict[str, np.ndarray]],
        dtype: Union[TensorDtype, Dict[str, TensorDtype]],
    ):
        if isinstance(data, dict) and len(data) == 1:
            # If just a single column, return as a single numpy array.
            data = next(iter(data.values()))
        model_input = self._arrays_to_tensors(data, dtype)
        model_output = self.call_model(model_input)

        if isinstance(model_output, dict):
            return {k: self._tensor_to_array(v) for k, v in model_output.items()}
        elif isinstance(model_output, list) and isinstance(model_output[0], dict):
            # Rebatching for model such as ssd300_vgg16 that returns
            # List[Dict[str, Tensor]] where each Dict[str, Tensor] corresponds
            # to one record
            output = collections.defaultdict(list)
            for record in model_output:
                for k, v in record.items():
                    output[k].append(self._tensor_to_array(v))
            return {k: np.array(v) for k, v in output.items()}
        else:
            return self._tensor_to_array(output)

    def _predict_pandas(
        self, data: pd.DataFrame, dtype: Union[TensorDtype, Dict[str, TensorDtype]]
    ) -> pd.DataFrame:
        tensors = convert_pandas_to_batch_type(
            data,
            DataType.NUMPY,
            self._cast_tensor_columns,
        )
        model_input = self._arrays_to_tensors(tensors, dtype)

        output = self.call_model(model_input)

        # Handle model multi-output. For example if model outputs 2 images.
        if isinstance(output, dict):
            return pd.DataFrame(
                {k: TensorArray(self._tensor_to_array(v)) for k, v in output.items()}
            )
        else:
            return pd.DataFrame(
                {"predictions": TensorArray(self._tensor_to_array(output))},
                columns=["predictions"],
            )
