import abc
from typing import Dict, TypeVar, Union

import numpy as np
import pandas as pd

from ray.air.util.data_batch_conversion import (
    BatchFormat,
    convert_pandas_to_batch_type,
    convert_batch_type_to_pandas,
)
from ray.train.predictor import Predictor
from ray.util.annotations import DeveloperAPI

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
    @DeveloperAPI
    def call_model(
        self, inputs: Union[TensorType, Dict[str, TensorType]]
    ) -> Union[TensorType, Dict[str, TensorType]]:
        """Inputs the tensor to the model for this Predictor and returns the result.

        Args:
            inputs: The tensor to input to the model.

        Returns:
            A tensor or dictionary of tensors containing the model output.
        """
        raise NotImplementedError

    @classmethod
    @DeveloperAPI
    def preferred_batch_format(cls) -> BatchFormat:
        return BatchFormat.NUMPY

    def _predict_pandas(
        self, data: pd.DataFrame, dtype: Union[TensorDtype, Dict[str, TensorDtype]]
    ) -> pd.DataFrame:
        numpy_input = convert_pandas_to_batch_type(
            data,
            BatchFormat.NUMPY,
            self._cast_tensor_columns,
        )
        numpy_output = self._predict_numpy(numpy_input, dtype)
        return convert_batch_type_to_pandas(numpy_output)

    def _predict_numpy(
        self,
        data: Union[np.ndarray, Dict[str, np.ndarray]],
        dtype: Union[TensorDtype, Dict[str, TensorDtype]],
    ) -> Union[np.ndarray, Dict[str, np.ndarray]]:
        # Single column selection return numpy array so preprocessors can be
        # reused in both training and prediction
        if isinstance(data, dict) and len(data) == 1:
            data = next(iter(data.values()))
        model_input = self._arrays_to_tensors(data, dtype)
        model_output = self.call_model(model_input)
        # TODO (jiaodong): Investigate perf implication of this.
        # Move DL Tensor to CPU and convert to numpy.
        if isinstance(model_output, dict):
            return {k: self._tensor_to_array(v) for k, v in model_output.items()}
        else:
            return {"predictions": self._tensor_to_array(model_output)}
