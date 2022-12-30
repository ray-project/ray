import abc
from typing import Any, Dict, TypeVar, Union

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
    @DeveloperAPI
    def array_to_tensor(
        self,
        array: np.ndarray,
        dtype: TensorDtype,
    ) -> TensorType:
        """Converts a NumPy ndarray batch to the tensor type for the DL framework.

        Args:
            array: The numpy array to convert to a tensor.
            dtype: The tensor dtype to use when creating the DL tensor.

        Returns:
            A deep learning framework specific tensor.
        """
        raise NotImplementedError

    @abc.abstractmethod
    @DeveloperAPI
    def tensor_to_array(self, tensor: TensorType) -> np.ndarray:
        """Converts tensor framework specific tensor to a NumPy array.

        Args:
            tensor: A framework specific tensor.

        Returns:
            A numpy array representing the input tensor.
        """
        raise NotImplementedError

    @abc.abstractmethod
    @DeveloperAPI
    def call_model(self, inputs: Any) -> Any:
        """Inputs the tensor to the model for this Predictor and returns the result.

        Args:
            inputs: The tensor to input to the model.

        Returns:
            A tensor or dictionary of tensors containing the model output.
        """
        raise NotImplementedError

    @DeveloperAPI
    def arrays_to_inputs(self, arrays: Union[np.ndarray, Dict[str, np.ndarray]]):
        # Single column selection return numpy array so preprocessors can be
        # reused in both training and prediction
        if isinstance(arrays, dict) and len(arrays) == 1:
            arrays = next(iter(arrays.values()))

        if isinstance(arrays, np.ndarray):
            inputs = self.array_to_tensor(arrays)
        else:
            assert isinstance(arrays, dict) and all(
                isinstance(value, np.ndarray) for value in arrays.values()
            )
            inputs = {
                column: self.array_to_tensor(array) for column, array in arrays.items()
            }

        return inputs

    @DeveloperAPI
    def outputs_to_arrays(
        self, outputs: Any
    ) -> Union[np.ndarray, Dict[str, np.ndarray]]:
        # TODO (jiaodong): Investigate perf implication of this.
        # Move DL Tensor to CPU and convert to numpy.
        if isinstance(outputs, dict):
            return {k: self.tensor_to_array(v) for k, v in outputs.items()}
        else:
            return {"predictions": self.tensor_to_array(outputs)}

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
        model_inputs = self.arrays_to_inputs(data)
        model_outputs = self.call_model(model_inputs)
        return self.outputs_to_arrays(model_outputs)
