from enum import Enum, auto
import logging

import numpy as np
import pandas as pd

import ray
from ray.air.data_batch_type import DataBatchType
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.util.annotations import DeveloperAPI

try:
    import pyarrow
except ImportError:
    pyarrow = None

logger = logging.getLogger(__name__)


@DeveloperAPI
class DataType(Enum):
    PANDAS = auto()
    ARROW = auto()
    NUMPY = auto()  # Either a single numpy array or a Dict of numpy arrays.


@DeveloperAPI
def convert_batch_type_to_pandas(data: DataBatchType) -> pd.DataFrame:
    """Convert the provided data to a Pandas DataFrame.

    Args:
        data: Data of type DataBatchType

    Returns:
        A pandas Dataframe representation of the input data.

    """
    global _tensor_cast_failed_warned
    from ray.air.util.tensor_extensions.pandas import TensorArray

    if isinstance(data, pd.DataFrame):
        return data

    elif isinstance(data, np.ndarray):
        try:
            # Try to convert numpy arrays to TensorArrays.
            data = TensorArray(data)
        except TypeError as e:
            # Fall back to existing NumPy array.
            if ray.util.log_once("datasets_tensor_array_cast_warning"):
                logger.warning(
                    "Tried to transparently convert ndarray batch to a TensorArray "
                    f"but the conversion failed, leaving ndarray batch as-is: {e}"
                )
        return pd.DataFrame({TENSOR_COLUMN_NAME: data})

    elif isinstance(data, dict):
        tensor_dict = {}
        for k, v in data.items():
            if not isinstance(v, np.ndarray):
                raise ValueError(
                    "All values in the provided dict must be of type "
                    f"np.ndarray. Found type {type(v)} for key {k} "
                    f"instead."
                )
            try:
                # Try to convert numpy arrays to TensorArrays.
                v = TensorArray(v)
            except TypeError as e:
                # Fall back to existing NumPy array.
                if ray.util.log_once("datasets_tensor_array_cast_warning"):
                    logger.warning(
                        f"Tried to transparently convert column ndarray {k} of batch "
                        "to a TensorArray but the conversion failed, leaving column "
                        f"as-is: {e}"
                    )
            tensor_dict[k] = v
        return pd.DataFrame(tensor_dict)

    elif pyarrow is not None and isinstance(data, pyarrow.Table):
        return data.to_pandas()
    else:
        raise ValueError(
            f"Received data of type: {type(data)}, but expected it to be one "
            f"of {DataBatchType}"
        )


@DeveloperAPI
def convert_pandas_to_batch_type(data: pd.DataFrame, type: DataType) -> DataBatchType:
    """Convert the provided Pandas dataframe to the provided ``type``.

    Args:
        data: A Pandas DataFrame
        type: The specific ``DataBatchType`` to convert to.

    Returns:
        The input data represented with the provided type.
    """
    if type == DataType.PANDAS:
        return data

    elif type == DataType.NUMPY:
        if len(data.columns) == 1:
            # If just a single column, return as a single numpy array.
            return data.iloc[:, 0].to_numpy()
        else:
            # Else return as a dict of numpy arrays.
            output_dict = {}
            for column in data:
                output_dict[column] = data[column].to_numpy()
            return output_dict

    elif type == DataType.ARROW:
        if not pyarrow:
            raise ValueError(
                "Attempted to convert data to Pyarrow Table but Pyarrow "
                "is not installed. Please do `pip install pyarrow` to "
                "install Pyarrow."
            )
        return pyarrow.Table.from_pandas(data)

    else:
        raise ValueError(
            f"Received type {type}, but expected it to be one of {DataType}"
        )
