from enum import Enum, auto

import numpy as np
import pandas as pd

from ray.air.data_batch_type import DataBatchType
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.util.annotations import DeveloperAPI

try:
    import pyarrow as pa
except ImportError:
    pass

class DataType(Enum):
    PANDAS = auto()
    ARROW = auto()
    NUMPY = auto()


@DeveloperAPI
def convert_batch_type_to_pandas(data: DataBatchType) -> pd.DataFrame:
    """Convert the provided data to a Pandas DataFrame.

    Args:
        data: Data of type DataBatchType

    Returns:
        A pandas Dataframe representation of the input data.

    """
    from ray.air.util.tensor_extensions.pandas import TensorArray

    if isinstance(data, pd.DataFrame):
        return data

    elif isinstance(data, np.ndarray):
        if data.dtype == object:
            return pd.DataFrame({TENSOR_COLUMN_NAME: list(data)})
        else:
            return pd.DataFrame({TENSOR_COLUMN_NAME: TensorArray(data)})

    elif isinstance(data, dict):
        tensor_dict = {}
        for k, v in data.items():
            if not isinstance(v, np.ndarray):
                raise ValueError(
                    "All values in the provided dict must be of type "
                    f"np.ndarray. Found type {type(v)} for key {k} "
                    f"instead."
                )
            # Convert numpy arrays to TensorArray.
            tensor_dict[k] = TensorArray(v)
        return pd.DataFrame(tensor_dict)

    elif isinstance(data, pa.Table):
        return data.to_pandas()
    else:
        raise ValueError(
            f"Received data of type: {type(data)}, but expected it to be one "
            f"of {DataBatchType}"
        )


@DeveloperAPI
def convert_pandas_to_batch_type(
    data: pd.DataFrame, type: DataType
) -> DataBatchType:
    """Convert the provided Pandas dataframe to the provided ``type``.

    Args:
        data: A Pandas DataFrame
        type: The specific ``DataBatchType`` to convert data type.

    Returns:
        The input data represented with the provided type.
    """
    if type == DataType.PANDAS:
        return data

    elif type == DataType.NUMPY:
        if len(data.columns) == 1:
            # If just a single column, return as a single tensor.
            return data.iloc[:, 0].to_numpy()
        else:
            # Else return as a dict of numpy arrays.
            output_dict = {}
            for column in data:
                output_dict[column] = data[column].to_numpy()
            return output_dict

    elif type == DataType.ARROW:
        return pa.Table.from_pandas(data)

    else:
        raise ValueError(
            f"Received type {type}, but expected it to be one of {DataType}"
        )
