from typing import Type

import numpy as np
import pandas as pd

from ray.air.data_batch_type import DataBatchType
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.util.annotations import DeveloperAPI

try:
    import pyarrow as pa
except ImportError:
    pass


@DeveloperAPI
def convert_batch_type_to_pandas(data: DataBatchType) -> pd.DataFrame:
    """Convert the provided data to a Pandas DataFrame.

    Args:
        data: Data of type DataBatchType

    Returns:
        A pandas Dataframe representation of the input data.

    """
    from ray.air.utils.tensor_extensions.pandas import TensorArray

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
    data: pd.DataFrame, type: Type[DataBatchType]
) -> DataBatchType:
    """Convert the provided Pandas dataframe to the provided ``type``.

    Args:
        data: A Pandas DataFrame
        type: The specific ``DataBatchType`` to convert data type.

    Returns:
        The input data represented with the provided type.
    """
    if type == pd.DataFrame:
        return data

    elif type == np.ndarray:
        # If just a single column, return as a single tensor.
        if len(data.columns) == 1:
            return data.iloc[:, 0].to_numpy()
        return data.to_numpy()

    elif type == dict:
        output_dict = {}
        for column in data:
            output_dict[column] = data[column].to_numpy()
        return output_dict

    elif type == pa.Table:
        return pa.Table.from_pandas(data)

    else:
        raise ValueError(
            f"Received type {type}, but expected it to be one of {DataBatchType}"
        )
