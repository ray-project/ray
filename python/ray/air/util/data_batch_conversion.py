from enum import Enum, auto
from typing import Union, List

import numpy as np
import pandas as pd

from ray.air.data_batch_type import DataBatchType
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.util.annotations import DeveloperAPI

try:
    import pyarrow
except ImportError:
    pyarrow = None


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
    if isinstance(data, np.ndarray):
        data = pd.DataFrame({TENSOR_COLUMN_NAME: _ndarray_to_column(data)})
    elif isinstance(data, dict):
        tensor_dict = {}
        for col_name, col in data.items():
            if not isinstance(col, np.ndarray):
                raise ValueError(
                    "All values in the provided dict must be of type "
                    f"np.ndarray. Found type {type(col)} for key {col_name} "
                    f"instead."
                )
            tensor_dict[col_name] = _ndarray_to_column(col)
        data = pd.DataFrame(tensor_dict)
    elif pyarrow is not None and isinstance(data, pyarrow.Table):
        data = data.to_pandas()
    elif not isinstance(data, pd.DataFrame):
        raise ValueError(
            f"Received data of type: {type(data)}, but expected it to be one "
            f"of {DataBatchType}"
        )
    return _cast_tensor_columns_to_ndarrays(data)


@DeveloperAPI
def convert_pandas_to_batch_type(data: pd.DataFrame, type: DataType) -> DataBatchType:
    """Convert the provided Pandas dataframe to the provided ``type``.

    Args:
        data: A Pandas DataFrame
        type: The specific ``DataBatchType`` to convert to.

    Returns:
        The input data represented with the provided type.
    """
    data = _cast_ndarray_columns_to_tensor_extension(data)
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


def _ndarray_to_column(arr: np.ndarray) -> Union[pd.Series, List[np.ndarray]]:
    """Convert a NumPy ndarray into an appropriate column format for insertion into a
    pandas DataFrame.

    If conversion to a pandas Series fails (e.g. if the ndarray is multi-dimensional),
    fall back to a list of NumPy ndarrays.
    """
    try:
        # Try to convert to Series, falling back to a list conversion if this fails
        # (e.g. if the ndarray is multi-dimensional.
        return pd.Series(arr)
    except ValueError:
        return list(arr)


def _cast_ndarray_columns_to_tensor_extension(df: pd.DataFrame) -> pd.DataFrame:
    """Cast all NumPy ndarray columns in df to our tensor extension type, TensorArray."""
    from ray.air.util.tensor_extensions.pandas import TensorArray
    from ray.data.context import DatasetContext

    ctx = DatasetContext.get_current()
    if not ctx.enable_tensor_extension_casting:
        return df

    # Try to convert any ndarray columns to TensorArray columns.
    # TODO(Clark): Once Pandas supports registering extension types for type
    # inference on construction, implement as much for NumPy ndarrays and remove
    # this. See https://github.com/pandas-dev/pandas/issues/41848
    for col_name, col in df.items():
        if (
            col.dtype.type is np.object_
            and not col.empty
            and isinstance(col.iloc[0], np.ndarray)
        ):
            try:
                df.loc[:, col_name] = TensorArray(col)
            except Exception as e:
                raise ValueError(
                    f"Tried to transparently convert column {col_name} to a "
                    "TensorArray but the conversion failed."
                ) from e
    return df


def _cast_tensor_columns_to_ndarrays(df: pd.DataFrame) -> pd.DataFrame:
    """Cast all tensor extension columns in df to NumPy ndarrays."""
    from ray.air.util.tensor_extensions.pandas import TensorDtype
    from ray.data.context import DatasetContext

    ctx = DatasetContext.get_current()
    if not ctx.enable_tensor_extension_casting:
        return df

    # Try to convert any tensor extension columns to ndarray columns.
    for col_name, col in df.items():
        if isinstance(col.dtype, TensorDtype):
            df.loc[:, col_name] = list(col.to_numpy())
    return df
