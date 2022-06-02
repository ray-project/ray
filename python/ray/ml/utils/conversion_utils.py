from typing import Union, Type

import numpy as np
import pandas as pd
import pyarrow as pa

DataBatchType = Union["pd.DataFrame", "np.ndarray"]

ArrowDataType = Union[pa.Tensor, pa.RecordBatch]


def convert_data_batch_to_arrow(data_batch: DataBatchType) -> ArrowDataType:
    # TODO: This switch-case is ugly
    if type(data_batch) == pd.DataFrame:
        return _convert_pandas_to_arrow(data_batch)
    elif type(data_batch) == np.ndarray:
        return _convert_numpy_to_arrow(data_batch)
    else:
        raise ValueError(f"Unsupported Data type: {type(data_batch)}")


def convert_arrow_to_data_batch(
    arrow_data: ArrowDataType, data_batch_type: Type[DataBatchType]
) -> DataBatchType:
    # TODO: This switch-case is ugly
    if data_batch_type == np.ndarray:
        return _convert_arrow_to_numpy(arrow_data)
    elif data_batch_type == pd.DataFrame:
        return _convert_arrow_to_pandas(arrow_data)
    else:
        raise ValueError(f"Unsupported Data type: {data_batch_type}")


def _convert_arrow_to_numpy(data: ArrowDataType) -> np.ndarray:
    if type(data) == pa.Tensor:
        # Zero-copy.
        return data.to_numpy()
    elif type(data) == pa.RecordBatch:
        return _convert_arrow_to_numpy(_convert_record_batch_to_tensor(data))
    else:
        raise ValueError(f"Unsupported Data type: {type(data)}")


def _convert_arrow_to_pandas(data: ArrowDataType) -> pd.DataFrame:
    if type(data) == pa.Tensor:
        return _convert_arrow_to_pandas(_convert_tensor_to_record_batch(data))
    elif type(data) == pa.RecordBatch:
        # TODO: Do we want to enforce zero copy?
        return data.to_pandas()
    else:
        raise ValueError(f"Unsupported Data type: {type(data)}")


def _convert_numpy_to_arrow(data: np.ndarray) -> ArrowDataType:
    # Zero-copy.
    return pa.Tensor.from_numpy(data)


def _convert_pandas_to_arrow(data: pd.DataFrame) -> ArrowDataType:
    # Not zero copy.
    return pa.RecordBatch.from_pandas(data)


def _convert_tensor_to_record_batch(tensor: pa.Tensor) -> pa.RecordBatch:
    # Create a single column Arrow Table consisting of the provided tensor.

    # TODO: Move this outside of ray.data to a common utility.
    from ray.data.extensions.tensor_extension import ArrowTensorArray

    # Zero copy.
    arrow_tensor_array = ArrowTensorArray.from_numpy(tensor.to_numpy())

    # TODO: Take in column names as a parameter?
    return pa.RecordBatch.from_arrays([arrow_tensor_array], names=["__array__"])


def _convert_record_batch_to_tensor(record_batch: pa.RecordBatch) -> pa.Tensor:
    return _convert_numpy_to_arrow(_convert_arrow_to_pandas(record_batch).to_numpy())
