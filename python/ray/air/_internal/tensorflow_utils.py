from typing import Optional, Union, Dict

import numpy as np
import pandas as pd
import tensorflow as tf
from pandas.api.types import is_object_dtype

from ray.air.util.data_batch_conversion import _unwrap_ndarray_object_type_if_needed


def convert_pandas_to_tf_tensor(
    df: pd.DataFrame, dtype: Optional[tf.dtypes.DType] = None
) -> tf.Tensor:
    """Convert a pandas dataframe to a TensorFlow tensor.

    This function works in two steps:
    1. Convert each dataframe column to a tensor.
    2. Concatenate the resulting tensors along the last axis.

    Arguments:
        df: The dataframe to convert to a TensorFlow tensor. Columns must be of
            a numeric dtype, ``TensorDtype``, or object dtype. If a column has
            an object dtype, the column must contain ``ndarray`` objects.
        dtype: Optional data type for the returned tensor. If a dtype isn't
            provided, the dtype is inferred from ``df``.

    Returns:
        A tensor constructed from the dataframe.

    Examples:
        >>> import pandas as pd
        >>> from ray.air._internal.tensorflow_utils import convert_pandas_to_tf_tensor
        >>>
        >>> df = pd.DataFrame({"X1": [1, 2, 3], "X2": [4, 5, 6]})
        >>> convert_pandas_to_tf_tensor(df[["X1"]]).shape
        TensorShape([3, 1])
        >>> convert_pandas_to_tf_tensor(df[["X1", "X2"]]).shape
        TensorShape([3, 2])

        >>> from ray.data.extensions import TensorArray
        >>> import numpy as np
        >>>
        >>> df = pd.DataFrame({"image": TensorArray(np.zeros((4, 3, 32, 32)))})
        >>> convert_pandas_to_tf_tensor(df).shape
        TensorShape([4, 3, 32, 32])
    """
    if dtype is None:
        try:
            # We need to cast the tensors to a common type so that we can concatenate
            # them. If the columns contain different types (for example, `float32`s
            # and `int32`s), then `tf.concat` raises an error.
            dtype: np.dtype = np.find_common_type(df.dtypes, [])

            # if the columns are `ray.data.extensions.tensor_extension.TensorArray`,
            # the dtype will be `object`. In this case, we need to set the dtype to
            # none, and use the automatic type casting of `tf.convert_to_tensor`.
            if is_object_dtype(dtype):
                dtype = None

        except TypeError:
            # `find_common_type` fails if a series has `TensorDtype`. In this case,
            # don't cast any of the series and continue.
            pass

    def tensorize(series):
        try:
            return tf.convert_to_tensor(series, dtype=dtype)
        except ValueError:
            return tf.stack([tensorize(element) for element in series])

    tensors = []
    for column in df.columns:
        series = df[column]
        tensor = tensorize(series)
        tensors.append(tensor)

    if len(tensors) > 1:
        tensors = [tf.expand_dims(tensor, axis=1) for tensor in tensors]

    concatenated_tensor = tf.concat(tensors, axis=1)

    if concatenated_tensor.ndim == 1:
        return tf.expand_dims(concatenated_tensor, axis=1)

    return concatenated_tensor


def convert_ndarray_to_tf_tensor(
    ndarray: np.ndarray,
    dtype: Optional[tf.dtypes.DType] = None,
) -> tf.Tensor:
    """Convert a NumPy ndarray to a TensorFlow Tensor.

    Args:
        ndarray: A NumPy ndarray that we wish to convert to a TensorFlow Tensor.
        dtype: A TensorFlow dtype for the created tensor; if None, the dtype will be
            inferred from the NumPy ndarray data.

    Returns: A TensorFlow Tensor.
    """
    ndarray = _unwrap_ndarray_object_type_if_needed(ndarray)
    return tf.convert_to_tensor(ndarray, dtype=dtype)


def convert_ndarray_batch_to_tf_tensor_batch(
    ndarrays: Union[np.ndarray, Dict[str, np.ndarray]],
    dtypes: Optional[Union[tf.dtypes.DType, Dict[str, tf.dtypes.DType]]] = None,
) -> Union[tf.Tensor, Dict[str, tf.Tensor]]:
    """Convert a NumPy ndarray batch to a TensorFlow Tensor batch.

    Args:
        ndarray: A (dict of) NumPy ndarray(s) that we wish to convert to a TensorFlow
            Tensor.
        dtype: A (dict of) TensorFlow dtype(s) for the created tensor; if None, the
            dtype will be inferred from the NumPy ndarray data.

    Returns: A (dict of) TensorFlow Tensor(s).
    """
    if isinstance(ndarrays, np.ndarray):
        # Single-tensor case.
        if isinstance(dtypes, dict):
            if len(dtypes) != 1:
                raise ValueError(
                    "When constructing a single-tensor batch, only a single dtype "
                    f"should be given, instead got: {dtypes}"
                )
            dtypes = next(iter(dtypes.values()))
        batch = convert_ndarray_to_tf_tensor(ndarrays, dtypes)
    else:
        # Multi-tensor case.
        batch = {
            col_name: convert_ndarray_to_tf_tensor(
                col_ndarray,
                dtype=dtypes[col_name] if isinstance(dtypes, dict) else dtypes,
            )
            for col_name, col_ndarray in ndarrays.items()
        }

    return batch
