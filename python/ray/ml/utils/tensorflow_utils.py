import pandas as pd
import tensorflow as tf


def convert_pandas_to_tf_tensor(df: pd.DataFrame) -> tf.Tensor:
    """Convert a pandas dataframe to a TensorFlow tensor.

    This function works in two steps:
    1. Convert each dataframe column to a tensor.
    2. Concatenate the resulting tensors along the last axis.

    Arguments:
        df: The dataframe to convert to a TensorFlow tensor.

    Returns:
        A tensor of data type ``float32`` constructed from the dataframe.

    Examples:
        >>> import pandas as pd
        >>> df = pd.DataFrame({"X1": [1, 2, 3], "X2": [4, 5, 6]})
        >>>
        >>> convert_pandas_to_tf_tensor(df[["X1"]]).shape
        TensorShape([3])
        >>> convert_pandas_to_tf_tensor(df[["X1", "X2"]]).shape
        TensorShape([3, 2])

        >>> import numpy as np
        >>>
        >>> df = pd.DataFrame({"image": 2 * [np.zeros([3, 32, 32])]})
        >>> convert_pandas_to_tf_tensor(df).shape
        TensorShape([2, 3, 32, 32])
    """

    def tensorize(series):
        try:
            return tf.convert_to_tensor(series)
        except ValueError:
            return tf.stack([tensorize(element) for element in series])

    tensors = []
    for column in df.columns:
        series = df[column]
        tensor = tensorize(series)

        # We need to cast the tensors to a common type so that we can concatenate them.
        # If the columns contain different types (for example, `float32`s and
        # `int32`s), then `tf.expand_dims` raises an error.
        tensor = tf.cast(tensor, dtype=tf.dtypes.float32)

        tensors.append(tensor)

    if len(tensors) > 1:
        tensors = [tf.expand_dims(tensor, axis=-1) for tensor in tensors]

    return tf.concat(tensors, axis=-1)
