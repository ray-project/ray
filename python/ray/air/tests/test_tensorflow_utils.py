import numpy as np
import pandas as pd
import tensorflow as tf

from ray.data.extensions import TensorArray
from ray.air._internal.tensorflow_utils import convert_pandas_to_tf_tensor


def test_convert_simple_df_to_tensor():
    df = pd.DataFrame({"X1": [1, 3], "X2": [2, 4]})

    actual_tensor = convert_pandas_to_tf_tensor(df)
    expected_tensor = tf.constant([[1, 2], [3, 4]], dtype=tf.dtypes.int64)
    tf.debugging.assert_equal(actual_tensor, expected_tensor)

    actual_tensor = convert_pandas_to_tf_tensor(df[["X1"]])
    expected_tensor = tf.constant([[1], [3]], dtype=tf.dtypes.int64)
    tf.debugging.assert_equal(actual_tensor, expected_tensor)


def test_convert_simple_df_to_tensor_with_dtype():
    df = pd.DataFrame({"X1": [1, 3], "X2": [2, 4]})

    actual_tensor = convert_pandas_to_tf_tensor(df, dtype=tf.dtypes.float16)

    expected_tensor = tf.constant([[1, 2], [3, 4]], dtype=tf.dtypes.float16)
    tf.debugging.assert_equal(actual_tensor, expected_tensor)


def test_convert_image_df_to_tensor():
    images = np.zeros([4, 3, 32, 32])
    df = pd.DataFrame({"image": TensorArray(images)})

    actual_tensor = convert_pandas_to_tf_tensor(df)

    expected_tensor = tf.zeros([4, 3, 32, 32], dtype=images.dtype)
    tf.debugging.assert_equal(actual_tensor, expected_tensor)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
