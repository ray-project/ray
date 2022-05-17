import numpy as np
import pandas as pd
import tensorflow as tf

from ray.ml.utils.tensorflow_utils import convert_pandas_to_tf_tensor


def test_convert_simple_df_to_tensor():
    df = pd.DataFrame({"X1": [1, 3], "X2": [2, 4]})

    actual_tensor = convert_pandas_to_tf_tensor(df)
    expected_tensor = tf.constant([[1, 2], [3, 4]], dtype=tf.dtypes.float32)
    tf.debugging.assert_equal(actual_tensor, expected_tensor)

    actual_tensor = convert_pandas_to_tf_tensor(df[["X1"]])
    expected_tensor = tf.constant([1, 3], dtype=tf.dtypes.float32)
    tf.debugging.assert_equal(actual_tensor, expected_tensor)


def test_convert_image_df_to_tensor():
    image = np.zeros([3, 32, 32])
    df = pd.DataFrame({"image": 4 * [image]})

    actual_tensor = convert_pandas_to_tf_tensor(df)

    expected_tensor = tf.zeros([4, 3, 32, 32], dtype=tf.dtypes.float32)
    tf.debugging.assert_equal(actual_tensor, expected_tensor)
