import sys

import numpy as np
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_util import _check_usage_record
from ray.data.tests.util import extract_values
from ray.tests.conftest import *  # noqa


def _to_nested_lists(value):
    if isinstance(value, np.ndarray):
        if value.dtype == object:
            return [_to_nested_lists(item) for item in value.tolist()]
        return value.tolist()
    if isinstance(value, tuple):
        return tuple(_to_nested_lists(item) for item in value)
    if isinstance(value, list):
        return [_to_nested_lists(item) for item in value]
    if isinstance(value, dict):
        return {key: _to_nested_lists(item) for key, item in value.items()}
    return value


def test_from_tf_e2e(ray_start_regular_shared_2_cpus):
    import tensorflow as tf
    import tensorflow_datasets as tfds

    tf_dataset = tfds.load("mnist", split=["train"], as_supervised=True)[0]
    tf_dataset = tf_dataset.take(8)  # Use subset to make test run faster.

    ray_dataset = ray.data.from_tf(tf_dataset)

    actual_data = extract_values("item", ray_dataset.take_all())
    expected_data = list(tf_dataset)
    assert len(actual_data) == len(expected_data)
    for (expected_features, expected_label), (actual_features, actual_label) in zip(
        expected_data, actual_data
    ):
        tf.debugging.assert_equal(expected_features, actual_features)
        tf.debugging.assert_equal(expected_label, actual_label)

    # Check that metadata fetch is included in stats.
    assert "FromItems" in ray_dataset.stats()
    # Underlying implementation uses `FromItems` operator
    assert ray_dataset._plan._logical_plan.dag.name == "FromItems"
    _check_usage_record(["FromItems"])


def test_from_tf_ragged_tensor(ray_start_regular_shared_2_cpus):
    import tensorflow as tf

    tf_dataset = tf.data.Dataset.from_tensors(
        tf.ragged.constant([[1, 2, 3], [4, 5]])
    ).concatenate(tf.data.Dataset.from_tensors(tf.ragged.constant([[6], [7, 8]])))

    ray_dataset = ray.data.from_tf(tf_dataset)

    actual_data = extract_values("item", ray_dataset.take_all())
    expected_data = list(tf_dataset)

    assert len(actual_data) == len(expected_data)
    for actual_item, expected_item in zip(actual_data, expected_data):
        assert _to_nested_lists(actual_item) == expected_item.to_list()


def test_from_tf_ragged_and_sparse_tensor(ray_start_regular_shared_2_cpus):
    import tensorflow as tf

    ragged_tensor = tf.ragged.constant([[1, 2, 3], [4, 5]])
    sparse_tensor = tf.sparse.from_dense([[1, 0, 0], [0, 2, 3]])
    tf_dataset = tf.data.Dataset.from_tensors(
        {"ragged": ragged_tensor, "sparse": sparse_tensor}
    )

    ray_dataset = ray.data.from_tf(tf_dataset)
    actual_item = ray_dataset.take_all()[0]

    assert _to_nested_lists(actual_item["ragged"]) == ragged_tensor.to_list()
    assert isinstance(actual_item["sparse"], tf.compat.v1.SparseTensorValue)
    np.testing.assert_array_equal(
        actual_item["sparse"].indices, sparse_tensor.indices.numpy()
    )
    np.testing.assert_array_equal(
        actual_item["sparse"].values, sparse_tensor.values.numpy()
    )
    np.testing.assert_array_equal(
        actual_item["sparse"].dense_shape, sparse_tensor.dense_shape.numpy()
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
