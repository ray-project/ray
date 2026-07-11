import sys

import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_util import _check_usage_record
from ray.data.tests.util import extract_values
from ray.tests.conftest import *  # noqa


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
    assert ray_dataset._logical_plan.dag.name == "FromItems"
    _check_usage_record(["FromItems"])


def test_from_tf_ragged(ray_start_regular_shared_2_cpus):
    # Regression test for https://github.com/ray-project/ray/issues/61570.
    # `as_numpy_iterator` can't convert ragged tensors, so `from_tf` needs to
    # convert them itself instead of crashing. The element spec must stay ragged
    # after slicing to reproduce the crash, so use a `ragged_rank=2` tensor:
    # `from_tensor_slices` peels off the outer dimension and leaves each element
    # as a (ragged) `RaggedTensor`.
    import numpy as np
    import tensorflow as tf

    tf_dataset = tf.data.Dataset.from_tensor_slices(
        tf.ragged.constant([[[1, 2], [3]], [[4, 5, 6]]])
    )
    assert isinstance(tf_dataset.element_spec, tf.RaggedTensorSpec), (
        "Test dataset must have a ragged element spec to exercise the fix"
    )

    ray_dataset = ray.data.from_tf(tf_dataset)

    actual_data = extract_values("item", ray_dataset.take_all())
    # Each row materializes as an object-dtype array of variable-length arrays.
    expected_data = [
        [np.array([1, 2]), np.array([3])],
        [np.array([4, 5, 6])],
    ]
    assert len(actual_data) == len(expected_data)
    for actual_row, expected_row in zip(actual_data, expected_data):
        assert len(actual_row) == len(expected_row)
        for actual_subarray, expected_subarray in zip(actual_row, expected_row):
            assert np.array_equal(actual_subarray, expected_subarray)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
