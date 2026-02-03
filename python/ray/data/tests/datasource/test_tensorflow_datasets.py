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
    assert ray_dataset._plan._logical_plan.dag.name == "FromItems"
    _check_usage_record(["FromItems"])


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
