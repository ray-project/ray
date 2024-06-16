import unittest

import numpy as np

import ray
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.tf_utils import two_hot

_, tf, _ = try_import_tf()


class TestTfUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_two_hot(self):
        # Test value that's exactly on one of the bucket boundaries. This used to return
        # a two-hot vector with a NaN in it, as k == kp1 at that boundary.
        check(
            two_hot(tf.convert_to_tensor([0.0]), 10, -5.0, 5.0),
            np.array([[0, 0, 0, 0, 0.5, 0.5, 0, 0, 0, 0]]),
        )

        # Test violating the boundaries (upper and lower).
        upper_bound = np.zeros((255,))
        upper_bound[-1] = 1.0
        lower_bound = np.zeros((255,))
        lower_bound[0] = 1.0
        check(
            two_hot(tf.convert_to_tensor([20.1, 50.0, 150.0, -20.00001])),
            np.array([upper_bound, upper_bound, upper_bound, lower_bound]),
        )

        # Test other cases.
        check(
            two_hot(tf.convert_to_tensor([2.5]), 11, -5.0, 5.0),
            np.array([[0, 0, 0, 0, 0, 0, 0, 0.5, 0.5, 0, 0]]),
        )
        check(
            two_hot(tf.convert_to_tensor([2.5, 0.1]), 10, -5.0, 5.0),
            np.array(
                [
                    [0, 0, 0, 0, 0, 0, 0.25, 0.75, 0, 0],
                    [0, 0, 0, 0, 0.41, 0.59, 0, 0, 0, 0],
                ]
            ),
        )
        check(
            two_hot(tf.convert_to_tensor([0.1]), 4, -1.0, 1.0),
            np.array([[0, 0.35, 0.65, 0]]),
        )
        check(
            two_hot(tf.convert_to_tensor([-0.5, -1.2]), 9, -6.0, 3.0),
            np.array(
                [
                    [0, 0, 0, 0, 0.11111, 0.88889, 0, 0, 0],
                    [0, 0, 0, 0, 0.73333, 0.26667, 0, 0, 0],
                ]
            ),
        )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
