from gym.spaces import Box, Dict, Discrete, MultiDiscrete, Tuple
import numpy as np
import tree  # pip install dm_tree
import unittest

import ray
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import merge_inputs_to_1d as merge_np
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.tf_utils import merge_inputs_to_1d as merge_tf
from ray.rllib.utils.torch_utils import merge_inputs_to_1d as merge_torch

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class TestUtils(unittest.TestCase):
    # Nested struct of data with B=3.
    struct = {
        "a": np.array([1, 3, 2]),
        "b": (
            np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]),
            np.array([[[8.0], [7.0], [6.0]], [[5.0], [4.0], [3.0]],
                      [[2.0], [1.0], [0.0]]]),
        ),
        "c": {
            "ca": np.array([[1, 2], [3, 5], [0, 1]]),
            "cb": np.array([1.0, 2.0, 3.0])
        }
    }
    # Corresponding space struct.
    spaces = Dict({
        "a": Discrete(4),
        "b": Tuple([Box(-1.0, 10.0, (3, )),
                    Box(-1.0, 1.0, (3, 1))]),
        "c": Dict({
            "ca": MultiDiscrete([4, 6]),
            "cb": Box(-1.0, 1.0, ()),
        })
    })

    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_merge_to_1d(self):
        check(
            merge_np(self.struct, spaces=self.spaces),
            np.array([
                [
                    0.0, 1.0, 0.0, 0.0, 1.0, 2.0, 3.0, 8.0, 7.0, 6.0, 0.0, 1.0,
                    0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0
                ],
                [
                    0.0, 0.0, 0.0, 1.0, 4.0, 5.0, 6.0, 5.0, 4.0, 3.0, 0.0, 0.0,
                    0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 2.0
                ],
                [
                    0.0, 0.0, 1.0, 0.0, 7.0, 8.0, 9.0, 2.0, 1.0, 0.0, 1.0, 0.0,
                    0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 3.0
                ],
            ]))

        struct_tf = tree.map_structure(lambda s: tf.convert_to_tensor(s),
                                       self.struct)
        check(
            merge_tf(struct_tf, spaces=self.spaces),
            np.array([
                [
                    0.0, 1.0, 0.0, 0.0, 1.0, 2.0, 3.0, 8.0, 7.0, 6.0, 0.0, 1.0,
                    0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0
                ],
                [
                    0.0, 0.0, 0.0, 1.0, 4.0, 5.0, 6.0, 5.0, 4.0, 3.0, 0.0, 0.0,
                    0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 2.0
                ],
                [
                    0.0, 0.0, 1.0, 0.0, 7.0, 8.0, 9.0, 2.0, 1.0, 0.0, 1.0, 0.0,
                    0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 3.0
                ],
            ]))

        struct_torch = tree.map_structure(lambda s: torch.from_numpy(s),
                                          self.struct)
        check(
            merge_torch(struct_torch, spaces=self.spaces),
            np.array([
                [
                    0.0, 1.0, 0.0, 0.0, 1.0, 2.0, 3.0, 8.0, 7.0, 6.0, 0.0, 1.0,
                    0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0
                ],
                [
                    0.0, 0.0, 0.0, 1.0, 4.0, 5.0, 6.0, 5.0, 4.0, 3.0, 0.0, 0.0,
                    0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 2.0
                ],
                [
                    0.0, 0.0, 1.0, 0.0, 7.0, 8.0, 9.0, 2.0, 1.0, 0.0, 1.0, 0.0,
                    0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 3.0
                ],
            ]))


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
