from gym.spaces import Box, Discrete, MultiDiscrete
import numpy as np
import tree  # pip install dm_tree
import unittest

import ray
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import flatten_inputs_to_1d_tensor as flatten_np
from ray.rllib.utils.test_utils import check
from ray.rllib.utils.tf_utils import flatten_inputs_to_1d_tensor as flatten_tf
from ray.rllib.utils.torch_utils import flatten_inputs_to_1d_tensor as flatten_torch

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class TestUtils(unittest.TestCase):
    # Nested struct of data with B=3.
    struct = {
        "a": np.array([1, 3, 2]),
        "b": (
            np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]),
            np.array(
                [[[8.0], [7.0], [6.0]], [[5.0], [4.0], [3.0]], [[2.0], [1.0], [0.0]]]
            ),
        ),
        "c": {
            "ca": np.array([[1, 2], [3, 5], [0, 1]]),
            "cb": np.array([1.0, 2.0, 3.0]),
        },
    }
    # Nested struct of data with B=2 and T=1.
    struct_w_time_axis = {
        "a": np.array([[1], [3]]),
        "b": (
            np.array([[[1.0, 2.0, 3.0]], [[4.0, 5.0, 6.0]]]),
            np.array([[[[8.0], [7.0], [6.0]]], [[[5.0], [4.0], [3.0]]]]),
        ),
        "c": {"ca": np.array([[[1, 2]], [[3, 5]]]), "cb": np.array([[1.0], [2.0]])},
    }
    # Corresponding space struct.
    spaces = dict(
        {
            "a": Discrete(4),
            "b": (Box(-1.0, 10.0, (3,)), Box(-1.0, 1.0, (3, 1))),
            "c": dict(
                {
                    "ca": MultiDiscrete([4, 6]),
                    "cb": Box(-1.0, 1.0, ()),
                }
            ),
        }
    )

    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_flatten_inputs_to_1d_tensor(self):
        # B=3; no time axis.
        check(
            flatten_np(self.struct, spaces_struct=self.spaces),
            np.array(
                [
                    [
                        0.0,
                        1.0,
                        0.0,
                        0.0,
                        1.0,
                        2.0,
                        3.0,
                        8.0,
                        7.0,
                        6.0,
                        0.0,
                        1.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        1.0,
                        0.0,
                        0.0,
                        0.0,
                        1.0,
                    ],
                    [
                        0.0,
                        0.0,
                        0.0,
                        1.0,
                        4.0,
                        5.0,
                        6.0,
                        5.0,
                        4.0,
                        3.0,
                        0.0,
                        0.0,
                        0.0,
                        1.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        1.0,
                        2.0,
                    ],
                    [
                        0.0,
                        0.0,
                        1.0,
                        0.0,
                        7.0,
                        8.0,
                        9.0,
                        2.0,
                        1.0,
                        0.0,
                        1.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        1.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        3.0,
                    ],
                ]
            ),
        )

        struct_tf = tree.map_structure(lambda s: tf.convert_to_tensor(s), self.struct)
        check(
            flatten_tf(struct_tf, spaces_struct=self.spaces),
            np.array(
                [
                    [
                        0.0,
                        1.0,
                        0.0,
                        0.0,
                        1.0,
                        2.0,
                        3.0,
                        8.0,
                        7.0,
                        6.0,
                        0.0,
                        1.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        1.0,
                        0.0,
                        0.0,
                        0.0,
                        1.0,
                    ],
                    [
                        0.0,
                        0.0,
                        0.0,
                        1.0,
                        4.0,
                        5.0,
                        6.0,
                        5.0,
                        4.0,
                        3.0,
                        0.0,
                        0.0,
                        0.0,
                        1.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        1.0,
                        2.0,
                    ],
                    [
                        0.0,
                        0.0,
                        1.0,
                        0.0,
                        7.0,
                        8.0,
                        9.0,
                        2.0,
                        1.0,
                        0.0,
                        1.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        1.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        3.0,
                    ],
                ]
            ),
        )

        struct_torch = tree.map_structure(lambda s: torch.from_numpy(s), self.struct)
        check(
            flatten_torch(struct_torch, spaces_struct=self.spaces),
            np.array(
                [
                    [
                        0.0,
                        1.0,
                        0.0,
                        0.0,
                        1.0,
                        2.0,
                        3.0,
                        8.0,
                        7.0,
                        6.0,
                        0.0,
                        1.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        1.0,
                        0.0,
                        0.0,
                        0.0,
                        1.0,
                    ],
                    [
                        0.0,
                        0.0,
                        0.0,
                        1.0,
                        4.0,
                        5.0,
                        6.0,
                        5.0,
                        4.0,
                        3.0,
                        0.0,
                        0.0,
                        0.0,
                        1.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        1.0,
                        2.0,
                    ],
                    [
                        0.0,
                        0.0,
                        1.0,
                        0.0,
                        7.0,
                        8.0,
                        9.0,
                        2.0,
                        1.0,
                        0.0,
                        1.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        1.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        3.0,
                    ],
                ]
            ),
        )

    def test_flatten_inputs_to_1d_tensor_w_time_axis(self):
        # B=2; T=1
        check(
            flatten_np(
                self.struct_w_time_axis, spaces_struct=self.spaces, time_axis=True
            ),
            np.array(
                [
                    [
                        [
                            0.0,
                            1.0,
                            0.0,
                            0.0,
                            1.0,
                            2.0,
                            3.0,
                            8.0,
                            7.0,
                            6.0,
                            0.0,
                            1.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            1.0,
                            0.0,
                            0.0,
                            0.0,
                            1.0,
                        ]
                    ],
                    [
                        [
                            0.0,
                            0.0,
                            0.0,
                            1.0,
                            4.0,
                            5.0,
                            6.0,
                            5.0,
                            4.0,
                            3.0,
                            0.0,
                            0.0,
                            0.0,
                            1.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            1.0,
                            2.0,
                        ]
                    ],
                ]
            ),
        )

        struct_tf = tree.map_structure(
            lambda s: tf.convert_to_tensor(s), self.struct_w_time_axis
        )
        check(
            flatten_tf(struct_tf, spaces_struct=self.spaces, time_axis=True),
            np.array(
                [
                    [
                        [
                            0.0,
                            1.0,
                            0.0,
                            0.0,
                            1.0,
                            2.0,
                            3.0,
                            8.0,
                            7.0,
                            6.0,
                            0.0,
                            1.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            1.0,
                            0.0,
                            0.0,
                            0.0,
                            1.0,
                        ]
                    ],
                    [
                        [
                            0.0,
                            0.0,
                            0.0,
                            1.0,
                            4.0,
                            5.0,
                            6.0,
                            5.0,
                            4.0,
                            3.0,
                            0.0,
                            0.0,
                            0.0,
                            1.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            1.0,
                            2.0,
                        ]
                    ],
                ]
            ),
        )

        struct_torch = tree.map_structure(
            lambda s: torch.from_numpy(s), self.struct_w_time_axis
        )
        check(
            flatten_torch(struct_torch, spaces_struct=self.spaces, time_axis=True),
            np.array(
                [
                    [
                        [
                            0.0,
                            1.0,
                            0.0,
                            0.0,
                            1.0,
                            2.0,
                            3.0,
                            8.0,
                            7.0,
                            6.0,
                            0.0,
                            1.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            1.0,
                            0.0,
                            0.0,
                            0.0,
                            1.0,
                        ]
                    ],
                    [
                        [
                            0.0,
                            0.0,
                            0.0,
                            1.0,
                            4.0,
                            5.0,
                            6.0,
                            5.0,
                            4.0,
                            3.0,
                            0.0,
                            0.0,
                            0.0,
                            1.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            1.0,
                            2.0,
                        ]
                    ],
                ]
            ),
        )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
