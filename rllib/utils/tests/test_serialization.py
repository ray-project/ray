import unittest
from collections import OrderedDict

import gymnasium as gym
import numpy as np

from ray.rllib.utils.serialization import (
    convert_numpy_to_python_primitives,
    gym_space_from_dict,
    gym_space_to_dict,
    space_from_dict,
    space_to_dict,
)
from ray.rllib.utils.spaces.flexdict import FlexDict
from ray.rllib.utils.spaces.repeated import Repeated
from ray.rllib.utils.spaces.simplex import Simplex


def _assert_array_equal(eq, a1, a2, margin=None):
    for a in zip(a1, a2):
        eq(a[0], a[1], margin)


class TestGymCheckEnv(unittest.TestCase):
    def test_box_space(self):
        env = gym.make("CartPole-v1")
        d = gym_space_to_dict(env.observation_space)
        sp = gym_space_from_dict(d)

        obs_space = env.observation_space
        _assert_array_equal(
            self.assertAlmostEqual, sp.low.tolist(), obs_space.low.tolist(), 0.001
        )
        _assert_array_equal(
            self.assertAlmostEqual, sp.high.tolist(), obs_space.high.tolist(), 0.001
        )
        _assert_array_equal(self.assertEqual, sp._shape, obs_space._shape)
        self.assertEqual(sp.dtype, obs_space.dtype)

    def test_discrete_space(self):
        env = gym.make("CartPole-v1")
        d = gym_space_to_dict(env.action_space)
        sp = gym_space_from_dict(d)

        action_space = env.action_space
        self.assertEqual(sp.n, action_space.n)

    def test_multi_binary_space(self):
        mb = gym.spaces.MultiBinary((2, 3))
        d = space_to_dict(mb)
        sp = space_from_dict(d)

        self.assertEqual(sp.n, mb.n)

    def test_multi_discrete_space(self):
        md_space = gym.spaces.MultiDiscrete(nvec=np.array([3, 4, 5]))
        d = gym_space_to_dict(md_space)
        sp = gym_space_from_dict(d)

        _assert_array_equal(self.assertAlmostEqual, sp.nvec, md_space.nvec, 0.001)
        self.assertEqual(md_space.dtype, sp.dtype)

    def test_tuple_space(self):
        env = gym.make("CartPole-v1")
        space = gym.spaces.Tuple(spaces=[env.observation_space, env.action_space])
        d = gym_space_to_dict(space)
        sp = gym_space_from_dict(d)

        _assert_array_equal(
            self.assertAlmostEqual,
            sp.spaces[0].low.tolist(),
            space.spaces[0].low.tolist(),
            0.001,
        )
        _assert_array_equal(
            self.assertAlmostEqual,
            sp.spaces[0].high.tolist(),
            space.spaces[0].high.tolist(),
            0.001,
        )
        _assert_array_equal(
            self.assertEqual, sp.spaces[0]._shape, space.spaces[0]._shape
        )
        self.assertEqual(sp.dtype, space.dtype)

        self.assertEqual(sp.spaces[1].n, space.spaces[1].n)

    def test_dict_space(self):
        env = gym.make("CartPole-v1")
        space = gym.spaces.Dict(
            spaces={"obs": env.observation_space, "action": env.action_space}
        )
        d = gym_space_to_dict(space)
        sp = gym_space_from_dict(d)

        _assert_array_equal(
            self.assertAlmostEqual,
            sp.spaces["obs"].low.tolist(),
            space.spaces["obs"].low.tolist(),
            0.001,
        )
        _assert_array_equal(
            self.assertAlmostEqual,
            sp.spaces["obs"].high.tolist(),
            space.spaces["obs"].high.tolist(),
            0.001,
        )
        _assert_array_equal(
            self.assertEqual, sp.spaces["obs"]._shape, space.spaces["obs"]._shape
        )
        self.assertEqual(sp.dtype, space.dtype)

        self.assertEqual(sp.spaces["action"].n, space.spaces["action"].n)

    def test_dict_space_with_ordered_dict(self):
        """Tests whether correct dict order is restored based on the original order."""
        # User provides an OrderedDict -> gymnasium should take it and not further
        # sort the keys. The same (user-provided) order must be restored.
        input_space = gym.spaces.Dict(
            OrderedDict(
                {
                    "b_key": gym.spaces.Box(low=np.array([-1.0]), high=np.array([1.0])),
                    "a_key": gym.spaces.Discrete(n=3),
                }
            )
        )
        serialized_dict = space_to_dict(input_space)
        deserialized_space = space_from_dict(serialized_dict)
        self.assertTrue(input_space == deserialized_space)

        # User provides a simple dict -> gymnasium automatically sorts all keys
        # alphabetically. The same (alphabetical) order must be restored.
        input_space = gym.spaces.Dict(
            {
                "b_key": gym.spaces.Box(low=np.array([-1.0]), high=np.array([1.0])),
                "a_key": gym.spaces.Discrete(n=3),
            }
        )
        serialized_dict = space_to_dict(input_space)
        deserialized_space = space_from_dict(serialized_dict)
        self.assertTrue(input_space == deserialized_space)

    def test_simplex_space(self):
        space = Simplex(shape=(3, 4), concentration=np.array((1, 2, 1, 2)))

        d = gym_space_to_dict(space)
        sp = gym_space_from_dict(d)

        _assert_array_equal(self.assertEqual, space.shape, sp.shape)
        _assert_array_equal(
            self.assertAlmostEqual, space.concentration, sp.concentration
        )
        self.assertEqual(space.dtype, sp.dtype)

    def test_repeated(self):
        space = Repeated(gym.spaces.Box(low=-1, high=1, shape=(1, 200)), max_len=8)

        d = gym_space_to_dict(space)
        sp = gym_space_from_dict(d)

        self.assertTrue(isinstance(sp.child_space, gym.spaces.Box))
        self.assertEqual(space.max_len, sp.max_len)
        self.assertEqual(space.dtype, sp.dtype)

    def test_flex_dict(self):
        space = FlexDict({})
        space["box"] = gym.spaces.Box(low=-1, high=1, shape=(1, 200))
        space["discrete"] = gym.spaces.Discrete(2)
        space["tuple"] = gym.spaces.Tuple(
            (gym.spaces.Box(low=-1, high=1, shape=(1, 200)), gym.spaces.Discrete(2))
        )

        d = gym_space_to_dict(space)
        sp = gym_space_from_dict(d)

        self.assertTrue(isinstance(sp["box"], gym.spaces.Box))
        self.assertTrue(isinstance(sp["discrete"], gym.spaces.Discrete))
        self.assertTrue(isinstance(sp["tuple"], gym.spaces.Tuple))

    def test_text(self):
        expected_space = gym.spaces.Text(min_length=3, max_length=10, charset="abc")
        d = gym_space_to_dict(expected_space)
        sp = gym_space_from_dict(d)

        self.assertEqual(expected_space.max_length, sp.max_length)
        self.assertEqual(expected_space.min_length, sp.min_length)

        charset = getattr(expected_space, "character_set", None)
        if charset is not None:
            self.assertEqual(expected_space.character_set, sp.character_set)
        else:
            charset = getattr(expected_space, "charset", None)
            if charset is None:
                raise ValueError(
                    "Text space does not have charset or character_set attribute."
                )
            self.assertEqual(expected_space.charset, sp.charset)

    def test_original_space(self):
        space = gym.spaces.Box(low=0.0, high=1.0, shape=(10,))
        space.original_space = gym.spaces.Dict(
            {
                "obs1": gym.spaces.Box(low=0.0, high=1.0, shape=(3,)),
                "obs2": gym.spaces.Box(low=0.0, high=1.0, shape=(7,)),
            }
        )

        d = space_to_dict(space)
        sp = space_from_dict(d)

        self.assertTrue(isinstance(sp, gym.spaces.Box))
        self.assertTrue(isinstance(sp.original_space, gym.spaces.Dict))
        self.assertTrue(isinstance(sp.original_space["obs1"], gym.spaces.Box))
        self.assertTrue(isinstance(sp.original_space["obs2"], gym.spaces.Box))


class TestConvertNumpyToPythonPrimitives(unittest.TestCase):
    def test_convert_numpy_to_python_primitives(self):
        # test utility for converting numpy types to python primitives
        test_cases = [
            [1, 2, 3],
            [1.0, 2.0, 3.0],
            ["abc", "def", "ghi"],
            [True, False, True],
        ]
        for test_case in test_cases:
            _assert_array_equal(
                self.assertEqual,
                convert_numpy_to_python_primitives(np.array(test_case)),
                test_case,
            )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
