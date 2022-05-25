import gym
import numpy as np
import unittest

from ray.rllib.utils.serialization import (
    gym_space_from_dict,
    gym_space_to_dict,
)


def _assert_array_equal(eq, a1, a2, margin=None):
    for a in zip(a1, a2):
        eq(a[0], a[1], margin)


class TestGymCheckEnv(unittest.TestCase):
    def test_box_space(self):
        env = gym.make("CartPole-v0")
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
        env = gym.make("CartPole-v0")
        d = gym_space_to_dict(env.action_space)
        sp = gym_space_from_dict(d)

        action_space = env.action_space
        self.assertEqual(sp.n, action_space.n)

    def test_multi_discrete_space(self):
        md_space = gym.spaces.MultiDiscrete(nvec=np.array([3, 4, 5]))
        d = gym_space_to_dict(md_space)
        sp = gym_space_from_dict(d)

        _assert_array_equal(self.assertAlmostEqual, sp.nvec, md_space.nvec, 0.001)
        self.assertEqual(md_space.dtype, sp.dtype)

    def test_tuple_space(self):
        env = gym.make("CartPole-v0")
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
        env = gym.make("CartPole-v0")
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


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
