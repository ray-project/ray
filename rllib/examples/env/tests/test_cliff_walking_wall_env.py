from ray.rllib.examples.env.cliff_walking_wall_env import (
    CliffWalkingWallEnv,
    ACTION_UP,
    ACTION_RIGHT,
    ACTION_DOWN,
    ACTION_LEFT,
)

import unittest


class TestCliffWalkingWallEnv(unittest.TestCase):
    def test_env(self):
        env = CliffWalkingWallEnv()
        obs, info = env.reset()
        # Starting position
        self.assertEqual(obs, 36)
        # Left, Right, and Down are no-ops
        obs, _, _, _, _ = env.step(ACTION_LEFT)
        self.assertEqual(obs, 36)
        obs, _, _, _, _ = env.step(ACTION_DOWN)
        self.assertEqual(obs, 36)
        obs, _, _, _, _ = env.step(ACTION_RIGHT)
        self.assertEqual(obs, 36)

        # Up and Down returns to starting position
        obs, _, _, _, _ = env.step(ACTION_UP)
        self.assertEqual(obs, 24)
        obs, _, _, _, _ = env.step(ACTION_DOWN)
        self.assertEqual(obs, 36)
        obs, _, _, _ = env.step(ACTION_DOWN)
        self.assertEqual(obs, 36)

        # Going down at the wall is a no-op
        env.step(ACTION_UP)
        obs, _, _, _ = env.step(ACTION_RIGHT)
        self.assertEqual(obs, 25)
        obs, _, _, _ = env.step(ACTION_DOWN)
        self.assertEqual(obs, 25)

        # Move all the way to the right wall
        for _ in range(10):
            env.step(ACTION_RIGHT)
        obs, rew, done, truncated, _ = env.step(ACTION_RIGHT)
        self.assertEqual(obs, 35)
        self.assertEqual(rew, -1)
        self.assertEqual(done, False)

        # Move to goal
        obs, rew, done, truncated, _ = env.step(ACTION_DOWN)
        self.assertEqual(obs, 47)
        self.assertEqual(rew, 10)
        self.assertEqual(done, True)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
