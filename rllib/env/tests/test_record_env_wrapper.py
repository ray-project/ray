from gym import wrappers
import tempfile
import unittest

from ray.rllib.env.utils import VideoMonitor, record_env_wrapper
from ray.rllib.examples.env.mock_env import MockEnv2
from ray.rllib.examples.env.multi_agent import BasicMultiAgent


class TestRecordEnvWrapper(unittest.TestCase):
    def test_wrap_gym_env(self):
        wrapped = record_env_wrapper(
            env=MockEnv2(10),
            record_env=tempfile.gettempdir(),
            log_dir="",
            policy_config={
                "in_evaluation": False,
            })
        # Type is wrappers.Monitor.
        self.assertTrue(isinstance(wrapped, wrappers.Monitor))
        self.assertFalse(isinstance(wrapped, VideoMonitor))

        wrapped.reset()
        # 10 steps for a complete episode.
        for i in range(10):
            wrapped.step(0)

        # MockEnv2 returns a reward of 100.0 every step.
        # So total reward is 1000.0.
        self.assertEqual(wrapped.get_episode_rewards(), [1000.0])

    def test_wrap_multi_agent_env(self):
        wrapped = record_env_wrapper(
            env=BasicMultiAgent(3),
            record_env=tempfile.gettempdir(),
            log_dir="",
            policy_config={
                "in_evaluation": False,
            })
        # Type is VideoMonitor.
        self.assertTrue(isinstance(wrapped, wrappers.Monitor))
        self.assertTrue(isinstance(wrapped, VideoMonitor))

        wrapped.reset()
        # BasicMultiAgent is hardcoded to run 25-step episodes.
        for i in range(25):
            wrapped.step({0: 0, 1: 0, 2: 0})

        # However VideoMonitor's _after_step is overwritten to not
        # use stats_recorder. So nothing to verify here, except that
        # it runs fine.


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
