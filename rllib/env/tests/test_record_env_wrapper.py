import glob
import gym
import numpy as np
import os
import shutil
import unittest

from ray.rllib.env.utils import VideoMonitor, record_env_wrapper
from ray.rllib.examples.env.mock_env import MockEnv2
from ray.rllib.examples.env.multi_agent import BasicMultiAgent
from ray.rllib.utils.test_utils import check


class TestRecordEnvWrapper(unittest.TestCase):
    def test_wrap_gym_env(self):
        record_env_dir = os.popen("mktemp -d").read()[:-1]
        print(f"tmp dir for videos={record_env_dir}")

        if not os.path.exists(record_env_dir):
            sys.exit(1)

        num_steps_per_episode = 10
        wrapped = record_env_wrapper(
            env=MockEnv2(num_steps_per_episode),
            record_env=record_env_dir,
            log_dir="",
            policy_config={
                "in_evaluation": False,
            },
        )
        # Non MultiAgentEnv: Wrapper's type is wrappers.Monitor.
        self.assertTrue(isinstance(wrapped, gym.wrappers.Monitor))
        self.assertFalse(isinstance(wrapped, VideoMonitor))

        wrapped.reset()
        # Expect one video file to have been produced in the tmp dir.
        os.chdir(record_env_dir)
        ls = glob.glob("*.mp4")
        self.assertTrue(len(ls) == 1)
        # 10 steps for a complete episode.
        for i in range(num_steps_per_episode):
            wrapped.step(0)
        # Another episode.
        wrapped.reset()
        for i in range(num_steps_per_episode):
            wrapped.step(0)
        # Expect another video file to have been produced (2nd episode).
        ls = glob.glob("*.mp4")
        self.assertTrue(len(ls) == 2)

        # MockEnv2 returns a reward of 100.0 every step.
        # So total reward is 1000.0 per episode (10 steps).
        check(
            np.array([100.0, 100.0]) * num_steps_per_episode,
            wrapped.get_episode_rewards(),
        )
        # Erase all generated files and the temp path just in case,
        # as to not disturb further CI-tests.
        shutil.rmtree(record_env_dir)

    def test_wrap_multi_agent_env(self):
        record_env_dir = os.popen("mktemp -d").read()[:-1]
        print(f"tmp dir for videos={record_env_dir}")

        if not os.path.exists(record_env_dir):
            sys.exit(1)

        wrapped = record_env_wrapper(
            env=BasicMultiAgent(3),
            record_env=record_env_dir,
            log_dir="",
            policy_config={
                "in_evaluation": False,
            },
        )
        # Type is VideoMonitor.
        self.assertTrue(isinstance(wrapped, gym.wrappers.Monitor))
        self.assertTrue(isinstance(wrapped, VideoMonitor))

        wrapped.reset()

        # BasicMultiAgent is hardcoded to run 25-step episodes.
        for i in range(25):
            wrapped.step({0: 0, 1: 0, 2: 0})

        # Expect one video file to have been produced in the tmp dir.
        os.chdir(record_env_dir)
        ls = glob.glob("*.mp4")
        self.assertTrue(len(ls) == 1)

        # However VideoMonitor's _after_step is overwritten to not
        # use stats_recorder. So nothing to verify here, except that
        # it runs fine.


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
