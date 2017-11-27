from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import logging
import time

from ray.rllib.models import ModelCatalog

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_and_wrap(env_creator, options):
    env = env_creator()
    env = ModelCatalog.get_preprocessor_as_wrapper(env, options)
    env = Diagnostic(env)
    return env


class Diagnostic(gym.Wrapper):
    def __init__(self, env=None):
        super(Diagnostic, self).__init__(env)
        self.diagnostics = DiagnosticsLogger()

    def _reset(self):
        observation = self.env.reset()
        return self.diagnostics._after_reset(observation)

    def _step(self, action):
        results = self.env.step(action)
        return self.diagnostics._after_step(*results)


class DiagnosticsLogger(object):
    def __init__(self, log_interval=503):
        self._episode_time = time.time()
        self._last_time = time.time()
        self._local_t = 0
        self._log_interval = log_interval
        self._episode_reward = 0
        self._episode_length = 0
        self._all_rewards = []
        self._last_episode_id = -1

    def _after_reset(self, observation):
        logger.info("Resetting environment")
        self._episode_reward = 0
        self._episode_length = 0
        self._all_rewards = []
        return observation

    def _after_step(self, observation, reward, done, info):
        to_log = {}
        if self._episode_length == 0:
            self._episode_time = time.time()

        self._local_t += 1

        if self._local_t % self._log_interval == 0:
            cur_time = time.time()
            self._last_time = cur_time

        if reward is not None:
            self._episode_reward += reward
            if observation is not None:
                self._episode_length += 1
            self._all_rewards.append(reward)

        if done:
            logger.info("Episode terminating: episode_reward=%s "
                        "episode_length=%s",
                        self._episode_reward, self._episode_length)
            total_time = time.time() - self._episode_time
            to_log["global/episode_reward"] = self._episode_reward
            to_log["global/episode_length"] = self._episode_length
            to_log["global/episode_time"] = total_time
            to_log["global/reward_per_time"] = (self._episode_reward /
                                                total_time)
            self._episode_reward = 0
            self._episode_length = 0
            self._all_rewards = []

        return observation, reward, done, to_log
