from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
from gym.spaces.box import Box
import logging
import time

from ray.rllib.models import ModelCatalog

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_env(env_id, options):
    env = gym.make(env_id)
    env = RLLibPreprocessing(env_id, env, options)
    env = Diagnostic(env)
    return env


class RLLibPreprocessing(gym.ObservationWrapper):
    def __init__(self, env_id, env=None, options=dict()):
        super(RLLibPreprocessing, self).__init__(env)
        self.preprocessor = ModelCatalog.get_preprocessor(
            env_id, env.observation_space.shape, options)
        self._process_shape = self.preprocessor.transform_shape(
            env.observation_space.shape)
        self.observation_space = Box(-1.0, 1.0, self._process_shape)

    def _observation(self, observation):
        return self.preprocessor.transform(observation).squeeze(0)


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
