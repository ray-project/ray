from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import cv2
import gym
from gym.spaces.box import Box
from gym import spaces
import logging
import numpy as np
import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def create_env(env_id):
    env = gym.make(env_id)
    env = Processing(env)
    return env

def _process_frame42(frame):
    frame = frame[34:34+160, :160]
    # Resize by half, then down to 42x42 (essentially mipmapping). If
    # we resize directly we lose pixels that, when mapped to 42x42,
    # aren't close enough to the pixel boundary.
    frame = cv2.resize(frame, (80, 80))
    frame = cv2.resize(frame, (42, 42))
    frame = frame.mean(2)
    frame = frame.astype(np.float32)
    frame *= (1.0 / 255.0)
    frame = np.reshape(frame, [42, 42, 1])
    return frame

class Processing(gym.Wrapper):
    def __init__(self, env=None):
        super(Processing, self).__init__(env)
        self.observation_space = Box(0.0, 1.0, [42, 42, 1])
        self.diagnostics = Diagnostics()

    def _observation(self, observation):
        return _process_frame42(observation)

    def _after_reset(self, observation):
        return self.diagnostics._after_reset(observation)

    def _after_step(self, observation, reward, done, info):
        return self.diagnostics._after_step(observation, reward, done, info)


class Diagnostics():
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
        logger.info('Resetting environment')
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
            elapsed = cur_time - self._last_time
            fps = self._log_interval / elapsed
            self._last_time = cur_time

        if reward is not None:
            self._episode_reward += reward
            if observation is not None:
                self._episode_length += 1
            self._all_rewards.append(reward)

        if done:
            logger.info('Episode terminating: episode_reward=%s episode_length=%s', self._episode_reward, self._episode_length)
            total_time = time.time() - self._episode_time
            to_log["global/episode_reward"] = self._episode_reward
            to_log["global/episode_length"] = self._episode_length
            to_log["global/episode_time"] = total_time
            to_log["global/reward_per_time"] = self._episode_reward / total_time
            self._episode_reward = 0
            self._episode_length = 0
            self._all_rewards = []

        return observation, reward, done, to_log

