from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import logging
import time
import numpy as np
import tensorflow as tf

from ray.rllib.models import ModelCatalog
from ray.rllib.utils.overrides import overrides

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_and_wrap(env_creator, options):
    env = env_creator()
    env = ModelCatalog.get_preprocessor_as_wrapper(env, options)
    #env = RayEnv(env)
    env = Diagnostic(env)
    return env

class RayEnv(gym.Wrapper):
    """
    Wrapper that takes the observation spaces and flattens and then concatenates them (if there are multiple)
    This allows for easy passing around of multiagent spaces without requiring a list
    """
    def __init__(self, env):
        self.input_shaper = Reshaper(env.observation_space)
        self.output_shaper = Reshaper(env.action_space)
        super(RayEnv, self).__init__(env)
        if isinstance(env.observation_space, list):
            self.n_agents = len(env.observation_space)
        else:
            self.n_agents = 1
        # temp
        self.observation_space = self.input_shaper.get_flat_box()
        self.action_space = self.output_shaper.get_flat_box()

    # @property
    # @overrides
    # def observation_space(self):
    #     return self.input_shaper.get_flat_box()
    #
    # @property
    # @overrides
    # def action_space(self):
    #     return self.output_shaper.get_flat_box()

    def split_input_tensor(self, tensor, axis=1):
        return self.input_shaper.split_tensor(tensor, axis)

    def split_output_tensor(self, tensor, axis=1):
        return self.output_shaper.split_tensor(tensor, axis)

    def split_output_number(self, number):
        return self.output_shaper.split_number(number)

    def split_along_agents(self, tensor, axis=-1):
        return tf.split(tensor, num_or_size_splits=self.n_agents, axis=axis)


    def get_action_dims(self):
        return self.output_shaper.get_slice_lengths()

    # need to overwrite step to flatten the observations
    def step(self, action):
        observation, reward, done, info = self.env.step(action)
        observation = np.asarray(observation).reshape(self.observation_space.shape[0])
        return observation, reward, done, info

    def reset(self):
        observation = np.asarray(self.env.reset())
        observation = observation.reshape(self.observation_space.shape[0])
        return observation


# FIXME (move this elsewhere in a bit)
class Reshaper(object):
    """
    This class keeps track of where in the flattened observation space we should be slicing and what the
    new shapes should be
    """
    # TODO(ev) support discrete action spaces
    def __init__(self, env_space):
        self.shapes = []
        self.slice_positions = []
        self.env_space = env_space
        if isinstance(env_space, list):
            for space in env_space:
                arr_shape = np.asarray(space.shape)
                self.shapes.append(arr_shape)
                if len(self.slice_positions) == 0:
                    self.slice_positions.append(np.product(arr_shape))
                else:
                    self.slice_positions.append(np.product(arr_shape) + self.slice_positions[-1])
        else:
            self.shapes.append(np.asarray(env_space.shape))
            self.slice_positions.append(np.product(env_space.shape))


    def get_flat_shape(self):
        import ipdb; ipdb.set_trace()
        return self.slice_positions[-1]


    def get_slice_lengths(self):
        diffed_list = np.diff(self.slice_positions).tolist()
        diffed_list.insert(0, self.slice_positions[0])
        return np.asarray(diffed_list)


    def get_flat_box(self):
        lows = []
        highs = []
        if isinstance(self.env_space, list):
            for i in range(len(self.env_space)):
                lows += self.env_space[i].low.tolist()
                highs += self.env_space[i].high.tolist()
            return gym.spaces.Box(np.asarray(lows), np.asarray(highs))
        else:
            return gym.spaces.Box(self.env_space.low, self.env_space.high)


    def split_tensor(self, tensor, axis=-1):
        # FIXME (ev) brittle
        # also, if its not a tes
        slice_rescale = int(tensor.shape.as_list()[axis] / int(np.sum(self.get_slice_lengths())))
        return tf.split(tensor, slice_rescale*self.get_slice_lengths(), axis=axis)


    def split_number(self, number):
        slice_rescale = int(number / int(np.sum(self.get_slice_lengths())))
        return slice_rescale*self.get_slice_lengths()


    def split_agents(self, tensor, axis=-1):
        return tf.split(tensor)

    # TODO add method to convert back to the original space




class Diagnostic(RayEnv):
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


