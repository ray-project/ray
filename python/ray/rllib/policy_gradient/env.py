from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import numpy as np

from ray.rllib.models import ModelCatalog


class BatchedEnv(object):
    """This holds multiple gym envs and performs steps on all of them."""
    def __init__(self, name, batchsize):
        self.envs = [gym.make(name) for _ in range(batchsize)]
        self.observation_space = self.envs[0].observation_space
        self.action_space = self.envs[0].action_space
        self.batchsize = batchsize
        self.preprocessor = ModelCatalog.get_preprocessor(
            name, self.envs[0].observation_space.shape)

    def reset(self):
        observations = [
            self.preprocessor.transform(env.reset()) for env in self.envs]
        self.shape = observations[0].shape
        self.dones = [False for _ in range(self.batchsize)]
        return np.vstack(observations)

    def step(self, actions, render=False):
        observations = []
        rewards = []
        for i, action in enumerate(actions):
            if self.dones[i]:
                observations.append(np.zeros(self.shape))
                rewards.append(0.0)
                continue
            observation, reward, done, info = self.envs[i].step(action)
            if render:
                self.envs[0].render()
            observations.append(self.preprocessor.transform(observation))
            rewards.append(reward)
            self.dones[i] = done
        return (np.vstack(observations), np.array(rewards, dtype="float32"),
                np.array(self.dones))
