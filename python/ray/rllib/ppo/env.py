from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

from ray.rllib.models import ModelCatalog


class BatchedEnv(object):
    """This holds multiple gym envs and performs steps on all of them."""
    def __init__(self, env_creator, batchsize, options):
        self.envs = [env_creator() for _ in range(batchsize)]
        self.observation_space = self.envs[0].observation_space
        self.action_space = self.envs[0].action_space
        self.batchsize = batchsize
        self.preprocessor = ModelCatalog.get_preprocessor(
            self.envs[0], options["model"])
        self.extra_frameskip = options.get("extra_frameskip", 1)
        assert self.extra_frameskip >= 1

    def reset(self):
        observations = [
            self.preprocessor.transform(env.reset())[None]
            for env in self.envs]
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
            reward = 0.0
            for j in range(self.extra_frameskip):
                observation, r, done, info = self.envs[i].step(action)
                reward += r
                if done:
                    break
            if render:
                self.envs[0].render()
            observations.append(self.preprocessor.transform(observation)[None])
            rewards.append(reward)
            self.dones[i] = done
        return (np.vstack(observations), np.array(rewards, dtype="float32"),
                np.array(self.dones))
