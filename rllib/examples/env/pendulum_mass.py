import numpy as np
import gym
from gym.spaces import Box
import inspect
import sys
from gym.envs.registration import registry, register, make, spec
from gym.envs.classic_control.pendulum import PendulumEnv

class PendulumMassEnv(PendulumEnv, gym.utils.EzPickle):

    def sample_tasks(self, n_tasks):
        # Mass is a random float between 0.5 and 2
        return np.random.uniform(low=0.5, high=2.0, size=(n_tasks,))

    def set_task(self, task):
        """
        Args:
            task: task of the meta-learning environment
        """
        self.m = task

    def get_task(self):
        """
        Returns:
            task: task of the meta-learning environment
        """
        return self.m

    def __str__(self):
        return 'PendulumMassEnv'