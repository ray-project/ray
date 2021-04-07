import numpy as np
import gym
from gym.envs.classic_control.pendulum import PendulumEnv
from ray.rllib.env.meta_env import MetaEnv


class PendulumMassEnv(PendulumEnv, gym.utils.EzPickle, MetaEnv):
    """PendulumMassEnv varies the weight of the pendulum

    Tasks are defined to be weight uniformly sampled between [0.5,2]
    """

    def sample_tasks(self, n_tasks):
        # Sample new pendulum masses (random floats between 0.5 and 2).
        return np.random.uniform(low=0.5, high=2.0, size=(n_tasks, ))

    def set_task(self, task):
        """
        Args:
            task (float): Task of the meta-learning environment (here: mass of
                the pendulum).
        """
        # self.m is the mass property of the pendulum.
        self.m = task

    def get_task(self):
        """
        Returns:
            float: The current mass of the pendulum (self.m in the PendulumEnv
                object).
        """
        return self.m
