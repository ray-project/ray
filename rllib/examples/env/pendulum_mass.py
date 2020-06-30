import numpy as np
import gym
from gym.envs.classic_control.pendulum import PendulumEnv
"""
Custom Meta-learning Environment Specifications:
1) Compatible with gym environment interface (check custom envs)
2) Requires sample_tasks(n_tasks): Returns n sampled tasks
3) Requires set_task(task): Sets environment task

Optional:
1) Modify step(action) function to incorporate task-specific reward
"""


class PendulumMassEnv(PendulumEnv, gym.utils.EzPickle):
    """PendulumMassEnv varies the weight of the pendulum

    Tasks are defined to be weight uniformly sampled between [0.5,2]
    """

    def sample_tasks(self, n_tasks):
        # Mass is a random float between 0.5 and 2
        return np.random.uniform(low=0.5, high=2.0, size=(n_tasks, ))

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
