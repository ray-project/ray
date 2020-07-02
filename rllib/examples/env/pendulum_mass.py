import numpy as np
import gym
from gym.envs.classic_control.pendulum import PendulumEnv
from ray.rllib.env.meta_env import MetaEnv

"""
Custom Meta-learning Environment Specifications:
1) Compatible with gym environment interface 
2) Requires sample_tasks(n_tasks): Returns n sampled tasks
3) Requires set_task(task): Sets environment task

Optional:
1) Modify step(action) function to incorporate task-specific reward
"""

class PendulumMassEnv(PendulumEnv, gym.utils.EzPickle, MetaEnv):
    """PendulumMassEnv varies the weight of the pendulum

    Tasks are defined to be weight uniformly sampled between [0.5,2]
    """
    @override(MetaEnv)
    def sample_tasks(self, n_tasks):
        # Mass is a random float between 0.5 and 2
        return np.random.uniform(low=0.5, high=2.0, size=(n_tasks, ))

    @override(MetaEnv)
    def set_task(self, task):
        """
        Args:
            task: task of the meta-learning environment
        """
        self.m = task

    @override(MetaEnv)
    def get_task(self):
        """
        Returns:
            task: task of the meta-learning environment
        """
        return self.m
