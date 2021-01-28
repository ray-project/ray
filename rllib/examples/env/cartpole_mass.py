import numpy as np
import gym
from gym.envs.classic_control.cartpole import CartPoleEnv
from ray.rllib.env.meta_env import MetaEnv


class CartPoleMassEnv(CartPoleEnv, gym.utils.EzPickle, MetaEnv):
    """CartPoleMassEnv varies the weights of the cart and the pole.
    """

    def sample_tasks(self, n_tasks):
        # Sample new cart- and pole masses (random floats between 0.5 and 2.0
        # (cart) and between 0.05 and 0.2 (pole)).
        cart_masses = np.random.uniform(low=0.5, high=2.0, size=(n_tasks, 1))
        pole_masses = np.random.uniform(low=0.05, high=0.2, size=(n_tasks, 1))
        return np.concatenate([cart_masses, pole_masses], axis=-1)

    def set_task(self, task):
        """
        Args:
            task (Tuple[float]): Masses of the cart and the pole.
        """
        self.masscart = task[0]
        self.masspole = task[1]

    def get_task(self):
        """
        Returns:
            Tuple[float]: The current mass of the cart- and pole.
        """
        return np.array([self.masscart, self.masspole])
