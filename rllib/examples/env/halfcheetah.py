import numpy as np
import warnings
from gym.envs.mujoco import HalfCheetahEnv
import inspect


def get_all_function_arguments(function, locals):
    """

    :param function: callable function object
    :param locals: local variable scope of the function
    :return:
        args: list or arguments
        kwargs_dict: dict of kwargs

    """
    kwargs_dict = {}
    for arg in inspect.getfullargspec(function).kwonlyargs:
        if arg not in ['args', 'kwargs']:
            kwargs_dict[arg] = locals[arg]
    args = [locals[arg] for arg in inspect.getfullargspec(function).args]

    if 'args' in locals:
        args += locals['args']

    if 'kwargs' in locals:
        kwargs_dict.update(locals['kwargs'])
    return args, kwargs_dict


class HalfCheetahWrapper(HalfCheetahEnv):


    def __init__(self, *args, **kwargs):
        """
        Half-Cheetah environment with randomized mujoco parameters
        :param log_scale_limit: lower / upper limit for uniform sampling in logspace of base 2
        :param random_seed: random seed for sampling the mujoco model params
        :param fix_params: boolean indicating whether the mujoco parameters shall be fixed
        :param rand_params: mujoco model parameters to sample
        """
        args_all, kwargs_all = get_all_function_arguments(self.__init__, locals())
        HalfCheetahEnv.__init__(self, *args, **kwargs)

    def reward(self, obs, action, obs_next):
        if obs.ndim == 2 and action.ndim == 2:
            assert obs.shape == obs_next.shape and action.shape[0] == obs.shape[0]
            forward_vel = obs_next[:, 8]
            ctrl_cost = 0.1 * np.sum(np.square(action), axis=1)
            reward = forward_vel - ctrl_cost
            return np.minimum(np.maximum(-1000.0, reward), 1000.0)
        else:
            forward_vel = obs_next[8]
            ctrl_cost = 0.1 * np.square(action).sum()
            reward = forward_vel - ctrl_cost
            return np.minimum(np.maximum(-1000.0, reward), 1000.0)


if __name__ == "__main__":
    env = HalfCheetahWrapper()
    env.reset()
    for _ in range(1000):
        env.step(env.action_space.sample())  # take a random action
