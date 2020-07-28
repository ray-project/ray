import numpy as np
from gym.envs.mujoco import HalfCheetahEnv
import inspect


def get_all_function_arguments(function, locals):
    kwargs_dict = {}
    for arg in inspect.getfullargspec(function).kwonlyargs:
        if arg not in ["args", "kwargs"]:
            kwargs_dict[arg] = locals[arg]
    args = [locals[arg] for arg in inspect.getfullargspec(function).args]

    if "args" in locals:
        args += locals["args"]

    if "kwargs" in locals:
        kwargs_dict.update(locals["kwargs"])
    return args, kwargs_dict


class HalfCheetahWrapper(HalfCheetahEnv):
    """HalfCheetah Wrapper that wraps Mujoco Halfcheetah-v2 env
    with an additional defined reward function for model-based RL.

    This is currently used for MBMPO.
    """

    def __init__(self, *args, **kwargs):
        HalfCheetahEnv.__init__(self, *args, **kwargs)

    def reward(self, obs, action, obs_next):
        if obs.ndim == 2 and action.ndim == 2:
            assert obs.shape == obs_next.shape
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
        env.step(env.action_space.sample())
