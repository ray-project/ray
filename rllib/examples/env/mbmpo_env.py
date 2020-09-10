import numpy as np
from gym.envs.mujoco import HalfCheetahEnv, HopperEnv


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


class HopperWrapper(HopperEnv):
    """Hopper Wrapper that wraps Mujoco Hopper-v2 env
    with an additional defined reward function for model-based RL.

    This is currently used for MBMPO.
    """

    def __init__(self, *args, **kwargs):
        HopperEnv.__init__(self, *args, **kwargs)

    def reward(self, obs, action, obs_next):
        alive_bonus = 1.0
        assert obs.ndim == 2 and action.ndim == 2
        assert obs.shape == obs_next.shape and action.shape[0] == obs.shape[0]
        vel = obs_next[:, 5]
        ctrl_cost = 1e-3 * np.sum(np.square(action), axis=1)
        reward = vel + alive_bonus - ctrl_cost
        return np.minimum(np.maximum(-1000.0, reward), 1000.0)


if __name__ == "__main__":
    env = HopperWrapper()
    env.reset()
    for _ in range(1000):
        env.step(env.action_space.sample())
