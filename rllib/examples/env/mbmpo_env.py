from gym.envs.classic_control import PendulumEnv, CartPoleEnv
import numpy as np

# MuJoCo may not be installed.
HalfCheetahEnv = HopperEnv = None

try:
    from gym.envs.mujoco import HalfCheetahEnv, HopperEnv
except Exception:
    pass


class CartPoleWrapper(CartPoleEnv):
    """Wrapper for the Cartpole-v0 environment.

    Adds an additional `reward` method for some model-based RL algos (e.g.
    MB-MPO).
    """

    def reward(self, obs, action, obs_next):
        # obs = batch * [pos, vel, angle, rotation_rate]
        x = obs_next[:, 0]
        theta = obs_next[:, 2]

        # 1.0 if we are still on, 0.0 if we are terminated due to bounds
        # (angular or x-axis) being breached.
        rew = 1.0 - ((x < -self.x_threshold) | (x > self.x_threshold) |
                     (theta < -self.theta_threshold_radians) |
                     (theta > self.theta_threshold_radians)).astype(np.float32)

        return rew


class PendulumWrapper(PendulumEnv):
    """Wrapper for the Pendulum-v0 environment.

    Adds an additional `reward` method for some model-based RL algos (e.g.
    MB-MPO).
    """

    def reward(self, obs, action, obs_next):
        # obs = [cos(theta), sin(theta), dtheta/dt]
        # To get the angle back from obs: atan2(sin(theta), cos(theta)).
        theta = np.arctan2(
            np.clip(obs[:, 1], -1.0, 1.0), np.clip(obs[:, 0], -1.0, 1.0))
        # Do everything in (B,) space (single theta-, action- and
        # reward values).
        a = np.clip(action, -self.max_torque, self.max_torque)[0]
        costs = self.angle_normalize(theta) ** 2 + \
            0.1 * obs[:, 2] ** 2 + 0.001 * (a ** 2)
        return -costs

    @staticmethod
    def angle_normalize(x):
        return (((x + np.pi) % (2 * np.pi)) - np.pi)


class HalfCheetahWrapper(HalfCheetahEnv or object):
    """Wrapper for the MuJoCo HalfCheetah-v2 environment.

    Adds an additional `reward` method for some model-based RL algos (e.g.
    MB-MPO).
    """

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


class HopperWrapper(HopperEnv or object):
    """Wrapper for the MuJoCo Hopper-v2 environment.

    Adds an additional `reward` method for some model-based RL algos (e.g.
    MB-MPO).
    """

    def reward(self, obs, action, obs_next):
        alive_bonus = 1.0
        assert obs.ndim == 2 and action.ndim == 2
        assert (obs.shape == obs_next.shape
                and action.shape[0] == obs.shape[0])
        vel = obs_next[:, 5]
        ctrl_cost = 1e-3 * np.sum(np.square(action), axis=1)
        reward = vel + alive_bonus - ctrl_cost
        return np.minimum(np.maximum(-1000.0, reward), 1000.0)


if __name__ == "__main__":
    env = PendulumWrapper()
    env.reset()
    for _ in range(100):
        env.step(env.action_space.sample())
        env.render()
