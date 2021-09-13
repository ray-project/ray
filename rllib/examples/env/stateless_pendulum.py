from gym.envs.classic_control import PendulumEnv


class StatelessPendulum(PendulumEnv):
    """Partially observable variant of the Pendulum gym environment.

    https://github.com/openai/gym/blob/master/gym/envs/classic_control/
    pendulum.py

    We delete the angular velocity component of the state, so that it
    can only be solved by a memory enhanced model (policy).
    """

    def step(self, action):
        next_obs, reward, done, info = super().step(action)
        # next_obs is [cos(theta), sin(theta), theta-dot (angular velocity)]
        return next_obs[:-1], reward, done, info

    def reset(self):
        init_obs = super().reset()
        # init_obs is [cos(theta), sin(theta), theta-dot (angular velocity)]
        return init_obs[:-1]
