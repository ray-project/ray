from PyFlyt.gym_envs import FlattenWaypointEnv
import gymnasium as gym


class RewardWrapper(gym.RewardWrapper):
    def __init__(self, env):
        super().__init__(env)

    def reward(self, reward):
        # Scale rewards:
        if reward >= 99.0 or reward <= -99.0:
            return reward / 10
        return reward


class QuadXWayPointsEnv(gym.Env):
    import PyFlyt.gym_envs  # noqa

    def __init__(self, config=None):
        env = gym.make("PyFlyt/QuadX-Waypoints-v1")
        # Wrap Environment to use max 10 and -10 for rewards
        env = RewardWrapper(env)

        self.env = FlattenWaypointEnv(env, context_length=1)
