from ray.tune.registry import register_env
from gymnasium.wrappers import RecordVideo
from PyFlyt.gym_envs import FlattenWaypointEnv
from gymnasium.wrappers import TransformReward

import gymnasium as gym
import PyFlyt.gym_envs # noqa

class RewardWrapper(gym.RewardWrapper):
    def __init__(self, env):
        super().__init__(env)
    def reward(self, reward):
        # Scale rewards:
        if reward >= 99.0 or reward <= -99.0:
            return reward / 10
        return reward

class QuadXWayPointsEnv(gym.Env):
    from gymnasium.wrappers import RecordVideo
    import PyFlyt.gym_envs # Must be here
    from PyFlyt.gym_envs import FlattenWaypointEnv
    from gymnasium.wrappers import TransformReward
    
    def __init__(self, config=None):
        env = gym.make("PyFlyt/QuadX-Waypoints-v1")
        # Wrap Environment to use max 10 and -10 for rewards
        env = RewardWrapper(env)
        
        self.env = FlattenWaypointEnv(env, context_length=1)