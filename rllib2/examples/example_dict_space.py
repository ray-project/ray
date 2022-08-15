"""
Example of supporting heterogeneous action spaces, a mixture of categorical and
"""

import gym
import numpy as np

import ray
from ray.rllib.algorithms.ppo import PPOConfig


class DictEnv(gym.Env):
    def __init__(self, *args, **kwargs):
        self.action_space = gym.spaces.Dict(
            {
                "robot_arm": gym.spaces.Box(
                    low=np.array([-1.0, -1.0]), high=np.array([1.0, 1.0])
                ),
                "gripper": gym.spaces.Discrete(n=3),
            }
        )

        self.observation_space = gym.spaces.Box(low=-np.ones(10), high=np.ones(10))
        super().__init__()

    def step(self, action):
        return self.observation_space.sample(), 1.0, False, {}

    def reset(self):
        return self.observation_space.sample()


def main():
    ray.init(local_mode=True)

    config = PPOConfig().framework("torch")

    trainer = config.build(env=DictEnv)
    print(trainer.workers.local_worker().policy_map["default_policy"].model)

    print("end")
    # for n in range(3):
    #     result = trainer.train()
    #     print(f"{n + 1}: {result['episode_reward_mean']}")


if __name__ == "__main__":
    main()
