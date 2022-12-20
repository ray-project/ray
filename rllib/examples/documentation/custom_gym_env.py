# __rllib-custom-gym-env-begin__
import gym

import ray
from ray.rllib.algorithms.ppo import PPOConfig


class SimpleCorridor(gym.Env):
    def __init__(self, config):
        self.end_pos = config["corridor_length"]
        self.cur_pos = 0
        self.action_space = gym.spaces.Discrete(2)  # right/left
        self.observation_space = gym.spaces.Discrete(self.end_pos)

    def reset(self):
        self.cur_pos = 0
        return self.cur_pos

    def step(self, action):
        if action == 0 and self.cur_pos > 0:  # move right (towards goal)
            self.cur_pos -= 1
        elif action == 1:  # move left (towards start)
            self.cur_pos += 1
        if self.cur_pos >= self.end_pos:
            return 0, 1.0, True, {}
        else:
            return self.cur_pos, -0.1, False, {}


ray.init()

config = PPOConfig().environment(SimpleCorridor, env_config={"corridor_length": 5})
algo = config.build()

for _ in range(3):
    print(algo.train())

algo.stop()
# __rllib-custom-gym-env-end__
