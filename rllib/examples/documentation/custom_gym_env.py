# __rllib-custom-gym-env-begin__
import gym

import ray
from ray.rllib.agents import ppo

from ray.rllib.examples.documentation.simple_corridor import SimpleCorridorImported


class SimpleCorridorOriginal(gym.Env):
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
config_old = {
    "env": SimpleCorridorOriginal,
    "env_config": {
        "corridor_length": 5,
    },
}
config_new = {
    "env": SimpleCorridorImported,
    "env_config": {
        "corridor_length": 5,
    },
}
trainer = ppo.PPOTrainer(config=config_new)  # this works
trainer = ppo.PPOTrainer(config=config_old)  # this fails

# the difference between the 2 is that one config uses the environment imported from
# another file. the other one uses the original environment declared inside of the
# launcher

# you will need to have gym==0.23.1 and remote sampling workers in order to produce
# the bug.

# __rllib-custom-gym-env-end__
