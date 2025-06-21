"""This script tests SAC discrete"""

from ray.rllib.algorithms.sac import SACConfig
import pprint

max_steps = 10000
config = SACConfig().environment("MountainCar-v0")

rl = config.build_algo()

for i in range(max_steps):
    result = rl.train()
    pprint.pprint(result)
