from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.ppo import PPOAgent
from ray import tune
import ray

if __name__ == "__main__":
    ray.init()
    # Test legacy *Agent classes work (renamed to Trainer)
    tune.run(
        PPOAgent,
        config={"env": "CartPole-v0"},
        stop={"training_iteration": 2})
