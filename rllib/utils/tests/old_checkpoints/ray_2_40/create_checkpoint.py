# This script used to create a msgpack-checkpoint in the same directory.

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig

config = (
    PPOConfig()
    .environment("CartPole-v1")
    .rl_module(model_config=DefaultModelConfig(fcnet_hiddens=[16]))
)

algo = config.build()
algo.train()

checkpoint_path = algo.save_to_path(use_msgpack=True)
print(f"Algorithm checkpoint created at\n{checkpoint_path}.")
