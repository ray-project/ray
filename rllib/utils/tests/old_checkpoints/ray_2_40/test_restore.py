import os

from ray.rllib.algorithms.ppo import PPO
from ray.rllib.utils.tests.old_checkpoints.ray_2_40.created_checkpoint import config

# Get the directory of the current script
script_directory = os.path.dirname(os.path.abspath(__file__))

# Restore the algorithm from the (old) msgpack-checkpoint, using the current
# Ray version's `config` object.
algo = PPO.from_checkpoint(path=script_directory, config=config)
results = algo.train()

print(f"Algorithm restored and trained once. Results={results}.")
