import gymnasium as gym
import numpy as np

import tree  # pip install dm_tree

from ray.rllib.algorithms.dreamerv3.dreamerv3 import DreamerV3Config
from ray.rllib.core.columns import Columns
from ray.rllib.utils.framework import convert_to_tensor


env_name = "CartPole-v1"
# Use the vector env API.
env = gym.make_vec(env_name, num_envs=1, vectorization_mode="sync")

terminated = truncated = False
# Reset the env.
obs, _ = env.reset()
# Every time, we start a new episode, we should set is_first to True for the upcoming
# action inference.
is_first = 1.0

# Create the algorithm from a simple config.
config = (
    DreamerV3Config()
    .environment("CartPole-v1")
    .training(model_size="XS", training_ratio=1024)
)
algo = config.build()

# Extract the actual RLModule from the local (Dreamer) EnvRunner.
rl_module = algo.env_runner.module
# Get initial states from RLModule (note that these are always B=1, so this matches
# our num_envs=1; if you are using a vector env >1, you would have to repeat the
# returned states `num_env` times to get the correct batch size):
states = rl_module.get_initial_state()
# Batch the states to B=1.
states = tree.map_structure(lambda s: s.unsqueeze(0), states)

while not terminated and not truncated:
    # Use the RLModule for action computations directly.
    # DreamerV3 expects this particular batch format:
    # obs=[B, T, ...]
    # prev. states=[B, ...]
    # `is_first`=[B]
    batch = {
        # States is already batched (see above).
        Columns.STATE_IN: states,
        # `obs` is already batched (due to vector env), but needs time-rank.
        Columns.OBS: convert_to_tensor(obs, framework="torch")[None],
        # Set to True at beginning of episode.
        "is_first": convert_to_tensor(is_first, "torch")[None],
    }
    outs = rl_module.forward_inference(batch)
    # Alternatively, call `forward_exploration` in case you want stochastic, non-greedy
    # actions.
    # outs = rl_module.forward_exploration(batch)

    # Extract actions (remove time-rank) from outs.
    actions = outs[Columns.ACTIONS].numpy()[0]
    # Extract states from out. States are returned as batched.
    states = outs[Columns.STATE_OUT]

    # Perform a step in the env. Note that actions are still batched, which
    # is ok, because we have a vector env.
    obs, reward, terminated, truncated, info = env.step(actions)
    # Not at the beginning of the episode anymore.
    is_first = 0.0
