import gymnasium as gym
import numpy as np

from ray.rllib.algorithms.dreamerv3.dreamerv3 import DreamerV3Config
from ray.rllib.core.columns import Columns
from ray.rllib.utils.framework import try_import_tf

tf1, tf, tfv = try_import_tf()


env_name = "CartPole-v1"
# Use the vector env API.
env = gym.vector.make(env_name, num_envs=1, asynchronous=False)

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


while not terminated and not truncated:
    # Use the RLModule for action computations directly.
    # DreamerV3 expects this particular batch format: obs, prev. states and the
    # `is_first` flag.
    batch = {
        # states is already batched (B=1)
        Columns.STATE_IN: states,
        # obs is already batched (due to vector env).
        Columns.OBS: tf.convert_to_tensor(obs),
        # set to True at beginning of episode.
        "is_first": tf.convert_to_tensor([is_first]),
    }
    outs = rl_module.forward_inference(batch)
    # Extract actions (which are in one hot format) and state-outs from outs
    actions = np.argmax(outs[Columns.ACTIONS].numpy(), axis=-1)
    states = outs[Columns.STATE_OUT]

    # Perform a step in the env.
    obs, reward, terminated, truncated, info = env.step(actions)
    # Not at the beginning of the episode anymore.
    is_first = 0.0
