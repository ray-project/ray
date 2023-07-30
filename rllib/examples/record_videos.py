"""
The following example demonstrates how to record videos of your agent's behavior.

RLlib exposes the ability of the Gymnasium API to record videos.
This is done internally by wrapping the environment with the
gymnasium.wrappers.RecordVideo wrapper. You can also wrap your environment with this
wrapper manually to record videos of your agent's behavior if RLlib's built-in
video recording does not meet your needs.

In order to run this example, please regard the following:
- You must have moviepy installed (pip install moviepy)
- You must have ffmpeg installed (system dependent, e.g. brew install ffmpeg)
- moviepy must find ffmpeg -> https://github.com/Zulko/moviepy/issues/1158.
- An environment can only be recorded if it can be rendered. For most environments,
    this can be achieved by setting the render_mode to 'rgb_array' in the environment
    config.
"""

# First, we create videos with default settings:
from ray.rllib.algorithms.ppo import PPOConfig

config = PPOConfig().environment(
    env="CartPole-v1", record=True, env_config={"render_mode": "rgb_array"}
)

# By default, videos will be saved to your experiment logs directory under
# ~/ray_results.

# Secondly, we create videos every 100 episodes::
config.environment(recording_interval=100)
algo = config.build()
algo.train()

algo = config.build()
algo.train()
