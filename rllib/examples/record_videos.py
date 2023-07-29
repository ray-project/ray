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

algo = config.build()
algo.train()


# Secondly, we create videos with a custom recording_schedule:
def custom_schedule(episode_id: int) -> bool:
    # This will make it so that only every 10th episode is recorded.
    return episode_id % 10 == 0


config.environment(recording_schedule=custom_schedule)

algo = config.build()
algo.train()
