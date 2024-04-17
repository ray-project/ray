"""TODO (sven)
"""
import gymnasium as gym
import numpy as np

from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env


class EnvRenderCallback(DefaultCallbacks):
    """A custom callback to render the environment.

    This can be used to create videos of the episodes for some or all EnvRunners
    and some or all env indices (in a vectorized env). These videos can then
    be sent to e.g. WandB as shown in this example script here.

    We override the `on_episode_step` method to create a single ts render image
    and temporarily store it in the Episode object.
    """
    def __init__(self):
        super().__init__()
        # Per sample round (on this EnvRunner), we want to only log the best- and
        # worst performing episode's videos in the custom metrics. Otherwise, too much
        # data would be sent to WandB.
        self.best_episode_and_return = (None, float("-inf"))
        self.worst_episode_and_return = (None, float("inf"))

    def on_episode_step(
        self,
        *,
        episode,
        env_runner,
        env,
        env_index,
        rl_module,
        **kwargs,
    ) -> None:
        """Adds render image to episode."""
        # If we have a vector env, only render the sub-env at index 0.
        if isinstance(env.unwrapped, gym.vector.VectorEnv):
            image = env.envs[0].render()
        else:
            image = env.render()
        episode.add_temporary_timestep_data("render_images", image)

    def on_episode_end(
        self,
        *,
        episode,
        env_runner,
        env,
        env_index,
        rl_module,
        **kwargs,
    ) -> None:
        # Get the episode's return.
        episode_return = episode.get_return()

        # Better than the best or worse than worst thus far?
        if (
            episode_return > self.best_episode_and_return[1]
            or episode_return < self.worst_episode_and_return[1]
        ):
            # Get all images of the episode.
            images = episode.get_temporary_timestep_data("render_images")
            # Create a video from the images by simply stacking them.
            video = np.stack(images, axis=0)

            if episode_return > self.best_episode_and_return[1]:
                self.best_episode_and_return = (video, episode_return)
            else:
                self.worst_episode_and_return = (video, episode_return)

    def on_sample_end(
        self,
        *,
        env_runner,
        metrics_logger,
        samples,
        **kwargs,
    ) -> None:
        # Log the best and worst video to MetricsLogger.
        env_runner.metrics.log_value(
            "episode_videos_best",
            self.best_episode_and_return[0],
            reduce=None,
        )
        env_runner.metrics.log_value(
            "episode_videos_worst",
            self.worst_episode_and_return[0],
            reduce=None,
        )
        # Reset our best/worst placeholders.
        self.best_episode_and_return = (None, float("-inf"))
        self.worst_episode_and_return = (None, float("inf"))


parser = add_rllib_example_script_args(
    default_iters=500,
    default_timesteps=500000,
    default_reward=-300.0,
)


if __name__ == "__main__":
    args = parser.parse_args()

    # Register our environment with tune.
    register_env(
        "env",
        lambda cfg: wrap_atari_for_new_api_stack(
            gym.make("ALE/MsPacman-v5", **cfg, **{"render_mode": "rgb_array"}),
            framestack=4,
        ),
    )

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        #.environment("env", env_config={
        #    # Make analogous to old v4 + NoFrameskip.
        #    "frameskip": 1,
        #    "full_action_space": False,
        #    "repeat_action_probability": 0.0,
        #})
        .environment("CartPole-v1", env_config={"render_mode": "rgb_array"})
        .callbacks(EnvRenderCallback)
        .training(
            # TODO: Atari.
            model=dict(
                {
                },
            ),
        )
    )

    run_rllib_example_script_experiment(base_config, args)
