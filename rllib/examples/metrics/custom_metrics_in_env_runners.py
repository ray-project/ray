"""TODO (sven)
"""
from collections import defaultdict
from typing import Optional, Sequence

import gymnasium as gym
import numpy as np

from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.utils.images import resize
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env
from ray import tune


class MsPacmanHeatmapCallback(DefaultCallbacks):
    """A custom callback to extract the position of MsPacman and log these in a heatmap.

    At each episode timestep, the current pacman (x/y)-position is determined and added
    to the episode's temporary storage. At the end of an episode, a simple 2D heatmap
    is created from this data and the heatmap is logged to the MetricsLogger (to be
    viewed in WandB).
    """

    def __init__(self, env_runner_indices: Optional[Sequence[int]] = None):
        """Initializes an MsPacmanHeatmapCallback instance.

        Args:
            env_runner_indices: The (optional) EnvRunner indices, for this callback
                should be active. If None, activates the heatmap for all EnvRunners.
                If a Sequence type, only logs/heatmaps, if the EnvRunner index is found
                in `env_runner_indices`.
        """
        super().__init__()
        # Only create heatmap on certain EnvRunner indices?
        self._env_runner_indices = env_runner_indices

        self._heatmaps = defaultdict(lambda: np.zeros((64, 64), dtype=np.int32))

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
        """Adds current pacman x/y-position to episode's temporary data."""

        # Skip, if this EnvRunner's index is not in `self._env_runner_indices`.
        if (
            self._env_runner_indices is not None
            and env_runner.worker_index not in self._env_runner_indices
        ):
            return

        # If we have a vector env, only render the sub-env at index 0.
        if isinstance(env.unwrapped, gym.vector.VectorEnv):
            image = env.envs[0].render()
        else:
            image = env.render()
        # Downsize to 64x64 for our utility function to work with.
        image = resize(image, 64, 64)
        xy_pos = self._get_pacman_xy_pos(image)
        episode.add_temporary_timestep_data("pacman_xy_pos", xy_pos)

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
        # Skip, if this EnvRunner's index is not in `self._env_runner_indices`.
        if (
            self._env_runner_indices is not None
            and env_runner.worker_index not in self._env_runner_indices
        ):
            return

        # Get all pacman x/y-positions from the episode.
        images = episode.get_temporary_timestep_data("render_images")
        # Create a video from the images by simply stacking them.
        video = np.expand_dims(
            np.stack(images, axis=0), axis=0
        )  # TODO: test to make sure WandB properly logs videos.
        # video = np.stack(images, axis=0)

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
        # For WandB videos, we need to put channels first.
        #image = np.transpose(image, axes=[2, 0, 1])

        # Log the best and worst video to MetricsLogger.
        metrics_logger.log_value(
            "episode_videos_best",
            self.best_episode_and_return[0],
            reduce=None,
            clear_on_reduce=True,
        )
        metrics_logger.log_value(
            "episode_videos_worst",
            self.worst_episode_and_return[0],
            reduce=None,
            clear_on_reduce=True,
        )
        # Reset our best/worst placeholders.
        self.best_episode_and_return = (None, float("-inf"))
        self.worst_episode_and_return = (None, float("inf"))

    def _get_pacman_xy_pos(self, image):
        # Define the yellow color range in RGB (Ms. Pac-Man is yellowish).
        # We allow some range around yellow to account for variation.
        yellow_lower = np.array([150, 150, 0], dtype=np.uint8)
        yellow_upper = np.array([255, 255, 100], dtype=np.uint8)
        # Create a mask that highlights the yellow pixels
        mask = np.all((image >= yellow_lower) & (image <= yellow_upper), axis=-1)
        # Find the coordinates of the yellow pixels
        yellow_pixels = np.argwhere(mask)
        if yellow_pixels.size == 0:
            return (0, 0)

        # Calculate the centroid of the yellow pixels to get Ms. Pac-Man's position
        y, x = yellow_pixels.mean(axis=0).astype(int)
        return (x, y)


parser = add_rllib_example_script_args(default_reward=450.0)


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
        # .environment("env", env_config={
        #    # Make analogous to old v4 + NoFrameskip.
        #    "frameskip": 1,
        #    "full_action_space": False,
        #    "repeat_action_probability": 0.0,
        # })
        .environment("CartPole-v1", env_config={"render_mode": "rgb_array"})
        # .rollouts(rollout_fragment_length=4000)#TODO: remove: only to show that a list of videos can be uploaded per iteration (b/c now we need 2 rollouts, producing 2 videos per str-key)
        .callbacks(EnvRenderCallback)
        .training(
            # TODO: Atari.
            model=dict(
                {},
            ),
        )
        .debugging(
            logger_config={
                "type": tune.logger.NoopLogger,
            }
        )
    )

    run_rllib_example_script_experiment(base_config, args)
