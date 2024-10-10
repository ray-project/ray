"""Example of adding custom metrics to the results returned by `EnvRunner.sample()`.


We use the `MetricsLogger` class, which RLlib provides inside all its components (only
when using the new API stack via `config.experimental(_enable_new_api_stack=True)`),
and which offers a unified API to log individual values per iteration, per episode
timestep, per episode (as a whole), per loss call, etc..
`MetricsLogger` objects are available in all custom API code, for example inside your
custom `Algorithm.training_step()` methods, custom loss functions, custom callbacks,
and custom EnvRunners.

In this script, we define a custom `DefaultCallbacks` class and then override its
`on_train_result()` method in order to alter the final `ResultDict` before it is sent
back to Ray Tune (and possibly a WandB logger).

For demonstration purposes only, we add the following simple metrics to this
`ResultDict`:
- The ratio of the delta-times for sampling vs. training. If this ratio is roughly 1.0,
it means we are spending roughly as much time sampling than we do updating our model.
- The number of times an episode reaches an overall return of > 100.0 and the number
of times an episode stays below an overall return of 12.0.
-
"""
from typing import Optional, Sequence

import gymnasium as gym
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize
import numpy as np

from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.utils.images import resize
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env


class MsPacmanHeatmapCallback(DefaultCallbacks):
    """A custom callback to extract information from MsPacman and log these.

    This callback logs:
    - the positions of MsPacman over an episode to produce heatmaps from this data.
    At each episode timestep, the current pacman (y/x)-position is determined and added
    to the episode's temporary storage. At the end of an episode, a simple 2D heatmap
    is created from this data and the heatmap is logged to the MetricsLogger (to be
    viewed in WandB).
    - the max distance travelled by MsPacman per episode, then averaging these max
    values over a window of size=100.
    - the mean distance travelled by MsPacman per episode (over an infinite window).
    - the number of lifes of MsPacman EMA-smoothed over time.

    This callback can be setup to only log stats on certain EnvRunner indices through
    the `env_runner_indices` c'tor arg.
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

        # Mapping from episode ID to max distance travelled thus far.
        self._episode_start_position = {}

    def on_episode_start(
        self,
        *,
        episode,
        env_runner,
        metrics_logger,
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

        yx_pos = self._get_pacman_yx_pos(env)
        self._episode_start_position[episode.id_] = yx_pos

    def on_episode_step(
        self,
        *,
        episode,
        env_runner,
        metrics_logger,
        env,
        env_index,
        rl_module,
        **kwargs,
    ) -> None:
        """Adds current pacman y/x-position to episode's temporary data."""

        # Skip, if this EnvRunner's index is not in `self._env_runner_indices`.
        if (
            self._env_runner_indices is not None
            and env_runner.worker_index not in self._env_runner_indices
        ):
            return

        yx_pos = self._get_pacman_yx_pos(env)
        episode.add_temporary_timestep_data("pacman_yx_pos", yx_pos)

        # Compute distance to start position.
        dist_travelled = np.sqrt(np.sum(np.square(
            np.array(self._episode_start_position[episode.id_]) -
            np.array(yx_pos)
        )))
        episode.add_temporary_timestep_data("pacman_dist_travelled", dist_travelled)

    def on_episode_end(
        self,
        *,
        episode,
        env_runner,
        metrics_logger,
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

        # Erase the start position record.
        del self._episode_start_position[episode.id_]

        # Get all pacman y/x-positions from the episode.
        yx_positions = episode.get_temporary_timestep_data("pacman_yx_pos")
        # h x w
        heatmap = np.zeros((80, 100), dtype=np.int32)
        for yx_pos in yx_positions:
            if yx_pos != (-1, -1):
                heatmap[yx_pos[0], yx_pos[1]] += 1

        # Create the actual heatmap image.
        # Normalize the heatmap to values between 0 and 1
        norm = Normalize(vmin=heatmap.min(), vmax=heatmap.max())
        # Use a colormap (e.g., 'hot') to map normalized values to RGB
        colormap = plt.get_cmap("coolwarm")  # try "hot" and "viridis" as well?
        # Returns a (64, 64, 4) array (RGBA).
        heatmap_rgb = colormap(norm(heatmap))
        # Convert RGBA to RGB by dropping the alpha channel and converting to uint8.
        heatmap_rgb = (heatmap_rgb[:, :, :3] * 255).astype(np.uint8)
        # Log the image.
        metrics_logger.log_value(
            "pacman_heatmap",
            heatmap_rgb,
            reduce=None,
            window=10,  # Log 10 images at most per EnvRunner/training iteration.
        )

        # Get the max distance travelled for this episode.
        dist_travelled = np.max(
            episode.get_temporary_timestep_data("pacman_dist_travelled")
        )

        # Log the max. dist travelled in this episode (window=100).
        metrics_logger.log_value(
            "pacman_max_dist_travelled",
            dist_travelled,
            # For future reductions (e.g. over n different episodes and all the
            # data coming from other env runners), reduce by max.
            reduce="max",
            # Always keep the last 100 values and max over this window.
            # Note that this means that over time, if the values drop to lower
            # numbers again, the reported `pacman_max_dist_travelled` might also
            # decrease again (meaning `window=100` makes this not a "lifetime max").
            window=100,
        )

        # Log the average dist travelled per episode (window=inf).
        metrics_logger.log_value(
            "pacman_max_dist_travelled",
            dist_travelled,
            reduce="mean",  # <- default
            window=float("inf"),  # never forget anything, average over lifetime
        )

        # Log the number of lifes (as EMA-smoothed; no window).
        metrics_logger.log_value(
            "pacman_lifes",
            episode.get_infos(-1)["lives"],
            reduce="mean",  # <- default (must be "mean" for EMA smothing)
            ema_coeff=0.01,  # <- default EMA coefficient (`window` must be None)
        )

    def _get_pacman_yx_pos(self, env):
        # If we have a vector env, only render the sub-env at index 0.
        if isinstance(env.unwrapped, gym.vector.VectorEnv):
            image = env.envs[0].render()
        else:
            image = env.render()
        # Downsize to 100x100 for our utility function to work with.
        image = resize(image, 100, 100)
        # Crop image at bottom 20% (where lives are shown, which may confuse the pacman
        # detector).
        image = image[:80]
        # Define the yellow color range in RGB (Ms. Pac-Man is yellowish).
        # We allow some range around yellow to account for variation.
        yellow_lower = np.array([200, 130, 65], dtype=np.uint8)
        yellow_upper = np.array([220, 175, 105], dtype=np.uint8)
        # Create a mask that highlights the yellow pixels
        mask = np.all((image >= yellow_lower) & (image <= yellow_upper), axis=-1)
        # Find the coordinates of the yellow pixels
        yellow_pixels = np.argwhere(mask)
        if yellow_pixels.size == 0:
            return (-1, -1)

        # Calculate the centroid of the yellow pixels to get Ms. Pac-Man's position
        y, x = yellow_pixels.mean(axis=0).astype(int)
        return y, x


parser = add_rllib_example_script_args(default_reward=450.0)
parser.set_defaults(enable_new_api_stack=True)


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
        .environment(
            "env",
            env_config={
                # Make analogous to old v4 + NoFrameskip.
                "frameskip": 1,
                "full_action_space": False,
                "repeat_action_probability": 0.0,
            },
        )
        .callbacks(MsPacmanHeatmapCallback)
        .training(
            # Make learning time fast, but note that this example may not
            # necessarily learn well (its purpose is to demo the
            # functionality of callbacks and the MetricsLogger).
            train_batch_size_per_learner=2000,
            minibatch_size=512,
            num_epochs=6,
        )
        .rl_module(
            model_config_dict={
                "vf_share_layers": True,
                "conv_filters": [[16, 4, 2], [32, 4, 2], [64, 4, 2], [128, 4, 2]],
                "conv_activation": "relu",
                "post_fcnet_hiddens": [256],
            }
        )
    )

    run_rllib_example_script_experiment(base_config, args)
