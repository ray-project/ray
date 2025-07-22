"""Example of using a custom Callback to render and log episode videos from a gym.Env.

This example:
    - shows how to set up your (Atari) gym.Env for human-friendly rendering inside the
    `AlgorithmConfig.environment()` method.
    - demonstrates how to write an RLlib custom callback class that renders all envs on
    all timesteps, stores the individual images temporarily in the Episode
    objects, and compiles a video from these images once the Episode terminates.
    - furthermore, in each sampling cycle (iteration), the callback uses the unified
    `MetricsLogger` facility - available in all RLlib components - to log the video of
    the best performing and worst performing episode and sends these videos to WandB.
    - configures the above callbacks class within the AlgorithmConfig.


How to run this script
----------------------
`python [script file name].py --env [env name e.g. 'ALE/Pong-v5']
--wandb-key=[your WandB API key] --wandb-project=[some WandB project name]
--wandb-run-name=[optional: WandB run name within --wandb-project]`

In order to see the actual videos, you need to have a WandB account and provide your
API key and a project name on the command line (see above). To log the videos in WandB
you need to have the `wandb` and `moviepy` packages installed (`pip install wandb
moviepy`).

Use the `--env` flag to control, which Atari env is used. Note that this example
only works with Atari envs.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.


Results to expect
-----------------
After the first training iteration, you should see the videos in your WandB account
under the provided `--wandb-project` name. Filter for "videos_best" or "videos_worst".

Note that the default Tune TensorboardX (TBX) logger might complain about the videos
being logged. This is ok, the TBX logger will simply ignore these. The WandB logger,
however, will recognize the video tensors shaped
(1 [batch], T [video len], 3 [rgb], [height], [width]) and properly create a WandB video
object to be sent to their server.

Your terminal output should look similar to this:
+---------------------+----------+-----------------+--------+------------------+
| Trial name          | status   | loc             |   iter |   total time (s) |
|                     |          |                 |        |                  |
|---------------------+----------+-----------------+--------+------------------+
| PPO_env_8d3f3_00000 | RUNNING  | 127.0.0.1:89991 |      1 |          239.633 |
+---------------------+----------+-----------------+--------+------------------+
+------------------------+------------------------+------------------------+
|   num_env_steps_sample |   num_env_steps_traine |   num_episodes_lifetim |
|             d_lifetime |             d_lifetime |                      e |
+------------------------+------------------------+------------------------|
|                   4000 |                   4000 |                     24 |
+------------------------+------------------------+------------------------+
"""
import gymnasium as gym
import numpy as np
from typing import Optional, Sequence

from ray.rllib.callbacks.callbacks import RLlibCallback
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.env.vector.vector_multi_agent_env import VectorMultiAgentEnv
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.utils.images import resize
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env
from ray import tune

parser = add_rllib_example_script_args(default_reward=20.0)
parser.set_defaults(
    env="ale_py:ALE/Pong-v5",
)


class EnvRenderCallback(RLlibCallback):
    """A custom callback to render the environment.

    This can be used to create videos of the episodes for some or all EnvRunners
    and some or all env indices (in a vectorized env). These videos can then
    be sent to e.g. WandB as shown in this example script here.

    We override the `on_episode_step` method to create a single ts render image
    and temporarily store it in the Episode object.
    """

    def __init__(self, env_runner_indices: Optional[Sequence[int]] = None):
        """Initializes an EnvRenderCallback instance.

        Args:
            env_runner_indices: The (optional) EnvRunner indices, for this callback
                should be active. If None, activates the rendering for all EnvRunners.
                If a Sequence type, only renders, if the EnvRunner index is found in
                `env_runner_indices`.
        """
        super().__init__()
        # Only render and record on certain EnvRunner indices?
        self._env_runner_indices = env_runner_indices
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
        metrics_logger,
        env,
        env_index,
        rl_module,
        **kwargs,
    ) -> None:
        """Adds current render image to episode's temporary data.

        Note that this would work with MultiAgentEpisodes as well.
        """
        # Skip, if this EnvRunner's index is not in `self._env_runner_indices`.
        if (
            self._env_runner_indices is not None
            and env_runner.worker_index not in self._env_runner_indices
        ):
            return

        # If we have a vector env, only render the sub-env at index 0.
        if isinstance(env.unwrapped, (gym.vector.VectorEnv, VectorMultiAgentEnv)):
            image = env.unwrapped.envs[0].render()
        # Render the gym.Env.
        else:
            image = env.unwrapped.render()

        # Original render images for CartPole are 400x600 (hxw). We'll downsize here to
        # a very small dimension (to save space and bandwidth).
        image = resize(image, 64, 96)
        # For WandB videos, we need to put channels first.
        image = np.transpose(image, axes=[2, 0, 1])
        # Add the compiled single-step image as temp. data to our Episode object.
        # Once the episode is done, we'll compile the video from all logged images
        # and log the video with the EnvRunner's `MetricsLogger.log_...()` APIs.
        # See below:
        # `on_episode_end()`: We compile the video and maybe store it).
        # `on_sample_end()` We log the best and worst video to the `MetricsLogger`.
        if "render_images" not in episode.custom_data:
            episode.custom_data["render_images"] = []
        episode.custom_data["render_images"].append(image)

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
        """Computes episode's return and compiles a video, iff best/worst in this iter.

        Note that the actual logging to the EnvRunner's MetricsLogger only happens
        at the very env of sampling (when we know, which episode was the best and
        worst). See `on_sample_end` for the implemented logging logic.
        """
        if (
            self._env_runner_indices is not None
            and env_runner.worker_index not in self._env_runner_indices
        ):
            return

        # Get the episode's return.
        episode_return = episode.get_return()

        # Better than the best or worse than worst Episode thus far?
        if (
            episode_return > self.best_episode_and_return[1]
            or episode_return < self.worst_episode_and_return[1]
        ):
            # Pull all images from the temp. data of the episode.
            images = episode.custom_data["render_images"]
            # `images` is now a list of 3D ndarrays

            # Create a video from the images by simply stacking them AND
            # adding an extra B=1 dimension. Note that Tune's WandB logger currently
            # knows how to log the different data types by the following rules:
            # array is shape=3D -> An image (c, h, w).
            # array is shape=4D -> A batch of images (B, c, h, w).
            # array is shape=5D -> A video (1, L, c, h, w), where L is the length of the
            # video.
            # -> Make our video ndarray a 5D one.
            video = np.expand_dims(np.stack(images, axis=0), axis=0)

            # `video` is from the best episode in this cycle (iteration).
            if episode_return > self.best_episode_and_return[1]:
                self.best_episode_and_return = (video, episode_return)
            # `video` is worst in this cycle (iteration).
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
        """Logs the best and worst video to this EnvRunner's MetricsLogger."""
        # Best video.
        if self.best_episode_and_return[0] is not None:
            metrics_logger.log_value(
                "episode_videos_best",
                self.best_episode_and_return[0],
                # Do not reduce the videos (across the various parallel EnvRunners).
                # This would not make sense (mean over the pixels?). Instead, we want to
                # log all best videos of all EnvRunners per iteration.
                reduce=None,
                # B/c we do NOT reduce over the video data (mean/min/max), we need to
                # make sure the list of videos in our MetricsLogger does not grow
                # infinitely and gets cleared after each `reduce()` operation, meaning
                # every time, the EnvRunner is asked to send its logged metrics.
                clear_on_reduce=True,
            )
            self.best_episode_and_return = (None, float("-inf"))
        # Worst video.
        if self.worst_episode_and_return[0] is not None:
            metrics_logger.log_value(
                "episode_videos_worst",
                self.worst_episode_and_return[0],
                # Same logging options as above.
                reduce=None,
                clear_on_reduce=True,
            )
            self.worst_episode_and_return = (None, float("inf"))


if __name__ == "__main__":
    args = parser.parse_args()

    # Register our environment with tune.
    def _env_creator(cfg):
        cfg.update({"render_mode": "rgb_array"})
        if args.env.startswith("ale_py:ALE/"):
            cfg.update(
                {
                    # Make analogous to old v4 + NoFrameskip.
                    "frameskip": 1,
                    "full_action_space": False,
                    "repeat_action_probability": 0.0,
                }
            )
            return wrap_atari_for_new_api_stack(gym.make(args.env, **cfg), framestack=4)
        else:
            return gym.make(args.env, **cfg)

    register_env("env", _env_creator)

    base_config = (
        get_trainable_cls(args.algo).get_default_config()
        # Use the above-registered environment.
        .environment("env")
        # Plug in our custom callback that controls, which videos are created (best,
        # and worst per sampling cycle per EnvRunner) and then logged via the
        # `MetricsLogger` API.
        .callbacks(EnvRenderCallback)
        # Switch off RLlib's logging to avoid having the large videos show up in any log
        # files.
        .debugging(logger_config={"type": tune.logger.NoopLogger})
        # The following settings are beneficial for Atari-type environments. Feel free
        # to adjust these when providing a non-Atari `--env` option.
        .training(
            lambda_=0.95,
            kl_coeff=0.5,
            clip_param=0.1,
            vf_clip_param=10.0,
            entropy_coeff=0.01,
            num_epochs=10,
            # Linearly adjust learning rate based on number of GPUs.
            lr=0.00015 * (args.num_learners or 1),
            grad_clip=100.0,
            grad_clip_by="global_norm",
        )
    )

    if base_config.is_atari:
        base_config.rl_module(
            model_config=DefaultModelConfig(
                conv_filters=[[16, 4, 2], [32, 4, 2], [64, 4, 2], [128, 4, 2]],
                conv_activation="relu",
                head_fcnet_hiddens=[256],
                vf_share_layers=True,
            ),
        )

    run_rllib_example_script_experiment(base_config, args)
