"""TODO (sven)
"""
import gymnasium as gym

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
    be sent to e.g. WandB as shown in this example here.

    We override the `on_episode_step` method to create a single ts render image
    and temporarily store it in the Episode object.
    """
    def on_episode_step(
        self,
        *,
        episode,
        env_runner,
        env,
        rl_module,
        env_index,
        **kwargs,
    ) -> None:
        """Adds render image to episode."""
        # If we have a vector env, only render the sub-env at index 0.
        if isinstance(env, gym.vector.VectorEnv):
            image = env.envs[0].render()
        else:
            image = env.render()
        episode.add_temporary_timestep_data("render_image", image)

    def on_episode_end(
        self,
        *,
        episode,
        env_runner,
        env,
        rl_module,
        env_index,
        **kwargs,
    ) -> None:
        env_runner.metrics_logger.log_per_episode_value(
            "",
            episode_id
        )


parser = add_rllib_example_script_args(
    default_iters=500,
    default_timesteps=500000,
    default_reward=-300.0,
)


if __name__ == "__main_"
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
        .environment("env", env_config={
            # Make analogous to old v4 + NoFrameskip.
            "frameskip": 1,
            "full_action_space": False,
            "repeat_action_probability": 0.0,
        })
        .callbacks(EnvRenderCallback)
        .training(
            model=dict(
                {
                },
            ),
        )
    )

    run_rllib_example_script_experiment(base_config, args)
