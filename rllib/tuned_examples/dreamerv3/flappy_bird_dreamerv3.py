"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""

# Run with:
# python [this script name].py

# To see all available options:
# python [this script name].py --help

from ray.rllib.algorithms.dreamerv3.dreamerv3 import DreamerV3Config
from ray import tune


# Number of GPUs to run on.
num_gpus = 0

# DreamerV3 config and default (1 GPU) learning rates.
config = DreamerV3Config()
w = config.world_model_lr
c = config.critic_lr


def _env_creator(ctx):
    import flappy_bird_gymnasium  # noqa doctest: +SKIP
    import gymnasium as gym
    from supersuit.generic_wrappers import resize_v1
    from ray.rllib.env.wrappers.atari_wrappers import NormalizedImageEnv

    return NormalizedImageEnv(
        resize_v1(  # resize to 64x64 and normalize images
            gym.make("FlappyBird-rgb-v0", audio_on=False), x_size=64, y_size=64
        )
    )


# Register the FlappyBird-rgb-v0 env including necessary wrappers via the
# `tune.register_env()` API.
tune.register_env("flappy-bird", _env_creator)

# Further specify the DreamerV3 config object to use.
(
    config.environment("flappy-bird")
    .resources(
        num_cpus_for_main_process=1,
    )
    .learners(
        num_learners=0 if num_gpus == 1 else num_gpus,
        num_gpus_per_learner=1 if num_gpus else 0,
    )
    .env_runners(
        # If we use >1 GPU and increase the batch size accordingly, we should also
        # increase the number of envs per worker.
        num_envs_per_env_runner=8 * (num_gpus or 1),
        remote_worker_envs=True,
    )
    .reporting(
        metrics_num_episodes_for_smoothing=(num_gpus or 1),
        report_images_and_videos=False,
        report_dream_data=False,
        report_individual_batch_item_stats=False,
    )
    # See Appendix A.
    .training(
        model_size="M",
        training_ratio=64,
        batch_size_B=16 * (num_gpus or 1),
        # Use a well established 4-GPU lr scheduling recipe:
        # ~ 1000 training updates with 0.4x[default rates], then over a few hundred
        # steps, increase to 4x[default rates].
        world_model_lr=[[0, 0.4 * w], [8000, 0.4 * w], [10000, 3 * w]],
        critic_lr=[[0, 0.4 * c], [8000, 0.4 * c], [10000, 3 * c]],
        actor_lr=[[0, 0.4 * c], [8000, 0.4 * c], [10000, 3 * c]],
    )
)
