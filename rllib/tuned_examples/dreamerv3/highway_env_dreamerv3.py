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

try:
    import highway_env  # noqa
except (ImportError, ModuleNotFoundError):
    print("You have to `pip install highway_env` in order to run this example!")

import gymnasium as gym

from ray.rllib.algorithms.dreamerv3.dreamerv3 import DreamerV3Config
from ray import tune


# Number of GPUs to run on.
num_gpus = 4

# Register the highway env (including necessary wrappers and options) via the
# `tune.register_env()` API.
# Create the specific env.
# e.g. roundabout-v0 or racetrack-v0
tune.register_env("flappy-bird", lambda ctx: gym.make("intersection-v0", policy_freq=5))

# Define the DreamerV3 config object to use.
config = DreamerV3Config()
w = config.world_model_lr
c = config.critic_lr

(
    config.resources(
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
