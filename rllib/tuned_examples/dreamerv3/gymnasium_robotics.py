"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""

# Run with:
# python run_regression_tests.py --dir [this file]

try:
    import gymnasium_robotics  # noqa
except (ImportError, ModuleNotFoundError):
    print("You have to `pip install gymnasium_robotics` in order to run this example!")

import gymnasium as gym

from ray.rllib.algorithms.dreamerv3.dreamerv3 import DreamerV3Config
from ray import tune


# Number of GPUs to run on.
num_gpus = 4

# Register the gymnasium robotics env (including necessary wrappers and options) via the
# `tune.register_env()` API.
# Create the specific gymnasium robotics env.
# e.g. AdroitHandHammerSparse-v1 or FrankaKitchen-v1.
# return gym.make("FrankaKitchen-v1", tasks_to_complete=["microwave", "kettle"])
tune.register_env("flappy-bird", lambda ctx: gym.make("AdroitHandHammer-v1"))

# Define the DreamerV3 config object to use.
config = DreamerV3Config()
w = config.world_model_lr
c = config.critic_lr
# Further specify the details of our config object.
(
    config.resources(
        num_cpus_for_main_process=8 * (num_gpus or 1),
    )
    .learners(
        num_learners=0 if num_gpus == 1 else num_gpus,
        num_gpus_per_learner=1 if num_gpus else 0,
    )
    # If we use >1 GPU and increase the batch size accordingly, we should also
    # increase the number of envs per worker.
    .env_runners(num_envs_per_env_runner=8 * (num_gpus or 1), remote_worker_envs=True)
    .reporting(
        metrics_num_episodes_for_smoothing=(num_gpus or 1),
        report_images_and_videos=False,
        report_dream_data=False,
        report_individual_batch_item_stats=False,
    )
    # See Appendix A.
    .training(
        model_size="XL",
        training_ratio=64,
        batch_size_B=16 * (num_gpus or 1),
        world_model_lr=[[0, 0.4 * w], [50000, 0.4 * w], [100000, 3 * w]],
        critic_lr=[[0, 0.4 * c], [50000, 0.4 * c], [100000, 3 * c]],
        actor_lr=[[0, 0.4 * c], [50000, 0.4 * c], [100000, 3 * c]],
    )
)
