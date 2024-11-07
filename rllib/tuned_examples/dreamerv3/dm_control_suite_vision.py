"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""

# Run with:
# python [this script name].py --env DMC/[task]/[domain] (e.g. DMC/cartpole/swingup)

# To see all available options:
# python [this script name].py --help

from ray.rllib.algorithms.dreamerv3.dreamerv3 import DreamerV3Config
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_iters=1000000,
    default_reward=800.0,
    default_timesteps=1000000,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

config = (
    DreamerV3Config()
    # Use image observations.
    .environment(
        env=args.env,
        env_config={"from_pixels": True},
    )
    .env_runners(
        num_env_runners=(args.num_env_runners or 0),
        # If we use >1 GPU and increase the batch size accordingly, we should also
        # increase the number of envs per worker.
        num_envs_per_env_runner=4 * (args.num_learners or 1),
        remote_worker_envs=True,
    )
    .reporting(
        metrics_num_episodes_for_smoothing=(args.num_learners or 1),
        report_images_and_videos=False,
        report_dream_data=False,
        report_individual_batch_item_stats=False,
    )
    # See Appendix A.
    .training(
        model_size="S",
        training_ratio=512,
        batch_size_B=16 * (args.num_learners or 1),
    )
)
