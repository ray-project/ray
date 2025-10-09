"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
from ray.rllib.algorithms.dreamerv3.dreamerv3 import DreamerV3Config
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_iters=10000,
    default_reward=-200.0,
    default_timesteps=100000,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()
# If we use >1 GPU and increase the batch size accordingly, we should also
# increase the number of envs per worker.
if args.num_envs_per_env_runner is None:
    args.num_envs_per_env_runner = args.num_learners or 1

# Run with:
# python [this script name].py

# To see all available options:
# python [this script name].py --help

default_config = DreamerV3Config()
lr_multiplier = args.num_learners or 1


config = (
    DreamerV3Config()
    .environment("Pendulum-v1")
    .env_runners(
        remote_worker_envs=(args.num_learners and args.num_learners > 1),
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
        training_ratio=1024,
        batch_size_B=16 * (args.num_learners or 1),
        world_model_lr=default_config.world_model_lr * lr_multiplier,
        actor_lr=default_config.actor_lr * lr_multiplier,
        critic_lr=default_config.critic_lr * lr_multiplier,
    )
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
