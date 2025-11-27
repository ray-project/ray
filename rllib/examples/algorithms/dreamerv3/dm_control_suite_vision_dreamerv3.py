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

from ray import tune
from ray.rllib.algorithms.dreamerv3.dreamerv3 import DreamerV3Config
from ray.rllib.env.wrappers.dm_control_wrapper import ActionClip, DMCEnv
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_iters=1000000,
    default_reward=800.0,
    default_timesteps=1000000,
)
parser.set_defaults(env="DMC/cartpole/swingup")
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()
# If we use >1 GPU and increase the batch size accordingly, we should also
# increase the number of envs per worker.
if args.num_envs_per_env_runner is None:
    args.num_envs_per_env_runner = 4 * (args.num_learners or 1)

parts = args.env.split("/")
assert len(parts) == 3, (
    "ERROR: DMC env must be formatted as 'DMC/[task]/[domain]', e.g. "
    f"'DMC/cartpole/swingup'! You provided '{args.env}'."
)


def env_creator(cfg):
    return ActionClip(
        DMCEnv(
            parts[1],
            parts[2],
            from_pixels=True,
            channels_first=False,
        )
    )


tune.register_env("env", env_creator)

default_config = DreamerV3Config()
lr_multiplier = (args.num_learners or 1) ** 0.5

config = (
    DreamerV3Config()
    # Use image observations.
    .environment(env="env")
    .env_runners(
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
        world_model_lr=default_config.world_model_lr * lr_multiplier,
        actor_lr=default_config.actor_lr * lr_multiplier,
        critic_lr=default_config.critic_lr * lr_multiplier,
    )
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
