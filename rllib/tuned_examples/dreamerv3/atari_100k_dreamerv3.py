"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""

# Run with:
# python [this script name].py --env ale_py:ALE/[gym ID e.g. Pong-v5]

# To see all available options:
# python [this script name].py --help

import gymnasium as gym

from ray import tune
from ray.rllib.algorithms.dreamerv3.dreamerv3 import DreamerV3Config
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_iters=1000000,
    default_reward=20.0,
    default_timesteps=100000,
)
parser.set_defaults(env="ale_py:ALE/Pong-v5")
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()
# If we use >1 GPU and increase the batch size accordingly, we should also
# increase the number of envs per worker.
if args.num_envs_per_env_runner is None:
    args.num_envs_per_env_runner = args.num_learners or 1


# Create the DreamerV3-typical Atari setup.
def _env_creator(cfg):
    return wrap_atari_for_new_api_stack(
        gym.make(args.env, **cfg, render_mode="rgb_array"),
        # No framestacking necessary for Dreamer.
        framestack=None,
        # No grayscaling necessary for Dreamer.
        grayscale=False,
    )


tune.register_env("env", _env_creator)

default_config = DreamerV3Config()
lr_multiplier = args.num_learners or 1

config = (
    DreamerV3Config()
    .environment(
        env="env",
        # [2]: "We follow the evaluation protocol of Machado et al. (2018) with 200M
        # environment steps, action repeat of 4, a time limit of 108,000 steps per
        # episode that correspond to 30 minutes of game play, no access to life
        # information, full action space, and sticky actions. Because the world model
        # integrates information over time, DreamerV2 does not use frame stacking.
        # The experiments use a single-task setup where a separate agent is trained
        # for each game. Moreover, each agent uses only a single environment instance.
        env_config={
            # "sticky actions" but not according to Danijar's 100k configs.
            "repeat_action_probability": 0.0,
            # "full action space" but not according to Danijar's 100k configs.
            "full_action_space": False,
            # Already done by MaxAndSkip wrapper: "action repeat" == 4.
            "frameskip": 1,
        },
    )
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
