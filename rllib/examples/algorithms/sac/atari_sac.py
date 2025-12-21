"""Example showing how to train SAC on Atari environments with frame stacking.

Soft Actor-Critic (SAC) is an off-policy algorithm typically used for continuous
control, but can be adapted for discrete action spaces like Atari games. This
example demonstrates SAC with image observations using custom frame stacking
connectors and convolutional neural networks.

This example:
- Trains on the Pong Atari environment (configurable via --env)
- Uses frame stacking (4 frames) via env-to-module and learner connectors
- Applies Atari-specific preprocessing (64x64 grayscale, reward clipping)
- Configures a CNN architecture suitable for visual observations
- Uses an entropy coefficient schedule that anneals from 0.01 to 0.0 over 3M steps

How to run this script
----------------------
`python atari_sac.py

To run on a different Atari environment:
`python atari_sac.py --env=ale_py:ALE/SpaceInvaders-v5``

To scale up with distributed learning using multiple learners and env-runners:
`python [script file name].py --num-learners=2 --num-env-runners=8`

To use a GPU-based learner add the number of GPUs per learners:
`python [script file name].py --num-learners=1 --num-gpus-per-learner=1`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0 --num-learners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.
By setting `--num-learners=0` and `--num-env-runners=0` will make them run locally
instead of remote Ray Actor where breakpoints aren't possible.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
Training should reach a reward of ~20 (winning most games) within 10M timesteps.
"""
import gymnasium as gym

from ray.rllib.algorithms.sac import SACConfig
from ray.rllib.connectors.env_to_module.frame_stacking import FrameStackingEnvToModule
from ray.rllib.connectors.learner.frame_stacking import FrameStackingLearner
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import register_env

parser = add_rllib_example_script_args(
    default_reward=20.0,
    default_timesteps=10_000_000,
)
parser.set_defaults(
    env="ale_py:ALE/Pong-v5",
)
args = parser.parse_args()


def _make_env_to_module_connector(env, spaces, device):
    return FrameStackingEnvToModule(num_frames=4)


def _make_learner_connector(input_observation_space, input_action_space):
    return FrameStackingLearner(num_frames=4)


def _env_creator(cfg):
    return wrap_atari_for_new_api_stack(
        gym.make(args.env, **cfg, **{"render_mode": "rgb_array"}),
        dim=64,
        framestack=None,
    )


register_env("env", _env_creator)


config = (
    SACConfig()
    .environment(
        "env",
        env_config={
            # Make analogous to old v4 + NoFrameskip.
            "frameskip": 1,
            "full_action_space": False,
            "repeat_action_probability": 0.0,
        },
        clip_rewards=True,
    )
    .env_runners(
        env_to_module_connector=_make_env_to_module_connector,
        num_envs_per_env_runner=2,
    )
    .learners(
        num_aggregator_actors_per_learner=2,
    )
    .training(
        learner_connector=_make_learner_connector,
        train_batch_size_per_learner=500,
        target_network_update_freq=2,
        # lr=0.0006 is very high, w/ 4 GPUs -> 0.0012
        # Might want to lower it for better stability, but it does learn well.
        actor_lr=2e-4 * (args.num_learners or 1) ** 0.5,
        critic_lr=8e-4 * (args.num_learners or 1) ** 0.5,
        alpha_lr=9e-4 * (args.num_learners or 1) ** 0.5,
        target_entropy="auto",
        n_step=(1, 5),  # 1?
        tau=0.005,
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 100000,
            "alpha": 0.6,
            "beta": 0.4,
        },
        num_steps_sampled_before_learning_starts=10_000,
    )
    .rl_module(
        model_config=DefaultModelConfig(
            vf_share_layers=True,
            conv_filters=[(16, 4, 2), (32, 4, 2), (64, 4, 2), (128, 4, 2)],
            conv_activation="relu",
            head_fcnet_hiddens=[256],
        )
    )
)


if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
