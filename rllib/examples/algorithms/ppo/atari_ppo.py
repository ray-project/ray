"""Example showing how to train PPO on Atari games with frame stacking.

This example demonstrates training PPO on the Atari Pong environment using
RLlib's ConnectorV2 API for frame stacking. Frame stacking is a common
technique in Atari RL that provides temporal information by stacking
consecutive observation frames.

This example:
- Trains on Atari Pong (ale_py:ALE/Pong-v5) by default
- Uses custom env-to-module and learner connectors for frame stacking (4 frames)
- Configures a CNN architecture optimized for Atari visual observations
- Applies reward clipping and tuned PPO hyperparameters
- Expects to reach reward of 18.0 within 3 million timesteps

How to run this script
----------------------
`python atari_ppo.py --env=ale_py:ALE/Pong-v5`

Use the `--env` flag to specify different Atari environments (e.g., Breakout,
SpaceInvaders).

To run with different configuration:
`python atari_ppo.py --stop-reward=20.0 --stop-timesteps=5000000`

To scale up with distributed learning using multiple learners and env-runners:
`python atari_ppo.py --num-learners=2 --num-env-runners=8`

To use a GPU-based learner add the number of GPUs per learners:
`python atari_ppo.py --num-learners=1 --num-gpus-per-learner=1`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0 --num-learners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
 --wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
With the default settings, you should expect to reach a reward of ~18.0 within
approximately 3 million environment timesteps. The learning rate is scaled
linearly with the number of learners for distributed training.
"""
import gymnasium as gym

from ray.rllib.algorithms.ppo import PPOConfig
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
    default_reward=18.0,
    default_timesteps=3_000_000,
    default_iters=1_000,
)
parser.set_defaults(
    env="ale_py:ALE/Pong-v5",
    num_env_runners=4,
    num_envs_per_env_runner=10,
    num_learners=1,
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
    PPOConfig()
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
        num_env_runners=args.num_env_runners,
        num_envs_per_env_runner=args.num_envs_per_env_runner,
        rollout_fragment_length=32,
        env_to_module_connector=_make_env_to_module_connector,
    )
    .learners(
        num_learners=args.num_learners,
    )
    .training(
        learner_connector=_make_learner_connector,
        train_batch_size_per_learner=1280,
        minibatch_size=256,
        num_epochs=10,
        lr=0.0006,
        vf_loss_coeff=1.25,
        entropy_coeff=[
            [0, 0.025],
            [1_000_000, 0.0],
        ],
        gamma=0.99,
        lambda_=0.95,
        kl_coeff=0.5,
        clip_param=0.4,
        vf_clip_param=10.0,
        grad_clip=100.0,
        grad_clip_by="global_norm",
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
