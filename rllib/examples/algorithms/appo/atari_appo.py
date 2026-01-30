"""Example showing how to run APPO on Atari environments with frame stacking.

This example demonstrates APPO (Asynchronous Proximal Policy Optimization) on
Atari Pong. APPO is a distributed, off-policy variant of PPO that uses a
circular replay buffer for improved sample efficiency and supports asynchronous
training across multiple learners and env runners.

This example:
    - demonstrates how to wrap Atari environments for preprocessing
    - configures a CNN-based model with 4 convolutional layers suitable for
    processing stacked Atari frames
    - shows how to use the `FrameStackingEnvToModule` and `FrameStackingLearner`
    ConnectorV2 pieces for proper frame stacking
    - uses 2 aggregator actors per learner for efficient experience collection
        (see: `num_aggregator_actors_per_learner=2` in the learner configuration)
    - schedules the entropy coefficient to decay from 0.025 to 0.0 over training

How to run this script
----------------------
`python atari_appo.py [options]`

To run with default settings on Pong:
`python atari_appo.py`

To run on a different Atari environment:
`python atari_appo.py --env=ale_py:ALE/SpaceInvaders-v5`

To scale up with distributed learning using multiple learners and env-runners:
`python atari_appo.py --num-learners=2 --num-env-runners=8`

To use a GPU-based learner add the number of GPUs per learners:
`python atari_appo.py --num-learners=1 --num-gpus-per-learner=1`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0 --num-learners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.
By setting `--num-learners=0` and `--num-env-runners=0` will make them run locally
instead of remote Ray Actor where breakpoints aren't possible.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key]
 --wandb-project=[some project name]
 --wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
The algorithm should reach the default reward threshold of 18.0 on Pong
within 10 million timesteps (40 million frames with 4x frame stacking,
see: `default_timesteps` in the code).
Training performance scales with the number of learners and env runners.
The entropy coefficient schedule (decaying from 0.025 to 0.0 over 1 million
timesteps) is crucial for achieving good final performance - removing this
schedule will likely result in suboptimal policies.
"""
import gymnasium as gym

from ray.rllib.algorithms.appo import APPOConfig
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
    default_iters=200,
    default_reward=18.0,
    default_timesteps=10_000_000,  # 40 million frames
)
parser.set_defaults(
    env="ale_py:ALE/Pong-v5",
    num_env_runners=4,
    num_envs_per_env_runner=10,
    num_learners=1,
    num_aggregator_actors_per_learner=2,
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
    APPOConfig()
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
        rollout_fragment_length=64,
        env_to_module_connector=_make_env_to_module_connector,
    )
    .learners(
        num_aggregator_actors_per_learner=args.num_aggregator_actors_per_learner,
        num_learners=args.num_learners,
    )
    .training(
        learner_connector=_make_learner_connector,
        train_batch_size_per_learner=1280,
        target_network_update_freq=2,
        lr=0.0006,
        vf_loss_coeff=1.25,
        entropy_coeff=[
            [0, 0.025],
            [1_000_000, 0.0],
        ],
        clip_param=0.4,
        lambda_=0.95,
        # Only update connector states and model weights every n training_step calls.
        broadcast_interval=8,
        circular_buffer_num_batches=4,
        circular_buffer_iterations_per_batch=2,
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
