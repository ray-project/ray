"""Example showing how to run APPO on Atari environments with frame stacking.

This example demonstrates APPO (Asynchronous Proximal Policy Optimization) on
Atari Pong. APPO is a distributed, off-policy variant of PPO that uses a
circular replay buffer for improved sample efficiency and supports asynchronous
training across multiple learners and env runners.

This example:
    - shows how to use the `FrameStackingEnvToModule` and `FrameStackingLearner`
    ConnectorV2 pieces for proper frame stacking
    - demonstrates how to wrap Atari environments for preprocessing
    - configures a CNN-based model with 4 convolutional layers suitable for
    processing stacked Atari frames
    - uses 2 aggregator actors per learner for efficient experience collection
    - schedules the entropy coefficient to decay from 0.01 to 0.0 over training

How to run this script
----------------------
`python atari_appo.py [options]`

To run with default settings on Pong:
`python atari_appo.py`

To run on a different Atari environment:
`python atari_appo.py --env=ale_py:ALE/SpaceInvaders-v5`

To scale up with multiple learners:
`python atari_appo.py --num-learners=2 --num-env-runners=8`

For debugging, use the following additional command line options
`--no-tune --num-learners=0`
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
The algorithm should reach the default reward threshold of XX on Breakout
within 10 million timesteps (40 million frames with 4x frame stacking).
The number of environment steps can be changed through `default_timesteps`.
Training performance scales with the number of learners and env runners.
The entropy coefficient schedule (decaying from 0.01 to 0.0 over 3 million
timesteps) is crucial for achieving good final performance - removing this
schedule will likely result in suboptimal policies.
"""
import gymnasium as gym

import ray
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.connectors.env_to_module.frame_stacking import FrameStackingEnvToModule
from ray.rllib.connectors.learner.frame_stacking import FrameStackingLearner
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_reward=18.0,  # TODO: Determine accurate reward scale
    default_timesteps=10_000_000,  # 40 million frames
)
parser.set_defaults(
    env="ale_py:ALE/Breakout-v5",
    num_env_runners=5,
    num_envs_per_env_runner=5,
)
args = parser.parse_args()


def env_creator(cfg):
    return wrap_atari_for_new_api_stack(
        gym.make(args.env, **cfg, **{"render_mode": "rgb_array"}),
        dim=64,
        framestack=None,
    )


def make_frame_stacking_env_to_module(env, spaces, device):
    return FrameStackingEnvToModule(num_frames=4)


def make_frame_stacking_learner(input_obs_space, input_action_space):
    return FrameStackingLearner(num_frames=4)


ray.tune.register_env("atari-env", env_creator)

config = (
    APPOConfig()
    .environment(
        "atari-env",
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
        env_to_module_connector=make_frame_stacking_env_to_module,
    )
    .learners(
        num_aggregator_actors_per_learner=2,
    )
    .training(
        learner_connector=make_frame_stacking_learner,
        train_batch_size_per_learner=500,
        target_network_update_freq=2,
        lr=0.0005 * ((args.num_learners or 1) ** 0.5),
        vf_loss_coeff=1.0,
        entropy_coeff=[[0, 0.01], [3_000_000, 0.0]],  # <- crucial parameter to finetune
        # Only update connector states and model weights every n training_step calls.
        broadcast_interval=5,
        # learner_queue_size=1,
        circular_buffer_num_batches=4,
        circular_buffer_iterations_per_batch=2,
    )
    .rl_module(
        model_config=DefaultModelConfig(
            vf_share_layers=True,
            conv_filters=[
                (16, 4, 2),
                (32, 4, 2),
                (64, 4, 2),
                (128, 4, 2),
            ],  # TO CONFIRM ARCHITECTURE
            conv_activation="relu",
            head_fcnet_hiddens=[256],
        )
    )
)


if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
