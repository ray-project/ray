"""Example showing how to train SAC on MuJoCo's Humanoid continuous control task.

Soft Actor-Critic (SAC) is an off-policy maximum entropy reinforcement learning
algorithm that excels at continuous control tasks. This example demonstrates SAC
on the Humanoid-v4 MuJoCo environment with prioritized experience replay and
n-step returns.

This example:
- Trains on the Humanoid-v4 MuJoCo locomotion environment
- Uses prioritized experience replay buffer (alpha=0.6, beta=0.4)
- Configures separate learning rates for actor, critic, and alpha (temperature)
- Applies n-step returns with random n in range [1, 5] for each sampled transition
- Uses automatic entropy tuning with target_entropy="auto"

How to run this script
----------------------
`python mujoco_sac.py`

To run on a different Atari environment:
`python mujoco_sac.py --env=HalfCheetah-v4``

To scale up with distributed learning using multiple learners and env-runners:
`python mujoco_sac.py --num-learners=2 --num-env-runners=8`

To use a GPU-based learner add the number of GPUs per learners:
`python mujoco_sac.py --num-learners=1 --num-gpus-per-learner=1`

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
Training should reach a reward of ~12,000 within 1M timesteps (~2000 iterations).
"""
import numpy as np
from torch import nn

from ray.rllib.algorithms.sac.sac import SACConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_reward=800.0,
    default_timesteps=1_000_000,
    default_iters=1000,
)
parser.set_defaults(
    env="Humanoid-v4",
    num_env_runners=4,
    num_envs_per_env_runner=16,
    num_learners=1,
)
args = parser.parse_args()

config = (
    SACConfig()
    .environment(args.env)
    .env_runners(
        num_env_runners=args.num_env_runners,
        num_envs_per_env_runner=args.num_envs_per_env_runner,
    )
    .learners(
        num_learners=args.num_learners,
    )
    .training(
        initial_alpha=1.0,
        actor_lr=0.0005,
        critic_lr=0.00035,
        alpha_lr=0.005,
        lr=None,
        target_entropy="auto",
        n_step=(1, 5),
        tau=0.005,
        target_network_update_freq=1,
        train_batch_size_per_learner=256,
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 256_000,
            "alpha": 0.6,
            "beta": 0.4,
        },
        num_steps_sampled_before_learning_starts=16_000,
    )
    .rl_module(
        model_config=DefaultModelConfig(
            fcnet_hiddens=[256, 256],
            fcnet_activation="relu",
            fcnet_kernel_initializer=nn.init.xavier_uniform_,
            head_fcnet_kernel_initializer="orthogonal_",
            head_fcnet_kernel_initializer_kwargs={"gain": np.sqrt(2)},
        ),
    )
)


if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
