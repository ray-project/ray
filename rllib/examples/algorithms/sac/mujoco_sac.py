"""Example showing how to train SAC on MuJoCo's HalfCheetah continuous control task.

Soft Actor-Critic (SAC) is an off-policy maximum entropy reinforcement learning
algorithm that excels at continuous control tasks. This example demonstrates SAC
on the HalfCheetah-v4 MuJoCo environment with prioritized experience replay and
n-step returns.

This example:
- Trains on the HalfCheetah-v4 MuJoCo locomotion environment
- Uses prioritized experience replay buffer (alpha=0.6, beta=0.4)
- Configures separate learning rates for actor, critic, and alpha (temperature)
- Applies n-step returns with random n in range [1, 5] for each sampled transition
- Uses automatic entropy tuning with target_entropy="auto"

How to run this script
----------------------
`python mujoco_sac.py`

For faster training with multiple learners:
`python mujoco_sac.py --num-learners=2 --num-env-runners=8`

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

+--------------------------------------+------------+--------+------------------+
| Trial name                           | status     |   iter |   total time (s) |
|--------------------------------------+------------+--------+------------------+
| SAC_HalfCheetah-v4_xxxxx_00000       | TERMINATED |   2000 |         XXXXX.XX |
+--------------------------------------+------------+--------+------------------+
"""
from torch import nn

from ray.rllib.algorithms.sac.sac import SACConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_timesteps=1_000_000,
    default_reward=12_000.0,
    default_iters=2_000,
)
args = parser.parse_args()

config = (
    SACConfig()
    .environment("HalfCheetah-v4")
    .training(
        initial_alpha=1.001,
        # lr=0.0006 is very high, w/ 4 GPUs -> 0.0012
        # Might want to lower it for better stability, but it does learn well.
        actor_lr=2e-4 * (args.num_learners or 1) ** 0.5,
        critic_lr=8e-4 * (args.num_learners or 1) ** 0.5,
        alpha_lr=9e-4 * (args.num_learners or 1) ** 0.5,
        lr=None,
        target_entropy="auto",
        n_step=(1, 5),  # 1?
        tau=0.005,
        train_batch_size_per_learner=256,
        target_network_update_freq=1,
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 100000,
            "alpha": 0.6,
            "beta": 0.4,
        },
        num_steps_sampled_before_learning_starts=10000,
    )
    .rl_module(
        model_config=DefaultModelConfig(
            fcnet_hiddens=[256, 256],
            fcnet_activation="relu",
            fcnet_kernel_initializer=nn.init.xavier_uniform_,
            head_fcnet_hiddens=[],
            head_fcnet_kernel_initializer="orthogonal_",
            head_fcnet_kernel_initializer_kwargs={"gain": 0.01},
        ),
    )
    .reporting(
        metrics_num_episodes_for_smoothing=5,
        min_sample_timesteps_per_iteration=1000,
    )
)


if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
