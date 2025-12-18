"""Example showing how to train SAC on the classic Pendulum continuous control task.

Soft Actor-Critic (SAC) is an off-policy maximum entropy reinforcement learning
algorithm. This example demonstrates SAC on the Pendulum-v1 environment, a simple
continuous control benchmark where the goal is to swing up and balance a pendulum.

This example:
- Trains on the Pendulum-v1 environment (simple single-joint pendulum swing-up)
- Uses prioritized experience replay buffer with capacity of 100k transitions
- Configures separate learning rates for actor, critic, and alpha (temperature)
- Applies n-step returns with random n in range [2, 5] for variance reduction
- Uses automatic entropy tuning with target_entropy="auto"

How to run this script
----------------------
`python pendulum_sac.py`

For faster training with multiple learners:
`python pendulum_sac.py --num-learners=2 --num-env-runners=4`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0 --num-learners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
Training should reach a reward of ~-250 (near-optimal swing-up) within 20k timesteps.

+------------------------------------+------------+--------+------------------+
| Trial name                         | status     |   iter |   total time (s) |
|------------------------------------+------------+--------+------------------+
| SAC_Pendulum-v1_xxxxx_00000        | TERMINATED |     XX |            XX.XX |
+------------------------------------+------------+--------+------------------+
"""
from torch import nn

from ray.rllib.algorithms.sac.sac import SACConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_timesteps=20000,
    default_reward=-250.0,
)
args = parser.parse_args()

config = (
    SACConfig()
    .environment("Pendulum-v1")
    .training(
        initial_alpha=1.001,
        # Use a smaller learning rate for the policy.
        actor_lr=2e-4 * (args.num_learners or 1) ** 0.5,
        critic_lr=8e-4 * (args.num_learners or 1) ** 0.5,
        alpha_lr=9e-4 * (args.num_learners or 1) ** 0.5,
        lr=None,
        target_entropy="auto",
        n_step=(2, 5),
        tau=0.005,
        train_batch_size_per_learner=256,
        target_network_update_freq=1,
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 100000,
            "alpha": 1.0,
            "beta": 0.0,
        },
        num_steps_sampled_before_learning_starts=256 * (args.num_learners or 1),
    )
    .rl_module(
        model_config=DefaultModelConfig(
            fcnet_hiddens=[256, 256],
            fcnet_activation="relu",
            fcnet_kernel_initializer=nn.init.xavier_uniform_,
            head_fcnet_hiddens=[],
            head_fcnet_activation=None,
            head_fcnet_kernel_initializer="orthogonal_",
            head_fcnet_kernel_initializer_kwargs={"gain": 0.01},
        ),
    )
)


if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
