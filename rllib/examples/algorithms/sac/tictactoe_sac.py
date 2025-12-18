"""Example showing how to train SAC in a multi-agent Pendulum environment.

This example demonstrates Soft Actor-Critic (SAC) in a multi-agent setting where
multiple independent agents each control their own pendulum. Each agent has its
own policy that learns to swing up and balance its pendulum.

This example:
- Trains on the MultiAgentPendulum environment with configurable number of agents
- Uses a multi-agent prioritized experience replay buffer
- Configures separate policies for each agent via policy_mapping_fn
- Applies n-step returns with random n in range [2, 5]
- Uses automatic entropy tuning with target_entropy="auto"

How to run this script
----------------------
`python tictactoe_sac.py --num-agents=2`

To train with more agents:
`python tictactoe_sac.py --num-agents=4`

For faster training with multiple learners:
`python tictactoe_sac.py --num-learners=2 --num-env-runners=4`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0 --num-learners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
Training should show all agents learning to swing up their pendulums within 500k
timesteps.

+--------------------------------------------+------------+--------+------------------+
| Trial name                                 | status     |   iter |   total time (s) |
|--------------------------------------------+------------+--------+------------------+
| SAC_multi_agent_pendulum_xxxxx_00000       | TERMINATED |    XXX |          XXXX.XX |
+--------------------------------------------+------------+--------+------------------+
"""
from torch import nn

from ray.rllib.algorithms.sac import SACConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentPendulum
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import register_env

parser = add_rllib_example_script_args(
    default_timesteps=500000,
)
parser.set_defaults(
    num_agents=2,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

register_env("multi_agent_pendulum", lambda cfg: MultiAgentPendulum(config=cfg))

config = (
    SACConfig()
    .environment("multi_agent_pendulum", env_config={"num_agents": args.num_agents})
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
            "type": "MultiAgentPrioritizedEpisodeReplayBuffer",
            "capacity": 100000,
            "alpha": 1.0,
            "beta": 0.0,
        },
        num_steps_sampled_before_learning_starts=256,
    )
    .multi_agent(
            policy_mapping_fn=lambda aid, *arg, **kw: f"p{aid}",
            policies={f"p{i}" for i in range(args.num_agents)},
        )
    .rl_module(
        model_config=DefaultModelConfig(
            fcnet_hiddens=[256, 256],
            fcnet_activation="relu",
            fcnet_kernel_initializer=nn.init.xavier_uniform_,
            head_fcnet_hiddens=[],
            head_fcnet_activation=None,
            head_fcnet_kernel_initializer=nn.init.orthogonal_,
            head_fcnet_kernel_initializer_kwargs={"gain": 0.01},
        ),
    )
)


if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
