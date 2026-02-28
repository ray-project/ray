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

To scale up with distributed learning using multiple learners and env-runners:
`python tictactoe_sac.py --num-learners=2 --num-env-runners=8`

To use a GPU-based learner add the number of GPUs per learners:
`python tictactoe_sac.py --num-learners=1 --num-gpus-per-learner=1`

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
Training should show all agents learning to swing up their pendulums within 500k
timesteps.
"""
import random

from torch import nn

from ray.rllib.algorithms.sac import SACConfig
from ray.rllib.core.rl_module import MultiRLModuleSpec, RLModuleSpec
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.envs.classes.multi_agent.tic_tac_toe import TicTacToe
from ray.rllib.examples.rl_modules.classes.random_rlm import RandomRLModule
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(default_timesteps=500_000, default_reward=-0.5)
parser.set_defaults(
    num_env_runners=4,
    num_envs_per_env_runner=6,
    num_learners=1,
    num_agents=5,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()


config = (
    SACConfig()
    .environment(TicTacToe)
    .env_runners(
        num_env_runners=args.num_env_runners,
        num_envs_per_env_runner=args.num_envs_per_env_runner,
    )
    .learners(
        num_learners=args.num_learners,
    )
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
        policies={f"p{i}" for i in range(args.num_agents)} | {"random"},
        policy_mapping_fn=lambda aid, eps, **kw: (
            random.choice([f"p{i}" for i in range(args.num_agents)] + ["random"])
        ),
        policies_to_train=[f"p{i}" for i in range(args.num_agents)],
    )
    .rl_module(
        rl_module_spec=MultiRLModuleSpec(
            rl_module_specs=(
                {
                    f"p{i}": RLModuleSpec(
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
                    for i in range(args.num_agents)
                }
                | {"random": RLModuleSpec(module_class=RandomRLModule)}
            ),
        ),
    )
)


if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
