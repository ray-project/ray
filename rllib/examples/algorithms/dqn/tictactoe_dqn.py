"""Example showing how to run multi-agent DQN on TicTacToe with self-play.

This example demonstrates multi-agent DQN training on a TicTacToe environment
where multiple learning policies compete against each other and a random
baseline. Self-play with diverse opponents helps agents learn robust strategies.

This example:
- uses the TicTacToe environment where two players alternate placing X and O
- configures multiple trainable DQN policies (default: 5) that learn simultaneously
- includes a non-trainable random policy as a baseline opponent
- uses random policy mapping so each game pairs arbitrary combinations of
  policies (learning or random), providing diverse training experiences
- configures prioritized experience replay with 50K capacity per policy
- enables double DQN and dueling architecture for stable multi-agent learning
- applies variable n-step returns (2-5 steps) and epsilon decay (1.0 to 0.02)
- scales learning rate with sqrt of number of learners for distributed training

How to run this script
----------------------
`python tictactoe_dqn.py [options]`

To run with default settings (5 learning policies):
`python tictactoe_dqn.py`

To change the number of competing policies:
`python tictactoe_dqn.py --num-agents=3`

To scale up with more parallel environments:
`python tictactoe_dqn.py --num-env-runners=10 --num-envs-per-env-runner=16`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key]
 --wandb-project=[some project name]
 --wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
The algorithm should reach the default reward threshold of -0.5 within
1,000,000 timesteps. In TicTacToe, rewards are +1 for win, -1 for loss, and
0 for draw. A mean reward of -0.5 indicates policies are winning more often
than losing against the mix of opponents including the random baseline.
"""
import random

from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.core.rl_module import MultiRLModuleSpec, RLModuleSpec
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from tic_tac_toe import TicTacToe
from ray.rllib.examples.rl_modules.classes.random_rlm import RandomRLModule
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_reward=-0.5,
    default_timesteps=1_000_000,
)
parser.set_defaults(
    num_env_runners=5,
    num_envs_per_env_runner=8,
    num_agents=5,
)
args = parser.parse_args()

config = (
    DQNConfig()
    .environment(TicTacToe)
    .training(
        lr=0.0005 * (args.num_learners or 1) ** 0.5,
        train_batch_size_per_learner=32,
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 50_000,
            "alpha": 0.6,
            "beta": 0.4,
        },
        n_step=(2, 5),
        double_q=True,
        dueling=True,
        epsilon=[(0, 1.0), (10_000, 0.02)],
    )
    .rl_module(
        rl_module_spec=MultiRLModuleSpec(
            rl_module_specs=(
                {
                    f"p{i}": RLModuleSpec(
                        model_config=DefaultModelConfig(vf_share_layers=True),
                    )
                    for i in range(args.num_agents)
                }
                | {"random": RLModuleSpec(module_class=RandomRLModule)}
            ),
        ),
    )
    .multi_agent(
        policies={f"p{i}" for i in range(args.num_agents)} | {"random"},
        policy_mapping_fn=lambda aid, eps, **kw: (
            random.choice([f"p{i}" for i in range(args.num_agents)] + ["random"])
        ),
        policies_to_train=[f"p{i}" for i in range(args.num_agents)],
    )
)


if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
