"""Example showing how to run multi-agent IMPALA on TicTacToe with self-play.

This example demonstrates multi-agent reinforcement learning using IMPALA on a
TicTacToe environment. The setup includes trainable policies that learn to play
against each other and a frozen random policy that provides diverse opponents.
This self-play with random opponents approach helps prevent overfitting to a
single opponent strategy.

This example:
    - trains multiple policies on the TicTacToe multi-agent environment
    - uses a RandomRLModule as a frozen opponent that is not trained
    - randomly maps agents to policies (including the random policy) each episode
    - demonstrates MultiRLModuleSpec for configuring multiple policies
    - uses 4 env runners by default for parallel experience collection

How to run this script
----------------------
`python tictactoe_impala.py [options]`

To run with default settings (5 trainable agents):
`python tictactoe_impala.py`

To run with a different number of trainable agents:
`python tictactoe_impala.py --num-agents=4`

To scale up with distributed learning using multiple learners and env-runners:
`python tictactoe_impala.py --num-learners=2 --num-env-runners=8`

To use a GPU-based learner add the number of GPUs per learner:
`python tictactoe_impala.py --num-learners=1 --num-gpus-per-learner=1`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0 --num-learners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.
By setting `--num-learners=0` and `--num-env-runners=0` will make them run locally
instead of as remote Ray Actors where breakpoints aren't possible.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key]
 --wandb-project=[some project name]
 --wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
The trained policies should achieve an average reward of at least -0.5 within
2 million timesteps. Since the policies play against both each other and a
random opponent, rewards will vary. A reward close to 0 or positive indicates
the policies are learning to win or draw more often than they lose.
"""
import random

from ray.rllib.algorithms.impala import IMPALAConfig
from ray.rllib.core.rl_module import MultiRLModuleSpec, RLModuleSpec
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.envs.classes.multi_agent.tic_tac_toe import TicTacToe
from ray.rllib.examples.rl_modules.classes.random_rlm import RandomRLModule
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_reward=-0.5,
    default_timesteps=2_000_000,
)
parser.set_defaults(
    num_env_runners=4,
    num_envs_per_env_runner=3,
    num_learners=1,
    num_agents=5,
)
args = parser.parse_args()

config = (
    IMPALAConfig()
    .environment(TicTacToe)
    .env_runners(
        num_env_runners=args.num_env_runners,
        num_envs_per_env_runner=args.num_envs_per_env_runner,
    )
    .learners(
        num_learners=args.num_learners,
    )
    .training(
        train_batch_size_per_learner=1000,
        grad_clip=30.0,
        grad_clip_by="global_norm",
        lr=0.0005,
        vf_loss_coeff=0.01,
        entropy_coeff=0.0,
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
